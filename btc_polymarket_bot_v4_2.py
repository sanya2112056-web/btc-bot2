"""
BTC Polymarket Trading Bot — v5 ADAPTIVE
Railway Variables: TELEGRAM_BOT_TOKEN + ANTHROPIC_API_KEY
Magic.Link | signature_type=1 | auto-trade every 15min

ЗМІНИ vs v3:
 - Видалено AI агента
 - DEAD сесія = повна тиша (без сигналів, без торгівлі)
 - Smart Exit: кожні 3 сек з 8-ї хвилини
 - 4 адаптивних промпти під фази ринку
 - Самовдосконалення: кожні 30 сигналів Claude аналізує CSV і оновлює правила
 - Уточнена інверсія: тільки ASK_HEAVY або DISTRIBUTION (на основі даних)
 - Кнопки YES/NO/продати з Polymarket
 - Winrate через зміну балансу
"""
import asyncio, logging, json, time, datetime, os, re, io, csv, requests
import anthropic
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
OPENAI_API_KEY     = os.getenv("ANTHROPIC_API_KEY", "")
OI_CACHE     = "oi_cache.json"
DUMP_FILE    = "signals_dump.json"
POLY_STATS   = "poly_stats.json"
POLY_WR      = "poly_winrate.json"
ADAPT_FILE   = "adaptive_rules.json"   # поточні адаптивні правила

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# СЕСІЯ
# ─────────────────────────────────────────────
class Session:
    def __init__(self, uid):
        self.uid        = uid
        self.key        = ""
        self.funder     = ""
        self.address    = None
        self.ok         = False
        self.auto       = False
        self.signals    = []
        self.trades     = []
        self.open_bets  = []
        self.state      = None
        self.tmp_key    = None
        self.pending    = {}
        self.hist       = "hist_%d.json" % uid
        self.err_f      = "err_%d.json"  % uid
        self._client    = None
        self._asia_auto = False
        self._load()

    def _load(self):
        try:
            if os.path.exists(self.hist):
                with open(self.hist) as f: self.signals = json.load(f)
        except: pass

    def save(self):
        try:
            with open(self.hist, "w") as f:
                json.dump(self.signals, f, ensure_ascii=False, indent=2)
        except: pass

    def log_err(self, sig):
        try:
            e = []
            if os.path.exists(self.err_f):
                with open(self.err_f) as f: e = json.load(f)
            e.append(sig); e = e[-300:]
            with open(self.err_f, "w") as f: json.dump(e, f, ensure_ascii=False, indent=2)
        except: pass

    def bet_size(self, bal):
        if not bal or bal <= 0: return 0.0
        return round(max(1.0, min(bal * 0.10, 500.0)), 2)

    def reset_client(self):
        self._client = None

    def consecutive_same(self):
        if not self.signals: return 0, None
        last_dec = self.signals[-1].get("dec", "")
        count = 0
        for sig in reversed(self.signals):
            if sig.get("dec", "") == last_dec: count += 1
            else: break
        return count, last_dec

_sessions = {}
def sess(uid):
    if uid not in _sessions: _sessions[uid] = Session(uid)
    return _sessions[uid]

# ─────────────────────────────────────────────
# АДАПТИВНІ ПРАВИЛА
# Завантажуються з файлу, оновлюються кожні 30 сигналів
# ─────────────────────────────────────────────
DEFAULT_RULES = {
    "skip_amd_phases":    ["DISTRIBUTION"],      # пропускати ці AMD фази
    "invert_conditions":  ["ASK_HEAVY"],          # інвертувати при цих OB bias
    "skip_if_score_below": 1,                     # пропускати якщо score нижче
    "boost_conditions":   ["MANIPULATION_DONE"],  # підсилені AMD (знижений поріг входу)
    "min_score_high":     5,                      # мін score для HIGH
    "notes": "Default rules based on initial CSV analysis. MANIPULATION_DONE=68%, DISTRIBUTION=25%, ASK_HEAVY=35%"
}

def load_rules():
    try:
        if os.path.exists(ADAPT_FILE):
            with open(ADAPT_FILE) as f: return json.load(f)
    except: pass
    return DEFAULT_RULES.copy()

def save_rules(rules):
    try:
        with open(ADAPT_FILE, "w") as f: json.dump(rules, f, ensure_ascii=False, indent=2)
    except Exception as e: log.error("save_rules: %s", e)

_rules = load_rules()

# ─────────────────────────────────────────────
# CLOB КЛІЄНТ
# ─────────────────────────────────────────────
def get_client(s):
    if s._client is not None: return s._client
    from py_clob_client_v2 import ClobClient
    HOST = "https://clob.polymarket.com"
    tmp    = ClobClient(host=HOST, chain_id=137, key=s.key,
                        signature_type=1, funder=s.funder)
    creds  = tmp.create_or_derive_api_key()
    client = ClobClient(host=HOST, chain_id=137, key=s.key,
                        creds=creds, signature_type=1, funder=s.funder)
    s._client = client
    try:
        ak = creds.api_key if hasattr(creds, "api_key") else str(creds)[:12]
        print("[Client v2] OK key=%s..." % str(ak)[:12])
    except: print("[Client v2] OK")
    return client

# ─────────────────────────────────────────────
# БАЛАНС
# ─────────────────────────────────────────────
def get_balance(s):
    if not s.ok or not s.key: return None, "Гаманець не підключено"
    try:
        from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams
        client = get_client(s)
        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        resp   = client.get_balance_allowance(params=params)
        raw = float(resp.get("balance") or 0) if isinstance(resp, dict) else float(getattr(resp, "balance", 0) or 0)
        bal = raw / 1e6 if raw > 1000 else raw
        return round(bal, 2), s.funder
    except Exception as e:
        err = str(e)
        if any(x in err.lower() for x in ["unauthorized", "401", "forbidden", "invalid"]): s.reset_client()
        return 0.0, err[:100]

def get_token_balance(s, token_id):
    try:
        from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams
        params = BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
        resp   = get_client(s).get_balance_allowance(params=params)
        raw    = float(resp.get("balance") or 0) if isinstance(resp, dict) else float(getattr(resp, "balance", 0) or 0)
        return round((raw / 1e6 if raw > 1000 else raw), 4)
    except: return 0.0

def get_token_price(token_id):
    try:
        r = requests.get("https://clob.polymarket.com/midpoints",
                         params={"token_id": token_id}, timeout=8)
        if r.status_code == 200:
            val = r.json().get("mid", 0)
            return float(val) if val else 0.0
    except: pass
    return 0.0

# ─────────────────────────────────────────────
# POLY WINRATE
# ─────────────────────────────────────────────
def poly_wr_add(uid, bet_id, direction, amount, bal_before, strength, session_name):
    record = {
        "bet_id": bet_id, "uid": uid, "direction": direction,
        "amount": amount, "bal_before": bal_before, "bal_after": None,
        "profit": None, "result": None, "strength": strength,
        "session": session_name,
        "placed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "resolved_at": None,
    }
    try:
        data = []
        if os.path.exists(POLY_WR):
            with open(POLY_WR) as f: data = json.load(f)
        data.append(record); data = data[-1000:]
        with open(POLY_WR, "w") as f: json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e: print("[WR add] %s" % e)

def poly_wr_resolve(uid, new_bal):
    try:
        if not os.path.exists(POLY_WR): return None
        with open(POLY_WR) as f: data = json.load(f)
        for rec in reversed(data):
            if rec.get("uid") == uid and rec.get("bal_after") is None:
                bal_before         = rec.get("bal_before", new_bal)
                profit             = round(new_bal - bal_before, 2)
                rec["bal_after"]   = new_bal
                rec["profit"]      = profit
                rec["result"]      = "WIN" if profit > 0 else "LOSS"
                rec["resolved_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                with open(POLY_WR, "w") as f: json.dump(data, f, ensure_ascii=False, indent=2)
                print("[WR resolve] bet=%s profit=%+.2f result=%s" % (
                    rec.get("bet_id", "?")[:8], profit, rec["result"]))
                return rec
        return None
    except Exception as e:
        print("[WR resolve] %s" % e); return None

def poly_winrate_msg(uid):
    if not os.path.exists(POLY_WR): return "Polymarket\n\nДаних поки немає."
    try:
        with open(POLY_WR) as f: data = json.load(f)
        my       = [x for x in data if x.get("uid") == uid]
        resolved = [x for x in my if x.get("bal_after") is not None]
        pending  = [x for x in my if x.get("bal_after") is None]
        if not resolved:
            return "Polymarket\n\nВ очікуванні: %d ставок" % len(pending)
        total    = len(resolved)
        wins     = [x for x in resolved if x.get("result") == "WIN"]
        losses   = [x for x in resolved if x.get("result") == "LOSS"]
        total_in = round(sum(x.get("amount", 0) for x in resolved), 2)
        total_pl = round(sum(x.get("profit", 0) for x in resolved), 2)
        wr       = round(len(wins) / total * 100, 1) if total else 0
        roi      = round(total_pl / total_in * 100, 1) if total_in > 0 else 0
        avg_win  = round(sum(x["profit"] for x in wins) / len(wins), 2) if wins else 0
        avg_loss = round(sum(x["profit"] for x in losses) / len(losses), 2) if losses else 0
        streak = 0; streak_type = ""
        for x in reversed(resolved):
            r = x.get("result", "")
            if streak == 0: streak_type = r
            if r == streak_type: streak += 1
            else: break
        last_bal = resolved[-1].get("bal_after", 0)
        lines = [
            "Polymarket  P&L", "",
            "Угоди        %d   WIN %d   LOSS %d" % (total, len(wins), len(losses)),
            "Вінрейт      %.1f%%" % wr,
            "Серія        %d %s поспіль" % (streak, streak_type), "",
            "Вкладено     $%.2f" % total_in,
            "P&L          %s$%.2f" % ("+" if total_pl >= 0 else "", total_pl),
            "ROI          %s%.1f%%" % ("+" if roi >= 0 else "", roi),
            "Сер. WIN     +$%.2f" % avg_win,
            "Сер. LOSS    $%.2f" % avg_loss,
            "Баланс зараз $%.2f" % last_bal, "",
            "Журнал ставок", "",
        ]
        for x in reversed(resolved[-20:]):
            p    = x.get("profit", 0)
            sign = "+" if p >= 0 else ""
            t    = (x.get("resolved_at") or x.get("placed_at") or "")[:16].replace("T", " ")
            lines.append("%s  %s  %s  $%.2f  %s$%.2f  %.2f→%.2f  %s" % (
                t, x.get("direction",""), x.get("result",""),
                x.get("amount",0), sign, abs(p),
                x.get("bal_before",0) or 0, x.get("bal_after",0) or 0,
                x.get("strength","")[:1]))
        if pending: lines += ["", "В очікуванні: %d ставка" % len(pending)]
        return "\n".join(lines)
    except Exception as e: return "Помилка: %s" % e

# ─────────────────────────────────────────────
# ІНВЕРСІЯ — уточнена на основі аналізу даних
# ASK_HEAVY = 35% вінрейт → інвертуємо
# DISTRIBUTION = 25% вінрейт → інвертуємо
# AMD CONTRA — не інвертуємо (тільки 3 кейси, не репрезентативно)
# ─────────────────────────────────────────────
def maybe_invert(dec, ob_bias, amd_phase, score):
    global _rules
    reasons = []
    ob_contra = ob_bias in _rules.get("invert_conditions", ["ASK_HEAVY"])
    amd_bad   = amd_phase in _rules.get("skip_amd_phases", ["DISTRIBUTION"])
    if ob_contra:  reasons.append("OB %s (low WR)" % ob_bias)
    if amd_bad:    reasons.append("AMD %s (low WR)" % amd_phase)
    if not reasons: return dec, False, ""
    new_dec = "DOWN" if dec == "UP" else "UP"
    return new_dec, True, " + ".join(reasons)

def should_skip_signal(amd_phase, score, strength):
    """Перевіряє чи треба взагалі пропустити сигнал."""
    global _rules
    min_sc = _rules.get("skip_if_score_below", 2)
    if score < min_sc: return True, "score %d < мін %d" % (score, min_sc)
    return False, ""

# ─────────────────────────────────────────────
# POLY STATS
# ─────────────────────────────────────────────
def poly_stats_update(bet_id, update):
    try:
        stats = {}
        if os.path.exists(POLY_STATS):
            with open(POLY_STATS) as f: stats = json.load(f)
        if bet_id not in stats: stats[bet_id] = {}
        stats[bet_id].update(update)
        with open(POLY_STATS, "w") as f: json.dump(stats, f, ensure_ascii=False, indent=2)
    except Exception as e: print("[PolyStats] %s" % e)

def poly_stats_get_all():
    try:
        if os.path.exists(POLY_STATS):
            with open(POLY_STATS) as f: return list(json.load(f).values())
    except: pass
    return []

# ─────────────────────────────────────────────
# CSV + JSON СТАТИСТИКА
# ─────────────────────────────────────────────
def build_stats_files(s):
    records = []
    for sig in s.signals: records.append(dict(sig))
    for p in poly_stats_get_all():
        p["type"] = "POLY_TRADE"; records.append(p)
    json_bytes = io.BytesIO(json.dumps(records, ensure_ascii=False, indent=2).encode("utf-8"))
    CSV_COLS = [
        "ts","decision","strength","confidence_score","outcome",
        "entry_price","exit_price","real_move","key_signal","logic","reasons",
        "session","market_condition","regime",
        "amd_phase","amd_direction","amd_reason","amd_conf",
        "sweep15m_type","sweep15m_level","sweep15m_ago",
        "sweep5m_type","sweep5m_level","sweep5m_ago",
        "bos5m","fvg_up","fvg_dn","trap_type","trap_hint",
        "struct_15m","struct_5m","struct_1m","vol_class","vol_avg",
        "btc_chg15","btc_chg5","btc_mom3","btc_mic",
        "oi_change","funding_rate","funding_sent",
        "liq_long","liq_short","liq_signal",
        "ob_bias","ob_imb","lsr_ratio","lsr_bias","crowd_long_pct",
        "consecutive_same","risk_note","inverted","invert_reason",
    ]
    csv_buf = io.StringIO()
    writer  = csv.DictWriter(csv_buf, fieldnames=CSV_COLS, extrasaction="ignore")
    writer.writeheader()
    for sig in s.signals:
        row = {
            "ts": sig.get("time",""), "decision": sig.get("dec",""),
            "strength": sig.get("strength",""), "confidence_score": sig.get("confidence_score",0),
            "outcome": sig.get("outcome","PENDING"), "entry_price": sig.get("entry",0),
            "exit_price": sig.get("exit",""), "real_move": sig.get("move",""),
            "key_signal": sig.get("key_signal",""), "logic": sig.get("logic",""),
            "reasons": " | ".join(sig.get("reasons",[])),
            "session": sig.get("session",""), "market_condition": sig.get("mkt_cond",""),
            "regime": sig.get("regime",""),
            "amd_phase": sig.get("amd_phase",""), "amd_direction": sig.get("amd_dir",""),
            "amd_reason": sig.get("amd_reason",""), "amd_conf": sig.get("amd_conf",0),
            "sweep15m_type": sig.get("sw15_type","NONE"), "sweep15m_level": sig.get("sw15_level",0),
            "sweep15m_ago": sig.get("sw15_ago",0),
            "sweep5m_type": sig.get("sw5_type","NONE"), "sweep5m_level": sig.get("sw5_level",0),
            "sweep5m_ago": sig.get("sw5_ago",0),
            "bos5m": sig.get("bos5m","NONE"), "fvg_up": sig.get("fvg_up",""),
            "fvg_dn": sig.get("fvg_dn",""), "trap_type": sig.get("trap","NONE"),
            "trap_hint": sig.get("trap_hint",""),
            "struct_15m": sig.get("st15m",""), "struct_5m": sig.get("st5m",""),
            "struct_1m": sig.get("st1m",""), "vol_class": sig.get("vol_class",""),
            "vol_avg": sig.get("vol_avg",0),
            "btc_chg15": sig.get("chg_cur",0), "btc_chg5": sig.get("mom5",0),
            "btc_mom3": sig.get("mom5",0), "btc_mic": sig.get("mic",0),
            "oi_change": sig.get("oic",0), "funding_rate": sig.get("fr",0),
            "funding_sent": sig.get("fs",""),
            "liq_long": sig.get("ll",0), "liq_short": sig.get("ls",0),
            "liq_signal": sig.get("lsig",""),
            "ob_bias": sig.get("ob_bias",""), "ob_imb": sig.get("ob_imb",0),
            "lsr_ratio": sig.get("lsr_ratio",0), "lsr_bias": sig.get("lsr_bias",""),
            "crowd_long_pct": sig.get("cl",0),
            "consecutive_same": sig.get("consec",0), "risk_note": sig.get("risk_note",""),
            "inverted": sig.get("inverted",False), "invert_reason": sig.get("invert_reason",""),
        }
        writer.writerow(row)
    csv_bytes = io.BytesIO(csv_buf.getvalue().encode("utf-8"))
    return json_bytes, csv_bytes

# ─────────────────────────────────────────────
# ПОШУК МАРКЕТУ
# ─────────────────────────────────────────────
def find_market():
    SLUG = "btc-updown-15m-"; ROUND = 900

    def end_ts(m):
        for f in ("end_date_iso","endDate","endDateIso","end_time","endTime","end_date"):
            v = m.get(f)
            if not v: continue
            try: return float(datetime.datetime.fromisoformat(str(v).replace("Z","+00:00")).timestamp())
            except:
                try: return float(v)
                except: pass
        return None

    def get_tokens(cid):
        try:
            r = requests.get("https://clob.polymarket.com/markets/%s" % cid, timeout=15)
            if r.status_code != 200: return None,None,0.5,0.5
            toks = r.json().get("tokens",[])
            yi = ni = ""; yp = np_ = 0.5
            for t in toks:
                oc  = (t.get("outcome") or "").upper()
                tid = (t.get("token_id") or t.get("tokenId") or "").strip()
                pr  = float(t.get("price",0.5) or 0.5)
                if oc in ("YES","UP","HIGHER","ABOVE"): yi=tid; yp=pr
                elif oc in ("NO","DOWN","LOWER","BELOW"): ni=tid; np_=pr
            if not yi and toks:
                yi = (toks[0].get("token_id") or toks[0].get("tokenId") or "").strip()
                yp = float(toks[0].get("price",0.5) or 0.5)
            if not ni and len(toks)>1:
                ni  = (toks[1].get("token_id") or toks[1].get("tokenId") or "").strip()
                np_ = float(toks[1].get("price",0.5) or 0.5)
            return yi,ni,yp,np_
        except Exception as e: print("[Market] %s"%e); return None,None,0.5,0.5

    def try_slug(slug, now):
        try:
            r = requests.get("https://gamma-api.polymarket.com/events",
                             params={"slug":slug}, timeout=15)
            if r.status_code != 200: return None
            raw = r.json()
            evs = raw if isinstance(raw,list) else ([raw] if isinstance(raw,dict) and raw else [])
            if not evs: return None
            ev = evs[0]; title = ev.get("title","") or slug
            for m in ev.get("markets",[]):
                if m.get("closed",True): continue
                cid = (m.get("conditionId") or m.get("condition_id") or m.get("id") or "").strip()
                if not cid: continue
                et = end_ts(m); diff = (et-now) if et else 900.0
                if diff <= 0: continue
                yi,ni,yp,np_ = get_tokens(cid)
                if not yi or not ni: continue
                q = m.get("question","") or title
                return {"yes_id":yi,"no_id":ni,"yes_p":yp,"no_p":np_,
                        "q":q,"cid":cid,"diff":round(diff,1),"end_ts":et or (now+900)}
        except Exception as e: print("[Market] %s"%e)
        return None

    now = time.time(); cur = int(now//ROUND)*ROUND
    for attempt in range(1,8):
        for ts in [cur,cur+ROUND,cur-ROUND]:
            r = try_slug("%s%d"%(SLUG,ts), now)
            if r: return r
        if attempt < 7: time.sleep(3)
    return None

# ─────────────────────────────────────────────
# СТАВКА
# ─────────────────────────────────────────────
def place_bet(s, direction, amount):
    if not s.ok:   return {"ok":False,"err":"Гаманець не підключено"}
    if amount < 1: return {"ok":False,"err":"Мінімум $1"}
    mkt = find_market()
    if not mkt: return {"ok":False,"err":"Активний маркет не знайдено"}
    token_id   = mkt["yes_id"] if direction=="UP" else mkt["no_id"]
    price      = mkt["yes_p"]  if direction=="UP" else mkt["no_p"]
    price      = max(0.01, min(0.99, float(price)))
    market_end = mkt.get("end_ts", time.time()+900)
    try:
        r = requests.get("https://clob.polymarket.com/midpoints",
                         params={"token_id":token_id}, timeout=10)
        if r.status_code == 200:
            mid = float(r.json().get("mid",price))
            if 0.01 <= mid <= 0.99: price = mid
    except: pass
    size = round(amount/price, 2)
    last_err = ""
    for attempt in range(1,4):
        try:
            from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions, Side
            client = get_client(s)
            resp   = client.create_and_post_order(
                order_args=OrderArgs(token_id=token_id, price=price, size=size, side=Side.BUY),
                options=PartialCreateOrderOptions(tick_size="0.01"),
                order_type=OrderType.GTC,
            )
            return {"ok":True,"resp":resp,"price":price,"pot":round(size-amount,2),
                    "mkt":mkt["q"][:60],"token_id":token_id,"size":size,"market_end":market_end}
        except Exception as e:
            last_err = str(e)
            if "version_mismatch" in last_err.lower() or "500" in last_err or "execution" in last_err.lower():
                s.reset_client()
                if attempt < 3: time.sleep(3); continue
            if any(x in last_err.lower() for x in ["unauthorized","401","forbidden","invalid api key"]):
                s.reset_client(); break
            if "not enough" in last_err.lower() or "allowance" in last_err.lower():
                return {"ok":False,"err":"Недостатньо USDC на Polymarket"}
            break
    return {"ok":False,"err":last_err}

# ─────────────────────────────────────────────
# FORCE SELL
# ─────────────────────────────────────────────
def force_sell(s, token_id, size, mode="normal"):
    cur_price = get_token_price(token_id)
    if cur_price == 0.0 and mode != "panic":
        return {"ok":False,"err":"Ціна токена = 0. Маркет закрився."}
    real_bal   = get_token_balance(s, token_id)
    sell_size  = real_bal if real_bal > 0 else size
    if mode == "panic":        sell_price = 0.01
    elif mode == "aggressive": sell_price = max(0.01, round(cur_price-0.05,4)) if cur_price>0 else 0.01
    else:                      sell_price = max(0.01, round(cur_price-0.02,4)) if cur_price>0 else 0.02
    last_err = ""
    for attempt in range(1,4):
        try:
            from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions, Side
            client = get_client(s)
            resp   = client.create_and_post_order(
                order_args=OrderArgs(token_id=token_id, price=sell_price, size=sell_size, side=Side.SELL),
                options=PartialCreateOrderOptions(tick_size="0.01"),
                order_type=OrderType.GTC,
            )
            return {"ok":True,"cur_price":cur_price,"sell_price":sell_price,"size_sold":sell_size,"resp":resp}
        except Exception as e:
            last_err = str(e)
            if "not enough balance" in last_err.lower():
                m = re.search(r"balance:\s*(\d+)", last_err)
                if m: sell_size = round(int(m.group(1))/1e6, 4)
                if attempt < 3: continue
                return {"ok":False,"err":"Недостатньо токенів: %s" % last_err}
            if "version_mismatch" in last_err.lower() or "500" in last_err or "execution" in last_err.lower():
                s.reset_client()
                if attempt < 3: time.sleep(2); sell_price = max(0.01, sell_price-0.03); continue
            if any(x in last_err.lower() for x in ["unauthorized","401","forbidden"]): s.reset_client(); break
            break
    return {"ok":False,"err":last_err}

# ─────────────────────────────────────────────
# SMART EXIT — кожні 3 секунди з 8-ї хвилини
#
# Логіка виходу:
#  Фаза 1 (0-8хв):   тримаємо без умов
#  Фаза 2 (8-13хв):  стежимо
#    токен >= 0.80 → фіксуємо прибуток
#    BTC пішов проти на 0.15%+ → стоп-лос
#  Фаза 3 (13-14.5хв): якщо в плюсі → виходимо
#  Остання хвилина: примусовий вихід
# ─────────────────────────────────────────────
def btc_move_against(bet, current_btc):
    direction  = bet.get("direction","")
    btc_entry  = bet.get("btc_at_entry", 0)
    if not btc_entry: return False
    move = (current_btc - btc_entry) / btc_entry * 100
    if direction == "UP"   and move < -0.15: return True
    if direction == "DOWN" and move >  0.15: return True
    return False

async def smart_exit_watcher(app):
    while True:
        await asyncio.sleep(3)
        for uid, s in list(_sessions.items()):
            if not s.ok or not s.open_bets: continue
            now = time.time(); still_open = []
            for bet in s.open_bets:
                try:
                    placed_at  = bet.get("placed_at", now)
                    market_end = bet.get("market_end", now+900)
                    age        = now - placed_at
                    time_left  = market_end - now
                    token_id   = bet.get("token_id","")
                    amount     = bet.get("amount",0)
                    entry_tok  = bet.get("entry_price",0.5)
                    direction  = bet.get("direction","")
                    mkt        = bet.get("mkt","")
                    bet_id     = bet.get("bet_id","")

                    # Перші 8 хвилин — не чіпаємо
                    if age < 480:
                        still_open.append(bet); continue

                    # Маркет закрився
                    if time_left <= 0:
                        await asyncio.sleep(40)
                        bal_after, _ = get_balance(s); bal_after = bal_after or 0
                        profit = round(bal_after - bet.get("bal_before", bal_after), 2)
                        result = "WIN" if profit > 0 else "LOSS"
                        poly_stats_update(bet_id, {
                            "status":"CLOSED_MARKET_END","profit":profit,"result":result,
                            "closed_at":datetime.datetime.now(datetime.timezone.utc).isoformat()
                        })
                        await app.bot.send_message(chat_id=uid, text=(
                            "Маркет закрито\n%s %s\n%s\n\nСтавка: $%.2f\nP&L: %s$%.2f\nРезультат: %s"
                        ) % (direction, mkt[:50], bet_id[:8], amount,
                             "+" if profit >= 0 else "", abs(profit), result))
                        continue

                    cur_tok = get_token_price(token_id)
                    if cur_tok <= 0: still_open.append(bet); continue

                    size      = bet.get("size",0)
                    gross_now = cur_tok * size
                    profit_now = round(gross_now - amount, 2)

                    should_sell = False; sell_mode = "normal"; sell_reason = ""

                    # Токен злетів до 0.80+ → фіксуємо
                    if cur_tok >= 0.80:
                        should_sell = True
                        sell_reason = "Фіксація прибутку (токен %.3f)" % cur_tok

                    # Остання хвилина + в плюсі → виходимо
                    elif time_left <= 60 and profit_now > 0:
                        should_sell = True
                        sell_reason = "Вихід з прибутком (%.0f сек)" % time_left

                    # Останні 30 сек
                    elif time_left <= 30:
                        should_sell = True; sell_mode = "aggressive"
                        sell_reason = "Примусовий вихід (%.0f сек)" % time_left

                    # Останні 15 сек
                    elif time_left <= 15:
                        should_sell = True; sell_mode = "panic"
                        sell_reason = "Panic exit (%.0f сек)" % time_left

                    # BTC пішов проти (перевіряємо з 8-ї хв)
                    elif age >= 480:
                        cur_btc = price_now()
                        if cur_btc and btc_move_against(bet, cur_btc):
                            should_sell = True
                            sell_reason = "Стоп-лос: BTC рухнув проти позиції"

                    if not should_sell: still_open.append(bet); continue

                    res = force_sell(s, token_id, size, mode=sell_mode)
                    if res["ok"]:
                        sp     = res.get("sell_price", cur_tok)
                        ss     = res.get("size_sold", size)
                        gross  = round(sp * ss, 4)
                        profit = round(gross - amount, 2)
                        pct    = round(profit / amount * 100, 1) if amount > 0 else 0
                        result = "WIN" if profit > 0 else "LOSS"
                        poly_stats_update(bet_id, {
                            "status":"CLOSED_SMART_EXIT","sell_price":sp,
                            "profit":profit,"result":result,"close_reason":sell_reason,
                            "closed_at":datetime.datetime.now(datetime.timezone.utc).isoformat()
                        })
                        await app.bot.send_message(chat_id=uid, text=(
                            "Позиція закрита\n%s %s\n%s\n\n"
                            "Причина: %s\nВхід: %.4f  Продаж: %.4f\n"
                            "Ставка: $%.2f  Повернулось: $%.4f\n"
                            "P&L: %s$%.2f (%s%.1f%%)\nРезультат: %s"
                        ) % (direction, mkt[:50], bet_id[:8], sell_reason,
                             entry_tok, sp, amount, gross,
                             "+" if profit >= 0 else "", abs(profit),
                             "+" if pct >= 0 else "", abs(pct), result))
                        log.info("[SmartExit] %s %s profit=%+.2f", bet_id[:8], direction, profit)
                    else:
                        if sell_mode == "panic":
                            poly_stats_update(bet_id, {"status":"PANIC_FAIL","error":res.get("err","")})
                            await app.bot.send_message(chat_id=uid, text=(
                                "Не вдалось закрити\n%s %s\nПеревір на polymarket.com\n%s"
                            ) % (direction, mkt[:40], res.get("err","")[:200]))
                        else: still_open.append(bet)
                except Exception as e:
                    log.error("[SmartExit] uid=%d: %s", uid, e)
                    still_open.append(bet)
            s.open_bets = still_open

async def check_open_bets(app, s):
    """Заглушка для сумісності."""
    pass

# ─────────────────────────────────────────────
# BINANCE
# ─────────────────────────────────────────────
def sget(url, p=None, t=15):
    try: return requests.get(url, params=p, timeout=t).json()
    except: return None

def candles(iv, lim):
    for _ in range(3):
        d = sget("https://fapi.binance.com/fapi/v1/klines", {"symbol":"BTCUSDT","interval":iv,"limit":lim})
        if d and isinstance(d,list): break
        time.sleep(2)
    if not d or not isinstance(d,list):
        d = sget("https://api.binance.com/api/v3/klines", {"symbol":"BTCUSDT","interval":iv,"limit":lim})
    if not d or not isinstance(d,list): return []
    return [{"t":int(k[0]),"o":float(k[1]),"h":float(k[2]),"l":float(k[3]),"c":float(k[4]),"v":float(k[5])} for k in d]

def price_now():
    for url,p in [("https://fapi.binance.com/fapi/v1/ticker/price",{"symbol":"BTCUSDT"}),
                  ("https://api.binance.com/api/v3/ticker/price",{"symbol":"BTCUSDT"})]:
        d = sget(url,p)
        if d and "price" in d: return float(d["price"])
    return None

def funding():
    d = sget("https://fapi.binance.com/fapi/v1/premiumIndex", {"symbol":"BTCUSDT"})
    if not d or not isinstance(d,dict): return {"rate":0.0,"sent":"NEUTRAL","mark":0.0,"basis":0.0}
    fr = float(d.get("lastFundingRate",0)); mk = float(d.get("markPrice",0)); ix = float(d.get("indexPrice",mk))
    return {"rate":fr,"sent":"LONGS_TRAPPED" if fr>0.0005 else "SHORTS_TRAPPED" if fr<-0.0003 else "NEUTRAL",
            "mark":mk,"basis":round(mk-ix,2)}

def liqs():
    d = sget("https://fapi.binance.com/fapi/v1/forceOrders", {"symbol":"BTCUSDT","limit":200})
    if not d or isinstance(d,dict): d = sget("https://fapi.binance.com/fapi/v1/allForceOrders", {"symbol":"BTCUSDT","limit":200})
    if not d or not isinstance(d,list): return {"ll":0.0,"ls":0.0,"sig":"NEUTRAL","exh":False}
    cut = int(time.time()*1000)-900000
    rec = [x for x in d if isinstance(x,dict) and int(x.get("time",0))>=cut] or d[:50]
    ll  = sum(float(x.get("origQty",0))*float(x.get("price",0)) for x in rec if x.get("side")=="SELL")
    ls  = sum(float(x.get("origQty",0))*float(x.get("price",0)) for x in rec if x.get("side")=="BUY")
    return {"ll":round(ll,2),"ls":round(ls,2),
            "sig":"SHORT_SQUEEZE" if ls>ll*2 else "LONG_CASCADE" if ll>ls*2 else "NEUTRAL",
            "exh":(ll+ls)>5e6}

def oi_data():
    d = sget("https://fapi.binance.com/fapi/v1/openInterest", {"symbol":"BTCUSDT"})
    if not d or not isinstance(d,dict): return 0.0,0.0
    cur = float(d.get("openInterest",0))
    try:
        prev = cur
        if os.path.exists(OI_CACHE):
            with open(OI_CACHE) as f: prev = json.load(f).get("oi",cur)
        with open(OI_CACHE,"w") as f: json.dump({"oi":cur},f)
        return cur, round((cur-prev)/prev*100,4) if prev>0 else 0.0
    except: return cur, 0.0

def orderbook():
    try:
        d = sget("https://fapi.binance.com/fapi/v1/depth", {"symbol":"BTCUSDT","limit":20})
        if not d or not isinstance(d,dict): return {"imb":0.0,"bias":"NEUTRAL"}
        b = sum(float(x[1]) for x in d.get("bids",[])[:10])
        a = sum(float(x[1]) for x in d.get("asks",[])[:10])
        t = b+a; imb = round((b-a)/t*100,2) if t>0 else 0.0
        return {"imb":imb,"bias":"BID_HEAVY" if imb>20 else "ASK_HEAVY" if imb<-20 else "BALANCED"}
    except: return {"imb":0.0,"bias":"NEUTRAL"}

def lsr():
    try:
        d = sget("https://fapi.binance.com/futures/data/topLongShortPositionRatio",
                 {"symbol":"BTCUSDT","period":"15m","limit":3})
        if not d or not isinstance(d,list): return {"ratio":1.0,"lp":50.0,"bias":"NEUTRAL"}
        lat = d[-1]; r = float(lat.get("longShortRatio",1.0)); lp = float(lat.get("longAccount",0.5))*100
        return {"ratio":round(r,3),"lp":round(lp,1),
                "bias":"CROWD_LONG" if r>1.5 else "CROWD_SHORT" if r<0.7 else "NEUTRAL"}
    except: return {"ratio":1.0,"lp":50.0,"bias":"NEUTRAL"}

# ─────────────────────────────────────────────
# SMC
# ─────────────────────────────────────────────
def swings(c):
    sh,sl=[],[]
    for i in range(2,len(c)-2):
        h=c[i]["h"]
        if h>c[i-1]["h"] and h>c[i+1]["h"] and h>c[i-2]["h"] and h>c[i+2]["h"]: sh.append({"p":h,"i":i})
        l=c[i]["l"]
        if l<c[i-1]["l"] and l<c[i+1]["l"] and l<c[i-2]["l"] and l<c[i+2]["l"]: sl.append({"p":l,"i":i})
    return sh[-5:],sl[-5:]

def structure(c):
    sh,sl=swings(c)
    if len(sh)<2 or len(sl)<2: return "RANGING"
    hs=[x["p"] for x in sh]; ls=[x["p"] for x in sl]
    if all(hs[i]>hs[i-1] for i in range(1,len(hs))) and all(ls[i]>ls[i-1] for i in range(1,len(ls))): return "BULLISH"
    if all(hs[i]<hs[i-1] for i in range(1,len(hs))) and all(ls[i]<ls[i-1] for i in range(1,len(ls))): return "BEARISH"
    return "RANGING"

def sweep(c):
    if len(c)<10: return {"type":"NONE","level":0.0,"ago":0}
    sh,sl=swings(c[:-3])
    for i,x in enumerate(reversed(c[-5:])):
        for sv in reversed(sh):
            if x["h"]>sv["p"] and x["c"]<sv["p"]: return {"type":"HIGH","level":sv["p"],"ago":i+1}
        for sv in reversed(sl):
            if x["l"]<sv["p"] and x["c"]>sv["p"]: return {"type":"LOW","level":sv["p"],"ago":i+1}
    return {"type":"NONE","level":0.0,"ago":0}

def detect_amd(c15,c5,price):
    if len(c15)<20: return {"phase":"NONE","dir":None,"conf":0,"reason":""}
    l10=c15[-10:]; l3=c15[-3:]; l35=c5[-3:] if len(c5)>=3 else []
    rng=(max(x["h"] for x in l10)-min(x["l"] for x in l10))/price*100
    ab=sum(abs(x["c"]-x["o"]) for x in l10)/len(l10)/price*100
    acc=rng<0.6 and ab<0.10
    sw15=sweep(c15); sw5=sweep(c5) if len(c5)>=10 else {"type":"NONE","level":0.0,"ago":0}
    sw=sw15 if sw15["type"]!="NONE" and sw15["ago"]<=4 else sw5
    manip=sw["type"]!="NONE" and sw["ago"]<=4
    mc=None
    if l35:
        mv=(l35[-1]["c"]-l35[0]["o"])/l35[0]["o"]*100
        mc="UP" if mv>0.05 else "DOWN" if mv<-0.05 else None
    lm=(l3[-1]["c"]-l3[0]["o"])/l3[0]["o"]*100
    if manip and acc:
        if sw["type"]=="LOW": return {"phase":"MANIPULATION_DONE","dir":"UP","conf":3 if mc=="UP" else 2,"reason":"ACCUM+SWEEP_LOW UP"}
        lc=c15[-1]["c"]
        if lc<sw["level"]*0.9998: return {"phase":"MANIPULATION_DONE","dir":"DOWN","conf":3 if mc=="DOWN" else 2,"reason":"SWEEP_HIGH+CLOSE_BELOW DOWN"}
        return {"phase":"MANIPULATION_DONE","dir":"UP","conf":1,"reason":"SWEEP_HIGH+HOLDS UP"}
    if acc: return {"phase":"ACCUMULATION","dir":None,"conf":1,"reason":"tight range"}
    if manip:
        lc=c15[-1]["c"]; d="DOWN" if sw["type"]=="HIGH" and lc<sw["level"]*0.9998 else "UP"
        return {"phase":"MANIPULATION","dir":d,"conf":2,"reason":"SWEEP_%s"%sw["type"]}
    if abs(lm)>0.15: return {"phase":"DISTRIBUTION","dir":"UP" if lm>0 else "DOWN","conf":1,"reason":"active move"}
    return {"phase":"NONE","dir":None,"conf":0,"reason":""}

def manip_detect(c,sw,price):
    r={"trap":"NONE","hint":None}
    if len(c)<5: return r
    last=c[-1]; body=abs(last["c"]-last["o"]); total=last["h"]-last["l"]
    if total>0:
        uw=last["h"]-max(last["c"],last["o"]); lw=min(last["c"],last["o"])-last["l"]; wr=1-(body/total)
        if wr>0.7 and total/last["c"]>0.002:
            if uw>lw*2: r.update({"trap":"WICK_TRAP_HIGH","hint":"DOWN"})
            elif lw>uw*2: r.update({"trap":"WICK_TRAP_LOW","hint":"UP"})
    if sw["type"]!="NONE" and sw["ago"]<=3:
        if sw["type"]=="HIGH" and last["c"]<sw["level"]*0.9995: r.update({"trap":"SWEEP_TRAP_HIGH","hint":"DOWN"})
        elif sw["type"]=="LOW" and last["c"]>sw["level"]*1.0005: r.update({"trap":"SWEEP_TRAP_LOW","hint":"UP"})
    return r

def vol_class(c15):
    if len(c15)<10: return "UNKNOWN",0.0
    rng=[(x["h"]-x["l"])/x["c"]*100 for x in c15[-10:]]
    avg=sum(rng)/len(rng); rec=sum(rng[-3:])/3; pri=sum(rng[:7])/7
    return ("LOW_VOL" if avg<0.08 else "EXPANSION" if rec>pri*1.5 else "HIGH_VOL" if avg>0.3 else "NORMAL"),round(avg,4)

def session():
    h=datetime.datetime.now(datetime.timezone.utc).hour
    if 7<=h<12:    return "LONDON",1
    elif 12<=h<17: return "NY_OPEN",0
    elif 17<=h<21: return "NY_PM",0
    elif 21<=h or h<3: return "ASIA",0
    return "DEAD",-1

def mkt_regime(c15):
    if len(c15)<10: return "RANGING"
    cl=[x["c"] for x in c15[-12:]]
    ups=sum(1 for i in range(1,len(cl)) if cl[i]>cl[i-1])
    if ups>=9 or (len(cl)-1-ups)>=9: return "TRENDING"
    alts=sum(1 for i in range(1,len(cl)-1) if (cl[i]>cl[i-1])!=(cl[i+1]>cl[i]))
    return "CHOPPY" if alts>=8 else "RANGING"

def fvg(c,price):
    fa=fb=None
    for i in range(1,len(c)-1):
        pv,nx=c[i-1],c[i+1]
        if nx["l"]>pv["h"]:
            mid=(nx["l"]+pv["h"])/2; dist=round((price-mid)/price*100,4)
            if mid<price and (fb is None or dist<fb["dist"]): fb={"top":nx["l"],"bot":pv["h"],"dist":dist}
        if nx["h"]<pv["l"]:
            mid=(pv["l"]+nx["h"])/2; dist=round((mid-price)/price*100,4)
            if mid>price and (fa is None or dist<fa["dist"]): fa={"top":pv["l"],"bot":nx["h"],"dist":dist}
    return fa,fb

def bos(c,htf):
    if len(c)<5: return None
    sh,sl=swings(c[:-1])
    if not sh or not sl: return None
    cl=c[-1]["c"]
    if cl>sh[-1]["p"]: return {"type":"CHoCH" if htf=="BEARISH" else "BOS","dir":"UP","level":sh[-1]["p"]}
    if cl<sl[-1]["p"]: return {"type":"CHoCH" if htf=="BULLISH" else "BOS","dir":"DOWN","level":sl[-1]["p"]}
    return None

def stops(c,price):
    sh,sl=swings(c)
    above=[{"p":sv["p"]} for sv in sh if sv["p"]>price]
    below=[{"p":sv["p"]} for sv in sl if sv["p"]<price]
    sa=min(above,key=lambda x:x["p"]-price) if above else None
    sb=min(below,key=lambda x:price-x["p"]) if below else None
    return sa,sb

# ─────────────────────────────────────────────
# BUILD PAYLOAD
# ─────────────────────────────────────────────
def build_payload(s):
    c15=candles("15m",20); c5=candles("5m",24); c1=candles("1m",20)
    if not c15: return None
    now=time.time(); px=c15[-1]["c"]
    chg_cur  = round((px-c15[-1]["o"])/c15[-1]["o"]*100,4)
    chg_prev = round((c15[-1]["o"]-c15[-2]["c"])/c15[-2]["c"]*100,4) if len(c15)>=2 else 0.0
    mom5     = round((c5[-1]["c"]-c5[-4]["c"])/c5[-4]["c"]*100,4) if len(c5)>=4 else 0.0
    mic      = round((c1[-1]["c"]-c1[-4]["c"])/c1[-4]["c"]*100,4) if len(c1)>=4 else 0.0
    spd1     = round((c1[-1]["c"]-c1[-1]["o"])/c1[-1]["o"]*100,4) if c1 else 0.0
    st15=structure(c15) if len(c15)>=6 else "RANGING"
    st5 =structure(c5)  if len(c5)>=6  else "RANGING"
    st1 =structure(c1)  if len(c1)>=6  else "RANGING"
    reg =mkt_regime(c15)
    sw5 =sweep(c5)  if len(c5)>=10  else {"type":"NONE","level":0.0,"ago":0}
    sw1 =sweep(c1)  if len(c1)>=10  else {"type":"NONE","level":0.0,"ago":0}
    sw15=sweep(c15) if len(c15)>=10 else {"type":"NONE","level":0.0,"ago":0}
    bc5 =bos(c5,st15) if len(c5)>=5 else None
    bc1 =bos(c1,st5)  if len(c1)>=5 else None
    f5a,f5b=fvg(c5[-12:],px) if len(c5)>=5 else (None,None)
    mn =manip_detect(c5[-6:] if len(c5)>=6 else c1[-6:],sw5,px)
    ad =detect_amd(c15,c5,px)
    sa,sb=stops(c5,px) if len(c5)>=6 else (None,None)
    da=round((sa["p"]-px)/px*100,4) if sa else 999.0
    db=round((px-sb["p"])/px*100,4) if sb else 999.0
    fn=funding(); lq=liqs(); ob=orderbook(); ls=lsr(); oi,oic=oi_data()
    vc,vs=vol_class(c15)
    sess_name,_=session()
    now_dt=datetime.datetime.now(datetime.timezone.utc)
    window_elapsed=(now_dt.minute%15)*60+now_dt.second
    window_left=900-window_elapsed
    strike_approx=c15[-1]["o"]
    vs_strike=round((px-strike_approx)/strike_approx*100,4)
    consec_count,consec_dir=s.consecutive_same()
    return {
        "ts":      now_dt.strftime("%Y-%m-%d %H:%M UTC"),
        "ts_unix": int(now),
        "window":  {"elapsed":round(window_elapsed),"left":round(window_left),
                    "strike":round(strike_approx,2),"vs_strike":vs_strike},
        "price":   {"cur":px,"chg_cur":chg_cur,"chg_prev":chg_prev,
                    "mom5":mom5,"mic":mic,"spd1":spd1,"mark":fn["mark"],"basis":fn["basis"]},
        "struct":  {"15m":st15,"5m":st5,"1m":st1,"reg":reg},
        "liq":     {"sw15":sw15,"sw5":sw5,"sw1":sw1,"sa":sa,"sb":sb,"da":da,"db":db,
                    "bos5":bc5,"bos1":bc1,"f5a":f5a,"f5b":f5b},
        "amd":     ad, "manip": mn,
        "pos":     {"fr":fn["rate"],"fs":fn["sent"],"ll":lq["ll"],"ls":lq["ls"],
                    "lsig":lq["sig"],"exh":lq["exh"],"oic":oic,"ob":ob["bias"],"obi":ob["imb"],
                    "lsr":ls["bias"],"lsrr":ls["ratio"],"cl":ls["lp"]},
        "ctx":     {"vol":vc,"vs":vs,"sess":sess_name,"reg":reg,
                    "consec_count":consec_count,"consec_dir":consec_dir or "NONE"},
    }

# ─────────────────────────────────────────────
# АДАПТИВНІ ПРОМПТИ — 4 режими ринку
#
# На основі аналізу даних:
# TRENDING=100%, MANIPULATION_DONE=68%, BALANCED_OB=67%
# DISTRIBUTION=25%, ASK_HEAVY=35%, DEAD=31%
# ─────────────────────────────────────────────
SYS_TRENDING = """You are a BTC 15-minute scalp trader on Polymarket.
CURRENT MARKET REGIME: TRENDING
In trending markets momentum is everything. Follow the trend aggressively.

=== PRIORITY FOR TRENDING ===
1. Momentum direction (mic, mom5, chg_cur) — is it accelerating?
2. BOS/CHoCH confirming trend continuation
3. Liquidations in trend direction (CASCADE=continue, SQUEEZE=caution)
4. Order book pressure in trend direction
5. AMD MANIPULATION_DONE — only if confirms trend

=== TRENDING RULES ===
- Trade WITH the trend unless 3+ signals say reversal
- consecutive_same is NOT a penalty in TRENDING — trend continues
- LOW_VOL in trending = accumulation before next leg
- SHORT_SQUEEZE in downtrend = potential reversal, wait for confirmation

=== SCORING (trending-adjusted) ===
+3 = MANIPULATION_DONE conf>=2 in trend direction
+2 = Fresh sweep confirming trend continuation
+2 = BOS/CHoCH in trend direction
+2 = Liquidations in trend direction
+1 = Momentum confirms trend (mic > 0.06% in trend dir)
+1 = Order book confirms trend
-2 = Signal AGAINST the trend (high risk)
-1 = CHOPPY pattern within trend

=== STRENGTH ===
score >= 4 → HIGH (lower threshold in trending)
score 2-3 → MEDIUM
score 1 → LOW

OUTPUT JSON only:
{"decision":"UP or DOWN","strength":"HIGH or MEDIUM or LOW",
"confidence_score":<int>,"market_condition":"TRENDING",
"key_signal":"max 8 words","logic":"2-3 sentences Ukrainian",
"reasons":["r1","r2","r3"],"risk_note":"what invalidates or NONE"}"""

SYS_RANGING = """You are a BTC 15-minute scalp trader on Polymarket.
CURRENT MARKET REGIME: RANGING
In ranging markets look for reversals at extremes. Sweeps and AMD are king.

=== PRIORITY FOR RANGING ===
1. FRESH SWEEP (ago=1-2) — swept level + close back = reversal
2. AMD MANIPULATION_DONE — strongest setup in ranging
3. Order book at range extremes
4. 1m momentum for entry timing
5. BOS/CHoCH breaking range

=== RANGING RULES ===
- Buy LOW of range, sell HIGH of range
- MANIPULATION_DONE is the highest probability setup (68% WR historically)
- DISTRIBUTION phase = skip or invert (25% WR)
- consecutive_same >= 3 = range is about to reverse, trade opposite
- BALANCED order book (67% WR) = wait for imbalance to develop

=== SCORING ===
+3 = MANIPULATION_DONE conf>=2
+2 = Fresh sweep ago=1 with close confirming reversal
+1 = Sweep ago=2 confirming
+2 = BOS/CHoCH breaking range
+2 = Liquidations at range extreme (squeeze/cascade)
+1 = BID_HEAVY at range low → UP / ASK_HEAVY at range high → DOWN
+1 = mic confirms direction
-1 = DISTRIBUTION phase (historically low WR)
-1 = LOW_VOL + RANGING (no conviction)
-1 = consecutive_same >= 3 (but consider reversal signal)

=== STRENGTH ===
score >= 5 → HIGH
score 3-4 → MEDIUM
score 1-2 → LOW

OUTPUT JSON only:
{"decision":"UP or DOWN","strength":"HIGH or MEDIUM or LOW",
"confidence_score":<int>,"market_condition":"RANGING",
"key_signal":"max 8 words","logic":"2-3 sentences Ukrainian",
"reasons":["r1","r2","r3"],"risk_note":"what invalidates or NONE"}"""

SYS_CHOPPY = """You are a BTC 15-minute scalp trader on Polymarket.
CURRENT MARKET REGIME: CHOPPY
In choppy markets be very selective. Only trade with strong confluent signals.

=== PRIORITY FOR CHOPPY ===
1. Only trade if MANIPULATION_DONE conf>=2 (clearest signal in noise)
2. Liquidation signals (SHORT_SQUEEZE or LONG_CASCADE) — real momentum
3. Fresh sweep ago=1 with immediate close-back
4. Skip everything else

=== CHOPPY RULES ===
- Maximum strength: MEDIUM (never HIGH in choppy)
- Need 3+ confirming signals minimum
- If score < 3, give LOW and default to DOWN
- Consecutive same doesn't matter in choppy
- Volume EXPANSION = potentially breaking out of chop

=== SCORING ===
+3 = MANIPULATION_DONE conf>=2
+2 = Liquidation signal (SQUEEZE or CASCADE)
+2 = Fresh sweep ago=1
+1 = BOS/CHoCH confirming direction
+1 = Strong momentum (mic > 0.1%)
-1 = All other signals (noise in choppy)

=== STRENGTH (choppy-adjusted) ===
score >= 5 → MEDIUM (max in choppy)
score 3-4 → LOW
score < 3 → LOW, default DOWN

OUTPUT JSON only:
{"decision":"UP or DOWN","strength":"HIGH or MEDIUM or LOW",
"confidence_score":<int>,"market_condition":"CHOPPY",
"key_signal":"max 8 words","logic":"2-3 sentences Ukrainian",
"reasons":["r1","r2","r3"],"risk_note":"what invalidates or NONE"}"""

SYS_DEFAULT = """You are a BTC 15-minute scalp trader on Polymarket.
Your ONLY job: will BTC price be HIGHER or LOWER in the next 15 minutes?

=== PRIORITY ORDER (top = most important) ===
1. FRESH SWEEP (ago=1-2) — price swept a liquidity level and closed back = reversal imminent
2. LIQUIDATIONS right now — CASCADE = momentum continues, SQUEEZE = reversal
3. ORDER BOOK imbalance — where are the orders stacked right now
4. 1m + 5m momentum (mic, chg5m) — what is price doing THIS moment
5. BOS/CHoCH on 5m — structure confirmed
6. AMD MANIPULATION_DONE — distribution leg incoming
7. VOLUME regime — EXPANSION = real move, LOW_VOL = fake

=== WHAT TO IGNORE ===
- Weekly/daily trends — too slow for 15min
- Macro (Fed, ETF, geopolitics) — priced in over days not minutes
- Old sweeps ago>=3 — market already absorbed them
- Session (minor factor only, not primary reason)

=== SCORING (start 0) ===
+3 = MANIPULATION_DONE conf>=2 (AMD confirmed, strongest pattern)
+2 = Fresh sweep ago=1 with close confirming direction
+1 = Sweep ago=2 confirming direction
-1 = Sweep ago>=3 (stale, ignore as primary)
+2 = BOS or CHoCH on 5m in signal direction
+2 = SHORT_SQUEEZE liquidations → UP momentum
+2 = LONG_CASCADE liquidations → DOWN momentum
+1 = BID_HEAVY orderbook → UP / ASK_HEAVY → DOWN
+1 = mic > +0.06% confirming UP / mic < -0.06% confirming DOWN
+1 = EXPANSION volume with momentum
+1 = FVG within 0.15% in signal direction (price magnet)
-1 = LOW_VOL + RANGING (no conviction)
-1 = CHOPPY regime
-1 = consecutive_same >= 3 (same signal repeated, skepticism)
-1 = mic contradicts signal direction

=== STRENGTH ===
score >= 5 → HIGH
score 3-4 → MEDIUM
score 1-2 → LOW
score <= 0 → LOW (weak/no setup — still give answer)

=== RULES ===
1. FULLY INDEPENDENT analysis every time. No memory of previous signals.
2. LOW_VOL + RANGING + no fresh sweep/BOS → LOW strength max, default DOWN.
3. consecutive_same >= 3 → need score >= 4 for same direction, else give opposite.
4. CHOPPY → always LOW strength.
5. Need at least 2 confirming signals for MEDIUM. At least 3 for HIGH.
6. HIGH strength only if score >= 6 AND at least 3 independent confirming signals.
7. Negative basis (Mark < Spot) = bearish futures pressure. Factor into direction.
8. DEAD session (03:00-07:00 UTC): DO NOT TRADE.

OUTPUT JSON only — no markdown, nothing outside JSON:
{"decision":"UP or DOWN","strength":"HIGH or MEDIUM or LOW",
"confidence_score":<int>,"market_condition":"TRENDING or RANGING or CHOPPY",
"key_signal":"max 8 words — the single main trigger",
"logic":"2-3 sentences Ukrainian — what will happen and WHY in 15min",
"reasons":["r1","r2","r3"],"risk_note":"what invalidates this trade or NONE"}"""

def get_adaptive_prompt(regime):
    """Вибирає промпт залежно від поточного режиму ринку."""
    global _rules
    # Додаємо адаптивні правила до промпту
    adapt_note = "\n\n=== ADAPTIVE RULES (learned from your trading history) ===\n%s" % \
                 _rules.get("notes", "No adaptive rules yet.")
    if regime == "TRENDING":  return SYS_TRENDING + adapt_note
    if regime == "CHOPPY":    return SYS_CHOPPY   + adapt_note
    if regime == "RANGING":   return SYS_RANGING  + adapt_note
    return SYS_DEFAULT + adapt_note

# ─────────────────────────────────────────────
# АНАЛІЗ AI
# ─────────────────────────────────────────────
def analyze_with_ai(p, s):
    try:
        client = anthropic.Anthropic(api_key=OPENAI_API_KEY)
        liq=p["liq"]; pos=p["pos"]; pr=p["price"]
        st=p["struct"]; ctx=p["ctx"]; mn=p["manip"]; ad=p["amd"]
        sw5=liq.get("sw5",{}); sw1=liq.get("sw1",{}); sw15=liq.get("sw15",{})
        bc5=liq.get("bos5"); bc1=liq.get("bos1")
        f5a=liq.get("f5a"); f5b=liq.get("f5b")
        win=p["window"]

        # Вибираємо промпт під режим
        regime = ctx["reg"]
        sys_prompt = get_adaptive_prompt(regime)

        msg = (
            "=== WINDOW ===\n"
            "Time:%s Sess:%s WindowElapsed:%.0fs WindowLeft:%.0fs\n"
            "Strike:$%.2f CurrentPrice:$%.2f VsStrike:%+.4f%%\n"
            "\n=== MOMENTUM (most important) ===\n"
            "Micro1m:%+.4f%% Speed1m:%+.4f%% Mom5m:%+.4f%%\n"
            "CurCandle:%+.4f%% PrevCandle:%+.4f%%\n"
            "\n=== STRUCTURE ===\n"
            "15m:%s 5m:%s 1m:%s Regime:%s Vol:%s(%.4f)\n"
            "BOS_5m:%s BOS_1m:%s\n"
            "Sw5m:%s@%.2f(ago=%d) Sw1m:%s@%.2f(ago=%d) Sw15m:%s(ago=%d)\n"
            "FVGup:%s FVGdn:%s StopsAbove:%.3f%% StopsBelow:%.3f%%\n"
            "AMD:%s->%s conf=%d [%s] Trap:%s hint=%s\n"
            "\n=== ORDER FLOW ===\n"
            "Mark:$%.2f Basis:%+.2f\n"
            "Funding:%+.6f(%s) Book:%s(%+.1f%%) L/S:%.3f(%s) CrowdLong:%.1f%%\n"
            "LiqLong:$%.0f LiqShort:$%.0f LiqSig:%s Exhausted:%s OI_chg:%+.4f%%\n"
            "\n=== META ===\n"
            "ConsecSame:%d(%s)"
        ) % (
            p["ts"], ctx["sess"], win["elapsed"], win["left"],
            win["strike"], pr["cur"], win["vs_strike"],
            pr["mic"], pr["spd1"], pr["mom5"],
            pr["chg_cur"], pr["chg_prev"],
            st["15m"], st["5m"], st["1m"], ctx["reg"], ctx["vol"], ctx["vs"],
            ("%s %s@%.2f"%(bc5["type"],bc5["dir"],bc5["level"])) if bc5 else "none",
            ("%s %s@%.2f"%(bc1["type"],bc1["dir"],bc1["level"])) if bc1 else "none",
            sw5.get("type","N"),sw5.get("level",0),sw5.get("ago",0),
            sw1.get("type","N"),sw1.get("level",0),sw1.get("ago",0),
            sw15.get("type","N"),sw15.get("ago",0),
            ("%.3f%%"%f5a["dist"]) if f5a else "none",
            ("%.3f%%"%f5b["dist"]) if f5b else "none",
            liq.get("da",999),liq.get("db",999),
            ad.get("phase","NONE"),ad.get("dir","?"),ad.get("conf",0),ad.get("reason",""),
            mn["trap"],str(mn["hint"]),
            pr["mark"],pr["basis"],
            pos["fr"],pos["fs"],pos["ob"],pos["obi"],
            pos["lsrr"],pos["lsr"],pos["cl"],
            pos["ll"],pos["ls"],pos["lsig"],str(pos["exh"]),pos["oic"],
            ctx["consec_count"],ctx["consec_dir"]
        )

        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=sys_prompt,
            messages=[{"role":"user","content":msg}])
        raw = resp.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[-2] if "```" in raw else raw
            raw = raw.lstrip("json").strip()
        return json.loads(raw)
    except Exception as e:
        log.error("AI: %s", e); return None

# ─────────────────────────────────────────────
# САМОВДОСКОНАЛЕННЯ — кожні 30 сигналів
# Claude аналізує твій CSV і оновлює правила
# ─────────────────────────────────────────────
def build_stats_csv_text(signals):
    """Будує компактний текст статистики для аналізу."""
    done = [s for s in signals if s.get("outcome") in ("WIN","LOSS")]
    if len(done) < 15: return None
    def wr(lst):
        if not lst: return 0,0
        w = sum(1 for x in lst if x.get("outcome")=="WIN")
        return w,len(lst)
    lines = ["Signal statistics for adaptive learning (%d signals):" % len(done), ""]
    for field, values in [
        ("session", ["ASIA","LONDON","NY_OPEN","NY_PM","DEAD"]),
        ("amd_phase", ["MANIPULATION_DONE","MANIPULATION","ACCUMULATION","DISTRIBUTION","NONE"]),
        ("ob_bias", ["BID_HEAVY","ASK_HEAVY","BALANCED"]),
        ("market_condition", ["TRENDING","RANGING","CHOPPY"]),
        ("vol_class", ["LOW_VOL","NORMAL","HIGH_VOL","EXPANSION"]),
    ]:
        lines.append("%s:" % field)
        for v in values:
            sub = [r for r in done if r.get(field)==v or r.get("mkt_cond")==v]
            w,n = wr(sub)
            if n >= 3: lines.append("  %s: %d/%d = %d%%" % (v, w, n, round(w/n*100)))
    # Scores
    lines.append("\nconfidence_score:")
    for sc in range(0,7):
        sub = [r for r in done if str(r.get("confidence_score",""))==str(sc)]
        w,n = wr(sub)
        if n>=2: lines.append("  score=%d: %d/%d = %d%%" % (sc,w,n,round(w/n*100)))
    return "\n".join(lines)

async def run_self_improvement(s, app):
    """Запускає аналіз і оновлює правила."""
    global _rules
    log.info("[Adapt] Running self-improvement for uid=%d", s.uid)
    stats_text = build_stats_csv_text(s.signals)
    if not stats_text:
        log.info("[Adapt] Not enough data")
        return

    prompt = """You are analyzing trading statistics to improve a BTC Polymarket bot.
Based on the win rate data below, update the trading rules to maximize win rate.

%s

Current rules:
%s

Your task:
1. Identify patterns with WR < 45%% that should be SKIPPED or INVERTED
2. Identify patterns with WR > 60%% that should be BOOSTED
3. Set minimum score threshold to skip weak signals
4. Return updated rules as JSON

Rules format:
{
  "skip_amd_phases": ["list of AMD phases to invert direction for, WR < 45%%"],
  "invert_conditions": ["list of OB bias values to invert, WR < 45%%"],
  "skip_if_score_below": <int, minimum score to trade>,
  "boost_conditions": ["list of AMD phases with WR > 60%%, use lower entry threshold"],
  "min_score_high": <int, minimum score for HIGH strength>,
  "notes": "one sentence summary of what changed and why"
}

Return ONLY valid JSON, no markdown.""" % (stats_text, json.dumps(_rules, indent=2))

    try:
        client = anthropic.Anthropic(api_key=OPENAI_API_KEY)
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=500,
            messages=[{"role":"user","content":prompt}])
        raw = resp.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"): raw = raw[4:]
            raw = raw.strip()
        new_rules = json.loads(raw)
        _rules = new_rules
        save_rules(_rules)
        log.info("[Adapt] Rules updated: %s", _rules.get("notes",""))
        await app.bot.send_message(chat_id=s.uid, text=(
            "Адаптація правил торгівлі\n\n"
            "Проаналізував %d сигналів.\n%s"
        ) % (len([x for x in s.signals if x.get("outcome") in ("WIN","LOSS")]),
             _rules.get("notes","Правила оновлено.")))
    except Exception as e:
        log.error("[Adapt] Error: %s", e)

# ─────────────────────────────────────────────
# СТАТИСТИКА
# ─────────────────────────────────────────────
def check_outcomes(s):
    now=int(time.time()); changed=False
    for sig in s.signals:
        if sig.get("outcome"): continue
        if now-sig.get("ts_unix",0)>=900:
            cur=price_now()
            if cur:
                entry=sig.get("entry",0); dec=sig.get("dec","")
                outcome="WIN" if (dec=="UP" and cur>entry) or (dec=="DOWN" and cur<entry) else "LOSS"
                sig["outcome"]=outcome; sig["exit"]=cur; sig["move"]=round(cur-entry,2)
                changed=True
                if outcome=="LOSS": s.log_err(sig)
    if changed: s.save()

def stats_msg(s):
    if not s.signals: return "Даних поки немає."
    checked=[g for g in s.signals if g.get("outcome")]
    if not checked: return "Перевіряємо результати..."
    wins=[g for g in checked if g["outcome"]=="WIN"]
    total=len(checked); wr=round(len(wins)/total*100,1)
    bar="▓"*int(wr/5)+"░"*(20-int(wr/5))
    lines=["СТАТИСТИКА СИГНАЛІВ",
           "Всього: %d  WIN: %d  LOSS: %d"%(total,len(wins),total-len(wins)),
           "Вінрейт: %.1f%%  [%s]"%(wr,bar),"","По силі:"]
    for st in ("HIGH","MEDIUM","LOW"):
        sub=[g for g in checked if g.get("strength")==st]
        if sub:
            w=len([g for g in sub if g["outcome"]=="WIN"])
            lines.append("  %s: %d/%d (%.1f%%)"%(st,w,len(sub),round(w/len(sub)*100,1)))
    lines+=["","По сесіях:"]
    for sn in ("ASIA","LONDON","NY_OPEN","NY_PM","DEAD"):
        sub=[g for g in checked if g.get("session")==sn]
        if sub:
            w=len([g for g in sub if g["outcome"]=="WIN"])
            lines.append("  %s: %d/%d (%.1f%%)"%(sn,w,len(sub),round(w/len(sub)*100,1)))
    lines+=["","По режиму:"]
    for reg in ("TRENDING","RANGING","CHOPPY"):
        sub=[g for g in checked if g.get("mkt_cond")==reg]
        if sub:
            w=len([g for g in sub if g["outcome"]=="WIN"])
            lines.append("  %s: %d/%d (%.1f%%)"%(reg,w,len(sub),round(w/len(sub)*100,1)))
    lines+=["","По AMD фазі:"]
    for ph in ("MANIPULATION_DONE","MANIPULATION","ACCUMULATION","DISTRIBUTION"):
        sub=[g for g in checked if g.get("amd_phase")==ph]
        if sub:
            w=len([g for g in sub if g["outcome"]=="WIN"])
            lines.append("  %-20s %d/%d (%.1f%%)"%(ph,w,len(sub),round(w/len(sub)*100,1)))
    return "\n".join(lines)

# ─────────────────────────────────────────────
# КЛАВІАТУРА
# ─────────────────────────────────────────────
def is_asia():
    h=datetime.datetime.now(datetime.timezone.utc).hour
    return h>=21 or h<3

def is_dead():
    h=datetime.datetime.now(datetime.timezone.utc).hour
    return 3<=h<7

def kb(s):
    w="Гаманець: ОК" if s.ok else "Підключити гаманець"
    a="Авто: ВИМК" if s.auto else "Авто: УВІМК"
    asia="🟢 Азія" if is_asia() else ("🔴 DEAD" if is_dead() else "⚪ Не Азія")
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(w,  callback_data="wallet"),
         InlineKeyboardButton(a,  callback_data="auto_toggle")],
        [InlineKeyboardButton("Баланс",      callback_data="balance"),
         InlineKeyboardButton("Статистика",  callback_data="stats")],
        [InlineKeyboardButton("Вінрейт Полі",callback_data="poly_wr"),
         InlineKeyboardButton("Аналіз",      callback_data="analyze")],
        [InlineKeyboardButton("Маркет",      callback_data="market"),
         InlineKeyboardButton("Помилки",     callback_data="errors")],
        [InlineKeyboardButton("Дані ринку",  callback_data="rawdata"),
         InlineKeyboardButton("Правила",     callback_data="show_rules")],
        [InlineKeyboardButton(asia,          callback_data="asia_info")],
    ])

WELCOME = (
    "BTC Polymarket Bot v5 ADAPTIVE\n\n"
    "Сигнали кожні 15 хв — :00 :15 :30 :45 UTC\n"
    "Ставка — 10% від балансу (мін $1)\n"
    "Smart Exit — автоматична фіксація позицій\n"
    "Адаптація — правила оновлюються кожні 30 сигналів\n"
    "DEAD сесія (03:00-07:00 UTC) — бот мовчить\n\n"
    "Як підключити:\n"
    "1. «Підключити гаманець»\n"
    "   Крок 1 — Приватний ключ\n"
    "   polymarket.com → Profile → Export Private Key\n"
    "   Крок 2 — Адреса гаманця\n"
    "   polymarket.com → Deposit → скопіюй адресу"
)

# ─────────────────────────────────────────────
# КОМАНДИ
# ─────────────────────────────────────────────
async def cmd_start(u,c):
    s=sess(u.effective_user.id)
    await u.message.reply_text(WELCOME, reply_markup=kb(s))

async def cmd_stats(u,c):
    s=sess(u.effective_user.id); check_outcomes(s)
    await u.message.reply_text(stats_msg(s))
    json_b,csv_b=build_stats_files(s)
    try: await u.message.reply_document(document=json_b, filename="signals_%d.json"%s.uid, caption="JSON лог")
    except Exception as e: print("[Stats] json: %s"%e)
    try: await u.message.reply_document(document=csv_b, filename="btc_signals_%d.csv"%s.uid, caption="CSV сигнали")
    except Exception as e: print("[Stats] csv: %s"%e)

async def cmd_analyze(u,c):
    s=sess(u.effective_user.id)
    if is_dead():
        await u.message.reply_text("DEAD сесія (03:00-07:00 UTC). Бот не торгує в цей час."); return
    await u.message.reply_text("Аналізую...")
    await cycle(c.application, s)

async def cmd_autoon(u,c):
    s=sess(u.effective_user.id)
    if not s.ok: await u.message.reply_text("Спочатку підключи гаманець."); return
    s.auto=True; bal,_=get_balance(s); bet=s.bet_size(bal)
    await u.message.reply_text(
        "Авто-торгівля увімкнена\nБаланс: %s\nСтавка: $%.2f (10%%)"%(
            ("$%.2f"%bal) if bal else "перевіряємо...",bet), reply_markup=kb(s))

async def cmd_autooff(u,c):
    s=sess(u.effective_user.id); s.auto=False
    await u.message.reply_text("Авто-торгівля вимкнена.", reply_markup=kb(s))

# ─────────────────────────────────────────────
# CALLBACKS
# ─────────────────────────────────────────────
async def on_callback(u,c):
    s=sess(u.effective_user.id); q=u.callback_query; await q.answer()

    if q.data=="wallet":
        s.state="key"; s.tmp_key=None
        await q.message.reply_text(
            "Підключення\n\nКрок 1/2 — Приватний ключ\n"
            "polymarket.com → Profile → Export Private Key\n\nВведи ключ (64 hex):")

    elif q.data=="auto_toggle":
        if not s.ok: await q.message.reply_text("Спочатку підключи гаманець."); return
        s.auto=not s.auto
        if s.auto:
            s._asia_auto=False; bal,_=get_balance(s); bet=s.bet_size(bal)
            await q.message.reply_text(
                "Авто увімкнено\nБаланс: $%.2f\nСтавка: $%.2f (10%%)"%(bal or 0,bet),
                reply_markup=kb(s))
        else:
            s._asia_auto=False
            await q.message.reply_text("Авто вимкнено.", reply_markup=kb(s))

    elif q.data=="asia_info":
        now_k=datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(hours=3)
        status = "DEAD — бот мовчить" if is_dead() else ("Торгую" if is_asia() else "Не активна")
        await q.message.reply_text(
            "Азія сесія: 21:00–03:00 UTC (00:00–06:00 Київ)\n"
            "DEAD сесія: 03:00–07:00 UTC (заблокована)\n"
            "Зараз: %s Київ\nСтатус: %s" % (now_k.strftime("%H:%M"), status))

    elif q.data=="balance":
        bal,err=get_balance(s)
        open_val=round(sum(b.get("amount",0) for b in s.open_bets),2)
        if bal is not None and bal>0:
            msg="Баланс: $%.2f USDC\nСтавка: $%.2f (10%%)"%(bal,s.bet_size(bal))
            if open_val>0: msg+="\nВ позиціях: $%.2f"%open_val
            await q.message.reply_text(msg)
        else:
            await q.message.reply_text("Баланс: $0\n%s\n\nПоповни на polymarket.com → Deposit"%err)

    elif q.data=="stats":
        check_outcomes(s)
        await q.message.reply_text(stats_msg(s))
        json_b,csv_b=build_stats_files(s)
        try: await q.message.reply_document(document=json_b, filename="signals_%d.json"%s.uid, caption="JSON лог")
        except: pass
        try: await q.message.reply_document(document=csv_b, filename="btc_signals_%d.csv"%s.uid, caption="CSV сигнали")
        except: pass

    elif q.data=="poly_wr":
        await q.message.reply_text(poly_winrate_msg(s.uid))

    elif q.data=="analyze":
        if is_dead():
            await q.message.reply_text("DEAD сесія (03:00-07:00 UTC). Бот не торгує."); return
        await q.message.reply_text("Аналізую...")
        await cycle(c.application, s)

    elif q.data=="market":
        await q.message.reply_text("Шукаю...")
        m=find_market()
        if m:
            await q.message.reply_text(
                "Маркет знайдено\n\n%s\n\ncid:\n%s\nYES:\n%s\nNO:\n%s\n\n"
                "YES:%.4f NO:%.4f  Закривається: %.0f сек" % (
                    m["q"][:80],m["cid"],m["yes_id"],m["no_id"],m["yes_p"],m["no_p"],m["diff"]))
        else:
            await q.message.reply_text("Маркет не знайдено.")

    elif q.data=="errors":
        if not os.path.exists(s.err_f):
            await q.message.reply_text("Помилок немає."); return
        try:
            with open(s.err_f) as f: errs=json.load(f)
            if not errs: await q.message.reply_text("Помилок немає."); return
            lines=["Останні LOSS (%d):"%len(errs),""]
            for i,e in enumerate(errs[-5:],1):
                lines.append("%d. %s %s\n   %s\n"%(i,e.get("dec","?"),e.get("strength","?"),e.get("key_signal","")[:60]))
            await q.message.reply_text("\n".join(lines)[:4000])
        except: await q.message.reply_text("Помилка читання файлу.")

    elif q.data=="show_rules":
        global _rules
        lines = [
            "Поточні адаптивні правила",
            "",
            "Пропустити/інвертувати OB:  %s" % ", ".join(_rules.get("invert_conditions",[])),
            "Пропустити AMD фази:        %s" % ", ".join(_rules.get("skip_amd_phases",[])),
            "Мін. score для торгівлі:    %d" % _rules.get("skip_if_score_below",2),
            "Підсилені AMD фази:         %s" % ", ".join(_rules.get("boost_conditions",[])),
            "Мін. score для HIGH:        %d" % _rules.get("min_score_high",5),
            "",
            "Нотатки:",
            _rules.get("notes","—"),
        ]
        await q.message.reply_text("\n".join(lines))

    elif q.data=="rawdata":
        await q.message.reply_text("Збираю дані ринку...")
        try:
            p=build_payload(s)
            if not p: await q.message.reply_text("Помилка даних Binance."); return
            pr=p["price"]; st=p["struct"]; ctx=p["ctx"]
            liq=p["liq"]; pos=p["pos"]; ad=p["amd"]
            mn=p["manip"]; win=p["window"]
            sw5=liq.get("sw5",{}); sw1=liq.get("sw1",{}); sw15=liq.get("sw15",{})
            bc5=liq.get("bos5"); bc1=liq.get("bos1")
            f5a=liq.get("f5a"); f5b=liq.get("f5b")
            sa=liq.get("sa"); sb=liq.get("sb")
            lines=[
                "ДАНІ РИНКУ  %s"%p["ts"],"",
                "ЦІНА",
                "  Поточна:     $%.2f"%pr["cur"],
                "  Mark price:  $%.2f"%pr["mark"],
                "  Basis:       %+.2f"%pr["basis"],
                "  Зміна свічки: %+.4f%%"%pr["chg_cur"],
                "  Попередня:   %+.4f%%"%pr["chg_prev"],
                "  Mom 5m:      %+.4f%%"%pr["mom5"],
                "  Мікро 1m:    %+.4f%%"%pr["mic"],
                "  Швидкість 1m: %+.4f%%"%pr["spd1"],"",
                "ВІКНО POLYMARKET",
                "  Пройшло:    %ds"%win["elapsed"],
                "  Залишилось: %ds"%win["left"],
                "  Strike:     $%.2f"%win["strike"],
                "  Vs strike:  %+.4f%%"%win["vs_strike"],"",
                "СТРУКТУРА",
                "  15m: %s   5m: %s   1m: %s"%(st["15m"],st["5m"],st["1m"]),
                "  Режим: %s"%ctx["reg"],
                "  Промпт: %s режим"%ctx["reg"],
                "  Волатильність: %s (%.4f)"%(ctx["vol"],ctx["vs"]),
                "  Сесія: %s"%ctx["sess"],"",
                "SWEEPS",
                "  5m:  %s @ %.2f  ago=%d"%(sw5.get("type","NONE"),sw5.get("level",0),sw5.get("ago",0)),
                "  1m:  %s @ %.2f  ago=%d"%(sw1.get("type","NONE"),sw1.get("level",0),sw1.get("ago",0)),
                "  15m: %s  ago=%d"%(sw15.get("type","NONE"),sw15.get("ago",0)),"",
                "BOS / CHoCH",
                "  5m: %s"%(("%s %s @ %.2f"%(bc5["type"],bc5["dir"],bc5["level"])) if bc5 else "немає"),
                "  1m: %s"%(("%s %s @ %.2f"%(bc1["type"],bc1["dir"],bc1["level"])) if bc1 else "немає"),"",
                "FVG",
                "  Вгору: %s"%(("dist=%.3f%%"%f5a["dist"]) if f5a else "немає"),
                "  Вниз:  %s"%(("dist=%.3f%%"%f5b["dist"]) if f5b else "немає"),"",
                "STOPS",
                "  Вище: %s"%(("$%.2f  (+%.3f%%)"%(sa["p"],liq.get("da",0))) if sa else "немає"),
                "  Нижче: %s"%(("$%.2f  (-%.3f%%)"%(sb["p"],liq.get("db",0))) if sb else "немає"),"",
                "AMD",
                "  Фаза:    %s"%ad.get("phase","NONE"),
                "  Напрям:  %s"%(ad.get("dir","?") or "—"),
                "  Conf:    %d"%ad.get("conf",0),
                "  Причина: %s"%ad.get("reason",""),"",
                "МАНІПУЛЯЦІЯ",
                "  Trap:  %s"%mn["trap"],
                "  Hint:  %s"%str(mn["hint"]),"",
                "ORDER FLOW",
                "  Funding: %+.6f  (%s)"%(pos["fr"],pos["fs"]),
                "  OB:      %s  imb=%+.1f%%"%(pos["ob"],pos["obi"]),
                "  L/S:     %.3f  (%s)"%(pos["lsrr"],pos["lsr"]),
                "  Crowd:   %.1f%%"%pos["cl"],
                "  OI chg:  %+.4f%%"%pos["oic"],"",
                "ЛІКВІДАЦІЇ",
                "  Long:    $%.0f"%pos["ll"],
                "  Short:   $%.0f"%pos["ls"],
                "  Сигнал:  %s"%pos["lsig"],"",
                "CONSECUTIVE SAME: %d (%s)"%(ctx["consec_count"],ctx["consec_dir"]),
                "",
                "АДАПТИВНІ ПРАВИЛА: %s"%_rules.get("notes","—"),
            ]
            text="\n".join(lines)
            for i in range(0,len(text),4000): await q.message.reply_text(text[i:i+4000])
        except Exception as e: await q.message.reply_text("Помилка: %s"%str(e)[:200])

    elif q.data=="skip":
        s.pending={}; await q.edit_message_text("Скасовано.")

    elif q.data.startswith("exec_"):
        parts=q.data.split("_"); direction=parts[1]; amount=float(parts[2])
        await q.edit_message_text("Розміщую $%.2f..."%amount)
        bet=place_bet(s,direction,amount)
        if bet["ok"]:
            await c.bot.send_message(chat_id=u.effective_chat.id,
                text="Ставка виконана\n%s | %s\n$%.2f — +$%.2f"%(
                    direction,bet.get("mkt","Polymarket"),amount,bet.get("pot",0)))
        else:
            await c.bot.send_message(chat_id=u.effective_chat.id,
                text="Ставка не виконана\n%s"%bet["err"])
        s.pending={}

# ─────────────────────────────────────────────
# ТЕКСТОВІ ПОВІДОМЛЕННЯ
# ─────────────────────────────────────────────
async def on_message(u,c):
    s=sess(u.effective_user.id); txt=u.message.text.strip()

    if s.state=="key":
        clean=txt.lower().replace("0x","").replace(" ","")
        if len(clean)!=64:
            await u.message.reply_text("Неправильна довжина (%d, потрібно 64).\nСпробуй ще раз:"%len(clean)); return
        s.tmp_key=txt; s.state="funder"
        await u.message.reply_text(
            "Ключ прийнято.\nКрок 2/2 — Адреса гаманця\n"
            "polymarket.com → Deposit → скопіюй адресу\n\nВведи (0x..., 42 символи):"); return

    if s.state=="funder":
        addr=txt.strip()
        if not addr.lower().startswith("0x") or len(addr)!=42:
            await u.message.reply_text("Неправильна адреса.\nСпробуй ще раз:"); return
        s.state=None; key=s.tmp_key or ""; s.tmp_key=None
        clean=key.lower().replace("0x","").replace(" ","")
        s.key="0x"+clean; s.funder=addr; s.ok=True; s._client=None
        try:
            from eth_account import Account
            s.address=Account.from_key(s.key).address
        except: s.address=s.key[:12]+"..."
        bal,err=get_balance(s); bet=s.bet_size(bal)
        bal_str=("$%.2f USDC"%bal) if (bal is not None and bal>0) else ("$0 (%s)"%err)
        await u.message.reply_text(
            "Гаманець підключено\n\nПідписувач: %s\nFunder: %s\n\nБаланс: %s\nСтавка: $%.2f (10%%)\n\nНатисни «Авто: УВІМК»"%(
                s.address,addr,bal_str,bet), reply_markup=kb(s)); return

    if s.pending and time.time()-s.pending.get("ts",0)<=600:
        try:
            amount=float(txt)
            if amount<1 or amount>500: await u.message.reply_text("Сума від $1 до $500"); return
            direction=s.pending["dir"]
            ikb=InlineKeyboardMarkup([[
                InlineKeyboardButton("Підтвердити $%.2f → %s"%(amount,direction),
                    callback_data="exec_%s_%.2f"%(direction,amount)),
                InlineKeyboardButton("Скасувати",callback_data="skip")]])
            await u.message.reply_text("Підтвердити?", reply_markup=ikb)
        except ValueError: pass

# ─────────────────────────────────────────────
# АВТО ТОРГІВЛЯ
# ─────────────────────────────────────────────
async def auto_trade(app, s, p, result):
    global _rules
    dec      = result.get("decision")
    strength = result.get("strength","LOW")
    logic    = result.get("logic","")
    score    = result.get("confidence_score",0)
    if not dec: return

    bal, err = get_balance(s)
    if bal is not None:
        resolved = poly_wr_resolve(s.uid, bal)
        if resolved:
            profit = resolved.get("profit",0)
            sign   = "+" if profit >= 0 else ""
            await app.bot.send_message(chat_id=s.uid, text=(
                "Результат попередньої ставки\n\n"
                "Напрямок:     %s\nСтавка:       $%.2f\n"
                "Баланс до:    $%.2f\nБаланс після: $%.2f\n"
                "P&L:          %s$%.2f\nРезультат:    %s"
            ) % (resolved.get("direction","?"), resolved.get("amount",0),
                 resolved.get("bal_before",0), bal, sign, abs(profit),
                 resolved.get("result","?")))

    if not bal or bal<=0:
        await app.bot.send_message(chat_id=s.uid,text="Баланс $0. Поповни на polymarket.com"); return
    amount=s.bet_size(bal)
    if amount<1:
        await app.bot.send_message(chat_id=s.uid,text="Ставка $%.2f < $1. Поповни баланс."%amount); return

    # Перевіряємо інверсію (уточнена версія)
    ob_bias   = p["pos"]["ob"]
    amd_phase = p["amd"].get("phase","")
    final_dec, inverted, invert_reason = maybe_invert(dec, ob_bias, amd_phase, score)

    bet=place_bet(s, final_dec, amount)
    if bet["ok"]:
        bet_id="%d_%d"%(s.uid,int(time.time()))
        open_bet={
            "bet_id":bet_id,"token_id":bet["token_id"],"direction":final_dec,
            "amount":amount,"size":bet["size"],"entry_price":bet["price"],
            "placed_at":time.time(),"market_end":bet.get("market_end",time.time()+900),
            "bal_before":bal,"mkt":bet.get("mkt",""),"strength":strength,"logic":logic,
            "score":score,"key_signal":result.get("key_signal",""),
            "amd_phase":p["amd"].get("phase",""),"session":p["ctx"]["sess"],
            "btc_at_entry":p["price"]["cur"],
            "risk_note":result.get("risk_note","NONE"),
        }
        s.open_bets.append(open_bet)
        poly_stats_update(bet_id,{"status":"OPEN","open_data":open_bet,
            "opened_at":datetime.datetime.now(datetime.timezone.utc).isoformat()})
        poly_wr_add(uid=s.uid, bet_id=bet_id, direction=final_dec,
                    amount=amount, bal_before=bal, strength=strength,
                    session_name=p["ctx"]["sess"])
        s.trades.append({"dec":final_dec,"amount":amount,"entry":p["price"]["cur"],
            "time":str(datetime.datetime.now(datetime.timezone.utc))})
        invert_note = "\nAI: %s → ІНВЕРТ: %s\nПричина: %s"%(dec,final_dec,invert_reason) if inverted else ""
        await app.bot.send_message(chat_id=s.uid,text=(
            "Ставка виконана  %s %s\n\n%s\n\n"
            "Баланс: $%.2f  Ставка: $%.2f (10%%)\nПотенційно: +$%.2f\n\n%s%s"
        )%("▲" if final_dec=="UP" else "▼",final_dec,bet.get("mkt","Polymarket"),
           bal,amount,bet.get("pot",0),logic,invert_note))
    else:
        await app.bot.send_message(chat_id=s.uid,text="Ставка не виконана\n%s"%bet["err"])

# ─────────────────────────────────────────────
# ЦИКЛ — з DEAD фільтром і самовдосконаленням
# ─────────────────────────────────────────────
_adapt_counter = {}  # uid → кількість сигналів з останньої адаптації

async def cycle(app, s):
    # DEAD сесія — повна тиша
    if is_dead():
        log.info("[Cycle] DEAD session — skipping uid=%d", s.uid)
        return

    check_outcomes(s)

    p=build_payload(s)
    if not p:
        await app.bot.send_message(chat_id=s.uid,text="Помилка даних Binance."); return

    result=analyze_with_ai(p, s)
    if not result:
        await app.bot.send_message(chat_id=s.uid,text="Помилка AI."); return

    dec      = result.get("decision","UP")
    strength = result.get("strength","LOW")
    logic    = result.get("logic","")
    score    = result.get("confidence_score",0)
    reasons  = result.get("reasons",[])
    key_sig  = result.get("key_signal","")
    mkt_cond = result.get("market_condition",p["ctx"]["reg"])
    risk_note= result.get("risk_note","NONE")
    ad=p["amd"]; mn=p["manip"]
    sw15=p["liq"].get("sw15",{}); sw5=p["liq"].get("sw5",{}); sw1=p["liq"].get("sw1",{})
    bc5=p["liq"].get("bos5"); bc1=p["liq"].get("bos1")
    f5a=p["liq"].get("f5a"); f5b=p["liq"].get("f5b")
    consec=p["ctx"]["consec_count"]

    # Інверсія
    ob_bias   = p["pos"]["ob"]
    amd_phase = ad.get("phase","")
    final_dec, inverted, invert_reason = maybe_invert(dec, ob_bias, amd_phase, score)

    # Зберігаємо сигнал
    sig={
        "dec":final_dec,"strength":strength,"confidence_score":score,"logic":logic,
        "reasons":reasons,"key_signal":key_sig,"risk_note":risk_note,
        "entry":p["price"]["cur"],"time":p["ts"],"ts_unix":p["ts_unix"],"outcome":None,
        "session":p["ctx"]["sess"],"mkt_cond":mkt_cond,"regime":p["ctx"]["reg"],
        "window_elapsed":p["window"]["elapsed"],"window_left":p["window"]["left"],
        "strike":p["window"]["strike"],"vs_strike":p["window"]["vs_strike"],
        "chg_cur":p["price"]["chg_cur"],"chg_prev":p["price"]["chg_prev"],
        "mom5":p["price"]["mom5"],"mic":p["price"]["mic"],"spd1":p["price"]["spd1"],
        "amd_phase":ad.get("phase","NONE"),"amd_dir":ad.get("dir",""),
        "amd_reason":ad.get("reason",""),"amd_conf":ad.get("conf",0),
        "sw15_type":sw15.get("type","NONE"),"sw15_ago":sw15.get("ago",0),
        "sw5_type":sw5.get("type","NONE"),"sw5_level":sw5.get("level",0),"sw5_ago":sw5.get("ago",0),
        "sw1_type":sw1.get("type","NONE"),"sw1_ago":sw1.get("ago",0),
        "bos5m":("%s_%s"%(bc5["type"],bc5["dir"])) if bc5 else "NONE",
        "bos1m":("%s_%s"%(bc1["type"],bc1["dir"])) if bc1 else "NONE",
        "fvg_up":("%.3f%%"%f5a["dist"]) if f5a else "",
        "fvg_dn":("%.3f%%"%f5b["dist"]) if f5b else "",
        "trap":mn["trap"],"trap_hint":str(mn["hint"]),
        "st15m":p["struct"]["15m"],"st5m":p["struct"]["5m"],"st1m":p["struct"]["1m"],
        "vol_class":p["ctx"]["vol"],"vol_avg":p["ctx"]["vs"],
        "fr":p["pos"]["fr"],"fs":p["pos"]["fs"],
        "ll":p["pos"]["ll"],"ls":p["pos"]["ls"],"lsig":p["pos"]["lsig"],
        "ob_bias":p["pos"]["ob"],"ob_imb":p["pos"]["obi"],
        "lsr_ratio":p["pos"]["lsrr"],"lsr_bias":p["pos"]["lsr"],"cl":p["pos"]["cl"],
        "oic":p["pos"]["oic"],"consec":consec,
        "inverted":inverted,"invert_reason":invert_reason,
    }
    s.signals.append(sig); s.save()

    try:
        dump=[]
        if os.path.exists(DUMP_FILE):
            with open(DUMP_FILE) as f: dump=json.load(f)
        dump.append(sig); dump=dump[-2000:]
        with open(DUMP_FILE,"w") as f: json.dump(dump,f,ensure_ascii=False,indent=2)
    except: pass

    # Самовдосконалення кожні 30 сигналів
    uid = s.uid
    _adapt_counter[uid] = _adapt_counter.get(uid, 0) + 1
    if _adapt_counter[uid] >= 30:
        _adapt_counter[uid] = 0
        asyncio.create_task(run_self_improvement(s, app))

    win      = p["window"]
    arrow    = "▲" if final_dec=="UP" else "▼"
    reas_s   = "\n".join("· "+r for r in reasons[:3]) if reasons else ""
    warn     = "\n⚠️ %d однакових підряд" % consec if consec >= 3 else ""
    win_info = "Вікно: %ds пройшло / %ds залишилось / vs_strike:%+.3f%%" % (
               win["elapsed"],win["left"],win["vs_strike"])
    regime_note = "\n📊 Режим: %s (промпт адаптовано)" % p["ctx"]["reg"]
    invert_note = "\n⚡ AI: %s → ІНВЕРТ: %s (%s)" % (dec,final_dec,invert_reason) if inverted else ""

    main_txt = (
        "СИГНАЛ  %s %s  |  %s  |  Score:%+d\n\n"
        "$%.2f  ·  %s  ·  %s%s%s%s\n"
        "%s\n\n"
        "%s\n\n%s\n\n%s\n\n"
        "Ризик: %s"
    ) % (arrow, final_dec, strength, score,
         p["price"]["cur"], mkt_cond, p["ctx"]["sess"], warn, regime_note, invert_note,
         win_info, key_sig, logic, reas_s, risk_note)

    print("[Cycle] uid=%d ai=%s final=%s inv=%s str=%s score=%d regime=%s sess=%s" % (
        s.uid, dec, final_dec, inverted, strength, score, p["ctx"]["reg"], p["ctx"]["sess"]))

    # Авто-Азія
    asia=is_asia()
    if asia and not s.auto and s.ok:
        s._asia_auto=True; s.auto=True
        await app.bot.send_message(chat_id=s.uid,
            text="Азія сесія — авто-торгівля увімкнена (00:00–06:00 Київ)")
    if not asia and s.auto and s._asia_auto:
        s._asia_auto=False; s.auto=False
        await app.bot.send_message(chat_id=s.uid,
            text="Азія завершилась — авто-торгівля вимкнена")

    await app.bot.send_message(chat_id=s.uid, text=main_txt)

    if s.auto:
        await auto_trade(app, s, p, result)
    else:
        s.pending={"dir":final_dec,"ts":time.time(),"price":p["price"]["cur"]}
        bal,_=get_balance(s); bet=s.bet_size(bal)
        ikb=InlineKeyboardMarkup([[
            InlineKeyboardButton("Так",callback_data="confirm_%s"%final_dec),
            InlineKeyboardButton("Ні", callback_data="skip")]])
        hint=(" (рекомендовано $%.2f = 10%%)"%bet) if bal else ""
        await app.bot.send_message(chat_id=s.uid,
            text=main_txt+"\n\nВведи суму USDC"+hint+":", reply_markup=ikb)

# ─────────────────────────────────────────────
# ПЛАНУВАЛЬНИК
# ─────────────────────────────────────────────
async def minute_tracker(app):
    while True:
        await asyncio.sleep(60)
        for uid,s in list(_sessions.items()):
            if not s.ok or not s.open_bets: continue
            for bet in s.open_bets:
                try:
                    cp=get_token_price(bet.get("token_id",""))
                    sz=bet.get("size",0); amt=bet.get("amount",0)
                    pct=round((cp*sz-amt)/amt*100,2) if amt>0 else 0
                    tl=bet.get("market_end",time.time())-time.time()
                    print("[Tracker] %s price=%.4f profit=%+.2f%% left=%.0fs"%(
                        bet.get("bet_id","")[:8],cp,pct,tl))
                except Exception as e: print("[Tracker] %s"%e)

async def scheduler(app):
    while True:
        now=datetime.datetime.now(datetime.timezone.utc)
        m2n=15-(now.minute%15)
        if m2n==15: m2n=0
        nxt=now.replace(second=2,microsecond=0)+datetime.timedelta(minutes=m2n)
        if nxt<=now: nxt+=datetime.timedelta(minutes=15)
        wait=(nxt-now).total_seconds()
        log.info("Наступний цикл через %.0fs о %s UTC",wait,nxt.strftime("%H:%M"))
        await asyncio.sleep(wait)
        await asyncio.sleep(5)
        if _sessions:
            for uid,s in list(_sessions.items()):
                try: await cycle(app,s)
                except Exception as e: log.error("Цикл uid=%d: %s",uid,e)

# ─────────────────────────────────────────────
# ЗАПУСК
# ─────────────────────────────────────────────
def main():
    app=Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    for cmd,fn in [("start",cmd_start),("stats",cmd_stats),
                   ("analyze",cmd_analyze),("autoon",cmd_autoon),("autooff",cmd_autooff)]:
        app.add_handler(CommandHandler(cmd,fn))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND,on_message))

    async def startup(app):
        asyncio.create_task(scheduler(app))
        asyncio.create_task(minute_tracker(app))
        asyncio.create_task(smart_exit_watcher(app))
        log.info("BTC Bot v5 ADAPTIVE. Smart Exit + 4 regime prompts + self-improvement.")

    app.post_init=startup
    app.run_polling(allowed_updates=Update.ALL_TYPES,drop_pending_updates=True)

if __name__=="__main__":
    main()
