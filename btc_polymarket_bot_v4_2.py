"""
BTC Polymarket Trading Bot — v6
Railway Variables: TELEGRAM_BOT_TOKEN + OPENAI_API_KEY
Magic.Link | signature_type=1 | auto-trade every 15min

ЗМІНИ v6:
 1. Winrate через баланс: зберігаємо bal_before при кожній ставці,
    при наступній ставці знімаємо bal_after і рахуємо profit = bal_after - bal_before.
    Кожна ставка записується в журнал одразу. Відображення чисте без ліній і смайлів.
 2. Стратегія: додано EMA trend filter (9/21 EMA на 5м), CVD proxy через tick volume,
    spike detector (аномальний обсяг = розворот або прискорення), ATR для фільтру
    LOW_VOL, pinbar detector на 1м і 5м.
 3. AI промпт: додано нові індикатори в scoring, уточнено правила для CHOPPY/RANGING.
 4. Сигнальне повідомлення: без ліній і смайлів, чисто і лаконічно.
"""

import asyncio, logging, json, time, datetime, os, re, io, csv, math, requests
from openai import OpenAI
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY", "")
OI_CACHE   = "oi_cache.json"
DUMP_FILE  = "signals_dump.json"
POLY_STATS = "poly_stats.json"
POLY_WR    = "poly_winrate.json"

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# СЕСІЯ
# ─────────────────────────────────────────────
class Session:
    def __init__(self, uid):
        self.uid           = uid
        self.key           = ""
        self.funder        = ""
        self.address       = None
        self.ok            = False
        self.auto          = False
        self.signals       = []
        self.trades        = []
        self.open_bets     = []
        self.state         = None
        self.tmp_key       = None
        self.pending       = {}
        self.hist          = "hist_%d.json" % uid
        self.err_f         = "err_%d.json"  % uid
        self._client       = None
        self._asia_auto    = False
        self._auto_ever_on = False
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
        return round(max(1.0, min(bal * 0.13, 500.0)), 2)

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
# CLOB КЛІЄНТ
# ─────────────────────────────────────────────
def get_client(s):
    if s._client is not None: return s._client
    from py_clob_client.client import ClobClient
    from py_clob_client.constants import POLYGON
    client = ClobClient(host="https://clob.polymarket.com", key=s.key,
                        chain_id=POLYGON, signature_type=1, funder=s.funder)
    creds = client.create_or_derive_api_creds()
    client.set_api_creds(creds)
    s._client = client
    ak = getattr(creds, "api_key", None) or (creds.get("apiKey", "") if isinstance(creds, dict) else "")
    print("[Client] OK key=%s..." % str(ak)[:12])
    return client

# ─────────────────────────────────────────────
# БАЛАНС
# ─────────────────────────────────────────────
def get_balance(s):
    if not s.ok or not s.key: return None, "Гаманець не підключено"
    try:
        from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
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
        from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
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
# WINRATE ЧЕРЕЗ БАЛАНС
#
# Логіка:
#   1. При кожній ставці записуємо запис у POLY_WR з bal_before і bal_after=null.
#   2. При наступній ставці: знімаємо поточний баланс (ПЕРЕД списанням нової ставки),
#      знаходимо останній незакритий запис цього юзера,
#      записуємо bal_after = поточний баланс,
#      profit = bal_after - bal_before.
#      Якщо profit > 0 — WIN, якщо <= 0 — LOSS.
#   3. Так ми точно знаємо скільки баланс змінився між двома ставками,
#      що і є реальним P&L попередньої ставки після резолюції маркету.
# ─────────────────────────────────────────────

def poly_wr_add(uid, bet_id, direction, amount, bal_before, strength, session, dow):
    """Записує нову ставку у winrate лог (bal_after = null)."""
    record = {
        "bet_id":     bet_id,
        "uid":        uid,
        "direction":  direction,
        "amount":     amount,
        "bal_before": bal_before,
        "bal_after":  None,
        "profit":     None,
        "result":     None,
        "strength":   strength,
        "session":    session,
        "dow":        dow,
        "placed_at":  datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "resolved_at": None,
    }
    try:
        data = []
        if os.path.exists(POLY_WR):
            with open(POLY_WR) as f: data = json.load(f)
        data.append(record)
        data = data[-1000:]
        with open(POLY_WR, "w") as f: json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print("[WR add] %s" % e)

def poly_wr_resolve(uid, new_bal):
    """
    Знаходить останній незакритий запис цього юзера і закриває його.
    new_bal — баланс ПІСЛЯ резолюції попередньої ставки (до списання нової).
    Повертає закритий запис або None.
    """
    try:
        if not os.path.exists(POLY_WR): return None
        with open(POLY_WR) as f: data = json.load(f)
        for rec in reversed(data):
            if rec.get("uid") == uid and rec.get("bal_after") is None:
                bal_before = rec.get("bal_before", new_bal)
                profit     = round(new_bal - bal_before, 2)
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
        print("[WR resolve] %s" % e)
        return None

def poly_wr_msg(uid):
    """
    Повертає текст вінрейту для конкретного юзера.
    Чисто, без ліній і смайлів.
    """
    if not os.path.exists(POLY_WR):
        return "Polymarket\n\nДаних поки немає."
    try:
        with open(POLY_WR) as f: data = json.load(f)
        my  = [x for x in data if x.get("uid") == uid]
        if not my:
            return "Polymarket\n\nДаних поки немає."

        resolved = [x for x in my if x.get("bal_after") is not None]
        pending  = [x for x in my if x.get("bal_after") is None]

        if not resolved:
            return (
                "Polymarket\n\n"
                "В очікуванні: %d ставок\n"
                "Результат з'явиться після наступної ставки." % len(pending)
            )

        total    = len(resolved)
        wins     = [x for x in resolved if x.get("result") == "WIN"]
        losses   = [x for x in resolved if x.get("result") == "LOSS"]
        total_in = round(sum(x.get("amount", 0) for x in resolved), 2)
        total_pl = round(sum(x.get("profit", 0) for x in resolved), 2)
        wr       = round(len(wins) / total * 100, 1) if total else 0
        roi      = round(total_pl / total_in * 100, 1) if total_in > 0 else 0
        avg_win  = round(sum(x["profit"] for x in wins) / len(wins), 2) if wins else 0
        avg_loss = round(sum(x["profit"] for x in losses) / len(losses), 2) if losses else 0

        # Streak
        streak = 0; streak_type = ""
        for x in reversed(resolved):
            r = x.get("result", "")
            if streak == 0: streak_type = r
            if r == streak_type: streak += 1
            else: break

        # Баланс зараз (остання відома)
        last_bal = resolved[-1].get("bal_after", 0)
        first_bal = resolved[0].get("bal_before", 0)
        total_delta = round(last_bal - first_bal, 2) if first_bal else total_pl

        lines = [
            "Polymarket  P&L",
            "",
            "Угоди        %d   WIN %d   LOSS %d" % (total, len(wins), len(losses)),
            "Вінрейт      %.1f%%" % wr,
            "Серія        %d %s поспіль" % (streak, streak_type),
            "",
            "Вкладено     $%.2f" % total_in,
            "P&L          %s$%.2f" % ("+" if total_pl >= 0 else "", total_pl),
            "ROI          %s%.1f%%" % ("+" if roi >= 0 else "", roi),
            "Сер. WIN     +$%.2f" % avg_win,
            "Сер. LOSS    $%.2f" % avg_loss,
            "Баланс зараз $%.2f" % last_bal,
            "",
            "Журнал ставок",
            "",
        ]

        # Остання 20 угоди в зворотньому порядку
        for x in reversed(resolved[-20:]):
            p      = x.get("profit", 0)
            sign   = "+" if p >= 0 else ""
            res    = x.get("result", "")
            t      = (x.get("resolved_at") or x.get("placed_at") or "")[:16].replace("T", " ")
            d      = x.get("direction", "")
            amt    = x.get("amount", 0)
            b_bef  = x.get("bal_before", 0)
            b_aft  = x.get("bal_after", 0)
            st     = x.get("strength", "")[:1]   # H/M/L
            lines.append(
                "%s  %s  %s  $%.2f  %s$%.2f  bal %s->%s  %s" % (
                    t, d, res, amt, sign, abs(p),
                    ("%.2f" % b_bef) if b_bef else "?",
                    ("%.2f" % b_aft) if b_aft else "?",
                    st
                )
            )

        if pending:
            lines += ["", "В очікуванні: %d ставка" % len(pending)]

        return "\n".join(lines)
    except Exception as e:
        return "Помилка: %s" % e


# ─────────────────────────────────────────────
# POLY STATS (для CSV)
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
        "ts", "decision", "strength", "confidence_score", "outcome",
        "entry_price", "exit_price", "real_move",
        "key_signal", "logic", "reasons",
        "session", "dow", "market_condition", "regime",
        "amd_phase", "amd_direction", "amd_reason", "amd_conf",
        "sweep5m_type", "sweep5m_level", "sweep5m_ago",
        "sweep1m_type", "sweep1m_level", "sweep1m_ago",
        "bos5m", "bos1m", "fvg_up", "fvg_dn",
        "trap_type", "trap_hint",
        "struct_15m", "struct_5m", "struct_1m",
        "vol_class", "vol_avg", "atr_pct",
        "chg_cur", "chg_prev", "mom5", "mic", "spd1",
        "ema_trend", "cvd_bias", "pinbar_1m", "vol_spike",
        "vs_strike", "window_elapsed", "window_left",
        "oi_change", "funding_rate", "funding_sent",
        "liq_long", "liq_short", "liq_signal",
        "ob_bias", "ob_imb", "lsr_ratio", "lsr_bias", "crowd_long_pct",
        "round_level", "funding_cycle_min", "stoch_rsi_1m",
        "consecutive_same", "risk_note",
    ]
    csv_buf = io.StringIO()
    writer  = csv.DictWriter(csv_buf, fieldnames=CSV_COLS, extrasaction="ignore")
    writer.writeheader()
    for sig in s.signals:
        row = {
            "ts":                sig.get("time", ""),
            "decision":          sig.get("dec", ""),
            "strength":          sig.get("strength", ""),
            "confidence_score":  sig.get("confidence_score", 0),
            "outcome":           sig.get("outcome", "PENDING"),
            "entry_price":       sig.get("entry", 0),
            "exit_price":        sig.get("exit", ""),
            "real_move":         sig.get("move", ""),
            "key_signal":        sig.get("key_signal", ""),
            "logic":             sig.get("logic", ""),
            "reasons":           " | ".join(sig.get("reasons", [])),
            "session":           sig.get("session", ""),
            "dow":               sig.get("dow", ""),
            "market_condition":  sig.get("mkt_cond", ""),
            "regime":            sig.get("regime", ""),
            "amd_phase":         sig.get("amd_phase", ""),
            "amd_direction":     sig.get("amd_dir", ""),
            "amd_reason":        sig.get("amd_reason", ""),
            "amd_conf":          sig.get("amd_conf", 0),
            "sweep5m_type":      sig.get("sw5_type", "NONE"),
            "sweep5m_level":     sig.get("sw5_level", 0),
            "sweep5m_ago":       sig.get("sw5_ago", 0),
            "sweep1m_type":      sig.get("sw1_type", "NONE"),
            "sweep1m_level":     sig.get("sw1_level", 0),
            "sweep1m_ago":       sig.get("sw1_ago", 0),
            "bos5m":             sig.get("bos5m", "NONE"),
            "bos1m":             sig.get("bos1m", "NONE"),
            "fvg_up":            sig.get("fvg_up", ""),
            "fvg_dn":            sig.get("fvg_dn", ""),
            "trap_type":         sig.get("trap", "NONE"),
            "trap_hint":         sig.get("trap_hint", ""),
            "struct_15m":        sig.get("st15m", ""),
            "struct_5m":         sig.get("st5m", ""),
            "struct_1m":         sig.get("st1m", ""),
            "vol_class":         sig.get("vol_class", ""),
            "vol_avg":           sig.get("vol_avg", 0),
            "atr_pct":           sig.get("atr_pct", 0),
            "chg_cur":           sig.get("chg_cur", 0),
            "chg_prev":          sig.get("chg_prev", 0),
            "mom5":              sig.get("mom5", 0),
            "mic":               sig.get("mic", 0),
            "spd1":              sig.get("spd1", 0),
            "ema_trend":         sig.get("ema_trend", ""),
            "cvd_bias":          sig.get("cvd_bias", ""),
            "pinbar_1m":         sig.get("pinbar_1m", ""),
            "vol_spike":         sig.get("vol_spike", False),
            "vs_strike":         sig.get("vs_strike", 0),
            "window_elapsed":    sig.get("window_elapsed", 0),
            "window_left":       sig.get("window_left", 0),
            "oi_change":         sig.get("oic", 0),
            "funding_rate":      sig.get("fr", 0),
            "funding_sent":      sig.get("fs", ""),
            "liq_long":          sig.get("ll", 0),
            "liq_short":         sig.get("ls", 0),
            "liq_signal":        sig.get("lsig", ""),
            "ob_bias":           sig.get("ob_bias", ""),
            "ob_imb":            sig.get("ob_imb", 0),
            "lsr_ratio":         sig.get("lsr_ratio", 0),
            "lsr_bias":          sig.get("lsr_bias", ""),
            "crowd_long_pct":    sig.get("cl", 0),
            "round_level":       sig.get("round_level", ""),
            "funding_cycle_min": sig.get("funding_cycle_min", 0),
            "stoch_rsi_1m":      sig.get("stoch_rsi_1m", 0),
            "consecutive_same":  sig.get("consec", 0),
            "risk_note":         sig.get("risk_note", ""),
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
        for f in ("end_date_iso", "endDate", "endDateIso", "end_time", "endTime", "end_date"):
            v = m.get(f)
            if not v: continue
            try: return float(datetime.datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp())
            except:
                try: return float(v)
                except: pass
        return None

    def get_tokens(cid):
        try:
            r = requests.get("https://clob.polymarket.com/markets/%s" % cid, timeout=15)
            if r.status_code != 200: return None, None, 0.5, 0.5
            toks = r.json().get("tokens", [])
            yi = ni = ""; yp = np_ = 0.5
            for t in toks:
                oc  = (t.get("outcome") or "").upper()
                tid = (t.get("token_id") or t.get("tokenId") or "").strip()
                pr  = float(t.get("price", 0.5) or 0.5)
                if oc in ("YES", "UP", "HIGHER", "ABOVE"): yi = tid; yp = pr
                elif oc in ("NO", "DOWN", "LOWER", "BELOW"): ni = tid; np_ = pr
            if not yi and toks:
                yi = (toks[0].get("token_id") or toks[0].get("tokenId") or "").strip()
                yp = float(toks[0].get("price", 0.5) or 0.5)
            if not ni and len(toks) > 1:
                ni  = (toks[1].get("token_id") or toks[1].get("tokenId") or "").strip()
                np_ = float(toks[1].get("price", 0.5) or 0.5)
            return yi, ni, yp, np_
        except Exception as e:
            print("[Market] %s" % e); return None, None, 0.5, 0.5

    def try_slug(slug, now):
        try:
            r = requests.get("https://gamma-api.polymarket.com/events",
                             params={"slug": slug}, timeout=15)
            if r.status_code != 200: return None
            raw = r.json()
            evs = raw if isinstance(raw, list) else ([raw] if isinstance(raw, dict) and raw else [])
            if not evs: return None
            ev = evs[0]; title = ev.get("title", "") or slug
            for m in ev.get("markets", []):
                if m.get("closed", True): continue
                cid = (m.get("conditionId") or m.get("condition_id") or m.get("id") or "").strip()
                if not cid: continue
                et = end_ts(m); diff = (et - now) if et else 900.0
                if diff <= 0: continue
                yi, ni, yp, np_ = get_tokens(cid)
                if not yi or not ni: continue
                q = m.get("question", "") or title
                return {"yes_id": yi, "no_id": ni, "yes_p": yp, "no_p": np_,
                        "q": q, "cid": cid, "diff": round(diff, 1), "end_ts": et or (now + 900)}
        except Exception as e:
            print("[Market] %s" % e)
        return None

    now = time.time(); cur = int(now // ROUND) * ROUND
    for attempt in range(1, 8):
        for ts in [cur, cur + ROUND, cur - ROUND]:
            r = try_slug("%s%d" % (SLUG, ts), now)
            if r: return r
        if attempt < 7: time.sleep(3)
    return None

# ─────────────────────────────────────────────
# СТАВКА
# ─────────────────────────────────────────────
def place_bet(s, direction, amount):
    if not s.ok:   return {"ok": False, "err": "Гаманець не підключено"}
    if amount < 1: return {"ok": False, "err": "Мінімум $1"}
    mkt = find_market()
    if not mkt: return {"ok": False, "err": "Активний маркет не знайдено"}
    token_id   = mkt["yes_id"] if direction == "UP" else mkt["no_id"]
    price      = mkt["yes_p"]  if direction == "UP" else mkt["no_p"]
    price      = max(0.01, min(0.99, float(price)))
    market_end = mkt.get("end_ts", time.time() + 900)
    try:
        r = requests.get("https://clob.polymarket.com/midpoints",
                         params={"token_id": token_id}, timeout=10)
        if r.status_code == 200:
            mid = float(r.json().get("mid", price))
            if 0.01 <= mid <= 0.99: price = mid
    except: pass
    size = round(amount / price, 2)
    last_err = ""
    for attempt in range(1, 4):
        try:
            from py_clob_client.clob_types import OrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY
            client = get_client(s)
            order  = client.create_order(OrderArgs(token_id=token_id, price=price, size=size, side=BUY))
            resp   = client.post_order(order, OrderType.GTC)
            return {"ok": True, "resp": resp, "price": price, "pot": round(size - amount, 2),
                    "mkt": mkt["q"][:60], "token_id": token_id, "size": size, "market_end": market_end}
        except Exception as e:
            last_err = str(e)
            if "500" in last_err or "execution" in last_err.lower():
                s.reset_client()
                if attempt < 3: time.sleep(3); continue
            if any(x in last_err.lower() for x in ["unauthorized", "401", "forbidden", "invalid api key"]):
                s.reset_client(); break
            if "not enough" in last_err.lower() or "allowance" in last_err.lower():
                return {"ok": False, "err": "Недостатньо USDC на Polymarket"}
            break
    return {"ok": False, "err": last_err}

# ─────────────────────────────────────────────
# FORCE SELL
# ─────────────────────────────────────────────
def force_sell(s, token_id, size, mode="normal"):
    cur_price = get_token_price(token_id)
    if cur_price == 0.0 and mode != "panic":
        return {"ok": False, "err": "Ціна токена = 0. Маркет закрився."}
    real_bal   = get_token_balance(s, token_id)
    sell_size  = real_bal if real_bal > 0 else size
    if mode == "panic":        sell_price = 0.01
    elif mode == "aggressive": sell_price = max(0.01, round(cur_price - 0.05, 4)) if cur_price > 0 else 0.01
    else:                      sell_price = max(0.01, round(cur_price - 0.02, 4)) if cur_price > 0 else 0.02
    last_err = ""
    for attempt in range(1, 4):
        try:
            from py_clob_client.clob_types import OrderArgs, OrderType
            from py_clob_client.order_builder.constants import SELL
            order = get_client(s).create_order(OrderArgs(token_id=token_id, price=sell_price,
                                                          size=sell_size, side=SELL))
            resp  = get_client(s).post_order(order, OrderType.GTC)
            return {"ok": True, "cur_price": cur_price, "sell_price": sell_price,
                    "size_sold": sell_size, "resp": resp}
        except Exception as e:
            last_err = str(e)
            if "not enough balance" in last_err.lower():
                m = re.search(r"balance:\s*(\d+)", last_err)
                if m: sell_size = round(int(m.group(1)) / 1e6, 4)
                if attempt < 3: continue
                return {"ok": False, "err": "Недостатньо токенів: %s" % last_err}
            if "500" in last_err or "execution" in last_err.lower():
                s.reset_client()
                if attempt < 3: time.sleep(2); sell_price = max(0.01, sell_price - 0.03); continue
            if any(x in last_err.lower() for x in ["unauthorized", "401", "forbidden"]): s.reset_client(); break
            break
    return {"ok": False, "err": last_err}

# ─────────────────────────────────────────────
# ВІДКРИТІ СТАВКИ
# ─────────────────────────────────────────────
async def check_open_bets(app, s):
    if not s.open_bets: return
    now = time.time(); still_open = []
    for bet in s.open_bets:
        age        = now - bet.get("placed_at", now)
        market_end = bet.get("market_end", now + 900)
        time_left  = market_end - now
        token_id   = bet.get("token_id", "")
        size       = bet.get("size", 0)
        amount     = bet.get("amount", 0)
        entry      = bet.get("entry_price", 0.5)
        direction  = bet.get("direction", "")
        mkt        = bet.get("mkt", "")
        bet_id     = bet.get("bet_id", "")
        if age < 60: still_open.append(bet); continue

        cur_price  = get_token_price(token_id)
        profit_pct = ((cur_price - entry) / entry * 100) if entry > 0 and cur_price > 0 else 0
        should_sell = False; sell_mode = "normal"; reason = ""
        if profit_pct >= 90:   should_sell = True; sell_mode = "normal";      reason = "прибуток +%.0f%%" % profit_pct
        elif time_left <= 15:  should_sell = True; sell_mode = "panic";       reason = "panic %.0f сек" % max(0, time_left)
        elif time_left <= 30:  should_sell = True; sell_mode = "aggressive";  reason = "aggressive %.0f сек" % max(0, time_left)
        elif time_left <= 60:  should_sell = True; sell_mode = "normal";      reason = "force %.0f сек" % max(0, time_left)
        if not should_sell: still_open.append(bet); continue

        if cur_price == 0.0 and time_left <= 0:
            await asyncio.sleep(45)
            bal_after, _ = get_balance(s); bal_after = bal_after or 0
            profit = round(bal_after - bet.get("bal_before", bal_after), 2)
            result = "WIN" if profit > 0 else "LOSS"
            arrow  = "UP" if direction == "UP" else "DN"
            poly_stats_update(bet_id, {"status": "CLOSED_REDEEM", "bal_after": bal_after,
                "profit": profit, "result": result,
                "closed_at": datetime.datetime.now(datetime.timezone.utc).isoformat()})
            await app.bot.send_message(chat_id=s.uid, text=(
                "Позиція закрита\n\n"
                "%s  %s\n%s\n\n"
                "Ставка      $%.2f\n"
                "Баланс      $%.2f\n"
                "P&L         %s$%.2f\n"
                "Результат   %s"
            ) % (arrow, direction, mkt[:50],
                 amount, bal_after, "+" if profit >= 0 else "", abs(profit), result))
            continue

        res = force_sell(s, token_id, size, mode=sell_mode)
        if res["ok"]:
            sp     = res.get("sell_price", cur_price); ss = res.get("size_sold", size)
            gross  = round(sp * ss, 4); profit = round(gross - amount, 2)
            pct2   = round(profit / amount * 100, 1) if amount > 0 else 0
            result = "WIN" if profit > 0 else "LOSS"
            arrow  = "UP" if direction == "UP" else "DN"
            poly_stats_update(bet_id, {"status": "CLOSED", "sell_price": sp,
                "gross": gross, "profit": profit, "result": result,
                "closed_at": datetime.datetime.now(datetime.timezone.utc).isoformat()})
            await app.bot.send_message(chat_id=s.uid, text=(
                "Позиція закрита\n\n"
                "%s  %s\n%s\n\n"
                "Причина     %s\n"
                "Вхід        %.4f\n"
                "Продаж      %.4f\n"
                "Ставка      $%.2f\n"
                "Повернулось $%.4f\n"
                "P&L         %s$%.2f  (%s%.1f%%)\n"
                "Результат   %s"
            ) % (arrow, direction, mkt[:50], reason, entry, sp, amount, gross,
                 "+" if profit >= 0 else "", abs(profit),
                 "+" if pct2 >= 0 else "", abs(pct2), result))
        else:
            if sell_mode == "panic":
                poly_stats_update(bet_id, {"status": "PANIC_FAIL", "error": res.get("err", "")})
                await app.bot.send_message(chat_id=s.uid, text=(
                    "Не вдалось закрити позицію\n\n"
                    "%s  %s\n%s\n\n"
                    "Перевір вручну на polymarket.com\n%s"
                ) % (direction, mkt[:40], reason, res.get("err", "")[:200]))
            else:
                still_open.append(bet)
    s.open_bets = still_open

# ─────────────────────────────────────────────
# BINANCE
# ─────────────────────────────────────────────
def sget(url, p=None, t=15):
    try: return requests.get(url, params=p, timeout=t).json()
    except: return None

def candles(iv, lim):
    for _ in range(3):
        d = sget("https://fapi.binance.com/fapi/v1/klines",
                 {"symbol": "BTCUSDT", "interval": iv, "limit": lim})
        if d and isinstance(d, list): break
        time.sleep(2)
    if not d or not isinstance(d, list):
        d = sget("https://api.binance.com/api/v3/klines",
                 {"symbol": "BTCUSDT", "interval": iv, "limit": lim})
    if not d or not isinstance(d, list): return []
    return [{"t": int(k[0]), "o": float(k[1]), "h": float(k[2]),
             "l": float(k[3]), "c": float(k[4]), "v": float(k[5])} for k in d]

def price_now():
    for url, p in [("https://fapi.binance.com/fapi/v1/ticker/price", {"symbol": "BTCUSDT"}),
                   ("https://api.binance.com/api/v3/ticker/price",   {"symbol": "BTCUSDT"})]:
        d = sget(url, p)
        if d and "price" in d: return float(d["price"])
    return None

def funding():
    d = sget("https://fapi.binance.com/fapi/v1/premiumIndex", {"symbol": "BTCUSDT"})
    if not d or not isinstance(d, dict):
        return {"rate": 0.0, "sent": "NEUTRAL", "mark": 0.0, "basis": 0.0}
    fr = float(d.get("lastFundingRate", 0))
    mk = float(d.get("markPrice", 0))
    ix = float(d.get("indexPrice", mk))
    return {"rate": fr,
            "sent": "LONGS_TRAPPED" if fr > 0.0005 else "SHORTS_TRAPPED" if fr < -0.0003 else "NEUTRAL",
            "mark": mk, "basis": round(mk - ix, 2)}

def liqs():
    d = sget("https://fapi.binance.com/fapi/v1/forceOrders", {"symbol": "BTCUSDT", "limit": 200})
    if not d or isinstance(d, dict):
        d = sget("https://fapi.binance.com/fapi/v1/allForceOrders", {"symbol": "BTCUSDT", "limit": 200})
    if not d or not isinstance(d, list):
        return {"ll": 0.0, "ls": 0.0, "sig": "NEUTRAL", "exh": False}
    cut = int(time.time() * 1000) - 900000
    rec = [x for x in d if isinstance(x, dict) and int(x.get("time", 0)) >= cut] or d[:50]
    ll  = sum(float(x.get("origQty", 0)) * float(x.get("price", 0)) for x in rec if x.get("side") == "SELL")
    ls  = sum(float(x.get("origQty", 0)) * float(x.get("price", 0)) for x in rec if x.get("side") == "BUY")
    return {"ll": round(ll, 2), "ls": round(ls, 2),
            "sig": "SHORT_SQUEEZE" if ls > ll * 2 else "LONG_CASCADE" if ll > ls * 2 else "NEUTRAL",
            "exh": (ll + ls) > 5e6}

def oi_data():
    d = sget("https://fapi.binance.com/fapi/v1/openInterest", {"symbol": "BTCUSDT"})
    if not d or not isinstance(d, dict): return 0.0, 0.0
    cur = float(d.get("openInterest", 0))
    try:
        prev = cur
        if os.path.exists(OI_CACHE):
            with open(OI_CACHE) as f: prev = json.load(f).get("oi", cur)
        with open(OI_CACHE, "w") as f: json.dump({"oi": cur}, f)
        return cur, round((cur - prev) / prev * 100, 4) if prev > 0 else 0.0
    except: return cur, 0.0

def orderbook():
    try:
        d = sget("https://fapi.binance.com/fapi/v1/depth", {"symbol": "BTCUSDT", "limit": 20})
        if not d or not isinstance(d, dict): return {"imb": 0.0, "bias": "NEUTRAL"}
        b = sum(float(x[1]) for x in d.get("bids", [])[:10])
        a = sum(float(x[1]) for x in d.get("asks", [])[:10])
        t = b + a; imb = round((b - a) / t * 100, 2) if t > 0 else 0.0
        return {"imb": imb, "bias": "BID_HEAVY" if imb > 20 else "ASK_HEAVY" if imb < -20 else "BALANCED"}
    except: return {"imb": 0.0, "bias": "NEUTRAL"}

def lsr():
    try:
        d = sget("https://fapi.binance.com/futures/data/topLongShortPositionRatio",
                 {"symbol": "BTCUSDT", "period": "15m", "limit": 3})
        if not d or not isinstance(d, list): return {"ratio": 1.0, "lp": 50.0, "bias": "NEUTRAL"}
        lat = d[-1]; r = float(lat.get("longShortRatio", 1.0)); lp = float(lat.get("longAccount", 0.5)) * 100
        return {"ratio": round(r, 3), "lp": round(lp, 1),
                "bias": "CROWD_LONG" if r > 1.5 else "CROWD_SHORT" if r < 0.7 else "NEUTRAL"}
    except: return {"ratio": 1.0, "lp": 50.0, "bias": "NEUTRAL"}

# ─────────────────────────────────────────────
# НОВІ ІНДИКАТОРИ v6
# ─────────────────────────────────────────────

def ema(closes, period):
    """Exponential Moving Average."""
    if len(closes) < period: return closes[-1] if closes else 0.0
    k = 2.0 / (period + 1)
    e = sum(closes[:period]) / period
    for c in closes[period:]:
        e = c * k + e * (1 - k)
    return e

def ema_trend_5m(c5):
    """
    EMA9 vs EMA21 на 5м свічках.
    Повертає: BULLISH / BEARISH / NEUTRAL + відстань у %
    """
    if len(c5) < 22: return "NEUTRAL", 0.0
    closes = [x["c"] for x in c5]
    e9  = ema(closes, 9)
    e21 = ema(closes, 21)
    diff = round((e9 - e21) / e21 * 100, 4)
    if e9 > e21 * 1.0002: return "BULLISH", diff
    if e9 < e21 * 0.9998: return "BEARISH", diff
    return "NEUTRAL", diff

def cvd_proxy(c5):
    """
    Cumulative Volume Delta proxy.
    Якщо свічка бичача (close > open) → об'єм іде в бік купівлі, навпаки — продажу.
    Рахуємо за останні 6 свічок 5м (30хв).
    Повертає bias: BUY_PRESSURE / SELL_PRESSURE / NEUTRAL + значення
    """
    if len(c5) < 6: return "NEUTRAL", 0.0
    recent = c5[-6:]
    cvd = 0.0
    for c in recent:
        body = c["c"] - c["o"]
        total = c["h"] - c["l"]
        if total == 0: continue
        # Пропорція тіла до загального діапазону
        ratio = abs(body) / total
        if body > 0: cvd += c["v"] * ratio
        else:        cvd -= c["v"] * ratio
    total_vol = sum(x["v"] for x in recent)
    pct = round(cvd / total_vol * 100, 2) if total_vol > 0 else 0.0
    bias = "BUY_PRESSURE" if pct > 15 else "SELL_PRESSURE" if pct < -15 else "NEUTRAL"
    return bias, pct

def atr(c, period=14):
    """Average True Range у % від ціни."""
    if len(c) < period + 1: return 0.0
    trs = []
    for i in range(1, len(c)):
        tr = max(c[i]["h"] - c[i]["l"],
                 abs(c[i]["h"] - c[i-1]["c"]),
                 abs(c[i]["l"] - c[i-1]["c"]))
        trs.append(tr)
    if len(trs) < period: return 0.0
    atr_val = sum(trs[-period:]) / period
    return round(atr_val / c[-1]["c"] * 100, 4)

def pinbar(c):
    """
    Pinbar detector на останній свічці.
    Повертає: BULLISH_PIN / BEARISH_PIN / NONE
    Bullish pin: довгий нижній хвіст (>60% від total range), маленьке тіло, закрив вище відкриття
    Bearish pin: довгий верхній хвіст (>60%), маленьке тіло, закрив нижче відкриття
    """
    if not c: return "NONE"
    last  = c[-1]
    total = last["h"] - last["l"]
    if total == 0: return "NONE"
    body  = abs(last["c"] - last["o"])
    uw    = last["h"] - max(last["c"], last["o"])
    lw    = min(last["c"], last["o"]) - last["l"]
    # Потрібно: тіло < 30% total, хвіст > 60% total
    if body / total > 0.35: return "NONE"
    if lw / total >= 0.60 and last["c"] >= last["o"]: return "BULLISH_PIN"
    if uw / total >= 0.60 and last["c"] <= last["o"]: return "BEARISH_PIN"
    return "NONE"

def volume_spike(c, lookback=10):
    """
    Чи поточний об'єм значно вище середнього.
    Повертає True якщо поточний об'єм > 2.5x середнього за lookback свічок.
    """
    if len(c) < lookback + 1: return False
    avg = sum(x["v"] for x in c[-(lookback+1):-1]) / lookback
    return c[-1]["v"] > avg * 2.5 if avg > 0 else False

# ─────────────────────────────────────────────
# СТАНДАРТНІ SMC ІНДИКАТОРИ
# ─────────────────────────────────────────────
def stoch_rsi(candles_1m, rsi_period=14, stoch_period=14, smooth_k=3):
    closes = [c["c"] for c in candles_1m]
    min_len = rsi_period + stoch_period + smooth_k + 2
    if len(closes) < min_len: return 50.0
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains  = [max(d, 0) for d in deltas]
    losses = [max(-d, 0) for d in deltas]
    ag = sum(gains[:rsi_period]) / rsi_period
    al = sum(losses[:rsi_period]) / rsi_period
    rsi_values = []
    for i in range(rsi_period, len(gains)):
        ag = (ag * (rsi_period - 1) + gains[i]) / rsi_period
        al = (al * (rsi_period - 1) + losses[i]) / rsi_period
        rs = ag / al if al > 0 else 100.0
        rsi_values.append(100.0 - (100.0 / (1.0 + rs)))
    if len(rsi_values) < stoch_period + smooth_k: return 50.0
    stoch_k = []
    for i in range(stoch_period - 1, len(rsi_values)):
        window = rsi_values[i - stoch_period + 1: i + 1]
        lo = min(window); hi = max(window)
        if hi == lo: stoch_k.append(50.0)
        else: stoch_k.append((rsi_values[i] - lo) / (hi - lo) * 100.0)
    if len(stoch_k) < smooth_k: return round(stoch_k[-1], 1) if stoch_k else 50.0
    return round(sum(stoch_k[-smooth_k:]) / smooth_k, 1)

def round_level_context(price):
    levels = [
        50000, 55000, 60000, 65000, 70000, 71000, 72000, 73000, 74000,
        75000, 76000, 77000, 78000, 79000, 80000, 81000, 82000, 83000,
        84000, 85000, 86000, 87000, 88000, 89000, 90000, 95000, 100000
    ]
    nearest = min(levels, key=lambda x: abs(x - price))
    dist    = round(abs(price - nearest) / price * 100, 3)
    return {"level": nearest, "dist_pct": dist, "zone": "NEAR_ROUND" if dist < 0.3 else "FAR"}

def funding_cycle_info():
    now_utc  = datetime.datetime.now(datetime.timezone.utc)
    cur_mins = now_utc.hour * 60 + now_utc.minute
    mins_to  = None
    for fh in [0, 8, 16]:
        d = fh * 60 - cur_mins
        if d > 0 and (mins_to is None or d < mins_to): mins_to = d
    return int(mins_to if mins_to else 24 * 60 - cur_mins)

def day_of_week_context():
    dow  = datetime.datetime.now(datetime.timezone.utc).weekday()
    names = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
    qual  = {"MON": "GOOD", "TUE": "BEST", "WED": "BEST",
             "THU": "GOOD", "FRI": "MODERATE", "SAT": "WEAK", "SUN": "WEAK"}
    n = names[dow]; return n, qual[n]

def macro_event_check():
    now = datetime.datetime.now(datetime.timezone.utc)
    if now.weekday() < 5 and now.hour == 13 and 25 <= now.minute <= 45:
        return True, "US_DATA_RELEASE"
    return False, "CLEAR"

def swings(c):
    sh, sl = [], []
    for i in range(2, len(c) - 2):
        h = c[i]["h"]
        if h > c[i-1]["h"] and h > c[i+1]["h"] and h > c[i-2]["h"] and h > c[i+2]["h"]:
            sh.append({"p": h, "i": i})
        l = c[i]["l"]
        if l < c[i-1]["l"] and l < c[i+1]["l"] and l < c[i-2]["l"] and l < c[i+2]["l"]:
            sl.append({"p": l, "i": i})
    return sh[-5:], sl[-5:]

def structure(c):
    sh, sl = swings(c)
    if len(sh) < 2 or len(sl) < 2: return "RANGING"
    hs = [x["p"] for x in sh]; ls = [x["p"] for x in sl]
    if all(hs[i] > hs[i-1] for i in range(1, len(hs))) and \
       all(ls[i] > ls[i-1] for i in range(1, len(ls))): return "BULLISH"
    if all(hs[i] < hs[i-1] for i in range(1, len(hs))) and \
       all(ls[i] < ls[i-1] for i in range(1, len(ls))): return "BEARISH"
    return "RANGING"

def sweep(c):
    if len(c) < 10: return {"type": "NONE", "level": 0.0, "ago": 0}
    sh, sl = swings(c[:-3])
    for i, x in enumerate(reversed(c[-5:])):
        for sv in reversed(sh):
            if x["h"] > sv["p"] and x["c"] < sv["p"]:
                return {"type": "HIGH", "level": sv["p"], "ago": i + 1}
        for sv in reversed(sl):
            if x["l"] < sv["p"] and x["c"] > sv["p"]:
                return {"type": "LOW", "level": sv["p"], "ago": i + 1}
    return {"type": "NONE", "level": 0.0, "ago": 0}

def detect_amd(c15, c5, price):
    if len(c15) < 20: return {"phase": "NONE", "dir": None, "conf": 0, "reason": ""}
    l10 = c15[-10:]; l3 = c15[-3:]; l35 = c5[-3:] if len(c5) >= 3 else []
    rng  = (max(x["h"] for x in l10) - min(x["l"] for x in l10)) / price * 100
    ab   = sum(abs(x["c"] - x["o"]) for x in l10) / len(l10) / price * 100
    acc  = rng < 0.6 and ab < 0.10
    sw15 = sweep(c15); sw5 = sweep(c5) if len(c5) >= 10 else {"type": "NONE", "level": 0.0, "ago": 0}
    sw   = sw15 if sw15["type"] != "NONE" and sw15["ago"] <= 4 else sw5
    manip = sw["type"] != "NONE" and sw["ago"] <= 4
    mc = None
    if l35:
        mv = (l35[-1]["c"] - l35[0]["o"]) / l35[0]["o"] * 100
        mc = "UP" if mv > 0.05 else "DOWN" if mv < -0.05 else None
    lm = (l3[-1]["c"] - l3[0]["o"]) / l3[0]["o"] * 100
    if manip and acc:
        if sw["type"] == "LOW":
            return {"phase": "MANIPULATION_DONE", "dir": "UP",
                    "conf": 3 if mc == "UP" else 2, "reason": "ACCUM+SWEEP_LOW"}
        lc = c15[-1]["c"]
        if lc < sw["level"] * 0.9998:
            return {"phase": "MANIPULATION_DONE", "dir": "DOWN",
                    "conf": 3 if mc == "DOWN" else 2, "reason": "SWEEP_HIGH+CLOSE_BELOW"}
        return {"phase": "MANIPULATION_DONE", "dir": "UP", "conf": 1, "reason": "SWEEP_HIGH+HOLDS"}
    if acc:
        return {"phase": "ACCUMULATION", "dir": None, "conf": 1, "reason": "TIGHT_RANGE"}
    if manip:
        lc = c15[-1]["c"]
        d  = "DOWN" if sw["type"] == "HIGH" and lc < sw["level"] * 0.9998 else "UP"
        return {"phase": "MANIPULATION", "dir": d, "conf": 2, "reason": "SWEEP_%s" % sw["type"]}
    if abs(lm) > 0.15:
        return {"phase": "DISTRIBUTION", "dir": "UP" if lm > 0 else "DOWN", "conf": 1, "reason": "ACTIVE_MOVE"}
    return {"phase": "NONE", "dir": None, "conf": 0, "reason": ""}

def manip_detect(c, sw, price):
    r = {"trap": "NONE", "hint": None}
    if len(c) < 5: return r
    last = c[-1]; body = abs(last["c"] - last["o"]); total = last["h"] - last["l"]
    if total > 0:
        uw = last["h"] - max(last["c"], last["o"])
        lw = min(last["c"], last["o"]) - last["l"]
        wr = 1 - (body / total)
        if wr > 0.7 and total / last["c"] > 0.002:
            if uw > lw * 2:   r.update({"trap": "WICK_TRAP_HIGH", "hint": "DOWN"})
            elif lw > uw * 2: r.update({"trap": "WICK_TRAP_LOW",  "hint": "UP"})
    if sw["type"] != "NONE" and sw["ago"] <= 3:
        if sw["type"] == "HIGH" and last["c"] < sw["level"] * 0.9995:
            r.update({"trap": "SWEEP_TRAP_HIGH", "hint": "DOWN"})
        elif sw["type"] == "LOW" and last["c"] > sw["level"] * 1.0005:
            r.update({"trap": "SWEEP_TRAP_LOW", "hint": "UP"})
    return r

def vol_class(c15):
    if len(c15) < 10: return "UNKNOWN", 0.0
    rng = [(x["h"] - x["l"]) / x["c"] * 100 for x in c15[-10:]]
    avg = sum(rng) / len(rng); rec = sum(rng[-3:]) / 3; pri = sum(rng[:7]) / 7
    return ("LOW_VOL"   if avg < 0.08 else
            "EXPANSION" if rec > pri * 1.5 else
            "HIGH_VOL"  if avg > 0.3  else "NORMAL"), round(avg, 4)

def session():
    h = datetime.datetime.now(datetime.timezone.utc).hour
    if 7  <= h < 12:  return "LONDON"
    elif 12 <= h < 17: return "NY_OPEN"
    elif 17 <= h < 21: return "NY_PM"
    elif h >= 21 or h < 3: return "ASIA"
    return "DEAD"

def mkt_regime(c15):
    if len(c15) < 10: return "RANGING"
    cl   = [x["c"] for x in c15[-12:]]
    ups  = sum(1 for i in range(1, len(cl)) if cl[i] > cl[i-1])
    if ups >= 9 or (len(cl) - 1 - ups) >= 9: return "TRENDING"
    alts = sum(1 for i in range(1, len(cl) - 1) if (cl[i] > cl[i-1]) != (cl[i+1] > cl[i]))
    return "CHOPPY" if alts >= 8 else "RANGING"

def fvg(c, price):
    fa = fb = None
    for i in range(1, len(c) - 1):
        pv, nx = c[i-1], c[i+1]
        if nx["l"] > pv["h"]:
            mid = (nx["l"] + pv["h"]) / 2; dist = round((price - mid) / price * 100, 4)
            if mid < price and (fb is None or dist < fb["dist"]):
                fb = {"top": nx["l"], "bot": pv["h"], "dist": dist}
        if nx["h"] < pv["l"]:
            mid = (pv["l"] + nx["h"]) / 2; dist = round((mid - price) / price * 100, 4)
            if mid > price and (fa is None or dist < fa["dist"]):
                fa = {"top": pv["l"], "bot": nx["h"], "dist": dist}
    return fa, fb

def bos(c, htf):
    if len(c) < 5: return None
    sh, sl = swings(c[:-1])
    if not sh or not sl: return None
    cl = c[-1]["c"]
    if cl > sh[-1]["p"]:
        return {"type": "CHoCH" if htf == "BEARISH" else "BOS", "dir": "UP", "level": sh[-1]["p"]}
    if cl < sl[-1]["p"]:
        return {"type": "CHoCH" if htf == "BULLISH" else "BOS", "dir": "DOWN", "level": sl[-1]["p"]}
    return None

def stops(c, price):
    sh, sl = swings(c)
    above  = [{"p": sv["p"]} for sv in sh if sv["p"] > price]
    below  = [{"p": sv["p"]} for sv in sl if sv["p"] < price]
    sa     = min(above, key=lambda x: x["p"] - price) if above else None
    sb     = min(below, key=lambda x: price - x["p"]) if below else None
    return sa, sb

# ─────────────────────────────────────────────
# BUILD PAYLOAD
# ─────────────────────────────────────────────
def build_payload(s):
    c15 = candles("15m", 24)
    c5  = candles("5m",  30)   # більше для EMA21
    c1  = candles("1m",  35)   # більше для StochRSI
    if not c15: return None

    now = time.time()
    px  = c15[-1]["c"]

    # Momentum
    chg_cur  = round((px - c15[-1]["o"]) / c15[-1]["o"] * 100, 4)
    chg_prev = round((c15[-1]["o"] - c15[-2]["c"]) / c15[-2]["c"] * 100, 4) if len(c15) >= 2 else 0.0
    mom5     = round((c5[-1]["c"] - c5[-4]["c"]) / c5[-4]["c"] * 100, 4) if len(c5) >= 4 else 0.0
    mic      = round((c1[-1]["c"] - c1[-4]["c"]) / c1[-4]["c"] * 100, 4) if len(c1) >= 4 else 0.0
    spd1     = round((c1[-1]["c"] - c1[-1]["o"]) / c1[-1]["o"] * 100, 4) if c1 else 0.0

    # Структура
    st15 = structure(c15) if len(c15) >= 6 else "RANGING"
    st5  = structure(c5)  if len(c5)  >= 6 else "RANGING"
    st1  = structure(c1)  if len(c1)  >= 6 else "RANGING"
    reg  = mkt_regime(c15)

    # Sweeps
    sw5  = sweep(c5)  if len(c5)  >= 10 else {"type": "NONE", "level": 0.0, "ago": 0}
    sw1  = sweep(c1)  if len(c1)  >= 10 else {"type": "NONE", "level": 0.0, "ago": 0}

    # BOS/CHoCH
    bc5  = bos(c5, st15) if len(c5) >= 5 else None
    bc1  = bos(c1, st5)  if len(c1) >= 5 else None

    # FVG
    f5a, f5b = fvg(c5[-12:], px) if len(c5) >= 5 else (None, None)

    # AMD і маніп
    mn   = manip_detect(c5[-6:] if len(c5) >= 6 else c1[-6:], sw5, px)
    ad   = detect_amd(c15, c5, px)

    # Stops
    sa, sb = stops(c5, px) if len(c5) >= 6 else (None, None)
    da     = round((sa["p"] - px) / px * 100, 4) if sa else 999.0
    db     = round((px - sb["p"]) / px * 100, 4) if sb else 999.0

    # Order flow
    fn = funding(); lq = liqs(); ob = orderbook(); ls = lsr(); oi, oic = oi_data()

    # Volatility + ATR
    vc, vs   = vol_class(c15)
    atr_pct  = atr(c5, 14)

    # Нові індикатори v6
    ema_tr, ema_diff = ema_trend_5m(c5)
    cvd_bias, cvd_val = cvd_proxy(c5)
    pin_1m   = pinbar(c1[-3:] if len(c1) >= 3 else c1)
    pin_5m   = pinbar(c5[-3:] if len(c5) >= 3 else c5)
    vol_spk  = volume_spike(c5, 10)
    srsi     = stoch_rsi(c1) if len(c1) >= 32 else 50.0
    rl       = round_level_context(px)
    fc_min   = funding_cycle_info()
    dow_name, dow_quality = day_of_week_context()
    macro_danger, macro_reason = macro_event_check()
    consec_count, consec_dir   = s.consecutive_same()
    sess_name = session()

    # Window
    candle_open_ts = c15[-1]["t"] / 1000
    window_elapsed = max(0, min(round(now - candle_open_ts), 900))
    window_left    = 900 - window_elapsed
    strike_approx  = c15[-1]["o"]
    vs_strike      = round((px - strike_approx) / strike_approx * 100, 4)

    return {
        "ts":      datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "ts_unix": int(now),
        "window":  {"elapsed": window_elapsed, "left": window_left,
                    "strike": round(strike_approx, 2), "vs_strike": vs_strike},
        "price":   {"cur": px, "chg_cur": chg_cur, "chg_prev": chg_prev,
                    "mom5": mom5, "mic": mic, "spd1": spd1,
                    "mark": fn["mark"], "basis": fn["basis"]},
        "struct":  {"15m": st15, "5m": st5, "1m": st1, "reg": reg},
        "liq":     {"sw5": sw5, "sw1": sw1, "sa": sa, "sb": sb, "da": da, "db": db,
                    "bos5": bc5, "bos1": bc1, "f5a": f5a, "f5b": f5b},
        "amd":     ad,
        "manip":   mn,
        "pos":     {"fr": fn["rate"], "fs": fn["sent"],
                    "ll": lq["ll"], "ls": lq["ls"], "lsig": lq["sig"], "exh": lq["exh"],
                    "oic": oic, "ob": ob["bias"], "obi": ob["imb"],
                    "lsr": ls["bias"], "lsrr": ls["ratio"], "cl": ls["lp"]},
        "ctx":     {"vol": vc, "vs": vs, "atr_pct": atr_pct,
                    "sess": sess_name, "reg": reg,
                    "dow": dow_name, "dow_q": dow_quality,
                    "ema_trend": ema_tr, "ema_diff": ema_diff,
                    "cvd_bias": cvd_bias, "cvd_val": cvd_val,
                    "pinbar_1m": pin_1m, "pinbar_5m": pin_5m,
                    "vol_spike": vol_spk,
                    "stoch_rsi": srsi, "round_level": rl,
                    "funding_cycle_min": fc_min,
                    "macro_danger": macro_danger, "macro_reason": macro_reason,
                    "consec_count": consec_count, "consec_dir": consec_dir or "NONE"},
    }

# ─────────────────────────────────────────────
# AI ПРОМПТ v6
# ─────────────────────────────────────────────
SYS = """You are a professional BTC 15-minute scalp trader on Polymarket.
Your ONLY job: predict will BTC be HIGHER or LOWER at the close of the current 15-minute window.

PRIORITY ORDER
1. LIQUIDATIONS — fresh cascade/squeeze = strongest short-term momentum signal
2. FRESH SWEEP (ago=1-2) + close confirms = reversal setup
3. ORDER BOOK heavy imbalance (>20%) — where price is being pulled right now
4. 1m MOMENTUM (mic, spd1) + EMA TREND alignment — current directional pressure
5. BOS/CHoCH on 5m or 1m — structure break confirmed
6. AMD MANIPULATION_DONE (conf>=2) — distribution leg imminent
7. CVD PRESSURE — real buyer/seller dominance over last 30min
8. PINBAR on 1m or 5m — rejection at level, reversal signal
9. VOLUME SPIKE — anomalous volume = acceleration or exhaustion
10. StochRSI extremes (<20 or >80) on 1m — overbought/oversold
11. ROUND LEVEL proximity + sweep = bounce magnet
12. FUNDING CYCLE (<30min) — position unwinding pressure

IGNORE FOR 15-MINUTE PREDICTION
Macro events, daily/weekly trends, old sweeps ago>=3, session as primary driver.

SCORING — start at 0, sum all that apply

LIQUIDATIONS (mutually exclusive):
  +3 SHORT_SQUEEZE (lsig) → UP momentum
  +3 LONG_CASCADE (lsig) → DOWN momentum

SWEEPS on 5m:
  +2 sw5 HIGH ago=1-2, close below level → DOWN reversal
  +2 sw5 LOW  ago=1-2, close above level → UP  reversal
  +1 sw5 confirms momentum direction (continuation)
  -1 sw5 ago>=3 or contradicts direction

SWEEPS on 1m (confirmation only):
  +1 sw1 HIGH ago=1 → DOWN confirmation
  +1 sw1 LOW  ago=1 → UP  confirmation

ORDER BOOK:
  +2 BID_HEAVY imb >30% → UP
  +1 BID_HEAVY imb 20-30% → UP
  +2 ASK_HEAVY imb <-30% → DOWN
  +1 ASK_HEAVY imb -20 to -30% → DOWN

MOMENTUM:
  +2 mic > +0.08% AND spd1 > +0.04% → strong UP
  +2 mic < -0.08% AND spd1 < -0.04% → strong DOWN
  +1 mic confirms direction (|mic| > 0.03%)
  -1 mic contradicts direction (|mic| > 0.03%)

EMA TREND (9/21 on 5m):
  +1 ema_trend=BULLISH and signal=UP (trend aligned)
  +1 ema_trend=BEARISH and signal=DOWN (trend aligned)
  -1 ema_trend contradicts signal direction (strong counter-trend signal needed)

CVD PROXY:
  +1 cvd_bias=BUY_PRESSURE and signal=UP (real buyers dominate)
  +1 cvd_bias=SELL_PRESSURE and signal=DOWN (real sellers dominate)
  -1 cvd_bias contradicts signal direction

PINBAR:
  +1 pinbar_1m=BULLISH_PIN and signal=UP (rejection at low, reversal up)
  +1 pinbar_1m=BEARISH_PIN and signal=DOWN (rejection at high, reversal down)
  +1 pinbar_5m confirms signal direction (stronger timeframe rejection)
  -1 pinbar contradicts direction

VOLUME SPIKE:
  +1 vol_spike=True + strong momentum in signal direction (acceleration)
  -1 vol_spike=True + exhaustion pattern (reversal candle after spike = fade)

STRUCTURE:
  +2 BOS on 5m in signal direction
  +2 CHoCH on 5m in signal direction
  +1 BOS or CHoCH on 1m confirming direction
  -1 BOS/CHoCH contradicts direction

AMD:
  +3 MANIPULATION_DONE conf>=2 in signal direction
  +2 MANIPULATION_DONE conf=1 in signal direction
  +1 MANIPULATION active in signal direction
  -1 AMD contradicts signal direction

FVG:
  +1 nearest FVG in signal direction within 0.15%

CONTEXT:
  +1 StochRSI < 20 and signal=UP (oversold)
  +1 StochRSI > 80 and signal=DOWN (overbought)
  +1 Round level NEAR_ROUND + swept + close confirms direction
  +1 Funding cycle <30min + pressure confirms direction
  +1 LONDON or NY_OPEN session
  -1 NY_PM session
  -2 DEAD session
  -1 SAT or SUN (dow_q=WEAK)
  -1 CHOPPY regime
  -1 LOW_VOL + no fresh signal
  -1 consecutive_same >= 3
  -1 macro_danger=True

WINDOW RULES
  window_left < 90sec → always LOW strength
  vs_strike > +0.20% → UP already winning, DOWN reversal risk high
  vs_strike < -0.20% → DOWN already winning, UP reversal risk high

STRENGTH THRESHOLDS
  score >= 6 → HIGH
  score 4-5  → MEDIUM
  score 2-3  → LOW
  score <= 1 → LOW

HARD RULES
1. Need 3+ independent signals for HIGH. Never HIGH on 1-2 signals.
2. CHOPPY regime → LOW max, regardless of score.
3. dow_q=WEAK → MEDIUM max.
4. window_left < 90sec → LOW always.
5. macro_danger=True → MEDIUM max.
6. consecutive_same >= 3 → need score >= 5 for same direction, else give opposite.
7. LOW_VOL + RANGING + no fresh sweep/BOS → LOW, default DOWN.
8. EMA trend contradicts signal → need 1 extra point to compensate (harder to reach HIGH).
9. CVD contradicts AND pinbar contradicts → reduce score by 1 more (conflicting signals).

OUTPUT — JSON only, nothing else:
{
  "decision": "UP or DOWN",
  "strength": "HIGH or MEDIUM or LOW",
  "confidence_score": <integer>,
  "market_condition": "TRENDING or RANGING or CHOPPY",
  "key_signal": "max 8 words",
  "logic": "2-3 sentences Ukrainian — what and WHY in 15min",
  "reasons": ["r1 with data", "r2 with data", "r3 with data"],
  "risk_note": "what invalidates this trade or NONE"
}"""

def analyze_with_ai(p, s):
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        liq = p["liq"]; pos = p["pos"]; pr = p["price"]
        st  = p["struct"]; ctx = p["ctx"]; mn = p["manip"]; ad = p["amd"]
        sw5 = liq.get("sw5", {}); sw1 = liq.get("sw1", {})
        bc5 = liq.get("bos5"); bc1 = liq.get("bos1")
        f5a = liq.get("f5a"); f5b = liq.get("f5b")
        win = p["window"]; rl = ctx["round_level"]

        msg = (
            "%s  Sess:%s  DOW:%s(%s)\n"
            "Window: elapsed=%ds left=%ds vs_strike=%+.3f%%\n"
            "Price:$%.2f chg_cur:%+.4f%% chg_prev:%+.4f%% mom5:%+.4f%% mic:%+.4f%% spd1:%+.4f%%\n"
            "Struct:15m=%s 5m=%s 1m=%s Regime:%s Vol:%s(%.4f) ATR:%.4f%%\n"
            "EMA_trend:%s(diff=%+.4f%%) CVD:%s(%.1f%%) Pinbar1m:%s Pinbar5m:%s VolSpike:%s\n"
            "AMD:%s->%s conf=%d [%s]\n"
            "Sw5m:%s@%.2f(ago=%d) Sw1m:%s@%.2f(ago=%d)\n"
            "Liq: Long:$%.0f Short:$%.0f Signal:%s Exhausted:%s\n"
            "OB:%s(%+.1f%%) L/S:%.3f(%s) CrowdLong:%.1f%%\n"
            "Fund:%+.6f(%s) OI_chg:%+.4f%%\n"
            "BOS5m:%s BOS1m:%s FVG:up=%s dn=%s\n"
            "Trap:%s hint=%s StopsAbove:%.3f%% Below:%.3f%%\n"
            "StochRSI_1m:%.1f RoundLvl:$%d(dist=%.2f%%,%s)\n"
            "FundingCycle:%dmin ConsecSame:%d(%s) MacroDanger:%s(%s)"
        ) % (
            p["ts"], ctx["sess"], ctx["dow"], ctx["dow_q"],
            win["elapsed"], win["left"], win["vs_strike"],
            pr["cur"], pr["chg_cur"], pr["chg_prev"], pr["mom5"], pr["mic"], pr["spd1"],
            st["15m"], st["5m"], st["1m"], ctx["reg"], ctx["vol"], ctx["vs"], ctx["atr_pct"],
            ctx["ema_trend"], ctx["ema_diff"], ctx["cvd_bias"], ctx["cvd_val"],
            ctx["pinbar_1m"], ctx["pinbar_5m"], str(ctx["vol_spike"]),
            ad.get("phase", "NONE"), ad.get("dir", "?"), ad.get("conf", 0), ad.get("reason", ""),
            sw5.get("type", "N"), sw5.get("level", 0), sw5.get("ago", 0),
            sw1.get("type", "N"), sw1.get("level", 0), sw1.get("ago", 0),
            pos["ll"], pos["ls"], pos["lsig"], str(pos["exh"]),
            pos["ob"], pos["obi"], pos["lsrr"], pos["lsr"], pos["cl"],
            pos["fr"], pos["fs"], pos["oic"],
            ("%s_%s@%.2f" % (bc5["type"], bc5["dir"], bc5["level"])) if bc5 else "none",
            ("%s_%s@%.2f" % (bc1["type"], bc1["dir"], bc1["level"])) if bc1 else "none",
            ("%.3f%%" % f5a["dist"]) if f5a else "none",
            ("%.3f%%" % f5b["dist"]) if f5b else "none",
            mn["trap"], str(mn["hint"]),
            liq.get("da", 999), liq.get("db", 999),
            ctx["stoch_rsi"], rl["level"], rl["dist_pct"], rl["zone"],
            ctx["funding_cycle_min"], ctx["consec_count"], ctx["consec_dir"],
            str(ctx["macro_danger"]), ctx["macro_reason"]
        )

        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "system", "content": SYS}, {"role": "user", "content": msg}],
            temperature=0.1,
            response_format={"type": "json_object"})
        return json.loads(resp.choices[0].message.content)
    except Exception as e:
        log.error("AI: %s", e); return None

# ─────────────────────────────────────────────
# СТАТИСТИКА СИГНАЛІВ
# ─────────────────────────────────────────────
def check_outcomes(s):
    now = int(time.time()); changed = False
    for sig in s.signals:
        if sig.get("outcome"): continue
        if now - sig.get("ts_unix", 0) >= 900:
            cur = price_now()
            if cur:
                entry   = sig.get("entry", 0); dec = sig.get("dec", "")
                outcome = "WIN" if (dec == "UP" and cur > entry) or (dec == "DOWN" and cur < entry) else "LOSS"
                sig["outcome"] = outcome; sig["exit"] = cur; sig["move"] = round(cur - entry, 2)
                changed = True
                if outcome == "LOSS": s.log_err(sig)
    if changed: s.save()

def stats_msg(s):
    if not s.signals: return "Даних поки немає."
    checked = [g for g in s.signals if g.get("outcome")]
    if not checked: return "Перевіряємо результати..."
    wins  = [g for g in checked if g["outcome"] == "WIN"]
    total = len(checked); wr = round(len(wins) / total * 100, 1)
    lines = [
        "Статистика сигналів AI",
        "",
        "Всього      %d   WIN %d   LOSS %d" % (total, len(wins), total - len(wins)),
        "Вінрейт     %.1f%%" % wr,
        "",
        "По силі",
    ]
    for st in ("HIGH", "MEDIUM", "LOW"):
        sub = [g for g in checked if g.get("strength") == st]
        if sub:
            w = len([g for g in sub if g["outcome"] == "WIN"])
            lines.append("  %-8s %d/%d  %.0f%%" % (st, w, len(sub), round(w / len(sub) * 100)))
    lines += ["", "По сесії"]
    for sn in ("ASIA", "LONDON", "NY_OPEN", "NY_PM", "DEAD"):
        sub = [g for g in checked if g.get("session") == sn]
        if sub:
            w = len([g for g in sub if g["outcome"] == "WIN"])
            lines.append("  %-10s %d/%d  %.0f%%" % (sn, w, len(sub), round(w / len(sub) * 100)))
    lines += ["", "По режиму"]
    for reg in ("TRENDING", "RANGING", "CHOPPY"):
        sub = [g for g in checked if g.get("mkt_cond") == reg]
        if sub:
            w = len([g for g in sub if g["outcome"] == "WIN"])
            lines.append("  %-10s %d/%d  %.0f%%" % (reg, w, len(sub), round(w / len(sub) * 100)))
    lines += ["", "По дню тижня"]
    for dow in ("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"):
        sub = [g for g in checked if g.get("dow") == dow]
        if sub:
            w = len([g for g in sub if g["outcome"] == "WIN"])
            lines.append("  %-5s %d/%d  %.0f%%" % (dow, w, len(sub), round(w / len(sub) * 100)))
    return "\n".join(lines)

# ─────────────────────────────────────────────
# КЛАВІАТУРА
# ─────────────────────────────────────────────
def is_asia():
    h = datetime.datetime.now(datetime.timezone.utc).hour
    return h >= 21 or h < 3

def kb(s):
    w    = "Гаманець: OK" if s.ok else "Підключити гаманець"
    a    = "Авто: ВИМК"  if s.auto else "Авто: УВІМК"
    asia = "Азія: активна" if is_asia() else "Азія: не активна"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(w,    callback_data="wallet"),
         InlineKeyboardButton(a,    callback_data="auto_toggle")],
        [InlineKeyboardButton("Баланс",       callback_data="balance"),
         InlineKeyboardButton("Статистика",   callback_data="stats")],
        [InlineKeyboardButton("Вінрейт Полі", callback_data="poly_wr"),
         InlineKeyboardButton("Аналіз",       callback_data="analyze")],
        [InlineKeyboardButton("Маркет",       callback_data="market"),
         InlineKeyboardButton("Помилки",      callback_data="errors")],
        [InlineKeyboardButton(asia,           callback_data="asia_info")],
    ])

WELCOME = (
    "BTC Polymarket Bot v6\n\n"
    "Сигнали кожні 15 хв  :00 :15 :30 :45 UTC\n"
    "Ставка  13% від балансу, мін $1\n"
    "Авто-Азія  вмикається якщо ти хоч раз увімкнув Авто\n\n"
    "Підключення\n\n"
    "1. Натисни Підключити гаманець\n\n"
    "Крок 1  Приватний ключ\n"
    "polymarket.com  Profile  Export Private Key\n\n"
    "Крок 2  Адреса гаманця\n"
    "polymarket.com  Deposit  скопіюй адресу 0x..."
)

# ─────────────────────────────────────────────
# КОМАНДИ
# ─────────────────────────────────────────────
async def cmd_start(u, c):
    s = sess(u.effective_user.id)
    await u.message.reply_text(WELCOME, reply_markup=kb(s))

async def cmd_stats(u, c):
    s = sess(u.effective_user.id); check_outcomes(s)
    await u.message.reply_text(stats_msg(s))
    json_b, csv_b = build_stats_files(s)
    try:
        await u.message.reply_document(document=json_b,
            filename="signals_%d.json" % s.uid, caption="JSON лог")
    except Exception as e: print("[Stats] json: %s" % e)
    try:
        await u.message.reply_document(document=csv_b,
            filename="btc_signals_%d.csv" % s.uid, caption="CSV сигнали")
    except Exception as e: print("[Stats] csv: %s" % e)

async def cmd_analyze(u, c):
    s = sess(u.effective_user.id)
    await u.message.reply_text("Аналізую...")
    await cycle(c.application, s)

async def cmd_autoon(u, c):
    s = sess(u.effective_user.id)
    if not s.ok: await u.message.reply_text("Спочатку підключи гаманець."); return
    s.auto = True; s._auto_ever_on = True
    bal, _ = get_balance(s); bet = s.bet_size(bal)
    await u.message.reply_text(
        "Авто-торгівля увімкнена\n\nБаланс: %s\nСтавка: $%.2f (13%%)" % (
            ("$%.2f" % bal) if bal else "перевіряємо...", bet), reply_markup=kb(s))

async def cmd_autooff(u, c):
    s = sess(u.effective_user.id); s.auto = False; s._asia_auto = False
    await u.message.reply_text("Авто-торгівля вимкнена.", reply_markup=kb(s))

# ─────────────────────────────────────────────
# CALLBACKS
# ─────────────────────────────────────────────
async def on_callback(u, c):
    s = sess(u.effective_user.id); q = u.callback_query; await q.answer()

    if q.data == "wallet":
        s.state = "key"; s.tmp_key = None
        await q.message.reply_text(
            "Підключення гаманця\n\n"
            "Крок 1/2  Приватний ключ\n\n"
            "polymarket.com  Profile  Export Private Key\n\n"
            "Введи ключ (64 hex символи):")

    elif q.data == "auto_toggle":
        if not s.ok: await q.message.reply_text("Спочатку підключи гаманець."); return
        s.auto = not s.auto
        if s.auto:
            s._auto_ever_on = True; s._asia_auto = False
            bal, _ = get_balance(s); bet = s.bet_size(bal)
            await q.message.reply_text(
                "Авто-торгівля увімкнена\n\nБаланс: $%.2f\nСтавка: $%.2f (13%%)" % (bal or 0, bet),
                reply_markup=kb(s))
        else:
            s._asia_auto = False
            await q.message.reply_text("Авто-торгівля вимкнена.", reply_markup=kb(s))

    elif q.data == "asia_info":
        now_k = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
        fc    = funding_cycle_info()
        await q.message.reply_text(
            "Азія сесія\n\n"
            "Час      21:00-03:00 UTC  (00:00-06:00 Київ)\n"
            "Зараз    %s Київ\n"
            "Статус   %s\n\n"
            "До funding   %d хв" % (
                now_k.strftime("%H:%M"),
                "Активна" if is_asia() else "Не активна", fc))

    elif q.data == "balance":
        bal, err = get_balance(s)
        open_val = round(sum(b.get("amount", 0) for b in s.open_bets), 2)
        if bal is not None and bal > 0:
            msg = "Баланс гаманця\n\nUSDC       $%.2f\nСтавка 13%%  $%.2f" % (bal, s.bet_size(bal))
            if open_val > 0: msg += "\nВ позиціях  $%.2f" % open_val
            await q.message.reply_text(msg)
        else:
            await q.message.reply_text(
                "Баланс: $0.00\n\n%s\n\nПоповни на polymarket.com  Deposit" % err)

    elif q.data == "stats":
        check_outcomes(s)
        await q.message.reply_text(stats_msg(s))
        json_b, csv_b = build_stats_files(s)
        try:
            await q.message.reply_document(document=json_b,
                filename="signals_%d.json" % s.uid, caption="JSON лог")
        except Exception as e: print("[Stats] json: %s" % e)
        try:
            await q.message.reply_document(document=csv_b,
                filename="btc_signals_%d.csv" % s.uid, caption="CSV сигнали")
        except Exception as e: print("[Stats] csv: %s" % e)

    elif q.data == "poly_wr":
        await q.message.reply_text(poly_wr_msg(s.uid))

    elif q.data == "analyze":
        await q.message.reply_text("Аналізую...")
        await cycle(c.application, s)

    elif q.data == "market":
        await q.message.reply_text("Шукаю маркет...")
        m = find_market()
        if m:
            await q.message.reply_text(
                "Активний маркет\n\n"
                "%s\n\ncid:\n%s\n\nYES:\n%s\n\nNO:\n%s\n\n"
                "YES %.4f  NO %.4f\nЗакривається %.0f сек" % (
                    m["q"][:80], m["cid"], m["yes_id"], m["no_id"],
                    m["yes_p"], m["no_p"], m["diff"]))
        else:
            await q.message.reply_text("Маркет не знайдено.")

    elif q.data == "errors":
        if not os.path.exists(s.err_f):
            await q.message.reply_text("Помилок немає."); return
        try:
            with open(s.err_f) as f: errs = json.load(f)
            if not errs: await q.message.reply_text("Помилок немає."); return
            lines = ["Останні LOSS  %d:" % len(errs), ""]
            for i, e in enumerate(errs[-5:], 1):
                lines.append("%d. %s %s  Score:%s\n   %s\n" % (
                    i, e.get("dec", "?"), e.get("strength", "?"),
                    e.get("confidence_score", "?"), e.get("key_signal", "")[:60]))
            await q.message.reply_text("\n".join(lines)[:4000])
        except: await q.message.reply_text("Помилка читання файлу.")

    elif q.data == "skip":
        s.pending = {}; await q.edit_message_text("Скасовано.")

    elif q.data.startswith("exec_"):
        parts     = q.data.split("_"); direction = parts[1]; amount = float(parts[2])
        await q.edit_message_text("Розміщую $%.2f..." % amount)
        bet = place_bet(s, direction, amount)
        if bet["ok"]:
            await c.bot.send_message(chat_id=u.effective_chat.id, text=(
                "Ставка виконана\n\n%s %s  %s\n\nСума       $%.2f\nПотенційно +$%.2f"
            ) % ("UP" if direction == "UP" else "DN", direction,
                 bet.get("mkt", "Polymarket"), amount, bet.get("pot", 0)))
        else:
            await c.bot.send_message(chat_id=u.effective_chat.id,
                text="Ставка не виконана\n\n%s" % bet["err"])
        s.pending = {}

# ─────────────────────────────────────────────
# ТЕКСТОВІ ПОВІДОМЛЕННЯ
# ─────────────────────────────────────────────
async def on_message(u, c):
    s = sess(u.effective_user.id); txt = u.message.text.strip()

    if s.state == "key":
        clean = txt.lower().replace("0x", "").replace(" ", "")
        if len(clean) != 64:
            await u.message.reply_text("Неправильна довжина (%d символів, потрібно 64).\nСпробуй ще раз:" % len(clean)); return
        s.tmp_key = txt; s.state = "funder"
        await u.message.reply_text(
            "Ключ прийнято.\n\n"
            "Крок 2/2  Адреса гаманця\n\n"
            "polymarket.com  Deposit  скопіюй адресу\n\nВведи адресу (0x..., 42 символи):"); return

    if s.state == "funder":
        addr = txt.strip()
        if not addr.lower().startswith("0x") or len(addr) != 42:
            await u.message.reply_text("Неправильна адреса.\nСпробуй ще раз:"); return
        s.state = None; key = s.tmp_key or ""; s.tmp_key = None
        clean = key.lower().replace("0x", "").replace(" ", "")
        s.key = "0x" + clean; s.funder = addr; s.ok = True; s._client = None
        try:
            from eth_account import Account
            s.address = Account.from_key(s.key).address
        except: s.address = s.key[:12] + "..."
        bal, err = get_balance(s); bet = s.bet_size(bal)
        bal_str  = ("$%.2f USDC" % bal) if (bal is not None and bal > 0) else ("$0  (%s)" % err)
        await u.message.reply_text(
            "Гаманець підключено\n\n"
            "Підписувач\n%s\n\nFunder\n%s\n\n"
            "Баланс   %s\nСтавка   $%.2f (13%%)\n\n"
            "Натисни Авто: УВІМК" % (s.address, addr, bal_str, bet),
            reply_markup=kb(s)); return

    if s.pending and time.time() - s.pending.get("ts", 0) <= 600:
        try:
            amount = float(txt)
            if amount < 1 or amount > 500:
                await u.message.reply_text("Сума від $1 до $500"); return
            direction = s.pending["dir"]
            ikb = InlineKeyboardMarkup([[
                InlineKeyboardButton("Підтвердити $%.2f  %s" % (amount, direction),
                    callback_data="exec_%s_%.2f" % (direction, amount)),
                InlineKeyboardButton("Скасувати", callback_data="skip")]])
            await u.message.reply_text(
                "Підтвердити?\n\n%s %s  $%.2f" % (
                    "UP" if direction == "UP" else "DN", direction, amount),
                reply_markup=ikb)
        except ValueError: pass

# ─────────────────────────────────────────────
# АВТО ТОРГІВЛЯ
# ─────────────────────────────────────────────
async def auto_trade(app, s, p, result):
    dec      = result.get("decision")
    strength = result.get("strength", "LOW")
    logic    = result.get("logic", "")
    score    = result.get("confidence_score", 0)
    if not dec: return

    # Знімаємо баланс ДО нової ставки — це і є закриття попередньої
    bal, err = get_balance(s)

    # Резолвимо попередню ставку через зміну балансу
    if bal is not None:
        resolved = poly_wr_resolve(s.uid, bal)
        if resolved:
            profit = resolved.get("profit", 0)
            sign   = "+" if profit >= 0 else ""
            await app.bot.send_message(chat_id=s.uid, text=(
                "Результат попередньої ставки\n\n"
                "Напрямок     %s\n"
                "Ставка       $%.2f\n"
                "Баланс до    $%.2f\n"
                "Баланс після $%.2f\n"
                "P&L          %s$%.2f\n"
                "Результат    %s"
            ) % (resolved.get("direction", "?"),
                 resolved.get("amount", 0),
                 resolved.get("bal_before", 0),
                 bal, sign, abs(profit),
                 resolved.get("result", "?")))

    if not bal or bal <= 0:
        await app.bot.send_message(chat_id=s.uid,
            text="Баланс $0. Поповни на polymarket.com  Deposit"); return
    amount = s.bet_size(bal)
    if amount < 1:
        await app.bot.send_message(chat_id=s.uid,
            text="Ставка $%.2f < $1. Поповни баланс." % amount); return

    # Пропускаємо слабкі сигнали
    if strength == "LOW" and score <= 2:
        await app.bot.send_message(chat_id=s.uid,
            text="Ставка пропущена  слабкий сигнал (LOW, score=%d)" % score)
        return

    bet = place_bet(s, dec, amount)
    if bet["ok"]:
        bet_id   = "%d_%d" % (s.uid, int(time.time()))
        open_bet = {
            "bet_id": bet_id, "token_id": bet["token_id"], "direction": dec,
            "amount": amount, "size": bet["size"], "entry_price": bet["price"],
            "placed_at": time.time(), "market_end": bet.get("market_end", time.time() + 900),
            "bal_before": bal, "mkt": bet.get("mkt", ""), "strength": strength, "logic": logic,
            "score": score, "key_signal": result.get("key_signal", ""),
            "amd_phase": p["amd"].get("phase", ""), "session": p["ctx"]["sess"],
            "dow": p["ctx"]["dow"],
        }
        s.open_bets.append(open_bet)
        poly_stats_update(bet_id, {"status": "OPEN", "open_data": open_bet,
            "opened_at": datetime.datetime.now(datetime.timezone.utc).isoformat()})

        # Записуємо в журнал winrate
        poly_wr_add(
            uid=s.uid, bet_id=bet_id, direction=dec, amount=amount,
            bal_before=bal, strength=strength,
            session=p["ctx"]["sess"], dow=p["ctx"]["dow"]
        )

        s.trades.append({"dec": dec, "amount": amount, "entry": p["price"]["cur"],
            "time": str(datetime.datetime.now(datetime.timezone.utc))})

        await app.bot.send_message(chat_id=s.uid, text=(
            "Ставка виконана\n\n"
            "%s %s  %s  %s\n\n"
            "Ставка      $%.2f\n"
            "Баланс      $%.2f\n"
            "Потенційно  +$%.2f\n\n"
            "%s"
        ) % ("UP" if dec == "UP" else "DN", dec, strength,
             bet.get("mkt", "Polymarket")[:40],
             amount, bal, bet.get("pot", 0), logic))
        print("[Auto] OK uid=%d bet_id=%s $%.2f" % (s.uid, bet_id, amount))
    else:
        await app.bot.send_message(chat_id=s.uid,
            text="Ставка не виконана\n\n%s" % bet["err"])

# ─────────────────────────────────────────────
# ЦИКЛ
# ─────────────────────────────────────────────
async def cycle(app, s):
    if s.ok and s.open_bets:
        await check_open_bets(app, s)
    check_outcomes(s)

    p = build_payload(s)
    if not p:
        await app.bot.send_message(chat_id=s.uid, text="Помилка даних Binance."); return

    result = analyze_with_ai(p, s)
    if not result:
        await app.bot.send_message(chat_id=s.uid, text="Помилка AI."); return

    dec       = result.get("decision", "UP")
    strength  = result.get("strength", "LOW")
    logic     = result.get("logic", "")
    score     = result.get("confidence_score", 0)
    reasons   = result.get("reasons", [])
    key_sig   = result.get("key_signal", "")
    mkt_cond  = result.get("market_condition", p["ctx"]["reg"])
    risk_note = result.get("risk_note", "NONE")
    ad = p["amd"]; mn = p["manip"]
    sw5 = p["liq"].get("sw5", {}); sw1 = p["liq"].get("sw1", {})
    bc5 = p["liq"].get("bos5");    bc1 = p["liq"].get("bos1")
    f5a = p["liq"].get("f5a");     f5b = p["liq"].get("f5b")
    consec = p["ctx"]["consec_count"]
    win    = p["window"]

    # Зберігаємо сигнал
    sig = {
        "dec": dec, "strength": strength, "confidence_score": score, "logic": logic,
        "reasons": reasons, "key_signal": key_sig, "risk_note": risk_note,
        "entry": p["price"]["cur"], "time": p["ts"], "ts_unix": p["ts_unix"], "outcome": None,
        "session": p["ctx"]["sess"], "dow": p["ctx"]["dow"],
        "mkt_cond": mkt_cond, "regime": p["ctx"]["reg"],
        "amd_phase": ad.get("phase", "NONE"), "amd_dir": ad.get("dir", ""),
        "amd_reason": ad.get("reason", ""), "amd_conf": ad.get("conf", 0),
        "sw5_type":  sw5.get("type", "NONE"), "sw5_level": sw5.get("level", 0), "sw5_ago": sw5.get("ago", 0),
        "sw1_type":  sw1.get("type", "NONE"), "sw1_level": sw1.get("level", 0), "sw1_ago": sw1.get("ago", 0),
        "bos5m": ("%s_%s" % (bc5["type"], bc5["dir"])) if bc5 else "NONE",
        "bos1m": ("%s_%s" % (bc1["type"], bc1["dir"])) if bc1 else "NONE",
        "fvg_up": ("%.3f%%" % f5a["dist"]) if f5a else "",
        "fvg_dn": ("%.3f%%" % f5b["dist"]) if f5b else "",
        "trap": mn["trap"], "trap_hint": str(mn["hint"]),
        "st15m": p["struct"]["15m"], "st5m": p["struct"]["5m"], "st1m": p["struct"]["1m"],
        "vol_class": p["ctx"]["vol"], "vol_avg": p["ctx"]["vs"], "atr_pct": p["ctx"]["atr_pct"],
        "chg_cur": p["price"]["chg_cur"], "chg_prev": p["price"]["chg_prev"],
        "mom5": p["price"]["mom5"], "mic": p["price"]["mic"], "spd1": p["price"]["spd1"],
        "ema_trend": p["ctx"]["ema_trend"], "cvd_bias": p["ctx"]["cvd_bias"],
        "pinbar_1m": p["ctx"]["pinbar_1m"], "vol_spike": p["ctx"]["vol_spike"],
        "vs_strike": win["vs_strike"], "window_elapsed": win["elapsed"], "window_left": win["left"],
        "oic": p["pos"]["oic"], "fr": p["pos"]["fr"], "fs": p["pos"]["fs"],
        "ll": p["pos"]["ll"], "ls": p["pos"]["ls"], "lsig": p["pos"]["lsig"],
        "ob_bias": p["pos"]["ob"], "ob_imb": p["pos"]["obi"],
        "lsr_ratio": p["pos"]["lsrr"], "lsr_bias": p["pos"]["lsr"], "cl": p["pos"]["cl"],
        "round_level": "%d(%.2f%%)" % (p["ctx"]["round_level"]["level"], p["ctx"]["round_level"]["dist_pct"]),
        "funding_cycle_min": p["ctx"]["funding_cycle_min"],
        "stoch_rsi_1m": p["ctx"]["stoch_rsi"],
        "consec": consec,
    }
    s.signals.append(sig); s.save()

    try:
        dump = []
        if os.path.exists(DUMP_FILE):
            with open(DUMP_FILE) as f: dump = json.load(f)
        dump.append(sig); dump = dump[-2000:]
        with open(DUMP_FILE, "w") as f: json.dump(dump, f, ensure_ascii=False, indent=2)
    except: pass

    # Формуємо сигнальне повідомлення — без ліній і смайлів
    arrow   = "UP" if dec == "UP" else "DN"
    extras  = []
    if consec >= 3:
        extras.append("%d однакових підряд" % consec)
    rl = p["ctx"]["round_level"]
    if rl["zone"] == "NEAR_ROUND":
        extras.append("Round $%d (%.2f%%)" % (rl["level"], rl["dist_pct"]))
    fc_min = p["ctx"]["funding_cycle_min"]
    if fc_min <= 30:
        extras.append("Funding %dхв %s" % (fc_min, "DN" if p["pos"]["fr"] > 0 else "UP"))
    srsi = p["ctx"]["stoch_rsi"]
    if srsi < 20:   extras.append("StochRSI %.0f перепроданий" % srsi)
    elif srsi > 80: extras.append("StochRSI %.0f перекуплений" % srsi)
    if p["pos"]["lsig"] in ("SHORT_SQUEEZE", "LONG_CASCADE"):
        extras.append(p["pos"]["lsig"])
    if p["ctx"]["ema_trend"] != "NEUTRAL":
        extras.append("EMA %s" % p["ctx"]["ema_trend"])
    if p["ctx"]["cvd_bias"] != "NEUTRAL":
        extras.append("CVD %s" % p["ctx"]["cvd_bias"])
    if p["ctx"]["vol_spike"]:
        extras.append("VOL SPIKE")
    if p["ctx"]["pinbar_1m"] != "NONE":
        extras.append("Pinbar 1m %s" % p["ctx"]["pinbar_1m"])

    msg_lines = [
        "СИГНАЛ  %s %s  %s  Score %+d" % (arrow, dec, strength, score),
        "$%.2f  %s  %s  %s" % (
            p["price"]["cur"], mkt_cond, p["ctx"]["sess"], p["ctx"]["dow"]),
    ]
    if extras:
        msg_lines.append("  ".join(extras))
    msg_lines += [
        "",
        key_sig,
        "",
        logic,
        "",
        "Причини",
    ]
    for r in reasons[:3]: msg_lines.append("  %s" % r)
    msg_lines += ["", "Ризик  %s" % risk_note]
    main_txt = "\n".join(msg_lines)

    print("[Cycle] uid=%d dec=%s str=%s score=%d consec=%d" % (
        s.uid, dec, strength, score, consec))

    # Авто-Азія
    asia = is_asia()
    if asia and not s.auto and s.ok and s._auto_ever_on:
        s._asia_auto = True; s.auto = True
        await app.bot.send_message(chat_id=s.uid,
            text="Азія сесія  авто-торгівля увімкнена (00:00-06:00 Київ)")
    if not asia and s.auto and s._asia_auto:
        s._asia_auto = False; s.auto = False
        await app.bot.send_message(chat_id=s.uid,
            text="Азія завершилась  авто-торгівля вимкнена")

    await app.bot.send_message(chat_id=s.uid, text=main_txt)

    if s.auto:
        await auto_trade(app, s, p, result)
    else:
        s.pending = {"dir": dec, "ts": time.time(), "price": p["price"]["cur"]}
        bal, _    = get_balance(s); bet = s.bet_size(bal)
        ikb = InlineKeyboardMarkup([[
            InlineKeyboardButton("Так", callback_data="confirm_%s" % dec),
            InlineKeyboardButton("Ні",  callback_data="skip")]])
        hint = ("\nРекомендовано $%.2f (13%%)" % bet) if bal else ""
        await app.bot.send_message(chat_id=s.uid,
            text=main_txt + "\n\nВведи суму USDC" + hint + ":", reply_markup=ikb)

# ─────────────────────────────────────────────
# ПЛАНУВАЛЬНИК
# ─────────────────────────────────────────────
async def minute_tracker(app):
    while True:
        await asyncio.sleep(60)
        for uid, s in list(_sessions.items()):
            if not s.ok or not s.open_bets: continue
            for bet in s.open_bets:
                try:
                    cp  = get_token_price(bet.get("token_id", ""))
                    sz  = bet.get("size", 0); amt = bet.get("amount", 0)
                    pct = round((cp * sz - amt) / amt * 100, 2) if amt > 0 else 0
                    tl  = bet.get("market_end", time.time()) - time.time()
                    print("[Tracker] %s price=%.4f profit=%+.2f%% left=%.0fs" % (
                        bet.get("bet_id", "")[:8], cp, pct, tl))
                except Exception as e: print("[Tracker] %s" % e)

async def position_watcher(app):
    while True:
        await asyncio.sleep(30)
        for uid, s in list(_sessions.items()):
            if s.ok and s.open_bets:
                try: await check_open_bets(app, s)
                except Exception as e: log.error("[Watcher] uid=%d: %s", uid, e)

async def scheduler(app):
    while True:
        now  = datetime.datetime.now(datetime.timezone.utc)
        m2n  = 15 - (now.minute % 15)
        if m2n == 15: m2n = 0
        nxt  = now.replace(second=2, microsecond=0) + datetime.timedelta(minutes=m2n)
        if nxt <= now: nxt += datetime.timedelta(minutes=15)
        wait = (nxt - now).total_seconds()
        log.info("Наступний цикл через %.0fs о %s UTC", wait, nxt.strftime("%H:%M"))
        await asyncio.sleep(wait)
        await asyncio.sleep(5)
        if _sessions:
            for uid, s in list(_sessions.items()):
                try: await cycle(app, s)
                except Exception as e: log.error("Цикл uid=%d: %s", uid, e)

# ─────────────────────────────────────────────
# ЗАПУСК
# ─────────────────────────────────────────────
def main():
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    for cmd, fn in [("start", cmd_start), ("stats", cmd_stats),
                    ("analyze", cmd_analyze), ("autoon", cmd_autoon), ("autooff", cmd_autooff)]:
        app.add_handler(CommandHandler(cmd, fn))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))

    async def startup(app):
        asyncio.create_task(scheduler(app))
        asyncio.create_task(position_watcher(app))
        asyncio.create_task(minute_tracker(app))
        log.info("BTC Bot v6. WR via balance delta. EMA+CVD+Pinbar+VolSpike added.")

    app.post_init = startup
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    main()
