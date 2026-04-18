"""
BTC Polymarket Trading Bot — v3
Railway Variables: TELEGRAM_BOT_TOKEN + OPENAI_API_KEY
Magic.Link | signature_type=1 | auto-trade every 15min
"""
import asyncio, logging, json, time, datetime, os, re, requests
from openai import OpenAI
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY", "")
OI_CACHE      = "oi_cache.json"
DUMP_FILE     = "signals_dump.json"
POLY_STATS    = "poly_stats.json"
POLY_WR_LOG   = "poly_winrate.json"

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# СЕСІЯ
# ─────────────────────────────────────────────
class Session:
    def __init__(self, uid):
        self.uid       = uid
        self.key       = ""
        self.funder    = ""
        self.address   = None
        self.ok        = False
        self.auto      = False
        self.signals   = []
        self.trades    = []
        self.open_bets = []
        self.state     = None
        self.tmp_key   = None
        self.pending   = {}
        self.hist      = "hist_%d.json" % uid
        self.err_f     = "err_%d.json"  % uid
        self._client   = None
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
        """Скільки останніх сигналів підряд в одному напрямку."""
        if len(self.signals) < 2: return 0, None
        last_dec = self.signals[-1].get("dec","")
        count = 0
        for sig in reversed(self.signals):
            if sig.get("dec","") == last_dec:
                count += 1
            else:
                break
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
    print("[Client] Init...")
    from py_clob_client.client import ClobClient
    from py_clob_client.constants import POLYGON
    client = ClobClient(host="https://clob.polymarket.com", key=s.key,
                        chain_id=POLYGON, signature_type=1, funder=s.funder)
    creds = client.create_or_derive_api_creds()
    client.set_api_creds(creds)
    s._client = client
    ak = getattr(creds,"api_key",None) or (creds.get("apiKey","") if isinstance(creds,dict) else "")
    print("[Client] OK key=%s..." % str(ak)[:12])
    return client

# ─────────────────────────────────────────────
# БАЛАНС USDC
# ─────────────────────────────────────────────
def get_balance(s):
    if not s.ok or not s.key: return None, "Гаманець не підключено"
    try:
        from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
        client = get_client(s)
        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        resp   = client.get_balance_allowance(params=params)
        raw = float(resp.get("balance") or 0) if isinstance(resp,dict) else float(getattr(resp,"balance",0) or 0)
        bal = raw / 1e6 if raw > 1000 else raw
        return round(bal,2), s.funder
    except ImportError:
        try:
            resp = get_client(s).get_balance_allowance()
            raw  = float(resp.get("balance") or 0) if isinstance(resp,dict) else 0.0
            return round((raw/1e6 if raw>1000 else raw),2), s.funder
        except Exception as e2: return 0.0, str(e2)[:100]
    except Exception as e:
        err=str(e)
        if any(x in err.lower() for x in ["unauthorized","401","forbidden","invalid"]): s.reset_client()
        return 0.0, err[:100]

# ─────────────────────────────────────────────
# БАЛАНС ТОКЕНА
# ─────────────────────────────────────────────
def get_token_balance(s, token_id: str) -> float:
    try:
        from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
        client = get_client(s)
        params = BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
        resp   = client.get_balance_allowance(params=params)
        raw    = float(resp.get("balance") or 0) if isinstance(resp,dict) else float(getattr(resp,"balance",0) or 0)
        bal    = raw / 1e6 if raw > 1000 else raw
        return round(bal, 4)
    except Exception as e:
        print("[TokenBal] Error: %s" % e)
        return 0.0

def get_token_price(token_id: str) -> float:
    try:
        r = requests.get("https://clob.polymarket.com/midpoints",
                         params={"token_id": token_id}, timeout=8)
        if r.status_code == 200:
            val = r.json().get("mid", 0)
            return float(val) if val else 0.0
    except: pass
    return 0.0

# ─────────────────────────────────────────────
# POLY STATS
# ─────────────────────────────────────────────
def poly_stats_update(bet_id: str, update: dict):
    try:
        stats = {}
        if os.path.exists(POLY_STATS):
            with open(POLY_STATS) as f: stats = json.load(f)
        if bet_id not in stats: stats[bet_id] = {}
        stats[bet_id].update(update)
        with open(POLY_STATS, "w") as f: json.dump(stats, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print("[PolyStats] Error: %s" % e)

def poly_stats_get_all() -> list:
    try:
        if os.path.exists(POLY_STATS):
            with open(POLY_STATS) as f: stats = json.load(f)
            return list(stats.values())
    except: pass
    return []

def poly_log_closed(record: dict):
    try:
        data = []
        if os.path.exists(POLY_WR_LOG):
            with open(POLY_WR_LOG) as f: data = json.load(f)
        data.append(record); data = data[-500:]
        with open(POLY_WR_LOG, "w") as f: json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print("[PolyWR] Error: %s" % e)

def _classify_poly_result(record: dict) -> str:
    sell_price   = record.get("sell_price", 0)
    gross_return = record.get("gross_return", 0)
    amount       = record.get("amount", 0)
    reason       = record.get("close_reason", "")
    if sell_price >= 0.80: return "WIN"
    if "прибуток" in reason: return "WIN"
    if amount > 0 and gross_return > amount: return "WIN"
    return "LOSS"

def poly_winrate_msg() -> str:
    if not os.path.exists(POLY_WR_LOG):
        return "Polymarket вінрейт\n\nДаних поки немає."
    try:
        with open(POLY_WR_LOG) as f: data = json.load(f)
        if not data: return "Polymarket вінрейт\n\nДаних поки немає."
        for x in data:
            x["result"] = _classify_poly_result(x)
            gross = x.get("gross_return", 0); amt = x.get("amount", 0)
            x["real_profit"] = round(gross - amt, 2)
            x["real_pct"]    = round((gross - amt) / amt * 100, 1) if amt > 0 else 0
        total        = len(data)
        wins         = [x for x in data if x.get("result") == "WIN"]
        total_in     = round(sum(x.get("amount", 0) for x in data), 2)
        total_out    = round(sum(x.get("gross_return", 0) for x in data), 2)
        total_profit = round(total_out - total_in, 2)
        wr           = round(len(wins) / total * 100, 1) if total else 0
        bar          = "▓" * int(wr/5) + "░" * (20 - int(wr/5))
        roi          = round(total_profit / total_in * 100, 1) if total_in > 0 else 0
        lines = [
            "Polymarket вінрейт", "",
            "Всього угод:   %d" % total,
            "WIN:           %d" % len(wins),
            "LOSS:          %d" % (total - len(wins)), "",
            "Вінрейт:  %.1f%%  [%s]" % (wr, bar), "",
            "Вкладено:      $%.2f" % total_in,
            "Повернулось:   $%.2f" % total_out,
            "P&L:           %s$%.2f" % ("+" if total_profit >= 0 else "", total_profit),
            "ROI:           %s%.1f%%" % ("+" if roi >= 0 else "", roi),
        ]
        lines += ["", "Останні угоди:"]
        for x in data[-7:]:
            p = x.get("real_profit", 0); sign = "+" if p >= 0 else ""
            t = x.get("closed_at","")[:16].replace("T"," ")
            lines.append("  %s %s  %s$%.2f  sell=%.3f  %s" % (
                x.get("direction","?"), x.get("result","?"), sign, abs(p),
                x.get("sell_price",0), t))
        return "\n".join(lines)
    except Exception as e:
        return "Помилка: %s" % e

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
            yi=ni=""; yp=np_=0.5
            for t in toks:
                oc=(t.get("outcome") or "").upper()
                tid=(t.get("token_id") or t.get("tokenId") or "").strip()
                pr=float(t.get("price",0.5) or 0.5)
                if oc in ("YES","UP","HIGHER","ABOVE"): yi=tid; yp=pr
                elif oc in ("NO","DOWN","LOWER","BELOW"): ni=tid; np_=pr
            if not yi and toks:
                yi=(toks[0].get("token_id") or toks[0].get("tokenId") or "").strip()
                yp=float(toks[0].get("price",0.5) or 0.5)
            if not ni and len(toks)>1:
                ni=(toks[1].get("token_id") or toks[1].get("tokenId") or "").strip()
                np_=float(toks[1].get("price",0.5) or 0.5)
            return yi,ni,yp,np_
        except Exception as e:
            print("[Market] token err: %s" % e); return None,None,0.5,0.5

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
                cid=(m.get("conditionId") or m.get("condition_id") or m.get("id") or "").strip()
                if not cid: continue
                et=end_ts(m); diff=(et-now) if et else 900.0
                if diff<=0: continue
                yi,ni,yp,np_=get_tokens(cid)
                if not yi or not ni: continue
                q=m.get("question","") or title
                print("[Market] OK: %s diff=%.0fs" % (q[:55], diff))
                return {"yes_id":yi,"no_id":ni,"yes_p":yp,"no_p":np_,
                        "q":q,"cid":cid,"diff":round(diff,1),
                        "end_ts":et or (now+900)}
        except Exception as e:
            print("[Market] err: %s" % e)
        return None

    now=time.time(); cur=int(now//ROUND)*ROUND
    for attempt in range(1,8):
        print("[Market] attempt %d/7" % attempt)
        for ts in [cur,cur+ROUND,cur-ROUND]:
            r=try_slug("%s%d"%(SLUG,ts),now)
            if r: return r
        if attempt<7: time.sleep(3)
    return None

# ─────────────────────────────────────────────
# СТАВКА
# ─────────────────────────────────────────────
def place_bet(s, direction: str, amount: float) -> dict:
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
        if r.status_code==200:
            mid=float(r.json().get("mid",price))
            if 0.01<=mid<=0.99: price=mid
    except: pass

    size = round(amount/price, 2)
    print("[Bet] dir=%s price=%.4f size=%.2f usdc=%.2f"%(direction,price,size,amount))

    last_err = ""
    for attempt in range(1,4):
        try:
            from py_clob_client.clob_types import OrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY
            client = get_client(s)
            order  = client.create_order(OrderArgs(token_id=token_id, price=price, size=size, side=BUY))
            resp   = client.post_order(order, OrderType.GTC)
            print("[Bet] OK attempt=%d: %s" % (attempt, str(resp)[:100]))
            return {"ok":True,"resp":resp,"price":price,"pot":round(size-amount,2),
                    "mkt":mkt["q"][:60],"token_id":token_id,"size":size,"market_end":market_end}
        except Exception as e:
            last_err=str(e)
            print("[Bet] attempt=%d FAIL: %s"%(attempt,last_err))
            if "500" in last_err or "execution" in last_err.lower():
                s.reset_client()
                if attempt<3: time.sleep(3); continue
            if any(x in last_err.lower() for x in ["unauthorized","401","forbidden","invalid api key"]):
                s.reset_client(); break
            if "not enough" in last_err.lower() or "allowance" in last_err.lower():
                return {"ok":False,"err":"Недостатньо USDC на Polymarket"}
            break
    return {"ok":False,"err":last_err}

# ─────────────────────────────────────────────
# FORCE SELL
# ─────────────────────────────────────────────
def force_sell(s, token_id: str, size: float, mode: str = "normal") -> dict:
    cur_price = get_token_price(token_id)
    print("[ForceSell] token=%s mode=%s cur_price=%.4f" % (token_id[:16], mode, cur_price))

    if cur_price == 0.0 and mode != "panic":
        return {"ok":False,"err":"Ціна токена = 0. Маркет закрився — перевір на polymarket.com"}

    real_bal = get_token_balance(s, token_id)
    sell_size = real_bal if real_bal > 0 else size

    if mode == "panic":       sell_price = 0.01
    elif mode == "aggressive": sell_price = max(0.01, round(cur_price - 0.05, 4)) if cur_price > 0 else 0.01
    else:                      sell_price = max(0.01, round(cur_price - 0.02, 4)) if cur_price > 0 else 0.02

    last_err = ""
    for attempt in range(1,4):
        try:
            from py_clob_client.clob_types import OrderArgs, OrderType
            from py_clob_client.order_builder.constants import SELL
            client = get_client(s)
            order  = client.create_order(OrderArgs(
                token_id=token_id, price=sell_price, size=sell_size, side=SELL))
            resp   = client.post_order(order, OrderType.GTC)
            return {"ok":True,"cur_price":cur_price,"sell_price":sell_price,
                    "size_sold":sell_size,"resp":resp}
        except Exception as e:
            last_err = str(e)
            if "not enough balance" in last_err.lower():
                m = re.search(r"balance:\s*(\d+)", last_err)
                if m:
                    sell_size = round(int(m.group(1)) / 1e6, 4)
                    if attempt < 3: continue
                return {"ok":False,"err":"Недостатньо токенів: %s" % last_err}
            if "500" in last_err or "execution" in last_err.lower():
                s.reset_client()
                if attempt < 3:
                    time.sleep(2)
                    sell_price = max(0.01, sell_price - 0.03)
                    continue
            if any(x in last_err.lower() for x in ["unauthorized","401","forbidden"]):
                s.reset_client(); break
            break
    return {"ok":False,"err":last_err}

# ─────────────────────────────────────────────
# ТРЕКЕР ПОЗИЦІЇ
# ─────────────────────────────────────────────
async def minute_tracker(app):
    while True:
        await asyncio.sleep(60)
        for uid, s in list(_sessions.items()):
            if not s.ok or not s.open_bets: continue
            for bet in s.open_bets:
                try:
                    token_id = bet.get("token_id","")
                    if not token_id: continue
                    cur_price = get_token_price(token_id)
                    size      = bet.get("size", 0)
                    amount    = bet.get("amount", 0)
                    cur_val   = round(cur_price * size, 4)
                    profit    = round(cur_val - amount, 4)
                    profit_pct= round(profit / amount * 100, 2) if amount > 0 else 0
                    time_left = bet.get("market_end", time.time()) - time.time()
                    print("[Tracker] %s price=%.4f profit=%+.2f%% left=%.0fs" % (
                        token_id[:12], cur_price, profit_pct, time_left))
                except Exception as e:
                    print("[Tracker] err: %s" % e)

# ─────────────────────────────────────────────
# ПЕРЕВІРКА ВІДКРИТИХ СТАВОК
# ─────────────────────────────────────────────
async def check_open_bets(app, s):
    if not s.open_bets: return
    now = time.time(); still_open = []

    for bet in s.open_bets:
        age        = now - bet.get("placed_at", now)
        market_end = bet.get("market_end", now + 900)
        time_left  = market_end - now
        token_id   = bet.get("token_id","")
        size       = bet.get("size", 0)
        amount     = bet.get("amount", 0)
        entry      = bet.get("entry_price", 0.5)
        direction  = bet.get("direction","")
        mkt        = bet.get("mkt","")
        bet_id     = bet.get("bet_id","")

        if age < 60: still_open.append(bet); continue

        cur_price  = get_token_price(token_id)
        profit_pct = ((cur_price - entry) / entry * 100) if entry > 0 and cur_price > 0 else 0

        should_sell = False; sell_mode = "normal"; reason = ""
        if profit_pct >= 90:
            should_sell = True; sell_mode = "normal"; reason = "прибуток +%.0f%%" % profit_pct
        elif time_left <= 15:
            should_sell = True; sell_mode = "panic";  reason = "panic %.0f сек" % max(0, time_left)
        elif time_left <= 30:
            should_sell = True; sell_mode = "aggressive"; reason = "aggressive %.0f сек" % max(0, time_left)
        elif time_left <= 60:
            should_sell = True; sell_mode = "normal"; reason = "force %.0f сек" % max(0, time_left)

        if not should_sell: still_open.append(bet); continue

        # Маркет закрився — чекаємо redeem
        if cur_price == 0.0 and time_left <= 0:
            await asyncio.sleep(45)
            bal_before = bet.get("bal_before", 0)
            bal_after, _ = get_balance(s); bal_after = bal_after or 0
            returned   = round(bal_after - bal_before + amount, 2)
            profit     = round(returned - amount, 2)
            profit_pct = round(profit / amount * 100, 1) if amount > 0 else 0
            result     = "WIN" if profit > 0 else "LOSS"
            sign       = "+" if profit >= 0 else ""
            arrow      = "▲" if direction=="UP" else "▼"
            closed = {
                "bet_id":bet_id,"direction":direction,"mkt":mkt,"amount":amount,
                "size":size,"entry_price":entry,"sell_price":0.0,"cur_price":0.0,
                "gross_return":returned,"profit":profit,"profit_pct":profit_pct,
                "result":result,"close_reason":"redeem","bal_before":bal_before,
                "bal_after":bal_after,"closed_at":datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "strength":bet.get("strength",""),"score":bet.get("score",0),
                "session":bet.get("session",""),"amd_phase":bet.get("amd_phase",""),
            }
            poly_log_closed(closed)
            poly_stats_update(bet_id, {"status":"CLOSED_REDEEM","close_data":closed})
            await app.bot.send_message(chat_id=s.uid, text=(
                "Маркет закрито  %s %s\n\n%s\n\n"
                "Повернулось: $%.2f  Ставка: $%.2f\n"
                "P&L: %s$%.2f (%s%.1f%%)\nРезультат: %s"
            ) % (arrow,direction,mkt[:55],returned,amount,sign,abs(profit),sign,abs(profit_pct),result))
            continue

        result = force_sell(s, token_id, size, mode=sell_mode)
        if result["ok"]:
            sell_price  = result.get("sell_price", cur_price)
            size_sold   = result.get("size_sold", size)
            gross       = round(sell_price * size_sold, 4)
            profit      = round(gross - amount, 2)
            profit_pct2 = round(profit / amount * 100, 1) if amount > 0 else 0
            poly_result = "WIN" if profit > 0 else "LOSS"
            sign        = "+" if profit >= 0 else "-"
            arrow       = "▲" if direction=="UP" else "▼"
            closed = {
                "bet_id":bet_id,"direction":direction,"mkt":mkt,"amount":amount,
                "size":size_sold,"entry_price":entry,"sell_price":sell_price,
                "cur_price":cur_price,"gross_return":gross,"profit":profit,
                "profit_pct":profit_pct2,"result":poly_result,"close_reason":reason,
                "closed_at":datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "strength":bet.get("strength",""),"score":bet.get("score",0),
                "session":bet.get("session",""),"amd_phase":bet.get("amd_phase",""),
            }
            poly_log_closed(closed)
            poly_stats_update(bet_id, {"status":"CLOSED","close_data":closed})
            await app.bot.send_message(chat_id=s.uid, text=(
                "Позицію закрито  %s %s\n\n%s\n\n"
                "Причина: %s\nВхід: %.4f  Продаж: %.4f\n"
                "Вкладено: $%.2f  Повернулось: $%.4f\n"
                "P&L: %s$%.2f (%s%.1f%%)\nРезультат: %s"
            ) % (arrow,direction,mkt[:55],reason,entry,sell_price,
                 amount,gross,sign,abs(profit),sign,abs(profit_pct2),poly_result))
        else:
            err_txt = result.get("err","")
            if sell_mode == "panic":
                poly_stats_update(bet_id, {"status":"PANIC_FAIL","error":err_txt})
                await app.bot.send_message(chat_id=s.uid, text=(
                    "Не вдалось закрити (panic)\n%s %s\nПеревір вручну на polymarket.com\n\n%s"
                ) % (direction, mkt[:50], err_txt))
            else:
                still_open.append(bet)

    s.open_bets = still_open

# ─────────────────────────────────────────────
# BINANCE
# ─────────────────────────────────────────────
def sget(url,p=None,t=15):
    try: return requests.get(url,params=p,timeout=t).json()
    except: return None

def candles(iv,lim):
    for _ in range(3):
        d=sget("https://fapi.binance.com/fapi/v1/klines",{"symbol":"BTCUSDT","interval":iv,"limit":lim})
        if d and isinstance(d,list): break
        time.sleep(2)
    if not d or not isinstance(d,list):
        d=sget("https://api.binance.com/api/v3/klines",{"symbol":"BTCUSDT","interval":iv,"limit":lim})
    if not d or not isinstance(d,list): return []
    return [{"t":int(k[0]),"o":float(k[1]),"h":float(k[2]),"l":float(k[3]),"c":float(k[4]),"v":float(k[5])} for k in d]

def price_now():
    for url,p in [("https://fapi.binance.com/fapi/v1/ticker/price",{"symbol":"BTCUSDT"}),
                  ("https://api.binance.com/api/v3/ticker/price",{"symbol":"BTCUSDT"})]:
        d=sget(url,p)
        if d and "price" in d: return float(d["price"])
    return None

def funding():
    d=sget("https://fapi.binance.com/fapi/v1/premiumIndex",{"symbol":"BTCUSDT"})
    if not d or not isinstance(d,dict): return {"rate":0.0,"sent":"NEUTRAL","mark":0.0,"basis":0.0}
    fr=float(d.get("lastFundingRate",0)); mk=float(d.get("markPrice",0)); ix=float(d.get("indexPrice",mk))
    return {"rate":fr,"sent":"LONGS_TRAPPED" if fr>0.0005 else "SHORTS_TRAPPED" if fr<-0.0003 else "NEUTRAL","mark":mk,"basis":round(mk-ix,2)}

def liqs():
    d=sget("https://fapi.binance.com/fapi/v1/forceOrders",{"symbol":"BTCUSDT","limit":200})
    if not d or isinstance(d,dict): d=sget("https://fapi.binance.com/fapi/v1/allForceOrders",{"symbol":"BTCUSDT","limit":200})
    if not d or not isinstance(d,list): return {"ll":0.0,"ls":0.0,"sig":"NEUTRAL","exh":False}
    cut=int(time.time()*1000)-900000
    rec=[x for x in d if isinstance(x,dict) and int(x.get("time",0))>=cut] or d[:50]
    ll=sum(float(x.get("origQty",0))*float(x.get("price",0)) for x in rec if x.get("side")=="SELL")
    ls=sum(float(x.get("origQty",0))*float(x.get("price",0)) for x in rec if x.get("side")=="BUY")
    return {"ll":round(ll,2),"ls":round(ls,2),"sig":"SHORT_SQUEEZE" if ls>ll*2 else "LONG_CASCADE" if ll>ls*2 else "NEUTRAL","exh":(ll+ls)>5e6}

def oi_data():
    d=sget("https://fapi.binance.com/fapi/v1/openInterest",{"symbol":"BTCUSDT"})
    if not d or not isinstance(d,dict): return 0.0,0.0
    cur=float(d.get("openInterest",0))
    try:
        prev=cur
        if os.path.exists(OI_CACHE):
            with open(OI_CACHE) as f: prev=json.load(f).get("oi",cur)
        with open(OI_CACHE,"w") as f: json.dump({"oi":cur},f)
        return cur,round((cur-prev)/prev*100,4) if prev>0 else 0.0
    except: return cur,0.0

def orderbook():
    try:
        d=sget("https://fapi.binance.com/fapi/v1/depth",{"symbol":"BTCUSDT","limit":20})
        if not d or not isinstance(d,dict): return {"imb":0.0,"bias":"NEUTRAL"}
        b=sum(float(x[1]) for x in d.get("bids",[])[:10]); a=sum(float(x[1]) for x in d.get("asks",[])[:10])
        t=b+a; imb=round((b-a)/t*100,2) if t>0 else 0.0
        return {"imb":imb,"bias":"BID_HEAVY" if imb>20 else "ASK_HEAVY" if imb<-20 else "BALANCED"}
    except: return {"imb":0.0,"bias":"NEUTRAL"}

def lsr():
    try:
        d=sget("https://fapi.binance.com/futures/data/topLongShortPositionRatio",{"symbol":"BTCUSDT","period":"15m","limit":3})
        if not d or not isinstance(d,list): return {"ratio":1.0,"lp":50.0,"bias":"NEUTRAL"}
        lat=d[-1]; r=float(lat.get("longShortRatio",1.0)); lp=float(lat.get("longAccount",0.5))*100
        return {"ratio":round(r,3),"lp":round(lp,1),"bias":"CROWD_LONG" if r>1.5 else "CROWD_SHORT" if r<0.7 else "NEUTRAL"}
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
        for s in reversed(sh):
            if x["h"]>s["p"] and x["c"]<s["p"]: return {"type":"HIGH","level":s["p"],"ago":i+1}
        for s in reversed(sl):
            if x["l"]<s["p"] and x["c"]>s["p"]: return {"type":"LOW","level":s["p"],"ago":i+1}
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
    above=[{"p":s["p"]} for s in sh if s["p"]>price]
    below=[{"p":s["p"]} for s in sl if s["p"]<price]
    sa=min(above,key=lambda x:x["p"]-price) if above else None
    sb=min(below,key=lambda x:price-x["p"]) if below else None
    return sa,sb

# ─────────────────────────────────────────────
# BUILD PAYLOAD — чистий аналіз без попереднього контексту
# ─────────────────────────────────────────────
def build_payload(s):
    c15=candles("15m",100); c5=candles("5m",50); c1=candles("1m",30)
    if not c15: return None
    px=c15[-1]["c"]; prev=c15[-2]["c"] if len(c15)>=2 else px
    chg15=round((px-prev)/prev*100,4)
    chg5=round((c5[-1]["c"]-c5[-4]["c"])/c5[-4]["c"]*100,4) if len(c5)>=4 else 0.0
    mom3=round((c15[-1]["c"]-c15[-4]["c"])/c15[-4]["c"]*100,4) if len(c15)>=4 else 0.0
    mic=round((c1[-1]["c"]-c1[-4]["c"])/c1[-4]["c"]*100,4) if len(c1)>=4 else 0.0
    st15=structure(c15) if len(c15)>=6 else "RANGING"
    st5=structure(c5)  if len(c5)>=6  else "RANGING"
    st1=structure(c1)  if len(c1)>=6  else "RANGING"
    sw15=sweep(c15); sw5=sweep(c5) if c5 else {"type":"NONE","level":0.0,"ago":0}
    sw1=sweep(c1)  if c1 else {"type":"NONE","level":0.0,"ago":0}
    sa,sb=stops(c15,px)
    f5a,f5b=fvg(c5[-30:],px) if len(c5)>=5 else (None,None)
    bc5=bos(c5,st15) if len(c5)>=5 else None
    mn=manip_detect(c5[-10:] if len(c5)>=10 else c15[-10:],sw5,px)
    ad=detect_amd(c15,c5,px)
    fn=funding(); lq=liqs(); oi,oic=oi_data(); ob=orderbook(); ls=lsr()
    vc,vs=vol_class(c15); sess_name,sb2=session(); reg=mkt_regime(c15)
    da=round((sa["p"]-px)/px*100,4) if sa else 999.0
    db=round((px-sb["p"])/px*100,4) if sb else 999.0

    # Підраховуємо скільки однакових сигналів підряд — передаємо в AI
    consec_count, consec_dir = s.consecutive_same()

    return {
        "ts":datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "ts_unix":int(time.time()),
        "price":{"cur":px,"chg15":chg15,"chg5":chg5,"mom3":mom3,"mic":mic,"mark":fn["mark"],"basis":fn["basis"]},
        "struct":{"15m":st15,"5m":st5,"1m":st1},
        "liq":{"sw15":sw15,"sw5":sw5,"sw1":sw1,"sa":sa,"sb":sb,"da":da,"db":db,"f5a":f5a,"f5b":f5b,"bos5":bc5},
        "amd":ad,"manip":mn,
        "pos":{"fr":fn["rate"],"fs":fn["sent"],"ll":lq["ll"],"ls":lq["ls"],"lsig":lq["sig"],
               "exh":lq["exh"],"oi":oi,"oic":oic,"ob":ob["bias"],"obi":ob["imb"],
               "lsr":ls["bias"],"lsrr":ls["ratio"],"cl":ls["lp"]},
        "ctx":{"vol":vc,"vs":vs,"sess":sess_name,"sb2":sb2,"reg":reg,
               "consec_count":consec_count,"consec_dir":consec_dir or "NONE"},
    }

# ─────────────────────────────────────────────
# AI — повністю незалежний аналіз кожного разу
# ─────────────────────────────────────────────
SYS="""You are an elite BTC short-term trader. Predict UP or DOWN for next 15 minutes on Polymarket.

CRITICAL RULES:
- Analyze ONLY current market data. Ignore any previous signals completely.
- Each analysis is INDEPENDENT — fresh eyes every time.
- If consecutive_same >= 3: be VERY skeptical of giving the same direction again. Require extra strong evidence.
- SWEEP signals older than 2 candles (ago >= 3) are WEAK — do not use as primary reason.
- In RANGING market with LOW_VOL and no clear signal → prefer DOWN or LOW strength only.
- CHOPPY market → reduce confidence by 2.

TIMEFRAMES: 15m=context | 5m=tactics | 1m=execution

SCORING (start from 0, can be negative):
+3 = AMD MANIPULATION_DONE with conf>=2
+2 = CHoCH or BOS (5m) in trend direction
+2 = SHORT_SQUEEZE liquidations
-2 = LONG_CASCADE liquidations
+2 = SWEEP ago<=1 (very fresh)
+1 = SWEEP ago==2
-1 = SWEEP ago>=3 (stale)
+1 = FVG in direction
+1 = LONDON or NY_OPEN session
-1 = DEAD session
-1 = CHOPPY regime
+1 = BID_HEAVY book (UP) or ASK_HEAVY (DOWN)
+1 = CROWD_SHORT bias (contrarian UP) or CROWD_LONG (contrarian DOWN)

STRENGTH: score>=5=HIGH | score 3-4=MEDIUM | score 1-2=LOW | score<=0=skip (still give answer but LOW)

OUTPUT JSON only — no markdown, no explanation outside JSON:
{"decision":"UP or DOWN","strength":"HIGH or MEDIUM or LOW",
"confidence_score":<int>,"market_condition":"TRENDING or RANGING or CHOPPY",
"key_signal":"one short sentence","logic":"2-3 sentences in Ukrainian",
"reasons":["r1","r2","r3"],"risk_note":"main risk or NONE"}"""

def analyze_with_ai(p, s):
    try:
        client=OpenAI(api_key=OPENAI_API_KEY)
        liq=p["liq"]; pos=p["pos"]; pr=p["price"]
        st=p["struct"]; ctx=p["ctx"]; mn=p["manip"]; ad=p["amd"]
        sw15=liq.get("sw15",{}); sw5=liq.get("sw5",{}); sw1=liq.get("sw1",{})
        bc5=liq.get("bos5"); f5a=liq.get("f5a"); f5b=liq.get("f5b")

        msg=(
            "Time:%s Sess:%s ConsecSame:%d(%s)\n"
            "PRICE:$%.2f 15m:%+.4f%% 5m:%+.4f%% Mom3:%+.4f%% Mic:%+.4f%%\n"
            "STRUCT:15m=%s 5m=%s 1m=%s Reg:%s Vol:%s(%.4f)\n"
            "AMD:%s->%s conf=%d [%s]\n"
            "Sw15m:%s@%.2f(ago=%d) Sw5m:%s@%.2f(ago=%d) Sw1m:%s@%.2f(ago=%d)\n"
            "StopsUp:%.3f%% StopsDn:%.3f%% BOS5m:%s FVG5:up=%s dn=%s\n"
            "Trap:%s hint=%s\n"
            "Fund:%+.6f(%s) LiqL:$%.0f LiqS:$%.0f LiqSig:%s Exh:%s\n"
            "OI:%+.4f%% Book:%s(%+.1f%%) L/S:%.3f(%s) CL:%.1f%%"
        ) % (
            p["ts"], ctx["sess"], ctx["consec_count"], ctx["consec_dir"],
            pr["cur"], pr["chg15"], pr["chg5"], pr["mom3"], pr["mic"],
            st["15m"], st["5m"], st["1m"], ctx["reg"], ctx["vol"], ctx["vs"],
            ad.get("phase","NONE"), ad.get("dir","?"), ad.get("conf",0), ad.get("reason",""),
            sw15.get("type","N"), sw15.get("level",0), sw15.get("ago",0),
            sw5.get("type","N"),  sw5.get("level",0),  sw5.get("ago",0),
            sw1.get("type","N"),  sw1.get("level",0),  sw1.get("ago",0),
            liq.get("da",999), liq.get("db",999),
            ("%s %s@%.2f" % (bc5["type"],bc5["dir"],bc5["level"])) if bc5 else "none",
            ("%.3f%%" % f5a["dist"]) if f5a else "none",
            ("%.3f%%" % f5b["dist"]) if f5b else "none",
            mn["trap"], str(mn["hint"]),
            pos["fr"], pos["fs"], pos["ll"], pos["ls"], pos["lsig"], str(pos["exh"]),
            pos["oic"], pos["ob"], pos["obi"], pos["lsrr"], pos["lsr"], pos["cl"]
        )

        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role":"system","content":SYS},{"role":"user","content":msg}],
            temperature=0.1,
            response_format={"type":"json_object"})
        return json.loads(resp.choices[0].message.content)
    except Exception as e:
        log.error("AI: %s", e); return None

# ─────────────────────────────────────────────
# СТАТИСТИКА
# ─────────────────────────────────────────────
def check_outcomes(s):
    now=int(time.time()); changed=False
    for sig in s.signals:
        if sig.get("outcome"): continue
        if now - sig.get("ts_unix",0) >= 900:
            cur = price_now()
            if cur:
                entry = sig.get("entry", 0)
                dec   = sig.get("dec","")
                outcome = "WIN" if (dec=="UP" and cur>entry) or (dec=="DOWN" and cur<entry) else "LOSS"
                sig["outcome"] = outcome
                sig["exit"]    = cur
                sig["move"]    = round(cur - entry, 2)
                changed = True
                if outcome == "LOSS": s.log_err(sig)
    if changed: s.save()

def stats(s):
    if not s.signals: return "Даних поки немає."
    checked = [g for g in s.signals if g.get("outcome")]
    if not checked: return "Перевіряємо результати..."
    wins = [g for g in checked if g["outcome"]=="WIN"]
    total = len(checked)
    wr = round(len(wins)/total*100,1)
    bar = "▓"*int(wr/5) + "░"*(20-int(wr/5))
    lines = [
        "СТАТИСТИКА",
        "Всього: %d  WIN: %d  LOSS: %d" % (total, len(wins), total-len(wins)),
        "Вінрейт: %.1f%%  [%s]" % (wr, bar), "",
    ]
    for st in ("HIGH","MEDIUM","LOW"):
        sub = [g for g in checked if g.get("strength")==st]
        if sub:
            w = len([g for g in sub if g["outcome"]=="WIN"])
            lines.append("%s: %d/%d (%.1f%%)" % (st,w,len(sub),round(w/len(sub)*100,1)))
    lines.append("")
    for sess_name in ("ASIA","LONDON","NY_OPEN","NY_PM","DEAD"):
        sub = [g for g in checked if g.get("session")==sess_name]
        if sub:
            w = len([g for g in sub if g["outcome"]=="WIN"])
            lines.append("%s: %d/%d (%.1f%%)" % (sess_name,w,len(sub),round(w/len(sub)*100,1)))
    lines.append("")
    for reg in ("TRENDING","RANGING","CHOPPY"):
        sub = [g for g in checked if g.get("mkt_cond")==reg]
        if sub:
            w = len([g for g in sub if g["outcome"]=="WIN"])
            lines.append("%s: %d/%d (%.1f%%)" % (reg,w,len(sub),round(w/len(sub)*100,1)))
    return "\n".join(lines)

def get_news():
    try:
        d=sget("https://min-api.cryptocompare.com/data/v2/news/",{"categories":"BTC,Bitcoin","lTs":0})
        if d and "Data" in d:
            bkw=["bull","surge","rally","etf"]; skw=["bear","drop","crash","dump","ban"]
            lines=[]
            for item in d["Data"][:5]:
                t=item.get("title","").lower()
                p_=sum(1 for k in bkw if k in t); n=sum(1 for k in skw if k in t)
                lines.append("[%s] %s" % ("BULLISH" if p_>n else "BEARISH" if n>p_ else "NEUTRAL",
                                          item.get("title","")[:70]))
            return "\n".join(lines)
    except: pass
    return "Новини недоступні"

# ─────────────────────────────────────────────
# КЛАВІАТУРА
# ─────────────────────────────────────────────
def is_asia_session() -> bool:
    h = datetime.datetime.now(datetime.timezone.utc).hour
    return h >= 21 or h < 3

def kb(s):
    w = "Гаманець: підключено" if s.ok else "Підключити гаманець"
    a = "Авто: ВИМК" if s.auto else "Авто: УВІМК"
    asia = "🟢 Азія активна" if is_asia_session() else "⚪ Не Азія"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(w,  callback_data="wallet"),
         InlineKeyboardButton(a,  callback_data="auto_toggle")],
        [InlineKeyboardButton("Баланс",       callback_data="balance"),
         InlineKeyboardButton("Статистика",   callback_data="stats")],
        [InlineKeyboardButton("Вінрейт Полі", callback_data="poly_wr"),
         InlineKeyboardButton("Аналіз",       callback_data="analyze")],
        [InlineKeyboardButton("Маркет",       callback_data="market"),
         InlineKeyboardButton("Новини",       callback_data="news")],
        [InlineKeyboardButton(asia,           callback_data="asia_status")],
    ])

WELCOME=(
    "BTC Polymarket Bot v3\n\n"
    "Сигнали кожні 15 хв — :00 :15 :30 :45 UTC\n"
    "Ставка — 13% від балансу (мін. $1)\n"
    "Авто-Азія — вмикається автоматично (00:00–06:00 Київ)\n\n"
    "Як підключити:\n"
    "1. Натисни «Підключити гаманець»\n"
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
    await u.message.reply_text(stats(s))

async def cmd_analyze(u,c):
    s=sess(u.effective_user.id)
    await u.message.reply_text("Аналізую...")
    await cycle(c.application, s)

async def cmd_autoon(u,c):
    s=sess(u.effective_user.id)
    if not s.ok: await u.message.reply_text("Спочатку підключи гаманець."); return
    s.auto=True; bal,_=get_balance(s); bet=s.bet_size(bal)
    await u.message.reply_text(
        "Авто-торгівля увімкнена\nБаланс: %s\nСтавка: $%.2f (13%%)" % (
            ("$%.2f"%bal) if bal else "перевіряємо...", bet), reply_markup=kb(s))

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
            "Підключення гаманця\n\nКрок 1/2 — Приватний ключ\n"
            "polymarket.com → Profile → Export Private Key\n\nВведи ключ (64 hex символи):")

    elif q.data=="auto_toggle":
        if not s.ok: await q.message.reply_text("Спочатку підключи гаманець."); return
        s.auto = not s.auto
        if s.auto:
            s._asia_auto = False
            bal,_ = get_balance(s); bet = s.bet_size(bal)
            await q.message.reply_text(
                "Авто-торгівля увімкнена\nБаланс: $%.2f\nСтавка: $%.2f (13%%)" % (
                    bal or 0, bet), reply_markup=kb(s))
        else:
            s._asia_auto = False
            await q.message.reply_text("Авто-торгівля вимкнена.", reply_markup=kb(s))

    elif q.data=="asia_status":
        now_kyiv = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
        status = "Торгую зараз" if is_asia_session() else "Не активна"
        await q.message.reply_text(
            "Азія сесія\n21:00–03:00 UTC (00:00–06:00 Київ)\n\n"
            "Зараз: %s Київ\nСтатус: %s" % (now_kyiv.strftime("%H:%M"), status))

    elif q.data=="balance":
        bal,err = get_balance(s)
        open_val = round(sum(b.get("amount",0) for b in s.open_bets), 2)
        if bal is not None and bal > 0:
            msg = "Баланс: $%.2f USDC\nСтавка: $%.2f (13%%)" % (bal, s.bet_size(bal))
            if open_val > 0: msg += "\nВ позиціях: $%.2f" % open_val
            await q.message.reply_text(msg)
        else:
            await q.message.reply_text("Баланс: $0.00\n%s\n\nПоповни на polymarket.com → Deposit" % err)

    elif q.data=="stats":
        check_outcomes(s)
        await q.message.reply_text(stats(s))

    elif q.data=="poly_wr":
        await q.message.reply_text(poly_winrate_msg())

    elif q.data=="analyze":
        await q.message.reply_text("Аналізую..."); await cycle(c.application, s)

    elif q.data=="market":
        await q.message.reply_text("Шукаю маркет...")
        m = find_market()
        if m:
            await q.message.reply_text(
                "Маркет знайдено\n\n%s\n\ncondition_id:\n%s\n\nYES:\n%s\n\nNO:\n%s\n\n"
                "YES: %.4f  NO: %.4f\nЗакривається через: %.0f сек" % (
                    m["q"][:80],m["cid"],m["yes_id"],m["no_id"],m["yes_p"],m["no_p"],m["diff"]))
        else:
            await q.message.reply_text("Маркет не знайдено.")

    elif q.data=="news":
        await q.message.reply_text("Новини BTC\n\n%s" % get_news())

    elif q.data=="skip":
        s.pending={}; await q.edit_message_text("Скасовано.")

    elif q.data.startswith("exec_"):
        parts=q.data.split("_"); direction=parts[1]; amount=float(parts[2])
        await q.edit_message_text("Розміщую ставку $%.2f..." % amount)
        bet=place_bet(s, direction, amount)
        if bet["ok"]:
            await c.bot.send_message(chat_id=u.effective_chat.id,
                text="Ставка виконана\n%s | %s\n$%.2f — потенційно +$%.2f" % (
                    direction, bet.get("mkt","Polymarket"), amount, bet.get("pot",0)))
        else:
            await c.bot.send_message(chat_id=u.effective_chat.id,
                text="Ставка не виконана\n%s" % bet["err"])
        s.pending={}

# ─────────────────────────────────────────────
# ТЕКСТОВІ ПОВІДОМЛЕННЯ
# ─────────────────────────────────────────────
async def on_message(u,c):
    s=sess(u.effective_user.id); txt=u.message.text.strip()

    if s.state=="key":
        clean=txt.lower().replace("0x","").replace(" ","")
        if len(clean)!=64:
            await u.message.reply_text("Неправильна довжина (%d, потрібно 64).\nСпробуй ще раз:" % len(clean)); return
        s.tmp_key=txt; s.state="funder"
        await u.message.reply_text(
            "Ключ прийнято.\n\nКрок 2/2 — Адреса гаманця\n"
            "polymarket.com → Deposit → скопіюй адресу\n\nВведи адресу (0x..., 42 символи):"); return

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
        bal_str=("$%.2f USDC"%bal) if (bal is not None and bal>0) else ("$0  (%s)"%err)
        await u.message.reply_text(
            "Гаманець підключено\n\nПідписувач: %s\nFunder: %s\n\nБаланс: %s\nСтавка: $%.2f (13%%)\n\nНатисни «Авто: УВІМК»" % (
                s.address, addr, bal_str, bet), reply_markup=kb(s)); return

    if s.pending and time.time()-s.pending.get("ts",0)<=600:
        try:
            amount=float(txt)
            if amount<1 or amount>500: await u.message.reply_text("Сума від $1 до $500"); return
            direction=s.pending["dir"]
            ikb=InlineKeyboardMarkup([[
                InlineKeyboardButton("Підтвердити $%.2f → %s"%(amount,direction),
                    callback_data="exec_%s_%.2f"%(direction,amount)),
                InlineKeyboardButton("Скасувати", callback_data="skip")]])
            await u.message.reply_text("Підтвердити ставку?", reply_markup=ikb)
        except ValueError: pass

# ─────────────────────────────────────────────
# АВТО ТОРГІВЛЯ
# ─────────────────────────────────────────────
async def auto_trade(app, s, p, result):
    dec=result.get("decision"); strength=result.get("strength","LOW"); logic=result.get("logic","")
    if not dec: return
    bal,err=get_balance(s)
    if not bal or bal<=0:
        await app.bot.send_message(chat_id=s.uid, text="Баланс $0. Поповни на polymarket.com → Deposit"); return
    amount=s.bet_size(bal)
    if amount<1:
        await app.bot.send_message(chat_id=s.uid, text="Ставка $%.2f < $1. Поповни баланс." % amount); return
    bet=place_bet(s, dec, amount)
    if bet["ok"]:
        bet_id = "%d_%d" % (s.uid, int(time.time()))
        open_bet={
            "bet_id":      bet_id,
            "token_id":    bet["token_id"],
            "direction":   dec,
            "amount":      amount,
            "size":        bet["size"],
            "entry_price": bet["price"],
            "placed_at":   time.time(),
            "market_end":  bet.get("market_end", time.time()+900),
            "bal_before":  bal,
            "mkt":         bet.get("mkt",""),
            "strength":    strength,
            "logic":       logic,
            "score":       result.get("confidence_score",0),
            "key_signal":  result.get("key_signal",""),
            "amd_phase":   p["amd"].get("phase",""),
            "session":     p["ctx"]["sess"],
        }
        s.open_bets.append(open_bet)
        poly_stats_update(bet_id, {
            "status":    "OPEN",
            "open_data": open_bet,
            "opened_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        })
        s.trades.append({"dec":dec,"amount":amount,"entry":p["price"]["cur"],
                         "time":str(datetime.datetime.now(datetime.timezone.utc))})
        await app.bot.send_message(chat_id=s.uid, text=(
            "Ставка виконана  %s %s\n\n%s\n\n"
            "Баланс: $%.2f\nСтавка: $%.2f (13%%)\nПотенційно: +$%.2f\n\n%s"
        ) % ("▲" if dec=="UP" else "▼", dec, bet.get("mkt","Polymarket"),
             bal, amount, bet.get("pot",0), logic))
        print("[Auto] OK uid=%d bet_id=%s $%.2f" % (s.uid, bet_id, amount))
    else:
        await app.bot.send_message(chat_id=s.uid, text="Ставка не виконана\n%s" % bet["err"])

# ─────────────────────────────────────────────
# ЦИКЛ
# ─────────────────────────────────────────────
async def cycle(app, s):
    # Спочатку закриваємо відкриті позиції
    if s.ok and s.open_bets:
        await check_open_bets(app, s)

    # Оновлюємо outcomes попередніх сигналів
    check_outcomes(s)

    # Збираємо дані і аналізуємо — незалежно від попереднього
    p = build_payload(s)
    if not p:
        await app.bot.send_message(chat_id=s.uid, text="Помилка даних Binance."); return

    result = analyze_with_ai(p, s)
    if not result:
        await app.bot.send_message(chat_id=s.uid, text="Помилка AI."); return

    dec      = result.get("decision","UP")
    strength = result.get("strength","LOW")
    logic    = result.get("logic","")
    score    = result.get("confidence_score",0)
    reasons  = result.get("reasons",[])
    key_sig  = result.get("key_signal","")
    mkt_cond = result.get("market_condition", p["ctx"]["reg"])
    ad=p["amd"]; mn=p["manip"]; sw15=p["liq"].get("sw15",{})

    sig={
        "dec":dec,"strength":strength,"confidence_score":score,"logic":logic,
        "reasons":reasons,"key_signal":key_sig,"entry":p["price"]["cur"],
        "time":p["ts"],"ts_unix":p["ts_unix"],"outcome":None,
        "st15m":p["struct"]["15m"],"mkt_cond":mkt_cond,"session":p["ctx"]["sess"],
        "sweep_type":sw15.get("type","NONE"),"amd_phase":ad.get("phase","NONE"),"trap":mn["trap"],
    }
    s.signals.append(sig); s.save()

    try:
        dump=[]
        if os.path.exists(DUMP_FILE):
            with open(DUMP_FILE) as f: dump=json.load(f)
        dump.append(sig); dump=dump[-2000:]
        with open(DUMP_FILE,"w") as f: json.dump(dump,f,ensure_ascii=False,indent=2)
    except: pass

    arrow    = "▲" if dec=="UP" else "▼"
    reas_s   = "\n".join("· "+r for r in reasons[:3]) if reasons else ""
    consec   = p["ctx"]["consec_count"]
    warn     = "\n⚠️ %d однакових сигналів підряд" % consec if consec >= 3 else ""
    main_txt = (
        "СИГНАЛ  %s %s  |  %s  |  Score:%+d\n\n"
        "$%.2f  ·  %s  ·  %s%s\n\n"
        "%s\n\n%s\n\n%s"
    ) % (arrow, dec, strength, score,
         p["price"]["cur"], mkt_cond, p["ctx"]["sess"], warn,
         key_sig, logic, reas_s)

    print("[Cycle] uid=%d auto=%s dec=%s str=%s consec=%d sess=%s" % (
        s.uid, s.auto, dec, strength, consec, p["ctx"]["sess"]))

    # Авто-Азія
    asia = is_asia_session()
    if asia and not s.auto and s.ok:
        s._asia_auto = True; s.auto = True
        await app.bot.send_message(chat_id=s.uid,
            text="Азія сесія розпочалась — авто-торгівля увімкнена (00:00–06:00 Київ)")

    if not asia and s.auto and getattr(s,"_asia_auto",False):
        s._asia_auto = False; s.auto = False
        await app.bot.send_message(chat_id=s.uid,
            text="Азія сесія завершилась — авто-торгівля вимкнена")

    await app.bot.send_message(chat_id=s.uid, text=main_txt)

    if s.auto:
        await auto_trade(app, s, p, result)
    else:
        s.pending = {"dir":dec,"ts":time.time(),"price":p["price"]["cur"]}
        bal,_ = get_balance(s); bet = s.bet_size(bal)
        ikb = InlineKeyboardMarkup([[
            InlineKeyboardButton("Так", callback_data="confirm_%s"%dec),
            InlineKeyboardButton("Ні",  callback_data="skip")]])
        hint = (" (рекомендовано $%.2f = 13%%)"%bet) if bal else ""
        await app.bot.send_message(chat_id=s.uid,
            text=main_txt+"\n\nВведи суму USDC"+hint+":", reply_markup=ikb)

# ─────────────────────────────────────────────
# ПЛАНУВАЛЬНИК
# ─────────────────────────────────────────────
async def position_watcher(app):
    while True:
        await asyncio.sleep(30)
        for uid,s in list(_sessions.items()):
            if s.ok and s.open_bets:
                try: await check_open_bets(app, s)
                except Exception as e: log.error("[Watcher] uid=%d: %s", uid, e)

async def scheduler(app):
    while True:
        now = datetime.datetime.now(datetime.timezone.utc)
        m2n = 15 - (now.minute % 15)
        if m2n == 15: m2n = 0
        nxt = now.replace(second=2, microsecond=0) + datetime.timedelta(minutes=m2n)
        if nxt <= now: nxt += datetime.timedelta(minutes=15)
        wait = (nxt - now).total_seconds()
        log.info("Наступний цикл через %.0fs о %s UTC", wait, nxt.strftime("%H:%M"))
        await asyncio.sleep(wait)
        await asyncio.sleep(5)
        if _sessions:
            for uid,s in list(_sessions.items()):
                try: await cycle(app, s)
                except Exception as e: log.error("Цикл uid=%d: %s", uid, e)

# ─────────────────────────────────────────────
# ЗАПУСК
# ─────────────────────────────────────────────
def main():
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    for cmd,fn in [("start",cmd_start),("stats",cmd_stats),
                   ("analyze",cmd_analyze),("autoon",cmd_autoon),("autooff",cmd_autooff)]:
        app.add_handler(CommandHandler(cmd, fn))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))

    async def startup(app):
        asyncio.create_task(scheduler(app))
        asyncio.create_task(position_watcher(app))
        asyncio.create_task(minute_tracker(app))
        log.info("BTC Polymarket Bot v3. Fresh analysis every cycle. No memory between signals.")

    app.post_init = startup
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__=="__main__":
    main()
