import asyncio, logging, json, time, datetime, os, csv, io, requests
from openai import OpenAI
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters

# ============================================================
# ТІЛЬКИ ЦІ 2 ЗМІННІ ПОТРІБНІ В RAILWAY
# ============================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY", "")
AUTO_MIN_STRENGTH  = "LOW"  # торгуємо всі сигнали

OI_CACHE_FILE = "oi_cache.json"
SIGNALS_DUMP  = "signals_dump.json"

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# SESSION
# ============================================================
class UserSession:
    def __init__(self, uid):
        self.uid             = uid
        self.private_key     = ""   # Magic.Link ключ від polymarket.com
        self.wallet_address  = None
        self.wallet_ok       = False
        self.poly_ok         = False
        self.auto_active     = False
        self.signal_history  = []
        self.trade_history   = []
        self.pending_trade   = {}
        self.wallet_state    = None  # "await_key"
        self.tmp_key         = None
        self.history_file    = "history_%d.json" % uid
        self.errors_file     = "errors_%d.json"  % uid
        self._load()

    def _load(self):
        try:
            if os.path.exists(self.history_file):
                with open(self.history_file) as f:
                    self.signal_history = json.load(f)
        except Exception: pass

    def save_history(self):
        try:
            with open(self.history_file, "w") as f:
                json.dump(self.signal_history, f, ensure_ascii=False, indent=2)
        except Exception: pass

    def save_error(self, sig):
        try:
            e = []
            if os.path.exists(self.errors_file):
                with open(self.errors_file) as f: e = json.load(f)
            e.append(sig); e = e[-300:]
            with open(self.errors_file, "w") as f: json.dump(e, f, ensure_ascii=False, indent=2)
        except Exception: pass

    def calc_bet(self, balance):
        if not balance or balance <= 0: return 0.0
        return round(max(1.0, min(balance * 0.10, 500.0)), 2)

_sessions = {}
def get_session(uid):
    if uid not in _sessions:
        _sessions[uid] = UserSession(uid)
    return _sessions[uid]

# ============================================================
# WALLET — Magic.Link
# ============================================================
def connect_wallet(s, private_key: str):
    """
    Підключення через Magic.Link ключ від Polymarket.
    polymarket.com → Profile → Export Private Key
    Не потрібен MetaMask, RPC або MATIC для газу.
    """
    try:
        key = private_key.strip().replace(" ", "").replace("\n", "").replace("\r", "")
        clean = key.lower()
        if clean.startswith("0x"):
            clean = clean[2:]
        if len(clean) != 64:
            return False, "Key must be 64 hex chars (got %d).\nGet it from: polymarket.com → Profile → Export Private Key" % len(clean)
        try:
            int(clean, 16)
        except ValueError:
            return False, "Key contains invalid characters"

        s.private_key = "0x" + clean
        s.wallet_ok   = True

        # Адреса
        try:
            from eth_account import Account
            s.wallet_address = Account.from_key(s.private_key).address
        except Exception:
            s.wallet_address = "0x" + clean[:8] + "..."

        s.poly_ok = True
        print("[Wallet] Connected: %s" % s.wallet_address)
        return True, s.wallet_address
    except Exception as e:
        return False, str(e)

# ============================================================
# BALANCE — через CLOB API
# ============================================================
def get_balance(s):
    if not s.wallet_ok:
        return None, "Wallet not connected"
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.constants import POLYGON
        key = s.private_key
        client = ClobClient(host="https://clob.polymarket.com",
                            key=key, chain_id=POLYGON, signature_type=0)
        try:    creds = client.derive_api_key()
        except: creds = client.create_api_key()
        client.set_api_creds(creds)
        info = client.get_balance_allowance(params={"asset_type": "USDC"})
        bal  = round(float(info.get("balance", 0)) / 1e6, 2)
        return bal, s.wallet_address or "Polymarket"
    except ImportError:
        pass
    except Exception as e:
        print("[Balance] CLOB error: %s" % e)

    # Fallback: Polygon RPC
    try:
        from eth_account import Account
        addr = Account.from_key(s.private_key).address
        s.wallet_address = addr
        for usdc in ["0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
                     "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"]:
            data    = "0x70a08231000000000000000000000000" + addr[2:].lower()
            payload = {"jsonrpc":"2.0","method":"eth_call",
                       "params":[{"to":usdc,"data":data},"latest"],"id":1}
            for rpc in ["https://polygon-rpc.com","https://polygon.drpc.org"]:
                try:
                    r   = requests.post(rpc, json=payload, timeout=10)
                    res = r.json().get("result","")
                    if res and res not in ("0x","0x0",""):
                        bal = round(int(res,16)/1e6, 2)
                        if bal > 0: return bal, addr
                except Exception: continue
        return 0.0, addr
    except Exception as e:
        return None, str(e)

# ============================================================
# POLYMARKET MARKET FINDER
# ============================================================
def find_btc_market():
    """
    Знаходить поточний раунд "Bitcoin Up or Down - 15 min"
    через slug: btc-updown-15m-{timestamp}
    Токени беремо через CLOB по condition_id.
    """
    SLUG_PREFIX = "btc-updown-15m-"
    ROUND_SEC   = 900

    def parse_end(m):
        for f in ("end_date_iso","endDate","endDateIso","end_time","endTime","end_date"):
            v = m.get(f)
            if not v: continue
            try:
                return float(datetime.datetime.fromisoformat(
                    str(v).replace("Z","+00:00")).timestamp())
            except Exception:
                try: return float(v)
                except Exception: pass
        return None

    def get_tokens(cid):
        try:
            r = requests.get("https://clob.polymarket.com/markets/%s" % cid, timeout=15)
            if r.status_code != 200: return None, None, 0.5, 0.5
            tokens = r.json().get("tokens", [])
            ty_id = tn_id = ""
            ty_pr = tn_pr = 0.5
            for t in tokens:
                oc  = (t.get("outcome") or "").upper().strip()
                tid = (t.get("token_id") or t.get("tokenId") or "").strip()
                pr  = float(t.get("price", 0.5) or 0.5)
                if oc in ("YES","UP","HIGHER","ABOVE"): ty_id=tid; ty_pr=pr
                elif oc in ("NO","DOWN","LOWER","BELOW"): tn_id=tid; tn_pr=pr
            if not ty_id and tokens:
                ty_id = (tokens[0].get("token_id") or tokens[0].get("tokenId") or "").strip()
                ty_pr = float(tokens[0].get("price",0.5) or 0.5)
            if not tn_id and len(tokens)>1:
                tn_id = (tokens[1].get("token_id") or tokens[1].get("tokenId") or "").strip()
                tn_pr = float(tokens[1].get("price",0.5) or 0.5)
            return ty_id, tn_id, ty_pr, tn_pr
        except Exception as e:
            print("[Market] CLOB token error: %s" % e)
            return None, None, 0.5, 0.5

    def try_slug(slug, now_ts):
        try:
            r = requests.get("https://gamma-api.polymarket.com/events",
                             params={"slug": slug}, timeout=15)
            if r.status_code != 200: return None
            raw    = r.json()
            events = raw if isinstance(raw,list) else [raw] if (isinstance(raw,dict) and raw) else []
            if not events: return None
            ev     = events[0]
            title  = ev.get("title","") or slug
            print("[Market] Event: %s" % title[:60])
            for m in ev.get("markets",[]):
                if m.get("closed",True): continue
                cid = (m.get("conditionId") or m.get("condition_id") or m.get("id") or "").strip()
                if not cid: continue
                end_ts = parse_end(m)
                diff   = (end_ts - now_ts) if end_ts else 900.0
                if diff <= 0: continue
                ty_id, tn_id, ty_pr, tn_pr = get_tokens(cid)
                if not ty_id or not tn_id:
                    print("[Market] No tokens for %s" % cid[:20]); continue
                q = m.get("question","") or title
                print("[MARKET] FOUND: %s" % q[:60])
                print("[TOKENS] YES=%s NO=%s" % (ty_id[:20], tn_id[:20]))
                return {"condition_id":cid,"token_id_yes":ty_id,"token_id_no":tn_id,
                        "price_yes":ty_pr,"price_no":tn_pr,"question":q,"diff_sec":round(diff,1)}
        except Exception as e:
            print("[Market] slug error: %s" % e)
        return None

    def scan():
        now_ts  = time.time()
        current = int(now_ts // ROUND_SEC) * ROUND_SEC
        print("[Market] Searching at %s UTC" %
              datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S"))
        for ts in [current, current + ROUND_SEC, current - ROUND_SEC]:
            slug   = "%s%d" % (SLUG_PREFIX, ts)
            print("[Market] Trying: %s" % slug)
            result = try_slug(slug, now_ts)
            if result: return result
        return None

    for attempt in range(1, 8):
        print("[Market] Attempt %d/7" % attempt)
        result = scan()
        if result: return result
        if attempt < 7:
            print("[Market] Retry in 3s...")
            time.sleep(3)
    print("[Market] NOT FOUND")
    return None

# ============================================================
# PLACE BET — Magic.Link signature_type=0
# ============================================================
def place_bet(s, direction: str, amount: float) -> dict:
    """
    Ставка через Polymarket CLOB API.
    Magic.Link акаунт: signature_type=0
    Не потрібен MATIC — Polymarket платить газ сам.
    """
    if not s.wallet_ok:
        return {"success":False,"error":"Wallet not connected"}
    if amount < 1:
        return {"success":False,"error":"Min $1"}

    market = find_btc_market()
    if not market:
        return {"success":False,"error":"No active BTC market found"}

    token_id = market["token_id_yes"] if direction=="UP" else market["token_id_no"]
    price    = market["price_yes"]    if direction=="UP" else market["price_no"]
    price    = max(0.01, min(0.99, float(price)))
    size     = round(amount / price, 2)

    print("[BET] dir=%s token=%s price=%.4f size=%.2f" % (
        direction, token_id[:20], price, size))

    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OrderArgs, OrderType, Side
        from py_clob_client.constants import POLYGON

        client = ClobClient(
            host="https://clob.polymarket.com",
            key=s.private_key,
            chain_id=POLYGON,
            signature_type=0,  # Magic.Link
        )

        try:    creds = client.derive_api_key()
        except: creds = client.create_api_key()
        client.set_api_creds(creds)
        s.poly_ok = True

        try:
            fresh = float(client.get_midpoint(token_id) or price)
            if 0.01 <= fresh <= 0.99:
                price = fresh; size = round(amount/price, 2)
                print("[BET] Fresh price: %.4f" % price)
        except Exception: pass

        order = client.create_order(OrderArgs(
            token_id=token_id, price=price, size=size, side=Side.BUY))
        resp = client.post_order(order, OrderType.GTC)
        print("[BET] OK: %s" % str(resp)[:100])
        return {"success":True,"order":resp,"price":price,
                "pot":round(size-amount,2),"market_name":market["question"][:60]}

    except ImportError:
        return {"success":False,"error":"pip install py-clob-client"}
    except Exception as e:
        err = str(e)
        print("[BET] FAIL: %s" % err)
        if "insufficient" in err.lower():
            return {"success":False,"error":"Insufficient USDC. Top up on polymarket.com"}
        if "allowance" in err.lower() or "approve" in err.lower():
            return {"success":False,"error":"Allowance error. Contact support."}
        if "401" in err or "invalid" in err.lower():
            return {"success":False,"error":"Auth error: %s" % err[:100]}
        return {"success":False,"error":err[:200]}

# ============================================================
# BINANCE DATA
# ============================================================
def safe_get(url, params=None, timeout=15):
    try: return requests.get(url, params=params, timeout=timeout).json()
    except Exception: return None

def fetch_candles(interval, limit):
    for _ in range(3):
        d = safe_get("https://fapi.binance.com/fapi/v1/klines",
                     {"symbol":"BTCUSDT","interval":interval,"limit":limit})
        if d and isinstance(d,list): break
        time.sleep(2)
    if not d or not isinstance(d,list):
        d = safe_get("https://api.binance.com/api/v3/klines",
                     {"symbol":"BTCUSDT","interval":interval,"limit":limit})
    if not d or not isinstance(d,list): return []
    return [{"t":int(k[0]),"o":float(k[1]),"h":float(k[2]),
             "l":float(k[3]),"c":float(k[4]),"v":float(k[5])} for k in d]

def fetch_price():
    for url,p in [("https://fapi.binance.com/fapi/v1/ticker/price",{"symbol":"BTCUSDT"}),
                  ("https://api.binance.com/api/v3/ticker/price",  {"symbol":"BTCUSDT"})]:
        d = safe_get(url,p)
        if d and "price" in d: return float(d["price"])
    return None

def fetch_funding():
    d = safe_get("https://fapi.binance.com/fapi/v1/premiumIndex",{"symbol":"BTCUSDT"})
    if not d or not isinstance(d,dict): return {"rate":0.0,"sentiment":"NEUTRAL","mark":0.0,"basis":0.0}
    fr=float(d.get("lastFundingRate",0)); mark=float(d.get("markPrice",0)); idx=float(d.get("indexPrice",mark))
    return {"rate":fr,"sentiment":"LONGS_TRAPPED" if fr>0.0005 else "SHORTS_TRAPPED" if fr<-0.0003 else "NEUTRAL",
            "mark":mark,"basis":round(mark-idx,2)}

def fetch_liquidations():
    d=safe_get("https://fapi.binance.com/fapi/v1/forceOrders",{"symbol":"BTCUSDT","limit":200})
    if not d or isinstance(d,dict): d=safe_get("https://fapi.binance.com/fapi/v1/allForceOrders",{"symbol":"BTCUSDT","limit":200})
    if not d or not isinstance(d,list): return {"liq_longs":0.0,"liq_shorts":0.0,"signal":"NEUTRAL","exhaustion":False,"total_usd":0.0}
    cutoff=int(time.time()*1000)-900000
    recent=[x for x in d if isinstance(x,dict) and int(x.get("time",0))>=cutoff] or d[:50]
    ll=sum(float(x.get("origQty",0))*float(x.get("price",0)) for x in recent if x.get("side")=="SELL")
    ls=sum(float(x.get("origQty",0))*float(x.get("price",0)) for x in recent if x.get("side")=="BUY")
    return {"liq_longs":round(ll,2),"liq_shorts":round(ls,2),
            "signal":"SHORT_SQUEEZE_FUEL" if ls>ll*2 else "LONG_CASCADE_FUEL" if ll>ls*2 else "NEUTRAL",
            "exhaustion":(ll+ls)>5_000_000,"total_usd":round(ll+ls,2)}

def fetch_oi():
    d=safe_get("https://fapi.binance.com/fapi/v1/openInterest",{"symbol":"BTCUSDT"})
    if not d or not isinstance(d,dict): return 0.0,0.0
    cur=float(d.get("openInterest",0))
    try:
        prev=cur
        if os.path.exists(OI_CACHE_FILE):
            with open(OI_CACHE_FILE) as f: prev=json.load(f).get("oi",cur)
        with open(OI_CACHE_FILE,"w") as f: json.dump({"oi":cur,"ts":int(time.time())},f)
        return cur,round((cur-prev)/prev*100,4) if prev>0 else 0.0
    except Exception: return cur,0.0

def fetch_orderbook():
    try:
        d=safe_get("https://fapi.binance.com/fapi/v1/depth",{"symbol":"BTCUSDT","limit":20})
        if not d or not isinstance(d,dict): return {"imbalance":0.0,"bias":"NEUTRAL"}
        bids=sum(float(b[1]) for b in d.get("bids",[])[:10])
        asks=sum(float(a[1]) for a in d.get("asks",[])[:10])
        total=bids+asks; imb=round((bids-asks)/total*100,2) if total>0 else 0.0
        return {"imbalance":imb,"bias":"BID_HEAVY" if imb>20 else "ASK_HEAVY" if imb<-20 else "BALANCED"}
    except Exception: return {"imbalance":0.0,"bias":"NEUTRAL"}

def fetch_lsr():
    try:
        d=safe_get("https://fapi.binance.com/futures/data/topLongShortPositionRatio",
                   {"symbol":"BTCUSDT","period":"15m","limit":3})
        if not d or not isinstance(d,list): return {"ratio":1.0,"long_pct":50.0,"bias":"NEUTRAL"}
        lat=d[-1]; ratio=float(lat.get("longShortRatio",1.0)); lp=float(lat.get("longAccount",0.5))*100
        return {"ratio":round(ratio,3),"long_pct":round(lp,1),
                "bias":"CROWD_LONG" if ratio>1.5 else "CROWD_SHORT" if ratio<0.7 else "NEUTRAL"}
    except Exception: return {"ratio":1.0,"long_pct":50.0,"bias":"NEUTRAL"}

# ============================================================
# SMC ENGINE
# ============================================================
def swing_points(c):
    sh,sl=[],[]
    for i in range(2,len(c)-2):
        h=c[i]["h"]
        if h>c[i-1]["h"] and h>c[i+1]["h"] and h>c[i-2]["h"] and h>c[i+2]["h"]: sh.append({"price":h,"idx":i})
        l=c[i]["l"]
        if l<c[i-1]["l"] and l<c[i+1]["l"] and l<c[i-2]["l"] and l<c[i+2]["l"]: sl.append({"price":l,"idx":i})
    return sh[-5:],sl[-5:]

def market_structure(c):
    sh,sl=swing_points(c)
    if len(sh)<2 or len(sl)<2: return "RANGING"
    hs=[x["price"] for x in sh]; ls=[x["price"] for x in sl]
    if all(hs[i]>hs[i-1] for i in range(1,len(hs))) and all(ls[i]>ls[i-1] for i in range(1,len(ls))): return "BULLISH"
    if all(hs[i]<hs[i-1] for i in range(1,len(hs))) and all(ls[i]<ls[i-1] for i in range(1,len(ls))): return "BEARISH"
    return "RANGING"

def liq_sweep(c):
    if len(c)<10: return {"type":"NONE","level":0.0,"ago":0}
    sh,sl=swing_points(c[:-3])
    for i,x in enumerate(reversed(c[-5:])):
        for s in reversed(sh):
            if x["h"]>s["price"] and x["c"]<s["price"]: return {"type":"HIGH","level":s["price"],"ago":i+1}
        for s in reversed(sl):
            if x["l"]<s["price"] and x["c"]>s["price"]: return {"type":"LOW","level":s["price"],"ago":i+1}
    return {"type":"NONE","level":0.0,"ago":0}

def stop_clusters(c,price):
    sh,sl=swing_points(c)
    above=[{"price":s["price"],"type":"swing_high"} for s in sh if s["price"]>price]
    below=[{"price":s["price"],"type":"swing_low"}  for s in sl if s["price"]<price]
    sa=min(above,key=lambda x:x["price"]-price) if above else None
    sb=min(below,key=lambda x:price-x["price"]) if below else None
    return sa,sb

def find_fvg(c,price):
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

def bos_choch(c,htf):
    if len(c)<5: return None
    sh,sl=swing_points(c[:-1])
    if not sh or not sl: return None
    cl=c[-1]["c"]
    if cl>sh[-1]["price"]: return {"type":"CHoCH" if htf=="BEARISH" else "BOS","dir":"UP","level":sh[-1]["price"]}
    if cl<sl[-1]["price"]: return {"type":"CHoCH" if htf=="BULLISH" else "BOS","dir":"DOWN","level":sl[-1]["price"]}
    return None

def detect_amd(c15,c5m,price):
    if len(c15)<20: return {"phase":"NONE","direction":None,"confidence":0,"reason":""}
    last10=c15[-10:]; last3=c15[-3:]; l3_5=c5m[-3:] if len(c5m)>=3 else []
    rng=(max(x["h"] for x in last10)-min(x["l"] for x in last10))/price*100
    avg_b=sum(abs(x["c"]-x["o"]) for x in last10)/len(last10)/price*100
    is_acc=rng<0.6 and avg_b<0.10
    sw15=liq_sweep(c15); sw5=liq_sweep(c5m) if len(c5m)>=10 else {"type":"NONE","level":0.0,"ago":0}
    sweep=sw15 if sw15["type"]!="NONE" and sw15["ago"]<=4 else sw5
    manip=sweep["type"]!="NONE" and sweep["ago"]<=4
    mc=None
    if l3_5:
        m=(l3_5[-1]["c"]-l3_5[0]["o"])/l3_5[0]["o"]*100
        mc="UP" if m>0.05 else "DOWN" if m<-0.05 else None
    lm=(last3[-1]["c"]-last3[0]["o"])/last3[0]["o"]*100
    if manip and is_acc:
        if sweep["type"]=="LOW":
            return {"phase":"MANIPULATION_DONE","direction":"UP","confidence":3 if mc=="UP" else 2,"reason":"ACCUM+SWEEP_LOW UP"}
        lc=c15[-1]["c"]
        if lc<sweep["level"]*0.9998:
            return {"phase":"MANIPULATION_DONE","direction":"DOWN","confidence":3 if mc=="DOWN" else 2,"reason":"SWEEP_HIGH+CLOSE_BELOW DOWN"}
        return {"phase":"MANIPULATION_DONE","direction":"UP","confidence":1,"reason":"SWEEP_HIGH+HOLDS UP"}
    if is_acc: return {"phase":"ACCUMULATION","direction":None,"confidence":1,"reason":"tight range"}
    if manip:
        if sweep["type"]=="LOW": d="UP"; r="SWEEP_LOW UP"
        else:
            lc=c15[-1]["c"]
            d="DOWN" if lc<sweep["level"]*0.9998 else "UP"
            r="SWEEP_HIGH DOWN" if d=="DOWN" else "SWEEP_HIGH HOLDS UP"
        return {"phase":"MANIPULATION","direction":d,"confidence":2,"reason":r}
    if abs(lm)>0.15:
        return {"phase":"DISTRIBUTION","direction":"UP" if lm>0 else "DOWN","confidence":1,"reason":"active move"}
    return {"phase":"NONE","direction":None,"confidence":0,"reason":""}

def detect_manip(c,sweep,price):
    r={"trap_type":"NONE","reversal_signal":None}
    if len(c)<5: return r
    last=c[-1]; body=abs(last["c"]-last["o"]); total=last["h"]-last["l"]
    if total>0:
        uw=last["h"]-max(last["c"],last["o"]); lw=min(last["c"],last["o"])-last["l"]
        wr=1-(body/total)
        if wr>0.7 and total/last["c"]>0.002:
            if uw>lw*2: r.update({"trap_type":"WICK_TRAP_HIGH","reversal_signal":"DOWN"})
            elif lw>uw*2: r.update({"trap_type":"WICK_TRAP_LOW","reversal_signal":"UP"})
    if sweep["type"]!="NONE" and sweep["ago"]<=3:
        if sweep["type"]=="HIGH" and last["c"]<sweep["level"]*0.9995: r.update({"trap_type":"SWEEP_TRAP_HIGH","reversal_signal":"DOWN"})
        elif sweep["type"]=="LOW" and last["c"]>sweep["level"]*1.0005: r.update({"trap_type":"SWEEP_TRAP_LOW","reversal_signal":"UP"})
    return r

def classify_vol(c15):
    if len(c15)<10: return "UNKNOWN",0.0
    ranges=[(x["h"]-x["l"])/x["c"]*100 for x in c15[-10:]]
    avg=sum(ranges)/len(ranges); rec=sum(ranges[-3:])/3; pri=sum(ranges[:7])/7
    return ("LOW_VOL" if avg<0.08 else "EXPANSION" if rec>pri*1.5 else "HIGH_VOL" if avg>0.3 else "NORMAL"),round(avg,4)

def classify_session():
    h=datetime.datetime.now(datetime.timezone.utc).hour
    if 7<=h<12:    return "LONDON",1
    elif 12<=h<17: return "NY_OPEN",0
    elif 17<=h<21: return "NY_AFTERNOON",0
    elif 21<=h or h<3: return "ASIA_ACTIVE",0
    return "DEAD_HOURS",-1

def classify_mkt(c15,c5m):
    if len(c15)<10: return "RANGING"
    closes=[x["c"] for x in c15[-12:]]
    ups=sum(1 for i in range(1,len(closes)) if closes[i]>closes[i-1])
    if ups>=9 or (len(closes)-1-ups)>=9: return "TRENDING"
    alts=sum(1 for i in range(1,len(closes)-1) if (closes[i]>closes[i-1])!=(closes[i+1]>closes[i]))
    return "CHOPPY" if alts>=8 else "RANGING"

def get_payload(s):
    c15=fetch_candles("15m",100); c5m=fetch_candles("5m",50); c1m=fetch_candles("1m",30)
    if not c15: return None
    price=c15[-1]["c"]; prev=c15[-2]["c"] if len(c15)>=2 else price
    chg15=round((price-prev)/prev*100,4)
    chg5=round((c5m[-1]["c"]-c5m[-4]["c"])/c5m[-4]["c"]*100,4) if len(c5m)>=4 else 0.0
    mom3=round((c15[-1]["c"]-c15[-4]["c"])/c15[-4]["c"]*100,4) if len(c15)>=4 else 0.0
    mic=round((c1m[-1]["c"]-c1m[-4]["c"])/c1m[-4]["c"]*100,4) if len(c1m)>=4 else 0.0
    st15=market_structure(c15) if len(c15)>=6 else "RANGING"
    st5=market_structure(c5m) if len(c5m)>=6 else "RANGING"
    st1=market_structure(c1m) if len(c1m)>=6 else "RANGING"
    sw15=liq_sweep(c15); sw5=liq_sweep(c5m) if c5m else {"type":"NONE","level":0.0,"ago":0}
    sw1=liq_sweep(c1m) if c1m else {"type":"NONE","level":0.0,"ago":0}
    sa,sb=stop_clusters(c15,price)
    f5a,f5b=find_fvg(c5m[-30:],price) if len(c5m)>=5 else (None,None)
    f1a,f1b=find_fvg(c1m[-20:],price) if len(c1m)>=5 else (None,None)
    bc5=bos_choch(c5m,st15) if len(c5m)>=5 else None
    manip=detect_manip(c5m[-10:] if len(c5m)>=10 else c15[-10:],sw5,price)
    amd=detect_amd(c15,c5m,price)
    fund=fetch_funding(); liqs=fetch_liquidations(); oi,oi_chg=fetch_oi()
    ob=fetch_orderbook(); lsr=fetch_lsr()
    vc,vs=classify_vol(c15); sess,sb2=classify_session(); mkt=classify_mkt(c15,c5m)
    da=round((sa["price"]-price)/price*100,4) if sa else 999.0
    db=round((price-sb["price"])/price*100,4) if sb else 999.0
    last_s="no_prev"
    if s.signal_history:
        last=s.signal_history[-1]
        last_s="prev=%s outcome=%s"%(last.get("decision","?"),last.get("outcome","PENDING"))
    return {
        "timestamp":datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "ts_unix":int(time.time()),
        "price":{"current":price,"chg_15m":chg15,"chg_5m":chg5,"momentum_3":mom3,"micro_mom":mic,"mark":fund["mark"],"basis":fund["basis"]},
        "structure":{"15m":st15,"5m":st5,"1m":st1},
        "liquidity":{"sweep_15m":sw15,"sweep_5m":sw5,"sweep_1m":sw1,"stops_above":sa,"stops_below":sb,
                     "dist_above":da,"dist_below":db,"fvg5_above":f5a,"fvg5_below":f5b,
                     "fvg1_above":f1a,"fvg1_below":f1b,"bos_choch_5m":bc5},
        "amd":amd,"manipulation":manip,
        "positioning":{"funding_rate":fund["rate"],"funding_sent":fund["sentiment"],
                       "liq_longs":liqs["liq_longs"],"liq_shorts":liqs["liq_shorts"],
                       "liq_signal":liqs["signal"],"exhaustion":liqs["exhaustion"],"liq_total":liqs["total_usd"],
                       "oi":oi,"oi_change":oi_chg,"ob_bias":ob["bias"],"ob_imbalance":ob["imbalance"],
                       "lsr_bias":lsr["bias"],"lsr_ratio":lsr["ratio"],"crowd_long":lsr["long_pct"]},
        "context":{"volatility":vc,"vol_score":vs,"session":sess,"session_boost":sb2,"market_condition":mkt,"last_signal":last_s},
    }

# ============================================================
# AI
# ============================================================
SYSTEM_PROMPT="""You are an elite BTC short-term trader. Predict UP or DOWN in 15 minutes on Polymarket.
Analyze from scratch. Do NOT copy previous signal.
TIMEFRAMES: 15m=context | 5m=tactics | 1m=execution
AMD: SWEEP_LOW+close_above=UP(+3) | SWEEP_HIGH+close_below=DOWN(+3) | SWEEP_HIGH+holds=UP(+1)
SCORING: +3AMD | +2CHoCH/BOS/SQUEEZE | -2CASCADE | +1/-1 FVG/session
STRENGTH: >=5=HIGH | 3-4=MEDIUM | 1-2=LOW | ALWAYS UP or DOWN
OUTPUT JSON: {"decision":"UP or DOWN","strength":"HIGH or MEDIUM or LOW","confidence_score":<int>,
"market_condition":"TRENDING or RANGING or CHOPPY","key_signal":"one sentence",
"logic":"2-3 sentences Ukrainian","reasons":["r1","r2","r3"],"risk_note":"risk or NONE"}"""

def analyze_with_ai(payload,s):
    try:
        client=OpenAI(api_key=OPENAI_API_KEY)
        liq=payload["liquidity"]; pos=payload["positioning"]; pr=payload["price"]
        st=payload["structure"]; ctx=payload["context"]; manip=payload["manipulation"]; amd=payload["amd"]
        sw15=liq.get("sweep_15m",{}); sw5=liq.get("sweep_5m",{}); sw1=liq.get("sweep_1m",{})
        bc5=liq.get("bos_choch_5m"); f5a=liq.get("fvg5_above"); f5b=liq.get("fvg5_below")
        sa=liq.get("stops_above") or {}; sb_=liq.get("stops_below") or {}
        msg=(
            "Time:%s Sess:%s(+%d) Last:%s\n"
            "PRICE:$%.2f 15m:%+.4f%% 5m:%+.4f%% Mom3:%+.4f%% Micro:%+.4f%%\n"
            "STRUCT:15m=%s 5m=%s 1m=%s Mkt:%s Vol:%s\n"
            "AMD:%s->%s conf=%d [%s]\n"
            "Sw15m:%s@%.2f(%dc) Sw5m:%s@%.2f(%dc) Sw1m:%s@%.2f(%dc)\n"
            "Up:%.3f%% Dn:%.3f%% BOS5m:%s\n"
            "FVG5:up=%s dn=%s Trap:%s hint=%s\n"
            "Fund:%+.6f(%s) LiqL:$%.0f LiqS:$%.0f Sig:%s\n"
            "OI:%+.4f%% Book:%s(%+.1f%%) L/S:%.3f(%s) CrowdL:%.1f%%"
        )%(
            payload["timestamp"],ctx["session"],ctx["session_boost"],ctx["last_signal"],
            pr["current"],pr["chg_15m"],pr["chg_5m"],pr["momentum_3"],pr["micro_mom"],
            st["15m"],st["5m"],st["1m"],ctx["market_condition"],ctx["volatility"],
            amd.get("phase","NONE"),amd.get("direction","?"),amd.get("confidence",0),amd.get("reason",""),
            sw15.get("type","N"),sw15.get("level",0),sw15.get("ago",0),
            sw5.get("type","N"),sw5.get("level",0),sw5.get("ago",0),
            sw1.get("type","N"),sw1.get("level",0),sw1.get("ago",0),
            liq.get("dist_above",999),liq.get("dist_below",999),
            ("%s %s@%.2f"%(bc5["type"],bc5["dir"],bc5["level"])) if bc5 else "none",
            ("%.3f%%"%f5a["dist"]) if f5a else "none",("%.3f%%"%f5b["dist"]) if f5b else "none",
            manip["trap_type"],str(manip["reversal_signal"]),
            pos["funding_rate"],pos["funding_sent"],pos["liq_longs"],pos["liq_shorts"],pos["liq_signal"],
            pos["oi_change"],pos["ob_bias"],pos["ob_imbalance"],pos["lsr_ratio"],pos["lsr_bias"],pos["crowd_long"]
        )
        resp=client.chat.completions.create(model="gpt-4o",
             messages=[{"role":"system","content":SYSTEM_PROMPT},{"role":"user","content":msg}],
             temperature=0.1,response_format={"type":"json_object"})
        return json.loads(resp.choices[0].message.content)
    except Exception as e:
        logger.error("AI: %s",e); return None

# ============================================================
# STATS
# ============================================================
def check_signals(s):
    now=int(time.time()); changed=False
    for sig in s.signal_history:
        if sig.get("outcome"): continue
        if now-sig.get("ts_unix",0)>=900:
            cur=fetch_price()
            if cur:
                entry=sig.get("entry_price",0); dec=sig.get("decision","")
                outcome="WIN" if (dec=="UP" and cur>entry) or (dec=="DOWN" and cur<entry) else "LOSS"
                sig["outcome"]=outcome; sig["exit_price"]=cur; sig["real_move"]=round(cur-entry,2)
                changed=True
                if outcome=="LOSS": s.save_error(sig)
    if changed: s.save_history()

def stats_text(s):
    if not s.signal_history: return "No data yet."
    checked=[g for g in s.signal_history if g.get("outcome")]
    if not checked: return "Checking results..."
    wins=[g for g in checked if g["outcome"]=="WIN"]; total=len(checked)
    wr=round(len(wins)/total*100,1)
    lines=["STATS","Total:%d WIN:%d Winrate:%.1f%%"%(total,len(wins),wr),""]
    for st in ("HIGH","MEDIUM","LOW"):
        sub=[g for g in checked if g.get("strength")==st]
        if sub:
            w=len([g for g in sub if g["outcome"]=="WIN"])
            lines.append("%s: %d/%d (%.1f%%)"%(st,w,len(sub),round(w/len(sub)*100,1)))
    return "\n".join(lines)

def get_news():
    try:
        d=safe_get("https://min-api.cryptocompare.com/data/v2/news/",{"categories":"BTC,Bitcoin","lTs":0})
        if d and "Data" in d:
            lines=[]; bkw=["bull","surge","rally","rise","gain","etf"]; skw=["bear","drop","crash","dump","ban"]
            for item in d["Data"][:5]:
                t=item.get("title","").lower(); p=sum(1 for k in bkw if k in t); n=sum(1 for k in skw if k in t)
                lines.append("[%s] %s"%("BULLISH" if p>n else "BEARISH" if n>p else "NEUTRAL",item.get("title","")[:70]))
            return "\n".join(lines)
    except Exception: pass
    return "News unavailable"

# ============================================================
# KEYBOARD
# ============================================================
def kb_main(s):
    w  = "✅ Wallet OK"  if s.wallet_ok   else "🔗 Connect Wallet"
    a  = "🔴 Auto OFF"  if s.auto_active  else "🟢 Auto ON"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(w, callback_data="wallet_connect"),
         InlineKeyboardButton(a, callback_data="toggle_auto")],
        [InlineKeyboardButton("💰 Balance",    callback_data="show_balance"),
         InlineKeyboardButton("📊 Stats",      callback_data="show_stats")],
        [InlineKeyboardButton("🔍 Analyze",    callback_data="analyze_now"),
         InlineKeyboardButton("📁 Export CSV", callback_data="download_log")],
        [InlineKeyboardButton("🏪 Test Market",callback_data="test_market"),
         InlineKeyboardButton("🔎 Raw Market", callback_data="raw_market")],
        [InlineKeyboardButton("📰 News",       callback_data="show_news"),
         InlineKeyboardButton("📉 Errors",     callback_data="show_errors")],
    ])

WELCOME=(
    "🤖 BTC Polymarket Bot\n\n"
    "Signals every 15min :00 :15 :30 :45 UTC\n"
    "Stake: 10% of Polymarket balance\n\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "SETUP (2 steps):\n\n"
    "1️⃣  Press 🔗 Connect Wallet\n"
    "     Enter your Polymarket key\n"
    "     (polymarket.com → Profile → Export Private Key)\n\n"
    "2️⃣  Press 🟢 Auto ON\n"
    "     Bot trades automatically\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "✅ No MetaMask needed\n"
    "✅ No MATIC for gas\n"
    "✅ Funds stay on Polymarket\n"
    "✅ Magic.Link account supported"
)

# ============================================================
# COMMANDS
# ============================================================
async def cmd_start(u,c):
    s=get_session(u.effective_user.id)
    await u.message.reply_text(WELCOME, reply_markup=kb_main(s))

async def cmd_help(u,c):
    s=get_session(u.effective_user.id)
    await u.message.reply_text(WELCOME, reply_markup=kb_main(s))

async def cmd_analyze(u,c):
    s=get_session(u.effective_user.id)
    await u.message.reply_text("🔍 Analyzing...")
    await run_cycle(c.application,s)

async def cmd_stats(u,c):
    s=get_session(u.effective_user.id)
    check_signals(s)
    await u.message.reply_text(stats_text(s))

async def cmd_autoon(u,c):
    s=get_session(u.effective_user.id)
    if not s.wallet_ok:
        await u.message.reply_text("First connect wallet. Press 🔗 Connect Wallet"); return
    s.auto_active=True
    bal,_=get_balance(s); bet=s.calc_bet(bal) if bal else 0.0
    await u.message.reply_text(
        "🟢 Auto ON\n\nBalance: %s\nStake: $%.2f (10%%)\n\nSignals :00 :15 :30 :45 UTC"%(
            ("$%.2f"%bal) if bal else "N/A", bet),
        reply_markup=kb_main(s))

async def cmd_autooff(u,c):
    s=get_session(u.effective_user.id)
    s.auto_active=False
    await u.message.reply_text("🔴 Auto OFF", reply_markup=kb_main(s))

async def cmd_balance(u,c):
    s=get_session(u.effective_user.id)
    await u.message.reply_text("Checking balance...")
    bal,info=get_balance(s)
    if bal is not None:
        bet=s.calc_bet(bal)
        await u.message.reply_text("💰 Balance: $%.2f USDC\n🎯 Next stake: $%.2f (10%%)"%(bal,bet))
    else:
        await u.message.reply_text("❌ %s"%info)

async def cmd_errors(u,c):
    s=get_session(u.effective_user.id)
    if not os.path.exists(s.errors_file):
        await u.message.reply_text("No errors yet."); return
    try:
        with open(s.errors_file) as f: errs=json.load(f)
    except Exception: await u.message.reply_text("Read error."); return
    if not errs: await u.message.reply_text("No errors yet."); return
    lines=["ERRORS (%d)"%len(errs),""]
    for i,e in enumerate(errs[-10:],1):
        lines.append("%d. %s %s $%.0f→$%.0f(%+.0f)\n   %s|%s\n   KEY: %s\n"%(
            i,e.get("decision","?"),e.get("strength","?"),
            e.get("entry_price",0),e.get("exit_price",0),e.get("real_move",0),
            e.get("st15m","?"),e.get("session","?"),e.get("key_signal","")[:80]))
    await u.message.reply_text("\n".join(lines)[:4000])

async def cmd_dump(u,c):
    if not os.path.exists(SIGNALS_DUMP):
        await u.message.reply_text("signals_dump.json is empty."); return
    try:
        with open(SIGNALS_DUMP,"rb") as f:
            await u.message.reply_document(document=f,filename="signals_dump.json",
                caption="%d bytes"%os.path.getsize(SIGNALS_DUMP))
    except Exception as e: await u.message.reply_text("Error: %s"%e)

# ============================================================
# CALLBACKS
# ============================================================
async def handle_callback(u,c):
    s=get_session(u.effective_user.id)
    q=u.callback_query; await q.answer()

    if q.data=="wallet_connect":
        s.wallet_state="await_key"; s.tmp_key=None
        await q.message.reply_text(
            "🔐 CONNECT WALLET\n\n"
            "Enter your Polymarket private key\n\n"
            "Where to find:\n"
            "polymarket.com → Profile (top right) →\n"
            "Developer → Export Private Key\n\n"
            "⚠️ This is your POLYMARKET key, not MetaMask!\n"
            "✅ No MATIC needed — Polymarket pays gas\n\n"
            "Enter key (64 hex chars):")

    elif q.data=="toggle_auto":
        if not s.wallet_ok:
            await q.message.reply_text("First connect wallet!"); return
        if s.auto_active:
            s.auto_active=False
            await q.message.reply_text("🔴 Auto OFF", reply_markup=kb_main(s))
        else:
            s.auto_active=True
            bal,_=get_balance(s); bet=s.calc_bet(bal) if bal else 0.0
            await q.message.reply_text(
                "🟢 Auto ON\n\nBalance: %s\nStake: $%.2f (10%%)\n\n:00 :15 :30 :45 UTC"%(
                    ("$%.2f"%bal) if bal else "N/A", bet),
                reply_markup=kb_main(s))

    elif q.data=="show_balance":
        bal,info=get_balance(s)
        if bal is not None:
            await q.message.reply_text("💰 $%.2f USDC\n🎯 Stake: $%.2f (10%%)"%(bal,s.calc_bet(bal)))
        else:
            await q.message.reply_text("❌ %s"%info)

    elif q.data=="show_stats":
        check_signals(s); await q.message.reply_text(stats_text(s))

    elif q.data=="analyze_now":
        await q.message.reply_text("🔍 Analyzing...")
        await run_cycle(c.application,s)

    elif q.data=="download_log":
        if not s.signal_history:
            await q.message.reply_text("No signals yet."); return
        buf=io.StringIO()
        import csv as _csv
        w=_csv.DictWriter(buf,fieldnames=["ts","decision","strength","outcome",
            "entry_price","exit_price","real_move","key_signal","session","mkt_cond"],
            extrasaction="ignore")
        w.writeheader()
        for sig in s.signal_history:
            row={k:sig.get(k,"") for k in ["decision","strength","outcome","entry_price",
                "exit_price","real_move","key_signal","session","mkt_cond"]}
            row["ts"]=sig.get("time",""); w.writerow(row)
        fname="signals_%s.csv"%datetime.datetime.now().strftime("%Y%m%d_%H%M")
        await q.message.reply_document(document=buf.getvalue().encode(),filename=fname,
                                       caption="%d records"%len(s.signal_history))

    elif q.data=="test_market":
        await q.message.reply_text("🔍 Searching market...")
        m=find_btc_market()
        if m:
            await q.message.reply_text(
                "✅ MARKET FOUND\n\nName: %s\n\ncondition_id:\n%s\n\n"
                "token_id YES:\n%s\n\ntoken_id NO:\n%s\n\n"
                "Price YES: %.4f\nPrice NO: %.4f\nCloses in: %.0f sec"%(
                    m["question"][:80],m["condition_id"],
                    m["token_id_yes"],m["token_id_no"],
                    m["price_yes"],m["price_no"],m["diff_sec"]))
        else:
            await q.message.reply_text("❌ MARKET NOT FOUND\nCheck Railway logs")

    elif q.data=="raw_market":
        await q.message.reply_text("Checking slugs...")
        now=time.time(); ROUND=900; prefix="btc-updown-15m-"
        current=int(now//ROUND)*ROUND; lines=[]
        for ts in [current-ROUND,current,current+ROUND]:
            slug="%s%d"%(prefix,ts)
            try:
                r=requests.get("https://gamma-api.polymarket.com/events",
                               params={"slug":slug},timeout=15)
                raw=r.json()
                evs=raw if isinstance(raw,list) else [raw] if isinstance(raw,dict) and raw else []
                if not evs or not evs[0]:
                    lines.append("empty: %s"%slug); continue
                ev=evs[0]; title=ev.get("title","?")
                mkts=ev.get("markets",[]); active=[m for m in mkts if not m.get("closed",True)]
                lines.append("SLUG: %s"%slug)
                lines.append("Title: %s"%title[:60])
                for m in active[:1]:
                    cid=m.get("conditionId") or m.get("id","?")
                    lines.append("cid: %s"%str(cid)[:40])
                    try:
                        rc=requests.get("https://clob.polymarket.com/markets/%s"%cid,timeout=15)
                        if rc.status_code==200:
                            tokens=rc.json().get("tokens",[])
                            lines.append("CLOB tokens: %d"%len(tokens))
                            for t in tokens:
                                oc=t.get("outcome","?"); pr=t.get("price","?")
                                tid=t.get("token_id") or t.get("tokenId","?")
                                lines.append("  %s: price=%s tid=%s"%(oc,pr,str(tid)[:20]))
                        else:
                            lines.append("CLOB HTTP %d"%rc.status_code)
                    except Exception as e:
                        lines.append("CLOB error: %s"%str(e)[:50])
                lines.append("")
            except Exception as e:
                lines.append("error: %s"%str(e)[:60])
        await q.message.reply_text("\n".join(lines)[:4000])

    elif q.data=="show_news":
        await q.message.reply_text("📰 News:\n\n%s"%get_news())

    elif q.data=="show_errors":
        s2=get_session(u.effective_user.id)
        if not os.path.exists(s2.errors_file):
            await q.message.reply_text("No errors yet."); return
        try:
            with open(s2.errors_file) as f: errs=json.load(f)
            if not errs: await q.message.reply_text("No errors yet."); return
            lines=["ERRORS (%d)"%len(errs),""]
            for i,e in enumerate(errs[-5:],1):
                lines.append("%d. %s %s\n   KEY: %s\n"%(
                    i,e.get("decision","?"),e.get("strength","?"),e.get("key_signal","")[:60]))
            await q.message.reply_text("\n".join(lines)[:4000])
        except Exception: await q.message.reply_text("Error reading errors file.")

    elif q.data=="skip":
        s.pending_trade={}; await q.edit_message_text("Skipped.")

    elif q.data.startswith("execute_"):
        parts=q.data.split("_"); direction=parts[1]; amount=float(parts[2])
        await q.edit_message_text("⏳ Placing $%.2f..."%amount)
        bet=place_bet(s,direction,amount)
        if bet["success"]:
            s.trade_history.append({"decision":direction,"amount":amount,
                "time":datetime.datetime.now(datetime.timezone.utc)})
            await c.bot.send_message(chat_id=u.effective_chat.id,
                text="✅ BET OK\n%s | %s\n$%.2f → +$%.2f"%(
                    direction,bet.get("market_name","Polymarket"),amount,bet.get("pot",0)))
        else:
            await c.bot.send_message(chat_id=u.effective_chat.id,text="❌ BET FAILED\n%s"%bet["error"])
        s.pending_trade={}

# ============================================================
# HANDLE MESSAGE
# ============================================================
async def handle_message(u,c):
    s=get_session(u.effective_user.id); txt=u.message.text.strip()

    if s.wallet_state=="await_key":
        clean=txt.lower().replace("0x","").replace(" ","")
        if len(clean)!=64:
            await u.message.reply_text("❌ Wrong length (%d chars, need 64)\n\nGet key from:\npolymarket.com → Profile → Export Private Key"%len(clean)); return
        s.tmp_key=txt; s.wallet_state=None
        await u.message.reply_text("⏳ Connecting to Polymarket...")
        ok,result=connect_wallet(s,txt)
        if ok:
            bal,_=get_balance(s); bet=s.calc_bet(bal) if bal else 0.0
            await u.message.reply_text(
                "✅ WALLET CONNECTED!\n\n"
                "📍 Address: %s\n"
                "💰 Balance: %s\n"
                "🎯 Next stake: $%.2f (10%%)\n\n"
                "✅ No approve needed — Magic.Link\n"
                "Now press 🟢 Auto ON"%(
                    result,("$%.2f USDC"%bal) if bal else "N/A",bet),
                reply_markup=kb_main(s))
        else:
            await u.message.reply_text("❌ Error:\n%s\n\nTry again: press 🔗 Connect Wallet"%result,
                reply_markup=kb_main(s))
        return

    if s.pending_trade and time.time()-s.pending_trade.get("timestamp",0)<=600:
        try:
            amount=float(txt)
            if amount<1 or amount>500: await u.message.reply_text("Amount $1-$500"); return
            direction=s.pending_trade["direction"]
            kb=InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ $%.2f on %s"%(amount,direction),
                    callback_data="execute_%s_%.2f"%(direction,amount)),
                InlineKeyboardButton("❌ Cancel",callback_data="skip")]])
            await u.message.reply_text("Confirm?",reply_markup=kb)
        except ValueError: pass

# ============================================================
# AUTO TRADE
# ============================================================
async def execute_auto_trade(app,s,payload,result):
    decision=result.get("decision"); strength=result.get("strength","LOW"); logic=result.get("logic","")
    print("[AUTO TRADE] uid=%d dec=%s str=%s"%(s.uid,decision,strength))
    if not decision: return
    bal,_=get_balance(s)
    if not bal or bal<=0:
        await app.bot.send_message(chat_id=s.uid,text="❌ No USDC balance on Polymarket. Top up at polymarket.com"); return
    amount=s.calc_bet(bal)
    if amount<1:
        await app.bot.send_message(chat_id=s.uid,text="❌ Stake $%.2f < $1. Top up."%amount); return
    bet=place_bet(s,decision,amount)
    if bet["success"]:
        s.trade_history.append({"decision":decision,"amount":amount,
            "entry":payload["price"]["current"],"time":datetime.datetime.now(datetime.timezone.utc)})
        await app.bot.send_message(chat_id=s.uid,
            text="✅ AUTO BET\n%s | %s\nBal: $%.2f → Stake: $%.2f (10%%) → +$%.2f\n\n%s"%(
                decision,bet.get("market_name","Polymarket"),bal,amount,bet.get("pot",0),logic))
        print("[AUTO TRADE] OK uid=%d amount=%.2f"%(s.uid,amount))
    else:
        await app.bot.send_message(chat_id=s.uid,text="❌ AUTO FAILED\n%s"%bet["error"])
        print("[AUTO TRADE] FAIL uid=%d: %s"%(s.uid,bet["error"]))

# ============================================================
# MAIN CYCLE
# ============================================================
async def run_cycle(app,s):
    check_signals(s)
    payload=get_payload(s)
    if not payload:
        await app.bot.send_message(chat_id=s.uid,text="❌ Binance data error"); return
    result=analyze_with_ai(payload,s)
    if not result:
        await app.bot.send_message(chat_id=s.uid,text="❌ AI error"); return

    # Зберігаємо сигнал
    decision=result.get("decision","UP"); strength=result.get("strength","LOW")
    logic=result.get("logic",""); score=result.get("confidence_score",0)
    reasons=result.get("reasons",[]); key_sig=result.get("key_signal","")
    mkt_cond=result.get("market_condition",payload["context"]["market_condition"])
    liq=payload["liquidity"]; sw15=liq.get("sweep_15m",{})
    ctx=payload["context"]; amd=payload["amd"]; manip=payload["manipulation"]

    sig={
        "decision":decision,"strength":strength,"confidence_score":score,
        "logic":logic,"reasons":reasons,"key_signal":key_sig,
        "entry_price":payload["price"]["current"],"time":payload["timestamp"],
        "ts_unix":payload["ts_unix"],"outcome":None,
        "st15m":payload["structure"]["15m"],"mkt_cond":mkt_cond,
        "session":ctx["session"],"sweep_type":sw15.get("type","NONE"),
        "amd_phase":amd.get("phase","NONE"),"trap_type":manip["trap_type"],
    }
    s.signal_history.append(sig); s.save_history()

    # Зберігаємо в dump
    try:
        dump=[]
        if os.path.exists(SIGNALS_DUMP):
            with open(SIGNALS_DUMP) as f: dump=json.load(f)
        dump.append(sig); dump=dump[-2000:]
        with open(SIGNALS_DUMP,"w") as f: json.dump(dump,f,ensure_ascii=False,indent=2)
    except Exception: pass

    str_ua={"HIGH":"🔴 STRONG","MEDIUM":"🟡 MEDIUM","LOW":"🟢 WEAK"}.get(strength,strength)
    reas_s="\n".join("• "+r for r in reasons[:3]) if reasons else ""
    main_txt=(
        "📊 SIGNAL\n\n%s | %s | Score:%+d\n$%.2f | %s | %s\n\n🎯 %s\n\n%s\n\n%s"
    )%("UP ↑" if decision=="UP" else "DOWN ↓",str_ua,score,
       payload["price"]["current"],mkt_cond,ctx["session"],key_sig,logic,reas_s)

    print("[RUN_CYCLE] uid=%d auto=%s dec=%s str=%s"%(s.uid,s.auto_active,decision,strength))

    if s.auto_active:
        await app.bot.send_message(chat_id=s.uid,text="🤖 AUTO SIGNAL\n\n"+main_txt)
        await execute_auto_trade(app,s,payload,result)
    else:
        s.pending_trade={"direction":decision,"timestamp":time.time(),"price":payload["price"]["current"]}
        bal,_=get_balance(s); bet=s.calc_bet(bal) if bal else 0.0
        kb=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Yes",callback_data="confirm_%s"%decision),
            InlineKeyboardButton("❌ No",callback_data="skip")]])
        hint=(" (recommended $%.2f = 10%%)"%bet) if bal else ""
        await app.bot.send_message(chat_id=s.uid,
            text=main_txt+"\n\nEnter USDC amount"+hint+":",reply_markup=kb)

# ============================================================
# SCHEDULER
# ============================================================
async def periodic(app):
    while True:
        now=datetime.datetime.now(datetime.timezone.utc)
        m2n=15-(now.minute%15)
        if m2n==15: m2n=0
        next_run=now.replace(second=2,microsecond=0)+datetime.timedelta(minutes=m2n)
        if next_run<=now: next_run+=datetime.timedelta(minutes=15)
        wait=(next_run-now).total_seconds()
        logger.info("Next cycle in %.0fs at %s UTC",wait,next_run.strftime("%H:%M"))
        await asyncio.sleep(wait)
        await asyncio.sleep(5)  # чекаємо 5 сек щоб маркет з'явився
        if _sessions:
            for uid,s in list(_sessions.items()):
                try: await run_cycle(app,s)
                except Exception as e: logger.error("Cycle uid=%d: %s",uid,e)

# ============================================================
# MAIN
# ============================================================
def main():
    app=Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("help",    cmd_help))
    app.add_handler(CommandHandler("analyze", cmd_analyze))
    app.add_handler(CommandHandler("autoon",  cmd_autoon))
    app.add_handler(CommandHandler("autooff", cmd_autooff))
    app.add_handler(CommandHandler("balance", cmd_balance))
    app.add_handler(CommandHandler("stats",   cmd_stats))
    app.add_handler(CommandHandler("errors",  cmd_errors))
    app.add_handler(CommandHandler("dump",    cmd_dump))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    async def on_startup(app):
        asyncio.create_task(periodic(app))
        logger.info("Bot started. Magic.Link mode. 10%% bet. GTC orders.")

    app.post_init=on_startup
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__=="__main__":
    main()
