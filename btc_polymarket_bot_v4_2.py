import asyncio, logging, json, time, datetime, os, csv, io, requests
from openai import OpenAI
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (Application, CommandHandler, CallbackQueryHandler,
                           MessageHandler, ConversationHandler, filters)

# ============================================================
# CONFIG — Railway: env vars, локально: вставте значення
# ============================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "INSERT_TOKEN")
OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY",     "INSERT_OPENAI_KEY")

# Глобальні дефолти (якщо користувач не підключив свій гаманець)
DEFAULT_PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "")
DEFAULT_RPC_URL     = os.getenv("ALCHEMY_RPC_URL", "https://polygon-rpc.com")

AUTO_BET_SIZE     = float(os.getenv("AUTO_BET_SIZE",    "5.0"))
AUTO_MIN_STRENGTH = os.getenv("AUTO_MIN_STRENGTH",       "MEDIUM")

OI_CACHE_FILE = "oi_cache.json"
SIGNALS_DUMP  = "signals_dump.json"

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# MULTI-USER STATE
# кожен user_id має свою ізольовану сесію
# ============================================================
class UserSession:
    def __init__(self, uid):
        self.uid             = uid
        self.private_key     = DEFAULT_PRIVATE_KEY
        self.rpc_url         = DEFAULT_RPC_URL
        self.wallet_address  = None
        self.wallet_ok       = False
        self.signal_history  = []
        self.trade_history   = []
        self.auto_active     = False
        self.pending_trade   = {}
        # файли прив'язані до user
        self.history_file = "signal_history_%d.json" % uid
        self.errors_file  = "errors_log_%d.json"     % uid

_sessions: dict = {}

def get_session(uid: int) -> UserSession:
    if uid not in _sessions:
        s = UserSession(uid)
        _load_history(s)
        _sessions[uid] = s
    return _sessions[uid]

def _load_history(s: UserSession):
    try:
        if os.path.exists(s.history_file):
            with open(s.history_file) as f:
                s.signal_history = json.load(f)
    except Exception as e:
        logger.warning("Hist load uid=%d: %s", s.uid, e)

def _save_history(s: UserSession):
    try:
        with open(s.history_file, "w") as f:
            json.dump(s.signal_history, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning("Hist save uid=%d: %s", s.uid, e)

def _save_error(s: UserSession, sig):
    try:
        errors = []
        if os.path.exists(s.errors_file):
            with open(s.errors_file) as f:
                errors = json.load(f)
        errors.append(sig)
        errors = errors[-300:]
        with open(s.errors_file, "w") as f:
            json.dump(errors, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning("Err save uid=%d: %s", s.uid, e)

# Conversation states для підключення гаманця
WALLET_KEY, WALLET_RPC = range(2)

# ============================================================
# HTTP
# ============================================================
def safe_get(url, params=None, timeout=30):
    try:
        return requests.get(url, params=params, timeout=timeout).json()
    except Exception as e:
        logger.warning("HTTP %s: %s", url, e)
        return None

def utc_now_str():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def ts_unix():
    return int(time.time())

# ============================================================
# BINANCE — futures + spot fallback
# ============================================================
def fetch_candles(interval, limit):
    for _ in range(3):
        data = safe_get("https://fapi.binance.com/fapi/v1/klines",
                        {"symbol": "BTCUSDT", "interval": interval, "limit": limit})
        if data and isinstance(data, list):
            break
        time.sleep(2)
    if not data or not isinstance(data, list):
        data = safe_get("https://api.binance.com/api/v3/klines",
                        {"symbol": "BTCUSDT", "interval": interval, "limit": limit})
    if not data or not isinstance(data, list):
        return []
    return [{"t":int(k[0]),"o":float(k[1]),"h":float(k[2]),
             "l":float(k[3]),"c":float(k[4]),"v":float(k[5])} for k in data]

def fetch_price():
    data = safe_get("https://fapi.binance.com/fapi/v1/ticker/price", {"symbol":"BTCUSDT"})
    if not data or not isinstance(data, dict):
        data = safe_get("https://api.binance.com/api/v3/ticker/price", {"symbol":"BTCUSDT"})
    if data and isinstance(data, dict):
        return float(data.get("price", 0))
    return None

def fetch_funding():
    data = safe_get("https://fapi.binance.com/fapi/v1/premiumIndex", {"symbol":"BTCUSDT"})
    if not data or not isinstance(data, dict):
        return {"rate":0.0,"sentiment":"NEUTRAL","mark":0.0,"basis":0.0}
    fr   = float(data.get("lastFundingRate", 0))
    mark = float(data.get("markPrice", 0))
    idx  = float(data.get("indexPrice", mark))
    sent = "LONGS_TRAPPED" if fr>0.0005 else "SHORTS_TRAPPED" if fr<-0.0003 else "NEUTRAL"
    return {"rate":fr,"sentiment":sent,"mark":mark,"basis":round(mark-idx,2)}

def fetch_liquidations():
    now_ms = int(time.time()*1000); cutoff = now_ms-900000
    data = safe_get("https://fapi.binance.com/fapi/v1/forceOrders",{"symbol":"BTCUSDT","limit":200})
    if not data or isinstance(data,dict):
        data = safe_get("https://fapi.binance.com/fapi/v1/allForceOrders",{"symbol":"BTCUSDT","limit":200})
    if not data or not isinstance(data, list):
        return {"liq_longs":0.0,"liq_shorts":0.0,"signal":"NEUTRAL","exhaustion":False,"total_usd":0.0}
    recent    = [x for x in data if isinstance(x,dict) and int(x.get("time",0))>=cutoff] or data[:50]
    liq_longs = sum(float(x.get("origQty",0))*float(x.get("price",0)) for x in recent if x.get("side")=="SELL")
    liq_shorts= sum(float(x.get("origQty",0))*float(x.get("price",0)) for x in recent if x.get("side")=="BUY")
    total = liq_longs+liq_shorts
    signal= "SHORT_SQUEEZE_FUEL" if liq_shorts>liq_longs*2 else "LONG_CASCADE_FUEL" if liq_longs>liq_shorts*2 else "NEUTRAL"
    return {"liq_longs":round(liq_longs,2),"liq_shorts":round(liq_shorts,2),
            "signal":signal,"exhaustion":total>5_000_000,"total_usd":round(total,2)}

def fetch_oi():
    data = safe_get("https://fapi.binance.com/fapi/v1/openInterest",{"symbol":"BTCUSDT"})
    if not data or not isinstance(data, dict): return 0.0, 0.0
    cur = float(data.get("openInterest",0))
    try:
        prev = cur
        if os.path.exists(OI_CACHE_FILE):
            with open(OI_CACHE_FILE) as f: prev=json.load(f).get("oi",cur)
        with open(OI_CACHE_FILE,"w") as f: json.dump({"oi":cur,"ts":ts_unix()},f)
        return cur, round((cur-prev)/prev*100,4) if prev>0 else 0.0
    except Exception: return cur, 0.0

def fetch_orderbook():
    try:
        data = safe_get("https://fapi.binance.com/fapi/v1/depth",{"symbol":"BTCUSDT","limit":20})
        if not data or not isinstance(data,dict): return {"imbalance":0.0,"bias":"NEUTRAL"}
        bids  = sum(float(b[1]) for b in data.get("bids",[])[:10])
        asks  = sum(float(a[1]) for a in data.get("asks",[])[:10])
        total = bids+asks
        imb   = round((bids-asks)/total*100,2) if total>0 else 0.0
        return {"imbalance":imb,"bias":"BID_HEAVY" if imb>20 else "ASK_HEAVY" if imb<-20 else "BALANCED"}
    except Exception: return {"imbalance":0.0,"bias":"NEUTRAL"}

def fetch_lsr():
    try:
        data = safe_get("https://fapi.binance.com/futures/data/topLongShortPositionRatio",
                        {"symbol":"BTCUSDT","period":"15m","limit":3})
        if not data or not isinstance(data,list): return {"ratio":1.0,"long_pct":50.0,"bias":"NEUTRAL"}
        latest   = data[-1]
        ratio    = float(latest.get("longShortRatio",1.0))
        long_pct = float(latest.get("longAccount",0.5))*100
        return {"ratio":round(ratio,3),"long_pct":round(long_pct,1),
                "bias":"CROWD_LONG" if ratio>1.5 else "CROWD_SHORT" if ratio<0.7 else "NEUTRAL"}
    except Exception: return {"ratio":1.0,"long_pct":50.0,"bias":"NEUTRAL"}

# ============================================================
# SMC ENGINE
# ============================================================
def swing_points(candles):
    sh,sl=[],[]
    for i in range(2,len(candles)-2):
        h=candles[i]["h"]
        if h>candles[i-1]["h"] and h>candles[i+1]["h"] and h>candles[i-2]["h"] and h>candles[i+2]["h"]:
            sh.append({"price":h,"idx":i})
        l=candles[i]["l"]
        if l<candles[i-1]["l"] and l<candles[i+1]["l"] and l<candles[i-2]["l"] and l<candles[i+2]["l"]:
            sl.append({"price":l,"idx":i})
    return sh[-5:],sl[-5:]

def market_structure(candles):
    sh,sl=swing_points(candles)
    if len(sh)<2 or len(sl)<2: return "RANGING"
    hs=[x["price"] for x in sh]; ls=[x["price"] for x in sl]
    if all(hs[i]>hs[i-1] for i in range(1,len(hs))) and all(ls[i]>ls[i-1] for i in range(1,len(ls))): return "BULLISH"
    if all(hs[i]<hs[i-1] for i in range(1,len(hs))) and all(ls[i]<ls[i-1] for i in range(1,len(ls))): return "BEARISH"
    return "RANGING"

def liq_sweep(candles):
    if len(candles)<10: return {"type":"NONE","level":0.0,"ago":0}
    sh,sl=swing_points(candles[:-3])
    for i,c in enumerate(reversed(candles[-5:])):
        for s in reversed(sh):
            if c["h"]>s["price"] and c["c"]<s["price"]: return {"type":"HIGH","level":s["price"],"ago":i+1}
        for s in reversed(sl):
            if c["l"]<s["price"] and c["c"]>s["price"]: return {"type":"LOW","level":s["price"],"ago":i+1}
    return {"type":"NONE","level":0.0,"ago":0}

def equal_levels(candles,tol=0.001):
    eq_h,eq_l=[],[]
    hs=[(i,c["h"]) for i,c in enumerate(candles)]; ls=[(i,c["l"]) for i,c in enumerate(candles)]
    for i in range(len(hs)):
        for j in range(i+1,len(hs)):
            if abs(hs[i][1]-hs[j][1])/hs[i][1]<tol and j-i>=2: eq_h.append({"price":(hs[i][1]+hs[j][1])/2})
    for i in range(len(ls)):
        for j in range(i+1,len(ls)):
            if abs(ls[i][1]-ls[j][1])/ls[i][1]<tol and j-i>=2: eq_l.append({"price":(ls[i][1]+ls[j][1])/2})
    return eq_h[-3:],eq_l[-3:]

def stop_clusters(candles,price):
    sh,sl=swing_points(candles); eq_h,eq_l=equal_levels(candles[-50:])
    above,below=[],[]
    for s in sh:
        if s["price"]>price: above.append({"price":s["price"],"type":"swing_high"})
    for s in sl:
        if s["price"]<price: below.append({"price":s["price"],"type":"swing_low"})
    for e in eq_h:
        if e["price"]>price: above.append({"price":e["price"],"type":"equal_highs"})
    for e in eq_l:
        if e["price"]<price: below.append({"price":e["price"],"type":"equal_lows"})
    sa=min(above,key=lambda x:x["price"]-price) if above else None
    sb=min(below,key=lambda x:price-x["price"]) if below else None
    return sa,sb

def find_fvg(candles,price):
    fa=fb=None
    for i in range(1,len(candles)-1):
        pv,nx=candles[i-1],candles[i+1]
        if nx["l"]>pv["h"]:
            mid=( nx["l"]+pv["h"])/2; dist=round((price-mid)/price*100,4)
            if mid<price and (fb is None or dist<fb["dist"]): fb={"top":nx["l"],"bot":pv["h"],"dist":dist,"size":round(nx["l"]-pv["h"],2)}
        if nx["h"]<pv["l"]:
            mid=(pv["l"]+nx["h"])/2; dist=round((mid-price)/price*100,4)
            if mid>price and (fa is None or dist<fa["dist"]): fa={"top":pv["l"],"bot":nx["h"],"dist":dist,"size":round(pv["l"]-nx["h"],2)}
    return fa,fb

def bos_choch(candles,struct_htf):
    if len(candles)<5: return None
    sh,sl=swing_points(candles[:-1])
    if not sh or not sl: return None
    cl=candles[-1]["c"]
    if cl>sh[-1]["price"]: return {"type":"CHoCH" if struct_htf=="BEARISH" else "BOS","dir":"UP","level":sh[-1]["price"]}
    if cl<sl[-1]["price"]: return {"type":"CHoCH" if struct_htf=="BULLISH" else "BOS","dir":"DOWN","level":sl[-1]["price"]}
    return None

def detect_manipulation(candles,sweep,price):
    result={"trap_type":"NONE","reversal_signal":None}
    if len(candles)<5: return result
    last=candles[-1]; body=abs(last["c"]-last["o"]); total=last["h"]-last["l"]
    if total>0:
        wr=1-(body/total); uw=last["h"]-max(last["c"],last["o"]); lw=min(last["c"],last["o"])-last["l"]
        if wr>0.7 and total/last["c"]>0.002:
            if uw>lw*2: result.update({"trap_type":"WICK_TRAP_HIGH","reversal_signal":"DOWN"})
            elif lw>uw*2: result.update({"trap_type":"WICK_TRAP_LOW","reversal_signal":"UP"})
    if sweep["type"]!="NONE" and sweep["ago"]<=3:
        if sweep["type"]=="HIGH" and last["c"]<sweep["level"]*0.9995: result.update({"trap_type":"SWEEP_TRAP_HIGH","reversal_signal":"DOWN"})
        elif sweep["type"]=="LOW" and last["c"]>sweep["level"]*1.0005: result.update({"trap_type":"SWEEP_TRAP_LOW","reversal_signal":"UP"})
    lt=any(c["l"]<candles[-6]["l"] for c in candles[-5:]) if len(candles)>=6 else False
    ht=any(c["h"]>candles[-6]["h"] for c in candles[-5:]) if len(candles)>=6 else False
    if lt and ht: result.update({"trap_type":"CHOP_ZONE","reversal_signal":None})
    return result

def detect_amd(c15,c5m,price):
    if len(c15)<20: return {"phase":"NONE","direction":None,"confidence":0,"reason":""}
    last10=c15[-10:]; last3=c15[-3:]; last3_5m=c5m[-3:] if len(c5m)>=3 else []
    highs=[c["h"] for c in last10]; lows=[c["l"] for c in last10]
    rng=(max(highs)-min(lows))/price*100
    avg_body=sum(abs(c["c"]-c["o"]) for c in last10)/len(last10)/price*100
    is_accum=rng<0.6 and avg_body<0.10
    sw15=liq_sweep(c15); sw5=liq_sweep(c5m) if len(c5m)>=10 else {"type":"NONE","level":0.0,"ago":0}
    sweep=sw15 if sw15["type"]!="NONE" and sw15["ago"]<=4 else sw5
    manip_ok=sweep["type"]!="NONE" and sweep["ago"]<=4
    mc=None
    if last3_5m:
        m=(last3_5m[-1]["c"]-last3_5m[0]["o"])/last3_5m[0]["o"]*100
        mc="UP" if m>0.05 else "DOWN" if m<-0.05 else None
    lm=(last3[-1]["c"]-last3[0]["o"])/last3[0]["o"]*100
    if manip_ok and is_accum:
        if sweep["type"]=="LOW":
            return {"phase":"MANIPULATION_DONE","direction":"UP","confidence":3 if mc=="UP" else 2,
                    "sweep_level":sweep["level"],"reason":"ACCUM+SWEEP_LOW: smart money bought UP"}
        else:
            lc=c15[-1]["c"]
            if lc<sweep["level"]*0.9998:
                return {"phase":"MANIPULATION_DONE","direction":"DOWN","confidence":3 if mc=="DOWN" else 2,
                        "sweep_level":sweep["level"],"reason":"ACCUM+SWEEP_HIGH+CLOSE_BELOW: reversal DOWN"}
            return {"phase":"MANIPULATION_DONE","direction":"UP","confidence":1,
                    "sweep_level":sweep["level"],"reason":"ACCUM+SWEEP_HIGH+HOLDS_ABOVE: bull continuation"}
    if is_accum: return {"phase":"ACCUMULATION","direction":None,"confidence":1,"reason":"tight range"}
    if manip_ok:
        if sweep["type"]=="LOW": d="UP"; r="SWEEP_LOW: reversal UP"
        else:
            lc=c15[-1]["c"]
            if lc<sweep["level"]*0.9998: d="DOWN"; r="SWEEP_HIGH+CLOSE_BELOW: reversal DOWN"
            else: d="UP"; r="SWEEP_HIGH+HOLDS: continuation UP"
        return {"phase":"MANIPULATION","direction":d,"confidence":2,"sweep_level":sweep["level"],"reason":r}
    if abs(lm)>0.15:
        return {"phase":"DISTRIBUTION","direction":"UP" if lm>0 else "DOWN","confidence":1,"reason":"active move"}
    return {"phase":"NONE","direction":None,"confidence":0,"reason":""}

# ============================================================
# CONTEXT
# ============================================================
def classify_vol(c15):
    if len(c15)<10: return "UNKNOWN",0.0
    ranges=[(c["h"]-c["l"])/c["c"]*100 for c in c15[-10:]]
    avg=sum(ranges)/len(ranges); rec=sum(ranges[-3:])/3; pri=sum(ranges[:7])/7
    if avg<0.08: cond="LOW_VOL"
    elif rec>pri*1.5: cond="EXPANSION"
    elif avg>0.3: cond="HIGH_VOL"
    else: cond="NORMAL"
    return cond,round(avg,4)

def classify_session():
    hour=datetime.datetime.now(datetime.timezone.utc).hour
    if 7<=hour<12:    return "LONDON",1
    elif 12<=hour<17: return "NY_OPEN",0
    elif 17<=hour<21: return "NY_AFTERNOON",0
    elif 21<=hour or hour<3: return "ASIA_ACTIVE",0
    else: return "DEAD_HOURS",-1

def classify_mkt(c15,c5m):
    if len(c15)<10: return "RANGING"
    closes=[c["c"] for c in c15[-12:]]
    ups=sum(1 for i in range(1,len(closes)) if closes[i]>closes[i-1]); downs=len(closes)-1-ups
    if ups>=9 or downs>=9: return "TRENDING"
    alts=sum(1 for i in range(1,len(closes)-1) if (closes[i]>closes[i-1])!=(closes[i+1]>closes[i]))
    return "CHOPPY" if alts>=8 else "RANGING"

def get_last_sig_ctx(s: UserSession):
    if not s.signal_history: return "no_prev"
    last=s.signal_history[-1]
    return "prev=%s outcome=%s move=%+.0f"%(last.get("decision","?"),last.get("outcome","PENDING"),last.get("real_move",0))

# ============================================================
# FULL PAYLOAD
# ============================================================
def get_full_payload(s: UserSession):
    c15=fetch_candles("15m",100); c5m=fetch_candles("5m",50); c1m=fetch_candles("1m",30)
    if not c15: return None
    price=c15[-1]["c"]; prev=c15[-2]["c"] if len(c15)>=2 else price
    chg_15m=round((price-prev)/prev*100,4)
    chg_5m=round((c5m[-1]["c"]-c5m[-4]["c"])/c5m[-4]["c"]*100,4) if len(c5m)>=4 else 0.0
    momentum_3=round((c15[-1]["c"]-c15[-4]["c"])/c15[-4]["c"]*100,4) if len(c15)>=4 else 0.0
    micro_mom=round((c1m[-1]["c"]-c1m[-4]["c"])/c1m[-4]["c"]*100,4) if len(c1m)>=4 else 0.0
    st15m=market_structure(c15) if len(c15)>=6 else "RANGING"
    st5m=market_structure(c5m)  if len(c5m)>=6 else "RANGING"
    st1m=market_structure(c1m)  if len(c1m)>=6 else "RANGING"
    sweep_15m=liq_sweep(c15); sweep_5m=liq_sweep(c5m) if c5m else {"type":"NONE","level":0.0,"ago":0}
    sweep_1m=liq_sweep(c1m) if c1m else {"type":"NONE","level":0.0,"ago":0}
    sa,sb=stop_clusters(c15,price)
    fvg5_a,fvg5_b=find_fvg(c5m[-30:],price) if len(c5m)>=5 else (None,None)
    fvg1_a,fvg1_b=find_fvg(c1m[-20:],price) if len(c1m)>=5 else (None,None)
    bc5m=bos_choch(c5m,st15m) if len(c5m)>=5 else None
    manip=detect_manipulation(c5m[-10:] if len(c5m)>=10 else c15[-10:],sweep_5m,price)
    amd=detect_amd(c15,c5m,price)
    fund=fetch_funding(); liqs=fetch_liquidations(); oi,oi_chg=fetch_oi()
    ob=fetch_orderbook(); lsr=fetch_lsr()
    vol_cond,vol_score=classify_vol(c15); session,sess_boost=classify_session()
    mkt_cond=classify_mkt(c15,c5m)
    dist_above=round((sa["price"]-price)/price*100,4) if sa else 999.0
    dist_below=round((price-sb["price"])/price*100,4) if sb else 999.0
    return {
        "timestamp":utc_now_str(),"ts_unix":ts_unix(),
        "price":{"current":price,"chg_15m":chg_15m,"chg_5m":chg_5m,"momentum_3":momentum_3,
                 "micro_mom":micro_mom,"mark":fund["mark"],"basis":fund["basis"]},
        "structure":{"15m":st15m,"5m":st5m,"1m":st1m},
        "liquidity":{"sweep_15m":sweep_15m,"sweep_5m":sweep_5m,"sweep_1m":sweep_1m,
                     "stops_above":sa,"stops_below":sb,"dist_above":dist_above,"dist_below":dist_below,
                     "fvg5_above":fvg5_a,"fvg5_below":fvg5_b,"fvg1_above":fvg1_a,"fvg1_below":fvg1_b,
                     "bos_choch_5m":bc5m},
        "amd":amd,"manipulation":manip,
        "positioning":{"funding_rate":fund["rate"],"funding_sent":fund["sentiment"],
                       "liq_longs":liqs["liq_longs"],"liq_shorts":liqs["liq_shorts"],
                       "liq_signal":liqs["signal"],"exhaustion":liqs["exhaustion"],
                       "liq_total":liqs["total_usd"],"oi":oi,"oi_change":oi_chg,
                       "ob_bias":ob["bias"],"ob_imbalance":ob["imbalance"],
                       "lsr_bias":lsr["bias"],"lsr_ratio":lsr["ratio"],"crowd_long":lsr["long_pct"]},
        "context":{"volatility":vol_cond,"vol_score":vol_score,"session":session,
                   "session_boost":sess_boost,"market_condition":mkt_cond,
                   "last_signal":get_last_sig_ctx(s)},
    }

# ============================================================
# POLYMARKET — динамічний пошук маркету BTC 15m
# ============================================================
_market_cache = {"id": None, "name": None, "token_yes": None, "token_no": None, "expires": 0}

def _find_btc15m_market():
    """
    Шукає ПОТОЧНИЙ активний маркет 'BTC Up or Down 15 minutes'.
    Кешує ID до кінця раунду. Повертає dict з id, name, token_yes, token_no.
    """
    now = time.time()
    if _market_cache["id"] and now < _market_cache["expires"]:
        return _market_cache

    queries = [
        "BTC Up or Down 15 minutes",
        "Bitcoin Up or Down 15",
        "BTC 15 min",
        "Bitcoin 15 minutes",
    ]
    endpoints = [
        "https://gamma-api.polymarket.com/markets",
        "https://clob.polymarket.com/markets",
    ]

    best = None
    for ep in endpoints:
        for q in queries:
            for attempt in range(3):
                try:
                    resp = requests.get(ep, params={"q": q, "active": "true", "limit": 50}, timeout=15)
                    markets = resp.json()
                    if not isinstance(markets, list):
                        markets = markets.get("markets", []) if isinstance(markets, dict) else []
                    for m in markets:
                        title = m.get("question","") or m.get("title","")
                        tl = title.lower()
                        if m.get("closed", True):
                            continue
                        if not (("btc" in tl or "bitcoin" in tl) and "15" in tl):
                            continue
                        # беремо маркет що завершується найближче в майбутньому
                        end_ts = 0
                        for tf in ("endDate","end_date_iso","endDateIso","end_time","endTime"):
                            v = m.get(tf)
                            if v:
                                try:
                                    end_ts = int(datetime.datetime.fromisoformat(
                                        str(v).replace("Z","+00:00")).timestamp())
                                except Exception:
                                    try: end_ts = int(v)
                                    except Exception: pass
                                break
                        if end_ts and end_ts < now:
                            continue  # вже завершений
                        mid = m.get("conditionId") or m.get("id") or m.get("condition_id")
                        if not mid:
                            continue
                        if best is None or (end_ts and end_ts < best.get("end_ts", 999999999)):
                            best = {"id": mid, "name": title, "end_ts": end_ts, "raw": m}
                    if best:
                        break
                except Exception as e:
                    logger.warning("Market search attempt %d: %s", attempt+1, e)
                    time.sleep(2)
            if best:
                break
        if best:
            break

    if not best:
        logger.error("BTC 15m market not found")
        return None

    # Витягуємо token_id для YES і NO через CLOB
    token_yes = token_no = None
    try:
        clob_resp = requests.get(
            "https://clob.polymarket.com/markets/%s" % best["id"], timeout=15)
        mdata = clob_resp.json()
        tokens = mdata.get("tokens", [])
        for t in tokens:
            oc = t.get("outcome","").upper()
            if oc in ("YES","UP"):   token_yes = t
            if oc in ("NO","DOWN"):  token_no  = t
        # fallback — пробуємо raw
        if not tokens:
            raw = best.get("raw",{})
            tokens = raw.get("tokens", raw.get("outcomes", []))
            for t in tokens:
                oc = (t.get("outcome") or t.get("name","")).upper()
                if oc in ("YES","UP"):   token_yes = t
                if oc in ("NO","DOWN"):  token_no  = t
    except Exception as e:
        logger.warning("Token fetch: %s", e)

    # Кешуємо до кінця раунду (або 14 хв якщо end_ts невідомий)
    expires = best["end_ts"] - 30 if best.get("end_ts") else now + 840
    _market_cache.update({"id": best["id"], "name": best["name"],
                           "token_yes": token_yes, "token_no": token_no,
                           "expires": expires})
    logger.info("Market found: %s [%s]", best["name"], best["id"])
    return _market_cache

def place_bet(s: UserSession, direction: str, amount: float):
    """
    direction = "UP" або "DOWN"
    Повертає {"success": bool, "error": str, "price": float, "pot": float}
    """
    if not s.wallet_ok or not s.private_key:
        return {"success": False, "error": "Гаманець не підключено. Натисни 🔗 Підключити гаманець"}

    market = _find_btc15m_market()
    if not market:
        # retry раз
        time.sleep(3)
        market = _find_btc15m_market()
    if not market:
        return {"success": False, "error": "Маркет BTC 15m не знайдено. Спробуй пізніше."}

    token = market["token_yes"] if direction == "UP" else market["token_no"]
    if not token:
        return {"success": False, "error": "Token для %s не знайдено в маркеті" % direction}

    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OrderArgs, OrderType

        key = (s.private_key.strip()
               .replace(" ","").replace("\n","").replace("\r","")
               .replace('"',"").replace("'",""))
        if key.lower().startswith("0x"):
            key = key[2:]

        client = ClobClient(host="https://clob.polymarket.com", key=key, chain_id=137)
        price  = float(token.get("price", 0.5))
        pot    = round(amount / price - amount, 2) if price > 0 else 0.0

        order = client.create_and_post_order(OrderArgs(
            token_id=token.get("token_id") or token.get("id",""),
            price=price, size=amount, side="BUY", order_type=OrderType.FOK))
        return {"success": True, "order": order, "price": price,
                "pot": pot, "market_name": market["name"]}
    except ImportError:
        return {"success": False, "error": "pip install py-clob-client"}
    except Exception as e:
        err = str(e)
        # детальне повідомлення про помилку
        if "insufficient" in err.lower():
            return {"success": False, "error": "Недостатньо USDC балансу"}
        if "allowance" in err.lower():
            return {"success": False, "error": "Потрібно approve USDC на Polymarket"}
        if "nonce" in err.lower():
            return {"success": False, "error": "Помилка nonce транзакції, спробуй ще"}
        return {"success": False, "error": err[:200]}

# ============================================================
# БАЛАНС USDC (Polygon)
# ============================================================
def get_balance(s: UserSession):
    if not s.wallet_ok or not s.wallet_address:
        return None, "Гаманець не підключено"
    try:
        rpcs = [s.rpc_url, "https://polygon.drpc.org",
                "https://polygon-bor-rpc.publicnode.com", "https://rpc.ankr.com/polygon"]
        usdc    = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
        call_d  = "0x70a08231000000000000000000000000" + s.wallet_address[2:]
        payload = {"jsonrpc":"2.0","method":"eth_call",
                   "params":[{"to":usdc,"data":call_d},"latest"],"id":1}
        for rpc in rpcs:
            if not rpc or "INSERT" in rpc:
                continue
            try:
                r = requests.post(rpc, json=payload, timeout=10)
                d = r.json()
                if d and "result" in d and d["result"] not in ("0x","0x0",None,""):
                    return round(int(d["result"],16)/1e6, 2), s.wallet_address
            except Exception:
                continue
        return None, "Всі RPC недоступні"
    except Exception as e:
        return None, str(e)

# ============================================================
# ПІДКЛЮЧЕННЯ ГАМАНЦЯ — валідація
# ============================================================
def validate_and_connect_wallet(s: UserSession, private_key: str, rpc_url: str):
    """
    Перевіряє приватний ключ, підключається до Polygon, повертає (ok, message)
    """
    try:
        from eth_account import Account
        key = (private_key.strip()
               .replace(" ","").replace("\n","").replace("\r","")
               .replace('"',"").replace("'",""))
        if key.lower().startswith("0x"):
            key = key[2:]
        if len(key) != 64:
            return False, "Ключ має бути 64 символи (hex). Зараз: %d символів.\nПереконайся що скопіював правильно без 0x префіксу." % len(key)
        try:
            int(key, 16)
        except ValueError:
            return False, "Ключ містить неприпустимі символи. Має бути тільки 0-9 і a-f."

        account = Account.from_key(key)
        addr    = account.address

        # Перевіряємо підключення до мережі
        rpcs_to_try = [rpc_url, "https://polygon.drpc.org", "https://polygon-bor-rpc.publicnode.com"]
        connected = False
        for rpc in rpcs_to_try:
            try:
                r = requests.post(rpc, json={"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}, timeout=8)
                d = r.json()
                if d.get("result") in ("0x89", "137"):  # Polygon mainnet
                    connected = True
                    break
                elif d.get("result"):
                    connected = True
                    break
            except Exception:
                continue

        if not connected:
            return False, "Не вдалося підключитись до Polygon RPC.\nСпробуй: https://polygon-rpc.com"

        s.private_key    = key
        s.rpc_url        = rpc_url
        s.wallet_address = addr
        s.wallet_ok      = True
        return True, addr

    except ImportError:
        return False, "Бібліотека eth-account не встановлена на сервері"
    except Exception as e:
        return False, "Помилка підключення: %s" % str(e)

# ============================================================
# NEWS
# ============================================================
def get_news():
    try:
        data = safe_get("https://min-api.cryptocompare.com/data/v2/news/",
                        {"categories":"BTC,Bitcoin","lTs":0})
        if data and "Data" in data and data["Data"]:
            lines=[]; bkw=["bull","surge","rally","rise","gain","buy","etf","adoption"]
            skw=["bear","drop","fall","crash","dump","sell","ban","hack","fear"]
            for item in data["Data"][:6]:
                t=item.get("title","").lower(); p=sum(1 for k in bkw if k in t); n=sum(1 for k in skw if k in t)
                sent="BULLISH" if p>n else "BEARISH" if n>p else "NEUTRAL"
                lines.append("[%s] %s"%(sent,item.get("title","")[:70]))
            return "\n".join(lines)
    except Exception: pass
    return "Новини недоступні"

# ============================================================
# AI SYSTEM PROMPT — без змін (v4.1 логіка)
# ============================================================
SYSTEM_PROMPT = """You are an elite BTC short-term trader. Task: predict if BTC will be HIGHER (UP) or LOWER (DOWN) than current price in exactly 15 minutes on Polymarket.

IMPORTANT: You analyze from scratch every time. Do NOT mechanically repeat or avoid previous signals — only the current data matters. The last signal context is shown only so you are aware of it, not to copy or invert it.

============================
TIMEFRAME LOGIC
============================
15m = context | 5m = tactics | 1m = execution

============================
AMD FRAMEWORK — FIXED LOGIC
============================
Accumulation = price ranges quietly, avg_body < 0.10%, range < 0.6%
Manipulation = liquidity sweep
SWEEP LOW → smart money BOUGHT → UP (+3)
SWEEP HIGH + close BELOW sweep level → reversal DOWN (+3)
SWEEP HIGH + HOLDS ABOVE → bull continuation UP (+1, NOT DOWN)
Check AMD.reason field always.

============================
RANGING MARKET RULES
============================
dist_below < 0.1% = bounce UP | dist_above < 0.1% = rejection DOWN
dist_above < 0.05% + sweep HIGH + close below = STRONG DOWN (-3)
dist_below < dist_above AND < 0.3% = lean UP
dist_above < dist_below AND < 0.3% = lean DOWN
both > 0.5% = use momentum + orderbook
CHOP_ZONE = reduce to LOW

============================
SCORING
============================
+3 AMD MANIPULATION_DONE UP | -3 AMD MANIPULATION_DONE DOWN
+2 dist_below<0.1% | CHoCH UP 5m | BOS UP 5m | SHORT_SQUEEZE | SWEEP_TRAP_LOW
-2 dist_above<0.1% | CHoCH DOWN 5m | BOS DOWN 5m | LONG_CASCADE | SWEEP_TRAP_HIGH
+1 FVG5m_below<0.15% | FVG1m_below<0.10% | dist_below<dist_above<0.3% | BID_HEAVY | CROWD_SHORT+SHORTS_TRAPPED | micro_mom>0.03% | OI_rising+price_up | LONDON | struct5m_BULLISH
-1 mirror for DOWN | CHOP_ZONE | DEAD_HOURS | exhaustion

STRENGTH: >=5=HIGH | 3-4=MEDIUM | 1-2=LOW | 0=micro_mom LOW
ALWAYS UP or DOWN.

OUTPUT JSON only:
{"decision":"UP or DOWN","strength":"HIGH or MEDIUM or LOW","confidence_score":<int>,"market_condition":"TRENDING or RANGING or CHOPPY","amd_used":true/false,"key_signal":"one sentence","logic":"2-3 sentences Ukrainian","reasons":["r1","r2","r3"],"risk_note":"risk or NONE"}"""

def analyze_with_ai(payload, s: UserSession):
    try:
        client=OpenAI(api_key=OPENAI_API_KEY)
        liq=payload["liquidity"]; pos=payload["positioning"]; pr=payload["price"]
        st=payload["structure"]; ctx=payload["context"]; manip=payload["manipulation"]; amd=payload["amd"]
        sw15=liq.get("sweep_15m",{}); sw5=liq.get("sweep_5m",{}); sw1=liq.get("sweep_1m",{})
        bc5=liq.get("bos_choch_5m"); f5a=liq.get("fvg5_above"); f5b=liq.get("fvg5_below")
        f1a=liq.get("fvg1_above"); f1b=liq.get("fvg1_below")
        sa=liq.get("stops_above") or {}; sb=liq.get("stops_below") or {}
        amd_s="phase=%s dir=%s conf=%d reason=%s"%(amd.get("phase","NONE"),amd.get("direction","?"),amd.get("confidence",0),amd.get("reason",""))
        msg=(
            "Time:%s Session:%s(boost=%+d) LastSignal:%s\n\n"
            "PRICE:$%.2f 15m:%+.4f%% 5m:%+.4f%% Mom3:%+.4f%% Micro1m:%+.4f%%\n"
            "Mark:$%.2f Basis:%+.2f\n\n"
            "STRUCT: 15m=%s 5m=%s 1m=%s Mkt:%s Vol:%s(%.4f%%)\n\n"
            "AMD: %s\n\n"
            "Sweep15m:%s@%.2f(%dc) Sweep5m:%s@%.2f(%dc) Sweep1m:%s@%.2f(%dc)\n"
            "StopsUp:%s dist=%.3f%% StopsDn:%s dist=%.3f%%\n"
            "FVG5m:up=%s dn=%s FVG1m:up=%s dn=%s BOS5m:%s\n\n"
            "Manip:trap=%s hint=%s\n\n"
            "Fund:%+.6f(%s) LiqL:$%.0f LiqS:$%.0f Sig:%s Exhaust:%s\n"
            "OI:%.0f OI_chg:%+.4f%% Book:%s(%+.1f%%) L/S:%.3f(%s) CrowdLong:%.1f%%"
        )%(
            payload["timestamp"],ctx["session"],ctx["session_boost"],ctx["last_signal"],
            pr["current"],pr["chg_15m"],pr["chg_5m"],pr["momentum_3"],pr["micro_mom"],pr["mark"],pr["basis"],
            st["15m"],st["5m"],st["1m"],ctx["market_condition"],ctx["volatility"],ctx["vol_score"],
            amd_s,
            sw15.get("type","N"),sw15.get("level",0),sw15.get("ago",0),
            sw5.get("type","N"),sw5.get("level",0),sw5.get("ago",0),
            sw1.get("type","N"),sw1.get("level",0),sw1.get("ago",0),
            sa.get("type","none"),liq.get("dist_above",999),
            sb.get("type","none"),liq.get("dist_below",999),
            ("dist=%.3f%%"%f5a["dist"]) if f5a else "none",("dist=%.3f%%"%f5b["dist"]) if f5b else "none",
            ("dist=%.3f%%"%f1a["dist"]) if f1a else "none",("dist=%.3f%%"%f1b["dist"]) if f1b else "none",
            ("%s %s@%.2f"%(bc5["type"],bc5["dir"],bc5["level"])) if bc5 else "none",
            manip["trap_type"],str(manip["reversal_signal"]),
            pos["funding_rate"],pos["funding_sent"],pos["liq_longs"],pos["liq_shorts"],pos["liq_signal"],pos["exhaustion"],
            pos["oi"],pos["oi_change"],pos["ob_bias"],pos["ob_imbalance"],pos["lsr_ratio"],pos["lsr_bias"],pos["crowd_long"]
        )
        resp=client.chat.completions.create(model="gpt-4o",
             messages=[{"role":"system","content":SYSTEM_PROMPT},{"role":"user","content":msg}],
             temperature=0.1,response_format={"type":"json_object"})
        return json.loads(resp.choices[0].message.content)
    except Exception as e:
        logger.error("AI uid=%d: %s",s.uid,e)
        return None

# ============================================================
# SIGNALS DUMP
# ============================================================
def save_signal_dump(payload, result, s: UserSession):
    try:
        dump=[]
        if os.path.exists(SIGNALS_DUMP):
            with open(SIGNALS_DUMP) as f: dump=json.load(f)
        liq=payload["liquidity"]; pos=payload["positioning"]; pr=payload["price"]
        st=payload["structure"]; ctx=payload["context"]; amd=payload["amd"]; manip=payload["manipulation"]
        sw15=liq.get("sweep_15m",{}); sw5=liq.get("sweep_5m",{}); sw1=liq.get("sweep_1m",{})
        sa=liq.get("stops_above") or {}; sb_=liq.get("stops_below") or {}
        f5a=liq.get("fvg5_above"); f5b=liq.get("fvg5_below"); f1a=liq.get("fvg1_above"); f1b=liq.get("fvg1_below"); bc5=liq.get("bos_choch_5m")
        record={
            "uid":s.uid,"ts":payload["timestamp"],"ts_unix":payload["ts_unix"],
            "outcome":"PENDING","exit_price":None,"real_move":None,
            "decision":result.get("decision"),"strength":result.get("strength"),
            "confidence_score":result.get("confidence_score"),"key_signal":result.get("key_signal"),
            "logic":result.get("logic"),"reasons":result.get("reasons"),"risk_note":result.get("risk_note"),
            "amd_used":result.get("amd_used"),"market_condition":result.get("market_condition"),
            "price":pr["current"],"chg_15m":pr["chg_15m"],"chg_5m":pr["chg_5m"],
            "momentum_3":pr["momentum_3"],"micro_mom":pr["micro_mom"],"mark":pr["mark"],"basis":pr["basis"],
            "st15m":st["15m"],"st5m":st["5m"],"st1m":st["1m"],
            "session":ctx["session"],"sess_boost":ctx["session_boost"],
            "volatility":ctx["volatility"],"vol_score":ctx["vol_score"],"mkt_cond_ctx":ctx["market_condition"],
            "amd_phase":amd.get("phase"),"amd_direction":amd.get("direction"),
            "amd_confidence":amd.get("confidence"),"amd_reason":amd.get("reason"),
            "sweep15m_type":sw15.get("type"),"sweep15m_level":sw15.get("level"),"sweep15m_ago":sw15.get("ago"),
            "sweep5m_type":sw5.get("type"),"sweep5m_level":sw5.get("level"),"sweep5m_ago":sw5.get("ago"),
            "sweep1m_type":sw1.get("type"),"sweep1m_level":sw1.get("level"),"sweep1m_ago":sw1.get("ago"),
            "dist_above":liq.get("dist_above"),"dist_below":liq.get("dist_below"),
            "fvg5_above_dist":f5a["dist"] if f5a else None,"fvg5_below_dist":f5b["dist"] if f5b else None,
            "fvg1_above_dist":f1a["dist"] if f1a else None,"fvg1_below_dist":f1b["dist"] if f1b else None,
            "bos5m_type":bc5["type"] if bc5 else None,"bos5m_dir":bc5["dir"] if bc5 else None,
            "trap_type":manip["trap_type"],"reversal_signal":manip["reversal_signal"],
            "funding_rate":pos["funding_rate"],"funding_sent":pos["funding_sent"],
            "liq_longs":pos["liq_longs"],"liq_shorts":pos["liq_shorts"],"liq_signal":pos["liq_signal"],
            "exhaustion":pos["exhaustion"],"oi":pos["oi"],"oi_change":pos["oi_change"],
            "ob_bias":pos["ob_bias"],"ob_imbalance":pos["ob_imbalance"],
            "lsr_bias":pos["lsr_bias"],"lsr_ratio":pos["lsr_ratio"],"crowd_long":pos["crowd_long"],
        }
        dump.append(record); dump=dump[-2000:]
        with open(SIGNALS_DUMP,"w") as f: json.dump(dump,f,ensure_ascii=False,indent=2)
    except Exception as e: logger.warning("Dump: %s",e)

def update_dump_outcome(ts_unix_val, uid, outcome, exit_price, real_move):
    try:
        if not os.path.exists(SIGNALS_DUMP): return
        with open(SIGNALS_DUMP) as f: dump=json.load(f)
        for rec in dump:
            if rec.get("ts_unix")==ts_unix_val and rec.get("uid")==uid and rec.get("outcome")=="PENDING":
                rec["outcome"]=outcome; rec["exit_price"]=exit_price; rec["real_move"]=real_move; break
        with open(SIGNALS_DUMP,"w") as f: json.dump(dump,f,ensure_ascii=False,indent=2)
    except Exception as e: logger.warning("Dump update: %s",e)

# ============================================================
# CSV ЛОГ
# ============================================================
def build_csv(s: UserSession) -> bytes:
    """Будує CSV з усіх сигналів користувача"""
    fields = ["ts","decision","strength","confidence_score","outcome","entry_price",
              "exit_price","real_move","key_signal","session","market_condition",
              "amd_phase","amd_direction","amd_reason","sweep15m_type","sweep15m_level",
              "dist_above","dist_below","trap_type","oi_change","funding_rate","funding_sent",
              "ob_bias","lsr_bias","liq_signal","logic"]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for sig in s.signal_history:
        row = {
            "ts":               sig.get("time",""),
            "decision":         sig.get("decision",""),
            "strength":         sig.get("strength",""),
            "confidence_score": sig.get("confidence_score",""),
            "outcome":          sig.get("outcome","PENDING"),
            "entry_price":      sig.get("entry_price",""),
            "exit_price":       sig.get("exit_price",""),
            "real_move":        sig.get("real_move",""),
            "key_signal":       sig.get("key_signal",""),
            "session":          sig.get("session",""),
            "market_condition": sig.get("mkt_cond",""),
            "amd_phase":        sig.get("amd_phase",""),
            "amd_direction":    sig.get("amd_direction",""),
            "amd_reason":       sig.get("amd_reason",""),
            "sweep15m_type":    sig.get("sweep_type",""),
            "sweep15m_level":   sig.get("sweep_level",""),
            "dist_above":       sig.get("dist_above",""),
            "dist_below":       sig.get("dist_below",""),
            "trap_type":        sig.get("trap_type",""),
            "oi_change":        sig.get("oi_change",""),
            "funding_rate":     sig.get("funding_rate",""),
            "funding_sent":     sig.get("funding_sent",""),
            "ob_bias":          sig.get("ob_bias",""),
            "lsr_bias":         sig.get("lsr_bias",""),
            "liq_signal":       sig.get("liq_signal",""),
            "logic":            sig.get("logic",""),
        }
        writer.writerow(row)
    return buf.getvalue().encode("utf-8")

# ============================================================
# CHECK SIGNALS
# ============================================================
def check_signals(s: UserSession):
    now=int(time.time()); changed=False
    for sig in s.signal_history:
        if sig.get("outcome"): continue
        if now-sig.get("ts_unix",0)>=900:
            cur=fetch_price()
            if cur:
                entry=sig.get("entry_price",0); dec=sig.get("decision","")
                outcome="WIN" if (dec=="UP" and cur>entry) or (dec=="DOWN" and cur<entry) else "LOSS"
                real_move=round(cur-entry,2)
                sig["outcome"]=outcome; sig["exit_price"]=cur; sig["real_move"]=real_move
                changed=True
                if outcome=="LOSS": _save_error(s,sig)
                update_dump_outcome(sig.get("ts_unix"),s.uid,outcome,cur,real_move)
    if changed: _save_history(s)

def get_stats_text(s: UserSession):
    if not s.signal_history: return "Немає даних."
    checked=[sg for sg in s.signal_history if sg.get("outcome")]
    if not checked: return "Результати перевіряються..."
    wins=[sg for sg in checked if sg["outcome"]=="WIN"]; total=len(checked); wr=round(len(wins)/total*100,1)
    lines=["=== СТАТИСТИКА v4.1 ===","Всього: %d | WIN: %d | Winrate: %.1f%%"%(total,len(wins),wr),""]
    for st in ("HIGH","MEDIUM","LOW"):
        sub=[sg for sg in checked if sg.get("strength")==st]
        if sub:
            w=len([sg for sg in sub if sg["outcome"]=="WIN"])
            lines.append("%s: %d/%d (%.1f%%)"%(st,w,len(sub),round(w/len(sub)*100,1)))
    lines.append("")
    for cond in ("TRENDING","RANGING","CHOPPY"):
        sub=[sg for sg in checked if sg.get("mkt_cond")==cond]
        if sub:
            w=len([sg for sg in sub if sg["outcome"]=="WIN"])
            lines.append("%s: %d/%d (%.1f%%)"%(cond,w,len(sub),round(w/len(sub)*100,1)))
    lines.append("")
    for sess in ("LONDON","NY_OPEN","NY_AFTERNOON","ASIA_ACTIVE","DEAD_HOURS"):
        sub=[sg for sg in checked if sg.get("session")==sess]
        if sub:
            w=len([sg for sg in sub if sg["outcome"]=="WIN"])
            lines.append("%s: %d/%d (%.1f%%)"%(sess,w,len(sub),round(w/len(sub)*100,1)))
    lines.append("\n/errors — деталі помилок")
    return "\n".join(lines)

# ============================================================
# TELEGRAM KEYBOARDS
# ============================================================
def main_keyboard(s: UserSession):
    wallet_btn = "✅ Гаманець підключено" if s.wallet_ok else "🔗 Підключити гаманець"
    auto_btn   = "🔴 Вимкнути авто" if s.auto_active else "🟢 Увімкнути авто"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📘 Як користуватись", callback_data="help_guide"),
         InlineKeyboardButton(wallet_btn, callback_data="wallet_connect")],
        [InlineKeyboardButton("📊 Статистика", callback_data="show_stats"),
         InlineKeyboardButton("📁 Завантажити лог", callback_data="download_log")],
        [InlineKeyboardButton(auto_btn, callback_data="toggle_auto"),
         InlineKeyboardButton("📡 Аналіз зараз", callback_data="analyze_now")],
        [InlineKeyboardButton("💰 Баланс", callback_data="show_balance"),
         InlineKeyboardButton("📰 Новини", callback_data="show_news")],
    ])

WELCOME_TEXT = (
    "👋 Бот запущено!\n\n"
    "Ви будете отримувати сигнали кожні 15 хв по BTC\n"
    "(вище / нижче поточної ціни)\n\n"
    "Маркет: BTC Up or Down — 15 minutes\n"
    "Платформа: Polymarket (Polygon)\n\n"
    "Щоб торгувати — підключіть гаманець нижче 👇"
)

HOW_TO_TEXT = (
    "📘 ЯК КОРИСТУВАТИСЬ\n\n"
    "1. Підключіть гаманець MetaMask\n"
    "   → Експортуйте приватний ключ\n"
    "   → Settings → Security → Export Private Key\n\n"
    "2. Бот аналізує ринок кожні 15 хв\n"
    "   → Відправляє сигнал UP або DOWN\n"
    "   → Ви підтверджуєте суму\n\n"
    "3. Ставка виконується автоматично\n"
    "   → YES = BTC вище\n"
    "   → NO = BTC нижче\n\n"
    "4. Результат через 15 хв\n\n"
    "⚠️ Ніколи не діліться ключем з іншими!\n"
    "Бот використовує його тільки для підпису транзакцій."
)

# ============================================================
# КОМАНДИ
# ============================================================
async def cmd_start(u, c):
    s = get_session(u.effective_user.id)
    await u.message.reply_text(WELCOME_TEXT, reply_markup=main_keyboard(s))

async def cmd_help(u, c):
    s = get_session(u.effective_user.id)
    await u.message.reply_text(HOW_TO_TEXT, reply_markup=main_keyboard(s))

async def cmd_analyze(u, c):
    s = get_session(u.effective_user.id)
    await u.message.reply_text("🔍 Аналіз...")
    await run_cycle_for_user(c.application, s)

async def cmd_stats(u, c):
    s = get_session(u.effective_user.id)
    check_signals(s)
    await u.message.reply_text(get_stats_text(s), reply_markup=main_keyboard(s))

async def cmd_errors(u, c):
    s = get_session(u.effective_user.id)
    check_signals(s)
    if not os.path.exists(s.errors_file):
        await u.message.reply_text("Помилок ще немає."); return
    try:
        with open(s.errors_file) as f: errors=json.load(f)
    except Exception:
        await u.message.reply_text("Помилка читання."); return
    if not errors:
        await u.message.reply_text("Помилок ще немає."); return
    lines=["=== ПОМИЛКИ (%d) ==="%len(errors),""]
    for i,e in enumerate(errors[-10:],1):
        lines.append("%d. %s %s(S:%s) $%.0f->$%.0f(%+.0f)\n   15M=%s,5M=%s|%s|%s\n   Sweep:%s(%sc) Trap:%s AMD:%s\n   KEY: %s\n"%(
            i,e.get("decision","?"),e.get("strength","?"),e.get("confidence_score","?"),
            e.get("entry_price",0),e.get("exit_price",0),e.get("real_move",0),
            e.get("st15m","?"),e.get("st5m","?"),e.get("mkt_cond","?"),e.get("session","?"),
            e.get("sweep_type","?"),e.get("sweep_ago","?"),e.get("trap_type","?"),e.get("amd_phase","?"),
            e.get("key_signal","")[:80]))
    text="\n".join(lines)
    if len(text)>4000: text=text[:4000]+"\n..."
    await u.message.reply_text(text)

async def cmd_balance(u, c):
    s = get_session(u.effective_user.id)
    await u.message.reply_text("Перевіряю баланс...")
    bal, addr = get_balance(s)
    if bal is not None:
        await u.message.reply_text("💰 Баланс: $%.2f USDC\n📍 %s...%s" % (bal, addr[:10], addr[-6:]))
    else:
        await u.message.reply_text("❌ %s" % addr)

async def cmd_news(u, c):
    await u.message.reply_text("📰 Новини BTC:\n\n%s" % get_news())

async def cmd_autoon(u, c):
    s = get_session(u.effective_user.id)
    s.auto_active = True
    await u.message.reply_text("🟢 Авто ON | $%.0f | Min: %s" % (AUTO_BET_SIZE, AUTO_MIN_STRENGTH))

async def cmd_autooff(u, c):
    s = get_session(u.effective_user.id)
    s.auto_active = False
    await u.message.reply_text("🔴 Авто OFF")

async def cmd_trades(u, c):
    s = get_session(u.effective_user.id)
    if not s.trade_history:
        await u.message.reply_text("Немає ставок."); return
    lines=["%s|$%.2f|%s"%(t["decision"],t["amount"],t["time"].strftime("%H:%M")) for t in s.trade_history[-10:]]
    await u.message.reply_text("Останні ставки:\n\n"+"\n".join(lines))

async def cmd_resetstats(u, c):
    s = get_session(u.effective_user.id)
    s.signal_history=[]; s.trade_history=[]
    for f in (s.history_file, s.errors_file):
        try:
            if os.path.exists(f): os.remove(f)
        except Exception: pass
    await u.message.reply_text("✅ Статистика очищена.\n(signals_dump.json збережено)")

async def cmd_dump(u, c):
    """Надсилає signals_dump.json файлом в чат"""
    s = get_session(u.effective_user.id)
    if not os.path.exists(SIGNALS_DUMP):
        await u.message.reply_text("signals_dump.json порожній."); return
    try:
        with open(SIGNALS_DUMP,"rb") as f:
            await u.message.reply_document(
                document=f, filename="signals_dump.json",
                caption="Повний дамп всіх сигналів (%d байт)" % os.path.getsize(SIGNALS_DUMP))
    except Exception as e:
        await u.message.reply_text("Помилка: %s" % e)

async def cmd_status(u, c):
    s = get_session(u.effective_user.id)
    await u.message.reply_text("Збираю дані...")
    p=get_full_payload(s)
    if not p: await u.message.reply_text("Помилка даних"); return
    pr=p["price"]; st=p["structure"]; liq=p["liquidity"]
    pos=p["positioning"]; ctx=p["context"]; manip=p["manipulation"]; amd=p["amd"]
    sw15=liq.get("sweep_15m",{}); sa=liq.get("stops_above") or {}; sb=liq.get("stops_below") or {}
    bc5=liq.get("bos_choch_5m")
    market = _find_btc15m_market()
    market_info = "✅ %s" % market["name"][:50] if market else "❌ Маркет не знайдено"
    wallet_info = "✅ %s...%s" % (s.wallet_address[:10],s.wallet_address[-6:]) if s.wallet_ok else "❌ Не підключено"
    await u.message.reply_text(
        "%s\n💲$%.2f|15m:%+.4f%%|5m:%+.4f%%\nMom:%+.4f%%|Micro:%+.4f%%\n\n"
        "15M=%s|5M=%s|1M=%s\nMkt:%s|Vol:%s|Sess:%s\n"
        "AMD:%s->%s [%s]\nBOS5m:%s\n\n"
        "Sweep15m:%s@%.2f(%dc)\nUp:%.2f(%.3f%%)\nDn:%.2f(%.3f%%)\n\n"
        "Trap:%s|Fund:%+.6f(%s)\nLiqs:%s|OI:%+.4f%%\nBook:%s(%+.1f%%)|L/S:%.2f(%s)\n\n"
        "🏪 Маркет: %s\n👛 Гаманець: %s\n🤖 Авто: %s" % (
            p["timestamp"],pr["current"],pr["chg_15m"],pr["chg_5m"],pr["momentum_3"],pr["micro_mom"],
            st["15m"],st["5m"],st["1m"],ctx["market_condition"],ctx["volatility"],ctx["session"],
            amd.get("phase","NONE"),amd.get("direction","?"),amd.get("reason",""),
            "%s %s@%.2f"%(bc5["type"],bc5["dir"],bc5["level"]) if bc5 else "none",
            sw15.get("type","NONE"),sw15.get("level",0),sw15.get("ago",0),
            sa.get("price",0),liq.get("dist_above",0),sb.get("price",0),liq.get("dist_below",0),
            manip["trap_type"],pos["funding_rate"],pos["funding_sent"],
            pos["liq_signal"],pos["oi_change"],pos["ob_bias"],pos["ob_imbalance"],pos["lsr_ratio"],pos["lsr_bias"],
            market_info,wallet_info,
            "ON($%.0f)"%AUTO_BET_SIZE if s.auto_active else "OFF"
        )
    )

# ============================================================
# ПІДКЛЮЧЕННЯ ГАМАНЦЯ — ConversationHandler
# ============================================================
async def wallet_start(u, c):
    """Починає процес підключення гаманця"""
    query = u.callback_query
    if query:
        await query.answer()
        await query.message.reply_text(
            "🔐 ПІДКЛЮЧЕННЯ ГАМАНЦЯ\n\n"
            "Крок 1/2: Введіть ваш приватний ключ MetaMask\n\n"
            "Де знайти:\n"
            "MetaMask → три крапки → Account Details → Export Private Key\n\n"
            "⚠️ Ніколи не діліться ключем з іншими!\n"
            "Бот використовує його виключно для підпису транзакцій на Polymarket.\n\n"
            "Введіть приватний ключ (64 символи hex):"
        )
    else:
        await u.message.reply_text(
            "🔐 Введіть приватний ключ MetaMask (64 символи hex):"
        )
    return WALLET_KEY

async def wallet_got_key(u, c):
    """Отримали приватний ключ — просимо RPC"""
    key = u.message.text.strip()
    # Базова перевірка довжини
    clean = key.lower().replace("0x","").replace(" ","")
    if len(clean) != 64:
        await u.message.reply_text(
            "❌ Неправильна довжина ключа (%d символів замість 64).\n\nСпробуйте ще раз або /cancel" % len(clean)
        )
        return WALLET_KEY
    c.user_data["pending_key"] = key
    await u.message.reply_text(
        "✅ Ключ прийнято.\n\n"
        "Крок 2/2: RPC URL для Polygon\n\n"
        "Натисніть Enter або відправте будь-який текст щоб використати дефолтний:\n"
        "https://polygon-rpc.com\n\n"
        "Або введіть свій Alchemy/Infura URL:"
    )
    return WALLET_RPC

async def wallet_got_rpc(u, c):
    """Отримали RPC — підключаємось"""
    rpc_input = u.message.text.strip()
    rpc = rpc_input if rpc_input.startswith("http") else DEFAULT_RPC_URL
    key = c.user_data.get("pending_key","")
    s   = get_session(u.effective_user.id)

    await u.message.reply_text("⏳ Підключення до Polygon...")
    ok, result = validate_and_connect_wallet(s, key, rpc)

    if ok:
        bal, _ = get_balance(s)
        bal_str = "$%.2f USDC" % bal if bal is not None else "баланс недоступний"
        await u.message.reply_text(
            "✅ Гаманець підключено успішно!\n\n"
            "📍 Адреса: %s\n"
            "💰 Баланс: %s\n"
            "🌐 Мережа: Polygon Mainnet\n\n"
            "Тепер ви можете торгувати на Polymarket автоматично." % (result, bal_str),
            reply_markup=main_keyboard(s)
        )
    else:
        await u.message.reply_text(
            "❌ Помилка підключення:\n\n%s\n\nСпробуйте ще раз: /wallet" % result,
            reply_markup=main_keyboard(s)
        )
    c.user_data.pop("pending_key", None)
    return ConversationHandler.END

async def wallet_cancel(u, c):
    s = get_session(u.effective_user.id)
    c.user_data.pop("pending_key", None)
    await u.message.reply_text("❌ Підключення скасовано.", reply_markup=main_keyboard(s))
    return ConversationHandler.END

# ============================================================
# CALLBACK BUTTONS
# ============================================================
async def handle_callback(u, c):
    s = get_session(u.effective_user.id)
    q = u.callback_query
    await q.answer()

    if q.data == "help_guide":
        await q.message.reply_text(HOW_TO_TEXT)

    elif q.data == "wallet_connect":
        await wallet_start(u, c)
        return  # ConversationHandler підхопить далі

    elif q.data == "show_stats":
        check_signals(s)
        await q.message.reply_text(get_stats_text(s))

    elif q.data == "download_log":
        if not s.signal_history:
            await q.message.reply_text("Немає сигналів для експорту.")
            return
        csv_bytes = build_csv(s)
        fname = "btc_signals_%d_%s.csv" % (s.uid, datetime.datetime.now().strftime("%Y%m%d_%H%M"))
        await q.message.reply_document(
            document=csv_bytes, filename=fname,
            caption="📁 Лог сигналів (%d записів)" % len(s.signal_history)
        )

    elif q.data == "toggle_auto":
        s.auto_active = not s.auto_active
        status = "🟢 Авто ON ($%.0f/сигнал)" % AUTO_BET_SIZE if s.auto_active else "🔴 Авто OFF"
        await q.message.reply_text(status, reply_markup=main_keyboard(s))

    elif q.data == "analyze_now":
        await q.message.reply_text("🔍 Аналіз...")
        await run_cycle_for_user(c.application, s)

    elif q.data == "show_balance":
        await q.message.reply_text("Перевіряю...")
        bal, addr = get_balance(s)
        if bal is not None:
            await q.message.reply_text("💰 $%.2f USDC\n📍 %s...%s" % (bal, addr[:10], addr[-6:]))
        else:
            await q.message.reply_text("❌ %s" % addr)

    elif q.data == "show_news":
        await q.message.reply_text("📰 Новини:\n\n%s" % get_news())

    elif q.data == "skip":
        s.pending_trade = {}
        await q.edit_message_text("❌ Пропущено.")

    elif q.data.startswith("confirm_"):
        await q.edit_message_text("Введи суму в USDC:")

    elif q.data.startswith("execute_"):
        parts     = q.data.split("_"); direction = parts[1]; amount = float(parts[2])
        await q.edit_message_text("⏳ Виконую ставку...")
        bet = place_bet(s, direction, amount)
        if bet["success"]:
            pot = bet.get("pot", 0)
            s.trade_history.append({"decision":direction,"amount":amount,
                                     "entry":s.pending_trade.get("price",0),
                                     "time":datetime.datetime.now(datetime.timezone.utc)})
            await c.bot.send_message(
                chat_id=u.effective_chat.id,
                text="✅ СТАВКА ВИКОНАНА\n%s | %s\n$%.2f → потенційно +$%.2f" % (
                    direction, bet.get("market_name","Polymarket"), amount, pot))
        else:
            await c.bot.send_message(
                chat_id=u.effective_chat.id,
                text="❌ СТАВКА НЕ ВИКОНАНА\n%s" % bet["error"])
        s.pending_trade = {}

# ============================================================
# ОБРОБКА ТЕКСТОВИХ ПОВІДОМЛЕНЬ (сума ставки)
# ============================================================
async def handle_message(u, c):
    s = get_session(u.effective_user.id)
    if not s.pending_trade or time.time()-s.pending_trade.get("timestamp",0)>600:
        s.pending_trade = {}
        return  # ігноруємо якщо нема активного сигналу
    try:
        amount = float(u.message.text.strip())
        if amount < 1 or amount > 500:
            await u.message.reply_text("⚠️ Сума від $1 до $500"); return
        direction = s.pending_trade["direction"]
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ $%.2f на %s" % (amount, direction),
                                 callback_data="execute_%s_%.2f" % (direction, amount)),
            InlineKeyboardButton("❌ Скасувати", callback_data="skip")
        ]])
        await u.message.reply_text("Підтвердити ставку?", reply_markup=kb)
    except ValueError:
        pass  # не число — ігноруємо

# ============================================================
# EXECUTE AUTO TRADE
# ============================================================
async def execute_auto_trade(app, s: UserSession, payload, result):
    decision = result.get("decision"); strength = result.get("strength","LOW"); logic = result.get("logic","")
    if not decision: return
    strength_order = {"HIGH":3,"MEDIUM":2,"LOW":1}
    if strength_order.get(strength,1) < strength_order.get(AUTO_MIN_STRENGTH,2):
        await app.bot.send_message(chat_id=s.uid,
                                   text="⚡ Сигнал %s(%s) слабкий. Пропускаю." % (decision,strength))
        return
    bet = place_bet(s, decision, AUTO_BET_SIZE)
    if bet["success"]:
        pot = bet.get("pot",0)
        s.trade_history.append({"decision":decision,"amount":AUTO_BET_SIZE,
                                 "entry":payload["price"]["current"],
                                 "time":datetime.datetime.now(datetime.timezone.utc)})
        bal, _ = get_balance(s)
        await app.bot.send_message(
            chat_id=s.uid,
            text="✅ АВТО-СТАВКА\n%s | %s\n$%.2f → +$%.2f\n💰 Баланс: %s\n\n%s" % (
                decision, bet.get("market_name","Polymarket"), AUTO_BET_SIZE, pot,
                "$%.2f"%bal if bal is not None else "N/A", logic))
    else:
        await app.bot.send_message(
            chat_id=s.uid,
            text="❌ АВТО-СТАВКА НЕ ВИКОНАНА\n%s\n%s|%s" % (bet["error"],decision,strength))

# ============================================================
# MAIN CYCLE для конкретного користувача
# ============================================================
async def run_cycle_for_user(app, s: UserSession):
    check_signals(s)
    payload = get_full_payload(s)
    if not payload:
        await app.bot.send_message(chat_id=s.uid, text="❌ Помилка даних Binance"); return
    result = analyze_with_ai(payload, s)
    if not result:
        await app.bot.send_message(chat_id=s.uid, text="❌ Помилка AI"); return

    save_signal_dump(payload, result, s)

    decision  = result.get("decision","UP"); strength = result.get("strength","LOW")
    logic     = result.get("logic","");      score    = result.get("confidence_score",0)
    reasons   = result.get("reasons",[]);    key_sig  = result.get("key_signal","")
    risk_note = result.get("risk_note","");  mkt_cond = result.get("market_condition",payload["context"]["market_condition"])

    liq=payload["liquidity"]; sw15=liq.get("sweep_15m",{})
    manip=payload["manipulation"]; ctx=payload["context"]; amd=payload["amd"]

    sig_record = {
        "decision":decision,"strength":strength,"confidence_score":score,
        "logic":logic,"reasons":reasons,"key_signal":key_sig,
        "entry_price":payload["price"]["current"],"time":payload["timestamp"],
        "ts_unix":payload["ts_unix"],"outcome":None,
        "st15m":payload["structure"]["15m"],"st5m":payload["structure"]["5m"],
        "mkt_cond":mkt_cond,"session":ctx["session"],"volatility":ctx["volatility"],
        "sweep_type":sw15.get("type","NONE"),"sweep_level":sw15.get("level",0),
        "sweep_ago":sw15.get("ago",0),"dist_above":liq.get("dist_above",0),
        "dist_below":liq.get("dist_below",0),"trap_type":manip["trap_type"],
        "amd_phase":amd.get("phase","NONE"),"amd_direction":amd.get("direction"),
        "amd_reason":amd.get("reason",""),
        "funding_rate":payload["positioning"]["funding_rate"],
        "funding_sent":payload["positioning"]["funding_sent"],
        "liq_signal":payload["positioning"]["liq_signal"],
        "oi_change":payload["positioning"]["oi_change"],
        "ob_bias":payload["positioning"]["ob_bias"],
        "lsr_bias":payload["positioning"]["lsr_bias"],
    }
    s.signal_history.append(sig_record)
    _save_history(s)

    dec_ua  = "ВИЩЕ ↑" if decision=="UP" else "НИЖЧЕ ↓"
    str_ua  = {"HIGH":"🔴 СИЛЬНИЙ","MEDIUM":"🟡 СЕРЕДНІЙ","LOW":"🟢 СЛАБКИЙ"}.get(strength,strength)
    reas_s  = "\n".join("• "+r for r in reasons[:3]) if reasons else ""
    risk_s  = "⚠️ %s" % risk_note if risk_note and risk_note.lower() not in ("none","","no") else ""

    main_txt = (
        "📊 СИГНАЛ\n\n"
        "%s | %s | Score:%+d\n"
        "$%.2f | %s | %s\n\n"
        "🎯 %s\n\n"
        "%s\n\n%s\n\n%s"
    ) % (dec_ua, str_ua, score, payload["price"]["current"], mkt_cond, ctx["session"],
         key_sig, logic, reas_s, risk_s)

    if s.auto_active:
        await app.bot.send_message(chat_id=s.uid, text="🤖 АВТО-СИГНАЛ\n\n"+main_txt)
        await execute_auto_trade(app, s, payload, result)
    else:
        btn_dir = "YES" if decision=="UP" else "NO"
        s.pending_trade = {"direction":btn_dir,"amount":None,
                           "timestamp":time.time(),"price":payload["price"]["current"]}
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Так", callback_data="confirm_%s"%btn_dir),
            InlineKeyboardButton("❌ Ні",  callback_data="skip")
        ]])
        await app.bot.send_message(
            chat_id=s.uid,
            text=main_txt+"\n\nВведи суму USDC для ставки:",
            reply_markup=kb)

    wrong = [sg for sg in s.signal_history if sg.get("outcome")=="LOSS"]
    if len(wrong)>0 and len(wrong)%5==0:
        await app.bot.send_message(chat_id=s.uid, text="📉 %d помилок у лозі. /errors" % len(wrong))

# ============================================================
# SCHEDULER — :00 :15 :30 :45 UTC — для ВСІХ активних сесій
# ============================================================
async def periodic(app):
    while True:
        now          = datetime.datetime.now(datetime.timezone.utc)
        mins_to_next = 15 - (now.minute % 15)
        if mins_to_next == 15: mins_to_next = 0
        next_run = now.replace(second=2,microsecond=0) + datetime.timedelta(minutes=mins_to_next)
        if next_run <= now: next_run += datetime.timedelta(minutes=15)
        wait = (next_run - now).total_seconds()
        logger.info("Наступний аналіз через %.0f сек о %s UTC", wait, next_run.strftime("%H:%M"))
        await asyncio.sleep(wait)
        # Виконуємо для всіх активних сесій
        if _sessions:
            for uid, s in list(_sessions.items()):
                try:
                    await run_cycle_for_user(app, s)
                except Exception as e:
                    logger.error("Cycle error uid=%d: %s", uid, e)
        else:
            logger.info("Немає активних сесій")

# ============================================================
# MAIN
# ============================================================
def main():
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # ConversationHandler для підключення гаманця
    wallet_conv = ConversationHandler(
        entry_points=[
            CommandHandler("wallet", wallet_start),
            CallbackQueryHandler(wallet_start, pattern="^wallet_connect$"),
        ],
        states={
            WALLET_KEY: [MessageHandler(filters.TEXT & ~filters.COMMAND, wallet_got_key)],
            WALLET_RPC: [MessageHandler(filters.TEXT & ~filters.COMMAND, wallet_got_rpc)],
        },
        fallbacks=[CommandHandler("cancel", wallet_cancel)],
        per_user=True,
    )

    app.add_handler(wallet_conv)
    app.add_handler(CommandHandler("start",      cmd_start))
    app.add_handler(CommandHandler("help",       cmd_help))
    app.add_handler(CommandHandler("status",     cmd_status))
    app.add_handler(CommandHandler("balance",    cmd_balance))
    app.add_handler(CommandHandler("news",       cmd_news))
    app.add_handler(CommandHandler("stats",      cmd_stats))
    app.add_handler(CommandHandler("errors",     cmd_errors))
    app.add_handler(CommandHandler("trades",     cmd_trades))
    app.add_handler(CommandHandler("autoon",     cmd_autoon))
    app.add_handler(CommandHandler("autooff",    cmd_autooff))
    app.add_handler(CommandHandler("analyze",    cmd_analyze))
    app.add_handler(CommandHandler("resetstats", cmd_resetstats))
    app.add_handler(CommandHandler("dump",       cmd_dump))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    async def on_startup(app):
        asyncio.create_task(periodic(app))
        logger.info("Bot started — multi-user mode")

    app.post_init = on_startup
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
