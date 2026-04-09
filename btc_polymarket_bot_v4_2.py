import asyncio, logging, json, time, datetime, os, csv, io, requests
from openai import OpenAI
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (Application, CommandHandler, CallbackQueryHandler,
                           MessageHandler, ConversationHandler, filters)

# ============================================================
# CONFIG
# ============================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "INSERT_TOKEN")
OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY",     "INSERT_OPENAI_KEY")
DEFAULT_RPC_URL    = os.getenv("ALCHEMY_RPC_URL",    "https://polygon-rpc.com")
FALLBACK_RPC       = "https://polygon-rpc.com"

# USDC на Polygon — правильний контракт
USDC_CONTRACT      = "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"
# Polymarket USDC spender (CTF Exchange)
POLYMARKET_SPENDER = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
# approve.txt — щоб не повторювати approve
APPROVED_FILE      = "approved.txt"

AUTO_MIN_STRENGTH  = os.getenv("AUTO_MIN_STRENGTH", "MEDIUM")
OI_CACHE_FILE      = "oi_cache.json"
SIGNALS_DUMP       = "signals_dump.json"
RISK_FILE          = "risk_settings.json"

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# MULTI-USER STATE
# ============================================================
class UserSession:
    def __init__(self, uid):
        self.uid             = uid
        self.private_key     = os.getenv("POLYMARKET_PRIVATE_KEY", "")
        self.rpc_url         = DEFAULT_RPC_URL
        self.wallet_address  = None
        self.wallet_ok       = False
        self.signal_history  = []
        self.trade_history   = []
        self.auto_active     = False
        self.pending_trade   = {}
        self.risk_percent    = self._load_risk()
        self.history_file    = "signal_history_%d.json" % uid
        self.errors_file     = "errors_log_%d.json"     % uid

    def _load_risk(self):
        """Завантажує % ризику з файлу або повертає дефолт 5%"""
        try:
            if os.path.exists(RISK_FILE):
                with open(RISK_FILE) as f:
                    data = json.load(f)
                    pct = data.get(str(self.uid), 5.0)
                    return float(pct)
        except Exception:
            pass
        return 5.0

    def save_risk(self):
        """Зберігає % ризику у файл"""
        try:
            data = {}
            if os.path.exists(RISK_FILE):
                with open(RISK_FILE) as f:
                    data = json.load(f)
            data[str(self.uid)] = self.risk_percent
            with open(RISK_FILE, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.warning("Risk save: %s", e)

    def calc_bet_size(self, balance: float) -> float:
        """Розраховує суму ставки як % від балансу"""
        if balance <= 0:
            return 0.0
        amount = balance * (self.risk_percent / 100.0)
        amount = max(1.0, amount)                      # мінімум $1
        amount = min(amount, balance * 0.20)           # максимум 20% балансу
        return round(amount, 2)

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
        logger.warning("Hist save: %s", e)

def _save_error(s: UserSession, sig):
    try:
        errors = []
        if os.path.exists(s.errors_file):
            with open(s.errors_file) as f:
                errors = json.load(f)
        errors.append(sig); errors = errors[-300:]
        with open(s.errors_file, "w") as f:
            json.dump(errors, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning("Err save: %s", e)

# ConversationHandler states
WALLET_KEY, WALLET_RPC = range(2)
AUTO_PERCENT = 10
SET_RISK_VAL = 11

# ============================================================
# HTTP
# ============================================================
def safe_get(url, params=None, timeout=30):
    try:
        return requests.get(url, params=params, timeout=timeout).json()
    except Exception as e:
        logger.warning("HTTP GET %s: %s", url, e)
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
        if data and isinstance(data, list): break
        time.sleep(2)
    if not data or not isinstance(data, list):
        data = safe_get("https://api.binance.com/api/v3/klines",
                        {"symbol": "BTCUSDT", "interval": interval, "limit": limit})
    if not data or not isinstance(data, list): return []
    return [{"t":int(k[0]),"o":float(k[1]),"h":float(k[2]),
             "l":float(k[3]),"c":float(k[4]),"v":float(k[5])} for k in data]

def fetch_price():
    data = safe_get("https://fapi.binance.com/fapi/v1/ticker/price", {"symbol":"BTCUSDT"})
    if not data or not isinstance(data, dict):
        data = safe_get("https://api.binance.com/api/v3/ticker/price", {"symbol":"BTCUSDT"})
    if data and isinstance(data, dict): return float(data.get("price",0))
    return None

def fetch_funding():
    data = safe_get("https://fapi.binance.com/fapi/v1/premiumIndex", {"symbol":"BTCUSDT"})
    if not data or not isinstance(data, dict):
        return {"rate":0.0,"sentiment":"NEUTRAL","mark":0.0,"basis":0.0}
    fr=float(data.get("lastFundingRate",0)); mark=float(data.get("markPrice",0))
    idx=float(data.get("indexPrice",mark))
    sent="LONGS_TRAPPED" if fr>0.0005 else "SHORTS_TRAPPED" if fr<-0.0003 else "NEUTRAL"
    return {"rate":fr,"sentiment":sent,"mark":mark,"basis":round(mark-idx,2)}

def fetch_liquidations():
    now_ms=int(time.time()*1000); cutoff=now_ms-900000
    data=safe_get("https://fapi.binance.com/fapi/v1/forceOrders",{"symbol":"BTCUSDT","limit":200})
    if not data or isinstance(data,dict):
        data=safe_get("https://fapi.binance.com/fapi/v1/allForceOrders",{"symbol":"BTCUSDT","limit":200})
    if not data or not isinstance(data,list):
        return {"liq_longs":0.0,"liq_shorts":0.0,"signal":"NEUTRAL","exhaustion":False,"total_usd":0.0}
    recent=[x for x in data if isinstance(x,dict) and int(x.get("time",0))>=cutoff] or data[:50]
    ll=sum(float(x.get("origQty",0))*float(x.get("price",0)) for x in recent if x.get("side")=="SELL")
    ls=sum(float(x.get("origQty",0))*float(x.get("price",0)) for x in recent if x.get("side")=="BUY")
    total=ll+ls
    sig="SHORT_SQUEEZE_FUEL" if ls>ll*2 else "LONG_CASCADE_FUEL" if ll>ls*2 else "NEUTRAL"
    return {"liq_longs":round(ll,2),"liq_shorts":round(ls,2),"signal":sig,
            "exhaustion":total>5_000_000,"total_usd":round(total,2)}

def fetch_oi():
    data=safe_get("https://fapi.binance.com/fapi/v1/openInterest",{"symbol":"BTCUSDT"})
    if not data or not isinstance(data,dict): return 0.0,0.0
    cur=float(data.get("openInterest",0))
    try:
        prev=cur
        if os.path.exists(OI_CACHE_FILE):
            with open(OI_CACHE_FILE) as f: prev=json.load(f).get("oi",cur)
        with open(OI_CACHE_FILE,"w") as f: json.dump({"oi":cur,"ts":ts_unix()},f)
        return cur,round((cur-prev)/prev*100,4) if prev>0 else 0.0
    except Exception: return cur,0.0

def fetch_orderbook():
    try:
        data=safe_get("https://fapi.binance.com/fapi/v1/depth",{"symbol":"BTCUSDT","limit":20})
        if not data or not isinstance(data,dict): return {"imbalance":0.0,"bias":"NEUTRAL"}
        bids=sum(float(b[1]) for b in data.get("bids",[])[:10])
        asks=sum(float(a[1]) for a in data.get("asks",[])[:10])
        total=bids+asks; imb=round((bids-asks)/total*100,2) if total>0 else 0.0
        return {"imbalance":imb,"bias":"BID_HEAVY" if imb>20 else "ASK_HEAVY" if imb<-20 else "BALANCED"}
    except Exception: return {"imbalance":0.0,"bias":"NEUTRAL"}

def fetch_lsr():
    try:
        data=safe_get("https://fapi.binance.com/futures/data/topLongShortPositionRatio",
                      {"symbol":"BTCUSDT","period":"15m","limit":3})
        if not data or not isinstance(data,list): return {"ratio":1.0,"long_pct":50.0,"bias":"NEUTRAL"}
        latest=data[-1]; ratio=float(latest.get("longShortRatio",1.0))
        long_pct=float(latest.get("longAccount",0.5))*100
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
            mid=(nx["l"]+pv["h"])/2; dist=round((price-mid)/price*100,4)
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
# БАЛАНС USDC — правильний контракт 0x3c499c...
# ============================================================
def get_balance(s: UserSession):
    if not s.wallet_ok or not s.wallet_address:
        return None, "Гаманець не підключено"
    try:
        # ERC-20 balanceOf(address) ABI call
        call_data = "0x70a08231000000000000000000000000" + s.wallet_address[2:].lower()
        payload   = {"jsonrpc":"2.0","method":"eth_call",
                     "params":[{"to": USDC_CONTRACT, "data": call_data}, "latest"],"id":1}
        rpcs = [s.rpc_url, FALLBACK_RPC,
                "https://polygon.drpc.org",
                "https://polygon-bor-rpc.publicnode.com",
                "https://rpc.ankr.com/polygon"]
        for rpc in rpcs:
            if not rpc: continue
            try:
                r = requests.post(rpc, json=payload, timeout=10)
                d = r.json()
                result = d.get("result","")
                if result and result not in ("0x","0x0","","0x0000000000000000000000000000000000000000000000000000000000000000"):
                    raw = int(result, 16)
                    # USDC має 6 decimals
                    return round(raw / 1e6, 2), s.wallet_address
            except Exception as e:
                logger.warning("RPC %s balance error: %s", rpc, e)
                continue
        return 0.0, s.wallet_address   # гаманець підключено але баланс 0
    except Exception as e:
        return None, str(e)

# ============================================================
# AUTO-APPROVE USDC — один раз при старті
# ============================================================
def approve_usdc(s: UserSession) -> tuple:
    """
    Апрувить USDC для Polymarket spender.
    Виконується 1 раз — перевіряє approved.txt
    Повертає (success: bool, message: str)
    """
    approved_key = "%s_%d" % (s.wallet_address, s.uid) if s.wallet_address else str(s.uid)
    # Перевіряємо чи вже апрувлено
    if os.path.exists(APPROVED_FILE):
        with open(APPROVED_FILE) as f:
            approved_list = f.read().splitlines()
        if approved_key in approved_list:
            logger.info("USDC already approved for %s", s.wallet_address)
            return True, "Already approved"

    try:
        from web3 import Web3
        from eth_account import Account

        rpc = s.rpc_url or FALLBACK_RPC
        w3  = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 30}))
        if not w3.is_connected():
            w3 = Web3(Web3.HTTPProvider(FALLBACK_RPC, request_kwargs={"timeout": 30}))

        key = s.private_key.strip().replace(" ","").replace("\n","").replace("\r","")
        if key.lower().startswith("0x"): key = key[2:]
        account = Account.from_key(key)

        # ERC-20 approve ABI: approve(address spender, uint256 amount)
        approve_abi = [{
            "name": "approve", "type": "function", "stateMutability": "nonpayable",
            "inputs": [{"name":"spender","type":"address"},{"name":"amount","type":"uint256"}],
            "outputs": [{"name":"","type":"bool"}]
        }]
        usdc = w3.eth.contract(
            address=Web3.to_checksum_address(USDC_CONTRACT),
            abi=approve_abi
        )
        spender    = Web3.to_checksum_address(POLYMARKET_SPENDER)
        allowance  = int(1e12 * 1e6)   # 1 трлн USDC (з 6 decimals) — максимальний approve
        nonce      = w3.eth.get_transaction_count(account.address)
        gas_price  = w3.eth.gas_price

        tx = usdc.functions.approve(spender, allowance).build_transaction({
            "chainId": 137,
            "from":    account.address,
            "nonce":   nonce,
            "gas":     100000,
            "gasPrice": gas_price,
        })
        signed = account.sign_transaction(tx)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

        if receipt.status == 1:
            logger.info("USDC approved. TxHash: %s", tx_hash.hex())
            # Зберігаємо в approved.txt
            with open(APPROVED_FILE, "a") as f:
                f.write(approved_key + "\n")
            return True, "Approved. Tx: 0x%s" % tx_hash.hex()
        else:
            return False, "Approve TX failed (status=0)"

    except ImportError:
        return False, "pip install web3"
    except Exception as e:
        err = str(e)
        logger.error("Approve error: %s", err)
        if "insufficient funds" in err.lower():
            return False, "Недостатньо MATIC для газу"
        return False, "Approve помилка: %s" % err[:200]

# ============================================================
# POLYMARKET — надійний пошук маркету BTC 15m (4 стратегії)
# ============================================================

_market_cache = {
    "id": None, "name": None,
    "token_yes": None, "token_no": None,
    "expires": 0
}

def _parse_end_ts(m: dict) -> int:
    """Витягує timestamp завершення маркету з будь-якого поля"""
    for field in ("endDate","end_date_iso","endDateIso","end_time",
                  "endTime","enddate","end_date","expirationTimestamp"):
        v = m.get(field)
        if v:
            try:
                return int(datetime.datetime.fromisoformat(
                    str(v).replace("Z","+00:00")).timestamp())
            except Exception:
                try:
                    return int(float(v))
                except Exception:
                    pass
    return 0

def _is_btc15m(title: str) -> bool:
    t = title.lower()
    return (("btc" in t or "bitcoin" in t) and "15" in t)

def _is_valid_timing(end_ts: int, now: float) -> bool:
    """
    Маркет повинен:
    - ще не завершитись (end_ts > now)
    - завершуватись не пізніше ніж через 20 хв (поточний раунд, не майбутній)
    - якщо end_ts невідомий (0) — беремо маркет
    """
    if end_ts == 0:
        return True
    return now < end_ts < now + 1200

def _extract_tokens(m: dict, mid: str):
    """Витягує token_yes і token_no з маркету"""
    token_yes = token_no = None
    tokens = m.get("tokens") or m.get("outcomes") or []
    if len(tokens) >= 2:
        for t in tokens:
            oc = (t.get("outcome") or t.get("name") or "").upper().strip()
            if oc in ("YES","UP","HIGHER"):    token_yes = t
            elif oc in ("NO","DOWN","LOWER"):  token_no  = t
        if not token_yes and tokens:          token_yes = tokens[0]
        if not token_no and len(tokens) > 1:  token_no  = tokens[1]

    # Якщо немає token_id — беремо з CLOB
    if not (token_yes and token_yes.get("token_id")):
        try:
            cr = requests.get(
                "https://clob.polymarket.com/markets/%s" % mid,
                timeout=12)
            if cr.status_code == 200:
                ct = cr.json().get("tokens", [])
                if len(ct) >= 2:
                    for t in ct:
                        oc = (t.get("outcome") or "").upper().strip()
                        if oc in ("YES","UP"):    token_yes = t
                        elif oc in ("NO","DOWN"): token_no  = t
                    if not token_yes: token_yes = ct[0]
                    if not token_no:  token_no  = ct[1]
        except Exception as e:
            logger.warning("[Market] CLOB token fetch %s: %s", mid, e)

    return token_yes, token_no

def _find_btc15m_market():
    """
    Надійний пошук активного BTC 15m маркету ТІЛЬКИ ПО ЧАСУ.
    НЕ використовує текстові фільтри типу "15m"/"minutes" — вони не надійні.

    Алгоритм:
    1. Запитуємо всі активні маркети
    2. Фільтруємо: closed=false, tokens=2, question містить btc/bitcoin
    3. Рахуємо diff = endDate - now
    4. Беремо маркет де 60 < diff < 900 секунд (активний поточний раунд)
    5. Кешуємо до кінця раунду
    """
    now_ts = time.time()

    # --- Кеш ---
    if _market_cache["id"] and now_ts < _market_cache["expires"]:
        return _market_cache

    print(f"[Market] Searching... now={datetime.datetime.utcnow().strftime('%H:%M:%S')}")

    best      = None
    best_diff = None

    # Запитуємо кілька сторінок маркетів
    params_list = [
        {"active": "true", "closed": "false", "limit": 100},
        {"active": "true", "closed": "false", "limit": 100, "offset": 100},
        {"active": "true", "closed": "false", "limit": 100, "offset": 200},
    ]

    for params in params_list:
        try:
            r = requests.get(
                "https://gamma-api.polymarket.com/markets",
                params=params, timeout=15)
            if r.status_code != 200:
                print(f"[Market] HTTP {r.status_code}")
                continue
            raw = r.json()
            markets = raw if isinstance(raw, list) else raw.get("markets", raw.get("data", []))
            if not markets:
                break

            for m in markets:
                # Фільтр 1: не закритий
                if m.get("closed", True):
                    continue

                # Фільтр 2: рівно 2 токени (YES/NO)
                tokens = m.get("tokens") or m.get("outcomes") or []
                if len(tokens) != 2:
                    continue

                # Фільтр 3: BTC або Bitcoin в назві (мінімальний текстовий фільтр)
                title = (m.get("question","") or m.get("title","")).lower()
                if "btc" not in title and "bitcoin" not in title:
                    continue

                # Фільтр 4: парсимо endDate
                end_ts = _parse_end_ts(m)
                if end_ts == 0:
                    continue

                # Фільтр 5: КЛЮЧОВИЙ — diff має бути між 60 і 900 секунд
                # (маркет завершується не раніше ніж через 1 хв і не пізніше 15 хв)
                diff = end_ts - now_ts
                if not (60 < diff < 900):
                    continue

                mid = m.get("conditionId") or m.get("condition_id") or m.get("id")
                if not mid:
                    continue

                print(f"[Market] Candidate: {title[:60]} | diff={diff:.0f}s | id={str(mid)[:20]}")

                # Беремо маркет з найменшим diff (найближчий до завершення = поточний раунд)
                if best_diff is None or diff < best_diff:
                    best_diff = diff
                    best = {"id": str(mid), "name": m.get("question","BTC 15m"),
                            "end_ts": end_ts, "raw": m}

        except Exception as e:
            print(f"[Market] API error: {e}")

    if not best:
        # Fallback: якщо не знайдено в 60-900с — розширюємо до 0-1500с
        print("[Market] Strict filter failed, trying relaxed (0-1500s)...")
        for params in params_list[:1]:
            try:
                r = requests.get(
                    "https://gamma-api.polymarket.com/markets",
                    params=params, timeout=15)
                if r.status_code != 200:
                    continue
                raw = r.json()
                markets = raw if isinstance(raw, list) else raw.get("markets", raw.get("data", []))
                for m in markets:
                    if m.get("closed", True): continue
                    tokens = m.get("tokens") or m.get("outcomes") or []
                    if len(tokens) != 2: continue
                    title = (m.get("question","") or m.get("title","")).lower()
                    if "btc" not in title and "bitcoin" not in title: continue
                    end_ts = _parse_end_ts(m)
                    if end_ts == 0: continue
                    diff = end_ts - now_ts
                    if not (0 < diff < 1500): continue
                    mid = m.get("conditionId") or m.get("condition_id") or m.get("id")
                    if not mid: continue
                    if best_diff is None or diff < best_diff:
                        best_diff = diff
                        best = {"id": str(mid), "name": m.get("question","BTC 15m"),
                                "end_ts": end_ts, "raw": m}
            except Exception as e:
                print(f"[Market] Relaxed fallback error: {e}")

    if not best:
        print("[Market] NO ACTIVE BTC 15m MARKET FOUND")
        return None

    # Витягуємо token_yes і token_no
    token_yes, token_no = _extract_tokens(best.get("raw",{}), best["id"])

    # Кешуємо до кінця раунду (мінус 30с запасу)
    expires = best["end_ts"] - 30
    _market_cache.update({
        "id":        best["id"],
        "name":      best["name"],
        "token_yes": token_yes,
        "token_no":  token_no,
        "expires":   expires,
    })
    print(f"[Market] FOUND: {best['name'][:60]} | id={best['id'][:20]} | diff={best_diff:.0f}s | expires in {expires-now_ts:.0f}s")
    return _market_cache


def _prefetch_next_market():
    """Скидає кеш за 2 хв до завершення раунду — щоб наступний маркет знайшовся вчасно"""
    now = time.time()
    if _market_cache["id"] and 0 < _market_cache["expires"] - now < 120:
        logger.info("[Market] Pre-fetching next round (expires in %.0fs)...",
                    _market_cache["expires"] - now)
        _market_cache["id"] = None
        _find_btc15m_market()

# ============================================================
# PLACE BET — GTC order, % від балансу
# ============================================================
def place_bet(s: UserSession, direction: str, amount: float) -> dict:
    """
    direction = "UP" або "DOWN"
    amount    = сума в USDC (вже розрахована з % балансу)
    Повертає {"success": bool, "error": str, "price": float, "pot": float, "market_name": str}
    """
    if not s.wallet_ok or not s.private_key:
        return {"success": False, "error": "Гаманець не підключено. Натисни 🔗 Підключити гаманець"}
    if amount < 1.0:
        return {"success": False, "error": "Мінімальна ставка $1. Поповніть баланс."}

    # Шукаємо маркет (retry 2 рази)
    market = _find_btc15m_market()
    if not market:
        time.sleep(3)
        # скидаємо кеш і пробуємо ще раз
        _market_cache["id"] = None
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
        if key.lower().startswith("0x"): key = key[2:]

        client = ClobClient(host="https://clob.polymarket.com", key=key, chain_id=137)
        price  = float(token.get("price", 0.5))
        if price <= 0 or price >= 1:
            price = 0.5  # fallback
        pot = round(amount / price - amount, 2) if price > 0 else 0.0

        token_id = token.get("token_id") or token.get("id") or token.get("tokenId", "")
        if not token_id:
            return {"success": False, "error": "token_id не знайдено"}

        # GTC замість FOK — order залишається в книзі
        order = client.create_and_post_order(OrderArgs(
            token_id=token_id,
            price=price,
            size=amount,
            side="BUY",
            order_type=OrderType.GTC   # ← ВИПРАВЛЕНО: GTC замість FOK
        ))

        logger.info("Order placed: uid=%d dir=%s amount=%.2f price=%.4f token=%s",
                    s.uid, direction, amount, price, token_id[:20])
        return {
            "success":     True,
            "order":       order,
            "price":       price,
            "pot":         pot,
            "market_name": market["name"],
            "token_id":    token_id,
        }

    except ImportError:
        return {"success": False, "error": "pip install py-clob-client"}
    except Exception as e:
        err = str(e)
        logger.error("Order error uid=%d: %s", s.uid, err)
        # Детальні повідомлення
        if "insufficient" in err.lower() or "balance" in err.lower():
            return {"success": False, "error": "Недостатньо USDC балансу ($%.2f потрібно)" % amount}
        if "allowance" in err.lower() or "approve" in err.lower():
            return {"success": False, "error": "Потрібно approve USDC. Запусти /approve"}
        if "nonce" in err.lower():
            return {"success": False, "error": "Помилка nonce. Спробуй ще раз."}
        if "not found" in err.lower() or "market" in err.lower():
            _market_cache["id"] = None  # скидаємо кеш маркету
            return {"success": False, "error": "Маркет застарів, спробуй ще раз"}
        return {"success": False, "error": err[:200]}

# ============================================================
# ПІДКЛЮЧЕННЯ ГАМАНЦЯ
# ============================================================
def validate_and_connect_wallet(s: UserSession, private_key: str, rpc_url: str):
    try:
        from eth_account import Account
        key = (private_key.strip()
               .replace(" ","").replace("\n","").replace("\r","")
               .replace('"',"").replace("'",""))
        if key.lower().startswith("0x"): key = key[2:]
        if len(key) != 64:
            return False, "Ключ має бути 64 символи hex. Зараз: %d символів." % len(key)
        try:
            int(key, 16)
        except ValueError:
            return False, "Ключ містить неприпустимі символи (тільки 0-9, a-f)."

        account = Account.from_key(key)
        addr    = account.address

        # Перевіряємо підключення до Polygon
        rpcs = [rpc_url, FALLBACK_RPC]
        connected = False
        working_rpc = rpc_url
        for rpc in rpcs:
            try:
                r = requests.post(rpc, json={"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}, timeout=8)
                d = r.json()
                chain = d.get("result","")
                # Polygon = 0x89 = 137
                if chain in ("0x89","137") or (isinstance(chain,str) and int(chain,16)==137):
                    connected=True; working_rpc=rpc; break
                elif chain:  # будь-яка відповідь = підключення є
                    connected=True; working_rpc=rpc; break
            except Exception: continue

        if not connected:
            return False, "Не вдалося підключитись до Polygon.\nСпробуй: https://polygon-rpc.com"

        s.private_key    = key
        s.rpc_url        = working_rpc
        s.wallet_address = addr
        s.wallet_ok      = True
        return True, addr

    except ImportError:
        return False, "eth-account не встановлено на сервері"
    except Exception as e:
        return False, "Помилка: %s" % str(e)

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
# AI PROMPT (v4.1 логіка — без змін)
# ============================================================
SYSTEM_PROMPT = """You are an elite BTC short-term trader. Task: predict if BTC will be HIGHER (UP) or LOWER (DOWN) than current price in exactly 15 minutes on Polymarket.

IMPORTANT: Analyze from scratch every time. Last signal = awareness only, do NOT copy or invert.

TIMEFRAMES: 15m=context | 5m=tactics | 1m=execution

AMD FRAMEWORK:
SWEEP LOW → smart money BOUGHT → UP (+3)
SWEEP HIGH + close BELOW level → reversal DOWN (+3)
SWEEP HIGH + HOLDS ABOVE → bull continuation UP (+1, NOT DOWN)

RANGING RULES:
dist_below<0.1% = bounce UP | dist_above<0.1% = rejection DOWN
dist_below<dist_above AND <0.3% = lean UP | dist_above<dist_below AND <0.3% = lean DOWN
both >0.5% = use momentum + orderbook | CHOP_ZONE = reduce to LOW

SCORING:
+3 AMD MANIPULATION_DONE UP | -3 DOWN
+2 dist_below<0.1% | CHoCH UP 5m | BOS UP 5m | SHORT_SQUEEZE | SWEEP_TRAP_LOW
-2 dist_above<0.1% | CHoCH DOWN | BOS DOWN | LONG_CASCADE | SWEEP_TRAP_HIGH
+1/-1 FVG | BID/ASK_HEAVY | CROWD_SHORT/LONG | micro_mom | OI | session | struct5m
PENALTIES: -1 CHOP_ZONE | DEAD_HOURS | exhaustion

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
        logger.error("AI uid=%d: %s",s.uid,e); return None

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
            "risk_percent":s.risk_percent,
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
    fields = ["ts","decision","strength","confidence_score","outcome","entry_price",
              "exit_price","real_move","key_signal","session","market_condition",
              "amd_phase","amd_direction","amd_reason","sweep15m_type","sweep15m_level",
              "dist_above","dist_below","trap_type","oi_change","funding_rate","funding_sent",
              "ob_bias","lsr_bias","liq_signal","risk_percent","logic"]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for sig in s.signal_history:
        row = {k: sig.get(k, "") for k in fields}
        row["ts"]       = sig.get("time","")
        row["entry_price"] = sig.get("entry_price","")
        row["risk_percent"] = s.risk_percent
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
    lines.append("\nРизик: %.0f%% від балансу за угоду" % s.risk_percent)
    lines.append("/errors — деталі помилок")
    return "\n".join(lines)

# ============================================================
# KEYBOARDS
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
         InlineKeyboardButton("⚙️ Ризик: %.0f%%"%s.risk_percent, callback_data="show_risk")],
        [InlineKeyboardButton("📰 Новини", callback_data="show_news"),
         InlineKeyboardButton("🔑 Approve USDC", callback_data="do_approve")],
    ])

WELCOME_TEXT = (
    "👋 Бот запущено!\n\n"
    "Сигнали кожні 15 хв по BTC (вище / нижче)\n\n"
    "Маркет: BTC Up or Down — 15 minutes\n"
    "Платформа: Polymarket (Polygon)\n"
    "USDC: 0x3c499c...3359 ✅\n\n"
    "Щоб торгувати — підключіть гаманець 👇"
)

HOW_TO_TEXT = (
    "📘 ЯК КОРИСТУВАТИСЬ\n\n"
    "1. Підключіть гаманець MetaMask\n"
    "   MetaMask → ··· → Account Details → Export Private Key\n\n"
    "2. Зробіть Approve USDC (1 раз)\n"
    "   Дозволяє Polymarket використовувати ваш USDC\n\n"
    "3. Встановіть % ризику\n"
    "   /setrisk або кнопка ⚙️ Ризик\n"
    "   Дефолт: 5% від балансу на угоду\n\n"
    "4. Увімкніть авто-торгівлю\n"
    "   /autoon або кнопка 🟢\n\n"
    "5. Бот торгує автоматично кожні 15 хв\n"
    "   YES = BTC вище | NO = BTC нижче\n\n"
    "⚠️ Ніколи не діліться приватним ключем!"
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
    bal, info = get_balance(s)
    if bal is not None:
        bet = s.calc_bet_size(bal)
        await u.message.reply_text(
            "💰 Баланс: $%.2f USDC\n"
            "📍 %s...%s\n"
            "⚙️ Ризик: %.0f%% → $%.2f на угоду" % (
                bal, info[:10], info[-6:], s.risk_percent, bet))
    else:
        await u.message.reply_text("❌ %s" % info)

async def cmd_news(u, c):
    await u.message.reply_text("📰 Новини BTC:\n\n%s" % get_news())

async def cmd_autoon(u, c):
    """Починає ConversationHandler для встановлення % ризику"""
    s = get_session(u.effective_user.id)
    if not s.wallet_ok:
        await u.message.reply_text("❌ Спочатку підключіть гаманець: /wallet"); return
    await u.message.reply_text(
        "⚙️ АВТО-ТОРГІВЛЯ\n\n"
        "Введи % від балансу на кожну угоду\n"
        "Наприклад: 5, 10, 15, 20\n\n"
        "Обмеження: мін $1, макс 20% балансу\n"
        "Поточний: %.0f%%\n\n"
        "Введи число:" % s.risk_percent
    )
    return AUTO_PERCENT

async def got_auto_percent(u, c):
    s = get_session(u.effective_user.id)
    try:
        pct = float(u.message.text.strip().replace("%",""))
        if pct < 1:
            await u.message.reply_text("❌ Мінімум 1%. Введи число ще раз:"); return AUTO_PERCENT
        if pct > 20:
            await u.message.reply_text("❌ Максимум 20% балансу. Введи число ще раз:"); return AUTO_PERCENT
        s.risk_percent = pct
        s.save_risk()
        s.auto_active  = True
        bal, _ = get_balance(s)
        bet = s.calc_bet_size(bal) if bal else "N/A"
        bet_str = "$%.2f" % bet if isinstance(bet, float) else str(bet)
        await u.message.reply_text(
            "✅ Авто-торгівля увімкнена!\n\n"
            "⚙️ Ризик: %.0f%% від балансу на угоду\n"
            "💰 Поточний баланс: %s\n"
            "🎯 Сума ставки: %s\n\n"
            "Бот торгуватиме автоматично о :00 :15 :30 :45 UTC" % (
                pct, "$%.2f"%bal if bal else "N/A", bet_str),
            reply_markup=main_keyboard(s))
    except ValueError:
        await u.message.reply_text("❌ Введи число (наприклад 10):"); return AUTO_PERCENT
    return ConversationHandler.END

async def cmd_autooff(u, c):
    s = get_session(u.effective_user.id)
    s.auto_active = False
    await u.message.reply_text("🔴 Авто-торгівля вимкнена.", reply_markup=main_keyboard(s))

async def cmd_risk(u, c):
    """Показує поточний %"""
    s = get_session(u.effective_user.id)
    bal, _ = get_balance(s)
    bet = s.calc_bet_size(bal) if bal else None
    await u.message.reply_text(
        "⚙️ УПРАВЛІННЯ РИЗИКОМ\n\n"
        "Поточний %: %.0f%% від балансу\n"
        "Баланс: %s\n"
        "Сума ставки: %s\n\n"
        "Змінити: /setrisk" % (
            s.risk_percent,
            "$%.2f" % bal if bal else "N/A",
            "$%.2f" % bet if bet else "N/A"))

async def cmd_setrisk(u, c):
    """Встановлює новий % ризику"""
    await u.message.reply_text(
        "⚙️ ЗМІНА РИЗИКУ\n\n"
        "Введи новий % від балансу (1-20):\n"
        "Наприклад: 5"
    )
    return SET_RISK_VAL

async def got_setrisk_val(u, c):
    s = get_session(u.effective_user.id)
    try:
        pct = float(u.message.text.strip().replace("%",""))
        if pct < 1 or pct > 20:
            await u.message.reply_text("❌ Введи від 1 до 20. Спробуй ще:"); return SET_RISK_VAL
        s.risk_percent = pct
        s.save_risk()
        await u.message.reply_text(
            "✅ Ризик оновлено: %.0f%% від балансу на угоду\n"
            "Збережено у файл." % pct,
            reply_markup=main_keyboard(s))
    except ValueError:
        await u.message.reply_text("❌ Введи число:"); return SET_RISK_VAL
    return ConversationHandler.END

async def cmd_approve(u, c):
    """Approve USDC для Polymarket"""
    s = get_session(u.effective_user.id)
    if not s.wallet_ok:
        await u.message.reply_text("❌ Спочатку підключіть гаманець: /wallet"); return
    await u.message.reply_text("⏳ Виконую approve USDC...\nЦе може зайняти 30-60 сек.")
    ok, msg = approve_usdc(s)
    if ok:
        await u.message.reply_text("✅ USDC approved!\n%s\n\nТепер можна торгувати." % msg)
    else:
        await u.message.reply_text("❌ Approve помилка:\n%s" % msg)

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
    await u.message.reply_text("✅ Статистика очищена.")

async def cmd_dump(u, c):
    s = get_session(u.effective_user.id)
    if not os.path.exists(SIGNALS_DUMP):
        await u.message.reply_text("signals_dump.json порожній."); return
    try:
        with open(SIGNALS_DUMP,"rb") as f:
            await u.message.reply_document(
                document=f, filename="signals_dump.json",
                caption="Повний дамп %d байт" % os.path.getsize(SIGNALS_DUMP))
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
    minfo = "✅ %s"%market["name"][:40] if market else "❌ Не знайдено"
    winfo = "✅ %s...%s"%(s.wallet_address[:10],s.wallet_address[-6:]) if s.wallet_ok else "❌ Не підключено"
    bal, _ = get_balance(s)
    bet = s.calc_bet_size(bal) if bal else 0.0
    await u.message.reply_text(
        "%s\n💲$%.2f|15m:%+.4f%%|5m:%+.4f%%\nMom:%+.4f%%|Micro:%+.4f%%\n\n"
        "15M=%s|5M=%s|1M=%s\nMkt:%s|Vol:%s|Sess:%s\n"
        "AMD:%s->%s [%s]\nBOS5m:%s\n\n"
        "Sweep15m:%s@%.2f(%dc)\nUp:%.2f(%.3f%%)\nDn:%.2f(%.3f%%)\n\n"
        "Trap:%s|Fund:%+.6f(%s)\nLiqs:%s|OI:%+.4f%%\nBook:%s(%+.1f%%)|L/S:%.2f(%s)\n\n"
        "🏪 %s\n👛 %s\n💰 Баланс: %s\n🎯 Ставка: $%.2f (%.0f%%)\n🤖 Авто: %s" % (
            p["timestamp"],pr["current"],pr["chg_15m"],pr["chg_5m"],pr["momentum_3"],pr["micro_mom"],
            st["15m"],st["5m"],st["1m"],ctx["market_condition"],ctx["volatility"],ctx["session"],
            amd.get("phase","NONE"),amd.get("direction","?"),amd.get("reason",""),
            "%s %s@%.2f"%(bc5["type"],bc5["dir"],bc5["level"]) if bc5 else "none",
            sw15.get("type","NONE"),sw15.get("level",0),sw15.get("ago",0),
            sa.get("price",0),liq.get("dist_above",0),sb.get("price",0),liq.get("dist_below",0),
            manip["trap_type"],pos["funding_rate"],pos["funding_sent"],
            pos["liq_signal"],pos["oi_change"],pos["ob_bias"],pos["ob_imbalance"],pos["lsr_ratio"],pos["lsr_bias"],
            minfo,winfo,
            "$%.2f"%bal if bal else "N/A",
            bet, s.risk_percent,
            "ON(%.0f%%)"%s.risk_percent if s.auto_active else "OFF"
        )
    )

# ============================================================
# WALLET CONVERSATION
# ============================================================
async def wallet_start(u, c):
    query = u.callback_query
    msg_obj = query.message if query else u.message
    if query: await query.answer()
    await msg_obj.reply_text(
        "🔐 ПІДКЛЮЧЕННЯ ГАМАНЦЯ\n\n"
        "Крок 1/2: Приватний ключ MetaMask\n\n"
        "Де знайти:\n"
        "MetaMask → ··· → Account Details → Export Private Key\n\n"
        "⚠️ Ключ використовується ТІЛЬКИ для підпису транзакцій.\n"
        "Бот ніколи не передає його третім особам.\n\n"
        "Введіть приватний ключ (64 символи hex або з 0x):"
    )
    return WALLET_KEY

async def wallet_got_key(u, c):
    key = u.message.text.strip()
    clean = key.lower().replace("0x","").replace(" ","")
    if len(clean) != 64:
        await u.message.reply_text("❌ Невірна довжина (%d замість 64).\nСпробуйте ще раз:" % len(clean))
        return WALLET_KEY
    c.user_data["pending_key"] = key
    await u.message.reply_text(
        "✅ Ключ прийнято.\n\n"
        "Крок 2/2: RPC URL для Polygon\n\n"
        "Відправте будь-який текст для дефолтного:\n"
        "https://polygon-rpc.com\n\n"
        "Або введіть свій Alchemy/Infura URL:"
    )
    return WALLET_RPC

async def wallet_got_rpc(u, c):
    rpc_input = u.message.text.strip()
    rpc = rpc_input if rpc_input.startswith("http") else DEFAULT_RPC_URL
    key = c.user_data.get("pending_key","")
    s   = get_session(u.effective_user.id)
    await u.message.reply_text("⏳ Підключення до Polygon Mainnet...")
    ok, result = validate_and_connect_wallet(s, key, rpc)
    if ok:
        bal, _ = get_balance(s)
        bet    = s.calc_bet_size(bal) if bal else 0.0
        await u.message.reply_text(
            "✅ Гаманець підключено!\n\n"
            "📍 Адреса: %s\n"
            "💰 Баланс USDC: %s\n"
            "🌐 Мережа: Polygon Mainnet\n"
            "📄 USDC контракт: 0x3c499c...3359\n\n"
            "Наступний крок: /approve (один раз)\n"
            "Потім: /autoon або /setrisk" % (
                result,
                "$%.2f"%bal if bal else "баланс недоступний"),
            reply_markup=main_keyboard(s))
    else:
        await u.message.reply_text("❌ Помилка:\n\n%s\n\nСпробуйте: /wallet" % result,
                                   reply_markup=main_keyboard(s))
    c.user_data.pop("pending_key", None)
    return ConversationHandler.END

async def wallet_cancel(u, c):
    s = get_session(u.effective_user.id)
    c.user_data.pop("pending_key", None)
    await u.message.reply_text("❌ Скасовано.", reply_markup=main_keyboard(s))
    return ConversationHandler.END

# ============================================================
# CALLBACKS
# ============================================================
async def handle_callback(u, c):
    s = get_session(u.effective_user.id)
    q = u.callback_query
    await q.answer()

    if q.data == "help_guide":
        await q.message.reply_text(HOW_TO_TEXT)

    elif q.data == "wallet_connect":
        await wallet_start(u, c)

    elif q.data == "show_stats":
        check_signals(s)
        await q.message.reply_text(get_stats_text(s))

    elif q.data == "download_log":
        if not s.signal_history:
            await q.message.reply_text("Немає сигналів."); return
        csv_bytes = build_csv(s)
        fname = "btc_signals_%s.csv" % datetime.datetime.now().strftime("%Y%m%d_%H%M")
        await q.message.reply_document(document=csv_bytes, filename=fname,
                                       caption="📁 %d записів" % len(s.signal_history))

    elif q.data == "toggle_auto":
        if not s.wallet_ok:
            await q.message.reply_text("❌ Підключіть гаманець: /wallet"); return
        if s.auto_active:
            s.auto_active = False
            print(f"[AUTO] uid={s.uid} AUTO OFF")
            await q.message.reply_text("🔴 Авто-торгівля ВИМКНЕНА", reply_markup=main_keyboard(s))
        else:
            s.auto_active = True
            print(f"[AUTO] uid={s.uid} AUTO ON risk={s.risk_percent}%")
            bal, _ = get_balance(s)
            bet = s.calc_bet_size(bal) if bal else 0.0
            await q.message.reply_text(
                "🟢 Авто-торгівля УВІМКНЕНА\n\n"
                "⚙️ Ризик: %.0f%% від балансу\n"
                "🎯 Сума ставки: $%.2f\n"
                "💰 Баланс: %s\n\n"
                "Торгівля почнеться на наступному сигналі (:00 :15 :30 :45 UTC)\n"
                "Змінити %: /setrisk" % (
                    s.risk_percent, bet,
                    "$%.2f"%bal if bal else "N/A"),
                reply_markup=main_keyboard(s))

    elif q.data == "analyze_now":
        await q.message.reply_text("🔍 Аналіз...")
        await run_cycle_for_user(c.application, s)

    elif q.data == "show_balance":
        bal, info = get_balance(s)
        if bal is not None:
            bet = s.calc_bet_size(bal)
            await q.message.reply_text("💰 $%.2f USDC\n📍 %s...%s\n🎯 Ставка: $%.2f"%(
                bal, info[:10], info[-6:], bet))
        else:
            await q.message.reply_text("❌ %s" % info)

    elif q.data == "show_risk":
        bal, _ = get_balance(s)
        bet = s.calc_bet_size(bal) if bal else None
        await q.message.reply_text(
            "⚙️ Ризик: %.0f%% від балансу\n💰 Баланс: %s\n🎯 Сума: %s\n\nЗмінити: /setrisk" % (
                s.risk_percent,
                "$%.2f"%bal if bal else "N/A",
                "$%.2f"%bet if bet else "N/A"))

    elif q.data == "do_approve":
        if not s.wallet_ok:
            await q.message.reply_text("❌ Підключіть гаманець: /wallet"); return
        await q.message.reply_text("⏳ Approve USDC...")
        ok, msg = approve_usdc(s)
        if ok:
            await q.message.reply_text("✅ Approved!\n%s" % msg)
        else:
            await q.message.reply_text("❌ %s" % msg)

    elif q.data == "show_news":
        await q.message.reply_text("📰 Новини:\n\n%s" % get_news())

    elif q.data == "skip":
        s.pending_trade = {}
        await q.edit_message_text("❌ Пропущено.")

    elif q.data.startswith("confirm_"):
        await q.edit_message_text("Введи суму в USDC:")

    elif q.data.startswith("execute_"):
        parts = q.data.split("_"); direction = parts[1]; amount = float(parts[2])
        await q.edit_message_text("⏳ Виконую ставку $%.2f..." % amount)
        bet = place_bet(s, direction, amount)
        if bet["success"]:
            s.trade_history.append({"decision":direction,"amount":amount,
                                     "entry":s.pending_trade.get("price",0),
                                     "time":datetime.datetime.now(datetime.timezone.utc)})
            await c.bot.send_message(chat_id=u.effective_chat.id,
                text="✅ СТАВКА ВИКОНАНА\n%s | %s\n$%.2f → потенційно +$%.2f\nGTC order активний"%(
                    direction, bet.get("market_name","Polymarket"), amount, bet.get("pot",0)))
        else:
            await c.bot.send_message(chat_id=u.effective_chat.id,
                text="❌ СТАВКА НЕ ВИКОНАНА\n%s" % bet["error"])
        s.pending_trade = {}

# ============================================================
# HANDLE MESSAGE
# ============================================================
async def handle_message(u, c):
    s = get_session(u.effective_user.id)
    text = u.message.text.strip()

    # Якщо очікуємо % для auto через inline-кнопку
    if c.user_data.get("auto_percent_pending"):
        try:
            pct = float(text.replace("%",""))
            if 1 <= pct <= 20:
                s.risk_percent = pct; s.save_risk(); s.auto_active = True
                c.user_data.pop("auto_percent_pending", None)
                bal, _ = get_balance(s)
                bet = s.calc_bet_size(bal) if bal else 0.0
                await u.message.reply_text(
                    "✅ Авто ON!\nРизик: %.0f%% → $%.2f на угоду" % (pct, bet),
                    reply_markup=main_keyboard(s))
                return
            else:
                await u.message.reply_text("❌ Від 1 до 20. Введи ще раз:"); return
        except ValueError:
            await u.message.reply_text("❌ Введи число (наприклад 10):"); return

    # Сума ставки при активному сигналі
    if s.pending_trade and time.time()-s.pending_trade.get("timestamp",0) <= 600:
        try:
            amount = float(text)
            if amount < 1 or amount > 500:
                await u.message.reply_text("⚠️ Сума від $1 до $500"); return
            direction = s.pending_trade["direction"]
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ $%.2f на %s"%(amount,direction),
                                     callback_data="execute_%s_%.2f"%(direction,amount)),
                InlineKeyboardButton("❌ Скасувати", callback_data="skip")]])
            await u.message.reply_text("Підтвердити?", reply_markup=kb)
        except ValueError:
            pass

# ============================================================
# AUTO TRADE
# ============================================================
async def execute_auto_trade(app, s: UserSession, payload, result):
    decision = result.get("decision"); strength = result.get("strength","LOW"); logic = result.get("logic","")
    print(f"[AUTO TRADE] uid={s.uid} auto_active={s.auto_active} decision={decision} strength={strength}")
    if not decision: return
    strength_order = {"HIGH":3,"MEDIUM":2,"LOW":1}
    if strength_order.get(strength,1) < strength_order.get(AUTO_MIN_STRENGTH,2):
        await app.bot.send_message(chat_id=s.uid,
            text="⚡ Сигнал %s(%s) слабкий. Пропускаю." % (decision,strength))
        return

    # Отримуємо баланс і рахуємо суму
    bal, _ = get_balance(s)
    if bal is None or bal <= 0:
        await app.bot.send_message(chat_id=s.uid,
            text="❌ Авто: не вдалося отримати баланс. Ставка пропущена.")
        return

    amount = s.calc_bet_size(bal)
    if amount < 1:
        await app.bot.send_message(chat_id=s.uid,
            text="❌ Авто: сума ставки $%.2f < $1. Поповніть баланс." % amount)
        return

    bet = place_bet(s, decision, amount)
    if bet["success"]:
        pot = bet.get("pot",0)
        s.trade_history.append({"decision":decision,"amount":amount,
                                 "entry":payload["price"]["current"],
                                 "time":datetime.datetime.now(datetime.timezone.utc)})
        await app.bot.send_message(chat_id=s.uid,
            text="✅ АВТО-СТАВКА\n%s | %s\n💰 Баланс: $%.2f\n🎯 Ставка: $%.2f (%.0f%%)\n📈 Потенційно: +$%.2f\nGTC order активний\n\n%s"%(
                decision, bet.get("market_name","Polymarket"),
                bal, amount, s.risk_percent, pot, logic))
        logger.info("Auto trade: uid=%d dir=%s amount=%.2f bal=%.2f",
                    s.uid, decision, amount, bal)
    else:
        await app.bot.send_message(chat_id=s.uid,
            text="❌ АВТО-СТАВКА НЕ ВИКОНАНА\n%s\n%s|%s"%(bet["error"],decision,strength))

# ============================================================
# MAIN CYCLE
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
        "risk_percent":s.risk_percent,
    }
    s.signal_history.append(sig_record)
    _save_history(s)

    dec_ua  = "ВИЩЕ ↑" if decision=="UP" else "НИЖЧЕ ↓"
    str_ua  = {"HIGH":"🔴 СИЛЬНИЙ","MEDIUM":"🟡 СЕРЕДНІЙ","LOW":"🟢 СЛАБКИЙ"}.get(strength,strength)
    reas_s  = "\n".join("• "+r for r in reasons[:3]) if reasons else ""
    risk_s  = "⚠️ %s"%risk_note if risk_note and risk_note.lower() not in ("none","","no") else ""

    main_txt = (
        "📊 СИГНАЛ\n\n%s | %s | Score:%+d\n$%.2f | %s | %s\n\n🎯 %s\n\n%s\n\n%s\n\n%s"
    ) % (dec_ua, str_ua, score, payload["price"]["current"], mkt_cond, ctx["session"],
         key_sig, logic, reas_s, risk_s)

    print(f"[RUN_CYCLE] uid={s.uid} auto_active={s.auto_active} decision={decision} strength={strength}")
    if s.auto_active:
        print(f"[RUN_CYCLE] Calling execute_auto_trade uid={s.uid}")
        await app.bot.send_message(chat_id=s.uid, text="🤖 АВТО-СИГНАЛ\n\n"+main_txt)
        await execute_auto_trade(app, s, payload, result)
    else:
        btn_dir = "YES" if decision=="UP" else "NO"
        # Рахуємо рекомендовану суму
        bal, _ = get_balance(s)
        rec_amount = s.calc_bet_size(bal) if bal else AUTO_MIN_STRENGTH
        s.pending_trade = {"direction":btn_dir,"amount":None,
                           "timestamp":time.time(),"price":payload["price"]["current"]}
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Так", callback_data="confirm_%s"%btn_dir),
            InlineKeyboardButton("❌ Ні",  callback_data="skip")]])
        bal_str = " (рек. $%.2f = %.0f%% балансу)"%(rec_amount, s.risk_percent) if bal else ""
        await app.bot.send_message(chat_id=s.uid,
            text=main_txt+"\n\nВведи суму USDC"+bal_str+":", reply_markup=kb)

    wrong = [sg for sg in s.signal_history if sg.get("outcome")=="LOSS"]
    if len(wrong)>0 and len(wrong)%5==0:
        await app.bot.send_message(chat_id=s.uid, text="📉 %d помилок. /errors"%len(wrong))

# ============================================================
# SCHEDULER
# ============================================================
async def periodic(app):
    while True:
        now=datetime.datetime.now(datetime.timezone.utc)
        mins_to_next=15-(now.minute%15)
        if mins_to_next==15: mins_to_next=0
        next_run=now.replace(second=2,microsecond=0)+datetime.timedelta(minutes=mins_to_next)
        if next_run<=now: next_run+=datetime.timedelta(minutes=15)
        wait=(next_run-now).total_seconds()
        logger.info("Наступний цикл через %.0f сек о %s UTC",wait,next_run.strftime("%H:%M"))
        await asyncio.sleep(wait)
        # Передзавантажуємо наступний маркет за 2 хв до кінця раунду
        _prefetch_next_market()
        if _sessions:
            for uid,s in list(_sessions.items()):
                try:
                    await run_cycle_for_user(app, s)
                except Exception as e:
                    logger.error("Cycle uid=%d: %s", uid, e)

# ============================================================
# MAIN
# ============================================================
def main():
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Wallet conversation
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

    # Autoon conversation
    autoon_conv = ConversationHandler(
        entry_points=[CommandHandler("autoon", cmd_autoon)],
        states={AUTO_PERCENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, got_auto_percent)]},
        fallbacks=[CommandHandler("cancel", wallet_cancel)],
        per_user=True,
    )

    # Setrisk conversation
    setrisk_conv = ConversationHandler(
        entry_points=[CommandHandler("setrisk", cmd_setrisk)],
        states={SET_RISK_VAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, got_setrisk_val)]},
        fallbacks=[CommandHandler("cancel", wallet_cancel)],
        per_user=True,
    )

    app.add_handler(wallet_conv)
    app.add_handler(autoon_conv)
    app.add_handler(setrisk_conv)
    app.add_handler(CommandHandler("start",      cmd_start))
    app.add_handler(CommandHandler("help",       cmd_help))
    app.add_handler(CommandHandler("status",     cmd_status))
    app.add_handler(CommandHandler("balance",    cmd_balance))
    app.add_handler(CommandHandler("news",       cmd_news))
    app.add_handler(CommandHandler("stats",      cmd_stats))
    app.add_handler(CommandHandler("errors",     cmd_errors))
    app.add_handler(CommandHandler("trades",     cmd_trades))
    app.add_handler(CommandHandler("autooff",    cmd_autooff))
    app.add_handler(CommandHandler("analyze",    cmd_analyze))
    app.add_handler(CommandHandler("resetstats", cmd_resetstats))
    app.add_handler(CommandHandler("dump",       cmd_dump))
    app.add_handler(CommandHandler("approve",    cmd_approve))
    app.add_handler(CommandHandler("risk",       cmd_risk))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    async def on_startup(app):
        asyncio.create_task(periodic(app))
        logger.info("Bot started — multi-user, GTC orders, USDC 0x3c499c...")

    app.post_init = on_startup
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
