import asyncio, logging, json, time, datetime, os, csv, io, requests
from openai import OpenAI
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (Application, CommandHandler, CallbackQueryHandler,
                           MessageHandler, filters)

# ============================================================
# CONFIG
# ============================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "INSERT_TOKEN")
OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY",     "INSERT_OPENAI_KEY")
DEFAULT_RPC_URL    = os.getenv("ALCHEMY_RPC_URL",    "https://polygon-rpc.com")
AUTO_MIN_STRENGTH  = os.getenv("AUTO_MIN_STRENGTH",   "MEDIUM")

USDC_CONTRACT      = "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"
POLYMARKET_SPENDER = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
APPROVED_FILE      = "approved.txt"
OI_CACHE_FILE      = "oi_cache.json"
SIGNALS_DUMP       = "signals_dump.json"

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# SESSION
# ============================================================
class UserSession:
    def __init__(self, uid):
        self.uid            = uid
        self.private_key    = os.getenv("POLYMARKET_PRIVATE_KEY", "")
        self.rpc_url        = DEFAULT_RPC_URL
        self.wallet_address = None
        self.wallet_ok      = False
        self.signal_history = []
        self.trade_history  = []
        self.auto_active    = False
        self.pending_trade  = {}
        self.wallet_state   = None   # "await_key" / "await_rpc"
        self.wallet_tmp_key = None
        self.history_file   = "signal_history_%d.json" % uid
        self.errors_file    = "errors_log_%d.json" % uid
        self._load_history()

    def _load_history(self):
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
            errs = []
            if os.path.exists(self.errors_file):
                with open(self.errors_file) as f: errs = json.load(f)
            errs.append(sig); errs = errs[-300:]
            with open(self.errors_file, "w") as f: json.dump(errs, f, ensure_ascii=False, indent=2)
        except Exception: pass

    def calc_bet(self, balance):
        """10% від балансу. Мін $1, макс $500."""
        if not balance or balance <= 0: return 0.0
        return round(max(1.0, min(balance * 0.10, 500.0)), 2)

_sessions = {}

def get_session(uid):
    if uid not in _sessions:
        _sessions[uid] = UserSession(uid)
    return _sessions[uid]

# ============================================================
# BINANCE
# ============================================================
def safe_get(url, params=None, timeout=15):
    try:
        return requests.get(url, params=params, timeout=timeout).json()
    except Exception as e:
        logger.warning("HTTP %s: %s", url, e)
        return None

def utc_now_str():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def ts_unix():
    return int(time.time())

def fetch_candles(interval, limit):
    for _ in range(3):
        data = safe_get("https://fapi.binance.com/fapi/v1/klines",
                        {"symbol":"BTCUSDT","interval":interval,"limit":limit})
        if data and isinstance(data, list): break
        time.sleep(2)
    if not data or not isinstance(data, list):
        data = safe_get("https://api.binance.com/api/v3/klines",
                        {"symbol":"BTCUSDT","interval":interval,"limit":limit})
    if not data or not isinstance(data, list): return []
    return [{"t":int(k[0]),"o":float(k[1]),"h":float(k[2]),
             "l":float(k[3]),"c":float(k[4]),"v":float(k[5])} for k in data]

def fetch_price():
    for url, p in [("https://fapi.binance.com/fapi/v1/ticker/price",{"symbol":"BTCUSDT"}),
                   ("https://api.binance.com/api/v3/ticker/price",  {"symbol":"BTCUSDT"})]:
        d = safe_get(url, p)
        if d and isinstance(d, dict) and "price" in d:
            return float(d["price"])
    return None

def fetch_funding():
    d = safe_get("https://fapi.binance.com/fapi/v1/premiumIndex",{"symbol":"BTCUSDT"})
    if not d or not isinstance(d,dict): return {"rate":0.0,"sentiment":"NEUTRAL","mark":0.0,"basis":0.0}
    fr=float(d.get("lastFundingRate",0)); mark=float(d.get("markPrice",0)); idx=float(d.get("indexPrice",mark))
    sent="LONGS_TRAPPED" if fr>0.0005 else "SHORTS_TRAPPED" if fr<-0.0003 else "NEUTRAL"
    return {"rate":fr,"sentiment":sent,"mark":mark,"basis":round(mark-idx,2)}

def fetch_liquidations():
    now_ms=int(time.time()*1000); cutoff=now_ms-900000
    d=safe_get("https://fapi.binance.com/fapi/v1/forceOrders",{"symbol":"BTCUSDT","limit":200})
    if not d or isinstance(d,dict):
        d=safe_get("https://fapi.binance.com/fapi/v1/allForceOrders",{"symbol":"BTCUSDT","limit":200})
    if not d or not isinstance(d,list):
        return {"liq_longs":0.0,"liq_shorts":0.0,"signal":"NEUTRAL","exhaustion":False,"total_usd":0.0}
    recent=[x for x in d if isinstance(x,dict) and int(x.get("time",0))>=cutoff] or d[:50]
    ll=sum(float(x.get("origQty",0))*float(x.get("price",0)) for x in recent if x.get("side")=="SELL")
    ls=sum(float(x.get("origQty",0))*float(x.get("price",0)) for x in recent if x.get("side")=="BUY")
    total=ll+ls
    sig="SHORT_SQUEEZE_FUEL" if ls>ll*2 else "LONG_CASCADE_FUEL" if ll>ls*2 else "NEUTRAL"
    return {"liq_longs":round(ll,2),"liq_shorts":round(ls,2),"signal":sig,
            "exhaustion":total>5_000_000,"total_usd":round(total,2)}

def fetch_oi():
    d=safe_get("https://fapi.binance.com/fapi/v1/openInterest",{"symbol":"BTCUSDT"})
    if not d or not isinstance(d,dict): return 0.0,0.0
    cur=float(d.get("openInterest",0))
    try:
        prev=cur
        if os.path.exists(OI_CACHE_FILE):
            with open(OI_CACHE_FILE) as f: prev=json.load(f).get("oi",cur)
        with open(OI_CACHE_FILE,"w") as f: json.dump({"oi":cur,"ts":ts_unix()},f)
        return cur, round((cur-prev)/prev*100,4) if prev>0 else 0.0
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

def get_last_sig_ctx(s):
    if not s.signal_history: return "no_prev"
    last=s.signal_history[-1]
    return "prev=%s outcome=%s move=%+.0f" % (last.get("decision","?"),last.get("outcome","PENDING"),last.get("real_move",0))

def get_full_payload(s):
    c15=fetch_candles("15m",100); c5m=fetch_candles("5m",50); c1m=fetch_candles("1m",30)
    if not c15: return None
    price=c15[-1]["c"]; prev=c15[-2]["c"] if len(c15)>=2 else price
    chg_15m=round((price-prev)/prev*100,4)
    chg_5m=round((c5m[-1]["c"]-c5m[-4]["c"])/c5m[-4]["c"]*100,4) if len(c5m)>=4 else 0.0
    momentum_3=round((c15[-1]["c"]-c15[-4]["c"])/c15[-4]["c"]*100,4) if len(c15)>=4 else 0.0
    micro_mom=round((c1m[-1]["c"]-c1m[-4]["c"])/c1m[-4]["c"]*100,4) if len(c1m)>=4 else 0.0
    st15m=market_structure(c15) if len(c15)>=6 else "RANGING"
    st5m=market_structure(c5m) if len(c5m)>=6 else "RANGING"
    st1m=market_structure(c1m) if len(c1m)>=6 else "RANGING"
    sw15=liq_sweep(c15); sw5=liq_sweep(c5m) if c5m else {"type":"NONE","level":0.0,"ago":0}
    sw1=liq_sweep(c1m) if c1m else {"type":"NONE","level":0.0,"ago":0}
    sa,sb=stop_clusters(c15,price)
    fvg5_a,fvg5_b=find_fvg(c5m[-30:],price) if len(c5m)>=5 else (None,None)
    fvg1_a,fvg1_b=find_fvg(c1m[-20:],price) if len(c1m)>=5 else (None,None)
    bc5m=bos_choch(c5m,st15m) if len(c5m)>=5 else None
    manip=detect_manipulation(c5m[-10:] if len(c5m)>=10 else c15[-10:],sw5,price)
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
        "liquidity":{"sweep_15m":sw15,"sweep_5m":sw5,"sweep_1m":sw1,
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
# БАЛАНС USDC
# ============================================================
def get_balance(s):
    if not s.wallet_ok or not s.wallet_address:
        return None, "Wallet not connected"
    call_data = "0x70a08231000000000000000000000000" + s.wallet_address[2:].lower()
    payload = {"jsonrpc":"2.0","method":"eth_call",
               "params":[{"to":USDC_CONTRACT,"data":call_data},"latest"],"id":1}
    rpcs = [s.rpc_url, "https://polygon-rpc.com",
            "https://polygon.drpc.org", "https://rpc.ankr.com/polygon"]
    for rpc in rpcs:
        if not rpc: continue
        try:
            r = requests.post(rpc, json=payload, timeout=10)
            res = r.json().get("result","")
            if res and res not in ("0x","0x0","","0x0000000000000000000000000000000000000000000000000000000000000000"):
                return round(int(res,16)/1e6, 2), s.wallet_address
        except Exception: continue
    return 0.0, s.wallet_address

# ============================================================
# APPROVE USDC
# ============================================================
def approve_usdc(s):
    key_id = "%s_%d" % (s.wallet_address or "", s.uid)
    if os.path.exists(APPROVED_FILE):
        with open(APPROVED_FILE) as f:
            if key_id in f.read().splitlines():
                return True, "Already approved"
    try:
        from web3 import Web3
        from eth_account import Account
        key = s.private_key.strip().replace(" ","").replace("\n","").replace("\r","")
        if key.lower().startswith("0x"): key = key[2:]
        rpc = s.rpc_url or "https://polygon-rpc.com"
        w3  = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout":30}))
        if not w3.is_connected():
            w3 = Web3(Web3.HTTPProvider("https://polygon-rpc.com", request_kwargs={"timeout":30}))
        account = Account.from_key(key)
        abi = [{"name":"approve","type":"function","stateMutability":"nonpayable",
                "inputs":[{"name":"spender","type":"address"},{"name":"amount","type":"uint256"}],
                "outputs":[{"name":"","type":"bool"}]}]
        usdc = w3.eth.contract(address=Web3.to_checksum_address(USDC_CONTRACT), abi=abi)
        spender = Web3.to_checksum_address(POLYMARKET_SPENDER)
        tx = usdc.functions.approve(spender, int(1e18)).build_transaction({
            "chainId":137, "from":account.address,
            "nonce":w3.eth.get_transaction_count(account.address),
            "gas":100000, "gasPrice":w3.eth.gas_price})
        signed  = account.sign_transaction(tx)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        if receipt.status == 1:
            with open(APPROVED_FILE,"a") as f: f.write(key_id+"\n")
            return True, "Approved. Tx: 0x%s" % tx_hash.hex()
        return False, "TX failed"
    except ImportError: return False, "pip install web3"
    except Exception as e:
        err=str(e)
        if "insufficient" in err.lower(): return False, "Not enough MATIC for gas"
        return False, err[:200]

# ============================================================
# POLYMARKET — пошук маркету
# ============================================================
_market_cache = {"id":None,"name":None,"token_yes":None,"token_no":None,"expires":0}

def _extract_tokens(m, mid):
    ty = tn = None
    tokens = m.get("tokens") or m.get("outcomes") or []
    if len(tokens) >= 2:
        for t in tokens:
            oc = (t.get("outcome") or t.get("name") or "").upper().strip()
            if oc in ("YES","UP","HIGHER"): ty = t
            elif oc in ("NO","DOWN","LOWER"): tn = t
        if not ty: ty = tokens[0]
        if not tn and len(tokens)>1: tn = tokens[1]
    if not (ty and ty.get("token_id")):
        try:
            r = requests.get("https://clob.polymarket.com/markets/%s" % mid, timeout=12)
            if r.status_code == 200:
                ct = r.json().get("tokens",[])
                if len(ct) >= 2:
                    for t in ct:
                        oc = (t.get("outcome") or "").upper()
                        if oc in ("YES","UP"): ty = t
                        elif oc in ("NO","DOWN"): tn = t
                    if not ty: ty = ct[0]
                    if not tn: tn = ct[1]
        except Exception as e:
            print("[Market] CLOB token fetch: %s" % e)
    return ty, tn

def _find_btc15m_market():
    """
    Знаходить активний BTC 15m маркет.
    Стратегія: closed=false + btc/bitcoin в назві + рівно 2 токени.
    Кеш 13 хвилин.
    """
    now_ts = time.time()
    if _market_cache["id"] and now_ts < _market_cache["expires"]:
        return _market_cache

    print("[Market] Searching %s UTC" % datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S"))
    found = None

    for params in [
        {"closed":"false","limit":100},
        {"closed":"false","limit":100,"offset":100},
        {"active":"true","closed":"false","limit":100},
        {"active":"true","limit":100},
    ]:
        try:
            r = requests.get("https://gamma-api.polymarket.com/markets",
                             params=params, timeout=15)
            print("[Market] HTTP %d params=%s" % (r.status_code, params))
            if r.status_code != 200: continue
            raw   = r.json()
            mlist = raw if isinstance(raw,list) else raw.get("markets", raw.get("data",[]))
            if not mlist: continue
            print("[Market] Got %d markets" % len(mlist))
            for m in mlist:
                if m.get("closed",True): continue
                title = (m.get("question","") or m.get("title","")).lower()
                if "btc" not in title and "bitcoin" not in title: continue
                tokens = m.get("tokens") or m.get("outcomes") or []
                if len(tokens) != 2: continue
                mid = (m.get("conditionId") or m.get("condition_id") or m.get("id","")).strip()
                if not mid: continue
                q = m.get("question","") or m.get("title","BTC")
                print("[Market] CANDIDATE: %s" % q[:70])
                found = {"id":mid,"name":q,"raw":m}
                break
        except Exception as e:
            print("[Market] Error: %s" % e)
        if found: break

    if not found:
        print("[Market] NOT FOUND")
        return None

    ty, tn = _extract_tokens(found["raw"], found["id"])
    _market_cache.update({"id":found["id"],"name":found["name"],
                          "token_yes":ty,"token_no":tn,"expires":now_ts+780})
    print("[Market] USING: %s id=%s" % (found["name"][:60], found["id"][:20]))
    return _market_cache

# ============================================================
# PLACE BET
# ============================================================
def place_bet(s, direction, amount):
    if not s.wallet_ok:
        return {"success":False,"error":"Wallet not connected. Use /wallet"}
    if amount < 1:
        return {"success":False,"error":"Min $1"}

    market = _find_btc15m_market()
    if not market:
        _market_cache["id"] = None
        time.sleep(2)
        market = _find_btc15m_market()
    if not market:
        return {"success":False,"error":"Market BTC 15m not found. Retry later."}

    token = market["token_yes"] if direction=="UP" else market["token_no"]
    if not token:
        return {"success":False,"error":"Token for %s not found" % direction}

    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OrderArgs, OrderType
        key = s.private_key.strip().replace(" ","").replace("\n","").replace("\r","")
        if key.lower().startswith("0x"): key = key[2:]
        client = ClobClient(host="https://clob.polymarket.com", key=key, chain_id=137)
        price  = float(token.get("price",0.5))
        if price<=0 or price>=1: price=0.5
        pot    = round(amount/price-amount,2) if price>0 else 0.0
        tid    = token.get("token_id") or token.get("id") or token.get("tokenId","")
        if not tid: return {"success":False,"error":"token_id not found"}
        order  = client.create_and_post_order(OrderArgs(
            token_id=tid, price=price, size=amount, side="BUY", order_type=OrderType.GTC))
        print("[BET] OK uid=%d dir=%s amount=%.2f price=%.4f" % (s.uid,direction,amount,price))
        return {"success":True,"order":order,"price":price,"pot":pot,"market_name":market["name"]}
    except ImportError:
        return {"success":False,"error":"pip install py-clob-client"}
    except Exception as e:
        err=str(e)
        print("[BET] FAIL uid=%d: %s" % (s.uid,err))
        if "insufficient" in err.lower() or "balance" in err.lower():
            return {"success":False,"error":"Not enough USDC balance"}
        if "allowance" in err.lower() or "approve" in err.lower():
            return {"success":False,"error":"Need approve. Use /approve"}
        if "nonce" in err.lower():
            return {"success":False,"error":"Nonce error. Retry."}
        _market_cache["id"] = None  # скидаємо кеш маркету при помилці
        return {"success":False,"error":err[:200]}

# ============================================================
# WALLET CONNECT
# ============================================================
def validate_and_connect_wallet(s, private_key, rpc_url):
    try:
        from eth_account import Account
        key = private_key.strip().replace(" ","").replace("\n","").replace("\r","")
        if key.lower().startswith("0x"): key=key[2:]
        if len(key)!=64:
            return False, "Key must be 64 hex chars. Got: %d" % len(key)
        try: int(key,16)
        except ValueError: return False, "Key has invalid characters"
        account = Account.from_key(key)
        addr    = account.address
        rpcs    = [rpc_url, "https://polygon-rpc.com"]
        connected = False
        for rpc in rpcs:
            try:
                r = requests.post(rpc, json={"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}, timeout=8)
                if r.json().get("result"): connected=True; break
            except Exception: continue
        if not connected: return False, "Cannot connect to Polygon RPC"
        s.private_key=key; s.rpc_url=rpc_url; s.wallet_address=addr; s.wallet_ok=True
        return True, addr
    except ImportError: return False, "eth-account not installed"
    except Exception as e: return False, str(e)

# ============================================================
# NEWS
# ============================================================
def get_news():
    try:
        data=safe_get("https://min-api.cryptocompare.com/data/v2/news/",{"categories":"BTC,Bitcoin","lTs":0})
        if data and "Data" in data and data["Data"]:
            lines=[]; bkw=["bull","surge","rally","rise","gain","buy","etf","adoption"]
            skw=["bear","drop","fall","crash","dump","sell","ban","hack","fear"]
            for item in data["Data"][:6]:
                t=item.get("title","").lower(); p=sum(1 for k in bkw if k in t); n=sum(1 for k in skw if k in t)
                sent="BULLISH" if p>n else "BEARISH" if n>p else "NEUTRAL"
                lines.append("[%s] %s"%(sent,item.get("title","")[:70]))
            return "\n".join(lines)
    except Exception: pass
    return "News unavailable"

# ============================================================
# AI
# ============================================================
SYSTEM_PROMPT = """You are an elite BTC short-term trader. Predict UP or DOWN in 15 minutes on Polymarket.
Analyze from scratch every time. Do NOT copy previous signal.
TIMEFRAMES: 15m=context | 5m=tactics | 1m=execution
AMD: SWEEP_LOW+close_above = UP (+3) | SWEEP_HIGH+close_below = DOWN (+3) | SWEEP_HIGH+holds = UP (+1)
RANGING: dist_below<0.1%=UP | dist_above<0.1%=DOWN | dist_below<dist_above=lean UP
SCORING: +3AMD_UP|-3AMD_DOWN | +2CHoCH_UP/BOS_UP/SQUEEZE | -2CHoCH_DOWN/CASCADE | +1/-1 FVG/OB/session
STRENGTH: >=5=HIGH | 3-4=MEDIUM | 1-2=LOW | ALWAYS UP or DOWN
OUTPUT JSON: {"decision":"UP or DOWN","strength":"HIGH or MEDIUM or LOW","confidence_score":<int>,
"market_condition":"TRENDING or RANGING or CHOPPY","amd_used":true/false,
"key_signal":"one sentence","logic":"2-3 sentences Ukrainian","reasons":["r1","r2","r3"],"risk_note":"risk or NONE"}"""

def analyze_with_ai(payload, s):
    try:
        client=OpenAI(api_key=OPENAI_API_KEY)
        liq=payload["liquidity"]; pos=payload["positioning"]; pr=payload["price"]
        st=payload["structure"]; ctx=payload["context"]; manip=payload["manipulation"]; amd=payload["amd"]
        sw15=liq.get("sweep_15m",{}); sw5=liq.get("sweep_5m",{}); sw1=liq.get("sweep_1m",{})
        bc5=liq.get("bos_choch_5m"); f5a=liq.get("fvg5_above"); f5b=liq.get("fvg5_below")
        f1a=liq.get("fvg1_above"); f1b=liq.get("fvg1_below")
        sa=liq.get("stops_above") or {}; sb=liq.get("stops_below") or {}
        msg=(
            "Time:%s Session:%s(boost=%+d) Last:%s\n"
            "PRICE:$%.2f 15m:%+.4f%% 5m:%+.4f%% Mom3:%+.4f%% Micro1m:%+.4f%%\n"
            "Mark:$%.2f Basis:%+.2f\n"
            "STRUCT:15m=%s 5m=%s 1m=%s Mkt:%s Vol:%s(%.4f%%)\n"
            "AMD:phase=%s dir=%s conf=%d [%s]\n"
            "Sweep15m:%s@%.2f(%dc) Sweep5m:%s@%.2f(%dc) Sweep1m:%s@%.2f(%dc)\n"
            "StopsUp:%s dist=%.3f%% StopsDn:%s dist=%.3f%%\n"
            "FVG5m:up=%s dn=%s FVG1m:up=%s dn=%s BOS5m:%s\n"
            "Manip:trap=%s hint=%s\n"
            "Fund:%+.6f(%s) LiqL:$%.0f LiqS:$%.0f Sig:%s Exhaust:%s\n"
            "OI:%.0f OI_chg:%+.4f%% Book:%s(%+.1f%%) L/S:%.3f(%s) CrowdLong:%.1f%%"
        ) % (
            payload["timestamp"],ctx["session"],ctx["session_boost"],ctx["last_signal"],
            pr["current"],pr["chg_15m"],pr["chg_5m"],pr["momentum_3"],pr["micro_mom"],
            pr["mark"],pr["basis"],
            st["15m"],st["5m"],st["1m"],ctx["market_condition"],ctx["volatility"],ctx["vol_score"],
            amd.get("phase","NONE"),amd.get("direction","?"),amd.get("confidence",0),amd.get("reason",""),
            sw15.get("type","N"),sw15.get("level",0),sw15.get("ago",0),
            sw5.get("type","N"),sw5.get("level",0),sw5.get("ago",0),
            sw1.get("type","N"),sw1.get("level",0),sw1.get("ago",0),
            sa.get("type","none"),liq.get("dist_above",999),
            sb.get("type","none"),liq.get("dist_below",999),
            ("dist=%.3f%%"%f5a["dist"]) if f5a else "none",
            ("dist=%.3f%%"%f5b["dist"]) if f5b else "none",
            ("dist=%.3f%%"%f1a["dist"]) if f1a else "none",
            ("dist=%.3f%%"%f1b["dist"]) if f1b else "none",
            ("%s %s@%.2f"%(bc5["type"],bc5["dir"],bc5["level"])) if bc5 else "none",
            manip["trap_type"],str(manip["reversal_signal"]),
            pos["funding_rate"],pos["funding_sent"],
            pos["liq_longs"],pos["liq_shorts"],pos["liq_signal"],pos["exhaustion"],
            pos["oi"],pos["oi_change"],pos["ob_bias"],pos["ob_imbalance"],
            pos["lsr_ratio"],pos["lsr_bias"],pos["crowd_long"]
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
def save_signal_dump(payload, result, s):
    try:
        dump=[]
        if os.path.exists(SIGNALS_DUMP):
            with open(SIGNALS_DUMP) as f: dump=json.load(f)
        liq=payload["liquidity"]; pos=payload["positioning"]; pr=payload["price"]
        st=payload["structure"]; ctx=payload["context"]; amd=payload["amd"]; manip=payload["manipulation"]
        sw15=liq.get("sweep_15m",{}); sw5=liq.get("sweep_5m",{})
        rec={
            "uid":s.uid,"ts":payload["timestamp"],"ts_unix":payload["ts_unix"],
            "outcome":"PENDING","exit_price":None,"real_move":None,
            "decision":result.get("decision"),"strength":result.get("strength"),
            "confidence_score":result.get("confidence_score"),"key_signal":result.get("key_signal"),
            "logic":result.get("logic"),"reasons":result.get("reasons"),
            "market_condition":result.get("market_condition"),
            "price":pr["current"],"chg_15m":pr["chg_15m"],"chg_5m":pr["chg_5m"],
            "momentum_3":pr["momentum_3"],"micro_mom":pr["micro_mom"],
            "st15m":st["15m"],"st5m":st["5m"],"st1m":st["1m"],
            "session":ctx["session"],"volatility":ctx["volatility"],"mkt_cond":ctx["market_condition"],
            "amd_phase":amd.get("phase"),"amd_direction":amd.get("direction"),"amd_reason":amd.get("reason"),
            "sweep15m_type":sw15.get("type"),"sweep15m_level":sw15.get("level"),
            "sweep5m_type":sw5.get("type"),"sweep5m_level":sw5.get("level"),
            "dist_above":liq.get("dist_above"),"dist_below":liq.get("dist_below"),
            "trap_type":manip["trap_type"],"funding_rate":pos["funding_rate"],
            "oi_change":pos["oi_change"],"ob_bias":pos["ob_bias"],
        }
        dump.append(rec); dump=dump[-2000:]
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
# CSV LOG
# ============================================================
def build_csv(s):
    fields=["ts","decision","strength","confidence_score","outcome","entry_price",
            "exit_price","real_move","key_signal","session","mkt_cond","amd_phase",
            "amd_direction","sweep_type","dist_above","dist_below","trap_type",
            "oi_change","ob_bias","logic"]
    buf=io.StringIO(); writer=csv.DictWriter(buf,fieldnames=fields,extrasaction="ignore")
    writer.writeheader()
    for sig in s.signal_history:
        row={k:sig.get(k,"") for k in fields}
        row["ts"]=sig.get("time",""); row["entry_price"]=sig.get("entry_price","")
        row["sweep_type"]=sig.get("sweep_type","")
        writer.writerow(row)
    return buf.getvalue().encode("utf-8")

# ============================================================
# CHECK SIGNALS
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
                real_move=round(cur-entry,2)
                sig["outcome"]=outcome; sig["exit_price"]=cur; sig["real_move"]=real_move
                changed=True
                if outcome=="LOSS": s.save_error(sig)
                update_dump_outcome(sig.get("ts_unix"),s.uid,outcome,cur,real_move)
    if changed: s.save_history()

def get_stats_text(s):
    if not s.signal_history: return "No data."
    checked=[sg for sg in s.signal_history if sg.get("outcome")]
    if not checked: return "Checking results..."
    wins=[sg for sg in checked if sg["outcome"]=="WIN"]; total=len(checked)
    wr=round(len(wins)/total*100,1)
    lines=["=== STATS v4.1 ===","Total: %d | WIN: %d | Winrate: %.1f%%"%(total,len(wins),wr),""]
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
    lines.append("\n/errors — details")
    return "\n".join(lines)

# ============================================================
# KEYBOARD
# ============================================================
def main_keyboard(s):
    wallet_btn = "Wallet OK" if s.wallet_ok else "Connect Wallet"
    auto_btn   = "AUTO OFF" if s.auto_active else "AUTO ON"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(wallet_btn,  callback_data="wallet_connect"),
         InlineKeyboardButton(auto_btn,    callback_data="toggle_auto")],
        [InlineKeyboardButton("Balance",   callback_data="show_balance"),
         InlineKeyboardButton("Analyze",   callback_data="analyze_now")],
        [InlineKeyboardButton("Stats",     callback_data="show_stats"),
         InlineKeyboardButton("Download Log", callback_data="download_log")],
        [InlineKeyboardButton("News",      callback_data="show_news"),
         InlineKeyboardButton("Approve USDC", callback_data="do_approve")],
    ])

WELCOME = (
    "BTC Polymarket Bot v4.1\n\n"
    "Signals every 15 min :00 :15 :30 :45 UTC\n"
    "Market: BTC Up or Down - 15 minutes\n"
    "Stake: 10% of balance per trade\n\n"
    "1. Connect Wallet\n"
    "2. /approve (once)\n"
    "3. Press AUTO ON\n\n"
    "/testmarket — test market search\n"
    "/status — market data\n"
    "/errors — loss details\n"
    "/dump — export all signals"
)

# ============================================================
# COMMANDS
# ============================================================
async def cmd_start(u,c):
    s=get_session(u.effective_user.id)
    await u.message.reply_text(WELCOME, reply_markup=main_keyboard(s))

async def cmd_help(u,c):
    s=get_session(u.effective_user.id)
    await u.message.reply_text(WELCOME, reply_markup=main_keyboard(s))

async def cmd_analyze(u,c):
    s=get_session(u.effective_user.id)
    await u.message.reply_text("Analyzing...")
    await run_cycle(c.application, s)

async def cmd_stats(u,c):
    s=get_session(u.effective_user.id)
    check_signals(s)
    await u.message.reply_text(get_stats_text(s))

async def cmd_errors(u,c):
    s=get_session(u.effective_user.id)
    check_signals(s)
    if not os.path.exists(s.errors_file):
        await u.message.reply_text("No errors yet."); return
    try:
        with open(s.errors_file) as f: errors=json.load(f)
    except Exception:
        await u.message.reply_text("Read error."); return
    if not errors:
        await u.message.reply_text("No errors yet."); return
    lines=["=== ERRORS (%d) ===" % len(errors),""]
    for i,e in enumerate(errors[-10:],1):
        lines.append("%d. %s %s(S:%s) $%.0f->$%.0f(%+.0f)\n   %s|%s|%s\n   Sweep:%s Trap:%s AMD:%s\n   KEY: %s\n" % (
            i,e.get("decision","?"),e.get("strength","?"),e.get("confidence_score","?"),
            e.get("entry_price",0),e.get("exit_price",0),e.get("real_move",0),
            e.get("st15m","?"),e.get("mkt_cond","?"),e.get("session","?"),
            e.get("sweep_type","?"),e.get("trap_type","?"),e.get("amd_phase","?"),
            e.get("key_signal","")[:80]))
    text="\n".join(lines)
    if len(text)>4000: text=text[:4000]+"\n..."
    await u.message.reply_text(text)

async def cmd_autoon(u,c):
    s=get_session(u.effective_user.id)
    if not s.wallet_ok:
        await u.message.reply_text("First connect wallet. Press 'Connect Wallet'"); return
    s.auto_active=True
    bal,_=get_balance(s)
    bet=s.calc_bet(bal) if bal else 0.0
    bal_s=("$%.2f"%bal) if bal else "N/A"
    print("[AUTO] uid=%d ON bal=%s bet=%.2f" % (s.uid,bal_s,bet))
    await u.message.reply_text(
        "AUTO ON\n\nBalance: %s\nStake: $%.2f (10%%)\n\n:00 :15 :30 :45 UTC" % (bal_s,bet),
        reply_markup=main_keyboard(s))

async def cmd_autooff(u,c):
    s=get_session(u.effective_user.id)
    s.auto_active=False
    await u.message.reply_text("AUTO OFF", reply_markup=main_keyboard(s))

async def cmd_balance(u,c):
    s=get_session(u.effective_user.id)
    await u.message.reply_text("Checking...")
    bal,info=get_balance(s)
    if bal is not None:
        bet=s.calc_bet(bal)
        await u.message.reply_text("Balance: $%.2f USDC\n%s...%s\nNext stake: $%.2f (10%%)" % (
            bal,info[:10],info[-6:],bet))
    else:
        await u.message.reply_text("Error: %s" % info)

async def cmd_news(u,c):
    await u.message.reply_text("News:\n\n%s" % get_news())

async def cmd_approve(u,c):
    s=get_session(u.effective_user.id)
    if not s.wallet_ok:
        await u.message.reply_text("First connect wallet."); return
    await u.message.reply_text("Approving USDC...")
    ok,msg=approve_usdc(s)
    if ok: await u.message.reply_text("Approved!\n%s\n\nNow press AUTO ON" % msg)
    else:  await u.message.reply_text("Approve error:\n%s" % msg)

async def cmd_trades(u,c):
    s=get_session(u.effective_user.id)
    if not s.trade_history:
        await u.message.reply_text("No trades yet."); return
    lines=["%s|$%.2f|%s"%(t["decision"],t["amount"],t["time"].strftime("%H:%M")) for t in s.trade_history[-10:]]
    await u.message.reply_text("Last trades:\n\n"+"\n".join(lines))

async def cmd_resetstats(u,c):
    s=get_session(u.effective_user.id)
    s.signal_history=[]; s.trade_history=[]
    for f in (s.history_file,s.errors_file):
        try:
            if os.path.exists(f): os.remove(f)
        except Exception: pass
    await u.message.reply_text("Stats cleared.")

async def cmd_dump(u,c):
    s=get_session(u.effective_user.id)
    if not os.path.exists(SIGNALS_DUMP):
        await u.message.reply_text("signals_dump.json is empty."); return
    try:
        with open(SIGNALS_DUMP,"rb") as f:
            await u.message.reply_document(document=f,filename="signals_dump.json",
                caption="Full dump %d bytes" % os.path.getsize(SIGNALS_DUMP))
    except Exception as e:
        await u.message.reply_text("Error: %s" % e)

async def cmd_testmarket(u,c):
    """Діагностика — показує що знаходить API"""
    await u.message.reply_text("Searching market...")
    _market_cache["id"]=None
    m=_find_btc15m_market()
    if m:
        ty=m.get("token_yes") or {}
        tn=m.get("token_no")  or {}
        await u.message.reply_text(
            "MARKET FOUND\n\nName: %s\nID: %s\nYES id: %s\nNO  id: %s\nYES price: %s\nNO price: %s" % (
                m["name"][:80], m["id"][:40],
                str(ty.get("token_id","?"))[:40], str(tn.get("token_id","?"))[:40],
                ty.get("price","?"), tn.get("price","?")))
    else:
        await u.message.reply_text("MARKET NOT FOUND\n\nCheck Railway logs for details.")

async def cmd_status(u,c):
    s=get_session(u.effective_user.id)
    await u.message.reply_text("Gathering data...")
    p=get_full_payload(s)
    if not p: await u.message.reply_text("Binance data error"); return
    pr=p["price"]; st=p["structure"]; liq=p["liquidity"]
    pos=p["positioning"]; ctx=p["context"]; manip=p["manipulation"]; amd=p["amd"]
    sw15=liq.get("sweep_15m",{}); sa=liq.get("stops_above") or {}; sb=liq.get("stops_below") or {}
    bc5=liq.get("bos_choch_5m")
    bal,_=get_balance(s)
    bet=s.calc_bet(bal) if bal else 0.0
    m=_market_cache
    await u.message.reply_text(
        "%s\n$%.2f|15m:%+.4f%%|5m:%+.4f%%\nMom:%+.4f%%|Micro:%+.4f%%\n\n"
        "15M=%s|5M=%s|1M=%s\nMkt:%s|Vol:%s|Sess:%s\n"
        "AMD:%s->%s [%s]\nBOS5m:%s\n\n"
        "Sweep15m:%s@%.2f(%dc)\nUp:%.2f(%.3f%%)\nDn:%.2f(%.3f%%)\n\n"
        "Trap:%s|Fund:%+.6f(%s)\nOI:%+.4f%%|Book:%s(%+.1f%%)\n\n"
        "Wallet:%s\nBalance:$%.2f | Bet:$%.2f (10%%)\nAuto:%s\n"
        "Market:%s" % (
            p["timestamp"],pr["current"],pr["chg_15m"],pr["chg_5m"],pr["momentum_3"],pr["micro_mom"],
            st["15m"],st["5m"],st["1m"],ctx["market_condition"],ctx["volatility"],ctx["session"],
            amd.get("phase","NONE"),amd.get("direction","?"),amd.get("reason",""),
            "%s %s@%.2f"%(bc5["type"],bc5["dir"],bc5["level"]) if bc5 else "none",
            sw15.get("type","NONE"),sw15.get("level",0),sw15.get("ago",0),
            sa.get("price",0),liq.get("dist_above",0),sb.get("price",0),liq.get("dist_below",0),
            manip["trap_type"],pos["funding_rate"],pos["funding_sent"],
            pos["oi_change"],pos["ob_bias"],pos["ob_imbalance"],
            s.wallet_address[:12]+"..." if s.wallet_ok else "NOT CONNECTED",
            bal or 0, bet,
            "ON" if s.auto_active else "OFF",
            m.get("name","not cached")[:50] if m.get("id") else "not found"))

# ============================================================
# WALLET FLOW — через handle_message
# ============================================================
async def cmd_wallet(u,c):
    s=get_session(u.effective_user.id)
    s.wallet_state="await_key"; s.wallet_tmp_key=None
    await u.message.reply_text(
        "CONNECT WALLET\n\n"
        "Step 1/2: Enter your MetaMask private key\n\n"
        "Where to find:\n"
        "MetaMask -> three dots -> Account Details -> Export Private Key\n\n"
        "WARNING: Never share your key with anyone!\n\n"
        "Enter 64 hex chars (with or without 0x):")

# ============================================================
# CALLBACKS
# ============================================================
async def handle_callback(u,c):
    s=get_session(u.effective_user.id)
    q=u.callback_query
    await q.answer()

    if q.data=="wallet_connect":
        s.wallet_state="await_key"; s.wallet_tmp_key=None
        await q.message.reply_text(
            "CONNECT WALLET\n\n"
            "Step 1/2: Enter MetaMask private key\n"
            "(MetaMask -> Account Details -> Export Private Key)\n\n"
            "Enter key:")

    elif q.data=="toggle_auto":
        if not s.wallet_ok:
            await q.message.reply_text("First connect wallet!"); return
        if s.auto_active:
            s.auto_active=False
            print("[AUTO] uid=%d OFF" % s.uid)
            await q.message.reply_text("AUTO OFF", reply_markup=main_keyboard(s))
        else:
            s.auto_active=True
            print("[AUTO] uid=%d ON" % s.uid)
            bal,_=get_balance(s)
            bet=s.calc_bet(bal) if bal else 0.0
            bal_s=("$%.2f"%bal) if bal else "N/A"
            await q.message.reply_text(
                "AUTO ON\n\nBalance: %s\nStake: $%.2f (10%%)\n\n:00 :15 :30 :45 UTC" % (bal_s,bet),
                reply_markup=main_keyboard(s))

    elif q.data=="show_balance":
        bal,info=get_balance(s)
        if bal is not None:
            bet=s.calc_bet(bal)
            await q.message.reply_text("Balance: $%.2f USDC\n%s...%s\nStake: $%.2f (10%%)" % (
                bal,info[:10],info[-6:],bet))
        else:
            await q.message.reply_text("Error: %s" % info)

    elif q.data=="analyze_now":
        await q.message.reply_text("Analyzing...")
        await run_cycle(c.application, s)

    elif q.data=="show_stats":
        check_signals(s)
        await q.message.reply_text(get_stats_text(s))

    elif q.data=="download_log":
        if not s.signal_history:
            await q.message.reply_text("No signals yet."); return
        csv_bytes=build_csv(s)
        fname="btc_signals_%s.csv" % datetime.datetime.now().strftime("%Y%m%d_%H%M")
        await q.message.reply_document(document=csv_bytes,filename=fname,
                                       caption="%d records" % len(s.signal_history))

    elif q.data=="show_news":
        await q.message.reply_text("News:\n\n%s" % get_news())

    elif q.data=="do_approve":
        if not s.wallet_ok:
            await q.message.reply_text("First connect wallet!"); return
        await q.message.reply_text("Approving USDC...")
        ok,msg=approve_usdc(s)
        if ok: await q.message.reply_text("Approved!\n%s" % msg)
        else:  await q.message.reply_text("Error: %s" % msg)

    elif q.data=="skip":
        s.pending_trade={}
        await q.edit_message_text("Skipped.")

    elif q.data.startswith("confirm_"):
        await q.edit_message_text("Enter amount in USDC:")

    elif q.data.startswith("execute_"):
        parts=q.data.split("_"); direction=parts[1]; amount=float(parts[2])
        await q.edit_message_text("Executing $%.2f..." % amount)
        bet=place_bet(s,direction,amount)
        if bet["success"]:
            s.trade_history.append({"decision":direction,"amount":amount,
                "entry":s.pending_trade.get("price",0),
                "time":datetime.datetime.now(datetime.timezone.utc)})
            await c.bot.send_message(chat_id=u.effective_chat.id,
                text="BET OK\n%s | %s\n$%.2f -> +$%.2f" % (
                    direction,bet.get("market_name","Polymarket"),amount,bet.get("pot",0)))
        else:
            await c.bot.send_message(chat_id=u.effective_chat.id,
                text="BET FAILED\n%s" % bet["error"])
        s.pending_trade={}

# ============================================================
# HANDLE MESSAGE
# ============================================================
async def handle_message(u,c):
    s=get_session(u.effective_user.id)
    txt=u.message.text.strip()

    if s.wallet_state=="await_key":
        clean=txt.lower().replace("0x","").replace(" ","")
        if len(clean)!=64:
            await u.message.reply_text("Wrong length (%d, need 64). Try again:" % len(clean))
            return
        s.wallet_tmp_key=txt; s.wallet_state="await_rpc"
        await u.message.reply_text(
            "Key accepted.\n\nStep 2/2: RPC URL for Polygon\n"
            "Send any text for default (https://polygon-rpc.com)\n"
            "Or enter your Alchemy URL:")
        return

    if s.wallet_state=="await_rpc":
        rpc=txt if txt.startswith("http") else DEFAULT_RPC_URL
        key=s.wallet_tmp_key or ""
        s.wallet_state=None; s.wallet_tmp_key=None
        await u.message.reply_text("Connecting...")
        ok,result=validate_and_connect_wallet(s,key,rpc)
        if ok:
            bal,_=get_balance(s)
            bet=s.calc_bet(bal) if bal else 0.0
            await u.message.reply_text(
                "Wallet connected!\n\nAddress: %s\nBalance: %s\nNext stake: $%.2f (10%%)\n\n"
                "Next: /approve (once) -> then press AUTO ON" % (
                    result, ("$%.2f"%bal) if bal else "N/A", bet),
                reply_markup=main_keyboard(s))
        else:
            await u.message.reply_text("Connection error:\n%s\n\nTry /wallet again" % result,
                reply_markup=main_keyboard(s))
        return

    if s.pending_trade and time.time()-s.pending_trade.get("timestamp",0)<=600:
        try:
            amount=float(txt)
            if amount<1 or amount>500:
                await u.message.reply_text("Amount $1-$500"); return
            direction=s.pending_trade["direction"]
            kb=InlineKeyboardMarkup([[
                InlineKeyboardButton("$%.2f on %s"%(amount,direction),
                    callback_data="execute_%s_%.2f"%(direction,amount)),
                InlineKeyboardButton("Cancel",callback_data="skip")]])
            await u.message.reply_text("Confirm?",reply_markup=kb)
        except ValueError:
            pass

# ============================================================
# AUTO TRADE
# ============================================================
async def execute_auto_trade(app, s, payload, result):
    decision=result.get("decision")
    strength=result.get("strength","LOW")
    logic=result.get("logic","")
    print("[AUTO TRADE] uid=%d dec=%s str=%s auto=%s" % (s.uid,decision,strength,s.auto_active))
    if not decision: return
    so={"HIGH":3,"MEDIUM":2,"LOW":1}
    if so.get(strength,1)<so.get(AUTO_MIN_STRENGTH,2):
        await app.bot.send_message(chat_id=s.uid,
            text="Signal %s(%s) weak. Skipping." % (decision,strength))
        return
    bal,_=get_balance(s)
    if not bal or bal<=0:
        await app.bot.send_message(chat_id=s.uid, text="No USDC balance. Top up."); return
    amount=s.calc_bet(bal)
    if amount<1:
        await app.bot.send_message(chat_id=s.uid,
            text="Stake $%.2f < $1. Top up balance." % amount); return
    bet=place_bet(s,decision,amount)
    if bet["success"]:
        pot=bet.get("pot",0)
        s.trade_history.append({"decision":decision,"amount":amount,
            "entry":payload["price"]["current"],
            "time":datetime.datetime.now(datetime.timezone.utc)})
        bal_after,_=get_balance(s)
        await app.bot.send_message(chat_id=s.uid,
            text="AUTO BET OK\n%s | %s\nBal: $%.2f -> Bet: $%.2f (10%%) -> +$%.2f\n\n%s" % (
                decision,bet.get("market_name","Polymarket"),bal,amount,pot,logic))
        print("[AUTO TRADE] OK uid=%d amount=%.2f bal=%.2f" % (s.uid,amount,bal))
    else:
        await app.bot.send_message(chat_id=s.uid,
            text="AUTO BET FAILED\n%s" % bet["error"])
        print("[AUTO TRADE] FAIL uid=%d: %s" % (s.uid,bet["error"]))

# ============================================================
# MAIN CYCLE
# ============================================================
async def run_cycle(app, s):
    check_signals(s)
    payload=get_full_payload(s)
    if not payload:
        await app.bot.send_message(chat_id=s.uid, text="Binance data error"); return
    result=analyze_with_ai(payload,s)
    if not result:
        await app.bot.send_message(chat_id=s.uid, text="AI error"); return

    save_signal_dump(payload,result,s)

    decision=result.get("decision","UP"); strength=result.get("strength","LOW")
    logic=result.get("logic",""); score=result.get("confidence_score",0)
    reasons=result.get("reasons",[]); key_sig=result.get("key_signal","")
    risk_note=result.get("risk_note","")
    mkt_cond=result.get("market_condition",payload["context"]["market_condition"])

    liq=payload["liquidity"]; sw15=liq.get("sweep_15m",{})
    manip=payload["manipulation"]; ctx=payload["context"]; amd=payload["amd"]

    sig_record={
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
        "oi_change":payload["positioning"]["oi_change"],
        "ob_bias":payload["positioning"]["ob_bias"],
    }
    s.signal_history.append(sig_record)
    s.save_history()

    dec_ua="UP" if decision=="UP" else "DOWN"
    str_ua={"HIGH":"STRONG","MEDIUM":"MEDIUM","LOW":"WEAK"}.get(strength,strength)
    reas_s="\n".join("- "+r for r in reasons[:3]) if reasons else ""
    risk_s=("RISK: %s"%risk_note) if risk_note and risk_note.lower() not in ("none","","no") else ""

    main_txt=(
        "SIGNAL\n\n%s | %s | Score:%+d\n$%.2f | %s | %s\n\nKEY: %s\n\n%s\n\n%s\n\n%s"
    ) % (dec_ua,str_ua,score,payload["price"]["current"],mkt_cond,ctx["session"],
         key_sig,logic,reas_s,risk_s)

    print("[RUN_CYCLE] uid=%d auto=%s dec=%s str=%s" % (s.uid,s.auto_active,decision,strength))

    if s.auto_active:
        await app.bot.send_message(chat_id=s.uid, text="AUTO SIGNAL\n\n"+main_txt)
        await execute_auto_trade(app,s,payload,result)
    else:
        btn_dir="YES" if decision=="UP" else "NO"
        s.pending_trade={"direction":btn_dir,"amount":None,
                         "timestamp":time.time(),"price":payload["price"]["current"]}
        kb=InlineKeyboardMarkup([[
            InlineKeyboardButton("Yes",callback_data="confirm_%s"%btn_dir),
            InlineKeyboardButton("No", callback_data="skip")]])
        bal,_=get_balance(s)
        bet=s.calc_bet(bal) if bal else 0.0
        bal_hint=(" (recommended $%.2f = 10%%)"%bet) if bal else ""
        await app.bot.send_message(chat_id=s.uid,
            text=main_txt+"\n\nEnter USDC amount"+bal_hint+":", reply_markup=kb)

    wrong=[sg for sg in s.signal_history if sg.get("outcome")=="LOSS"]
    if len(wrong)>0 and len(wrong)%5==0:
        await app.bot.send_message(chat_id=s.uid,
            text="%d losses logged. /errors" % len(wrong))

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
        logger.info("Next cycle in %.0fs at %s UTC", wait, next_run.strftime("%H:%M"))
        await asyncio.sleep(wait)
        # Скидаємо кеш маркету за 2 хв до закінчення раунду
        if _market_cache["id"] and 0 < _market_cache["expires"]-time.time() < 120:
            print("[Market] Pre-fetch: resetting cache")
            _market_cache["id"]=None
        if _sessions:
            for uid,s in list(_sessions.items()):
                try:
                    await run_cycle(app,s)
                except Exception as e:
                    logger.error("Cycle uid=%d: %s",uid,e)

# ============================================================
# MAIN
# ============================================================
def main():
    app=Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start",      cmd_start))
    app.add_handler(CommandHandler("help",       cmd_help))
    app.add_handler(CommandHandler("wallet",     cmd_wallet))
    app.add_handler(CommandHandler("approve",    cmd_approve))
    app.add_handler(CommandHandler("autoon",     cmd_autoon))
    app.add_handler(CommandHandler("autooff",    cmd_autooff))
    app.add_handler(CommandHandler("analyze",    cmd_analyze))
    app.add_handler(CommandHandler("status",     cmd_status))
    app.add_handler(CommandHandler("balance",    cmd_balance))
    app.add_handler(CommandHandler("news",       cmd_news))
    app.add_handler(CommandHandler("stats",      cmd_stats))
    app.add_handler(CommandHandler("errors",     cmd_errors))
    app.add_handler(CommandHandler("trades",     cmd_trades))
    app.add_handler(CommandHandler("resetstats", cmd_resetstats))
    app.add_handler(CommandHandler("dump",       cmd_dump))
    app.add_handler(CommandHandler("testmarket", cmd_testmarket))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    async def on_startup(app):
        asyncio.create_task(periodic(app))
        logger.info("Bot started. No ConversationHandler. 10%% bet. GTC orders.")

    app.post_init=on_startup
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__=="__main__":
    main()
