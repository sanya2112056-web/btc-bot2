"""
BTC Polymarket Trading Bot
Railway Variables: TELEGRAM_BOT_TOKEN + OPENAI_API_KEY
Magic.Link акаунт | signature_type=1 | create_or_derive_api_creds
"""
import asyncio, logging, json, time, datetime, os, requests, hmac, hashlib, base64
from openai import OpenAI
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY", "")
OI_CACHE  = "oi_cache.json"
DUMP_FILE = "signals_dump.json"

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# СЕСІЯ
# ─────────────────────────────────────────────
class Session:
    def __init__(self, uid):
        self.uid     = uid
        self.key     = ""
        self.funder  = ""
        self.address = None
        self.ok      = False
        self.auto    = False
        self.signals = []
        self.trades  = []
        self.state   = None
        self.tmp_key = None
        self.hist    = "hist_%d.json" % uid
        self.err_f   = "err_%d.json"  % uid
        self._creds  = None   # кешовані L2 креди {apiKey, secret, passphrase}
        self._load()

    def _load(self):
        try:
            if os.path.exists(self.hist):
                with open(self.hist) as f: self.signals = json.load(f)
        except Exception: pass

    def save(self):
        try:
            with open(self.hist, "w") as f:
                json.dump(self.signals, f, ensure_ascii=False, indent=2)
        except Exception: pass

    def log_err(self, sig):
        try:
            e = []
            if os.path.exists(self.err_f):
                with open(self.err_f) as f: e = json.load(f)
            e.append(sig); e = e[-300:]
            with open(self.err_f, "w") as f: json.dump(e, f, ensure_ascii=False, indent=2)
        except Exception: pass

    def bet_size(self, bal):
        if not bal or bal <= 0: return 0.0
        return round(max(1.0, min(bal * 0.10, 500.0)), 2)

    def invalidate_creds(self):
        self._creds = None

_sessions = {}
def sess(uid):
    if uid not in _sessions: _sessions[uid] = Session(uid)
    return _sessions[uid]

# ─────────────────────────────────────────────
# L2 CREDENTIALS — отримуємо один раз і кешуємо
# ─────────────────────────────────────────────
def get_or_cache_creds(s):
    """
    Повертає кешовані L2 креди.
    Якщо ще немає — отримує через py-clob-client і зберігає.
    Повертає dict: {apiKey, secret, passphrase}
    """
    if s._creds:
        return s._creds
    print("[Creds] Отримуємо L2 креди...")
    from py_clob_client.client import ClobClient
    from py_clob_client.constants import POLYGON
    client = ClobClient(
        host           = "https://clob.polymarket.com",
        key            = s.key,
        chain_id       = POLYGON,
        signature_type = 1,
        funder         = s.funder,
    )
    creds = client.create_or_derive_api_creds()
    if isinstance(creds, dict):
        s._creds = creds
    else:
        s._creds = {
            "apiKey":     getattr(creds, "api_key", ""),
            "secret":     getattr(creds, "api_secret", ""),
            "passphrase": getattr(creds, "api_passphrase", ""),
        }
    print("[Creds] OK key=%s..." % s._creds.get("apiKey","")[:12])
    return s._creds

# ─────────────────────────────────────────────
# HMAC підпис для прямих HTTP запитів до Polymarket
# ─────────────────────────────────────────────
def make_hmac_headers(creds: dict, method: str, path: str, address: str) -> dict:
    """
    Будує заголовки з HMAC підписом для Polymarket CLOB API.
    address — адреса підписувача (signer), не funder.
    """
    api_key    = creds.get("apiKey") or creds.get("api_key", "")
    secret     = creds.get("secret", "")
    passphrase = creds.get("passphrase", "")
    ts         = str(int(time.time()))

    # secret — base64url рядок, декодуємо в bytes
    try:
        # додаємо padding якщо потрібно
        pad = 4 - len(secret) % 4
        secret_bytes = base64.urlsafe_b64decode(secret + "=" * pad)
    except Exception:
        secret_bytes = secret.encode("utf-8")

    message   = ts + method.upper() + path
    signature = base64.urlsafe_b64encode(
        hmac.new(secret_bytes, message.encode("utf-8"), hashlib.sha256).digest()
    ).decode("utf-8")

    return {
        "POLY_ADDRESS":    address,
        "POLY_SIGNATURE":  signature,
        "POLY_TIMESTAMP":  ts,
        "POLY_API_KEY":    api_key,
        "POLY_PASSPHRASE": passphrase,
        "Content-Type":    "application/json",
    }

# ─────────────────────────────────────────────
# БАЛАНС — прямий HTTP запит з HMAC підписом
# Не використовуємо get_balance_allowance() з py-clob-client
# бо він падає через внутрішній стан клієнта
# ─────────────────────────────────────────────
def get_balance(s):
    if not s.ok or not s.key:
        return None, "Гаманець не підключено"
    try:
        creds   = get_or_cache_creds(s)
        address = s.address or s.funder
        headers = make_hmac_headers(creds, "GET", "/balance-allowance", address)

        r = requests.get(
            "https://clob.polymarket.com/balance-allowance",
            params={"asset_type": "USDC"},
            headers=headers,
            timeout=15,
        )
        print("[Balance] HTTP %d: %s" % (r.status_code, r.text[:200]))

        if r.status_code == 200:
            data    = r.json()
            raw_val = float(data.get("balance") or data.get("usdc_balance") or 0)
            # Polymarket повертає мікро-USDC (6 decimals) якщо число > 1000
            bal = raw_val / 1e6 if raw_val > 1000 else raw_val
            print("[Balance] $%.2f" % round(bal, 2))
            return round(bal, 2), s.funder
        elif r.status_code in (401, 403):
            s.invalidate_creds()
            return 0.0, "Помилка авторизації (%d) — спробуй перепідключити гаманець" % r.status_code
        else:
            return 0.0, "HTTP %d: %s" % (r.status_code, r.text[:80])

    except Exception as e:
        err = str(e)
        print("[Balance] Error: %s" % err)
        return 0.0, err[:100]

# ─────────────────────────────────────────────
# ПОШУК МАРКЕТУ
# ─────────────────────────────────────────────
def find_market():
    SLUG  = "btc-updown-15m-"
    ROUND = 900

    def end_ts(m):
        for f in ("end_date_iso","endDate","endDateIso","end_time","endTime","end_date"):
            v = m.get(f)
            if not v: continue
            try: return float(datetime.datetime.fromisoformat(str(v).replace("Z","+00:00")).timestamp())
            except Exception:
                try: return float(v)
                except Exception: pass
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
                if oc in ("YES","UP","HIGHER","ABOVE"):   yi=tid; yp=pr
                elif oc in ("NO","DOWN","LOWER","BELOW"): ni=tid; np_=pr
            if not yi and toks:
                yi=(toks[0].get("token_id") or toks[0].get("tokenId") or "").strip()
                yp=float(toks[0].get("price",0.5) or 0.5)
            if not ni and len(toks)>1:
                ni=(toks[1].get("token_id") or toks[1].get("tokenId") or "").strip()
                np_=float(toks[1].get("price",0.5) or 0.5)
            return yi, ni, yp, np_
        except Exception as e:
            print("[Market] token err: %s" % e)
            return None, None, 0.5, 0.5

    def try_slug(slug, now):
        try:
            r = requests.get("https://gamma-api.polymarket.com/events",
                             params={"slug": slug}, timeout=15)
            if r.status_code != 200: return None
            raw = r.json()
            evs = raw if isinstance(raw, list) else ([raw] if isinstance(raw, dict) and raw else [])
            if not evs: return None
            ev = evs[0]; title = ev.get("title","") or slug
            print("[Market] %s" % title[:60])
            for m in ev.get("markets", []):
                if m.get("closed", True): continue
                cid  = (m.get("conditionId") or m.get("condition_id") or m.get("id") or "").strip()
                if not cid: continue
                et   = end_ts(m)
                diff = (et - now) if et else 900.0
                if diff <= 0: continue
                yi, ni, yp, np_ = get_tokens(cid)
                if not yi or not ni: continue
                q = m.get("question","") or title
                print("[Market] ЗНАЙДЕНО: %s diff=%.0fs" % (q[:55], diff))
                return {"yes_id":yi,"no_id":ni,"yes_p":yp,"no_p":np_,
                        "q":q,"cid":cid,"diff":round(diff,1)}
        except Exception as e:
            print("[Market] err: %s" % e)
        return None

    now = time.time(); cur = int(now // ROUND) * ROUND
    for attempt in range(1, 8):
        print("[Market] Спроба %d/7" % attempt)
        for ts in [cur, cur+ROUND, cur-ROUND]:
            r = try_slug("%s%d" % (SLUG, ts), now)
            if r: return r
        if attempt < 7:
            print("[Market] Retry in 3s..."); time.sleep(3)
    print("[Market] НЕ ЗНАЙДЕНО"); return None

# ─────────────────────────────────────────────
# СТАВКА
# ─────────────────────────────────────────────
def place_bet(s, direction: str, amount: float) -> dict:
    if not s.ok:   return {"ok":False,"err":"Гаманець не підключено"}
    if amount < 1: return {"ok":False,"err":"Мінімум $1"}

    mkt = find_market()
    if not mkt: return {"ok":False,"err":"Активний маркет не знайдено"}

    token_id = mkt["yes_id"] if direction == "UP" else mkt["no_id"]
    price    = mkt["yes_p"]  if direction == "UP" else mkt["no_p"]
    price    = max(0.01, min(0.99, float(price)))

    # Уточнюємо ціну через midpoint
    try:
        r = requests.get("https://clob.polymarket.com/midpoints",
                         params={"token_id": token_id}, timeout=10)
        if r.status_code == 200:
            mid = float(r.json().get("mid", price))
            if 0.01 <= mid <= 0.99: price = mid
    except Exception: pass

    size = round(amount / price, 2)
    print("[Bet] dir=%s price=%.4f size=%.2f usdc=%.2f" % (direction, price, size, amount))

    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OrderArgs, OrderType, Side, ApiCreds
        from py_clob_client.constants import POLYGON

        raw_creds = get_or_cache_creds(s)
        api_creds = ApiCreds(
            api_key        = raw_creds.get("apiKey") or raw_creds.get("api_key", ""),
            api_secret     = raw_creds.get("secret", ""),
            api_passphrase = raw_creds.get("passphrase", ""),
        )
        # Для підпису ордера (EIP-712) потрібен повний клієнт з приватним ключем
        client = ClobClient(
            host           = "https://clob.polymarket.com",
            key            = s.key,
            chain_id       = POLYGON,
            signature_type = 1,
            funder         = s.funder,
            creds          = api_creds,
        )
        client.set_api_creds(api_creds)

        order = client.create_order(OrderArgs(
            token_id = token_id,
            price    = price,
            size     = size,
            side     = Side.BUY,
        ))
        resp = client.post_order(order, OrderType.GTC)
        print("[Bet] OK: %s" % str(resp)[:100])
        return {
            "ok":    True,
            "resp":  resp,
            "price": price,
            "pot":   round(size - amount, 2),
            "mkt":   mkt["q"][:60],
        }

    except Exception as e:
        err = str(e)
        print("[Bet] FAIL: %s" % err)
        if any(x in err.lower() for x in ["unauthorized", "401", "forbidden", "invalid api key"]):
            s.invalidate_creds()
            print("[Bet] Скинули кеш кредів")
        if "not enough" in err.lower() or "allowance" in err.lower():
            return {"ok":False,"err":"Недостатньо USDC на Polymarket"}
        return {"ok":False,"err":err[:200]}

# ─────────────────────────────────────────────
# BINANCE
# ─────────────────────────────────────────────
def sget(url, p=None, t=15):
    try: return requests.get(url, params=p, timeout=t).json()
    except Exception: return None

def candles(iv, lim):
    for _ in range(3):
        d = sget("https://fapi.binance.com/fapi/v1/klines",{"symbol":"BTCUSDT","interval":iv,"limit":lim})
        if d and isinstance(d, list): break
        time.sleep(2)
    if not d or not isinstance(d, list):
        d = sget("https://api.binance.com/api/v3/klines",{"symbol":"BTCUSDT","interval":iv,"limit":lim})
    if not d or not isinstance(d, list): return []
    return [{"t":int(k[0]),"o":float(k[1]),"h":float(k[2]),"l":float(k[3]),"c":float(k[4]),"v":float(k[5])} for k in d]

def price_now():
    for url, p in [("https://fapi.binance.com/fapi/v1/ticker/price",{"symbol":"BTCUSDT"}),
                   ("https://api.binance.com/api/v3/ticker/price",  {"symbol":"BTCUSDT"})]:
        d = sget(url, p)
        if d and "price" in d: return float(d["price"])
    return None

def funding():
    d = sget("https://fapi.binance.com/fapi/v1/premiumIndex",{"symbol":"BTCUSDT"})
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
    except Exception: return cur,0.0

def orderbook():
    try:
        d=sget("https://fapi.binance.com/fapi/v1/depth",{"symbol":"BTCUSDT","limit":20})
        if not d or not isinstance(d,dict): return {"imb":0.0,"bias":"NEUTRAL"}
        b=sum(float(x[1]) for x in d.get("bids",[])[:10]); a=sum(float(x[1]) for x in d.get("asks",[])[:10])
        t=b+a; imb=round((b-a)/t*100,2) if t>0 else 0.0
        return {"imb":imb,"bias":"BID_HEAVY" if imb>20 else "ASK_HEAVY" if imb<-20 else "BALANCED"}
    except Exception: return {"imb":0.0,"bias":"NEUTRAL"}

def lsr():
    try:
        d=sget("https://fapi.binance.com/futures/data/topLongShortPositionRatio",{"symbol":"BTCUSDT","period":"15m","limit":3})
        if not d or not isinstance(d,list): return {"ratio":1.0,"lp":50.0,"bias":"NEUTRAL"}
        lat=d[-1]; r=float(lat.get("longShortRatio",1.0)); lp=float(lat.get("longAccount",0.5))*100
        return {"ratio":round(r,3),"lp":round(lp,1),"bias":"CROWD_LONG" if r>1.5 else "CROWD_SHORT" if r<0.7 else "NEUTRAL"}
    except Exception: return {"ratio":1.0,"lp":50.0,"bias":"NEUTRAL"}

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
    if 7<=h<12: return "LONDON",1
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

def build_payload(s):
    c15=candles("15m",100); c5=candles("5m",50); c1=candles("1m",30)
    if not c15: return None
    px=c15[-1]["c"]; prev=c15[-2]["c"] if len(c15)>=2 else px
    chg15=round((px-prev)/prev*100,4)
    chg5=round((c5[-1]["c"]-c5[-4]["c"])/c5[-4]["c"]*100,4) if len(c5)>=4 else 0.0
    mom3=round((c15[-1]["c"]-c15[-4]["c"])/c15[-4]["c"]*100,4) if len(c15)>=4 else 0.0
    mic=round((c1[-1]["c"]-c1[-4]["c"])/c1[-4]["c"]*100,4) if len(c1)>=4 else 0.0
    st15=structure(c15) if len(c15)>=6 else "RANGING"
    st5=structure(c5) if len(c5)>=6 else "RANGING"
    st1=structure(c1) if len(c1)>=6 else "RANGING"
    sw15=sweep(c15); sw5=sweep(c5) if c5 else {"type":"NONE","level":0.0,"ago":0}
    sw1=sweep(c1) if c1 else {"type":"NONE","level":0.0,"ago":0}
    sa,sb=stops(c15,px)
    f5a,f5b=fvg(c5[-30:],px) if len(c5)>=5 else (None,None)
    bc5=bos(c5,st15) if len(c5)>=5 else None
    mn=manip_detect(c5[-10:] if len(c5)>=10 else c15[-10:],sw5,px)
    ad=detect_amd(c15,c5,px)
    fn=funding(); lq=liqs(); oi,oic=oi_data(); ob=orderbook(); ls=lsr()
    vc,vs=vol_class(c15); sess_name,sb2=session(); reg=mkt_regime(c15)
    da=round((sa["p"]-px)/px*100,4) if sa else 999.0
    db=round((px-sb["p"])/px*100,4) if sb else 999.0
    last_s="no_prev"
    if s.signals:
        last=s.signals[-1]
        last_s="prev=%s outcome=%s"%(last.get("dec","?"),last.get("outcome","PENDING"))
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
        "ctx":{"vol":vc,"vs":vs,"sess":sess_name,"sb2":sb2,"reg":reg,"last":last_s},
    }

# ─────────────────────────────────────────────
# AI
# ─────────────────────────────────────────────
SYS="""You are an elite BTC short-term trader. Predict UP or DOWN in 15 minutes on Polymarket.
Analyze fresh every time. Do NOT repeat previous signal without new evidence.
TIMEFRAMES: 15m=context | 5m=tactics | 1m=execution
AMD: SWEEP_LOW+close_above=UP(+3) | SWEEP_HIGH+close_below=DOWN(+3) | SWEEP_HIGH+holds=UP(+1)
SCORING: +3AMD | +2CHoCH/BOS/SQUEEZE | -2CASCADE | +1/-1 FVG/session
STRENGTH: >=5=HIGH | 3-4=MEDIUM | 1-2=LOW
OUTPUT JSON only: {"decision":"UP or DOWN","strength":"HIGH or MEDIUM or LOW","confidence_score":<int>,
"market_condition":"TRENDING or RANGING or CHOPPY","key_signal":"one sentence",
"logic":"2-3 sentences Ukrainian","reasons":["r1","r2","r3"],"risk_note":"risk or NONE"}"""

def analyze_with_ai(p,s):
    try:
        client=OpenAI(api_key=OPENAI_API_KEY)
        liq=p["liq"]; pos=p["pos"]; pr=p["price"]
        st=p["struct"]; ctx=p["ctx"]; mn=p["manip"]; ad=p["amd"]
        sw15=liq.get("sw15",{}); sw5=liq.get("sw5",{}); sw1=liq.get("sw1",{})
        bc5=liq.get("bos5"); f5a=liq.get("f5a"); f5b=liq.get("f5b")
        msg=("Time:%s Sess:%s Last:%s\nPRICE:$%.2f 15m:%+.4f%% 5m:%+.4f%% Mom3:%+.4f%% Mic:%+.4f%%\n"
             "STRUCT:15m=%s 5m=%s 1m=%s Reg:%s Vol:%s\nAMD:%s->%s conf=%d [%s]\n"
             "Sw15m:%s@%.2f(%dc) Sw5m:%s@%.2f(%dc) Sw1m:%s@%.2f(%dc)\n"
             "StopsUp:%.3f%% StopsDn:%.3f%% BOS5m:%s FVG5:up=%s dn=%s Trap:%s hint=%s\n"
             "Fund:%+.6f(%s) LiqL:$%.0f LiqS:$%.0f Sig:%s\nOI:%+.4f%% Book:%s(%+.1f%%) L/S:%.3f(%s) CL:%.1f%%")%(
            p["ts"],ctx["sess"],ctx["last"],pr["cur"],pr["chg15"],pr["chg5"],pr["mom3"],pr["mic"],
            st["15m"],st["5m"],st["1m"],ctx["reg"],ctx["vol"],
            ad.get("phase","NONE"),ad.get("dir","?"),ad.get("conf",0),ad.get("reason",""),
            sw15.get("type","N"),sw15.get("level",0),sw15.get("ago",0),
            sw5.get("type","N"),sw5.get("level",0),sw5.get("ago",0),
            sw1.get("type","N"),sw1.get("level",0),sw1.get("ago",0),
            liq.get("da",999),liq.get("db",999),
            ("%s %s@%.2f"%(bc5["type"],bc5["dir"],bc5["level"])) if bc5 else "none",
            ("%.3f%%"%f5a["dist"]) if f5a else "none",("%.3f%%"%f5b["dist"]) if f5b else "none",
            mn["trap"],str(mn["hint"]),
            pos["fr"],pos["fs"],pos["ll"],pos["ls"],pos["lsig"],
            pos["oic"],pos["ob"],pos["obi"],pos["lsrr"],pos["lsr"],pos["cl"])
        resp=client.chat.completions.create(model="gpt-4o",
             messages=[{"role":"system","content":SYS},{"role":"user","content":msg}],
             temperature=0.1,response_format={"type":"json_object"})
        return json.loads(resp.choices[0].message.content)
    except Exception as e:
        log.error("AI: %s",e); return None

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

def stats(s):
    if not s.signals: return "Даних поки немає."
    checked=[g for g in s.signals if g.get("outcome")]
    if not checked: return "Перевіряємо результати..."
    wins=[g for g in checked if g["outcome"]=="WIN"]; total=len(checked)
    wr=round(len(wins)/total*100,1)
    lines=["СТАТИСТИКА","Всього: %d  Перемоги: %d  Вінрейт: %.1f%%"%(total,len(wins),wr),""]
    for st in ("HIGH","MEDIUM","LOW"):
        sub=[g for g in checked if g.get("strength")==st]
        if sub:
            w=len([g for g in sub if g["outcome"]=="WIN"])
            lines.append("%s: %d/%d (%.1f%%)"%(st,w,len(sub),round(w/len(sub)*100,1)))
    return "\n".join(lines)

def get_news():
    try:
        d=sget("https://min-api.cryptocompare.com/data/v2/news/",{"categories":"BTC,Bitcoin","lTs":0})
        if d and "Data" in d:
            bkw=["bull","surge","rally","etf"]; skw=["bear","drop","crash","dump","ban"]
            lines=[]
            for item in d["Data"][:5]:
                t=item.get("title","").lower(); p_=sum(1 for k in bkw if k in t); n=sum(1 for k in skw if k in t)
                lines.append("[%s] %s"%("BULLISH" if p_>n else "BEARISH" if n>p_ else "NEUTRAL",item.get("title","")[:70]))
            return "\n".join(lines)
    except Exception: pass
    return "Новини недоступні"

# ─────────────────────────────────────────────
# КЛАВІАТУРА
# ─────────────────────────────────────────────
def kb(s):
    w="Гаманець: OK"  if s.ok   else "Підключити гаманець"
    a="Авто: ВИМК"    if s.auto else "Авто: УВІМК"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(w,callback_data="wallet"),
         InlineKeyboardButton(a,callback_data="auto_toggle")],
        [InlineKeyboardButton("Баланс",    callback_data="balance"),
         InlineKeyboardButton("Статистика",callback_data="stats")],
        [InlineKeyboardButton("Аналіз",    callback_data="analyze"),
         InlineKeyboardButton("Маркет",    callback_data="market")],
        [InlineKeyboardButton("Новини",    callback_data="news"),
         InlineKeyboardButton("Помилки",   callback_data="errors")],
    ])

WELCOME=(
    "BTC Polymarket Bot\n\n"
    "Сигнали кожні 15 хв — :00 :15 :30 :45 UTC\n"
    "Ставка — 10% від балансу Polymarket\n\n"
    "ЯК ПІДКЛЮЧИТИ:\n\n"
    "1. Натисни «Підключити гаманець»\n\n"
    "   Крок 1: Приватний ключ\n"
    "   polymarket.com → Profile → Export Private Key\n\n"
    "   Крок 2: Polymarket адреса (funder)\n"
    "   polymarket.com → Deposit → скопіюй адресу\n\n"
    "2. Натисни «Авто: УВІМК»\n\n"
    "Без MetaMask. Без MATIC. Кошти на Polymarket."
)

# ─────────────────────────────────────────────
# КОМАНДИ
# ─────────────────────────────────────────────
async def cmd_start(u,c):
    s=sess(u.effective_user.id)
    await u.message.reply_text(WELCOME,reply_markup=kb(s))

async def cmd_stats(u,c):
    s=sess(u.effective_user.id); check_outcomes(s)
    await u.message.reply_text(stats(s))

async def cmd_analyze(u,c):
    s=sess(u.effective_user.id)
    await u.message.reply_text("Аналізую...")
    await cycle(c.application,s)

async def cmd_autoon(u,c):
    s=sess(u.effective_user.id)
    if not s.ok: await u.message.reply_text("Спочатку підключи гаманець."); return
    s.auto=True
    bal,_=get_balance(s); bet=s.bet_size(bal)
    await u.message.reply_text(
        "Авто-торгівля увімкнена\n\nБаланс: %s\nСтавка: $%.2f (10%%)\n\nСигнали :00 :15 :30 :45 UTC"%(
            ("$%.2f"%bal) if bal else "перевіряємо...",bet),reply_markup=kb(s))

async def cmd_autooff(u,c):
    s=sess(u.effective_user.id); s.auto=False
    await u.message.reply_text("Авто-торгівля вимкнена.",reply_markup=kb(s))

# ─────────────────────────────────────────────
# CALLBACKS
# ─────────────────────────────────────────────
async def on_callback(u,c):
    s=sess(u.effective_user.id); q=u.callback_query; await q.answer()

    if q.data=="wallet":
        s.state="key"; s.tmp_key=None
        await q.message.reply_text(
            "ПІДКЛЮЧЕННЯ ГАМАНЦЯ\n\nКрок 1/2 — Приватний ключ\n\n"
            "polymarket.com → Profile → Export Private Key\n\n"
            "Це ключ від POLYMARKET, не MetaMask.\n"
            "MATIC не потрібен.\n\n"
            "Введи ключ (64 hex символи):")

    elif q.data=="auto_toggle":
        if not s.ok: await q.message.reply_text("Спочатку підключи гаманець."); return
        if s.auto:
            s.auto=False; await q.message.reply_text("Авто-торгівля вимкнена.",reply_markup=kb(s))
        else:
            s.auto=True; bal,_=get_balance(s); bet=s.bet_size(bal)
            await q.message.reply_text(
                "Авто-торгівля увімкнена\n\nБаланс: %s\nСтавка: $%.2f (10%%)"%(
                    ("$%.2f"%bal) if bal else "перевіряємо...",bet),reply_markup=kb(s))

    elif q.data=="balance":
        bal, err = get_balance(s)
        if bal is not None and bal > 0:
            await q.message.reply_text("Баланс: $%.2f USDC\nСтавка: $%.2f (10%%)"%(bal,s.bet_size(bal)))
        else:
            await q.message.reply_text(
                "Баланс: $0.00\nДеталі: %s\n\nЯкщо гаманець правильний — поповни на polymarket.com → Deposit" % err)

    elif q.data=="stats":
        check_outcomes(s); await q.message.reply_text(stats(s))

    elif q.data=="analyze":
        await q.message.reply_text("Аналізую..."); await cycle(c.application,s)

    elif q.data=="market":
        await q.message.reply_text("Шукаю маркет...")
        m=find_market()
        if m:
            await q.message.reply_text(
                "Маркет знайдено\n\n%s\n\ncondition_id:\n%s\n\nYES token:\n%s\n\nNO token:\n%s\n\n"
                "Ціна YES: %.4f | NO: %.4f\nЗакривається через: %.0f сек"%(
                    m["q"][:80],m["cid"],m["yes_id"],m["no_id"],m["yes_p"],m["no_p"],m["diff"]))
        else:
            await q.message.reply_text("Маркет не знайдено.")

    elif q.data=="news":
        await q.message.reply_text("Новини:\n\n%s"%get_news())

    elif q.data=="errors":
        if not os.path.exists(s.err_f):
            await q.message.reply_text("Помилок поки немає."); return
        try:
            with open(s.err_f) as f: errs=json.load(f)
            if not errs: await q.message.reply_text("Помилок поки немає."); return
            lines=["ОСТАННІ ПОМИЛКИ (%d)"%len(errs),""]
            for i,e in enumerate(errs[-5:],1):
                lines.append("%d. %s %s\n   %s\n"%(i,e.get("dec","?"),e.get("strength","?"),e.get("key_signal","")[:60]))
            await q.message.reply_text("\n".join(lines)[:4000])
        except Exception: await q.message.reply_text("Помилка читання файлу.")

    elif q.data=="skip":
        if hasattr(s,"pending"): s.pending={}
        await q.edit_message_text("Скасовано.")

    elif q.data.startswith("exec_"):
        parts=q.data.split("_"); direction=parts[1]; amount=float(parts[2])
        await q.edit_message_text("Розміщую ставку $%.2f..."%amount)
        bet=place_bet(s,direction,amount)
        if bet["ok"]:
            await c.bot.send_message(chat_id=u.effective_chat.id,
                text="Ставка виконана\n%s | %s\n$%.2f — потенційно +$%.2f"%(
                    direction,bet.get("mkt","Polymarket"),amount,bet.get("pot",0)))
        else:
            await c.bot.send_message(chat_id=u.effective_chat.id,
                text="Ставка не виконана\n%s"%bet["err"])
        if hasattr(s,"pending"): s.pending={}

# ─────────────────────────────────────────────
# ТЕКСТОВІ ПОВІДОМЛЕННЯ
# ─────────────────────────────────────────────
async def on_message(u,c):
    s=sess(u.effective_user.id); txt=u.message.text.strip()

    if s.state=="key":
        clean=txt.lower().replace("0x","").replace(" ","")
        if len(clean)!=64:
            await u.message.reply_text("Неправильна довжина (%d символів, потрібно 64).\nСпробуй ще раз:"%len(clean)); return
        s.tmp_key=txt; s.state="funder"
        await u.message.reply_text(
            "Ключ прийнято.\n\nКрок 2/2 — Polymarket адреса (funder)\n\n"
            "polymarket.com → Deposit → скопіюй адресу\n\n"
            "Введи адресу (0x..., 42 символи):"); return

    if s.state=="funder":
        addr=txt.strip()
        if not addr.lower().startswith("0x") or len(addr)!=42:
            await u.message.reply_text("Неправильна адреса (42 символи, починається з 0x).\nСпробуй ще раз:"); return
        s.state=None; key=s.tmp_key or ""; s.tmp_key=None
        clean=key.lower().replace("0x","").replace(" ","")
        s.key="0x"+clean; s.funder=addr; s.ok=True
        s._creds=None  # скидаємо кеш при новому підключенні
        try:
            from eth_account import Account
            s.address=Account.from_key(s.key).address
        except Exception: s.address=s.key[:12]+"..."
        print("[Wallet] signer=%s funder=%s"%(s.address,addr))
        bal,err=get_balance(s); bet=s.bet_size(bal)
        bal_str=("$%.2f USDC"%bal) if (bal is not None and bal>0) else ("$0 (%s)"%err)
        await u.message.reply_text(
            "Гаманець підключено!\n\nПідписувач: %s\nFunder: %s\nБаланс: %s\nСтавка: $%.2f (10%%)\n\nНатисни «Авто: УВІМК»"%(
                s.address,addr,bal_str,bet),
            reply_markup=kb(s)); return

    if hasattr(s,"pending") and s.pending and time.time()-s.pending.get("ts",0)<=600:
        try:
            amount=float(txt)
            if amount<1 or amount>500: await u.message.reply_text("Сума від $1 до $500"); return
            direction=s.pending["dir"]
            ikb=InlineKeyboardMarkup([[
                InlineKeyboardButton("Підтвердити $%.2f на %s"%(amount,direction),
                    callback_data="exec_%s_%.2f"%(direction,amount)),
                InlineKeyboardButton("Скасувати",callback_data="skip")]])
            await u.message.reply_text("Підтвердити?",reply_markup=ikb)
        except ValueError: pass

# ─────────────────────────────────────────────
# АВТО ТОРГІВЛЯ
# ─────────────────────────────────────────────
async def auto_trade(app,s,p,result):
    dec=result.get("decision"); strength=result.get("strength","LOW"); logic=result.get("logic","")
    print("[Auto] uid=%d dec=%s str=%s"%(s.uid,dec,strength))
    if not dec: return
    bal,err=get_balance(s)
    if not bal or bal<=0:
        await app.bot.send_message(chat_id=s.uid,
            text="Баланс $0. Поповни на polymarket.com → Deposit\n(%s)"%err); return
    amount=s.bet_size(bal)
    if amount<1:
        await app.bot.send_message(chat_id=s.uid,text="Ставка $%.2f < $1. Поповни баланс."%amount); return
    bet=place_bet(s,dec,amount)
    if bet["ok"]:
        s.trades.append({"dec":dec,"amount":amount,"entry":p["price"]["cur"],
                         "time":str(datetime.datetime.now(datetime.timezone.utc))})
        await app.bot.send_message(chat_id=s.uid,
            text="Авто-ставка виконана\n%s | %s\nБаланс: $%.2f\nСтавка: $%.2f (10%%)\nПотенційно: +$%.2f\n\n%s"%(
                dec,bet.get("mkt","Polymarket"),bal,amount,bet.get("pot",0),logic))
        print("[Auto] OK uid=%d amount=%.2f"%(s.uid,amount))
    else:
        await app.bot.send_message(chat_id=s.uid,text="Авто-ставка не виконана\n%s"%bet["err"])
        print("[Auto] FAIL uid=%d: %s"%(s.uid,bet["err"]))

# ─────────────────────────────────────────────
# ЦИКЛ
# ─────────────────────────────────────────────
async def cycle(app,s):
    check_outcomes(s)
    p=build_payload(s)
    if not p:
        await app.bot.send_message(chat_id=s.uid,text="Помилка даних Binance."); return
    result=analyze_with_ai(p,s)
    if not result:
        await app.bot.send_message(chat_id=s.uid,text="Помилка AI."); return

    dec=result.get("decision","UP"); strength=result.get("strength","LOW")
    logic=result.get("logic",""); score=result.get("confidence_score",0)
    reasons=result.get("reasons",[]); key_sig=result.get("key_signal","")
    mkt_cond=result.get("market_condition",p["ctx"]["reg"])
    ad=p["amd"]; mn=p["manip"]; sw15=p["liq"].get("sw15",{})

    sig={"dec":dec,"strength":strength,"confidence_score":score,"logic":logic,
         "reasons":reasons,"key_signal":key_sig,"entry":p["price"]["cur"],
         "time":p["ts"],"ts_unix":p["ts_unix"],"outcome":None,
         "st15m":p["struct"]["15m"],"mkt_cond":mkt_cond,"session":p["ctx"]["sess"],
         "sweep_type":sw15.get("type","NONE"),"amd_phase":ad.get("phase","NONE"),"trap":mn["trap"]}
    s.signals.append(sig); s.save()

    try:
        dump=[]
        if os.path.exists(DUMP_FILE):
            with open(DUMP_FILE) as f: dump=json.load(f)
        dump.append(sig); dump=dump[-2000:]
        with open(DUMP_FILE,"w") as f: json.dump(dump,f,ensure_ascii=False,indent=2)
    except Exception: pass

    str_ua={"HIGH":"STRONG","MEDIUM":"MEDIUM","LOW":"WEAK"}.get(strength,strength)
    reas_s="\n".join("— "+r for r in reasons[:3]) if reasons else ""
    main_txt=("СИГНАЛ\n\n%s | %s | Score:%+d\n$%.2f | %s | %s\n\n%s\n\n%s\n\n%s")%(
        "UP" if dec=="UP" else "DOWN",str_ua,score,
        p["price"]["cur"],mkt_cond,p["ctx"]["sess"],key_sig,logic,reas_s)

    print("[Cycle] uid=%d auto=%s dec=%s str=%s"%(s.uid,s.auto,dec,strength))

    if s.auto:
        await app.bot.send_message(chat_id=s.uid,text="АВТО СИГНАЛ\n\n"+main_txt)
        await auto_trade(app,s,p,result)
    else:
        if not hasattr(s,"pending"): s.pending={}
        s.pending={"dir":dec,"ts":time.time(),"price":p["price"]["cur"]}
        bal,_=get_balance(s); bet=s.bet_size(bal)
        ikb=InlineKeyboardMarkup([[
            InlineKeyboardButton("Так",callback_data="confirm_%s"%dec),
            InlineKeyboardButton("Ні",callback_data="skip")]])
        hint=(" (рекомендовано $%.2f = 10%%)"%bet) if bal else ""
        await app.bot.send_message(chat_id=s.uid,
            text=main_txt+"\n\nВведи суму USDC"+hint+":",reply_markup=ikb)

# ─────────────────────────────────────────────
# ПЛАНУВАЛЬНИК
# ─────────────────────────────────────────────
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
        log.info("Бот запущено. Magic.Link. signature_type=1. Прямий HTTP баланс.")

    app.post_init=startup
    app.run_polling(allowed_updates=Update.ALL_TYPES,drop_pending_updates=True)

if __name__=="__main__":
    main()
