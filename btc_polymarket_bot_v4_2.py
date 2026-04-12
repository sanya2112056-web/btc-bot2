import asyncio, logging, json, time, datetime, os, requests
from openai import OpenAI
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters

# ============================================================
# CONFIG
# ============================================================
TELEGRAM_BOT_TOKEN     = "INSERT_TOKEN"
TELEGRAM_CHAT_ID       = "INSERT_CHAT_ID"
OPENAI_API_KEY         = "INSERT_OPENAI_KEY"
POLYMARKET_PRIVATE_KEY = "INSERT_POLYGON_KEY"
ALCHEMY_RPC_URL        = "INSERT_ALCHEMY_URL"

AUTO_BET_SIZE     = 5.0
AUTO_MIN_STRENGTH = "MEDIUM"
OI_CACHE_FILE     = "oi_cache.json"
HISTORY_FILE      = "signal_history.json"
ERRORS_FILE       = "errors_log.json"
FULL_LOG_FILE     = "full_analysis_log.json"

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

pending_trade     = {}
signal_history    = []
trade_history     = []
auto_trade_active = False

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
    return [{"t": int(k[0]), "o": float(k[1]), "h": float(k[2]),
             "l": float(k[3]), "c": float(k[4]), "v": float(k[5])} for k in data]

def fetch_price():
    data = safe_get("https://fapi.binance.com/fapi/v1/ticker/price", {"symbol": "BTCUSDT"})
    if not data or not isinstance(data, dict):
        data = safe_get("https://api.binance.com/api/v3/ticker/price", {"symbol": "BTCUSDT"})
    if data and isinstance(data, dict):
        return float(data.get("price", 0))
    return None

def fetch_funding():
    data = safe_get("https://fapi.binance.com/fapi/v1/premiumIndex", {"symbol": "BTCUSDT"})
    if not data or not isinstance(data, dict):
        return {"rate": 0.0, "sentiment": "NEUTRAL", "mark": 0.0, "basis": 0.0}
    fr   = float(data.get("lastFundingRate", 0))
    mark = float(data.get("markPrice", 0))
    idx  = float(data.get("indexPrice", mark))
    sent = "LONGS_TRAPPED" if fr > 0.0005 else "SHORTS_TRAPPED" if fr < -0.0003 else "NEUTRAL"
    return {"rate": fr, "sentiment": sent, "mark": mark, "basis": round(mark - idx, 2)}

def fetch_liquidations():
    now_ms = int(time.time() * 1000)
    cutoff = now_ms - 900000
    data = safe_get("https://fapi.binance.com/fapi/v1/forceOrders",
                    {"symbol": "BTCUSDT", "limit": 200})
    if not data or isinstance(data, dict):
        data = safe_get("https://fapi.binance.com/fapi/v1/allForceOrders",
                        {"symbol": "BTCUSDT", "limit": 200})
    if not data or not isinstance(data, list):
        return {"liq_longs": 0.0, "liq_shorts": 0.0, "signal": "NEUTRAL",
                "exhaustion": False, "total_usd": 0.0}
    recent     = [x for x in data if isinstance(x, dict) and int(x.get("time", 0)) >= cutoff] or data[:50]
    liq_longs  = sum(float(x.get("origQty", 0)) * float(x.get("price", 0))
                     for x in recent if x.get("side") == "SELL")
    liq_shorts = sum(float(x.get("origQty", 0)) * float(x.get("price", 0))
                     for x in recent if x.get("side") == "BUY")
    total  = liq_longs + liq_shorts
    signal = ("SHORT_SQUEEZE_FUEL" if liq_shorts > liq_longs * 2
              else "LONG_CASCADE_FUEL" if liq_longs > liq_shorts * 2 else "NEUTRAL")
    return {"liq_longs": round(liq_longs, 2), "liq_shorts": round(liq_shorts, 2),
            "signal": signal, "exhaustion": total > 5_000_000, "total_usd": round(total, 2)}

def fetch_oi():
    data = safe_get("https://fapi.binance.com/fapi/v1/openInterest", {"symbol": "BTCUSDT"})
    if not data or not isinstance(data, dict):
        return 0.0, 0.0
    cur = float(data.get("openInterest", 0))
    try:
        prev = cur
        if os.path.exists(OI_CACHE_FILE):
            with open(OI_CACHE_FILE) as f:
                prev = json.load(f).get("oi", cur)
        with open(OI_CACHE_FILE, "w") as f:
            json.dump({"oi": cur, "ts": ts_unix()}, f)
        return cur, round((cur - prev) / prev * 100, 4) if prev > 0 else 0.0
    except Exception:
        return cur, 0.0

def fetch_orderbook():
    try:
        data = safe_get("https://fapi.binance.com/fapi/v1/depth",
                        {"symbol": "BTCUSDT", "limit": 20})
        if not data or not isinstance(data, dict):
            return {"imbalance": 0.0, "bias": "NEUTRAL"}
        bids  = sum(float(b[1]) for b in data.get("bids", [])[:10])
        asks  = sum(float(a[1]) for a in data.get("asks", [])[:10])
        total = bids + asks
        imb   = round((bids - asks) / total * 100, 2) if total > 0 else 0.0
        return {"imbalance": imb,
                "bias": "BID_HEAVY" if imb > 20 else "ASK_HEAVY" if imb < -20 else "BALANCED"}
    except Exception:
        return {"imbalance": 0.0, "bias": "NEUTRAL"}

def fetch_lsr():
    try:
        data = safe_get("https://fapi.binance.com/futures/data/topLongShortPositionRatio",
                        {"symbol": "BTCUSDT", "period": "15m", "limit": 3})
        if not data or not isinstance(data, list):
            return {"ratio": 1.0, "long_pct": 50.0, "bias": "NEUTRAL"}
        latest   = data[-1]
        ratio    = float(latest.get("longShortRatio", 1.0))
        long_pct = float(latest.get("longAccount", 0.5)) * 100
        return {"ratio": round(ratio, 3), "long_pct": round(long_pct, 1),
                "bias": "CROWD_LONG" if ratio > 1.5 else "CROWD_SHORT" if ratio < 0.7 else "NEUTRAL"}
    except Exception:
        return {"ratio": 1.0, "long_pct": 50.0, "bias": "NEUTRAL"}

# ============================================================
# SMC ENGINE  (1m / 5m / 15m — без 1H)
# ============================================================

def swing_points(candles):
    sh, sl = [], []
    for i in range(2, len(candles) - 2):
        h = candles[i]["h"]
        if (h > candles[i-1]["h"] and h > candles[i+1]["h"]
                and h > candles[i-2]["h"] and h > candles[i+2]["h"]):
            sh.append({"price": h, "idx": i})
        l = candles[i]["l"]
        if (l < candles[i-1]["l"] and l < candles[i+1]["l"]
                and l < candles[i-2]["l"] and l < candles[i+2]["l"]):
            sl.append({"price": l, "idx": i})
    return sh[-5:], sl[-5:]

def market_structure(candles):
    sh, sl = swing_points(candles)
    if len(sh) < 2 or len(sl) < 2:
        return "RANGING"
    hs = [x["price"] for x in sh]
    ls = [x["price"] for x in sl]
    if all(hs[i] > hs[i-1] for i in range(1, len(hs))) and \
       all(ls[i] > ls[i-1] for i in range(1, len(ls))):
        return "BULLISH"
    if all(hs[i] < hs[i-1] for i in range(1, len(hs))) and \
       all(ls[i] < ls[i-1] for i in range(1, len(ls))):
        return "BEARISH"
    return "RANGING"

def liq_sweep(candles):
    if len(candles) < 10:
        return {"type": "NONE", "level": 0.0, "ago": 0}
    sh, sl = swing_points(candles[:-3])
    for i, c in enumerate(reversed(candles[-5:])):
        for s in reversed(sh):
            if c["h"] > s["price"] and c["c"] < s["price"]:
                return {"type": "HIGH", "level": s["price"], "ago": i + 1}
        for s in reversed(sl):
            if c["l"] < s["price"] and c["c"] > s["price"]:
                return {"type": "LOW", "level": s["price"], "ago": i + 1}
    return {"type": "NONE", "level": 0.0, "ago": 0}

def equal_levels(candles, tol=0.001):
    eq_h, eq_l = [], []
    hs = [(i, c["h"]) for i, c in enumerate(candles)]
    ls = [(i, c["l"]) for i, c in enumerate(candles)]
    for i in range(len(hs)):
        for j in range(i + 1, len(hs)):
            if abs(hs[i][1] - hs[j][1]) / hs[i][1] < tol and j - i >= 2:
                eq_h.append({"price": (hs[i][1] + hs[j][1]) / 2})
    for i in range(len(ls)):
        for j in range(i + 1, len(ls)):
            if abs(ls[i][1] - ls[j][1]) / ls[i][1] < tol and j - i >= 2:
                eq_l.append({"price": (ls[i][1] + ls[j][1]) / 2})
    return eq_h[-3:], eq_l[-3:]

def stop_clusters(candles, price):
    sh, sl     = swing_points(candles)
    eq_h, eq_l = equal_levels(candles[-50:])
    above, below = [], []
    for s in sh:
        if s["price"] > price:
            above.append({"price": s["price"], "type": "swing_high"})
    for s in sl:
        if s["price"] < price:
            below.append({"price": s["price"], "type": "swing_low"})
    for e in eq_h:
        if e["price"] > price:
            above.append({"price": e["price"], "type": "equal_highs"})
    for e in eq_l:
        if e["price"] < price:
            below.append({"price": e["price"], "type": "equal_lows"})
    sa = min(above, key=lambda x: x["price"] - price) if above else None
    sb = min(below, key=lambda x: price - x["price"]) if below else None
    return sa, sb

def find_fvg(candles, price):
    """FVG на малих таймфреймах (1m або 5m) — найточніший для 15хв прогнозу"""
    fa = fb = None
    for i in range(1, len(candles) - 1):
        pv, nx = candles[i-1], candles[i+1]
        if nx["l"] > pv["h"]:
            mid  = (nx["l"] + pv["h"]) / 2
            dist = round((price - mid) / price * 100, 4)
            if mid < price and (fb is None or dist < fb["dist"]):
                fb = {"top": nx["l"], "bot": pv["h"], "dist": dist,
                      "size": round(nx["l"] - pv["h"], 2)}
        if nx["h"] < pv["l"]:
            mid  = (pv["l"] + nx["h"]) / 2
            dist = round((mid - price) / price * 100, 4)
            if mid > price and (fa is None or dist < fa["dist"]):
                fa = {"top": pv["l"], "bot": nx["h"], "dist": dist,
                      "size": round(pv["l"] - nx["h"], 2)}
    return fa, fb

def bos_choch(candles, struct_htf):
    """BOS/CHoCH на 5m відносно структури 15m"""
    if len(candles) < 5:
        return None
    sh, sl = swing_points(candles[:-1])
    if not sh or not sl:
        return None
    cl = candles[-1]["c"]
    if cl > sh[-1]["price"]:
        return {"type": "CHoCH" if struct_htf == "BEARISH" else "BOS",
                "dir": "UP", "level": sh[-1]["price"]}
    if cl < sl[-1]["price"]:
        return {"type": "CHoCH" if struct_htf == "BULLISH" else "BOS",
                "dir": "DOWN", "level": sl[-1]["price"]}
    return None

def detect_manipulation(candles, sweep, price):
    result = {"trap_type": "NONE", "reversal_signal": None}
    if len(candles) < 5:
        return result
    last  = candles[-1]
    body  = abs(last["c"] - last["o"])
    total = last["h"] - last["l"]
    if total > 0:
        wick_ratio = 1 - (body / total)
        upper_wick = last["h"] - max(last["c"], last["o"])
        lower_wick = min(last["c"], last["o"]) - last["l"]
        if wick_ratio > 0.7 and total / last["c"] > 0.002:
            if upper_wick > lower_wick * 2:
                result.update({"trap_type": "WICK_TRAP_HIGH", "reversal_signal": "DOWN"})
            elif lower_wick > upper_wick * 2:
                result.update({"trap_type": "WICK_TRAP_LOW", "reversal_signal": "UP"})
    if sweep["type"] != "NONE" and sweep["ago"] <= 3:
        if sweep["type"] == "HIGH" and last["c"] < sweep["level"] * 0.9995:
            result.update({"trap_type": "SWEEP_TRAP_HIGH", "reversal_signal": "DOWN"})
        elif sweep["type"] == "LOW" and last["c"] > sweep["level"] * 1.0005:
            result.update({"trap_type": "SWEEP_TRAP_LOW", "reversal_signal": "UP"})
    lows_taken  = any(c["l"] < candles[-6]["l"] for c in candles[-5:]) if len(candles) >= 6 else False
    highs_taken = any(c["h"] > candles[-6]["h"] for c in candles[-5:]) if len(candles) >= 6 else False
    if lows_taken and highs_taken:
        result.update({"trap_type": "CHOP_ZONE", "reversal_signal": None})
    return result

# ============================================================
# AMD  (на 15m + підтвердження на 5m)
# ВИПРАВЛЕНО: Sweep HIGH в ranging ≠ завжди DOWN
# Sweep HIGH = ліквідність взята вгорі
#   ЯКЩО ціна потім ЗАКРИЛАСЬ НИЖЧЕ sweep рівня → reversal DOWN (smart money sold)
#   ЯКЩО ціна тримається вище → continuation UP (просто взяли стопи і продовжать)
# ============================================================

def detect_amd(c15, c5m, price):
    if len(c15) < 20:
        return {"phase": "NONE", "direction": None, "confidence": 0, "reason": ""}

    last10_15 = c15[-10:]
    last3_15  = c15[-3:]
    last3_5m  = c5m[-3:] if len(c5m) >= 3 else []

    # Accumulation: tight range on 15m
    highs     = [c["h"] for c in last10_15]
    lows      = [c["l"] for c in last10_15]
    rng       = (max(highs) - min(lows)) / price * 100
    avg_body  = sum(abs(c["c"] - c["o"]) for c in last10_15) / len(last10_15) / price * 100
    is_accum  = rng < 0.6 and avg_body < 0.10

    sweep_15m = liq_sweep(c15)
    sweep_5m  = liq_sweep(c5m) if len(c5m) >= 10 else {"type": "NONE", "level": 0.0, "ago": 0}

    # Вибираємо найсвіжіший sweep
    sweep = sweep_15m if sweep_15m["type"] != "NONE" and sweep_15m["ago"] <= 4 else sweep_5m
    manip_ok = sweep["type"] != "NONE" and sweep["ago"] <= 4

    # 5m мікро-momentum для підтвердження напрямку після маніпуляції
    micro_conf = None
    if last3_5m:
        m5_move = (last3_5m[-1]["c"] - last3_5m[0]["o"]) / last3_5m[0]["o"] * 100
        micro_conf = "UP" if m5_move > 0.05 else "DOWN" if m5_move < -0.05 else None

    last_move_15 = (last3_15[-1]["c"] - last3_15[0]["o"]) / last3_15[0]["o"] * 100

    if manip_ok and is_accum:
        if sweep["type"] == "LOW":
            # Sweep вниз + ціна закрилась вище = smart money bought = UP
            direction = "UP"
            reason    = "ACCUM+SWEEP_LOW: smart money bought below, expecting UP distribution"
            conf      = 3 if micro_conf == "UP" else 2
        else:  # sweep HIGH
            # КЛЮЧОВА ВИПРАВА: sweep HIGH = взяли стопи вгорі
            # Якщо ціна ЗАКРИЛАСЬ НИЖЧЕ рівня sweep — це реальний reversal DOWN
            # Якщо ціна ТРИМАЄТЬСЯ ВИЩЕ рівня sweep — continuation, НЕ DOWN
            last_close = c15[-1]["c"]
            if last_close < sweep["level"] * 0.9998:
                direction = "DOWN"
                reason    = "ACCUM+SWEEP_HIGH+CLOSE_BELOW: confirmed reversal DOWN"
                conf      = 3 if micro_conf == "DOWN" else 2
            else:
                direction = "UP"
                reason    = "ACCUM+SWEEP_HIGH+HOLDS_ABOVE: price holding above sweep = bull continuation"
                conf      = 1
        return {"phase": "MANIPULATION_DONE", "direction": direction,
                "confidence": conf, "sweep_level": sweep["level"], "reason": reason}

    if is_accum:
        return {"phase": "ACCUMULATION", "direction": None, "confidence": 1,
                "reason": "tight range, wait for sweep"}

    if manip_ok:
        if sweep["type"] == "LOW":
            d = "UP"
            reason = "SWEEP_LOW: potential reversal UP"
        else:
            last_close = c15[-1]["c"]
            if last_close < sweep["level"] * 0.9998:
                d = "DOWN"; reason = "SWEEP_HIGH+CLOSE_BELOW: reversal DOWN"
            else:
                d = "UP"; reason = "SWEEP_HIGH+HOLDS: continuation UP"
        return {"phase": "MANIPULATION", "direction": d, "confidence": 2,
                "sweep_level": sweep["level"], "reason": reason}

    if abs(last_move_15) > 0.15:
        return {"phase": "DISTRIBUTION",
                "direction": "UP" if last_move_15 > 0 else "DOWN",
                "confidence": 1, "reason": "active move"}

    return {"phase": "NONE", "direction": None, "confidence": 0, "reason": ""}

# ============================================================
# CONTEXT
# ============================================================

def classify_vol(c15):
    if len(c15) < 10:
        return "UNKNOWN", 0.0
    ranges = [(c["h"] - c["l"]) / c["c"] * 100 for c in c15[-10:]]
    avg    = sum(ranges) / len(ranges)
    rec    = sum(ranges[-3:]) / 3
    pri    = sum(ranges[:7]) / 7
    if avg < 0.08:        cond = "LOW_VOL"
    elif rec > pri * 1.5: cond = "EXPANSION"
    elif avg > 0.3:       cond = "HIGH_VOL"
    else:                 cond = "NORMAL"
    return cond, round(avg, 4)

def classify_session():
    hour = datetime.datetime.now(datetime.timezone.utc).hour
    if 7 <= hour < 12:    return "LONDON", 1
    elif 12 <= hour < 17: return "NY_OPEN", 0
    elif 17 <= hour < 21: return "NY_AFTERNOON", 0
    elif 21 <= hour or hour < 3: return "ASIA_ACTIVE", 0
    else:                  return "DEAD_HOURS", -1

def classify_mkt(c15, c5m):
    """Ринкова структура на 15m + 5m (без 1H — він нерелевантний для 15хв торгівлі)"""
    if len(c15) < 10:
        return "RANGING"
    closes = [c["c"] for c in c15[-12:]]
    ups    = sum(1 for i in range(1, len(closes)) if closes[i] > closes[i-1])
    downs  = len(closes) - 1 - ups
    if ups >= 9 or downs >= 9:
        return "TRENDING"
    alts = sum(1 for i in range(1, len(closes) - 1)
               if (closes[i] > closes[i-1]) != (closes[i+1] > closes[i]))
    return "CHOPPY" if alts >= 8 else "RANGING"

# ============================================================
# LAST SIGNAL CONTEXT — щоб AI не повторював
# ============================================================

def get_last_signal_context():
    """Повертає інфо про останній сигнал щоб AI не повторював механічно"""
    if not signal_history:
        return "no_prev"
    last = signal_history[-1]
    outcome = last.get("outcome", "PENDING")
    decision = last.get("decision", "?")
    real_move = last.get("real_move", 0)
    return "prev=%s outcome=%s real_move=%+.0f" % (decision, outcome, real_move)

# ============================================================
# FULL PAYLOAD
# ============================================================

def get_full_payload():
    c15 = fetch_candles("15m", 100)
    c5m = fetch_candles("5m",  50)
    c1m = fetch_candles("1m",  30)

    if not c15:
        return None

    price      = c15[-1]["c"]
    prev       = c15[-2]["c"] if len(c15) >= 2 else price
    chg_15m    = round((price - prev) / prev * 100, 4)
    chg_5m     = round((c5m[-1]["c"] - c5m[-4]["c"]) / c5m[-4]["c"] * 100, 4) if len(c5m) >= 4 else 0.0
    momentum_3 = round((c15[-1]["c"] - c15[-4]["c"]) / c15[-4]["c"] * 100, 4) if len(c15) >= 4 else 0.0
    micro_mom  = round((c1m[-1]["c"] - c1m[-4]["c"]) / c1m[-4]["c"] * 100, 4) if len(c1m) >= 4 else 0.0

    # Структура: 15m (контекст) і 5m (тактика)
    st15m = market_structure(c15) if len(c15) >= 6 else "RANGING"
    st5m  = market_structure(c5m) if len(c5m) >= 6 else "RANGING"
    st1m  = market_structure(c1m) if len(c1m) >= 6 else "RANGING"

    # Sweep на всіх TF
    sweep_15m = liq_sweep(c15)
    sweep_5m  = liq_sweep(c5m) if c5m else {"type": "NONE", "level": 0.0, "ago": 0}
    sweep_1m  = liq_sweep(c1m) if c1m else {"type": "NONE", "level": 0.0, "ago": 0}

    # Stop clusters на 15m
    sa, sb = stop_clusters(c15, price)

    # FVG на 5m і 1m (найточніше для 15хв прогнозу)
    fvg5_a, fvg5_b = find_fvg(c5m[-30:], price) if len(c5m) >= 5 else (None, None)
    fvg1_a, fvg1_b = find_fvg(c1m[-20:], price) if len(c1m) >= 5 else (None, None)

    # BOS/CHoCH на 5m відносно 15m структури
    bc5m = bos_choch(c5m, st15m) if len(c5m) >= 5 else None

    # Manipulation на 5m (найточніше)
    manip = detect_manipulation(c5m[-10:] if len(c5m) >= 10 else c15[-10:],
                                sweep_5m, price)

    # AMD
    amd = detect_amd(c15, c5m, price)

    fund         = fetch_funding()
    liqs         = fetch_liquidations()
    oi, oi_chg   = fetch_oi()
    ob           = fetch_orderbook()
    lsr          = fetch_lsr()

    vol_cond, vol_score = classify_vol(c15)
    session, sess_boost = classify_session()
    mkt_cond             = classify_mkt(c15, c5m)

    dist_above = round((sa["price"] - price) / price * 100, 4) if sa else 999.0
    dist_below = round((price - sb["price"]) / price * 100, 4) if sb else 999.0

    last_sig_ctx = get_last_signal_context()

    return {
        "timestamp": utc_now_str(), "ts_unix": ts_unix(),
        "price": {
            "current":    price,
            "chg_15m":    chg_15m,
            "chg_5m":     chg_5m,
            "momentum_3": momentum_3,
            "micro_mom":  micro_mom,
            "mark":       fund["mark"],
            "basis":      fund["basis"],
        },
        "structure": {"15m": st15m, "5m": st5m, "1m": st1m},
        "liquidity": {
            "sweep_15m":   sweep_15m,
            "sweep_5m":    sweep_5m,
            "sweep_1m":    sweep_1m,
            "stops_above": sa,
            "stops_below": sb,
            "dist_above":  dist_above,
            "dist_below":  dist_below,
            "fvg5_above":  fvg5_a,
            "fvg5_below":  fvg5_b,
            "fvg1_above":  fvg1_a,
            "fvg1_below":  fvg1_b,
            "bos_choch_5m": bc5m,
        },
        "amd": amd,
        "manipulation": manip,
        "positioning": {
            "funding_rate": fund["rate"],
            "funding_sent": fund["sentiment"],
            "liq_longs":    liqs["liq_longs"],
            "liq_shorts":   liqs["liq_shorts"],
            "liq_signal":   liqs["signal"],
            "exhaustion":   liqs["exhaustion"],
            "liq_total":    liqs["total_usd"],
            "oi":           oi,
            "oi_change":    oi_chg,
            "ob_bias":      ob["bias"],
            "ob_imbalance": ob["imbalance"],
            "lsr_bias":     lsr["bias"],
            "lsr_ratio":    lsr["ratio"],
            "crowd_long":   lsr["long_pct"],
        },
        "context": {
            "volatility":       vol_cond,
            "vol_score":        vol_score,
            "session":          session,
            "session_boost":    sess_boost,
            "market_condition": mkt_cond,
            "last_signal":      last_sig_ctx,
        },
    }

# ============================================================
# NEWS / BALANCE / POLYMARKET
# ============================================================

def get_news():
    try:
        data = safe_get("https://min-api.cryptocompare.com/data/v2/news/",
                        {"categories": "BTC,Bitcoin", "lTs": 0})
        if data and "Data" in data and data["Data"]:
            lines = []
            bkw = ["bull","surge","rally","rise","gain","buy","etf","adoption"]
            skw = ["bear","drop","fall","crash","dump","sell","ban","hack","fear"]
            for item in data["Data"][:6]:
                t    = item.get("title","").lower()
                p    = sum(1 for k in bkw if k in t)
                n    = sum(1 for k in skw if k in t)
                sent = "BULLISH" if p > n else "BEARISH" if n > p else "NEUTRAL"
                lines.append("[%s] %s" % (sent, item.get("title","")[:70]))
            return "\n".join(lines)
    except Exception:
        pass
    return "Новини недоступні"

def get_balance(s):
    """Баланс через Builder API Keys з Railway Variables."""
    api_key    = os.getenv("POLY_API_KEY","")
    secret     = os.getenv("POLY_SECRET","")
    passphrase = os.getenv("POLY_PASSPHRASE","")
    if not api_key or not secret:
        print("[Balance] POLY_API_KEY не встановлено в Railway")
        return 0.0, "Встанови POLY_API_KEY в Railway Variables"
    import hmac as _h, hashlib, base64
    ts   = str(int(time.time()))
    path = "/balance-allowance?asset_type=USDC"
    sig  = base64.b64encode(_h.new(secret.encode(),(ts+"GET"+path).encode(),hashlib.sha256).digest()).decode()
    r    = requests.get("https://clob.polymarket.com/balance-allowance",
               params={"asset_type":"USDC"},
               headers={"POLY-API-KEY":api_key,"POLY-SIGNATURE":sig,
                        "POLY-TIMESTAMP":ts,"POLY-PASSPHRASE":passphrase},
               timeout=15)
    print("[Balance] HTTP %d: %s" % (r.status_code, r.text[:100]))
    if r.status_code == 200:
        raw = float(r.json().get("balance") or 0)
        bal = raw / 1e6 if raw > 1000 else raw
        print("[Balance] $%.2f" % round(bal,2))
        return round(bal,2), getattr(s,'funder',None) or "Polymarket"
    return 0.0, getattr(s,'funder',None) or "error"


def find_market():
    """btc-updown-15m-{timestamp} через slug + токени через CLOB."""
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
            if r.status_code != 200: return None,None,0.5,0.5
            toks = r.json().get("tokens",[])
            yi=ni=""; yp=np_=0.5
            for t in toks:
                oc=(t.get("outcome") or "").upper()
                tid=(t.get("token_id") or t.get("tokenId") or "").strip()
                pr=float(t.get("price",0.5) or 0.5)
                if oc in ("YES","UP","HIGHER","ABOVE"):   yi=tid; yp=pr
                elif oc in ("NO","DOWN","LOWER","BELOW"): ni=tid; np_=pr
            if not yi and toks: yi=(toks[0].get("token_id") or toks[0].get("tokenId") or "").strip(); yp=float(toks[0].get("price",0.5) or 0.5)
            if not ni and len(toks)>1: ni=(toks[1].get("token_id") or toks[1].get("tokenId") or "").strip(); np_=float(toks[1].get("price",0.5) or 0.5)
            return yi,ni,yp,np_
        except Exception as e:
            print("[Market] token err: %s"%e); return None,None,0.5,0.5

    def try_slug(slug, now):
        try:
            r=requests.get("https://gamma-api.polymarket.com/events",
                           params={"slug":slug},timeout=15)
            if r.status_code!=200: return None
            raw=r.json(); evs=raw if isinstance(raw,list) else ([raw] if isinstance(raw,dict) and raw else [])
            if not evs: return None
            ev=evs[0]; title=ev.get("title","") or slug
            print("[Market] Event: %s"%title[:60])
            for m in ev.get("markets",[]):
                if m.get("closed",True): continue
                cid=(m.get("conditionId") or m.get("condition_id") or m.get("id") or "").strip()
                if not cid: continue
                et=end_ts(m); diff=(et-now) if et else 900.0
                if diff<=0: continue
                yi,ni,yp,np_=get_tokens(cid)
                if not yi or not ni: continue
                q=m.get("question","") or title
                print("[Market] ЗНАЙДЕНО: %s diff=%.0fs"%(q[:60],diff))
                print("[Market] YES=%s NO=%s"%(yi[:20],ni[:20]))
                return {"yes_id":yi,"no_id":ni,"yes_p":yp,"no_p":np_,
                        "q":q,"cid":cid,"diff":round(diff,1)}
        except Exception as e:
            print("[Market] slug err: %s"%e)
        return None

    now=time.time(); cur=int(now//ROUND)*ROUND
    for attempt in range(1,8):
        print("[Market] Спроба %d/7 | %s UTC"%(attempt,datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S")))
        for ts in [cur,cur+ROUND,cur-ROUND]:
            r=try_slug("%s%d"%(SLUG,ts),now)
            if r: return r
        if attempt<7: print("[Market] Retry in 3s..."); time.sleep(3)
    print("[Market] НЕ ЗНАЙДЕНО"); return None


def place_bet(s, direction: str, amount: float) -> dict:
    """Ставка через Builder API Keys + py-clob-client."""
    if not s.ok:   return {"success":False,"error":"Гаманець не підключено"}
    if amount < 1: return {"success":False,"error":"Мінімум $1"}

    api_key    = os.getenv("POLY_API_KEY","")
    secret     = os.getenv("POLY_SECRET","")
    passphrase = os.getenv("POLY_PASSPHRASE","")
    if not api_key: return {"success":False,"error":"POLY_API_KEY не встановлено в Railway Variables"}

    mkt = find_market()
    if not mkt: return {"success":False,"error":"Активний маркет не знайдено"}

    # Сумісність з різними форматами маркету
    yes_id = mkt.get("yes_id") or mkt.get("token_id_yes","")
    no_id  = mkt.get("no_id")  or mkt.get("token_id_no","")
    yes_p  = mkt.get("yes_p")  or mkt.get("price_yes", 0.5)
    no_p   = mkt.get("no_p")   or mkt.get("price_no",  0.5)
    q_text = mkt.get("q") or mkt.get("question","BTC")

    token_id = yes_id if direction=="UP" else no_id
    price    = yes_p  if direction=="UP" else no_p
    price    = max(0.01, min(0.99, float(price)))

    # Свіжа ціна
    try:
        r = requests.get("https://clob.polymarket.com/midpoints",
                         params={"token_id":token_id},timeout=10)
        if r.status_code==200:
            mid=float(r.json().get("mid",price))
            if 0.01<=mid<=0.99: price=mid
    except Exception: pass

    size = round(amount / price, 2)
    print("[Bet] dir=%s token=%s price=%.4f size=%.2f usdc=%.2f" % (
        direction, token_id[:20], price, size, amount))

    # Спроба 1: py-clob-client з ApiCreds
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OrderArgs, OrderType, Side, ApiCreds
        from py_clob_client.constants import POLYGON

        creds  = ApiCreds(api_key=api_key, api_secret=secret, api_passphrase=passphrase)
        funder = getattr(s,'funder',None) or getattr(s,'wallet_address',None) or ""
        key    = getattr(s,'key',None) or getattr(s,'private_key',"")

        client = ClobClient(host="https://clob.polymarket.com", key=key,
                            chain_id=POLYGON, signature_type=1,
                            funder=funder, creds=creds)
        client.set_api_creds(creds)

        order = client.create_order(OrderArgs(
            token_id=token_id, price=price, size=size, side=Side.BUY))
        resp  = client.post_order(order, OrderType.GTC)
        print("[Bet] py-clob OK: %s" % str(resp)[:80])
        return {"success":True,"order":resp,"price":price,
                "pot":round(size-amount,2),"market_name":q_text[:60]}

    except ImportError:
        print("[Bet] py-clob-client не встановлено, HTTP fallback...")
    except Exception as e:
        print("[Bet] py-clob error: %s" % str(e)[:150])

    # Спроба 2: прямий HTTP
    try:
        import json as _j, hmac as _h, hashlib, base64
        from eth_account import Account
        from eth_account.messages import encode_defunct

        key     = getattr(s,'key',None) or getattr(s,'private_key',"")
        funder  = getattr(s,'funder',None) or getattr(s,'wallet_address',"")
        account = Account.from_key(key)
        signer  = account.address

        order_ts = str(int(time.time()))
        body = _j.dumps({
            "order":{"salt":str(int(time.time()*1000)),
                     "maker":funder,"signer":signer,
                     "taker":"0x0000000000000000000000000000000000000000",
                     "tokenId":token_id,
                     "makerAmount":str(int(amount*1e6)),
                     "takerAmount":str(int(size*1e6)),
                     "expiration":"0","nonce":"0","feeRateBps":"0",
                     "side":"BUY","signatureType":1,"signature":"0x"},
            "orderType":"GTC","owner":funder,
        },separators=(",",":"))

        sign_str = order_ts+"POST"+"/order"+body
        asig = base64.b64encode(
            _h.new(secret.encode(),sign_str.encode(),hashlib.sha256).digest()
        ).decode()

        ro = requests.post("https://clob.polymarket.com/order", data=body,
            headers={"POLY-API-KEY":api_key,"POLY-SIGNATURE":asig,
                     "POLY-TIMESTAMP":order_ts,"POLY-PASSPHRASE":passphrase,
                     "Content-Type":"application/json"},timeout=20)
        print("[Bet] HTTP %d: %s" % (ro.status_code, ro.text[:150]))

        if ro.status_code in (200,201):
            return {"success":True,"order":ro.json(),"price":price,
                    "pot":round(size-amount,2),"market_name":q_text[:60]}
        return {"success":False,"error":"HTTP%d: %s"%(ro.status_code,ro.text[:150])}

    except Exception as e:
        err=str(e); print("[Bet] FAIL: %s"%err)
        if "not enough" in err.lower() or "allowance" in err.lower():
            return {"success":False,"error":"Недостатньо USDC на Polymarket"}
        return {"success":False,"error":err[:200]}


def analyze_with_ai(payload):
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        liq    = payload["liquidity"]
        pos    = payload["positioning"]
        pr     = payload["price"]
        st     = payload["structure"]
        ctx    = payload["context"]
        manip  = payload["manipulation"]
        amd    = payload["amd"]

        sw15  = liq.get("sweep_15m",{})
        sw5   = liq.get("sweep_5m",{})
        sw1   = liq.get("sweep_1m",{})
        bc5   = liq.get("bos_choch_5m")
        f5a   = liq.get("fvg5_above")
        f5b   = liq.get("fvg5_below")
        f1a   = liq.get("fvg1_above")
        f1b   = liq.get("fvg1_below")
        sa    = liq.get("stops_above") or {}
        sb    = liq.get("stops_below") or {}

        sw15_s = "%s@%.2f(%dc)"%(sw15.get("type","NONE"),sw15.get("level",0),sw15.get("ago",0))
        sw5_s  = "%s@%.2f(%dc)"%(sw5.get("type","NONE"),sw5.get("level",0),sw5.get("ago",0))
        sw1_s  = "%s@%.2f(%dc)"%(sw1.get("type","NONE"),sw1.get("level",0),sw1.get("ago",0))
        bc5_s  = "%s %s@%.2f"%(bc5["type"],bc5["dir"],bc5["level"]) if bc5 else "none"
        f5a_s  = "dist=%.3f%% sz=%.2f"%(f5a["dist"],f5a.get("size",0)) if f5a else "none"
        f5b_s  = "dist=%.3f%% sz=%.2f"%(f5b["dist"],f5b.get("size",0)) if f5b else "none"
        f1a_s  = "dist=%.3f%% sz=%.2f"%(f1a["dist"],f1a.get("size",0)) if f1a else "none"
        f1b_s  = "dist=%.3f%% sz=%.2f"%(f1b["dist"],f1b.get("size",0)) if f1b else "none"
        sa_s   = "%.2f(%s) dist=%.3f%%"%(sa.get("price",0),sa.get("type",""),liq.get("dist_above",999)) if sa else "none"
        sb_s   = "%.2f(%s) dist=%.3f%%"%(sb.get("price",0),sb.get("type",""),liq.get("dist_below",999)) if sb else "none"
        amd_s  = "phase=%s dir=%s conf=%d reason=%s"%(
                    amd.get("phase","NONE"),amd.get("direction","?"),
                    amd.get("confidence",0),amd.get("reason",""))

        msg = (
            "=== MARKET DATA ===\n"
            "Time: %s | Session: %s (boost=%+d)\n"
            "LastSignal: %s\n\n"
            "PRICE: $%.2f\n"
            "15m_chg: %+.4f%%  5m_chg: %+.4f%%  Mom3x15m: %+.4f%%  Micro1m: %+.4f%%\n"
            "Mark: $%.2f  Basis: %+.2f\n\n"
            "STRUCTURE (no 1H — only relevant TFs for 15min trade):\n"
            "  15m=%s  5m=%s  1m=%s\n"
            "  MarketCond(based on 15m): %s  Vol: %s (%.4f%%)\n\n"
            "AMD: %s\n\n"
            "SWEEPS:\n"
            "  15m: %s\n"
            "  5m:  %s\n"
            "  1m:  %s\n\n"
            "STOPS/LIQUIDITY:\n"
            "  Above: %s\n"
            "  Below: %s\n\n"
            "FVG (5m): above=%s  below=%s\n"
            "FVG (1m): above=%s  below=%s\n"
            "BOS/CHoCH(5m): %s\n\n"
            "MANIPULATION: trap=%s  hint=%s\n\n"
            "POSITIONING:\n"
            "  Funding: %+.6f (%s)\n"
            "  LiqLongs: $%.0f  LiqShorts: $%.0f  Signal: %s  Exhaust: %s\n"
            "  OI: %.0f  OI_chg: %+.4f%%\n"
            "  Orderbook: %s (%+.1f%%)\n"
            "  L/S ratio: %.3f (%s)  CrowdLong: %.1f%%"
        ) % (
            payload["timestamp"], ctx["session"], ctx["session_boost"],
            ctx["last_signal"],
            pr["current"],
            pr["chg_15m"], pr["chg_5m"], pr["momentum_3"], pr["micro_mom"],
            pr["mark"], pr["basis"],
            st["15m"], st["5m"], st["1m"],
            ctx["market_condition"], ctx["volatility"], ctx["vol_score"],
            amd_s,
            sw15_s, sw5_s, sw1_s,
            sa_s, sb_s,
            f5a_s, f5b_s, f1a_s, f1b_s, bc5_s,
            manip["trap_type"], str(manip["reversal_signal"]),
            pos["funding_rate"], pos["funding_sent"],
            pos["liq_longs"], pos["liq_shorts"], pos["liq_signal"], pos["exhaustion"],
            pos["oi"], pos["oi_change"],
            pos["ob_bias"], pos["ob_imbalance"],
            pos["lsr_ratio"], pos["lsr_bias"], pos["crowd_long"]
        )

        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role":"system","content":SYSTEM_PROMPT},
                      {"role":"user","content":msg}],
            temperature=0.1,
            response_format={"type":"json_object"}
        )
        return json.loads(resp.choices[0].message.content)
    except Exception as e:
        logger.error("AI: %s", e)
        return None

# ============================================================
# HISTORY
# ============================================================

def save_error(sig):
    try:
        errors = []
        if os.path.exists(ERRORS_FILE):
            with open(ERRORS_FILE) as f:
                errors = json.load(f)
        errors.append(sig)
        errors = errors[-300:]
        with open(ERRORS_FILE,"w") as f:
            json.dump(errors, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning("Err save: %s", e)

def save_full_log(payload, result):
    try:
        logs = []
        if os.path.exists(FULL_LOG_FILE):
            with open(FULL_LOG_FILE) as f:
                logs = json.load(f)
        logs.append({"payload":payload,"result":result,"ts":utc_now_str()})
        logs = logs[-500:]
        with open(FULL_LOG_FILE,"w") as f:
            json.dump(logs, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning("Log: %s", e)

def load_history():
    global signal_history
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE) as f:
                signal_history = json.load(f)
    except Exception as e:
        logger.warning("Hist load: %s", e)

def save_history():
    try:
        with open(HISTORY_FILE,"w") as f:
            json.dump(signal_history, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning("Hist save: %s", e)

def check_signals():
    now     = int(time.time())
    changed = False
    for sig in signal_history:
        if sig.get("outcome"):
            continue
        if now - sig.get("ts_unix",0) >= 900:
            cur = fetch_price()
            if cur:
                entry = sig.get("entry_price",0)
                dec   = sig.get("decision","")
                sig["outcome"]    = "WIN" if (dec=="UP" and cur>entry) or (dec=="DOWN" and cur<entry) else "LOSS"
                sig["exit_price"] = cur
                sig["real_move"]  = round(cur - entry, 2)
                changed = True
                if sig["outcome"] == "LOSS":
                    save_error(sig)
    if changed:
        save_history()

def get_stats_text():
    if not signal_history:
        return "Немає даних."
    checked = [s for s in signal_history if s.get("outcome")]
    if not checked:
        return "Результати перевіряються..."
    wins  = [s for s in checked if s["outcome"]=="WIN"]
    total = len(checked)
    wr    = round(len(wins)/total*100,1)
    lines = ["=== СТАТИСТИКА v4.1 ===",
             "Всього: %d | WIN: %d | Winrate: %.1f%%" % (total,len(wins),wr),""]
    for st in ("HIGH","MEDIUM","LOW"):
        sub = [s for s in checked if s.get("strength")==st]
        if sub:
            w = len([s for s in sub if s["outcome"]=="WIN"])
            lines.append("%s: %d/%d (%.1f%%)"%(st,w,len(sub),round(w/len(sub)*100,1)))
    lines.append("")
    for cond in ("TRENDING","RANGING","CHOPPY"):
        sub = [s for s in checked if s.get("mkt_cond")==cond]
        if sub:
            w = len([s for s in sub if s["outcome"]=="WIN"])
            lines.append("%s: %d/%d (%.1f%%)"%(cond,w,len(sub),round(w/len(sub)*100,1)))
    lines.append("")
    for sess in ("LONDON","NY_OPEN","NY_AFTERNOON","ASIA_ACTIVE","DEAD_HOURS"):
        sub = [s for s in checked if s.get("session")==sess]
        if sub:
            w = len([s for s in sub if s["outcome"]=="WIN"])
            lines.append("%s: %d/%d (%.1f%%)"%(sess,w,len(sub),round(w/len(sub)*100,1)))
    lines.append("\n/errors — деталі помилок")
    return "\n".join(lines)

# ============================================================
# TELEGRAM
# ============================================================

WELCOME = (
    "BTC Polymarket Bot v4.1\n"
    "AMD+FVG+1m/5m/15m | Self-correcting AI\n"
    "Сигнали о :00 :15 :30 :45 UTC\n\n"
    "/analyze   — аналіз зараз\n"
    "/status    — ринкові дані\n"
    "/balance   — баланс USDC\n"
    "/news      — новини BTC\n"
    "/stats     — winrate\n"
    "/errors    — програні сигнали\n"
    "/autoon    — авто ($%.0f/сигнал)\n"
    "/autooff   — стоп авто\n"
    "/trades    — останні ставки\n"
    "/resetstats — скинути статистику\n"
    "/help      — довідка"
) % AUTO_BET_SIZE

async def cmd_start(u,c):   await u.message.reply_text(WELCOME)
async def cmd_help(u,c):    await u.message.reply_text(WELCOME)
async def cmd_analyze(u,c): await u.message.reply_text("Аналіз..."); await run_cycle(c.application)
async def cmd_stats(u,c):   check_signals(); await u.message.reply_text(get_stats_text())

async def cmd_resetstats(u,c):
    global signal_history, trade_history
    signal_history = []; trade_history = []
    for f in (HISTORY_FILE, ERRORS_FILE, FULL_LOG_FILE):
        try:
            if os.path.exists(f): os.remove(f)
        except Exception: pass
    await u.message.reply_text("Статистика очищена.")

async def cmd_errors(u,c):
    check_signals()
    if not os.path.exists(ERRORS_FILE):
        await u.message.reply_text("Помилок ще немає."); return
    try:
        with open(ERRORS_FILE) as f: errors = json.load(f)
    except Exception:
        await u.message.reply_text("Помилка читання."); return
    if not errors:
        await u.message.reply_text("Помилок ще немає."); return
    lines = ["=== ПОМИЛКИ (%d) ===" % len(errors),""]
    for i,e in enumerate(errors[-10:],1):
        lines.append(
            "%d. %s %s(S:%s) $%.0f->$%.0f(%+.0f)\n"
            "   15M=%s,5M=%s|%s|%s\n"
            "   Sweep15m:%s(%sc) Trap:%s AMD:%s\n"
            "   OI:%+.3f%% d_up:%.3f%% d_dn:%.3f%%\n"
            "   KEY: %s\n" % (
                i,e.get("decision","?"),e.get("strength","?"),e.get("confidence_score","?"),
                e.get("entry_price",0),e.get("exit_price",0),e.get("real_move",0),
                e.get("st15m","?"),e.get("st5m","?"),e.get("mkt_cond","?"),e.get("session","?"),
                e.get("sweep_type","?"),e.get("sweep_ago","?"),
                e.get("trap_type","?"),e.get("amd_phase","?"),
                e.get("oi_change",0),e.get("dist_above",0),e.get("dist_below",0),
                e.get("key_signal","")[:100]
            )
        )
    text = "\n".join(lines)
    if len(text)>4000: text=text[:4000]+"\n..."
    await u.message.reply_text(text)

async def cmd_autoon(u,c):
    global auto_trade_active; auto_trade_active=True
    await u.message.reply_text("Авто ON | $%.0f | Min: %s"%(AUTO_BET_SIZE,AUTO_MIN_STRENGTH))

async def cmd_autooff(u,c):
    global auto_trade_active; auto_trade_active=False
    await u.message.reply_text("Авто OFF")

async def cmd_trades(u,c):
    if not trade_history: await u.message.reply_text("Немає ставок."); return
    lines=["%s|$%.2f|%s"%(t["decision"],t["amount"],t["time"].strftime("%H:%M")) for t in trade_history[-10:]]
    await u.message.reply_text("Останні ставки:\n\n"+"\n".join(lines))

async def cmd_news(u,c): await u.message.reply_text("Новини BTC:\n\n%s"%get_news())

async def cmd_balance(u,c):
    await u.message.reply_text("Перевіряю...")
    bal,addr=get_balance()
    if bal is not None: await u.message.reply_text("$%.2f USDC\n%s...%s"%(bal,addr[:10],addr[-6:]))
    else: await u.message.reply_text("Помилка:\n%s"%addr)

async def cmd_status(u,c):
    await u.message.reply_text("Збираю дані...")
    p=get_full_payload()
    if not p: await u.message.reply_text("Помилка даних"); return
    pr=p["price"]; st=p["structure"]; liq=p["liquidity"]
    pos=p["positioning"]; ctx=p["context"]; manip=p["manipulation"]; amd=p["amd"]
    sw15=liq.get("sweep_15m",{}); sa=liq.get("stops_above") or {}; sb=liq.get("stops_below") or {}
    bc5=liq.get("bos_choch_5m")
    await u.message.reply_text(
        "%s\n$%.2f|15m:%+.4f%%|5m:%+.4f%%\nMom3:%+.4f%%|Micro1m:%+.4f%%\n\n"
        "15M=%s|5M=%s|1M=%s\nMkt:%s|Vol:%s|Sess:%s\n"
        "BOS5m:%s\nAMD:%s->%s [%s]\n\n"
        "Sweep15m:%s@%.2f(%dc)\nUp:%.2f(%.3f%%)\nDn:%.2f(%.3f%%)\n\n"
        "Trap:%s|Hint:%s\nFund:%+.6f(%s)\nLiqs:%s|OI:%+.4f%%\n"
        "Book:%s(%+.1f%%)|L/S:%.2f(%s)\n\nAuto:%s\nLast:%s"%(
            p["timestamp"],pr["current"],pr["chg_15m"],pr["chg_5m"],
            pr["momentum_3"],pr["micro_mom"],
            st["15m"],st["5m"],st["1m"],
            ctx["market_condition"],ctx["volatility"],ctx["session"],
            "%s %s@%.2f"%(bc5["type"],bc5["dir"],bc5["level"]) if bc5 else "none",
            amd.get("phase","NONE"),amd.get("direction","?"),amd.get("reason",""),
            sw15.get("type","NONE"),sw15.get("level",0),sw15.get("ago",0),
            sa.get("price",0),liq.get("dist_above",0),
            sb.get("price",0),liq.get("dist_below",0),
            manip["trap_type"],str(manip["reversal_signal"]),
            pos["funding_rate"],pos["funding_sent"],
            pos["liq_signal"],pos["oi_change"],
            pos["ob_bias"],pos["ob_imbalance"],pos["lsr_ratio"],pos["lsr_bias"],
            "ON($%.0f)"%AUTO_BET_SIZE if auto_trade_active else "OFF",
            ctx["last_signal"]
        )
    )

async def handle_message(u,c):
    global pending_trade
    if not pending_trade or time.time()-pending_trade.get("timestamp",0)>600:
        pending_trade={}; await u.message.reply_text("Немає активних сигналів."); return
    try:
        amount=float(u.message.text.strip())
        if amount<1 or amount>500: await u.message.reply_text("Сума від $1 до $500"); return
        direction=pending_trade["direction"]
        kb=InlineKeyboardMarkup([[
            InlineKeyboardButton("$%.2f на %s"%(amount,direction),
                                 callback_data="execute_%s_%.2f"%(direction,amount)),
            InlineKeyboardButton("Скасувати",callback_data="skip")]])
        await u.message.reply_text("Підтвердити?",reply_markup=kb)
    except ValueError: await u.message.reply_text("Введи число, наприклад: 10")

async def handle_callback(u,c):
    global pending_trade; q=u.callback_query; await q.answer()
    if q.data=="skip": pending_trade={}; await q.edit_message_text("Пропущено."); return
    if q.data.startswith("confirm_"): await q.edit_message_text("Введи суму в USDC:"); return
    if q.data.startswith("execute_"):
        parts=q.data.split("_"); direction=parts[1]; amount=float(parts[2])
        await q.edit_message_text("Шукаю ринок...")
        market_id,market_name=find_market()
        if not market_id:
            await c.bot.send_message(chat_id=TELEGRAM_CHAT_ID,text="Ринок не знайдено.")
            pending_trade={}; return
        outcome="YES" if direction=="UP" else "NO"
        bet_result=place_bet(market_id,outcome,amount)
        if bet_result["success"]:
            price=bet_result.get("price",0.0); pot=round(amount/price-amount,2) if price>0 else 0.0
            trade_history.append({"decision":direction,"amount":amount,
                                   "entry":pending_trade.get("price",0),
                                   "time":datetime.datetime.now(datetime.timezone.utc)})
            await c.bot.send_message(chat_id=TELEGRAM_CHAT_ID,
                                     text="%s|%s|$%.2f->$%.2f"%(direction,market_name,amount,pot))
        else:
            await c.bot.send_message(chat_id=TELEGRAM_CHAT_ID,text="Помилка: %s"%bet_result["error"])
        pending_trade={}

async def execute_auto_trade(app, payload, result):
    decision=result.get("decision"); strength=result.get("strength","LOW"); logic=result.get("logic","")
    if not decision: return
    strength_order={"HIGH":3,"MEDIUM":2,"LOW":1}
    if strength_order.get(strength,1)<strength_order.get(AUTO_MIN_STRENGTH,2):
        await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID,
                                   text="Сигнал %s(%s) слабкий. Пропускаю."%(decision,strength)); return
    outcome="YES" if decision=="UP" else "NO"
    market_id,market_name=find_market()
    if not market_id: await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID,text="Ринок не знайдено."); return
    bet_result=place_bet(market_id,outcome,AUTO_BET_SIZE)
    if bet_result["success"]:
        price=bet_result.get("price",0.0); pot=round(AUTO_BET_SIZE/price-AUTO_BET_SIZE,2) if price>0 else 0.0
        trade_history.append({"decision":decision,"amount":AUTO_BET_SIZE,
                               "entry":payload["price"]["current"],
                               "time":datetime.datetime.now(datetime.timezone.utc)})
        bal,_=get_balance()
        await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID,
                                   text="СТАВКА\n%s|%s\n$%.2f->$%.2f\nBal:%s\n\n%s"%(
                                       decision,market_name,AUTO_BET_SIZE,pot,
                                       "$%.2f"%bal if bal is not None else "N/A",logic))
    else:
        await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID,
                                   text="СТАВКА НЕВИК.\n%s\n%s|%s"%(bet_result["error"],decision,strength))

# ============================================================
# MAIN CYCLE
# ============================================================

async def run_cycle(app):
    global pending_trade
    logger.info("Цикл аналізу")
    check_signals()
    payload=get_full_payload()
    if not payload:
        await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID,text="Помилка даних Binance"); return
    result=analyze_with_ai(payload)
    if not result:
        await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID,text="Помилка AI"); return
    save_full_log(payload,result)

    decision  = result.get("decision","UP")
    strength  = result.get("strength","LOW")
    logic     = result.get("logic","")
    score     = result.get("confidence_score",0)
    reasons   = result.get("reasons",[])
    key_sig   = result.get("key_signal","")
    risk_note = result.get("risk_note","")
    mkt_cond  = result.get("market_condition",payload["context"]["market_condition"])
    amd_used  = result.get("amd_used",False)

    liq    = payload["liquidity"]
    sw15   = liq.get("sweep_15m",{})
    manip  = payload["manipulation"]
    ctx    = payload["context"]
    amd    = payload["amd"]

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
        "funding_sent":payload["positioning"]["funding_sent"],
        "liq_signal":payload["positioning"]["liq_signal"],
        "oi_change":payload["positioning"]["oi_change"],
        "ob_bias":payload["positioning"]["ob_bias"],
        "lsr_bias":payload["positioning"]["lsr_bias"],
    }
    signal_history.append(sig_record)
    save_history()

    dec_ua  = "ВИЩЕ" if decision=="UP" else "НИЖЧЕ"
    str_ua  = {"HIGH":"СИЛЬНИЙ","MEDIUM":"СЕРЕДНІЙ","LOW":"СЛАБКИЙ"}.get(strength,strength)
    reas_s  = "\n".join("- "+r for r in reasons[:3]) if reasons else ""
    risk_s  = "⚠️ %s"%risk_note if risk_note and risk_note.lower() not in ("none","","no") else ""

    main_txt=(
        "%s | %s | Score:%+d\n"
        "$%.2f | %s | %s\n\n"
        "KEY: %s\n\n"
        "%s\n\n%s\n\n%s"
    )%(dec_ua,str_ua,score,payload["price"]["current"],mkt_cond,ctx["session"],
       key_sig,logic,reas_s,risk_s)

    if auto_trade_active:
        await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID,text="АВТО-СИГНАЛ\n\n"+main_txt)
        await execute_auto_trade(app,payload,result)
    else:
        btn_dir="YES" if decision=="UP" else "NO"
        pending_trade={"direction":btn_dir,"amount":None,
                       "timestamp":time.time(),"price":payload["price"]["current"]}
        kb=InlineKeyboardMarkup([[
            InlineKeyboardButton("Так",callback_data="confirm_%s"%btn_dir),
            InlineKeyboardButton("Ні", callback_data="skip")]])
        await app.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text="СИГНАЛ\n\n"+main_txt+"\n\nВведи суму USDC:",
            reply_markup=kb)

    wrong=[s for s in signal_history if s.get("outcome")=="LOSS"]
    if len(wrong)>0 and len(wrong)%5==0:
        await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID,
                                   text="%d помилок у лозі. /errors"%len(wrong))

# ============================================================
# SCHEDULER — :00 :15 :30 :45 UTC
# ============================================================

async def periodic(app):
    while True:
        now          = datetime.datetime.now(datetime.timezone.utc)
        mins_to_next = 15 - (now.minute % 15)
        if mins_to_next == 15:
            mins_to_next = 0
        next_run = now.replace(second=2, microsecond=0) + datetime.timedelta(minutes=mins_to_next)
        if next_run <= now:
            next_run += datetime.timedelta(minutes=15)
        wait = (next_run - now).total_seconds()
        logger.info("Наступний аналіз через %.0f сек о %s UTC", wait, next_run.strftime("%H:%M"))
        await asyncio.sleep(wait)
        await run_cycle(app)

# ============================================================
# MAIN
# ============================================================

def main():
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
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
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    async def on_startup(app):
        load_history()
        asyncio.create_task(periodic(app))
        await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=WELCOME)
    app.post_init = on_startup
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    main()
