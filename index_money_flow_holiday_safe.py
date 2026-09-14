import os
import gzip
import json
import time
import threading
from copy import deepcopy
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
import psycopg
from dotenv import load_dotenv
import upstox_client

# ============================================================
# INDEX MONEY-FLOW + AGGRESSION ENGINE v1.0
# NIFTY + BANKNIFTY
# ============================================================
# - Separate Railway service
# - Fixed index universe: NIFTY and BANKNIFTY
# - 09:18 IST baseline by default
# - Money-flow ATM = strike with highest CE+PE traded value near spot
# - Freeze 3 OTM CE + 3 OTM PE around money-flow ATM
# - 3-minute snapshots
# - Futures OI, option OI, PCR, IV/Greeks, fresh option value
# - Tick-classified futures aggression and order-book imbalance
# - Separate Neon tables from stock Money Flow engine
# ============================================================

load_dotenv()
IST = ZoneInfo("Asia/Kolkata")

TOKEN = os.getenv("UPSTOX_TOKEN", "").strip()
DATABASE_URL = os.getenv("NEON_DATABASE_URL", "").strip()

if not TOKEN:
    raise RuntimeError("UPSTOX_TOKEN is missing")
if not DATABASE_URL:
    raise RuntimeError("NEON_DATABASE_URL is missing")

IS_RAILWAY = bool(
    os.getenv("RAILWAY_ENVIRONMENT")
    or os.getenv("RAILWAY_PROJECT_ID")
    or os.getenv("RAILWAY_SERVICE_ID")
)
BASE_DIR = Path(os.getenv("BASE_DIR", "/tmp/index_money_flow" if IS_RAILWAY else r"C:\upstox_dashboard\index_money_flow"))
BASE_DIR.mkdir(parents=True, exist_ok=True)

UPSTOX_NSE_MASTER_URL = os.getenv(
    "UPSTOX_NSE_MASTER_URL",
    "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz",
)
MASTER_CACHE = BASE_DIR / "NSE.json.gz"
MASTER_CACHE_MAX_AGE_HOURS = 12
FULL_QUOTE_URL = "https://api.upstox.com/v2/market-quote/quotes"

BASELINE_HOUR = int(os.getenv("INDEX_BASELINE_HOUR", "9"))
BASELINE_MINUTE = int(os.getenv("INDEX_BASELINE_MINUTE", "18"))
BASELINE_TIME = dtime(BASELINE_HOUR, BASELINE_MINUTE)
MARKET_END = dtime(15, 20)
SNAPSHOT_MINUTES = int(os.getenv("SNAPSHOT_MINUTES", "3"))

# NSE F&O holidays for 2026. Extra/future dates can be added with
# NSE_TRADING_HOLIDAYS=YYYY-MM-DD,YYYY-MM-DD,...
NSE_FO_HOLIDAYS_2026 = {
    "2026-01-26","2026-03-03","2026-03-26","2026-03-31","2026-04-03",
    "2026-04-14","2026-05-01","2026-05-28","2026-06-26","2026-09-14",
    "2026-10-02","2026-10-20","2026-11-10","2026-11-24","2026-12-25",
}

def configured_holidays():
    dates = set(NSE_FO_HOLIDAYS_2026)
    raw = os.getenv("NSE_TRADING_HOLIDAYS", "").strip()
    if raw:
        dates.update(x.strip() for x in raw.split(",") if x.strip())
    return dates

NSE_TRADING_HOLIDAYS = configured_holidays()

def is_nse_trading_day(day):
    return day.weekday() < 5 and day.isoformat() not in NSE_TRADING_HOLIDAYS

def holiday_reason(day):
    if day.weekday() >= 5:
        return "WEEKEND"
    if day.isoformat() in NSE_TRADING_HOLIDAYS:
        return "NSE F&O TRADING HOLIDAY"
    return None

def stop_if_non_trading_day():
    today = datetime.now(IST).date()
    reason = holiday_reason(today)
    if reason:
        log(f"{reason}: {today.isoformat()} | collector will not start.")
        raise SystemExit(0)


# Candidate strike radius for money-flow ATM search.
MONEY_FLOW_SEARCH_WINGS = int(os.getenv("INDEX_MONEY_FLOW_SEARCH_WINGS", "6"))
FROZEN_OTM_WINGS = int(os.getenv("INDEX_FROZEN_OTM_WINGS", "3"))
SUBSCRIPTION_WINGS = int(os.getenv("INDEX_SUBSCRIPTION_WINGS", "12"))

INDEX_CONFIG = {
    "NIFTY": {
        "spot_key": os.getenv("NIFTY_SPOT_KEY", "NSE_INDEX|Nifty 50"),
        "aliases": {"NIFTY", "NIFTY 50", "NIFTY50"},
    },
    "BANKNIFTY": {
        "spot_key": os.getenv("BANKNIFTY_SPOT_KEY", "NSE_INDEX|Nifty Bank"),
        "aliases": {"BANKNIFTY", "NIFTY BANK", "NIFTYBANK"},
    },
}

FUTURE_TYPES = {"FUT", "FUTIDX", "FUTSTK"}
OPTION_TYPES = {"CE", "PE", "OPTIDX", "OPTSTK"}

def log(msg):
    print(f"{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} IST | {msg}", flush=True)

def safe_num(v, default=0.0):
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default

def safe_int(v, default=0):
    try:
        if v is None:
            return default
        return int(float(v))
    except Exception:
        return default

def parse_expiry(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        x = float(v)
        if x > 10_000_000_000:
            x /= 1000.0
        return datetime.fromtimestamp(x, tz=IST).date()
    s = str(v).strip()
    if not s:
        return None
    for fn in (
        lambda x: datetime.fromisoformat(x.replace("Z", "+00:00")).date(),
        lambda x: datetime.strptime(x[:10], "%Y-%m-%d").date(),
    ):
        try:
            return fn(s)
        except Exception:
            pass
    return None

def instrument_type(r):
    return str(r.get("instrument_type") or r.get("instrumentType") or "").upper()

def trading_symbol(r):
    return str(r.get("trading_symbol") or r.get("tradingsymbol") or "").upper()

def underlying_symbol(r):
    return str(r.get("underlying_symbol") or r.get("underlyingSymbol") or "").upper()

def strike_price(r):
    return safe_num(r.get("strike_price", r.get("strike", 0)))

def option_side(r):
    side = str(r.get("option_type") or "").upper()
    if side in {"CE", "PE"}:
        return side
    it = instrument_type(r)
    if it in {"CE", "PE"}:
        return it
    ts = trading_symbol(r)
    if ts.endswith("CE"):
        return "CE"
    if ts.endswith("PE"):
        return "PE"
    return ""

def norm(s):
    return str(s or "").upper().replace(" ", "").replace("-", "").replace("_", "")

def row_matches_index(r, aliases):
    values = {
        underlying_symbol(r),
        trading_symbol(r),
        str(r.get("name", "")).upper(),
        str(r.get("short_name", "")).upper(),
    }
    na = {norm(x) for x in aliases}
    for v in values:
        nv = norm(v)
        if not nv:
            continue
        if nv in na or any(nv.startswith(a) for a in na):
            return True
    return False

def download_master():
    refresh = True
    if MASTER_CACHE.exists():
        refresh = (time.time() - MASTER_CACHE.stat().st_mtime) > MASTER_CACHE_MAX_AGE_HOURS * 3600
    if refresh:
        log("Downloading Upstox NSE instrument master...")
        r = requests.get(UPSTOX_NSE_MASTER_URL, timeout=30)
        r.raise_for_status()
        MASTER_CACHE.write_bytes(r.content)
    with gzip.open(MASTER_CACHE, "rt", encoding="utf-8") as f:
        rows = json.load(f)
    log(f"Instrument master loaded: {len(rows):,} rows")
    return rows

def chunks(items, size=500):
    items = list(items)
    for i in range(0, len(items), size):
        yield items[i:i+size]

def get_full_quotes(keys):
    keys = list(dict.fromkeys([k for k in keys if k]))
    headers = {"Accept": "application/json", "Authorization": f"Bearer {TOKEN}"}
    out = {}
    for batch in chunks(keys):
        r = requests.get(
            FULL_QUOTE_URL,
            params={"instrument_key": ",".join(batch)},
            headers=headers,
            timeout=30,
        )
        r.raise_for_status()
        for q in r.json().get("data", {}).values():
            token = q.get("instrument_token")
            if token:
                out[token] = q
        time.sleep(0.05)
    return out

def quote_value_cr(q, lot_size=1):
    if not q:
        return 0.0
    vol = safe_num(q.get("volume"))
    px = safe_num(q.get("average_price")) or safe_num(q.get("last_price"))
    return vol * px / 10_000_000

def nearest_strike(strikes, price):
    return min(strikes, key=lambda x: abs(x-price))

def discover_index_contracts(master, symbol, cfg):
    aliases = cfg["aliases"]
    today = datetime.now(IST).date()
    fo = [
        r for r in master
        if str(r.get("segment", "")).upper() == "NSE_FO"
        and row_matches_index(r, aliases)
        and parse_expiry(r.get("expiry")) is not None
        and parse_expiry(r.get("expiry")) >= today
    ]
    futures = [r for r in fo if instrument_type(r) in FUTURE_TYPES]
    if not futures:
        raise RuntimeError(f"{symbol}: no live index future found")
    fut_exp = min(parse_expiry(r.get("expiry")) for r in futures)
    fut = next(r for r in futures if parse_expiry(r.get("expiry")) == fut_exp)

    options = [r for r in fo if option_side(r) in {"CE","PE"} or instrument_type(r) in OPTION_TYPES]
    options = [r for r in options if option_side(r) in {"CE","PE"}]
    if not options:
        raise RuntimeError(f"{symbol}: no live index options found")
    opt_exp = min(parse_expiry(r.get("expiry")) for r in options)
    options = [r for r in options if parse_expiry(r.get("expiry")) == opt_exp]
    return fut, fut_exp, options, opt_exp

def select_subscription_options(option_rows, spot):
    strikes = sorted({strike_price(r) for r in option_rows if strike_price(r) > 0})
    atm = nearest_strike(strikes, spot)
    i = strikes.index(atm)
    lo = max(0, i-SUBSCRIPTION_WINGS)
    hi = min(len(strikes), i+SUBSCRIPTION_WINGS+1)
    chosen = set(strikes[lo:hi])
    return [r for r in option_rows if strike_price(r) in chosen and option_side(r) in {"CE","PE"}]

def choose_money_flow_atm(ctx, quotes, spot):
    strikes = sorted({m["strike"] for m in ctx["option_meta"].values()})
    spot_atm = nearest_strike(strikes, spot)
    i = strikes.index(spot_atm)
    lo = max(0, i-MONEY_FLOW_SEARCH_WINGS)
    hi = min(len(strikes), i+MONEY_FLOW_SEARCH_WINGS+1)
    candidates = strikes[lo:hi]

    scores = []
    for strike in candidates:
        total = 0.0
        ce = pe = 0.0
        for key, meta in ctx["option_meta"].items():
            if meta["strike"] != strike:
                continue
            v = quote_value_cr(quotes.get(key, {}), ctx["lot_size"])
            total += v
            if meta["type"] == "CE":
                ce += v
            elif meta["type"] == "PE":
                pe += v
        scores.append((total, strike, ce, pe))
    scores.sort(reverse=True)
    if not scores:
        return spot_atm, 0.0, 0.0, 0.0
    total, strike, ce, pe = scores[0]
    return strike, ce, pe, total

def build_context(master, symbol, cfg):
    spot_key = cfg["spot_key"]
    spot_q = get_full_quotes([spot_key]).get(spot_key)
    if not spot_q:
        raise RuntimeError(f"{symbol}: spot quote unavailable for {spot_key}")
    spot = safe_num(spot_q.get("last_price"))
    fut, fut_exp, options, opt_exp = discover_index_contracts(master, symbol, cfg)
    subs = select_subscription_options(options, spot)
    option_meta = {
        r["instrument_key"]: {
            "strike": strike_price(r),
            "type": option_side(r),
            "name": trading_symbol(r),
        } for r in subs
    }
    return {
        "symbol": symbol,
        "spot_key": spot_key,
        "future_key": fut["instrument_key"],
        "future_expiry": fut_exp,
        "option_expiry": opt_exp,
        "lot_size": safe_int(fut.get("lot_size"), 1),
        "option_meta": option_meta,
        "baseline_atm": None,
        "call_strikes": set(),
        "put_strikes": set(),
        "option_opening_ltp": {},
        "t0": None,
        "prev_snapshot": None,
        "last_snapshot_key": None,
        "baseline_call_value_cr": 0.0,
        "baseline_put_value_cr": 0.0,
        "baseline_option_value_cr": 0.0,
        "baseline_future_value_cr": 0.0,
    }

def freeze_baseline(ctx):
    keys = [ctx["spot_key"], ctx["future_key"], *ctx["option_meta"].keys()]
    quotes = get_full_quotes(keys)
    spot_q = quotes.get(ctx["spot_key"], {})
    fut_q = quotes.get(ctx["future_key"], {})
    spot = safe_num(spot_q.get("last_price"))
    if spot <= 0:
        raise RuntimeError(f"{ctx['symbol']}: invalid spot at baseline")

    atm, ce_val, pe_val, opt_val = choose_money_flow_atm(ctx, quotes, spot)
    strikes = sorted({m["strike"] for m in ctx["option_meta"].values()})
    i = strikes.index(atm)
    calls = strikes[i+1:i+1+FROZEN_OTM_WINGS]
    puts = list(reversed(strikes[max(0, i-FROZEN_OTM_WINGS):i]))
    if len(calls) < FROZEN_OTM_WINGS or len(puts) < FROZEN_OTM_WINGS:
        raise RuntimeError(f"{ctx['symbol']}: insufficient OTM strikes around money-flow ATM {atm}")

    ctx["baseline_atm"] = atm
    ctx["call_strikes"] = set(calls)
    ctx["put_strikes"] = set(puts)
    ctx["baseline_call_value_cr"] = ce_val
    ctx["baseline_put_value_cr"] = pe_val
    ctx["baseline_option_value_cr"] = opt_val
    ctx["baseline_future_value_cr"] = quote_value_cr(fut_q, ctx["lot_size"])

    for key, meta in ctx["option_meta"].items():
        if meta["strike"] in ctx["call_strikes"] | ctx["put_strikes"]:
            ltp = safe_num(quotes.get(key, {}).get("last_price"))
            if ltp > 0:
                ctx["option_opening_ltp"][key] = ltp

    log(
        f"{ctx['symbol']} BASELINE | spot={spot:.2f} money-flow ATM={atm:g} "
        f"| CE value={ce_val:.2f}Cr PE value={pe_val:.2f}Cr "
        f"| calls={sorted(calls)} puts={sorted(puts, reverse=True)}"
    )

def frozen_contracts(ctx):
    calls = sorted(ctx["call_strikes"])
    puts = sorted(ctx["put_strikes"], reverse=True)
    wanted = [("CE", i+1, s) for i,s in enumerate(calls)]
    wanted += [("PE", i+1, s) for i,s in enumerate(puts)]
    out = []
    for typ, wing, strike in wanted:
        for key, m in ctx["option_meta"].items():
            if m["type"] == typ and m["strike"] == strike:
                out.append((key,typ,wing,strike))
                break
    return out

def ensure_tables():
    sql = """
    CREATE TABLE IF NOT EXISTS public.index_money_flow_universe (
        trading_date DATE NOT NULL,
        freeze_ts TIMESTAMPTZ NOT NULL,
        symbol TEXT NOT NULL,
        spot_instrument_key TEXT,
        future_instrument_key TEXT,
        future_expiry DATE,
        option_expiry DATE,
        lot_size INTEGER,
        money_flow_atm NUMERIC,
        futures_value_cr NUMERIC,
        call_value_cr NUMERIC,
        put_value_cr NUMERIC,
        total_option_value_cr NUMERIC,
        total_money_flow_cr NUMERIC,
        PRIMARY KEY (trading_date, symbol)
    );

    CREATE TABLE IF NOT EXISTS public.index_engine_snapshots (
        id BIGSERIAL PRIMARY KEY,
        trading_date DATE NOT NULL,
        ts TIMESTAMPTZ NOT NULL,
        symbol TEXT NOT NULL,
        money_flow_atm NUMERIC,
        spot NUMERIC,
        spot_change_pct_t0 NUMERIC,
        future NUMERIC,
        future_change_pct_t0 NUMERIC,
        future_basis NUMERIC,
        future_oi BIGINT,
        future_oi_change_t0 BIGINT,
        future_oi_change_pct_t0 NUMERIC,
        future_oi_change_3m BIGINT,
        call_oi BIGINT,
        put_oi BIGINT,
        call_oi_change_t0 BIGINT,
        put_oi_change_t0 BIGINT,
        call_oi_change_3m BIGINT,
        put_oi_change_3m BIGINT,
        pcr NUMERIC,
        call_iv NUMERIC,
        put_iv NUMERIC,
        call_fresh_value_cr NUMERIC,
        put_fresh_value_cr NUMERIC,
        oi_50pct_state TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (trading_date, ts, symbol)
    );

    CREATE TABLE IF NOT EXISTS public.index_option_snapshots (
        id BIGSERIAL PRIMARY KEY,
        trading_date DATE NOT NULL,
        ts TIMESTAMPTZ NOT NULL,
        symbol TEXT NOT NULL,
        money_flow_atm NUMERIC,
        expiry DATE,
        instrument_key TEXT,
        option_type TEXT NOT NULL,
        wing_no INTEGER NOT NULL,
        strike NUMERIC NOT NULL,
        ltp NUMERIC,
        opening_ltp NUMERIC,
        price_multiple NUMERIC,
        doubled BOOLEAN,
        oi BIGINT,
        oi_change_3m BIGINT,
        iv NUMERIC,
        delta NUMERIC,
        gamma NUMERIC,
        theta NUMERIC,
        vega NUMERIC,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (trading_date, ts, symbol, option_type, wing_no)
    );

    CREATE TABLE IF NOT EXISTS public.index_futures_aggression_snapshots (
        id BIGSERIAL PRIMARY KEY,
        trading_date DATE NOT NULL,
        ts TIMESTAMPTZ NOT NULL,
        symbol TEXT NOT NULL,
        future_instrument_key TEXT NOT NULL,
        ltp NUMERIC,
        last_trade_time TIMESTAMPTZ,
        last_trade_qty BIGINT,
        volume_traded BIGINT,
        open_interest BIGINT,
        best_bid_price NUMERIC,
        best_bid_qty BIGINT,
        best_ask_price NUMERIC,
        best_ask_qty BIGINT,
        depth_bid_qty BIGINT,
        depth_ask_qty BIGINT,
        book_imbalance NUMERIC,
        total_buy_qty BIGINT,
        total_sell_qty BIGINT,
        total_qty_imbalance NUMERIC,
        aggressive_buy_qty BIGINT NOT NULL DEFAULT 0,
        aggressive_sell_qty BIGINT NOT NULL DEFAULT 0,
        unclassified_trade_qty BIGINT NOT NULL DEFAULT 0,
        trade_delta BIGINT NOT NULL DEFAULT 0,
        delta_pct NUMERIC,
        price_change_3m_pct NUMERIC,
        oi_change_3m BIGINT,
        oi_change_3m_pct NUMERIC,
        aggression_state TEXT,
        tick_count INTEGER NOT NULL DEFAULT 0,
        classified_trade_count INTEGER NOT NULL DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (trading_date, ts, symbol)
    );

    CREATE INDEX IF NOT EXISTS idx_index_engine_date_symbol_ts
      ON public.index_engine_snapshots(trading_date, symbol, ts);
    CREATE INDEX IF NOT EXISTS idx_index_options_date_symbol_ts
      ON public.index_option_snapshots(trading_date, symbol, ts);
    CREATE INDEX IF NOT EXISTS idx_index_aggression_date_symbol_ts
      ON public.index_futures_aggression_snapshots(trading_date, symbol, ts);

    -- Backward-compatible migration for databases created by an older version.
    ALTER TABLE public.index_engine_snapshots
      ADD COLUMN IF NOT EXISTS spot_change_pct_t0 NUMERIC;
    ALTER TABLE public.index_engine_snapshots
      ADD COLUMN IF NOT EXISTS future_change_pct_t0 NUMERIC;
    """
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    log("Index tables ready.")

def obj_to_dict(obj):
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    out = {}
    for name in dir(obj):
        if name.startswith("_"):
            continue
        try:
            v = getattr(obj, name)
        except Exception:
            continue
        if not callable(v):
            out[name] = v
    return out

def pick(d, *names, default=None):
    if not isinstance(d, dict):
        d = obj_to_dict(d)
    for n in names:
        if n in d and d[n] is not None:
            return d[n]
    return default

def ms_to_dt(v):
    try:
        if not v:
            return None
        return datetime.fromtimestamp(int(v)/1000.0, tz=ZoneInfo("UTC"))
    except Exception:
        return None

def extract_market(feed):
    d = obj_to_dict(feed)
    ff = obj_to_dict(pick(d, "fullFeed","ff","full_feed", default={}))
    market = pick(ff, "marketFF","market_ff","marketFf","indexFF","index_ff")
    if market is None:
        market = pick(d, "marketFF","indexFF","market_ff","index_ff", default={})
    return obj_to_dict(market)

def extract_row(feed):
    m = extract_market(feed)
    if not m:
        return None
    ltpc = obj_to_dict(pick(m, "ltpc", default={}))
    greeks = obj_to_dict(pick(m, "optionGreeks","option_greeks", default={}))
    return {
        "ltp": safe_num(pick(ltpc,"ltp")),
        "ltt": pick(ltpc,"ltt"),
        "cp": safe_num(pick(ltpc,"cp")),
        "ltq": safe_int(pick(ltpc,"ltq")),
        "oi": safe_int(pick(m,"oi")),
        "volume": safe_int(pick(m,"vtt")),
        "iv": safe_num(pick(m,"iv"), None),
        "delta": safe_num(pick(greeks,"delta"), None),
        "gamma": safe_num(pick(greeks,"gamma"), None),
        "theta": safe_num(pick(greeks,"theta"), None),
        "vega": safe_num(pick(greeks,"vega"), None),
    }

def extract_aggression_tick(feed):
    m = extract_market(feed)
    if not m:
        return None
    ltpc = obj_to_dict(pick(m,"ltpc",default={}))
    level = obj_to_dict(pick(m,"marketLevel","market_level",default={}))
    quotes = list(pick(level,"bidAskQuote","bid_ask_quote",default=[]) or [])
    depth_bid = depth_ask = 0
    best_bid_p = best_ask_p = None
    best_bid_q = best_ask_q = 0
    for i,q in enumerate(quotes[:5]):
        qd = obj_to_dict(q)
        bp = safe_num(pick(qd,"bidP","bp"), None)
        ap = safe_num(pick(qd,"askP","ap"), None)
        bq = safe_int(pick(qd,"bidQ","bq"))
        aq = safe_int(pick(qd,"askQ","aq"))
        depth_bid += bq
        depth_ask += aq
        if i == 0:
            best_bid_p,best_ask_p = bp,ap
            best_bid_q,best_ask_q = bq,aq
    ltt = pick(ltpc,"ltt")
    return {
        "ltp": safe_num(pick(ltpc,"ltp")),
        "ltt": ms_to_dt(ltt),
        "ltt_raw": safe_int(ltt),
        "ltq": safe_int(pick(ltpc,"ltq")),
        "vtt": safe_int(pick(m,"vtt")),
        "oi": safe_int(pick(m,"oi")),
        "best_bid_price": best_bid_p,
        "best_bid_qty": best_bid_q,
        "best_ask_price": best_ask_p,
        "best_ask_qty": best_ask_q,
        "depth_bid_qty": depth_bid,
        "depth_ask_qty": depth_ask,
        "tbq": safe_int(pick(m,"tbq")),
        "tsq": safe_int(pick(m,"tsq")),
    }

def total_option_oi(ctx, snap, typ):
    allowed = ctx["call_strikes"] if typ == "CE" else ctx["put_strikes"]
    total = 0
    for k,m in ctx["option_meta"].items():
        if m["type"] == typ and m["strike"] in allowed:
            total += safe_int((snap.get(k) or {}).get("oi"))
    return total

def average_metric(ctx, snap, typ, field):
    allowed = ctx["call_strikes"] if typ == "CE" else ctx["put_strikes"]
    vals = []
    for k,m in ctx["option_meta"].items():
        if m["type"] == typ and m["strike"] in allowed:
            v = (snap.get(k) or {}).get(field)
            if v is not None:
                vals.append(float(v))
    return sum(vals)/len(vals) if vals else None

def fresh_value(ctx, snap, prev, typ):
    allowed = ctx["call_strikes"] if typ == "CE" else ctx["put_strikes"]
    total = 0.0
    for k,m in ctx["option_meta"].items():
        if m["type"] != typ or m["strike"] not in allowed:
            continue
        now = snap.get(k) or {}
        old = prev.get(k) or {}
        doi = safe_num(now.get("oi")) - safe_num(old.get("oi"))
        if doi > 0:
            total += doi * safe_num(now.get("ltp")) / 10_000_000
    return total

def oi_50pct_state(call_change, put_change):
    # User rule: one side <= 50% of the other side, using summed CE/PE OI change.
    if call_change > 0 and put_change > 0:
        if put_change <= 0.5 * call_change:
            return "CALL OI DOMINANT (PUT <=50%)"
        if call_change <= 0.5 * put_change:
            return "PUT OI DOMINANT (CALL <=50%)"
        return "BALANCED"
    if call_change > 0 and put_change <= 0:
        return "CALL BUILD / PUT UNWIND"
    if put_change > 0 and call_change <= 0:
        return "PUT BUILD / CALL UNWIND"
    if call_change < 0 and put_change < 0:
        return "BOTH UNWINDING"
    return "MIXED"

def save_universe(ctxs, ts):
    sql = """
    INSERT INTO public.index_money_flow_universe(
      trading_date,freeze_ts,symbol,spot_instrument_key,future_instrument_key,
      future_expiry,option_expiry,lot_size,money_flow_atm,
      futures_value_cr,call_value_cr,put_value_cr,total_option_value_cr,total_money_flow_cr
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT(trading_date,symbol) DO UPDATE SET
      freeze_ts=EXCLUDED.freeze_ts,money_flow_atm=EXCLUDED.money_flow_atm,
      futures_value_cr=EXCLUDED.futures_value_cr,call_value_cr=EXCLUDED.call_value_cr,
      put_value_cr=EXCLUDED.put_value_cr,total_option_value_cr=EXCLUDED.total_option_value_cr,
      total_money_flow_cr=EXCLUDED.total_money_flow_cr;
    """
    rows=[]
    for c in ctxs.values():
        rows.append((ts.date(),ts,c["symbol"],c["spot_key"],c["future_key"],c["future_expiry"],
                     c["option_expiry"],c["lot_size"],c["baseline_atm"],c["baseline_future_value_cr"],
                     c["baseline_call_value_cr"],c["baseline_put_value_cr"],c["baseline_option_value_cr"],
                     c["baseline_future_value_cr"]+c["baseline_option_value_cr"]))
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.executemany(sql,rows)
        conn.commit()

def save_engine_snapshot(ctx, ts, snap):
    spotrow, futrow = snap.get(ctx["spot_key"]), snap.get(ctx["future_key"])
    if not spotrow or not futrow:
        return
    if ctx["t0"] is None:
        ctx["t0"] = deepcopy(snap)
    t0, prev = ctx["t0"], ctx["prev_snapshot"]

    spot = safe_num(spotrow.get("ltp"))
    fut = safe_num(futrow.get("ltp"))
    t0spot = safe_num((t0.get(ctx["spot_key"]) or {}).get("ltp"))
    t0fut = safe_num((t0.get(ctx["future_key"]) or {}).get("ltp"))
    spot_pct_t0 = (spot / t0spot - 1) * 100 if t0spot else 0
    fut_pct_t0 = (fut / t0fut - 1) * 100 if t0fut else 0
    foi = safe_int(futrow.get("oi"))
    t0foi = safe_int((t0.get(ctx["future_key"]) or {}).get("oi"))
    doi_t0 = foi-t0foi
    doi_pct_t0 = doi_t0/t0foi*100 if t0foi else 0
    doi3 = foi-safe_int((prev.get(ctx["future_key"]) or {}).get("oi")) if prev else 0

    coi = total_option_oi(ctx,snap,"CE")
    poi = total_option_oi(ctx,snap,"PE")
    t0coi = total_option_oi(ctx,t0,"CE")
    t0poi = total_option_oi(ctx,t0,"PE")
    cchg = coi-t0coi
    pchg = poi-t0poi
    c3 = coi-total_option_oi(ctx,prev,"CE") if prev else 0
    p3 = poi-total_option_oi(ctx,prev,"PE") if prev else 0
    pcr = poi/coi if coi else None
    civ = average_metric(ctx,snap,"CE","iv")
    piv = average_metric(ctx,snap,"PE","iv")
    cfresh = fresh_value(ctx,snap,prev,"CE") if prev else 0
    pfresh = fresh_value(ctx,snap,prev,"PE") if prev else 0
    state50 = oi_50pct_state(cchg,pchg)

    sql = """
    INSERT INTO public.index_engine_snapshots(
      trading_date,ts,symbol,money_flow_atm,spot,spot_change_pct_t0,
      future,future_change_pct_t0,future_basis,future_oi,
      future_oi_change_t0,future_oi_change_pct_t0,future_oi_change_3m,
      call_oi,put_oi,call_oi_change_t0,put_oi_change_t0,call_oi_change_3m,put_oi_change_3m,
      pcr,call_iv,put_iv,call_fresh_value_cr,put_fresh_value_cr,oi_50pct_state
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT(trading_date,ts,symbol) DO UPDATE SET
      spot=EXCLUDED.spot,spot_change_pct_t0=EXCLUDED.spot_change_pct_t0,
      future=EXCLUDED.future,future_change_pct_t0=EXCLUDED.future_change_pct_t0,
      future_oi=EXCLUDED.future_oi,
      future_oi_change_t0=EXCLUDED.future_oi_change_t0,
      future_oi_change_pct_t0=EXCLUDED.future_oi_change_pct_t0,
      future_oi_change_3m=EXCLUDED.future_oi_change_3m,
      call_oi_change_t0=EXCLUDED.call_oi_change_t0,put_oi_change_t0=EXCLUDED.put_oi_change_t0,
      call_oi_change_3m=EXCLUDED.call_oi_change_3m,put_oi_change_3m=EXCLUDED.put_oi_change_3m,
      pcr=EXCLUDED.pcr,call_iv=EXCLUDED.call_iv,put_iv=EXCLUDED.put_iv,
      call_fresh_value_cr=EXCLUDED.call_fresh_value_cr,put_fresh_value_cr=EXCLUDED.put_fresh_value_cr,
      oi_50pct_state=EXCLUDED.oi_50pct_state;
    """
    vals=(ts.date(),ts,ctx["symbol"],ctx["baseline_atm"],spot,spot_pct_t0,
          fut,fut_pct_t0,fut-spot,foi,doi_t0,doi_pct_t0,doi3,
          coi,poi,cchg,pchg,c3,p3,pcr,civ,piv,cfresh,pfresh,state50)
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(sql,vals)
        conn.commit()

    # Six frozen option rows
    rows=[]
    for key,typ,wing,strike in frozen_contracts(ctx):
        now=snap.get(key)
        if not now:
            continue
        ltp=safe_num(now.get("ltp"))
        if ltp>0 and key not in ctx["option_opening_ltp"]:
            ctx["option_opening_ltp"][key]=ltp
        opening=ctx["option_opening_ltp"].get(key)
        multiple=ltp/opening if opening else None
        old=(prev.get(key) if prev else None) or {}
        oichg=safe_int(now.get("oi"))-safe_int(old.get("oi"),safe_int(now.get("oi")))
        rows.append((ts.date(),ts,ctx["symbol"],ctx["baseline_atm"],ctx["option_expiry"],key,typ,wing,
                     strike,ltp,opening,multiple,bool(multiple is not None and multiple>=2),
                     safe_int(now.get("oi")),oichg,now.get("iv"),now.get("delta"),now.get("gamma"),
                     now.get("theta"),now.get("vega")))
    if rows:
        osql = """
        INSERT INTO public.index_option_snapshots(
          trading_date,ts,symbol,money_flow_atm,expiry,instrument_key,option_type,wing_no,strike,
          ltp,opening_ltp,price_multiple,doubled,oi,oi_change_3m,iv,delta,gamma,theta,vega
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(trading_date,ts,symbol,option_type,wing_no) DO UPDATE SET
          ltp=EXCLUDED.ltp,price_multiple=EXCLUDED.price_multiple,doubled=EXCLUDED.doubled,
          oi=EXCLUDED.oi,oi_change_3m=EXCLUDED.oi_change_3m,iv=EXCLUDED.iv,
          delta=EXCLUDED.delta,gamma=EXCLUDED.gamma,theta=EXCLUDED.theta,vega=EXCLUDED.vega;
        """
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.executemany(osql,rows)
            conn.commit()

    log(
        f"{ctx['symbol']} | spot={spot:.2f} ({spot_pct_t0:+.2f}% T0) "
        f"fut={fut:.2f} ({fut_pct_t0:+.2f}% T0) OI T0={doi_pct_t0:+.2f}% "
        f"| CEΔ={cchg:+,} PEΔ={pchg:+,} | {state50}"
    )
    ctx["prev_snapshot"] = deepcopy(snap)

class Aggression:
    def __init__(self, contexts):
        self.meta={c["future_key"]:c for c in contexts.values()}
        self.last={}
        self.bucket={}
        self.prev_flush={}
        self.lock=threading.Lock()

    def reset(self,key):
        self.bucket[key]={"buy":0,"sell":0,"unc":0,"ticks":0,"classified":0,"latest":None}

    def on_feed(self,key,feed):
        if key not in self.meta:
            return
        t=extract_aggression_tick(feed)
        if not t:
            return
        with self.lock:
            if key not in self.bucket:
                self.reset(key)
            b=self.bucket[key]
            prev=self.last.get(key)
            b["ticks"]+=1
            newtrade=t["ltq"]>0 and t["ltt_raw"]>0 and (prev is None or t["ltt_raw"]!=prev.get("ltt_raw"))
            if newtrade:
                ref=prev or t
                bid,ask=ref.get("best_bid_price"),ref.get("best_ask_price")
                side=None
                if ask is not None and t["ltp"]>=ask:
                    side="BUY"
                elif bid is not None and t["ltp"]<=bid:
                    side="SELL"
                elif bid is not None and ask is not None:
                    mid=(bid+ask)/2
                    if t["ltp"]>mid: side="BUY"
                    elif t["ltp"]<mid: side="SELL"
                if side=="BUY":
                    b["buy"]+=t["ltq"]; b["classified"]+=1
                elif side=="SELL":
                    b["sell"]+=t["ltq"]; b["classified"]+=1
                else:
                    b["unc"]+=t["ltq"]
            b["latest"]=t
            self.last[key]=t

    def flush(self, ts):
        rows=[]
        with self.lock:
            for key,c in self.meta.items():
                b=self.bucket.get(key)
                if not b or not b["latest"]:
                    continue
                t=b["latest"]; prev=self.prev_flush.get(key)
                buy,sell=b["buy"],b["sell"]
                delta=buy-sell
                classified=buy+sell
                delta_pct=delta/classified*100 if classified else None
                depth_total=t["depth_bid_qty"]+t["depth_ask_qty"]
                book=(t["depth_bid_qty"]-t["depth_ask_qty"])/depth_total*100 if depth_total else None
                total=t["tbq"]+t["tsq"]
                totalimb=(t["tbq"]-t["tsq"])/total*100 if total else None
                pchg=oichg=oipct=None
                if prev:
                    pchg=(t["ltp"]/prev["ltp"]-1)*100 if prev["ltp"] else None
                    oichg=t["oi"]-prev["oi"]
                    oipct=oichg/prev["oi"]*100 if prev["oi"] else None
                state="NEUTRAL"
                if pchg is not None and oipct is not None:
                    if pchg<0 and oipct>0: state="NEW SHORT BUILD"
                    elif pchg>0 and oipct>0: state="NEW LONG BUILD"
                    elif pchg>0 and oipct<0: state="SHORT COVERING"
                    elif pchg<0 and oipct<0: state="LONG UNWINDING"
                rows.append((ts.date(),ts,c["symbol"],key,t["ltp"],t["ltt"],t["ltq"],t["vtt"],t["oi"],
                             t["best_bid_price"],t["best_bid_qty"],t["best_ask_price"],t["best_ask_qty"],
                             t["depth_bid_qty"],t["depth_ask_qty"],book,t["tbq"],t["tsq"],totalimb,
                             buy,sell,b["unc"],delta,delta_pct,pchg,oichg,oipct,state,b["ticks"],b["classified"]))
                self.prev_flush[key]=dict(t)
                self.reset(key)
        if not rows:
            return
        sql="""
        INSERT INTO public.index_futures_aggression_snapshots(
          trading_date,ts,symbol,future_instrument_key,ltp,last_trade_time,last_trade_qty,volume_traded,
          open_interest,best_bid_price,best_bid_qty,best_ask_price,best_ask_qty,depth_bid_qty,depth_ask_qty,
          book_imbalance,total_buy_qty,total_sell_qty,total_qty_imbalance,aggressive_buy_qty,
          aggressive_sell_qty,unclassified_trade_qty,trade_delta,delta_pct,price_change_3m_pct,
          oi_change_3m,oi_change_3m_pct,aggression_state,tick_count,classified_trade_count
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(trading_date,ts,symbol) DO UPDATE SET
          ltp=EXCLUDED.ltp,open_interest=EXCLUDED.open_interest,total_qty_imbalance=EXCLUDED.total_qty_imbalance,
          aggressive_buy_qty=EXCLUDED.aggressive_buy_qty,aggressive_sell_qty=EXCLUDED.aggressive_sell_qty,
          trade_delta=EXCLUDED.trade_delta,delta_pct=EXCLUDED.delta_pct,price_change_3m_pct=EXCLUDED.price_change_3m_pct,
          oi_change_3m=EXCLUDED.oi_change_3m,oi_change_3m_pct=EXCLUDED.oi_change_3m_pct,
          aggression_state=EXCLUDED.aggression_state,tick_count=EXCLUDED.tick_count,
          classified_trade_count=EXCLUDED.classified_trade_count;
        """
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.executemany(sql,rows)
            conn.commit()

def wait_until_baseline():
    while True:
        now = datetime.now(IST)

        if not is_nse_trading_day(now.date()):
            log(f"{holiday_reason(now.date())}: {now.date().isoformat()} | collector will not run.")
            if IS_RAILWAY:
                raise SystemExit(0)
            time.sleep(300)
            continue

        if now.time() < BASELINE_TIME:
            time.sleep(min(30, max(1, (datetime.combine(now.date(), BASELINE_TIME, tzinfo=IST) - now).total_seconds())))
            continue

        if now.time() > MARKET_END:
            log("Session closed for today.")
            if IS_RAILWAY:
                raise SystemExit(0)
            time.sleep(300)
            continue

        return now

def main():
    stop_if_non_trading_day()
    ensure_tables()
    master=download_master()
    baseline_ts=wait_until_baseline().replace(second=0,microsecond=0)

    contexts={}
    for symbol,cfg in INDEX_CONFIG.items():
        try:
            c=build_context(master,symbol,cfg)
            freeze_baseline(c)
            contexts[symbol]=c
        except Exception as e:
            log(f"{symbol} setup failed: {e}")
    if not contexts:
        raise RuntimeError("No index contexts could be built")

    save_universe(contexts, baseline_ts)

    names={}
    instrument_keys=[]
    for c in contexts.values():
        for key in [c["spot_key"],c["future_key"],*c["option_meta"].keys()]:
            instrument_keys.append(key)
            names[key]=key
    instrument_keys=list(dict.fromkeys(instrument_keys))

    latest={}
    lock=threading.Lock()
    aggression=Aggression(contexts)

    config=upstox_client.Configuration()
    config.access_token=TOKEN
    api_client=upstox_client.ApiClient(config)

    def on_message(message):
        msg=obj_to_dict(message)
        feeds=obj_to_dict(pick(msg,"feeds",default={}) or {})
        for key,feed in feeds.items():
            row=extract_row(feed)
            if row:
                with lock:
                    latest[key]=row
            aggression.on_feed(key,feed)

    streamer=upstox_client.MarketDataStreamerV3(api_client,instrument_keys,"full")
    streamer.on("message",on_message)
    streamer.on("open",lambda: log(f"Connected. {len(instrument_keys)} instruments subscribed."))
    streamer.on("error",lambda e: log(f"STREAM ERROR: {e}"))
    streamer.on("close",lambda: log("Stream closed."))

    def scheduler():
        while True:
            now=datetime.now(IST)
            if not is_nse_trading_day(now.date()):
                log(f"{holiday_reason(now.date())}: stopping collector.")
                if IS_RAILWAY:
                    os._exit(0)
                return
            if now.time()>MARKET_END:
                log("Index collection complete.")
                try:
                    aggression.flush(now.replace(second=0,microsecond=0))
                except Exception as e:
                    log(f"Final aggression flush error: {e}")
                if IS_RAILWAY:
                    os._exit(0)
                return
            if now.time()>=BASELINE_TIME and now.minute % SNAPSHOT_MINUTES==0 and 2<=now.second<=8:
                ts=now.replace(microsecond=0)
                snapkey=ts.strftime("%Y-%m-%d %H:%M")
                with lock:
                    all_data=deepcopy(latest)
                for sym,c in contexts.items():
                    if c["last_snapshot_key"]==snapkey:
                        continue
                    keys={c["spot_key"],c["future_key"],*c["option_meta"].keys()}
                    snap={k:v for k,v in all_data.items() if k in keys}
                    try:
                        save_engine_snapshot(c,ts,snap)
                    except Exception as e:
                        log(f"{sym} snapshot error: {e}")
                    c["last_snapshot_key"]=snapkey
                try:
                    aggression.flush(ts)
                except Exception as e:
                    log(f"Aggression flush error: {e}")
                time.sleep(2)
            time.sleep(0.4)

    threading.Thread(target=scheduler,daemon=True).start()
    log("Connecting to Upstox...")
    streamer.connect()

if __name__=="__main__":
    main()