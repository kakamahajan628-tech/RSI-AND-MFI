import os
import numpy as np
import ccxt.async_support as ccxt
import asyncio
import uvicorn
import sqlite3
import httpx
import logging
import threading
from fastapi import FastAPI, Request
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional, Tuple, List, Dict

# ================================================================
# LOGGING — no more silent pass
# ================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("SENTINEL")

# ================================================================
# VERSION
# ================================================================
BOT_VERSION = "V22.0"
BOT_NAME    = f"SENTINEL MATRIX ENGINE {BOT_VERSION}"

# ================================================================
# ENV VARS
# ================================================================
TELEGRAM_TOKEN  = os.environ.get("TELEGRAM_TOKEN") or os.environ.get("API_KEY")
RAW_CHAT_ID     = os.environ.get("CHAT_ID", "")
ALLOWED_CHAT_ID = (
    str(RAW_CHAT_ID).strip()
    if RAW_CHAT_ID and str(RAW_CHAT_ID).strip() not in ["None","none","","null","False"]
    else None
)
RENDER_URL    = "https://rsi-and-mfi.onrender.com"
LOOP_INTERVAL = 120
# Persistent path — works on Render disk & local
DB_PATH       = os.path.join(os.path.dirname(os.path.abspath(__file__)), "coins.db")
OHLCV_LIMIT   = 500   # larger window → RSI convergence accuracy

# ================================================================
# SQLITE PERSISTENCE  (not /tmp — survives redeploy)
# ================================================================
def db_init():
    con = sqlite3.connect(DB_PATH)
    con.execute("CREATE TABLE IF NOT EXISTS coins (symbol TEXT PRIMARY KEY)")
    defaults = ["BTC/USDT","ETH/USDT","SOL/USDT","BB/USDT","LAB/USDT","BEAT/USDT"]
    for s in defaults:
        con.execute("INSERT OR IGNORE INTO coins VALUES (?)", (s,))
    con.commit(); con.close()

def db_load() -> List[str]:
    con = sqlite3.connect(DB_PATH)
    rows = con.execute("SELECT symbol FROM coins").fetchall()
    con.close()
    return [r[0] for r in rows]

def db_add(symbol: str):
    con = sqlite3.connect(DB_PATH)
    con.execute("INSERT OR IGNORE INTO coins VALUES (?)", (symbol,))
    con.commit(); con.close()

def db_remove(symbol: str):
    con = sqlite3.connect(DB_PATH)
    con.execute("DELETE FROM coins WHERE symbol=?", (symbol,))
    con.commit(); con.close()

# ================================================================
# TELEGRAM — async httpx + queue (no blocking requests lib)
# ================================================================
_tg_queue: asyncio.Queue = None
_http: httpx.AsyncClient = None

async def tg_worker():
    """Drains send queue at 350ms cadence — safe against Telegram flood limits"""
    while True:
        chat_id, text = await _tg_queue.get()
        try:
            url     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            payload = {"chat_id": int(chat_id), "text": text, "parse_mode": "HTML"}
            r = await _http.post(url, json=payload, timeout=10)
            log.info(f"[TG] chat={chat_id} status={r.status_code}")
        except Exception as e:
            log.error(f"[TG ERROR] {e}")
        finally:
            _tg_queue.task_done()
        await asyncio.sleep(0.35)

def send_telegram_msg(chat_id: int, text: str):
    if _tg_queue:
        try:
            _tg_queue.put_nowait((chat_id, text))
        except asyncio.QueueFull:
            log.warning("[TG QUEUE FULL] Message dropped")

async def setup_webhook():
    if not TELEGRAM_TOKEN or not _http:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook?url={RENDER_URL}/tg-webhook"
    try:
        r   = await _http.get(url, timeout=10)
        log.info(f"[WEBHOOK] {r.json()}")
    except Exception as e:
        log.error(f"[WEBHOOK] {e}")

def esc(text: str) -> str:
    return text.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

# ================================================================
# SIGNAL COOLDOWN — suppress duplicate signals
# ================================================================
# key: symbol → (score, signal_label, timestamp)
_last_signal: Dict[str, Tuple[int, str, float]] = {}

def should_send_signal(symbol: str, score: int, sig_lbl: str) -> bool:
    """Send only if signal changed OR score shifted >=2 points"""
    now = datetime.now(timezone.utc).timestamp()
    prev = _last_signal.get(symbol)
    if prev is None:
        _last_signal[symbol] = (score, sig_lbl, now)
        return True
    prev_score, prev_lbl, _ = prev
    if sig_lbl != prev_lbl or abs(score - prev_score) >= 2:
        _last_signal[symbol] = (score, sig_lbl, now)
        return True
    return False

# ================================================================
# FASTAPI LIFESPAN
# ================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global engine_instance, _tg_queue, _http
    db_init()
    _tg_queue      = asyncio.Queue(maxsize=500)
    _http          = httpx.AsyncClient()
    engine_instance = CompleteSentinelEngine()

    await setup_webhook()

    tg_task   = asyncio.create_task(tg_worker())
    loop_task = asyncio.create_task(engine_instance.engine_core_loop())

    if ALLOWED_CHAT_ID:
        coins    = engine_instance.tracked_symbols
        boot_msg = (
            f"🚀 <b>{esc(BOT_NAME)} ONLINE</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Indicators : RSI · MFI · OBV · Volume\n"
            f"📈 Analysis   : EMA20/50/200 · Trend · BOS/CHoCH\n"
            f"              Divergence · Signal Score\n"
            f"🔕 Cooldown   : Duplicate signals suppressed\n"
            f"⏱  Timeframes : 1m · 5m · 15m · 1h · 4h\n"
            f"🔄 Auto-Loop  : Every {LOOP_INTERVAL}s\n"
            f"💾 DB         : Persistent (restart-safe)\n"
            f"🪙 Coins      : {len(coins)} loaded\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Use /help for commands."
        )
        send_telegram_msg(int(ALLOWED_CHAT_ID), boot_msg)

    yield

    loop_task.cancel(); tg_task.cancel()
    await engine_instance.close_clients()
    await _http.aclose()

app = FastAPI(lifespan=lifespan)
engine_instance = None

@app.get("/")
def health():
    return {"status":"ONLINE","engine":BOT_NAME,
            "timestamp":datetime.now(timezone.utc).isoformat()}

# ================================================================
# WEBHOOK HANDLER
# ================================================================
@app.post("/tg-webhook")
async def tg_webhook(request: Request):
    global engine_instance
    try:
        data = await request.json()
        if "message" not in data:
            return {"status":"ignored"}

        msg     = data["message"]
        chat_id = msg["chat"]["id"]
        text    = msg.get("text","").strip()

        if ALLOWED_CHAT_ID and str(chat_id) != ALLOWED_CHAT_ID:
            log.warning(f"[SECURITY] Blocked chat {chat_id}")
            return {"status":"unauthorized"}
        if not text.startswith("/"):
            return {"status":"not a command"}

        parts   = text.split()
        command = parts[0].lower()

        if command in ("/start","/help"):
            send_telegram_msg(chat_id, (
                f"📊 <b>{esc(BOT_NAME)}</b>\n\n"
                "Commands:\n"
                "🔹 <code>/add SOL</code> — Add coin\n"
                "🔹 <code>/remove SOL</code> — Remove coin\n"
                "🔹 <code>/list</code> — Show tracked coins\n"
                "🔹 <code>/scan</code> — Instant full scan\n\n"
                "<i>Tip: Just coin name — /USDT auto-added</i>"
            ))

        elif command == "/add":
            if len(parts) < 2:
                send_telegram_msg(chat_id, "⚠️ Usage: <code>/add SOL</code>")
            else:
                raw  = parts[1].upper().strip()
                coin = raw if "/" in raw else f"{raw}/USDT"
                engine_instance.add_coin(coin)
                send_telegram_msg(chat_id, f"✅ Added: <b>{esc(coin)}</b>")

        elif command == "/remove":
            if len(parts) < 2:
                send_telegram_msg(chat_id, "⚠️ Usage: <code>/remove SOL</code>")
            else:
                raw  = parts[1].upper().strip()
                coin = raw if "/" in raw else f"{raw}/USDT"
                engine_instance.remove_coin(coin)
                send_telegram_msg(chat_id, f"🗑️ Removed: <b>{esc(coin)}</b>")

        elif command == "/list":
            with engine_instance.symbol_lock:
                coins = engine_instance.tracked_symbols.copy()
            lines = "\n".join([f"  {i+1}. {c}" for i,c in enumerate(coins)])
            send_telegram_msg(chat_id,
                f"📋 <b>Tracked ({len(coins)}):</b>\n\n{lines or 'Empty'}")

        elif command == "/scan":
            send_telegram_msg(chat_id, "🔍 Scanning all coins… please wait.")
            asyncio.create_task(engine_instance.run_scan(chat_id, cooldown=False))

    except Exception as e:
        log.error(f"[WEBHOOK ERROR] {e}")
    return {"status":"ok"}


# ================================================================
# CORE ENGINE
# ================================================================
class CompleteSentinelEngine:

    TIMEFRAMES = ['1m','5m','15m','1h','4h']

    def __init__(self):
        self.symbol_lock     = threading.Lock()
        self.tracked_symbols = db_load()

        # Persistent CCXT clients — instantiated once, connection-pooled
        swap = {"options":{"defaultType":"swap"},"enableRateLimit":True}
        spot = {"options":{"defaultType":"spot"},"enableRateLimit":True}
        self.gate = {'future': ccxt.gateio(swap), 'spot': ccxt.gateio(spot)}
        self.okx  = {'future': ccxt.okx(swap),    'spot': ccxt.okx(spot)}

    async def close_clients(self):
        for c in list(self.gate.values()) + list(self.okx.values()):
            try: await c.close()
            except Exception as e: log.warning(f"[CLOSE] {e}")

    def add_coin(self, s: str):
        s = s.upper().strip()
        with self.symbol_lock:
            if s not in self.tracked_symbols:
                self.tracked_symbols.append(s); db_add(s)

    def remove_coin(self, s: str):
        s = s.upper().strip()
        with self.symbol_lock:
            if s in self.tracked_symbols:
                self.tracked_symbols.remove(s); db_remove(s)

    # ── RSI (Wilder — OHLCV_LIMIT=500 ensures convergence) ───
    def calc_rsi(self, closes: np.ndarray) -> float:
        c = np.asarray(closes, dtype=float)
        if len(c) < 15: return 50.0
        d  = np.diff(c)
        g  = np.where(d > 0, d, 0.0)
        ls = np.where(d < 0, -d, 0.0)
        ag = np.mean(g[:14]); al = np.mean(ls[:14])
        for i in range(14, len(g)):
            ag = (ag*13 + g[i]) / 14
            al = (al*13 + ls[i]) / 14
        if al == 0: return 100.0
        return round(100 - 100/(1 + ag/al), 2)

    # ── RSI series (built once, reused for divergence) ────────
    def build_rsi_series(self, closes: np.ndarray, lookback: int = 60) -> np.ndarray:
        """Build RSI for last `lookback` bars — O(n) single pass"""
        c = np.asarray(closes, dtype=float)
        if len(c) < 30: return np.full(lookback, 50.0)
        # seed on full history, then slide window
        d  = np.diff(c)
        g  = np.where(d > 0, d, 0.0)
        ls = np.where(d < 0, -d, 0.0)
        ag = np.mean(g[:14]); al = np.mean(ls[:14])
        rsi_vals = []
        for i in range(14, len(g)):
            ag = (ag*13 + g[i]) / 14
            al = (al*13 + ls[i]) / 14
            if i >= len(g) - lookback:
                rs  = ag/al if al else 1e9
                rsi_vals.append(round(100 - 100/(1+rs), 2))
        return np.array(rsi_vals) if rsi_vals else np.full(lookback, 50.0)

    # ── MFI (full rolling formula) ────────────────────────────
    def calc_mfi(self, h, l, c, v, period=14) -> float:
        h = np.asarray(h, dtype=float); l = np.asarray(l, dtype=float)
        c = np.asarray(c, dtype=float); v = np.asarray(v, dtype=float)
        if len(c) < period+1: return 50.0
        tp       = (h + l + c) / 3.0
        mf       = tp * v
        diff_tp  = np.diff(tp)
        pos      = np.sum(np.where(diff_tp > 0, mf[1:], 0.0)[-period:])
        neg      = np.sum(np.where(diff_tp < 0, mf[1:], 0.0)[-period:])
        if neg == 0: return 100.0
        return round(100 - 100/(1 + pos/neg), 2)

    # ── OBV + 5-bar trend direction ───────────────────────────
    def calc_obv(self, c: np.ndarray, v: np.ndarray) -> Tuple[float, str]:
        c = np.asarray(c, dtype=float); v = np.asarray(v, dtype=float)
        if len(c) < 2: return 0.0, "⚪"
        obv = np.zeros(len(c))
        for i in range(1, len(c)):
            obv[i] = obv[i-1] + (v[i] if c[i]>c[i-1] else -v[i] if c[i]<c[i-1] else 0)
        trend = "↑🟢" if obv[-1] > obv[-6] else "↓🔴"
        return obv[-1], trend

    # ── Volume ratio vs 20-bar avg ────────────────────────────
    def calc_vol_ratio(self, v: np.ndarray) -> Tuple[float, float]:
        v    = np.asarray(v, dtype=float)
        avg  = np.mean(v[-21:-1]) if len(v) >= 21 else np.mean(v[:-1])
        return v[-1], (v[-1]/avg if avg > 0 else 1.0)

    # ── EMA ───────────────────────────────────────────────────
    def calc_ema(self, c: np.ndarray, period: int) -> float:
        c = np.asarray(c, dtype=float)
        if len(c) < period: return float(c[-1])
        k = 2/(period+1); val = np.mean(c[:period])
        for x in c[period:]: val = x*k + val*(1-k)
        return round(val, 8)

    # ── EMA structure labels ──────────────────────────────────
    def ema_structure(self, c: np.ndarray, price: float) -> str:
        e20  = self.calc_ema(c, 20)
        e50  = self.calc_ema(c, 50)
        e200 = self.calc_ema(c, 200) if len(c) >= 200 else None
        def _fmt(v): return f"{v:,.5f}"
        line = f"EMA20:{_fmt(e20)}  EMA50:{_fmt(e50)}"
        if e200: line += f"  EMA200:{_fmt(e200)}"
        # Stack alignment: price vs EMAs
        above = []
        if price > e20:  above.append("20")
        if price > e50:  above.append("50")
        if e200 and price > e200: above.append("200")
        tag = f"  Price above EMA: {','.join(above) or 'none'}"
        return line + tag

    # ── Trend (EMA9/21 + 200 bias) ────────────────────────────
    def calc_trend(self, c: np.ndarray, price: float) -> Tuple[str, str]:
        c = np.asarray(c, dtype=float)
        if len(c) < 22: return "Neutral", "⚪"
        e9   = self.calc_ema(c, 9)
        e21  = self.calc_ema(c, 21)
        e200 = self.calc_ema(c, 200) if len(c) >= 200 else None
        slope = c[-1] - c[-6]

        if e9 > e21 and slope > 0:
            bias = (" | ✅ Above EMA200" if e200 and price > e200
                    else " | ⚠️ Below EMA200" if e200 else "")
            return f"Bullish{bias}", "🟢"
        elif e9 < e21 and slope < 0:
            bias = (" | ✅ Below EMA200" if e200 and price < e200
                    else " | ⚠️ Above EMA200" if e200 else "")
            return f"Bearish{bias}", "🔴"
        return "Neutral", "⚪"

    # ── Market Structure: BOS / CHoCH (close-confirmed) ───────
    def calc_market_structure(self, c: np.ndarray) -> Tuple[str, str]:
        c = np.asarray(c, dtype=float)
        if len(c) < 40: return "No Data", "⚪"

        seg = c[-60:] if len(c) >= 60 else c
        # Exclude last 2 bars — avoid unconfirmed live candle false signals
        seg_confirmed = seg[:-2]

        p_highs, p_lows = [], []
        for i in range(5, len(seg_confirmed)-5):
            if seg_confirmed[i] == np.max(seg_confirmed[i-5:i+6]):
                p_highs.append((i, seg_confirmed[i]))
            if seg_confirmed[i] == np.min(seg_confirmed[i-5:i+6]):
                p_lows.append((i, seg_confirmed[i]))

        if len(p_highs) < 3 or len(p_lows) < 3:
            return "Consolidation", "⚪"

        last_high = p_highs[-1][1]; prev_high = p_highs[-2][1]
        last_low  = p_lows[-1][1];  prev_low  = p_lows[-2][1]

        # Close confirmation: current close must break level
        price_close = c[-3]   # last confirmed close

        was_uptrend   = p_highs[-2][1] > p_highs[-3][1]
        was_downtrend = p_lows[-2][1]  < p_lows[-3][1]

        if price_close > last_high:
            return ("CHoCH 🔄 Bull", "🟢") if was_downtrend else ("BOS ↑ Bull", "🟢")
        elif price_close < last_low:
            return ("CHoCH 🔄 Bear", "🔴") if was_uptrend else ("BOS ↓ Bear", "🔴")
        return "Inside Range", "⚪"

    # ── Divergence (pivot-based, cached RSI series) ───────────
    def calc_divergence(self, closes: np.ndarray) -> Tuple[str, str]:
        c = np.asarray(closes, dtype=float)
        if len(c) < 60: return "No Data", "⚪"

        # Build RSI series once — no repeated recalculation
        rsi_arr = self.build_rsi_series(c, lookback=60)
        prc_arr = c[-len(rsi_arr):]
        if len(rsi_arr) < 10: return "No Data", "⚪"

        def pivots_low(arr):
            return [i for i in range(2, len(arr)-2) if arr[i] == np.min(arr[i-2:i+3])]
        def pivots_high(arr):
            return [i for i in range(2, len(arr)-2) if arr[i] == np.max(arr[i-2:i+3])]

        pl = pivots_low(prc_arr);  ph = pivots_high(prc_arr)
        rl = pivots_low(rsi_arr);  rh = pivots_high(rsi_arr)

        if len(pl) >= 2 and len(rl) >= 2:
            if prc_arr[pl[-1]] < prc_arr[pl[-2]] and rsi_arr[rl[-1]] > rsi_arr[rl[-2]]:
                return "Bullish Div 🔺", "🟢"
        if len(ph) >= 2 and len(rh) >= 2:
            if prc_arr[ph[-1]] > prc_arr[ph[-2]] and rsi_arr[rh[-1]] < rsi_arr[rh[-2]]:
                return "Bearish Div 🔻", "🔴"
        if len(pl) >= 2 and len(rl) >= 2:
            if prc_arr[pl[-1]] > prc_arr[pl[-2]] and rsi_arr[rl[-1]] < rsi_arr[rl[-2]]:
                return "Hidden Bull 🔺", "🟢"
        if len(ph) >= 2 and len(rh) >= 2:
            if prc_arr[ph[-1]] < prc_arr[ph[-2]] and rsi_arr[rh[-1]] > rsi_arr[rh[-2]]:
                return "Hidden Bear 🔻", "🔴"
        return "No Divergence", "⚪"

    # ── Signal Score — separated trend vs reversal tracks ─────
    def calc_signal_score(self, rsi, mfi, trend_icon, div_lbl,
                          struct_icon, obv_dir, vol_ratio
                          ) -> Tuple[int, str, str]:
        """
        Two separate tracks:
          trend_pts   — follows the current momentum direction
          reversal_pts — counts oversold/divergence for counter-move probability
        Net = trend - reversal for direction; magnitude = confidence.
        """
        bull = 0; bear = 0

        # Trend track (weighted higher)
        if trend_icon == "🟢":   bull += 3
        elif trend_icon == "🔴": bear += 3

        if struct_icon == "🟢":   bull += 3
        elif struct_icon == "🔴": bear += 3

        if "🟢" in obv_dir: bull += 1
        elif "🔴" in obv_dir: bear += 1

        # Reversal track (RSI/MFI extremes)
        if rsi <= 30:   bull += 2   # oversold → potential bull
        elif rsi >= 70: bear += 2
        if mfi <= 20:   bull += 2
        elif mfi >= 80: bear += 2

        # Mid-zone gentle push
        if 30 < rsi < 45: bull += 1
        elif 55 < rsi < 70: bear += 1

        # Divergence (strongest reversal signal)
        if "Bullish Div"  in div_lbl: bull += 3
        elif "Hidden Bull" in div_lbl: bull += 2
        elif "Bearish Div" in div_lbl: bear += 3
        elif "Hidden Bear"  in div_lbl: bear += 2

        # Volume spike bonus goes to dominant side
        if vol_ratio >= 2.0:
            bonus = min(2, int(vol_ratio//2))
            if bull >= bear: bull += bonus
            else:            bear += bonus

        total = bull + bear
        net   = bull - bear
        score = round((bull/total)*10) if total > 0 else 5

        if   net >= 7: return score, "STRONG BUY 🚀",   "🟢"
        elif net >= 3: return score, "BUY 📈",           "🟢"
        elif net <= -7: return score, "STRONG SELL 📉",  "🔴"
        elif net <= -3: return score, "SELL 🔻",         "🔴"
        else:           return score, "NEUTRAL ⚪",      "⚪"

    # ── Compact number formatter ──────────────────────────────
    def fmt(self, v: float, signed=False) -> str:
        neg = v < 0; a = abs(v)
        if a >= 1e9:   s = f"{a/1e9:.2f}B"
        elif a >= 1e6: s = f"{a/1e6:.2f}M"
        elif a >= 1e3: s = f"{a/1e3:.1f}K"
        else:          s = f"{a:.2f}"
        prefix = ("-" if neg else "+") if signed else ("-" if neg else "")
        return prefix + s

    def rsi_lbl(self, v):
        if v >= 70: return f"{v:.1f} 🔴OB"
        if v <= 30: return f"{v:.1f} 🟢OS"
        return          f"{v:.1f} ⚪MID"

    def mfi_lbl(self, v):
        if v >= 80: return f"{v:.1f} 🔴OB"
        if v <= 20: return f"{v:.1f} 🟢OS"
        return          f"{v:.1f} ⚪MID"

    def vol_lbl(self, vol, ratio):
        s = self.fmt(vol)
        if   ratio >= 3.0: return f"{s} x{ratio:.1f}🔥🔥"
        elif ratio >= 2.0: return f"{s} x{ratio:.1f}🔥"
        elif ratio >= 1.5: return f"{s} x{ratio:.1f}↑"
        return f"{s} x{ratio:.1f}"

    # ── Exchange router ───────────────────────────────────────
    async def fetch_exchange_data(self, symbol: str):
        attempts = [
            (self.gate['future'], f"{symbol}:USDT", "Gate (FUTURE)"),
            (self.okx['future'],  f"{symbol}:USDT", "OKX (FUTURE)"),
            (self.gate['spot'],   symbol,            "Gate (SPOT)"),
            (self.okx['spot'],    symbol,            "OKX (SPOT)"),
        ]
        for client, mkt, label in attempts:
            try:
                ticker = await client.fetch_ticker(mkt)
                if ticker and ticker.get('last'):
                    return float(ticker['last']), label, client, mkt
            except Exception as e:
                log.debug(f"[ROUTER] {label} {symbol}: {e}")
        log.warning(f"[ROUTER] No data found for {symbol}")
        return None, None, None, symbol

    # ── Per-symbol report ─────────────────────────────────────
    async def process_symbol(self, symbol: str) -> Optional[Tuple[str, int, str]]:
        """Returns (html_report, score, sig_lbl) or None"""
        price, source, client, mkt = await self.fetch_exchange_data(symbol)
        if not price or not client:
            return None

        D  = "─" * 54
        D2 = "═" * 54

        lines = [
            D2,
            f"  📊 {symbol}",
            f"  💵 Price  : ${price:,.6f}",
            f"  🏛  Source : {source}",
            D2,
            f"{'TF':<5}│{'RSI(14)':<12}│{'MFI(14)':<12}│{'OBV':<11}│VOL",
            D,
        ]

        tf_store: Dict = {}

        async def fetch_tf(tf):
            try:
                ohlcv = await client.fetch_ohlcv(mkt, tf, limit=OHLCV_LIMIT)
                return tf, ohlcv
            except Exception as e:
                log.warning(f"[OHLCV] {symbol} {tf}: {e}")
                return tf, None

        results = await asyncio.gather(*[fetch_tf(tf) for tf in self.TIMEFRAMES])

        for tf, ohlcv in results:
            if not ohlcv or len(ohlcv) < 20:
                lines.append(f"{tf:<5}│{'--':^12}│{'--':^12}│{'--':^11}│--")
                continue
            h = np.array([float(x[2]) for x in ohlcv])
            l = np.array([float(x[3]) for x in ohlcv])
            c = np.array([float(x[4]) for x in ohlcv])
            v = np.array([float(x[5]) for x in ohlcv])

            rsi          = self.calc_rsi(c)
            mfi          = self.calc_mfi(h, l, c, v)
            obv, obv_dir = self.calc_obv(c, v)
            vol, ratio   = self.calc_vol_ratio(v)

            tf_store[tf] = dict(h=h, l=l, c=c, v=v, rsi=rsi, mfi=mfi,
                                obv=obv, obv_dir=obv_dir, vol=vol, ratio=ratio)

            lines.append(
                f"{tf:<5}│{self.rsi_lbl(rsi):<12}│{self.mfi_lbl(mfi):<12}"
                f"│{self.fmt(obv,True)+obv_dir:<11}│{self.vol_lbl(vol,ratio)}"
            )

        lines.append(D)

        # Analysis on 1h → fallback 15m → 5m → 1m
        pri = next((tf_store[t] for t in ['1h','15m','5m','1m'] if t in tf_store), None)

        score = 5; sig_lbl = "NEUTRAL ⚪"; sig_icon = "⚪"

        if pri:
            c_p   = pri['c']
            rsi_p = pri['rsi']

            trend_lbl,  trend_icon  = self.calc_trend(c_p, price)
            struct_lbl, struct_icon = self.calc_market_structure(c_p)
            div_lbl,    div_icon    = self.calc_divergence(c_p)
            ema_line                = self.ema_structure(c_p, price)

            score, sig_lbl, sig_icon = self.calc_signal_score(
                rsi_p, pri['mfi'], trend_icon, div_lbl,
                struct_icon, pri['obv_dir'], pri['ratio']
            )

            lines += [
                "",
                f"  📐 EMA (1h basis)",
                f"  {ema_line}",
                D,
                f"  📈 TREND      : {trend_icon} {trend_lbl}",
                f"  🏗  STRUCTURE  : {struct_icon} {struct_lbl}",
                f"  🔀 DIVERGENCE : {div_icon} {div_lbl}",
                D,
                f"  ⚡ SIGNAL  {score}/10  →  {sig_icon} {sig_lbl}",
            ]
        else:
            lines.append("  ⚠️  Insufficient data")

        lines.append(D2)
        html = f"<pre>{esc(chr(10).join(lines))}</pre>"
        return html, score, sig_lbl

    # ── Scan runner — concurrent symbols ─────────────────────
    async def run_scan(self, chat_id: int, cooldown: bool = True):
        with self.symbol_lock:
            symbols = self.tracked_symbols.copy()

        async def process_and_send(sym):
            result = await self.process_symbol(sym)
            if not result:
                return
            html, score, sig_lbl = result
            # Cooldown gate: skip if signal unchanged (auto-loop only)
            if cooldown and not should_send_signal(sym, score, sig_lbl):
                log.info(f"[COOLDOWN] {sym} — same signal, skipped")
                return
            send_telegram_msg(chat_id, html)

        await asyncio.gather(*[process_and_send(s) for s in symbols])

    # ── 2-minute auto loop ────────────────────────────────────
    async def engine_core_loop(self):
        log.info(f"[ENGINE] {BOT_NAME} — auto-loop started")
        while True:
            if ALLOWED_CHAT_ID:
                ts = datetime.now(timezone.utc).strftime('%H:%M:%S UTC')
                log.info(f"[LOOP] Scan @ {ts}")
                try:
                    await self.run_scan(int(ALLOWED_CHAT_ID), cooldown=True)
                except Exception as e:
                    log.error(f"[LOOP ERROR] {e}")
            else:
                log.warning("[LOOP] CHAT_ID not set — skipping")

            log.info(f"[LOOP] Next scan in {LOOP_INTERVAL}s")
            await asyncio.sleep(LOOP_INTERVAL)


# ================================================================
# ENTRY POINT
# ================================================================
if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=10000,
                log_level="warning", reload=False)
