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
# LOGGING
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
BOT_VERSION = "V24.1-STABLE"
BOT_NAME    = f"SENTINEL MATRIX ENGINE {BOT_VERSION}"

# ================================================================
# ENV VARS FUNCTIONAL FIX
# ================================================================
try:
    from dotenv import load_dotenv
    load_dotenv()
    log.info("[ENV] .env file loaded via python-dotenv")
except ImportError:
    log.info("[ENV] python-dotenv not installed — using system env vars only")

def clean_env_var(key: str) -> Optional[str]:
    val = os.environ.get(key, "").strip().strip("'").strip('"')
    if val in ["None", "none", "", "null", "False", "false"]:
        return None
    return val

TELEGRAM_TOKEN = clean_env_var("TELEGRAM_TOKEN") or clean_env_var("API_KEY")
RAW_CHAT_ID    = clean_env_var("CHAT_ID")

def parse_chat_id(raw_id: Optional[str]) -> Optional[int]:
    if not raw_id:
        return None
    try:
        cleaned = "".join([c for c in raw_id if c.isdigit() or c == '-'])
        return int(cleaned)
    except ValueError:
        return None

ALLOWED_CHAT_ID = parse_chat_id(RAW_CHAT_ID)

RENDER_URL    = clean_env_var("RENDER_URL") or "https://rsi-and-mfi.onrender.com"
LOOP_INTERVAL = int(os.environ.get("LOOP_INTERVAL", "120").strip())
DB_PATH       = os.path.join(os.path.dirname(os.path.abspath(__file__)), "coins.db")
OHLCV_LIMIT   = 500

# FIX #3: Render (and most cloud hosts) assign a dynamic $PORT for health
# checks. Hardcoding a port causes the platform's health check to fail,
# which leads to periodic restarts/kills. We read $PORT with a safe fallback.
PORT = int(os.environ.get("PORT", "10000"))

# FIX #4: cap concurrent exchange calls so we don't get rate-limited (429)
# when the tracked coin list is large. Also reduces odds of IP bans.
MAX_CONCURRENT_SCANS = int(os.environ.get("MAX_CONCURRENT_SCANS", "5"))

log.info(f"[ENV MATRIX] TELEGRAM_TOKEN  = {'VALID ✅' if TELEGRAM_TOKEN else 'CRITICAL MISSING ❌'}")
log.info(f"[ENV MATRIX] RAW CHAT_ID      = '{RAW_CHAT_ID}'")
log.info(f"[ENV MATRIX] PARSED CHAT_ID   = {ALLOWED_CHAT_ID if ALLOWED_CHAT_ID else 'CRITICAL MISSING ❌'}")
log.info(f"[ENV MATRIX] RENDER_URL       = {RENDER_URL}")
log.info(f"[ENV MATRIX] PORT             = {PORT}")

if not TELEGRAM_TOKEN or not ALLOWED_CHAT_ID:
    log.error("[CRITICAL SHUTDOWN] Required environment variables are missing! Fix Render Dashboard.")

# ================================================================
# SQLITE PERSISTENCE
# ================================================================
def db_init():
    con = sqlite3.connect(DB_PATH, timeout=20)
    con.execute("CREATE TABLE IF NOT EXISTS coins (symbol TEXT PRIMARY KEY)")
    # FIX #5: persist engine on/off state so that if the process gets
    # restarted (Render spin-down, redeploy, OOM kill, etc.) it can
    # automatically resume scanning without waiting for a manual /start.
    con.execute("CREATE TABLE IF NOT EXISTS engine_state (key TEXT PRIMARY KEY, value TEXT)")
    con.execute("INSERT OR IGNORE INTO engine_state VALUES ('is_active', '0')")
    defaults = ["BTC/USDT","ETH/USDT","SOL/USDT","BB/USDT","LAB/USDT","BEAT/USDT"]
    for s in defaults:
        con.execute("INSERT OR IGNORE INTO coins VALUES (?)", (s,))
    con.commit(); con.close()

def db_load() -> List[str]:
    con = sqlite3.connect(DB_PATH, timeout=20)
    rows = con.execute("SELECT symbol FROM coins").fetchall()
    con.close()
    return [r[0] for r in rows]

def db_add(symbol: str):
    con = sqlite3.connect(DB_PATH, timeout=20)
    con.execute("INSERT OR IGNORE INTO coins VALUES (?)", (symbol,))
    con.commit(); con.close()

def db_remove(symbol: str):
    con = sqlite3.connect(DB_PATH, timeout=20)
    con.execute("DELETE FROM coins WHERE symbol=?", (symbol,))
    con.commit(); con.close()

def db_clear_all():
    con = sqlite3.connect(DB_PATH, timeout=20)
    con.execute("DELETE FROM coins")
    con.commit(); con.close()

def db_load_engine_active() -> bool:
    con = sqlite3.connect(DB_PATH, timeout=20)
    row = con.execute("SELECT value FROM engine_state WHERE key='is_active'").fetchone()
    con.close()
    return bool(row and row[0] == "1")

def db_save_engine_active(active: bool):
    con = sqlite3.connect(DB_PATH, timeout=20)
    con.execute("UPDATE engine_state SET value=? WHERE key='is_active'", ("1" if active else "0",))
    con.commit(); con.close()

# ENGINE RUNTIME STATE MATRIX
# FIX #5: on a fresh process boot, resume whatever state was last saved
# (so a Render restart/spin-down doesn't silently leave the bot asleep
# until someone notices and sends /start again). Runs here — after the
# db_* functions above are actually defined — so it no longer raises a
# silent NameError and always falls back to False.
try:
    db_init()
    is_engine_active = db_load_engine_active()
except Exception:
    is_engine_active = False

# ================================================================
# TELEGRAM — async httpx + queue
# ================================================================
_tg_queue: asyncio.Queue = None
_http: httpx.AsyncClient = None

async def tg_worker():
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
        r = await _http.get(url, timeout=10)
        log.info(f"[WEBHOOK] {r.json()}")
    except Exception as e:
        log.error(f"[WEBHOOK] {e}")

def esc(text: str) -> str:
    return text.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

# ================================================================
# FASTAPI LIFESPAN
# ================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global engine_instance, _tg_queue, _http
    # NOTE: db_init() already runs once at module import time (top-level
    # try/except above). Calling it again here was redundant and could
    # race with that first open under SQLite's single-writer behavior on
    # some cloud/multi-worker setups — so it's intentionally not repeated.
    _tg_queue       = asyncio.Queue(maxsize=500)
    _http           = httpx.AsyncClient()
    engine_instance = CompleteSentinelEngine()

    tg_task   = asyncio.create_task(tg_worker())
    loop_task = asyncio.create_task(engine_instance.engine_core_loop())

    yield
    loop_task.cancel(); tg_task.cancel()
    await engine_instance.close_clients()
    await _http.aclose()

app = FastAPI(lifespan=lifespan)
engine_instance = None

@app.api_route("/", methods=["GET", "HEAD"])
def health():
    return {
        "status"       : "ONLINE",
        "engine"       : BOT_NAME,
        "engine_active": is_engine_active,
        "chat_id_set"  : ALLOWED_CHAT_ID is not None,
        "coins_loaded" : len(engine_instance.tracked_symbols) if engine_instance else 0,
        "timestamp"    : datetime.now(timezone.utc).isoformat()
    }

# ================================================================
# WEBHOOK HANDLER
# ================================================================
@app.post("/tg-webhook")
async def tg_webhook(request: Request):
    global engine_instance, is_engine_active
    try:
        data = await request.json()
        if "message" not in data:
            return {"status":"ignored"}
        msg     = data["message"]
        chat_id = msg["chat"]["id"]
        text    = msg.get("text","").strip()

        if ALLOWED_CHAT_ID and int(chat_id) != ALLOWED_CHAT_ID:
            log.warning(f"[SECURITY] Blocked unauthorized chat {chat_id}")
            return {"status":"unauthorized"}

        if not text.startswith("/"):
            return {"status":"not a command"}

        parts   = text.split()
        command = parts[0].lower()

        if command == "/start":
            if not is_engine_active:
                is_engine_active = True
                db_save_engine_active(True)
                log.info("[CONTROL] Engine activated via Telegram command.")
                with engine_instance.symbol_lock:
                    coins = engine_instance.tracked_symbols.copy()
                send_telegram_msg(chat_id, (
                    f"🚀 <b>{esc(BOT_NAME)} CORES ACTIVATED</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🔄 Auto-scan loop is now active.\n"
                    f"⏱ Interval: Every {LOOP_INTERVAL}s\n"
                    f"🔕 Cooldown: DISABLED (Raw Feed Mode)\n"
                    f"🪙 Tracked Coins: {len(coins)} loaded\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"Use <code>/stop</code> to put the bot to sleep."
                ))
            else:
                send_telegram_msg(chat_id, "⚠️ Engine is already running and monitoring markets.")

        elif command == "/stop":
            if is_engine_active:
                is_engine_active = False
                db_save_engine_active(False)
                log.info("[CONTROL] Engine paused/put to sleep via Telegram command.")
                send_telegram_msg(chat_id, (
                    f"😴 <b>{esc(BOT_NAME)} SLEEP MODE</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🛑 Auto-scan loops have been paused.\n"
                    f"💤 Bot will sleep until you send <code>/start</code> again."
                ))
            else:
                send_telegram_msg(chat_id, "💤 Bot is already sleeping.")

        elif command == "/help":
            send_telegram_msg(chat_id, (
                f"📊 <b>{esc(BOT_NAME)} Manual</b>\n\n"
                "Commands:\n"
                "🚀 <code>/start</code> — Activate core loop & alerts\n"
                "🛑 <code>/stop</code> — Pause loop and let bot sleep\n"
                "🔹 <code>/add SOL</code> — Add coin to checklist\n"
                "🔹 <code>/remove SOL</code> — Remove coin from checklist\n"
                "🔹 <code>/del</code> — Clear ALL tracked coins at once\n"
                "🔹 <code>/list</code> — Show tracked coins\n"
                "🔍 <code>/scan</code> — Force instant scan right now\n"
                "🔹 <code>/status</code> — Check system core status"
            ))

        elif command == "/status":
            # FIX #2: read from the in-memory list instead of hitting SQLite
            # on every /status call.
            with engine_instance.symbol_lock:
                coin_count = len(engine_instance.tracked_symbols)
            send_telegram_msg(chat_id, (
                f"🔧 <b>Bot Status Control Panel</b>\n\n"
                f"Version    : {BOT_NAME}\n"
                f"Core Loop  : {'🟢 RUNNING' if is_engine_active else '🔴 SLEEPING / PAUSED'}\n"
                f"LOOP TIME  : Every {LOOP_INTERVAL}s\n"
                f"COOLDOWN   : ❌ REMOVED (Raw Feed)\n"
                f"Coins      : {coin_count} tracked\n"
                f"Time (UTC) : {datetime.now(timezone.utc).strftime('%H:%M:%S')}"
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

        elif command == "/del":
            removed = engine_instance.clear_coins()
            send_telegram_msg(chat_id, (
                f"🧹 <b>All coins cleared</b>\n"
                f"Removed {removed} tracked coin(s). List is now empty.\n"
                f"Use <code>/add SOL</code> to start adding new ones."
            ))

        elif command == "/list":
            with engine_instance.symbol_lock:
                coins = engine_instance.tracked_symbols.copy()
            lines = "\n".join([f"  {i+1}. {c}" for i,c in enumerate(coins)])
            send_telegram_msg(chat_id, f"📋 <b>Tracked ({len(coins)}):</b>\n\n{lines or 'Empty'}")

        elif command == "/scan":
            send_telegram_msg(chat_id, "🔍 Scanning all coins… please wait.")
            asyncio.create_task(engine_instance.run_scan(chat_id))

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
        # FIX #6: explicit timeout (ms). Without this, a stalled network call
        # to an exchange can hang indefinitely and freeze the whole scan,
        # since it never raises and never frees up its semaphore slot.
        swap        = {"options":{"defaultType":"swap"},"enableRateLimit":True,"timeout":15000}
        spot        = {"options":{"defaultType":"spot"},"enableRateLimit":True,"timeout":15000}
        future_only = {"enableRateLimit":True,"timeout":15000}

        # Multi-exchange routing — Binance intentionally EXCLUDED.
        # Each entry exposes a 'future' and 'spot' client so both routes are
        # attempted per exchange, same pattern as the original Gate/OKX pair.
        self.gate   = {'future': ccxt.gate(swap),   'spot': ccxt.gate(spot)}
        self.okx    = {'future': ccxt.okx(swap),    'spot': ccxt.okx(spot)}
        self.bybit  = {'future': ccxt.bybit(swap),  'spot': ccxt.bybit(spot)}
        self.mexc   = {'future': ccxt.mexc(swap),   'spot': ccxt.mexc(spot)}
        self.bitget = {'future': ccxt.bitget(swap), 'spot': ccxt.bitget(spot)}
        # KuCoin splits spot/futures into two separate ccxt classes.
        self.kucoin = {'future': ccxt.kucoinfutures(future_only), 'spot': ccxt.kucoin(future_only)}

        self._all_exchanges = [self.gate, self.okx, self.bybit, self.mexc, self.bitget, self.kucoin]

        # FIX #4: bound concurrent exchange calls to avoid rate-limit storms
        self.scan_semaphore = asyncio.Semaphore(MAX_CONCURRENT_SCANS)

    async def close_clients(self):
        for pair in self._all_exchanges:
            for c in pair.values():
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

    def clear_coins(self) -> int:
        with self.symbol_lock:
            count = len(self.tracked_symbols)
            self.tracked_symbols.clear()
            db_clear_all()
        return count

    # ── Technical Indicators Block ──
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

    def build_rsi_series(self, closes: np.ndarray, lookback: int = 60) -> np.ndarray:
        c = np.asarray(closes, dtype=float)
        if len(c) < 30: return np.full(lookback, 50.0)
        d  = np.diff(c)
        g  = np.where(d > 0, d, 0.0)
        ls = np.where(d < 0, -d, 0.0)
        ag = np.mean(g[:14]); al = np.mean(ls[:14])
        rsi_vals = []
        for i in range(14, len(g)):
            ag = (ag*13 + g[i]) / 14
            al = (al*13 + ls[i]) / 14
            if i >= len(g) - lookback:
                rs = ag/al if al else 1e9
                rsi_vals.append(round(100 - 100/(1+rs), 2))
        return np.array(rsi_vals) if rsi_vals else np.full(lookback, 50.0)

    def calc_mfi(self, h, l, c, v, period=14) -> float:
        h = np.asarray(h,dtype=float); l = np.asarray(l,dtype=float)
        c = np.asarray(c,dtype=float); v = np.asarray(v,dtype=float)
        if len(c) < period+1: return 50.0
        tp      = (h + l + c) / 3.0
        mf      = tp * v
        diff_tp = np.diff(tp)
        pos     = np.sum(np.where(diff_tp > 0, mf[1:], 0.0)[-period:])
        neg     = np.sum(np.where(diff_tp < 0, mf[1:], 0.0)[-period:])
        if neg == 0: return 100.0
        return round(100 - 100/(1 + pos/neg), 2)

    # ── Major Liquidity Zones (simplified order-book read) ──
    def calc_liquidity_heatmap(self, bids: list, asks: list, price: float, grab_pct: float = 10.0) -> List[str]:
        """
        Free, Coinglass-style order-book liquidity heatmap — but filtered
        down to only the EXTREME clusters (statistical outliers vs the
        rest of the book), not every minor level. Also flags whether each
        cluster sits within `grab_pct`% of current price (i.e. a move
        that size would already have swept/grabbed that liquidity), and
        gives a total $ figure for how much liquidity sits inside that
        band on each side.
        """
        bucket_size = max(price * 0.001, 1e-9)  # ~0.1% of price per bucket

        def cluster(levels):
            buckets: Dict[float, float] = {}
            for lvl_price, amount in levels:
                if lvl_price <= 0 or amount <= 0:
                    continue
                value  = lvl_price * amount
                bucket = round(lvl_price / bucket_size) * bucket_size
                buckets[bucket] = buckets.get(bucket, 0.0) + value
            return buckets

        ask_buckets = cluster(asks)  # resistance, above price
        bid_buckets = cluster(bids)  # support, below price

        def extreme_zones(buckets: Dict[float, float]):
            if not buckets:
                return []
            vals = np.array(list(buckets.values()), dtype=float)
            mean = float(np.mean(vals)); std = float(np.std(vals))
            threshold = mean + std if std > 0 else mean
            zones = [(p, v) for p, v in buckets.items() if v >= threshold]
            if not zones:
                # always surface the single biggest cluster even if nothing
                # is a clean statistical outlier
                zones = [max(buckets.items(), key=lambda x: x[1])]
            return sorted(zones, key=lambda x: -x[1])[:4]  # cap clutter

        ask_zones = extreme_zones(ask_buckets)
        bid_zones = extreme_zones(bid_buckets)
        ask_zones.sort(key=lambda x: x[0])                 # nearest resistance first
        bid_zones.sort(key=lambda x: x[0], reverse=True)   # nearest support first

        max_val = max([v for _, v in ask_zones + bid_zones] + [1.0])

        def bar(v: float) -> str:
            blocks = int(round((v / max_val) * 10)) if max_val > 0 else 0
            return "█" * max(1, min(10, blocks))

        EPS = 1e-6  # floating-point tolerance so an exact 10.0% boundary isn't excluded by rounding noise

        def grab_note(dist_pct: float) -> str:
            return (f"⚠️ within {grab_pct:.0f}% — grab risk"
                    if abs(dist_pct) <= grab_pct + EPS else
                    f"🛡️ {abs(dist_pct):.1f}% away")

        # Sum only the EXTREME clusters actually shown above that also sit
        # inside the ±grab_pct% band — i.e. "if price moves this much, this
        # much of the liquidity you can see above/below gets swept."
        up_grab_total = sum(
            v for p, v in ask_zones if (p/price - 1) * 100 <= grab_pct + EPS
        )
        down_grab_total = sum(
            v for p, v in bid_zones if (1 - p/price) * 100 <= grab_pct + EPS
        )

        lines = [
            "🎯 <b>Extreme Liquidity Clusters</b>",
            "<i>Free order-book heatmap (Coinglass-style) — outlier zones only</i>",
        ]
        if ask_zones:
            for p, v in ask_zones:
                dist = (p/price - 1) * 100
                lines.append(f"🔺 ${p:,.4f}  (+{dist:.2f}%)  {bar(v)} ${self.fmt(v)}  {grab_note(dist)}")
        else:
            lines.append("🔺 No extreme resistance cluster found")

        lines.append(f"💵 ${price:,.4f} ← current price")

        if bid_zones:
            for p, v in bid_zones:
                dist = (p/price - 1) * 100
                lines.append(f"🔻 ${p:,.4f}  ({dist:.2f}%)  {bar(v)} ${self.fmt(v)}  {grab_note(dist)}")
        else:
            lines.append("🔻 No extreme support cluster found")

        # Only show the summary note when there's actually something inside
        # the band on that side — no more "~$0.00" noise every scan.
        if up_grab_total > 0:
            lines.append(
                f"📌 Note: price +{grab_pct:.0f}% upar jaate hi total ~${self.fmt(up_grab_total)} liquidity grab ho jayegi"
            )
        if down_grab_total > 0:
            lines.append(
                f"📌 Note: price -{grab_pct:.0f}% neeche jaate hi total ~${self.fmt(down_grab_total)} liquidity grab ho jayegi"
            )
        return lines

    # ── Funding Rate (separate block, own fetch/format) ──
    async def fetch_funding_info(self, symbol: str):
        """
        Tries each futures-capable exchange (Binance excluded) in turn
        until one returns a funding rate. Returns (rate, next_funding_dt,
        source_label) or (None, None, None) if nobody has data for it.
        """
        mkt = f"{symbol}:USDT"
        futures_clients = [
            (self.gate['future'],   "Gate"),
            (self.okx['future'],    "OKX"),
            (self.bybit['future'],  "Bybit"),
            (self.mexc['future'],   "MEXC"),
            (self.bitget['future'], "Bitget"),
            (self.kucoin['future'], "KuCoin"),
        ]
        for client, label in futures_clients:
            try:
                fr = await client.fetch_funding_rate(mkt)
                if fr and fr.get('fundingRate') is not None:
                    rate    = float(fr['fundingRate'])
                    next_ts = fr.get('nextFundingTimestamp') or fr.get('fundingTimestamp')
                    next_dt = datetime.fromtimestamp(next_ts/1000, tz=timezone.utc) if next_ts else None
                    return rate, next_dt, label
            except Exception as e:
                log.debug(f"[FUNDING] {label} {symbol}: {e}")
        return None, None, None

    def fmt_funding_block(self, rate: Optional[float], next_dt, source: Optional[str]) -> List[str]:
        lines = ["💸 <b>Funding Rate</b>"]
        if rate is None:
            lines.append("⚪ No funding data available")
            return lines
        pct  = rate * 100
        icon = "🟢" if pct >= 0 else "🔴"
        lines.append(f"{icon} Rate: {pct:+.4f}%  ({esc(source)})")
        if next_dt:
            remaining = next_dt - datetime.now(timezone.utc)
            secs = max(0, int(remaining.total_seconds()))
            h, rem = divmod(secs, 3600)
            m, _   = divmod(rem, 60)
            lines.append(f"⏱ Next in: {h}h {m}m  ({next_dt.strftime('%H:%M UTC')})")
        else:
            lines.append("⏱ Next funding time unavailable")
        return lines

    def calc_obv(self, c: np.ndarray, v: np.ndarray) -> Tuple[float, str]:
        c = np.asarray(c,dtype=float); v = np.asarray(v,dtype=float)
        if len(c) < 2: return 0.0, "⚪"
        obv = np.zeros(len(c))
        for i in range(1, len(c)):
            obv[i] = obv[i-1] + (v[i] if c[i]>c[i-1] else -v[i] if c[i]<c[i-1] else 0)
        trend = "↑🟢" if obv[-1] > obv[-6] else "↓🔴"
        return obv[-1], trend

    def calc_vol_ratio(self, v: np.ndarray) -> Tuple[float, float]:
        v   = np.asarray(v, dtype=float)
        avg = np.mean(v[-21:-1]) if len(v) >= 21 else np.mean(v[:-1])
        return v[-1], (v[-1]/avg if avg > 0 else 1.0)

    def calc_ema(self, c: np.ndarray, period: int) -> float:
        c = np.asarray(c, dtype=float)
        if len(c) < period: return float(c[-1])
        k = 2/(period+1); val = np.mean(c[:period])
        for x in c[period:]: val = x*k + val*(1-k)
        return round(val, 8)

    def ema_structure(self, c: np.ndarray, price: float) -> str:
        e20  = self.calc_ema(c, 20)
        e50  = self.calc_ema(c, 50)
        e200 = self.calc_ema(c, 200) if len(c) >= 200 else None
        def _f(v): return f"{v:,.5f}"
        line  = f"EMA20:{_f(e20)}  EMA50:{_f(e50)}"
        if e200: line += f"  EMA200:{_f(e200)}"
        above = [str(p) for p,e in [(20,e20),(50,e50)] + ([(200,e200)] if e200 else []) if price > e]
        line += f"  | Price>EMA: {','.join(above) or 'none'}"
        return line

    def calc_trend(self, c: np.ndarray, price: float) -> Tuple[str, str]:
        c = np.asarray(c, dtype=float)
        if len(c) < 22: return "Neutral", "⚪"
        e9   = self.calc_ema(c, 9)
        e21  = self.calc_ema(c, 21)
        e200 = self.calc_ema(c, 200) if len(c) >= 200 else None
        slope = c[-1] - c[-6]
        if e9 > e21 and slope > 0:
            bias = (" | ✅ Above EMA200" if e200 and price > e200 else " | ⚠️ Below EMA200" if e200 else "")
            return f"Bullish{bias}", "🟢"
        elif e9 < e21 and slope < 0:
            bias = (" | ✅ Below EMA200" if e200 and price < e200 else " | ⚠️ Above EMA200" if e200 else "")
            return f"Bearish{bias}", "🔴"
        return "Neutral", "⚪"

    def calc_market_structure(self, c: np.ndarray) -> Tuple[str, str]:
        c = np.asarray(c, dtype=float)
        if len(c) < 40: return "No Data", "⚪"
        seg = c[-60:] if len(c) >= 60 else c
        seg = seg[:-2]
        ph, pl = [], []
        for i in range(5, len(seg)-5):
            if seg[i] == np.max(seg[i-5:i+6]): ph.append((i, seg[i]))
            if seg[i] == np.min(seg[i-5:i+6]): pl.append((i, seg[i]))
        if len(ph) < 3 or len(pl) < 3: return "Consolidation", "⚪"
        price_close   = c[-3]
        was_uptrend   = ph[-2][1] > ph[-3][1]
        was_downtrend = pl[-2][1] < pl[-3][1]
        if price_close > ph[-1][1]:
            return ("CHoCH 🔄 Bull","🟢") if was_downtrend else ("BOS ↑ Bull","🟢")
        elif price_close < pl[-1][1]:
            return ("CHoCH 🔄 Bear","🔴") if was_uptrend else ("BOS ↓ Bear","🔴")
        return "Inside Range", "⚪"

    def calc_divergence(self, closes: np.ndarray) -> Tuple[str, str]:
        c = np.asarray(closes, dtype=float)
        if len(c) < 60: return "No Data", "⚪"
        rsi_arr = self.build_rsi_series(c, lookback=60)
        prc_arr = c[-len(rsi_arr):]
        if len(rsi_arr) < 10: return "No Data", "⚪"
        def pl(a): return [i for i in range(2,len(a)-2) if a[i]==np.min(a[i-2:i+3])]
        def ph(a): return [i for i in range(2,len(a)-2) if a[i]==np.max(a[i-2:i+3])]
        p_l=pl(prc_arr); p_h=ph(prc_arr); r_l=pl(rsi_arr); r_h=ph(rsi_arr)
        if len(p_l)>=2 and len(r_l)>=2:
            if prc_arr[p_l[-1]]<prc_arr[p_l[-2]] and rsi_arr[r_l[-1]]>rsi_arr[r_l[-2]]:
                return "Bullish Div 🔺","🟢"
        if len(p_h)>=2 and len(r_h)>=2:
            if prc_arr[p_h[-1]]>prc_arr[p_h[-2]] and rsi_arr[r_h[-1]]<rsi_arr[r_h[-2]]:
                return "Bearish Div 🔻","🔴"
        if len(p_l)>=2 and len(r_l)>=2:
            if prc_arr[p_l[-1]]>prc_arr[p_l[-2]] and rsi_arr[r_l[-1]]<rsi_arr[r_l[-2]]:
                return "Hidden Bull 🔺","🟢"
        if len(p_h)>=2 and len(r_h)>=2:
            if prc_arr[p_h[-1]]<prc_arr[p_h[-2]] and rsi_arr[r_h[-1]]>rsi_arr[r_h[-2]]:
                return "Hidden Bear 🔻","🔴"
        return "No Divergence","⚪"

    # ── Advanced Indicators (SuperTrend / VWAP / Bollinger / ADX / StochRSI) ──
    def calc_supertrend(self, h, l, c, period: int = 10, multiplier: float = 3.0):
        h = np.asarray(h, dtype=float); l = np.asarray(l, dtype=float); c = np.asarray(c, dtype=float)
        n = len(c)
        if n < period * 2:
            return 0.0, "⚪", "No Data", None
        tr = np.zeros(n)
        tr[0] = h[0] - l[0]
        for i in range(1, n):
            tr[i] = max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1]))
        atr = np.zeros(n)
        atr[:period] = np.mean(tr[:period])
        for i in range(period, n):
            atr[i] = (atr[i-1]*(period-1) + tr[i]) / period
        hl2 = (h + l) / 2.0
        basic_upper = hl2 + multiplier * atr
        basic_lower = hl2 - multiplier * atr
        final_upper = np.zeros(n); final_lower = np.zeros(n)
        final_upper[0] = basic_upper[0]; final_lower[0] = basic_lower[0]
        for i in range(1, n):
            final_upper[i] = basic_upper[i] if (basic_upper[i] < final_upper[i-1] or c[i-1] > final_upper[i-1]) else final_upper[i-1]
            final_lower[i] = basic_lower[i] if (basic_lower[i] > final_lower[i-1] or c[i-1] < final_lower[i-1]) else final_lower[i-1]
        st = np.zeros(n); is_upper = np.zeros(n, dtype=bool)
        st[0] = final_upper[0]; is_upper[0] = True
        for i in range(1, n):
            if is_upper[i-1] and c[i] <= final_upper[i]:
                st[i] = final_upper[i]; is_upper[i] = True
            elif is_upper[i-1] and c[i] > final_upper[i]:
                st[i] = final_lower[i]; is_upper[i] = False
            elif (not is_upper[i-1]) and c[i] >= final_lower[i]:
                st[i] = final_lower[i]; is_upper[i] = False
            else:
                st[i] = final_upper[i]; is_upper[i] = True
        bullish = not is_upper[-1]
        return round(float(st[-1]), 8), ("🟢" if bullish else "🔴"), ("Bullish" if bullish else "Bearish"), bullish

    def calc_vwap(self, h, l, c, v):
        h = np.asarray(h, dtype=float); l = np.asarray(l, dtype=float)
        c = np.asarray(c, dtype=float); v = np.asarray(v, dtype=float)
        tp = (h + l + c) / 3.0
        cum_v = np.cumsum(v)
        vwap = float(np.cumsum(tp*v)[-1] / cum_v[-1]) if cum_v[-1] > 0 else float(c[-1])
        above = c[-1] > vwap
        return round(vwap, 8), ("🟢" if above else "🔴"), ("Price Above VWAP" if above else "Price Below VWAP")

    def calc_bollinger(self, c, period: int = 20, num_std: float = 2.0):
        c = np.asarray(c, dtype=float)
        if len(c) < period:
            return None
        window = c[-period:]
        mid = float(np.mean(window)); std = float(np.std(window))
        upper = mid + num_std*std; lower = mid - num_std*std
        price = float(c[-1])
        bandwidth = (upper - lower) / mid if mid else 0.0
        squeeze = False
        if len(c) >= period * 3:
            bw_hist = []
            for i in range(period, len(c)):
                w = c[i-period:i]
                m = np.mean(w); s = np.std(w)
                bw_hist.append(((m + num_std*s) - (m - num_std*s)) / m if m else 0.0)
            recent = bw_hist[-20:]
            avg_bw = float(np.mean(recent)) if recent else bandwidth
            squeeze = bandwidth < avg_bw * 0.6
        if price > upper:   pos = "Above Upper Band 🔴OB"
        elif price < lower: pos = "Below Lower Band 🟢OS"
        else:                pos = "Inside Bands ⚪"
        return upper, mid, lower, pos, squeeze

    def calc_adx(self, h, l, c, period: int = 14):
        h = np.asarray(h, dtype=float); l = np.asarray(l, dtype=float); c = np.asarray(c, dtype=float)
        n = len(c)
        if n < period * 2 + 1:
            return 0.0, "No Data ⚪"
        up_move   = h[1:] - h[:-1]
        down_move = l[:-1] - l[1:]
        plus_dm   = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        minus_dm  = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
        tr = np.maximum.reduce([h[1:]-l[1:], np.abs(h[1:]-c[:-1]), np.abs(l[1:]-c[:-1])])

        def smooth(arr):
            s = np.zeros(len(arr))
            s[period-1] = np.sum(arr[:period])
            for i in range(period, len(arr)):
                s[i] = s[i-1] - (s[i-1]/period) + arr[i]
            return s

        atr_s      = smooth(tr)
        plus_dm_s  = smooth(plus_dm)
        minus_dm_s = smooth(minus_dm)
        plus_di  = 100 * (plus_dm_s / (atr_s + 1e-9))
        minus_di = 100 * (minus_dm_s / (atr_s + 1e-9))
        dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di + 1e-9)
        start = period*2 - 1
        if start >= len(dx):
            return 0.0, "No Data ⚪"
        adx = np.zeros(len(dx))
        adx[start] = np.mean(dx[period-1:start+1])
        for i in range(start+1, len(dx)):
            adx[i] = (adx[i-1]*(period-1) + dx[i]) / period
        val = round(float(adx[-1]), 2)
        if val >= 50:   lbl = f"{val} 🔥 Very Strong Trend"
        elif val >= 25: lbl = f"{val} 💪 Strong Trend"
        else:            lbl = f"{val} 😴 Weak/Range"
        return val, lbl

    def calc_stoch_rsi(self, c, rsi_period: int = 14, stoch_period: int = 14, smooth_k: int = 3, smooth_d: int = 3):
        c = np.asarray(c, dtype=float)
        rsi_series = self.build_rsi_series(c, lookback=max(stoch_period + smooth_k + smooth_d + 10, 60))
        if len(rsi_series) < stoch_period + smooth_k:
            return 50.0, 50.0, "⚪ MID"
        k_vals = []
        for i in range(stoch_period-1, len(rsi_series)):
            window = rsi_series[i-stoch_period+1:i+1]
            lo, hi = np.min(window), np.max(window)
            k_vals.append(100*(rsi_series[i]-lo)/(hi-lo) if hi > lo else 50.0)
        k_vals = np.array(k_vals)
        k_smooth = np.convolve(k_vals, np.ones(smooth_k)/smooth_k, mode='valid') if len(k_vals) >= smooth_k else k_vals
        d_smooth = np.convolve(k_smooth, np.ones(smooth_d)/smooth_d, mode='valid') if len(k_smooth) >= smooth_d else k_smooth
        k_final = round(float(k_smooth[-1]), 2) if len(k_smooth) else 50.0
        d_final = round(float(d_smooth[-1]), 2) if len(d_smooth) else 50.0
        if k_final >= 80:   lbl = "🔴 OB"
        elif k_final <= 20: lbl = "🟢 OS"
        else:                lbl = "⚪ MID"
        return k_final, d_final, lbl

    def calc_signal_score(self, rsi, mfi, trend_icon, div_lbl, struct_icon, obv_dir, vol_ratio,
                           st_bull: Optional[bool] = None, stoch_k: Optional[float] = None,
                           adx_val: Optional[float] = None) -> Tuple[int,str,str]:
        bull = 0; bear = 0
        if trend_icon  == "🟢": bull += 3
        elif trend_icon == "🔴": bear += 3
        if struct_icon  == "🟢": bull += 3
        elif struct_icon == "🔴": bear += 3
        if "🟢" in obv_dir: bull += 1
        elif "🔴" in obv_dir: bear += 1
        if rsi <= 30:  bull += 2
        elif rsi >= 70: bear += 2
        if mfi <= 20:  bull += 2
        elif mfi >= 80: bear += 2
        if 30<rsi<45:  bull += 1
        elif 55<rsi<70: bear += 1
        if "Bullish Div" in div_lbl: bull += 3
        elif "Hidden Bull" in div_lbl: bull += 2
        elif "Bearish Div" in div_lbl: bear += 3
        elif "Hidden Bear" in div_lbl: bear += 2
        if vol_ratio >= 2.0:
            bonus = min(2, int(vol_ratio//2))
            if bull >= bear: bull += bonus
            else: bear += bonus
        # ── Advanced indicator contributions ──
        if st_bull is True:   bull += 2
        elif st_bull is False: bear += 2
        if stoch_k is not None:
            if stoch_k <= 20:  bull += 1
            elif stoch_k >= 80: bear += 1
        if adx_val is not None and adx_val >= 25:
            # Strong trend (ADX) reinforces whichever side is already leading
            if bull > bear: bull += 1
            elif bear > bull: bear += 1
        total = bull + bear
        net   = bull - bear
        score = round((bull/total)*10) if total > 0 else 5
        if   net >= 7:  return score, "STRONG BUY 🚀",  "🟢"
        elif net >= 3:  return score, "BUY 📈",          "🟢"
        elif net <= -7: return score, "STRONG SELL 📉",  "🔴"
        elif net <= -3: return score, "SELL 🔻",         "🔴"
        else:           return score, "NEUTRAL ⚪",      "⚪"

    # ── Formatters ──
    def fmt(self, v: float, signed=False) -> str:
        neg=v<0; a=abs(v)
        if a>=1e9:   s=f"{a/1e9:.2f}B"
        elif a>=1e6: s=f"{a/1e6:.2f}M"
        elif a>=1e3: s=f"{a/1e3:.1f}K"
        else: s=f"{a:.2f}"
        return (("-" if neg else "+") if signed else ("-" if neg else "")) + s

    def rsi_lbl(self,v):
        if v>=70: return f"{v:.1f} 🔴OB"
        if v<=30: return f"{v:.1f} 🟢OS"
        return f"{v:.1f} ⚪MID"

    def mfi_lbl(self,v):
        if v>=80: return f"{v:.1f} 🔴OB"
        if v<=20: return f"{v:.1f} 🟢OS"
        return f"{v:.1f} ⚪MID"

    def vol_lbl(self,vol,ratio):
        s=self.fmt(vol)
        if ratio>=3.0: return f"{s} x{ratio:.1f}🔥🔥"
        elif ratio>=2.0: return f"{s} x{ratio:.1f}🔥"
        elif ratio>=1.5: return f"{s} x{ratio:.1f}↑"
        return f"{s} x{ratio:.1f}"

    async def fetch_exchange_data(self, symbol: str):
        attempts = [
            (self.gate['future'],   f"{symbol}:USDT", "Gate (FUTURE)"),
            (self.okx['future'],    f"{symbol}:USDT", "OKX (FUTURE)"),
            (self.bybit['future'],  f"{symbol}:USDT", "Bybit (FUTURE)"),
            (self.mexc['future'],   f"{symbol}:USDT", "MEXC (FUTURE)"),
            (self.bitget['future'], f"{symbol}:USDT", "Bitget (FUTURE)"),
            (self.kucoin['future'], f"{symbol}:USDT", "KuCoin (FUTURE)"),
            (self.gate['spot'],     symbol,            "Gate (SPOT)"),
            (self.okx['spot'],      symbol,            "OKX (SPOT)"),
            (self.bybit['spot'],    symbol,            "Bybit (SPOT)"),
            (self.mexc['spot'],     symbol,            "MEXC (SPOT)"),
            (self.bitget['spot'],   symbol,            "Bitget (SPOT)"),
            (self.kucoin['spot'],   symbol,            "KuCoin (SPOT)"),
        ]
        for client, mkt, label in attempts:
            try:
                ticker = await client.fetch_ticker(mkt)
                if ticker and ticker.get('last'):
                    return float(ticker['last']), label, client, mkt
            except Exception as e:
                log.debug(f"[ROUTER] {label} {symbol}: {e}")
        log.warning(f"[ROUTER] No data for {symbol}")
        return None, None, None, symbol

    async def process_symbol(self, symbol: str) -> Optional[Tuple[str,int,str]]:
        price, source, client, mkt = await self.fetch_exchange_data(symbol)
        if not price or not client:
            return None

        # Light emoji separators instead of the old solid block lines
        D2 = "🔶━━━━━━━━━━━━━━━━━━━🔶"
        D  = "▫️▫️▫️▫️▫️▫️▫️▫️▫️▫️▫️▫️▫️▫️"

        lines = [
            D2,
            f"   📊 {symbol}",
            f"   💵 Price  : ${price:,.6f}",
            f"   🏛  Source : {source}",
            D2,
            f"{'TF':<5}│{'RSI(14)':<12}│{'MFI(14)':<12}│OBV",
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
                lines.append(f"{tf:<5}│{'--':^12}│{'--':^12}│--")
                continue
            h=np.array([float(x[2]) for x in ohlcv])
            l=np.array([float(x[3]) for x in ohlcv])
            c=np.array([float(x[4]) for x in ohlcv])
            v=np.array([float(x[5]) for x in ohlcv])

            rsi          = self.calc_rsi(c)
            mfi          = self.calc_mfi(h,l,c,v)
            obv, obv_dir = self.calc_obv(c,v)
            vol, ratio   = self.calc_vol_ratio(v)

            tf_store[tf] = dict(h=h,l=l,c=c,v=v,rsi=rsi,mfi=mfi,obv=obv,obv_dir=obv_dir,vol=vol,ratio=ratio)
            lines.append(
                f"{tf:<5}│{self.rsi_lbl(rsi):<12}│{self.mfi_lbl(mfi):<12}"
                f"│{self.fmt(obv,True)+obv_dir}"
            )
        lines.append(D)

        # ── Volume moved below the main table, its own compact block ──
        lines.append("   📊 VOLUME")
        for tf in self.TIMEFRAMES:
            if tf in tf_store:
                d = tf_store[tf]
                lines.append(f"   {tf:<5}: {self.vol_lbl(d['vol'], d['ratio'])}")
            else:
                lines.append(f"   {tf:<5}: --")
        lines.append(D)

        pri = next((tf_store[t] for t in ['1h','15m','5m','1m'] if t in tf_store), None)
        score=5; sig_lbl="NEUTRAL ⚪"; sig_icon="⚪"
        if pri:
            c_p   = pri['c']
            trend_lbl,  trend_icon  = self.calc_trend(c_p, price)
            struct_lbl, struct_icon = self.calc_market_structure(c_p)
            div_lbl,    div_icon    = self.calc_divergence(c_p)
            ema_line                = self.ema_structure(c_p, price)

            # ── Advanced indicators (new) ──
            st_val, st_icon, st_lbl, st_bull = self.calc_supertrend(pri['h'], pri['l'], c_p)
            vwap_val, vwap_icon, vwap_lbl    = self.calc_vwap(pri['h'], pri['l'], c_p, pri['v'])
            boll                             = self.calc_bollinger(c_p)
            adx_val, adx_lbl                 = self.calc_adx(pri['h'], pri['l'], c_p)
            k_val, d_val, stoch_lbl          = self.calc_stoch_rsi(c_p)

            score, sig_lbl, sig_icon = self.calc_signal_score(
                pri['rsi'], pri['mfi'], trend_icon, div_lbl,
                struct_icon, pri['obv_dir'], pri['ratio'],
                st_bull=st_bull, stoch_k=k_val, adx_val=adx_val
            )
            lines += [
                f"   📐 EMA (1h basis)",
                f"   {ema_line}",
                D,
                f"   📈 TREND      : {trend_icon} {trend_lbl}",
                f"   🏗  STRUCTURE  : {struct_icon} {struct_lbl}",
                f"   🔀 DIVERGENCE : {div_icon} {div_lbl}",
                D,
                f"   🧭 ADVANCED INDICATORS",
                f"   SuperTrend : {st_icon} {st_lbl} @ {st_val:,.5f}",
                f"   VWAP       : {vwap_icon} {vwap_lbl} @ {vwap_val:,.5f}",
            ]
            if boll:
                _, _, _, boll_pos, boll_squeeze = boll
                lines.append(f"   BB(20,2)   : {boll_pos}" + ("  ⚠️ SQUEEZE" if boll_squeeze else ""))
            lines += [
                f"   ADX(14)    : {adx_lbl}",
                f"   StochRSI   : K={k_val:.1f} D={d_val:.1f} {stoch_lbl}",
                D,
                f"   ⚡ SIGNAL  {score}/10  →  {sig_icon} {sig_lbl}",
            ]
        else:
            lines.append("   ⚠️  Insufficient data")

        html_body = esc(chr(10).join(lines))

        # ── Liquidity zones block (order-book snapshot) — kept outside
        # the <pre> block since it uses <b> tags for the header/price ──
        liq_lines = []
        try:
            book = await client.fetch_order_book(mkt, limit=50)
            bids = book.get('bids') or []
            asks = book.get('asks') or []
            if bids or asks:
                liq_lines = self.calc_liquidity_heatmap(bids, asks, price)
        except Exception as e:
            log.debug(f"[LIQ ZONES] {symbol} order book fetch failed: {e}")

        # ── Funding rate block (order-book se alag, apna khud ka block) ──
        funding_lines = []
        try:
            f_rate, f_next_dt, f_source = await self.fetch_funding_info(symbol)
            funding_lines = self.fmt_funding_block(f_rate, f_next_dt, f_source)
        except Exception as e:
            log.debug(f"[FUNDING] {symbol} fetch failed: {e}")

        html = f"<pre>{html_body}</pre>"
        if liq_lines:
            html += "\n" + "\n".join(liq_lines)
        if funding_lines:
            html += "\n" + "\n".join(funding_lines)
        html += f"\n{D2}"
        return html, score, sig_lbl

    # FIX #1: run_scan now shields each symbol with try/except so a single
    # network error / rate-limit / exchange hiccup can't silently swallow
    # results for the rest of the batch. FIX #4: semaphore caps concurrency.
    async def run_scan(self, chat_id: int):
        with self.symbol_lock:
            symbols = self.tracked_symbols.copy()

        SYMBOL_TIMEOUT = 40  # seconds — hard ceiling per coin, no matter what stalls

        async def process_and_send(sym):
            async with self.scan_semaphore:
                log.info(f"[SCAN] {sym} → starting")
                try:
                    result = await asyncio.wait_for(self.process_symbol(sym), timeout=SYMBOL_TIMEOUT)
                    if not result:
                        log.info(f"[SCAN] {sym} → no data, skipped")
                        return
                    html, _, _ = result
                    # Direct transmission block — Cooldown skip filter completely wiped out
                    send_telegram_msg(chat_id, html)
                    log.info(f"[SCAN] {sym} → sent")
                except asyncio.TimeoutError:
                    log.error(f"[SCAN TIMEOUT] {sym} exceeded {SYMBOL_TIMEOUT}s — skipped, moving on")
                except Exception as e:
                    log.error(f"[SCAN ERROR] Failed to process/send {sym}: {e}")

        await asyncio.gather(*[process_and_send(s) for s in symbols])

    # Controlled Matrix Auto Loop
    async def engine_core_loop(self):
        await asyncio.sleep(5)
        await setup_webhook()

        if not ALLOWED_CHAT_ID:
            log.error("[ENGINE] ❌ CHAT_ID is missing or invalid. Engine loop crashed on initial state validation.")
            return

        log.info(f"[ENGINE] {BOT_NAME} loaded. Awaiting active trigger command /start via Telegram.")

        # FIX #5: make process restarts visible in Telegram (not just Render
        # logs), and auto-resume scanning if it was active before the restart.
        boot_note = (
            f"♻️ <b>{esc(BOT_NAME)} process restarted</b>\n"
            f"State resumed: {'🟢 RUNNING' if is_engine_active else '🔴 SLEEPING'}\n"
            f"If this is unexpected, check Render logs — the container likely "
            f"restarted (spin-down, redeploy, or crash)."
        )
        send_telegram_msg(ALLOWED_CHAT_ID, boot_note)

        while True:
            if is_engine_active:
                ts = datetime.now(timezone.utc).strftime('%H:%M:%S UTC')
                log.info(f"[LOOP CORE] Active Scan Triggered @ {ts}")
                try:
                    await self.run_scan(ALLOWED_CHAT_ID)
                except Exception as e:
                    log.error(f"[LOOP ERROR] {e}")
            else:
                # Idle state matrix — prevents resource usage while bot is in sleep mode
                log.debug("[LOOP CORE] Engine is currently in sleep mode. Skipping cycle iteration.")

            await asyncio.sleep(LOOP_INTERVAL)

# ================================================================
# ENTRY POINT
# ================================================================
if __name__ == "__main__":
    # FIX #3: use the platform-assigned $PORT (falls back to 10000 locally)
    uvicorn.run("bot:app", host="0.0.0.0", port=PORT, log_level="warning", reload=False)
