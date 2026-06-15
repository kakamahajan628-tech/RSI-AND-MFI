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
BOT_VERSION = "V23.0-CONTROLLED"
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

# ENGINE RUNTIME STATE MATRIX
# Starts as False until the user explicitly hits /start in Telegram
is_engine_active = False

log.info(f"[ENV MATRIX] TELEGRAM_TOKEN  = {'VALID ✅' if TELEGRAM_TOKEN else 'CRITICAL MISSING ❌'}")
log.info(f"[ENV MATRIX] RAW CHAT_ID      = '{RAW_CHAT_ID}'")
log.info(f"[ENV MATRIX] PARSED CHAT_ID   = {ALLOWED_CHAT_ID if ALLOWED_CHAT_ID else 'CRITICAL MISSING ❌'}")
log.info(f"[ENV MATRIX] RENDER_URL       = {RENDER_URL}")

if not TELEGRAM_TOKEN or not ALLOWED_CHAT_ID:
    log.error("[CRITICAL SHUTDOWN] Required environment variables are missing! Fix Render Dashboard.")

# ================================================================
# SQLITE PERSISTENCE
# ================================================================
def db_init():
    con = sqlite3.connect(DB_PATH, timeout=20)
    con.execute("CREATE TABLE IF NOT EXISTS coins (symbol TEXT PRIMARY KEY)")
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
    db_init()
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

@app.get("/")
def health():
    return {
        "status"       : "ONLINE",
        "engine"       : BOT_NAME,
        "engine_active": is_engine_active,
        "chat_id_set"  : ALLOWED_CHAT_ID is not None,
        "coins_loaded" : len(db_load()),
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
                log.info("[CONTROL] Engine activated via Telegram command.")
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
                "🔹 <code>/list</code> — Show tracked coins\n"
                "🔍 <code>/scan</code> — Force instant scan right now\n"
                "🔹 <code>/status</code> — Check system core status"
            ))
            
        elif command == "/status":
            send_telegram_msg(chat_id, (
                f"🔧 <b>Bot Status Control Panel</b>\n\n"
                f"Version    : {BOT_NAME}\n"
                f"Core Loop  : {'🟢 RUNNING' if is_engine_active else '🔴 SLEEPING / PAUSED'}\n"
                f"LOOP TIME  : Every {LOOP_INTERVAL}s\n"
                f"COOLDOWN   : ❌ REMOVED (Raw Feed)\n"
                f"Coins      : {len(db_load())} tracked\n"
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
        swap = {"options":{"defaultType":"swap"},"enableRateLimit":True}
        spot = {"options":{"defaultType":"spot"},"enableRateLimit":True}
        self.gate = {'future': ccxt.gate(swap), 'spot': ccxt.gate(spot)}
        self.okx  = {'future': ccxt.okx(swap),  'spot': ccxt.okx(spot)}

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

    def calc_signal_score(self, rsi, mfi, trend_icon, div_lbl, struct_icon, obv_dir, vol_ratio) -> Tuple[int,str,str]:
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
        log.warning(f"[ROUTER] No data for {symbol}")
        return None, None, None, symbol

    async def process_symbol(self, symbol: str) -> Optional[Tuple[str,int,str]]:
        price, source, client, mkt = await self.fetch_exchange_data(symbol)
        if not price or not client:
            return None
        D  = "─" * 54
        D2 = "═" * 54
        lines = [
            D2,
            f"   📊 {symbol}",
            f"   💵 Price  : ${price:,.6f}",
            f"   🏛  Source : {source}",
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
                f"│{self.fmt(obv,True)+obv_dir:<11}│{self.vol_lbl(vol,ratio)}"
            )
        lines.append(D)
        pri = next((tf_store[t] for t in ['1h','15m','5m','1m'] if t in tf_store), None)
        score=5; sig_lbl="NEUTRAL ⚪"; sig_icon="⚪"
        if pri:
            c_p   = pri['c']
            trend_lbl,  trend_icon  = self.calc_trend(c_p, price)
            struct_lbl, struct_icon = self.calc_market_structure(c_p)
            div_lbl,    div_icon    = self.calc_divergence(c_p)
            ema_line                = self.ema_structure(c_p, price)
            score, sig_lbl, sig_icon = self.calc_signal_score(
                pri['rsi'], pri['mfi'], trend_icon, div_lbl,
                struct_icon, pri['obv_dir'], pri['ratio']
            )
            lines += [
                "",
                f"   📐 EMA (1h basis)",
                f"   {ema_line}",
                D,
                f"   📈 TREND      : {trend_icon} {trend_lbl}",
                f"   🏗  STRUCTURE  : {struct_icon} {struct_lbl}",
                f"   🔀 DIVERGENCE : {div_icon} {div_lbl}",
                D,
                f"   ⚡ SIGNAL  {score}/10  →  {sig_icon} {sig_lbl}",
            ]
        else:
            lines.append("   ⚠️  Insufficient data")
        lines.append(D2)
        html = f"<pre>{esc(chr(10).join(lines))}</pre>"
        return html, score, sig_lbl

    # COOLDOWN CRITICAL REMOVAL: Process symbols and send direct updates every single time
    async def run_scan(self, chat_id: int):
        with self.symbol_lock:
            symbols = self.tracked_symbols.copy()
        async def process_and_send(sym):
            result = await self.process_symbol(sym)
            if not result: return
            html, _, _ = result
            # Direct transmission block — Cooldown skip filter completely wiped out
            send_telegram_msg(chat_id, html)
        await asyncio.gather(*[process_and_send(s) for s in symbols])

    # Controlled Matrix Auto Loop
    async def engine_core_loop(self):
        await asyncio.sleep(5)
        await setup_webhook()
        
        if not ALLOWED_CHAT_ID:
            log.error("[ENGINE] ❌ CHAT_ID is missing or invalid. Engine loop crashed on initial state validation.")
            return

        log.info(f"[ENGINE] {BOT_NAME} loaded. Awaiting active trigger command /start via Telegram.")

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
    uvicorn.run("bot:app", host="0.0.0.0", port=10000, log_level="warning", reload=False)
