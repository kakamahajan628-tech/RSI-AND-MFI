import pandas as pd
import numpy as np
import ccxt.async_support as ccxt
import asyncio
import sqlite3
import uvicorn
import threading
import httpx  # Telegram alerts ke liye
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple

# ==========================================
# CONFIGURATION (Apna Data Bharein)
# ==========================================
TELEGRAM_TOKEN = "8621970578:AAFnKwq9Uq4ljwxvS6KNBe_4MndLtgeuylo"
TELEGRAM_CHAT_ID = "YOUR_CHAT_ID" # Apna Chat ID yahan dalein

# ==========================================
# 0. API DATA MODELS
# ==========================================
class CoinRequest(BaseModel):
    symbol: str

# ==========================================
# 1. RENDER HEALTH CHECK & API
# ==========================================
app = FastAPI()
sentinel_engine = None 

@app.get("/")
def health_check():
    global sentinel_engine
    tracked = list(sentinel_engine.tracked_symbols) if sentinel_engine else []
    return {
        "status": "ONLINE",
        "currently_tracking": tracked,
        "time_utc": datetime.now(timezone.utc).isoformat()
    }

@app.post("/add_coin")
def add_coin(req: CoinRequest):
    symbol_upper = req.symbol.upper().strip()
    sentinel_engine.add_symbol(symbol_upper)
    return {"message": f"Added {symbol_upper}"}

@app.post("/remove_coin")
def remove_coin(req: CoinRequest):
    symbol_upper = req.symbol.upper().strip()
    sentinel_engine.remove_symbol(symbol_upper)
    return {"message": f"Removed {symbol_upper}"}

def start_render_health_gateway():
    uvicorn.run(app, host="0.0.0.0", port=10000, log_level="error")

# ==========================================
# 2. TELEGRAM ALERT SYSTEM (No Conflict)
# ==========================================
async def send_telegram_alert(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    async with httpx.AsyncClient() as client:
        try:
            await client.post(url, json=payload)
        except Exception as e:
            print(f"Telegram Error: {e}")

# ==========================================
# 3. DATABASE LAYER
# ==========================================
class AdvancedSignalDatabase:
    def __init__(self, db_name="sentinel_quantum_v8.db"):
        self.db_name = db_name
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS tracked_coins (symbol TEXT PRIMARY KEY)")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS quantitative_journal (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT, symbol TEXT, direction TEXT,
                    entry_price REAL, stop_loss REAL, take_profit REAL,
                    trade_status TEXT, calculated_confidence REAL
                )
            """)
            conn.commit()

    def log_trade(self, symbol, direction, entry, sl, tp, conf):
        with sqlite3.connect(self.db_name) as conn:
            conn.execute("""
                INSERT INTO quantitative_journal (timestamp, symbol, direction, entry_price, stop_loss, take_profit, trade_status, calculated_confidence)
                VALUES (?, ?, ?, ?, ?, ?, 'OPEN', ?)
            """, (datetime.now(timezone.utc).isoformat(), symbol, direction, entry, sl, tp, conf))

# ========================================================
# 4. ENGINE CORE (Modified with 1-Min Loop & Alerts)
# ========================================================
class CompleteSentinelEngine:
    def __init__(self):
        self.db = AdvancedSignalDatabase()
        self.tracked_symbols = set()
        self.load_tracked_symbols()

    def load_tracked_symbols(self):
        with sqlite3.connect(self.db.db_name) as conn:
            rows = conn.execute("SELECT symbol FROM tracked_coins").fetchall()
        self.tracked_symbols = {r[0] for r in rows} if rows else {"BTC/USDT", "ETH/USDT"}

    def add_symbol(self, symbol):
        self.tracked_symbols.add(symbol)
        with sqlite3.connect(self.db.db_name) as conn:
            conn.execute("INSERT OR IGNORE INTO tracked_coins (symbol) VALUES (?)", (symbol,))

    def remove_symbol(self, symbol):
        self.tracked_symbols.discard(symbol)
        with sqlite3.connect(self.db.db_name) as conn:
            conn.execute("DELETE FROM tracked_coins WHERE symbol = ?", (symbol,))

    async def process_market_execution(self, symbol, exchange):
        try:
            # Simple placeholder logic for calculation (Aapki geometry logic yahan replace kar sakte hain)
            ohlcv = await exchange.fetch_ohlcv(symbol, '15m', limit=50)
            df = pd.DataFrame(ohlcv, columns=['t', 'o', 'h', 'l', 'c', 'v'])
            
            price = df['c'].iloc[-1]
            # --- Dummy Logic for Signal ---
            confidence = 70.0 # Just for example
            
            if confidence >= 65.0:
                sl = price * 1.02
                tp = price * 0.94
                self.db.log_trade(symbol, "SHORT", price, sl, tp, confidence)
                
                msg = (f"🚨 *V8 SIGNAL TRIGGERED*\n\n"
                       f"Pair: `{symbol}`\n"
                       f"Entry: `{price}`\n"
                       f"SL: `{sl:.2f}`\n"
                       f"TP: `{tp:.2f}`\n"
                       f"Confidence: `{confidence}%`")
                await send_telegram_alert(msg)
                print(f"✅ Signal sent for {symbol}")
        except Exception as e:
            print(f"Error {symbol}: {e}")

    async def engine_core_loop(self):
        exchange_client = ccxt.okx({"enableRateLimit": True})
        print("🚀 Engine Loop Started (1 Minute Interval)")
        try:
            while True:
                symbols = list(self.tracked_symbols)
                for s in symbols:
                    if s in self.tracked_symbols:
                        await self.process_market_execution(s, exchange_client)
                
                await asyncio.sleep(60) # 1 Minute Wait
        finally:
            await exchange_client.close()

# ========================================================
# 5. EXECUTION
# ========================================================
if __name__ == "__main__":
    sentinel_engine = CompleteSentinelEngine()
    
    # Run FastAPI in background
    threading.Thread(target=start_render_health_gateway, daemon=True).start()
    
    # Run Main Loop
    try:
        asyncio.run(sentinel_engine.engine_core_loop())
    except KeyboardInterrupt:
        print("Stopping Engine...")
