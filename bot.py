import os
import pandas as pd
import numpy as np
import ccxt.async_support as ccxt
import asyncio
import sqlite3
import uvicorn
import threading
import requests
from fastapi import FastAPI, Request
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple

# ==========================================
# 0. RENDER HEALTH CHECK & TELEGRAM GATEWAY
# ==========================================
app = FastAPI()

# Global Engine Reference Webhook Core ke liye
engine_instance = None

@app.get("/")
def health_check():
    """Keeps the Render web service container alive."""
    return {
        "status": "ONLINE",
        "engine": "RSI Audited Auto-Loop V17",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

# ---- RENDER ENVIRONMENT VARIABLES FETCH & CLEANING ----
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN") or os.environ.get("API_KEY") 

RAW_CHAT_ID = os.environ.get("CHAT_ID")
# Strict sanitize filter to destroy trailing strings/spaces/falsy objects
if RAW_CHAT_ID and str(RAW_CHAT_ID).strip() not in ["None", "none", "", "null", "False"]:
    ALLOWED_CHAT_ID = str(RAW_CHAT_ID).strip()
else:
    ALLOWED_CHAT_ID = None

RENDER_URL = "https://rsi-and-mfi.onrender.com"  

def send_telegram_msg(chat_id: int, text: str):
    """Sends responses back to Telegram safely with strict audit prints"""
    if not TELEGRAM_TOKEN or not chat_id:
        print(f"⚠️ [TELEGRAM FATAL] Dispatch skipped! Token empty or invalid Chat ID node detected: {chat_id}")
        return
    
    print(f"📡 [DISPATCH SYSTEM] Sending Telegram Message To Chat ID: {chat_id}")
    print(f"📝 [PREVIEW TEXT - FIRST 100 CHARS]:\n{text[:100]}...\n")
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": int(chat_id), "text": text}
    try:
        r = requests.post(url, json=payload)
        print(f"📡 [TELEGRAM API OUTBOUND STATUS CODE] : {r.status_code}")
        print(f"📄 [TELEGRAM API RAW RESPONSE TEXT] : {r.text}")
    except Exception as e:
        print(f"❌ Critical Exception caught inside Telegram channel broadcaster: {e}")

def setup_telegram_webhook():
    """Automated Webhook Registration for Render Architecture"""
    if not TELEGRAM_TOKEN:
        print("⚠️ [TELEGRAM WEBHOOK ERROR] Bot Token missing in Render Configuration Variables!")
        return
    webhook_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook?url={RENDER_URL}/tg-webhook"
    try:
        res = requests.get(webhook_url).json()
        print(f"📡 [TELEGRAM WEBHOOK REGISTRATION STATUS] : {res}")
    except Exception as e:
        print(f"❌ Webhook setups failed: {e}")

@app.post("/tg-webhook")
async def telegram_webhook_handler(request: Request):
    """Listens to direct updates from Telegram servers securely"""
    global engine_instance
    try:
        data = await request.json()
        if "message" not in data:
            return {"status": "ignored"}
            
        message = data["message"]
        chat_id = message["chat"]["id"]
        text = message.get("text", "").strip()

        if ALLOWED_CHAT_ID and str(chat_id) != ALLOWED_CHAT_ID:
            print(f"🔒 [SECURITY] Unauthorized chat blocked: {chat_id}")
            return {"status": "unauthorized"}

        if not text.startswith("/"):
            return {"status": "not a command"}

        parts = text.split(" ")
        command = parts[0].lower()

        if command == "/start":
            welcome_msg = (
                "📊 RSI MATRIX SCANNER V17 BOOTED 📊\n\n"
                "Available Commands:\n"
                "🔹 /add COIN/USDT — Matrix me coin add karein\n"
                "🔹 /remove COIN/USDT — Matrix se coin hatayein\n"
                "🔹 /list — Active tracked matrix coins dekhein\n"
                "🔹 /scan — Instant dynamic fallback matrix update paayein"
            )
            send_telegram_msg(chat_id, welcome_msg)

        elif command == "/add":
            if len(parts) < 2:
                send_telegram_msg(chat_id, "⚠️ Pattern: /add SOL/USDT")
            else:
                coin = parts[1].upper()
                if engine_instance:
                    engine_instance.add_coin(coin)
                    send_telegram_msg(chat_id, f"✅ Asset Matrix Element Added: {coin}")

        elif command == "/remove":
            if len(parts) < 2:
                send_telegram_msg(chat_id, "⚠️ Pattern: /remove ETH/USDT")
            else:
                coin = parts[1].upper()
                if engine_instance:
                    engine_instance.remove_coin(coin)
                    send_telegram_msg(chat_id, f"🗑️ Stopped scanning matrix node: {coin}")

        elif command == "/list":
            if engine_instance:
                with engine_instance.symbol_lock:
                    coins = "\n".join([f"• {c}" for c in engine_instance.tracked_symbols])
                send_telegram_msg(chat_id, f"📋 Active Dynamic Tracking Matrix:\n\n{coins if coins else 'Empty matrix'}")

        elif command == "/scan":
            if engine_instance:
                send_telegram_msg(chat_id, "🔍 Fetching Cross-Exchange Data Pipeline... Please wait.")
                asyncio.create_task(engine_instance.trigger_instant_scan(chat_id))

    except Exception as err:
        print(f"❌ [WEBHOOK CORRUPTION ERROR] : {str(err)}")
    return {"status": "ok"}


# ========================================================
# 2. CORE RSI & MULTI-EXCHANGE ROUTING ENGINE
# ========================================================
class CompleteSentinelEngine:
    def __init__(self):
        self.tracked_symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BB/USDT", "LAB/USDT", "BEAT/USDT"] 
        self.symbol_lock = threading.Lock() 
        self.timeframes = ['5m', '15m', '1h', '4h']

    def add_coin(self, symbol: str):
        symbol = symbol.upper().strip()
        with self.symbol_lock:
            if symbol not in self.tracked_symbols:
                self.tracked_symbols.append(symbol)

    def remove_coin(self, symbol: str):
        symbol = symbol.upper().strip()
        with self.symbol_lock:
            if symbol in self.tracked_symbols:
                self.tracked_symbols.remove(symbol)

    def calculate_rsi(self, closes: np.ndarray, symbol: str, tf: str) -> float:
        """Pure Wilder's RSI Array Core Calculation"""
        closes = np.asarray(closes, dtype=float)
        if len(closes) < 15:
            return 50.0

        delta = np.diff(closes)
        gain = np.where(delta > 0, delta, 0.0)
        loss = np.where(delta < 0, -delta, 0.0)

        avg_gain = np.mean(gain[:14])
        avg_loss = np.mean(loss[:14])

        for i in range(14, len(gain)):
            avg_gain = ((avg_gain * 13) + gain[i]) / 14
            avg_loss = ((avg_loss * 13) + loss[i]) / 14

        if avg_loss == 0:
            return 100.0

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return round(float(rsi), 2)

    def get_rsi_status(self, rsi_val: float) -> str:
        """Standard Boundary Matrix Labels"""
        if rsi_val >= 70.0:
            return f"{rsi_val:.1f}  🔴 OB"
        elif rsi_val <= 30.0:
            return f"{rsi_val:.1f}  🟢 OS"
        else:
            return f"{rsi_val:.1f}  ⚪ MID"

    async def fetch_exchange_data(self, symbol: str, gate_client, okx_client) -> Tuple[Optional[float], Optional[str], Optional[object], str]:
        """CCXT Router: Gate Future (:USDT) -> OKX Future (:USDT) -> Gate Spot -> OKX Spot"""
        # 1. GATE FUTURE
        try:
            gate_future_symbol = f"{symbol}:USDT" if not symbol.endswith(":USDT") else symbol
            ticker = await gate_client['future'].fetch_ticker(gate_future_symbol)
            if ticker and ticker.get('last'):
                return float(ticker['last']), "Gate (FUTURE)", gate_client['future'], gate_future_symbol
        except Exception:
            pass

        # 2. OKX FUTURE
        try:
            okx_future_symbol = f"{symbol}:USDT" if not symbol.endswith(":USDT") else symbol
            ticker = await okx_client['future'].fetch_ticker(okx_future_symbol)
            if ticker and ticker.get('last'):
                return float(ticker['last']), "OKX (FUTURE)", okx_client['future'], okx_future_symbol
        except Exception:
            pass

        # 3. GATE SPOT
        try:
            gate_spot_symbol = symbol
            ticker = await gate_client['spot'].fetch_ticker(gate_spot_symbol)
            if ticker and ticker.get('last'):
                return float(ticker['last']), "Gate (SPOT)", gate_client['spot'], gate_spot_symbol
        except Exception:
            pass

        # 4. OKX SPOT
        try:
            okx_spot_symbol = symbol
            ticker = await okx_client['spot'].fetch_ticker(okx_spot_symbol)
            if ticker and ticker.get('last'):
                return float(ticker['last']), "OKX (SPOT)", okx_client['spot'], okx_spot_symbol
        except Exception:
            pass

        return None, None, None, symbol

    async def process_single_symbol_report(self, symbol: str, gate_client, okx_client) -> Optional[str]:
        """Generates the text output block for a single tracking symbol"""
        current_price, source_label, resolved_client, market_symbol = await self.fetch_exchange_data(symbol, gate_client, okx_client)
        
        if not current_price or not resolved_client:
            print(f"❌ [SYMBOL FAIL MONITOR] FAILED SYMBOL => {symbol} (Could not resolve pricing on any pipeline node)")
            return None

        report = f"📊 RSI UPDATE: {symbol}\n"
        report += f"💵 Price: ${current_price:,.4f}\n"
        report += f"🏛 Source: {source_label}\n"
        report += f"───────────────────\n"
        report += f"TF    │ RSI (14)\n"
        report += f"───────────────────\n"
        
        for tf in self.timeframes:
            try:
                ohlcv = await resolved_client.fetch_ohlcv(market_symbol, tf, limit=100)
                if not ohlcv or len(ohlcv) < 15:
                    report += f"{tf:<5} │ --.-  ⚪ MID\n"
                    continue

                closes = np.array([float(x[4]) for x in ohlcv])
                rsi_value = self.calculate_rsi(closes, symbol, tf)
                status_str = self.get_rsi_status(rsi_value)
                report += f"{tf:<5} │ {status_str}\n"
            except Exception:
                report += f"{tf:<5} │ --.-  ⚪ MID\n"
                
        report += f"───────────────────\n"
        return report

    async def trigger_instant_scan(self, chat_id: int):
        """On-Demand processing via Telegram commands"""
        gate_client = {
            'future': ccxt.gateio({"options": {"defaultType": "swap"}, "enableRateLimit": True}),
            'spot': ccxt.gateio({"options": {"defaultType": "spot"}, "enableRateLimit": True})
        }
        okx_client = {
            'future': ccxt.okx({"options": {"defaultType": "swap"}, "enableRateLimit": True}),
            'spot': ccxt.okx({"options": {"defaultType": "spot"}, "enableRateLimit": True})
        }
        
        try:
            with self.symbol_lock:
                symbols = self.tracked_symbols.copy()
            
            for symbol in symbols:
                report = await self.process_single_symbol_report(symbol, gate_client, okx_client)
                if report:
                    send_telegram_msg(chat_id, report.strip())
        finally:
            await gate_client['future'].close()
            await gate_client['spot'].close()
            await okx_client['future'].close()
            await okx_client['spot'].close()

    async def engine_core_loop(self):
        """Continuous Automated 2-Minute Engine Core Loop with Strict Auditing Logs"""
        # FASTEST BOOT TEST TRIGGER POINT
        print("⚡ [STARTUP AUDIT] AUTO LOOP ONLINE - Verifying initialization vectors...")
        print(f"🛠️ [ENV VARIABLES DUMP] CHAT_ID = {ALLOWED_CHAT_ID} (Raw input from panel: {RAW_CHAT_ID})")
        
        if ALLOWED_CHAT_ID:
            print("🚀 [STARTUP AUDIT] Dispatching immediate boot verification message to secure target channel...")
            send_telegram_msg(int(ALLOWED_CHAT_ID), "✅ *AUTO LOOP STARTED SUCCESSFULLY*\nYour 120-second automated matrix sync engine is now strictly operational.")
        
        while True:
            # Explicit Timestamp & Audit Logs inside iteration loop boundaries
            print(f"🔄 [LOOP LOG] LOOP STARTED at Time Context: {datetime.now(timezone.utc)}")
            print(f"🔑 [LOOP LOG] Tracking Variables Verified -> CHAT_ID = {ALLOWED_CHAT_ID}")
            
            try:
                if ALLOWED_CHAT_ID:
                    target_int_id = int(ALLOWED_CHAT_ID)
                    
                    gate_client = {
                        'future': ccxt.gateio({"options": {"defaultType": "swap"}, "enableRateLimit": True}),
                        'spot': ccxt.gateio({"options": {"defaultType": "spot"}, "enableRateLimit": True})
                    }
                    okx_client = {
                        'future': ccxt.okx({"options": {"defaultType": "swap"}, "enableRateLimit": True}),
                        'spot': ccxt.okx({"options": {"defaultType": "spot"}, "enableRateLimit": True})
                    }
                    
                    try:
                        with self.symbol_lock:
                            symbols = self.tracked_symbols.copy()
                        
                        print(f"📋 [LOOP LOG] Target assets array checklist: {symbols}")
                        for symbol in symbols:
                            report = await self.process_single_symbol_report(symbol, gate_client, okx_client)
                            if report:
                                send_telegram_msg(target_int_id, report.strip())
                    finally:
                        await gate_client['future'].close()
                        await gate_client['spot'].close()
                        await okx_client['future'].close()
                        await okx_client['spot'].close()
                else:
                    print("⚠️ [LOOP LOG SKIP] Auto loop skipped broadcasting because CHAT_ID env variable is evaluated as None/Falsy object.")
                        
            except Exception as loop_err:
                print(f"💥 Internal loop execution fault: {str(loop_err)}")
                
            print("⏳ [LOOP LOG] Entering 120 second precision sleep block sequence...")
            await asyncio.sleep(120) 


# ==========================================
# 4. MAIN ASYNC PROCESS LAUNCHER
# ==========================================
async def start_combined_services():
    """Runs FastAPI Server and Trading Loop concurrently on a unified event loop"""
    global engine_instance
    engine_instance = CompleteSentinelEngine()
    setup_telegram_webhook()

    config = uvicorn.Config(app, host="0.0.0.0", port=10000, log_level="warning")
    server = uvicorn.Server(config)

    print("🚀 [LAUNCHER] Booting FastAPI Server and Trading Loop in parallel state...")
    await asyncio.gather(
        server.serve(),
        engine_instance.engine_core_loop()
    )


if __name__ == "__main__":
    # Standard top-level execution loop block mapping
    asyncio.run(start_combined_services())
