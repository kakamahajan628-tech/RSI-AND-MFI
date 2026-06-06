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
        "engine": "RSI-MFI Matrix Scanner V8",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

# ---- RENDER ENVIRONMENT VARIABLES FETCH ----
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN") or os.environ.get("API_KEY") 
ALLOWED_CHAT_ID = os.environ.get("CHAT_ID")
RENDER_URL = "https://rsi-and-mfi.onrender.com"  

def setup_telegram_webhook():
    """Automated Webhook Registration for Render Architecture"""
    if not TELEGRAM_TOKEN:
        print("⚠️ [TELEGRAM ERROR] Bot Token missing in Render Env Variables!")
        return
    webhook_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook?url={RENDER_URL}/tg-webhook"
    try:
        res = requests.get(webhook_url).json()
        print(f"📡 [TELEGRAM WEBHOOK STATUS] : {res}")
    except Exception as e:
        print(f"❌ Webhook setups failed: {e}")

def send_telegram_msg(chat_id: int, text: str):
    """Sends responses back to Telegram User Node"""
    if not TELEGRAM_TOKEN:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    try:
        requests.post(url, json=payload)
    except Exception as e:
        print(f"❌ Failed to send telegram message: {e}")

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

        if ALLOWED_CHAT_ID and str(chat_id) != str(ALLOWED_CHAT_ID):
            print(f"🔒 [SECURITY] Unauthorized chat blocked: {chat_id}")
            return {"status": "unauthorized"}

        if not text.startswith("/"):
            return {"status": "not a command"}

        parts = text.split(" ")
        command = parts[0].lower()

        if command == "/start":
            welcome_msg = (
                "📊 RSI & MFI MATRIX SCANNER BOOTED 📊\n\n"
                "Available Commands:\n"
                "🔹 /add COIN/USDT — Matrix me coin add karein\n"
                "🔹 /remove COIN/USDT — Matrix se coin hatayein\n"
                "🔹 /list — Active tracked matrix coins dekhein\n"
                "🔹 /scan — Instant pure matrix ka RSI update paayein"
            )
            send_telegram_msg(chat_id, welcome_msg)

        elif command == "/add":
            if len(parts) < 2:
                send_telegram_msg(chat_id, "⚠️ Pattern: /add SOL/USDT")
            else:
                coin = parts[1].upper()
                if engine_instance:
                    engine_instance.add_coin(coin)
                    send_telegram_msg(chat_id, f"✅ Scalper Activated for: {coin}")

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
                send_telegram_msg(chat_id, "🔍 Fetching Gate.io Futures Matrix Pipeline... Please wait.")
                asyncio.create_task(engine_instance.trigger_instant_scan(chat_id))

    except Exception as err:
        print(f"❌ [WEBHOOK CORRUPTION ERROR] : {str(err)}")
    return {"status": "ok"}

def start_render_health_gateway():
    """Runs Uvicorn server on a background thread to satisfy Render's port binding."""
    setup_telegram_webhook()
    uvicorn.run(app, host="0.0.0.0", port=10000, log_level="warning")


# ========================================================
# 2. CORE RSI & PRICE CALCULATION ENGINE
# ========================================================
class CompleteSentinelEngine:
    def __init__(self):
        self.tracked_symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT"] 
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

    def calculate_rsi(self, prices, period=14) -> float:
        """Standard Technical RSI Formula"""
        if len(prices) < period + 1:
            return 50.0
        delta = np.diff(prices)
        gain = np.where(delta > 0, delta, 0.0)
        loss = np.where(delta < 0, -delta, 0.0)
        
        avg_gain = np.mean(gain[:period])
        avg_loss = np.mean(loss[:period])
        
        for i in range(period, len(delta)):
            avg_gain = (avg_gain * (period - 1) + gain[i]) / period
            avg_loss = (avg_loss * (period - 1) + loss[i]) / period
            
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return float(100.0 - (100.0 / (100.0 + rs)))

    def get_rsi_status(self, rsi_val: float) -> str:
        """Returns status flag strings exactly as expected"""
        if rsi_val >= 70.0:
            return f"{rsi_val:.1f}  🔴 OB"
        elif rsi_val <= 30.0:
            return f"{rsi_val:.1f}  🟢 OS"
        else:
            return f"{rsi_val:.1f}  ⚪ MID"

    async def fetch_asset_matrix_report(self, symbol: str, exchange: ccxt.Exchange) -> Optional[str]:
        """Fetches full multi-timeframe state profile for a coin"""
        try:
            # Gate.io futures format compatibility (e.g., SOL/USDT -> SOL_USDT)
            gate_symbol = symbol.replace("/", "_")
            
            # Current Price Engine
            ticker = await exchange.fetch_ticker(gate_symbol)
            current_price = ticker.get('last', 0.0)
            
            report = f"📊 RSI UPDATE: {symbol}\n"
            report += f"💵 Price: ${current_price:,.4f}\n"
            report += f"🏛 Source: Gate (FUTURE)\n"
            report += f"───────────────────\n"
            report += f"TF    │ RSI (14)\n"
            report += f"───────────────────\n"
            
            for tf in self.timeframes:
                ohlcv = await exchange.fetch_ohlcv(gate_symbol, tf, limit=50)
                if len(ohlcv) < 20:
                    report += f"{tf:<5} │ --.0  ⚪ MID\n"
                    continue
                closes = np.array([x[4] for x in ohlcv])
                rsi_value = self.calculate_rsi(closes)
                status_str = self.get_rsi_status(rsi_value)
                report += f"{tf:<5} │ {status_str}\n"
                
            report += f"───────────────────\n"
            return report
        except Exception as e:
            print(f"❌ Error fetching analytics for {symbol}: {e}")
            return None

    async def trigger_instant_scan(self, chat_id: int):
        """Sends immediate status metrics to client user on demand"""
        exchange_client = ccxt.gate({"options": {"defaultType": "swap"}, "enableRateLimit": True})
        try:
            with self.symbol_lock:
                symbols = self.tracked_symbols.copy()
            
            final_report = ""
            for symbol in symbols:
                report = await self.fetch_asset_matrix_report(symbol, exchange_client)
                if report:
                    final_report += report + "\n"
            
            if final_report:
                send_telegram_msg(chat_id, final_report.strip())
            else:
                send_telegram_msg(chat_id, "❌ Verification stream failed. Check if asset names match Gate.io syntax.")
        finally:
            await exchange_client.close()

    async def engine_core_loop(self):
        """Automated 5-Minute Continuous Matrix Broadcaster"""
        print("\n🔥 RSI-MFI SCANNER ACTIVE ON GATE.IO FUTURES API 🔥\n")
        exchange_client = ccxt.gate({"options": {"defaultType": "swap"}, "enableRateLimit": True})
        
        try:
            while True:
                # Agar automatic 5-min me alerts bhejne hain toh CHAT_ID variables configured hona chahiye
                if ALLOWED_CHAT_ID:
                    with self.symbol_lock:
                        symbols = self.tracked_symbols.copy()
                    
                    final_report = ""
                    for symbol in symbols:
                        report = await self.fetch_asset_matrix_report(symbol, exchange_client)
                        if report:
                            final_report += report + "\n"
                    
                    if final_report:
                        send_telegram_msg(int(ALLOWED_CHAT_ID), final_report.strip())
                
                await asyncio.sleep(300) # Automated 5-minute tracking check
        except Exception as loop_err:
            print(f"💥 Critical Loop Error: {str(loop_err)}")
        finally:
            await exchange_client.close()

# ========================================================
# 5. ENTRY POINT PROCESS ROUTER
# ========================================================
if __name__ == "__main__":
    # Step 1: Initialize Engine System globally 
    engine_instance = CompleteSentinelEngine()

    # Step 2: Start Render Server along with Telegram Webhook on Daemon Thread
    server_thread = threading.Thread(target=start_render_health_gateway, daemon=True)
    server_thread.start()
    
    # Step 3: Run Quant Trading Loop
    asyncio.run(engine_instance.engine_core_loop())
