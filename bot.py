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
        "engine": "RSI Dual-Exchange Matrix Scanner V10",
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
                "📊 RSI MATRIX SCANNER V10 BOOTED 📊\n\n"
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

def start_render_health_gateway():
    """Runs Uvicorn server on a background thread to satisfy Render's port binding."""
    setup_telegram_webhook()
    uvicorn.run(app, host="0.0.0.0", port=10000, log_level="warning")


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

    def calculate_rsi(self, df_ohlcv: List, period: int = 14) -> Optional[float]:
        """Robust Market Parsing Architecture supporting structural mutations across CCXT networks"""
        try:
            if not df_ohlcv or len(df_ohlcv) < period + 2:
                return None
                
            # Pandas integration for guaranteed index sequencing alignment
            df = pd.DataFrame(df_ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
            
            # Data cleansing to ensure smooth calculation profiles
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            df = df.dropna(subset=['close'])
            
            if len(df) < period + 1:
                return None

            closes = df['close'].values
            delta = np.diff(closes)
            
            # Wilder's Smoothing Moving Average Logic Initialization
            gains = np.where(delta > 0, delta, 0.0)
            losses = np.where(delta < 0, -delta, 0.0)
            
            avg_gain = np.mean(gains[:period])
            avg_loss = np.mean(losses[:period])
            
            for i in range(period, len(delta)):
                avg_gain = (avg_gain * (period - 1) + gains[i]) / period
                avg_loss = (avg_loss * (period - 1) + losses[i]) / period
                
            if avg_loss == 0:
                return 100.0 if avg_gain > 0 else 50.0
                
            rs = avg_gain / avg_loss
            rsi_val = 100.0 - (100.0 / (100.0 + rs))
            
            if np.isnan(rsi_val) or np.isinf(rsi_val):
                return None
                
            return float(rsi_val)
        except Exception:
            return None

    def get_rsi_status(self, rsi_val: Optional[float]) -> str:
        """Standard Boundary Matrix Labels with explicit failover handling"""
        if rsi_val is None:
            return "--.-  ⚪ MID"
        if rsi_val >= 70.0:
            return f"{rsi_val:.1f}  🔴 OB"
        elif rsi_val <= 30.0:
            return f"{rsi_val:.1f}  🟢 OS"
        else:
            return f"{rsi_val:.1f}  ⚪ MID"

    async def fetch_exchange_data(self, symbol: str, gate_client, okx_client) -> Tuple[Optional[float], Optional[str], Optional[object], str]:
        """Prioritizes: Gate Future -> OKX Future -> Gate Spot -> OKX Spot"""
        # 1. GATE FUTURE
        try:
            gate_future_symbol = symbol.replace("/", "_")
            ticker = await gate_client['future'].fetch_ticker(gate_future_symbol)
            if ticker and ticker.get('last'):
                return float(ticker['last']), "Gate (FUTURE)", gate_client['future'], gate_future_symbol
        except Exception:
            pass

        # 2. OKX FUTURE
        try:
            okx_future_symbol = symbol + ":USDT"
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
            return None

        report = f"📊 RSI UPDATE: {symbol}\n"
        report += f"💵 Price: ${current_price:,.4f}\n"
        report += f"🏛 Source: {source_label}\n"
        report += f"───────────────────\n"
        report += f"TF    │ RSI (14)\n"
        report += f"───────────────────\n"
        
        for tf in self.timeframes:
            try:
                # Limit sets to 150 candles for highly accurate Wilder exponential weight seeds
                ohlcv = await resolved_client.fetch_ohlcv(market_symbol, tf, limit=150)
                rsi_value = self.calculate_rsi(ohlcv)
                status_str = self.get_rsi_status(rsi_value)
                report += f"{tf:<5} │ {status_str}\n"
            except Exception:
                report += f"{tf:<5} │ --.-  ⚪ MID\n"
                
        report += f"───────────────────\n"
        return report

    async def trigger_instant_scan(self, chat_id: int):
        """On-Demand trigger for processing pipeline data via Telegram commands"""
        gate_client = {
            'future': ccxt.gate({"options": {"defaultType": "swap"}, "enableRateLimit": True}),
            'spot': ccxt.gate({"options": {"defaultType": "spot"}, "enableRateLimit": True})
        }
        okx_client = {
            'future': ccxt.okx({"options": {"defaultType": "swap"}, "enableRateLimit": True}),
            'spot': ccxt.okx({"options": {"defaultType": "spot"}, "enableRateLimit": True})
        }
        
        try:
            with self.symbol_lock:
                symbols = self.tracked_symbols.copy()
            
            final_report = ""
            for symbol in symbols:
                report = await self.process_single_symbol_report(symbol, gate_client, okx_client)
                if report:
                    final_report += report + "\n"
            
            if final_report:
                send_telegram_msg(chat_id, final_report.strip())
            else:
                send_telegram_msg(chat_id, "❌ Verification across both exchanges failed. Validate matrix asset tickers.")
        finally:
            await gate_client['future'].close()
            await gate_client['spot'].close()
            await okx_client['future'].close()
            await okx_client['spot'].close()

    async def engine_core_loop(self):
        """Continuous Automated 2-Minute Dynamic Matrix System Broadcaster"""
        print("\n🔥 SYSTEM ACTIVE: FALLBACK STRUCTURE INTEGRATED FOR GATE & OKX (2M TICK) 🔥\n")
        
        while True:
            try:
                if ALLOWED_CHAT_ID:
                    gate_client = {
                        'future': ccxt.gate({"options": {"defaultType": "swap"}, "enableRateLimit": True}),
                        'spot': ccxt.gate({"options": {"defaultType": "spot"}, "enableRateLimit": True})
                    }
                    okx_client = {
                        'future': ccxt.okx({"options": {"defaultType": "swap"}, "enableRateLimit": True}),
                        'spot': ccxt.okx({"options": {"defaultType": "spot"}, "enableRateLimit": True})
                    }
                    
                    try:
                        with self.symbol_lock:
                            symbols = self.tracked_symbols.copy()
                        
                        final_report = ""
                        for symbol in symbols:
                            report = await self.process_single_symbol_report(symbol, gate_client, okx_client)
                            if report:
                                final_report += report + "\n"
                        
                        if final_report:
                            send_telegram_msg(int(ALLOWED_CHAT_ID), final_report.strip())
                    finally:
                        await gate_client['future'].close()
                        await gate_client['spot'].close()
                        await okx_client['future'].close()
                        await okx_client['spot'].close()
                        
            except Exception as loop_err:
                print(f"💥 Internal loop execution fault: {str(loop_err)}")
                
            await asyncio.sleep(120) # Pure 2 Minutes Auto-Update sequence

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
