import os
import asyncio
import logging
import sys
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
import ccxt
import ta
import pandas as pd
from threading import Thread
from flask import Flask

# Logging setup to track everything in Render console
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    stream=sys.stdout
)

# Render ke environment variables se values read ho rahi hain
TOKEN = os.getenv("TELEGRAM_TOKEN")
raw_chat_id = os.getenv("USER_CHAT_ID")

# Chat ID ko safely integer mein convert karne ke liye
USER_CHAT_ID = int(raw_chat_id) if (raw_chat_id and raw_chat_id.strip().isdigit()) else None

# Initialize only Gate.io safely (Binance/Bybit removed due to geoblocking)
EXCHANGES = []
try:
    EXCHANGES.append(ccxt.gateio({'enableRateLimit': True}))
    logging.info("Gate.io exchange initialized successfully.")
except Exception as e:
    logging.error(f"Error initializing Gate.io: {e}")

TRACKED_PAIRS = {}
TIMEFRAMES = ['5m', '15m', '1h', '4h']

def fetch_ohlcv_with_fallback(symbol, timeframe, limit=50):
    for exchange in EXCHANGES:
        try:
            market_symbol = symbol.upper()
            ohlcv = exchange.fetch_ohlcv(market_symbol, timeframe, limit=limit)
            if ohlcv and len(ohlcv) >= 14:
                return ohlcv, exchange.name
        except Exception as e:
            logging.warning(f"Failed fetching {symbol} from {exchange.name}: {e}")
            continue
    return None, None

def calculate_indicators(ohlcv_data):
    try:
        df = pd.DataFrame(ohlcv_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['RSI'] = ta.momentum.rsi(close=df['close'], window=14)
        df['MFI'] = ta.volume.money_flow_index(high=df['high'], low=df['low'], close=df['close'], volume=df['volume'], window=14)
        return df.iloc[-1]
    except Exception as e:
        logging.error(f"Indicator calculation error: {e}")
        return None

# Startup Signal Function
async def send_startup_message(application: Application):
    if USER_CHAT_ID:
        try:
            await asyncio.sleep(3) # Stable connection ke liye thoda pause
            await application.bot.send_message(
                chat_id=USER_CHAT_ID,
                text="🚀 *Bot started successfully with Gate.io!* Ready to track RSI and MFI matrix.\nUse `/track COIN/USDT` to start.",
                parse_mode="Markdown"
            )
            logging.info(f"Startup message sent to chat ID: {USER_CHAT_ID}")
        except Exception as e:
            logging.error(f"Could not send startup message: {e}")
    else:
        logging.warning("USER_CHAT_ID missing or invalid in Render dashboard.")

# Telegram Command Handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "⚡ *RSI/MFI Matrix Tracker Active (Gate.io)*\n\n"
        "Commands:\n"
        "`/track BTC/USDT` - Start tracking a pair\n"
        "`/stop BTC/USDT` - Stop tracking a pair\n"
        "`/status` - Show active tracks", 
        parse_mode="Markdown"
    )

async def track_coin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("❌ Please specify a pair. Example: `/track BTC/USDT`")
        return
    symbol = context.args[0].upper()
    if chat_id not in TRACKED_PAIRS: 
        TRACKED_PAIRS[chat_id] = set()
    TRACKED_PAIRS[chat_id].add(symbol)
    logging.info(f"Started tracking {symbol} for chat {chat_id}")
    await update.message.reply_text(f"✅ Now tracking *{symbol}* on Gate.io across 5m, 15m, 1h, and 4h combined matrix.", parse_mode="Markdown")

async def stop_coin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args: 
        await update.message.reply_text("❌ Please specify a pair. Example: `/stop BTC/USDT`")
        return
    symbol = context.args[0].upper()
    if chat_id in TRACKED_PAIRS and symbol in TRACKED_PAIRS[chat_id]:
        TRACKED_PAIRS[chat_id].remove(symbol)
        await update.message.reply_text(f"🛑 Stopped tracking *{symbol}*.", parse_mode="Markdown")
    else:
        await update.message.reply_text(f"❌ You are not tracking {symbol}.")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    pairs = TRACKED_PAIRS.get(chat_id, set())
    if not pairs: 
        await update.message.reply_text("You are currently tracking 0 assets.")
    else: 
        await update.message.reply_text(f"📋 *Currently Tracking (Gate.io):*\n" + "\n".join([f"• {p}" for p in pairs]), parse_mode="Markdown")

# Background Monitoring Loop
async def monitoring_job(application: Application):
    logging.info("Background tracking core loop running every 60s...")
    while True:
        await asyncio.sleep(60)
        for chat_id, pairs in list(TRACKED_PAIRS.items()):
            for symbol in list(pairs):
                timeframe_data = {}
                trigger_alert = False
                last_price = 0.0
                detected_source = "Unknown"

                for tf in TIMEFRAMES:
                    try:
                        ohlcv, used_exchange = fetch_ohlcv_with_fallback(symbol, tf)
                        if ohlcv is None: 
                            continue
                        last_candle = calculate_indicators(ohlcv)
                        if last_candle is None:
                            continue
                        
                        rsi_val = last_candle['RSI']
                        mfi_val = last_candle['MFI']
                        price = last_candle['close']
                        
                        if pd.isna(rsi_val) or pd.isna(mfi_val):
                            continue

                        last_price = price
                        detected_source = used_exchange
                        timeframe_data[tf] = (rsi_val, mfi_val)

                        # Trigger condition check
                        if rsi_val <= 30 or rsi_val >= 70 or mfi_val <= 20 or mfi_val >= 80:
                            trigger_alert = True
                    except Exception as inner_err:
                        logging.error(f"Error fetching/calculating for {symbol} on {tf}: {inner_err}")

                # Alert send logic
                if trigger_alert and timeframe_data:
                    msg = f"🚨 *MARKET METRIC SCAN: {symbol}*\n"
                    msg += f"• *Price:* ${last_price:,.4f}\n"
                    msg += f"• *Source:* {detected_source}\n"
                    msg += "▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
                    msg += "`TF    │ RSI (14) │ MFI (14)`\n"
                    msg += "────────────────────\n"
                    
                    for tf in TIMEFRAMES:
                        if tf in timeframe_data:
                            rsi, mfi = timeframe_data[tf]
                            
                            rsi_alert = "⚠️" if (rsi <= 30 or rsi >= 70) else "  "
                            mfi_alert = "🚨" if (mfi <= 20 or mfi >= 80) else "  "
                            
                            msg += f"`{tf:<5}│ {rsi:<8.2f}{rsi_alert}│ {mfi:<8.2f}{mfi_alert}`\n"
                    
                    msg += "────────────────────\n"
                    msg += "*Status: Active threshold cross detected.*"
                    
                    try:
                        await application.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")
                    except Exception as send_err:
                        logging.error(f"Failed to send combined matrix alert: {send_err}")

# Web Server for Render Keep-Alive
app = Flask(__name__)
@app.route('/')
def health_check(): 
    return "Bot is Alive and Running", 200

def run_web_server():
    port = int(os.environ.get("PORT", 5000))
    logging.info(f"Starting web server on port {port}...")
    app.run(host='0.0.0.0', port=port)

def main():
    if not TOKEN:
        logging.error("CRITICAL: TELEGRAM_TOKEN variable is missing in Render Environment.")
        sys.exit(1)
        
    logging.info("Initializing Telegram bot framework...")
    
    # Run Web Server thread
    Thread(target=run_web_server, daemon=True).start()
    
    # Build Telegram App
    application = Application.builder().token(TOKEN).build()
    
    # Handlers registration
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("track", track_coin))
    application.add_handler(CommandHandler("stop", stop_coin))
    application.add_handler(CommandHandler("status", status))
    
    # Tasks integration
    loop = asyncio.get_event_loop()
    loop.create_task(send_startup_message(application))
    loop.create_task(monitoring_job(application))
    
    logging.info("Polling loop activated. Listening for commands...")
    application.run_polling()

if __name__ == '__main__':
    main()
