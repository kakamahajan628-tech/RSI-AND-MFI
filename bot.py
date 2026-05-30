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

TOKEN = os.getenv("TELEGRAM_TOKEN")
USER_CHAT_ID = os.getenv("USER_CHAT_ID")  # Aapki chat ID jahan startup message jayega

# Initialize exchanges safely
EXCHANGES = []
try:
    EXCHANGES.append(ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}}))
    EXCHANGES.append(ccxt.bybit({'enableRateLimit': True}))
    EXCHANGES.append(ccxt.gateio({'enableRateLimit': True}))
except Exception as e:
    logging.error(f"Error initializing exchanges: {e}")

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
    """Bot start hote hi aapko personal message bhejega"""
    if USER_CHAT_ID:
        try:
            await asyncio.sleep(2) # Thoda wait taaki connection stable ho jaye
            await application.bot.send_message(
                chat_id=USER_CHAT_ID,
                text="🚀 *Bot started successfully!* Ready to track RSI and MFI.\nUse `/track COIN/USDT` to start.",
                parse_mode="Markdown"
            )
            logging.info(f"Startup message sent successfully to chat ID: {USER_CHAT_ID}")
        except Exception as e:
            logging.error(f"Could not send startup message: {e}. Make sure you have started the bot in Telegram.")
    else:
        logging.warning("USER_CHAT_ID missing in environment variables. Skipping startup message.")

# Telegram Command Handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "⚡ *RSI/MFI Tracker Active*\n\n"
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
    await update.message.reply_text(f"✅ Now tracking *{symbol}* across 5m, 15m, 1h, and 4h intervals.", parse_mode="Markdown")

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
        await update.message.reply_text(f"📋 *Currently Tracking:*\n" + "\n".join([f"• {p}" for p in pairs]), parse_mode="Markdown")

# Background Monitoring Loop
async def monitoring_job(application: Application):
    logging.info("Background tracking core loop running every 60s...")
    while True:
        await asyncio.sleep(60)
        for chat_id, pairs in list(TRACKED_PAIRS.items()):
            for symbol in list(pairs):
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

                        # ALERT CONDITIONS (RSI <= 30 or >= 70 | MFI <= 20 or >= 80)
                        if rsi_val <= 30 or rsi_val >= 70 or mfi_val <= 20 or mfi_val >= 80:
                            msg = (
                                f"🚨 *INDICATOR ALERT: {symbol}*\n"
                                f"• *Timeframe:* {tf}\n"
                                f"• *Price:* ${price:,.2f}\n"
                                f"• *Source:* {used_exchange}\n\n"
                                f"📈 *RSI (14):* {rsi_val:.2f}\n"
                                f"🧪 *MFI (14):* {mfi_val:.2f}"
                            )
                            await application.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")
                    except Exception as loop_err:
                        logging.error(f"Error inside loop instance for {symbol}: {loop_err}")

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
        logging.error("CRITICAL: TELEGRAM_TOKEN variable missing completely.")
        sys.exit(1)
        
    logging.info("Initializing Telegram bot framework...")
    
    # Run Web Server thread
    Thread(target=run_web_server, daemon=True).start()
    
    # Build Telegram App
    application = Application.builder().token(TOKEN).build()
    
    # Command Handlers registration
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("track", track_coin))
    application.add_handler(CommandHandler("stop", stop_coin))
    application.add_handler(CommandHandler("status", status))
    
    # Start startup message task & background checker loop inside asyncio
    loop = asyncio.get_event_loop()
    loop.create_task(send_startup_message(application))
    loop.create_task(monitoring_job(application))
    
    logging.info("Polling loop activated. Listening for commands...")
    application.run_polling()

if __name__ == '__main__':
    main()
