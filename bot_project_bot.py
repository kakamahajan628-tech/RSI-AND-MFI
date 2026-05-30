import os
import asyncio
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
import ccxt
import ta
import pandas as pd
from threading import Thread
from flask import Flask

# Setup logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# ----------------- CONFIG & INITIALIZATION -----------------
TOKEN = os.getenv("TELEGRAM_TOKEN")

# Initialize multiple exchanges for fallback handling
EXCHANGES = [
    ccxt.binance({'enableRateLimit': True}),
    ccxt.bybit({'enableRateLimit': True}),
    ccxt.gateio({'enableRateLimit': True})
]

# In-memory dictionary tracking user pairs: { chat_id: set(['BTC/USDT', 'ETH/USDT']) }
TRACKED_PAIRS = {}
TIMEFRAMES = ['5m', '15m', '1h', '4h']

# ----------------- EXCHANGE FALLBACK CORE -----------------
def fetch_ohlcv_with_fallback(symbol, timeframe, limit=50):
    """Loops through available exchanges if the previous one fails."""
    for exchange in EXCHANGES:
        try:
            market_symbol = symbol.upper()
            ohlcv = exchange.fetch_ohlcv(market_symbol, timeframe, limit=limit)
            if ohlcv:
                return ohlcv, exchange.name
        except Exception as e:
            logging.warning(f"Failed fetching {symbol} from {exchange.name}: {e}. Trying next exchange...")
            continue
    return None, None

def calculate_indicators(ohlcv_data):
    """Calculates 14-period RSI and MFI from raw exchange arrays."""
    df = pd.DataFrame(ohlcv_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['RSI'] = ta.momentum.rsi(close=df['close'], window=14)
    df['MFI'] = ta.volume.money_flow_index(
        high=df['high'], low=df['low'], close=df['close'], volume=df['volume'], window=14
    )
    return df.iloc[-1]  # Return the most recent completed candle data

# ----------------- TELEGRAM COMMAND HANDLERS -----------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "⚡ *RSI/MFI Tracker Active*\n\n"
        "Commands:\n"
        "`/track BTC/USDT` - Start tracking a pair\n"
        "`/stop BTC/USDT` - Stop tracking a pair\n"
        "`/status` - Show active tracks", parse_mode="Markdown"
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
    await update.message.reply_text(f"✅ Now tracking *{symbol}* across 5m, 15m, 1h, and 4h intervals.", parse_mode="Markdown")

async def stop_coin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("❌ Please specify a pair to stop. Example: `/stop BTC/USDT`")
        return
        
    symbol = context.args[0].upper()
    if chat_id in TRACKED_PAIRS and symbol in TRACKED_PAIRS[chat_id]:
        TRACKED_PAIRS[chat_id].remove(symbol)
        await update.message.reply_text(f"🛑 Stopped tracking *{symbol}*.", parse_mode="Markdown")
    else:
        await update.message.reply_text(f"❌ You aren't tracking {symbol}.")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    pairs = TRACKED_PAIRS.get(chat_id, set())
    if not pairs:
        await update.message.reply_text("You are currently tracking 0 assets.")
    else:
        await update.message.reply_text(f"📋 *Currently Tracking:*\n" + "\n".join([f"• {p}" for p in pairs]), parse_mode="Markdown")

# ----------------- BACKGROUND MONITORING LOOP -----------------
async def monitoring_job(application: Application):
    """The permanent 1-minute check interval engine."""
    while True:
        await asyncio.sleep(60) # Wait exactly 60 seconds
        
        for chat_id, pairs in list(TRACKED_PAIRS.items()):
            for symbol in list(pairs):
                for tf in TIMEFRAMES:
                    ohlcv, used_exchange = fetch_ohlcv_with_fallback(symbol, tf)
                    if ohlcv is None:
                        continue
                        
                    try:
                        last_candle = calculate_indicators(ohlcv)
                        rsi_val = last_candle['RSI']
                        mfi_val = last_candle['MFI']
                        price = last_candle['close']
                        
                        # ALERT LOGIC: Trigger on standard oversold/overbought zone entries
                        if rsi_val <= 30 or rsi_val >= 70 or mfi_val <= 20 or mfi_val >= 80:
                            msg = (
                                f"🚨 *%s ALERT: {symbol}*\n"
                                f"• *Timeframe:* {tf}\n"
                                f"• *Price:* ${price:,.2f}\n"
                                f"• *Source:* {used_exchange}\n\n"
                                f"📈 *RSI (14):* {rsi_val:.2f}\n"
                                f"🧪 *MFI (14):* {mfi_val:.2f}"
                            )
                            alert_type = "OVERSOLD" if (rsi_val <= 30 or mfi_val <= 20) else "OVERBOUGHT"
                            await application.bot.send_message(chat_id=chat_id, text=msg % alert_type, parse_mode="Markdown")
                    except Exception as calc_error:
                        logging.error(f"Calculation error for {symbol} on {tf}: {calc_error}")

# ----------------- WEB SERVER FOR RENDER -----------------
app = Flask(__name__)
@app.route('/')
def health_check(): return "Bot is Alive", 200

def run_web_server():
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)

# ----------------- MAIN EXECUTION ENTRY -----------------
def main():
    if not TOKEN:
        logging.error("TELEGRAM_TOKEN variable missing!")
        return

    # Start the mandatory HTTP server inside a background native thread
    Thread(target=run_web_server, daemon=True).start()

    # Build the Telegram Core Application framework
    application = Application.builder().token(TOKEN).build()
    
    # Register interactive commands
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("track", track_coin))
    application.add_handler(CommandHandler("stop", stop_coin))
    application.add_handler(CommandHandler("status", status))
    
    # Register our separate async tracking loop to run alongside the bot framework
    loop = asyncio.get_event_loop()
    loop.create_task(monitoring_job(application))
    
    # Launch the bot
    application.run_polling()

if __name__ == '__main__':
    main()
