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

# Detailed Logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    stream=sys.stdout
)

TOKEN = os.getenv("TELEGRAM_TOKEN")

# --- Exchange Setup ---
EXCHANGES = []
try:
    gate = ccxt.gateio({'enableRateLimit': True})
    okx = ccxt.okx({
        'enableRateLimit': True,
        'password': os.getenv("OKX_API_PASSWORD")
    })
    EXCHANGES = [gate, okx]
    logging.info("Exchanges initialized for Raw RSI tracking.")
except Exception as e:
    logging.error(f"Exchange Init Error: {e}")

TRACKED_PAIRS = {}
TIMEFRAMES = ['5m', '15m', '1h', '4h']

# --- Dedicated RSI Fetcher (Futures with Spot Fallback) ---
def fetch_rsi_smart(exchange, symbol, timeframe):
    market_types = ['future', 'spot']
    
    for m_type in market_types:
        try:
            exchange.options['defaultType'] = m_type
            
            # Fetch Candles & Live Price
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=50)
            ticker = exchange.fetch_ticker(symbol)
            
            if ohlcv and len(ohlcv) >= 14:
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                rsi_series = ta.momentum.rsi(close=df['close'], window=14)
                rsi_val = rsi_series.iloc[-1]
                
                if pd.isna(rsi_val):
                    continue
                    
                return {
                    "price": ticker['last'],
                    "rsi": rsi_val,
                    "market_type": m_type.upper(),
                    "exchange_name": exchange.name
                }
        except Exception:
            continue  # Fallback to Spot if Future fails
            
    return None

# --- Telegram Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📈 *Pure RSI Matrix Bot Active*\n\n"
        "Commands:\n"
        "`/track COIN/USDT` - Track RSI (Updates every 30s)\n"
        "`/status` - View active watchlist",
        parse_mode="Markdown"
    )

async def track_coin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("❌ Example: `/track ETH/USDT`")
        return
    
    symbol = context.args[0].upper()
    chat_id = update.effective_chat.id
    
    if chat_id not in TRACKED_PAIRS:
        TRACKED_PAIRS[chat_id] = set()
        
    TRACKED_PAIRS[chat_id].add(symbol)
    logging.info(f"Tracking RSI for {symbol}")
    await update.message.reply_text(f"✅ Now broadcasting *{symbol}* RSI matrix updates every 30 seconds.", parse_mode="Markdown")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    pairs = TRACKED_PAIRS.get(chat_id, set())
    if not pairs:
        await update.message.reply_text("Watchlist is empty.")
    else:
        await update.message.reply_text("📋 *RSI Watchlist:*\n" + "\n".join([f"• {p}" for p in pairs]), parse_mode="Markdown")

# --- Core Loop ---
async def monitoring_job(application: Application):
    while True:
        await asyncio.sleep(30)  # Scan every 30 seconds
        
        if not TRACKED_PAIRS:
            continue
            
        for chat_id, pairs in list(TRACKED_PAIRS.items()):
            for symbol in list(pairs):
                data_matrix = {}
                current_price = 0.0
                detected_market = ""
                detected_source = ""

                for tf in TIMEFRAMES:
                    for ex in EXCHANGES:
                        loop = asyncio.get_running_loop()
                        data = await loop.run_in_executor(None, fetch_rsi_smart, ex, symbol, tf)
                        
                        if data:
                            current_price = data['price']
                            detected_market = data['market_type']
                            detected_source = data['exchange_name']
                            data_matrix[tf] = data['rsi']
                            
                            # Log to Render Console
                            logging.info(f"[{symbol} - {tf}] Price: {current_price} | RSI: {data['rsi']:.2f}")
                            break  # Move to next timeframe

                # Condition 2 Removed: Agar data aaya hai, toh seedha chat par report bhej do
                if data_matrix:
                    msg = f"📊 *RSI UPDATE: {symbol}*\n"
                    msg += f"💵 *Price:* `${current_price:,.4f}`\n"
                    msg += f"🏛 *Source:* {detected_source} ({detected_market})\n"
                    msg += "───────────────────\n"
                    msg += "`TF    │ RSI (14)`\n"
                    msg += "───────────────────\n"
                    
                    for tf in TIMEFRAMES:
                        if tf in data_matrix:
                            r = data_matrix[tf]
                            # Visual tags for quick scanning
                            tag = "🔴 [OB]" if r >= 70 else ("🟢 [OS]" if r <= 30 else "⚪ [MID]")
                            msg += f"`{tf:<5}│ {r:<6.1f}` {tag}\n"
                            
                    msg += "───────────────────\n"
                    try:
                        await application.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")
                    except Exception as e:
                        logging.error(f"Telegram send error: {e}")

# --- Web Keep-Alive ---
app = Flask(__name__)
@app.route('/')
def health(): return "RSI Bot Running", 200

def main():
    Thread(target=lambda: app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000))), daemon=True).start()
    
    if not TOKEN:
        sys.exit(1)
        
    app_bot = Application.builder().token(TOKEN).build()
    app_bot.add_handler(CommandHandler("start", start))
    app_bot.add_handler(CommandHandler("track", track_coin))
    app_bot.add_handler(CommandHandler("status", status))
    
    loop = asyncio.get_event_loop()
    loop.create_task(monitoring_job(app_bot))
    app_bot.run_polling()

if __name__ == '__main__':
    main()
