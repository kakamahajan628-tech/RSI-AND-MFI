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

# Logging Setup
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    stream=sys.stdout
)

TOKEN = os.getenv("TELEGRAM_TOKEN")
USER_CHAT_ID = int(os.getenv("USER_CHAT_ID")) if os.getenv("USER_CHAT_ID") else None

# --- Smart Exchange Initialization ---
EXCHANGES = []
try:
    # Gate.io instance (Bina defaultType ke, taaki dono markets access ho sakein)
    gate = ccxt.gateio({
        'apiKey': os.getenv("GATE_API_KEY"),
        'secret': os.getenv("GATE_API_SECRET"),
        'enableRateLimit': True,
    })
    # OKX instance
    okx = ccxt.okx({
        'apiKey': os.getenv("OKX_API_KEY"),
        'secret': os.getenv("OKX_API_SECRET"),
        'password': os.getenv("OKX_API_PASSWORD"),
        'enableRateLimit': True,
    })
    EXCHANGES = [gate, okx]
    logging.info("Exchanges initialized in Multi-Market mode (Futures/Spot).")
except Exception as e:
    logging.error(f"Exchange Init Error: {e}")

TRACKED_PAIRS = {}
TIMEFRAMES = ['5m', '15m', '1h', '4h']

# --- Smart Market Fetcher (Futures with Spot Fallback) ---
async def fetch_market_data_smart(exchange, symbol, timeframe):
    """
    Pehle Futures dhoondega, agar wahan coin nahi mila ya list nahi hai, 
    toh automatic Spot market se data nikalega.
    """
    market_types = ['future', 'spot']
    
    for m_type in market_types:
        try:
            # Exchange ko temporary us market type par switch karo
            exchange.options['defaultType'] = m_type
            
            # Agar future hai toh market symbol ccxt standard ke mutabiq convert ho sakta hai (e.g., BTC/USDT:USDT)
            # ccxt load_markets() internally handles matching, but safe side directly fetch karte hain
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=50)
            ticker = exchange.fetch_ticker(symbol)
            
            if ohlcv and len(ohlcv) >= 14:
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                rsi = ta.momentum.rsi(close=df['close'], window=14).iloc[-1]
                mfi = ta.volume.money_flow_index(high=df['high'], low=df['low'], close=df['close'], volume=df['volume'], window=14).iloc[-1]
                
                return {
                    "price": ticker['last'],
                    "rsi": rsi,
                    "mfi": mfi,
                    "market_type": m_type.upper(),
                    "exchange_name": exchange.name
                }
        except Exception:
            # Agar Futures mein error aaya (jaise 'Bad Symbol' ya 'Not Found'), 
            # toh loop agle step par chalega aur Spot try karega.
            continue
            
    return None

# --- Telegram Handlers ---
async def track_coin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("❌ Use: `/track COIN/USDT`")
        return
    symbol = context.args[0].upper()
    chat_id = update.effective_chat.id
    if chat_id not in TRACKED_PAIRS: TRACKED_PAIRS[chat_id] = set()
    TRACKED_PAIRS[chat_id].add(symbol)
    await update.message.reply_text(f"✅ Tracking **{symbol}**\n(Pehle Futures check hoga, fallback to Spot).")

async def get_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "💰 *Spot Wallet Balances:*\n\n"
    for ex in EXCHANGES:
        if not ex.apiKey: continue
        try:
            ex.options['defaultType'] = 'spot' # Force spot for balance command
            bal = ex.fetch_balance()
            total = {k: v for k, v in bal['total'].items() if v > 0}
            if total:
                msg += f"🏦 *{ex.name}:*\n" + "\n".join([f"• {c}: `{a:.4f}`" for c, a in total.items()]) + "\n\n"
        except: msg += f"❌ {ex.name}: API Error\n"
    await update.message.reply_text(msg if "🏦" in msg else "No Spot balance found.", parse_mode="Markdown")

# --- Monitoring Loop ---
async def monitoring_job(application: Application):
    while True:
        await asyncio.sleep(60)
        for chat_id, pairs in list(TRACKED_PAIRS.items()):
            for symbol in list(pairs):
                data_matrix = {}
                alert_triggered = False
                current_price = 0.0
                detected_market = ""
                detected_source = ""

                for tf in TIMEFRAMES:
                    for ex in EXCHANGES:
                        data = await fetch_market_data_smart(ex, symbol, tf)
                        if data:
                            current_price = data['price']
                            detected_market = data['market_type']
                            detected_source = data['exchange_name']
                            data_matrix[tf] = (data['rsi'], data['mfi'])
                            
                            if data['rsi'] <= 30 or data['rsi'] >= 70 or data['mfi'] <= 20 or data['mfi'] >= 80:
                                alert_triggered = True
                            break # Timeframe mil gaya toh exchange loop se bahar niklo

                if alert_triggered and data_matrix:
                    msg = f"🚨 *METRIC ALERT: {symbol}*\n"
                    msg += f"💵 *Price:* `${current_price:,.4f}`\n"
                    msg += f"🏛 *Source:* {detected_source} ({detected_market})\n"
                    msg += "───────────────────\n"
                    msg += "`TF    │ RSI    │ MFI`\n"
                    for tf in TIMEFRAMES:
                        if tf in data_matrix:
                            r, m = data_matrix[tf]
                            msg += f"`{tf:<5}│ {r:<6.1f} │ {m:<6.1f}`\n"
                    msg += "───────────────────\n"
                    try:
                        await application.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")
                    except: pass

# --- Web Server ---
app = Flask(__name__)
@app.route('/')
def health(): return "Smart Bot Alive", 200

def main():
    Thread(target=lambda: app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000))), daemon=True).start()
    app_bot = Application.builder().token(TOKEN).build()
    app_bot.add_handler(CommandHandler("track", track_coin))
    app_bot.add_handler(CommandHandler("balance", get_balance))
    
    loop = asyncio.get_event_loop()
    loop.create_task(monitoring_job(app_bot))
    app_bot.run_polling()

if __name__ == '__main__':
    main()
