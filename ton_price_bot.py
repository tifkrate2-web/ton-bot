import asyncio
import aiohttp
import json
import os
from datetime import datetime
from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode

# ========== CONFIG (from Railway environment variables) ==========
BOT_TOKEN      = os.environ["BOT_TOKEN"]       # Set in Railway Variables tab
CHANNEL_ID     = os.environ["CHANNEL_ID"]      # e.g. @TONPriceChannel
INTERVAL_MINUTES = int(os.environ.get("INTERVAL_MINUTES", "5"))
# ================================================================

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/tonusdt@ticker"

# Shared real-time price data (updated every second via WebSocket)
latest_data = {}


# ──────────────────────────────────────────────
# 1. BINANCE WEBSOCKET — keeps price up to date
# ──────────────────────────────────────────────
async def listen_binance_ws():
    global latest_data
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(BINANCE_WS_URL) as ws:
                    print("✅ Connected to Binance WebSocket (real-time)")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            latest_data = {
                                "price":      float(data["c"]),
                                "change_24h": float(data["P"]),
                                "high_24h":   float(data["h"]),
                                "low_24h":    float(data["l"]),
                                "volume_24h": float(data["v"]),
                                "quote_vol":  float(data["q"]),
                            }
        except Exception as e:
            print(f"[WS Error] {e} — reconnecting in 5s...")
            await asyncio.sleep(5)


# ──────────────────────────────────────────────
# 2. MESSAGE FORMATTER
# ──────────────────────────────────────────────
def format_message(d: dict) -> str:
    price    = d["price"]
    change   = d["change_24h"]
    high     = d["high_24h"]
    low      = d["low_24h"]
    vol_usdt = d["quote_vol"]

    arrow = "🟢" if change >= 0 else "🔴"
    sign  = "+" if change >= 0 else ""
    now   = datetime.utcnow().strftime("%Y\\-%m\\-%d %H:%M UTC")

    return (
        f"💎 *TON / USDT — Binance*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💵 Price:        `${price:,.4f}`\n"
        f"{arrow} 24h Change:  `{sign}{change:.2f}%`\n"
        f"📈 24h High:    `${high:,.4f}`\n"
        f"📉 24h Low:     `${low:,.4f}`\n"
        f"📊 Vol \\(USDT\\):  `${vol_usdt:,.0f}`\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"⚡ *Live via Binance*  🕐 `{now}`"
    )


# ──────────────────────────────────────────────
# 3. AUTO-POST LOOP — posts every N minutes
# ──────────────────────────────────────────────
async def post_price_loop(bot: Bot):
    print(f"📡 Auto-posting to {CHANNEL_ID} every {INTERVAL_MINUTES} min...")

    while not latest_data:
        print("⏳ Waiting for first Binance tick...")
        await asyncio.sleep(1)

    while True:
        try:
            msg = format_message(latest_data)
            await bot.send_message(
                chat_id=CHANNEL_ID,
                text=msg,
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            print(f"[{datetime.utcnow()}] ✅ Posted: ${latest_data['price']:,.4f}")
        except Exception as e:
            print(f"[{datetime.utcnow()}] ❌ Post error: {e}")

        await asyncio.sleep(INTERVAL_MINUTES * 60)


# ──────────────────────────────────────────────
# 4. /price COMMAND — users can check manually
# ──────────────────────────────────────────────
async def price_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not latest_data:
        await update.message.reply_text("⏳ Fetching price, please try again in a second...")
        return
    msg = format_message(latest_data)
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2)


# ──────────────────────────────────────────────
# 5. /start COMMAND
# ──────────────────────────────────────────────
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 *TON Price Bot*\n\n"
        "Commands:\n"
        "💎 /price — Get current TON price\n\n"
        f"📡 Auto-posts to channel every {INTERVAL_MINUTES} min\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
    )


# ──────────────────────────────────────────────
# 6. MAIN — run everything concurrently
# ──────────────────────────────────────────────
async def main():
    # Build the Telegram app
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("price", price_command))

    bot = app.bot

    # Run all tasks together
    await asyncio.gather(
        listen_binance_ws(),
        post_price_loop(bot),
        app.run_polling(close_loop=False),
    )


if __name__ == "__main__":
    asyncio.run(main())
