import asyncio
import logging
import os
import uvicorn
from threading import Thread
from pathlib import Path
from datetime import datetime, timedelta, date

# Load .env if present
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    for line in env_path.read_text().strip().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip())

from aiogram.types import BotCommand
from aiogram import Bot
from app.api import app as fastapi_app
from app.bot import create_bot, dp, lesson_reminder_scheduler, daily_notification_scheduler, notify_version_update
from app.database import init_db
from app.config import DATA_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


async def on_startup():
    """Initialize database and bot commands."""
    await init_db()
    logger.info("Database initialized")
    bot = create_bot()
    
    # Set bot commands
    await bot.set_my_commands([
        BotCommand(command="start", description="Открыть CRM"),
        BotCommand(command="now", description="Текущая тренировка"),
        BotCommand(command="summary", description="Ежедневная сводка"),
        BotCommand(command="help", description="Помощь"),
    ])
    return bot


def run_api():
    """Run FastAPI server in a thread."""
    uvicorn.run(fastapi_app, host="0.0.0.0", port=8080, log_level="info")


async def run_bot():
    """Run Telegram bot with all schedulers."""
    bot = await on_startup()
    await notify_version_update()
    asyncio.create_task(lesson_reminder_scheduler())
    asyncio.create_task(daily_notification_scheduler())
    logger.info("Starting bot polling...")
    await dp.start_polling(bot)
    return bot


def main():
    # Run FastAPI in a thread
    api_thread = Thread(target=run_api, daemon=True)
    api_thread.start()
    logger.info("API server started on port 8080")
    
    # Run bot in main asyncio loop (includes all schedulers)
    asyncio.run(run_bot())


if __name__ == "__main__":
    main()
