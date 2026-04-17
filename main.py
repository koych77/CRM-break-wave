import asyncio
import logging
import os
from pathlib import Path
from threading import Thread

import uvicorn
from aiogram.types import BotCommand


env_path = Path(__file__).parent / ".env"
if env_path.exists():
    for line in env_path.read_text().strip().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip())


from app.api import app as fastapi_app
from app.bot import bot, daily_notification_scheduler, dp, lesson_reminder_scheduler
from app.database import init_db


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


async def on_startup():
    await init_db()
    logger.info("Database initialized")

    await bot.set_my_commands([
        BotCommand(command="start", description="Открыть CRM"),
        BotCommand(command="now", description="Текущая тренировка"),
        BotCommand(command="summary", description="Ежедневная сводка"),
        BotCommand(command="help", description="Помощь"),
    ])

    asyncio.create_task(lesson_reminder_scheduler())
    logger.info("Lesson reminder scheduler started")

    asyncio.create_task(daily_notification_scheduler())
    logger.info("Daily notification scheduler started")


def run_api():
    uvicorn.run(fastapi_app, host="0.0.0.0", port=8080, log_level="info")


async def run_bot():
    await on_startup()
    logger.info("Starting bot polling...")
    await dp.start_polling(bot)


def main():
    api_thread = Thread(target=run_api, daemon=True)
    api_thread.start()
    logger.info("API server started on port 8080")

    asyncio.run(run_bot())


if __name__ == "__main__":
    main()
