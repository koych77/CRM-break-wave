import asyncio
import logging
import os
import uvicorn
from threading import Thread
from pathlib import Path

# Load .env if present
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    for line in env_path.read_text().strip().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip())

from aiogram.types import BotCommand
from app.api import app as fastapi_app
from app.bot import bot, dp
from app.database import init_db
from app.config import DATA_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


async def on_startup():
    await init_db()
    logger.info("Database initialized")
    
    # Set bot commands
    await bot.set_my_commands([
        BotCommand(command="start", description="Открыть CRM"),
        BotCommand(command="help", description="Помощь"),
    ])


def run_api():
    uvicorn.run(fastapi_app, host="0.0.0.0", port=8080, log_level="info")


async def run_bot():
    await on_startup()
    logger.info("Starting bot polling...")
    await dp.start_polling(bot)


def main():
    # Run FastAPI in a thread
    api_thread = Thread(target=run_api, daemon=True)
    api_thread.start()
    logger.info("API server started on port 8080")
    
    # Run bot in main asyncio loop
    asyncio.run(run_bot())


if __name__ == "__main__":
    main()
