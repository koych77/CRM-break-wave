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

from aiogram.types import BotCommand, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram import Bot
from app.api import app as fastapi_app
from app.bot import bot, dp
from app.database import init_db, async_session
from app.config import DATA_DIR
from sqlalchemy import select

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


async def lesson_reminder_scheduler():
    """Background task: check for unmarked lessons and send reminders."""
    from app.models import Coach, Student, Lesson, Attendance
    
    while True:
        try:
            await asyncio.sleep(300)  # Check every 5 minutes
            
            now = datetime.now()
            current_weekday = now.weekday()
            current_date = now.date()
            
            async with async_session() as s:
                # Get all coaches
                coaches_result = await s.execute(select(Coach).where(Coach.is_active == True))
                coaches = coaches_result.scalars().all()
                
                for coach in coaches:
                    # Get students who should have lesson now
                    students_result = await s.execute(
                        select(Student).where(
                            Student.coach_id == coach.id,
                            Student.is_active == True
                        )
                    )
                    students = students_result.scalars().all()
                    
                    unmarked_students = []
                    for student in students:
                        days = student.lesson_days.split(",") if student.lesson_days else []
                        if str(current_weekday) in days:
                            # Check time window
                            lesson_time = student.lesson_time or "18:00"
                            lesson_hour, lesson_min = map(int, lesson_time.split(":"))
                            lesson_start = lesson_hour * 60 + lesson_min
                            
                            now_total = now.hour * 60 + now.minute
                            
                            # If lesson started 0-45 min ago
                            if 0 <= now_total - lesson_start <= 45:
                                # Check if marked
                                existing = await s.execute(
                                    select(Lesson).where(
                                        Lesson.student_id == student.id,
                                        Lesson.date == current_date
                                    )
                                )
                                if not existing.scalar_one_or_none():
                                    unmarked_students.append(student)
                    
                    # Send reminder if there are unmarked students
                    if unmarked_students and len(unmarked_students) > 0:
                        try:
                            student_names = "\n".join([f"• {s.name}" for s in unmarked_students[:5]])
                            if len(unmarked_students) > 5:
                                student_names += f"\n... и ещё {len(unmarked_students) - 5}"
                            
                            kb = InlineKeyboardMarkup(inline_keyboard=[
                                [InlineKeyboardButton(text="✅ Отметить посещаемость", callback_data="quick_attendance")],
                                [InlineKeyboardButton(text="❌ Тренировки нет", callback_data="skip_lesson")]
                            ])
                            
                            await bot.send_message(
                                coach.telegram_id,
                                f"⏰ Напоминание!\n\n"
                                f"📅 Тренировка началась ({now.strftime('%H:%M')})\n"
                                f"👥 Не отмечены ({len(unmarked_students)}):\n{student_names}\n\n"
                                f"Отметьте посещаемость:",
                                reply_markup=kb
                            )
                            logger.info(f"Sent reminder to coach {coach.telegram_id}")
                        except Exception as e:
                            logger.error(f"Failed to send reminder: {e}")
                            
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Scheduler error: {e}")


async def on_startup():
    await init_db()
    logger.info("Database initialized")
    
    # Set bot commands
    await bot.set_my_commands([
        BotCommand(command="start", description="Открыть CRM"),
        BotCommand(command="help", description="Помощь"),
    ])
    
    # Start reminder scheduler
    asyncio.create_task(lesson_reminder_scheduler())
    logger.info("Lesson reminder scheduler started")


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
