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
import json

def get_lesson_time_for_day(student, day_of_week):
    """Get lesson time for specific day from lesson_times JSON."""
    try:
        times = json.loads(student.lesson_times or '{}')
        return times.get(str(day_of_week), times.get('default', '18:00'))
    except:
        return '18:00'

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


async def lesson_reminder_scheduler():
    """Background task: check for unmarked lessons and send reminders every 15 min."""
    from app.models import Coach, Student, Lesson, Attendance
    
    # Track last reminder time per coach to avoid spam
    last_reminder = {}
    
    while True:
        try:
            await asyncio.sleep(300)  # Check every 5 minutes
            
            now = datetime.now()
            current_weekday = now.weekday()
            current_date = now.date()
            
            async with async_session() as s:
                coaches_result = await s.execute(select(Coach).where(Coach.is_active == True))
                coaches = coaches_result.scalars().all()
                
                for coach in coaches:
                    # Check if we already sent reminder in last 15 minutes
                    coach_id = coach.id
                    last_time = last_reminder.get(coach_id)
                    if last_time and (now - last_time).total_seconds() < 900:  # 15 minutes
                        continue
                    
                    # Get students grouped by time
                    students_result = await s.execute(
                        select(Student).where(
                            Student.coach_id == coach.id,
                            Student.is_active == True
                        )
                    )
                    students = students_result.scalars().all()
                    
                    # Group by time slot
                    groups = {}
                    for student in students:
                        days = student.lesson_days.split(",") if student.lesson_days else []
                        if str(current_weekday) in days:
                            lesson_time = get_lesson_time_for_day(student, current_weekday)
                            lesson_hour, lesson_min = map(int, lesson_time.split(":"))
                            lesson_start = lesson_hour * 60 + lesson_min
                            
                            now_total = now.hour * 60 + now.minute
                            
                            # Check if in active lesson window (started 0-90 min ago)
                            if 0 <= now_total - lesson_start <= 90:
                                if lesson_time not in groups:
                                    groups[lesson_time] = []
                                
                                # Check if marked
                                existing = await s.execute(
                                    select(Lesson).where(
                                        Lesson.student_id == student.id,
                                        Lesson.date == current_date
                                    )
                                )
                                if not existing.scalar_one_or_none():
                                    groups[lesson_time].append(student)
                    
                    # Find first unmarked group
                    for time_key in sorted(groups.keys()):
                        unmarked = groups[time_key]
                        if unmarked:
                            try:
                                student_names = "\n".join([f"• {s.name}" for s in unmarked[:5]])
                                if len(unmarked) > 5:
                                    student_names += f"\n... и ещё {len(unmarked) - 5}"
                                
                                kb = InlineKeyboardMarkup(inline_keyboard=[
                                    [InlineKeyboardButton(
                                        text=f"✅ Отметить ({len(unmarked)} чел.)", 
                                        callback_data=f"quick_group:{time_key}"
                                    )],
                                    [InlineKeyboardButton(
                                        text="❌ Тренировки нет", 
                                        callback_data=f"skip_group:{time_key}"
                                    )]
                                ])
                                
                                await bot.send_message(
                                    coach.telegram_id,
                                    f"⏰ Напоминание о тренировке!\n\n"
                                    f"🕐 Время: {time_key}\n"
                                    f"👥 Не отмечены: {len(unmarked)}\n\n"
                                    f"{student_names}",
                                    reply_markup=kb
                                )
                                
                                last_reminder[coach_id] = now
                                logger.info(f"Sent reminder to coach {coach_id} for group {time_key}")
                                break  # Only send one reminder at a time
                            except Exception as e:
                                logger.error(f"Failed to send reminder: {e}")
                            
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Scheduler error: {e}")


async def daily_notification_scheduler():
    """Background task: send daily summaries at 9:00 AM."""
    from app.bot import send_daily_summary
    
    while True:
        try:
            now = datetime.now()
            
            # Calculate time until 9:00 AM
            target_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
            if target_time <= now:
                target_time += timedelta(days=1)
            
            wait_seconds = (target_time - now).total_seconds()
            logger.info(f"Daily notification scheduler: waiting {wait_seconds/3600:.1f} hours until 9:00 AM")
            
            await asyncio.sleep(wait_seconds)
            
            # Send daily summaries
            await send_daily_summary()
            
            # Wait a bit to avoid double-sending
            await asyncio.sleep(60)
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in daily notification scheduler: {e}")
            await asyncio.sleep(3600)  # Retry in 1 hour


async def on_startup():
    await init_db()
    logger.info("Database initialized")
    
    # Set bot commands
    await bot.set_my_commands([
        BotCommand(command="start", description="Открыть CRM"),
        BotCommand(command="now", description="Текущая тренировка"),
        BotCommand(command="summary", description="Ежедневная сводка"),
        BotCommand(command="help", description="Помощь"),
    ])
    
    # Start reminder scheduler
    asyncio.create_task(lesson_reminder_scheduler())
    logger.info("Lesson reminder scheduler started")
    
    # Start daily notification scheduler
    asyncio.create_task(daily_notification_scheduler())
    logger.info("Daily notification scheduler started")


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
