import logging
import json
from aiogram import Bot, Dispatcher, Router, types, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    WebAppInfo, CallbackQuery, Message
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from sqlalchemy import select, func, and_, or_
from datetime import datetime, date, timedelta
from app.database import async_session
from app.models import Coach, Student, Lesson, Attendance, Payment, AdminUser
from app.config import BOT_TOKEN, ADMIN_IDS, ADMIN_SECRET, WEBAPP_URL


# Helper function to get lesson time for a specific day
def get_lesson_time_for_day(student: Student, day_of_week: int) -> str:
    """Get lesson time for specific day from lesson_times JSON."""
    try:
        times = json.loads(student.lesson_times or '{}')
        return times.get(str(day_of_week), times.get('default', '18:00'))
    except:
        return '18:00'

logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)

# === Helpers ===

async def is_admin(user_id: int) -> bool:
    if ADMIN_IDS and user_id in ADMIN_IDS:
        return True
    async with async_session() as s:
        result = await s.execute(select(AdminUser).where(AdminUser.telegram_id == user_id))
        return result.scalar_one_or_none() is not None


async def is_coach(user_id: int) -> bool:
    async with async_session() as s:
        result = await s.execute(select(Coach).where(Coach.telegram_id == user_id))
        return result.scalar_one_or_none() is not None


async def get_coach(user_id: int):
    async with async_session() as s:
        result = await s.execute(select(Coach).where(Coach.telegram_id == user_id))
        return result.scalar_one_or_none()


async def register_coach(user_id: int, first_name: str = None, username: str = None):
    async with async_session() as s:
        existing = await s.execute(select(Coach).where(Coach.telegram_id == user_id))
        if existing.scalar_one_or_none():
            return False
        coach = Coach(telegram_id=user_id, first_name=first_name, username=username)
        s.add(coach)
        await s.commit()
        logger.info(f"Registered coach: {user_id} ({first_name})")
        return True


# === FSM States ===

class StudentForm(StatesGroup):
    name = State()
    phone = State()
    lesson_price = State()
    lessons_count = State()
    location = State()
    lesson_days = State()
    lesson_time = State()
    confirm = State()


class PaymentForm(StatesGroup):
    select_student = State()
    amount = State()
    period = State()
    confirm = State()


# === Commands ===

@router.message(CommandStart())
async def cmd_start(message: Message):
    user_id = message.from_user.id
    logger.info(f"Start command from user: {user_id} ({message.from_user.first_name})")
    
    # Check roles
    is_admin_user = await is_admin(user_id)
    coach = await get_coach(user_id)
    
    # If admin - show admin panel + coach interface if registered
    if is_admin_user:
        admin_text = (
            "👑 <b>Админ-панель CRM Break Wave</b>\n\n"
            "Вы администратор системы.\n"
            "Админ-команды:\n"
            "/coaches - список тренеров\n"
            "/stats - общая статистика\n\n"
        )
        
        if coach:
            # Admin is also a coach - show both interfaces
            webapp_url = WEBAPP_URL or "https://your-app.up.railway.app"
            
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="📱 Открыть CRM (как тренер)",
                    web_app=WebAppInfo(url=f"{webapp_url}/")
                )],
                [InlineKeyboardButton(text="👥 Мои ученики", callback_data="my_students")],
                [InlineKeyboardButton(text="⚠️ Проверить оплаты", callback_data="check_payments")],
            ])
            
            await message.answer(
                admin_text + 
                f"✅ Вы также зарегистрированы как тренер: {coach.first_name or 'Тренер'}\n\n"
                "Быстрые действия:",
                parse_mode="HTML",
                reply_markup=kb
            )
        else:
            # Admin but not a coach
            await message.answer(
                admin_text + 
                "❌ Вы не зарегистрированы как тренер.\n"
                "Используйте /coach <код> чтобы стать тренером.",
                parse_mode="HTML"
            )
        return
    
    # Check if coach
    coach = await get_coach(user_id)
    if coach:
        logger.info(f"Coach found: {coach.first_name} (ID: {user_id})")
    else:
        logger.info(f"Coach not found for user: {user_id}")
        await message.answer(
            "👋 <b>Добро пожаловать в CRM Break Wave!</b>\n\n"
            "Эта система для тренеров школы.\n"
            "Для доступа обратитесь к администратору или используйте секретный код.\n\n"
            "Если у вас есть код, введите: /coach <код>"
        )
        return
    
    # Coach registered - check for current lessons first
    now = datetime.now()
    current_weekday = now.weekday()
    current_date = now.date()
    
    async with async_session() as s:
        # Get students grouped by time
        result = await s.execute(
            select(Student).where(
                Student.coach_id == coach.id,
                Student.is_active == True
            )
        )
        students = result.scalars().all()
        
        # Group by time and check for unmarked
        groups = {}
        for st in students:
            days = st.lesson_days.split(",") if st.lesson_days else []
            if str(current_weekday) in days:
                time_key = get_lesson_time_for_day(st, current_weekday)
                lesson_hour, lesson_min = map(int, time_key.split(":"))
                lesson_start = lesson_hour * 60 + lesson_min
                now_total = now.hour * 60 + now.minute
                
                # Check if lesson is now (±30 min) or passed (up to 90 min ago)
                if -30 <= now_total - lesson_start <= 90:
                    if time_key not in groups:
                        groups[time_key] = {"students": [], "marked": 0}
                    
                    existing = await s.execute(
                        select(Lesson).where(
                            Lesson.student_id == st.id,
                            Lesson.date == current_date
                        )
                    )
                    is_marked = existing.scalar_one_or_none() is not None
                    
                    groups[time_key]["students"].append({"student": st, "marked": is_marked})
                    if is_marked:
                        groups[time_key]["marked"] += 1
        
        # Find first active group with unmarked students
        for time_key in sorted(groups.keys()):
            group = groups[time_key]
            unmarked = [s for s in group["students"] if not s["marked"]]
            
            if unmarked:
                text = f"📋 <b>Тренировка {time_key}</b>\n\n"
                text += f"👥 Не отмечены: {len(unmarked)}/{len(group['students'])}\n\n"
                
                for item in unmarked[:5]:
                    text += f"⏳ {item['student'].name}\n"
                if len(unmarked) > 5:
                    text += f"... и ещё {len(unmarked) - 5}\n"
                
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
                
                await message.answer(text, parse_mode="HTML", reply_markup=kb)
                return
    
    # No active lessons - show main menu
    webapp_url = WEBAPP_URL or "https://your-app.up.railway.app"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="📱 Открыть CRM",
            web_app=WebAppInfo(url=f"{webapp_url}/")
        )],
        [InlineKeyboardButton(text="👥 Мои ученики", callback_data="my_students")],
        [InlineKeyboardButton(text="⚠️ Проверить оплаты", callback_data="check_payments")],
    ])
    
    await message.answer(
        f"👋 Привет, {coach.first_name or 'тренер'}!\n\n"
        "<b>CRM Break Wave</b> — управление учениками, посещаемостью и оплатой.\n\n"
        f"📅 Сейчас активных тренировок нет.\n"
        f"🕐 Текущее время: {now.strftime('%H:%M')}",
        parse_mode="HTML",
        reply_markup=kb
    )


@router.message(Command("coach"))
async def cmd_coach_register(message: Message):
    user_id = message.from_user.id
    
    # Check if already registered
    existing = await get_coach(user_id)
    if existing:
        await message.answer(
            "👋 <b>Вы уже зарегистрированы как тренер!</b>\n\n"
            f"Тренер: {existing.first_name or 'Без имени'}\n"
            f"ID: {existing.telegram_id}\n\n"
            "Нажмите /start чтобы открыть CRM"
        )
        return
    
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Используй: /coach <секретный код>")
        return
    
    if parts[1].strip() != ADMIN_SECRET:
        await message.answer("❌ Неверный код.")
        return
    
    registered = await register_coach(
        user_id,
        message.from_user.first_name,
        message.from_user.username
    )
    
    if registered:
        logger.info(f"New coach registered: {user_id} ({message.from_user.first_name})")
        await message.answer(
            "✅ <b>Вы зарегистрированы как тренер!</b>\n\n"
            "Теперь вы можете использовать CRM систему.\n"
            "Нажмите /start чтобы открыть приложение."
        )
    else:
        await message.answer("👋 Вы уже зарегистрированы! Нажмите /start")


@router.message(Command("me"))
async def cmd_me(message: Message):
    """Show user registration status."""
    user_id = message.from_user.id
    
    is_admin_user = await is_admin(user_id)
    coach = await get_coach(user_id)
    
    text_parts = []
    
    # Admin status
    if is_admin_user:
        text_parts.append("👑 <b>Администратор</b>")
    
    # Coach status
    if coach:
        text_parts.append("✅ <b>Тренер</b>")
        text_parts.append(f"\nИмя: {coach.first_name or 'Не указано'}")
        text_parts.append(f"ID: {coach.telegram_id}")
        text_parts.append(f"Username: @{coach.username or 'нет'}")
        text_parts.append(f"Дата регистрации: {coach.created_at.strftime('%d.%m.%Y') if coach.created_at else '—'}")
    elif not is_admin_user:
        text_parts.append("❌ <b>Не зарегистрированы</b>")
        text_parts.append("\nИспользуйте: /coach <код>")
    
    if not text_parts:
        text_parts.append("❌ <b>Нет доступа</b>")
    
    await message.answer("\n".join(text_parts), parse_mode="HTML")


@router.message(Command("help"))
async def cmd_help(message: Message):
    user_id = message.from_user.id
    is_admin_user = await is_admin(user_id)
    coach = await get_coach(user_id)
    
    text = "📋 <b>Помощь по CRM Break Wave</b>\n\n"
    
    if is_admin_user:
        text += "👑 <b>Админ-команды:</b>\n"
        text += "/coaches - список тренеров\n"
        text += "/stats - общая статистика\n\n"
    
    if coach or is_admin_user:
        text += "📱 <b>Тренерские команды:</b>\n"
        text += "/start - открыть CRM\n"
        text += "/now - текущая тренировка\n"
        text += "/me - мой статус\n\n"
        text += "<b>Основные возможности:</b>\n"
        text += "• Ученики — база с настройками\n"
        text += "• Расписание — календарь занятий\n"
        text += "• Посещаемость — отметки\n"
        text += "• Оплата — контроль абонементов\n\n"
    
    if not is_admin_user and not coach:
        text += "❌ У вас нет доступа.\n"
        text += "Используйте: /coach <код>\n\n"
    
    text += "/help - эта справка"
    await message.answer(text, parse_mode="HTML")


@router.message(Command("now"))
async def cmd_now(message: Message):
    """Show current lesson for quick attendance."""
    coach = await get_coach(message.from_user.id)
    if not coach:
        await message.answer("❌ У вас нет доступа")
        return
    
    from datetime import datetime
    now = datetime.now()
    current_weekday = now.weekday()
    current_date = now.date()
    
    async with async_session() as s:
        result = await s.execute(
            select(Student).where(
                Student.coach_id == coach.id,
                Student.is_active == True
            )
        )
        students = result.scalars().all()
        
        # Group by time
        groups = {}
        for st in students:
            days = st.lesson_days.split(",") if st.lesson_days else []
            if str(current_weekday) in days:
                time_key = get_lesson_time_for_day(st, current_weekday)
                if time_key not in groups:
                    groups[time_key] = []
                
                # Check if marked
                existing = await s.execute(
                    select(Lesson).where(
                        Lesson.student_id == st.id,
                        Lesson.date == current_date
                    )
                )
                lesson_exists = existing.scalar_one_or_none()
                status = None
                if lesson_exists:
                    att = await s.execute(
                        select(Attendance).where(Attendance.lesson_id == lesson_exists.id)
                    )
                    att_record = att.scalar_one_or_none()
                    if att_record:
                        status = att_record.status
                
                groups[time_key].append({
                    "student": st,
                    "status": status,
                    "marked": lesson_exists is not None
                })
        
        if not groups:
            await message.answer("📅 Сегодня у вас нет тренировок")
            return
        
        # Show first unmarked group
        for time_key, students_list in sorted(groups.items()):
            unmarked = [s for s in students_list if not s["marked"]]
            
            if unmarked:
                text = f"📋 Тренировка {time_key}\n\n"
                for item in unmarked:
                    st = item["student"]
                    text += f"⏳ {st.name}\n"
                
                marked_count = len([s for s in students_list if s["marked"]])
                total_count = len(students_list)
                
                if marked_count > 0:
                    text += f"\n✅ Отмечено: {marked_count}/{total_count}"
                
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(
                        text="📱 Открыть CRM",
                        web_app=WebAppInfo(url=f"{WEBAPP_URL or 'https://your-app.up.railway.app'}/")
                    )],
                    [InlineKeyboardButton(text="❌ Тренировки нет", callback_data="skip_lesson")]
                ])
                
                await message.answer(text, reply_markup=kb)
                return
        
        # All marked
        text = "✅ Все тренировки сегодня отмечены!"
        await message.answer(text)


# === Callback Handlers ===

@router.callback_query(F.data == "my_students")
async def cb_my_students(callback: CallbackQuery):
    coach = await get_coach(callback.from_user.id)
    if not coach:
        await callback.answer("Нет доступа")
        return
    
    async with async_session() as s:
        result = await s.execute(
            select(Student).where(Student.coach_id == coach.id, Student.is_active == True)
        )
        students = result.scalars().all()
    
    if not students:
        await callback.message.edit_text(
            "У вас пока нет учеников.\n\n"
            "Добавьте первого ученика через Mini App."
        )
        return
    
    text = f"👥 <b>Ваши ученики ({len(students)}):</b>\n\n"
    for st in students:
        days = st.lesson_days or "1,3"
        days_str = ",".join([{"0":"Пн","1":"Вт","2":"Ср","3":"Чт","4":"Пт","5":"Сб","6":"Вс"}[d] for d in days.split(",")])
        # Show first time or indicate different times
        try:
            times = json.loads(st.lesson_times or '{}')
            if len(times) <= 1:
                time_str = times.get('default', '18:00')
            else:
                time_str = "разное"
        except:
            time_str = "18:00"
        text += f"• <b>{st.name}</b>\n  📍 {st.location} | 🕐 {days_str} {time_str}\n  💰 {st.lesson_price} Br/{st.lessons_count} занятий\n\n"
    
    await callback.message.edit_text(text, parse_mode="HTML")


@router.callback_query(F.data == "check_payments")
async def cb_check_payments(callback: CallbackQuery):
    coach = await get_coach(callback.from_user.id)
    if not coach:
        await callback.answer("Нет доступа")
        return
    
    today = date.today()
    
    async with async_session() as s:
        # Find students with ending or overdue subscriptions
        result = await s.execute(
            select(Student, Payment).outerjoin(
                Payment, 
                and_(Payment.student_id == Student.id, Payment.status.in_(["pending", "overdue"]))
            ).where(
                Student.coach_id == coach.id,
                Student.is_active == True
            )
        )
        rows = result.all()
        
        overdue = []
        ending_soon = []
        
        for student, payment in rows:
            if student.subscription_end:
                days_left = (student.subscription_end - today).days
                if days_left < 0:
                    overdue.append(student)
                elif days_left <= 3:
                    ending_soon.append((student, days_left))
    
    if not overdue and not ending_soon:
        await callback.message.edit_text(
            "✅ <b>Все оплаты в порядке!</b>\n\n"
            "Нет просроченных абонементов."
        )
        return
    
    text = "⚠️ <b>Внимание к оплатам:</b>\n\n"
    
    if overdue:
        text += f"❌ <b>Просрочено ({len(overdue)}):</b>\n"
        for st in overdue:
            text += f"• {st.name} — закончил {st.subscription_end.strftime('%d.%m.%Y')}\n"
        text += "\n"
    
    if ending_soon:
        text += f"⏳ <b>Заканчивается скоро ({len(ending_soon)}):</b>\n"
        for st, days in ending_soon:
            text += f"• {st.name} — осталось {days} дн.\n"
    
    await callback.message.edit_text(text, parse_mode="HTML")


@router.callback_query(F.data == "quick_attendance")
async def cb_quick_attendance(callback: CallbackQuery):
    """Show quick attendance screen."""
    coach = await get_coach(callback.from_user.id)
    if not coach:
        await callback.answer("Нет доступа")
        return
    
    from datetime import datetime
    now = datetime.now()
    current_weekday = now.weekday()
    current_date = now.date()
    
    async with async_session() as s:
        result = await s.execute(
            select(Student).where(
                Student.coach_id == coach.id,
                Student.is_active == True
            )
        )
        students = result.scalars().all()
        
        # Filter students for current time
        current_students = []
        for st in students:
            days = st.lesson_days.split(",") if st.lesson_days else []
            if str(current_weekday) in days:
                lesson_time = get_lesson_time_for_day(st, current_weekday)
                lesson_hour, lesson_min = map(int, lesson_time.split(":"))
                lesson_start = lesson_hour * 60 + lesson_min
                now_total = now.hour * 60 + now.minute
                
                # Within lesson time window
                if -15 <= now_total - lesson_start <= 90:
                    # Check if already marked
                    existing = await s.execute(
                        select(Lesson).where(
                            Lesson.student_id == st.id,
                            Lesson.date == current_date
                        )
                    )
                    lesson_exists = existing.scalar_one_or_none()
                    status = None
                    if lesson_exists:
                        att = await s.execute(
                            select(Attendance).where(Attendance.lesson_id == lesson_exists.id)
                        )
                        att_record = att.scalar_one_or_none()
                        if att_record:
                            status = att_record.status
                    
                    current_students.append({
                        "student": st,
                        "status": status,
                        "marked": lesson_exists is not None
                    })
        
        if not current_students:
            await callback.message.edit_text("❌ Сейчас нет тренировок")
            return
        
        # Build attendance list
        text = f"📋 Тренировка ({now.strftime('%H:%M')})\n\n"
        
        for item in current_students:
            st = item["student"]
            status = item["status"]
            
            if status == "present":
                emoji = "✅"
            elif status == "absent":
                emoji = "❌"
            elif status == "sick":
                emoji = "🤒"
            else:
                emoji = "⏳"
            
            text += f"{emoji} {st.name}\n"
        
        text += "\nОтметьте учеников в Mini App"
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📱 Открыть CRM", web_app=WebAppInfo(url=f"{WEBAPP_URL or 'https://your-app.up.railway.app'}/"))]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data == "skip_lesson")
async def cb_skip_lesson(callback: CallbackQuery):
    """Mark lesson as skipped."""
    coach = await get_coach(callback.from_user.id)
    if not coach:
        await callback.answer("Нет доступа")
        return
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎉 Праздник", callback_data="skip_reason:holiday")],
        [InlineKeyboardButton(text="🤒 Тренер болеет", callback_data="skip_reason:sick")],
        [InlineKeyboardButton(text="🏠 Другое", callback_data="skip_reason:other")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_skip")]
    ])
    
    await callback.message.edit_text(
        "❌ Тренировка отменена\n\nВыберите причину:",
        reply_markup=kb
    )


@router.callback_query(F.data.startswith("skip_reason:"))
async def cb_skip_reason(callback: CallbackQuery):
    """Handle skip reason selection."""
    coach = await get_coach(callback.from_user.id)
    if not coach:
        await callback.answer("Нет доступа")
        return
    
    reason = callback.data.split(":")[1]
    reason_text = {"holiday": "Праздник", "sick": "Тренер болеет", "other": "Другое"}.get(reason, "Другое")
    
    from datetime import date
    from app.models import Lesson, Attendance, Student
    
    today = date.today()
    
    async with async_session() as s:
        # Get all students for this coach
        result = await s.execute(
            select(Student).where(
                Student.coach_id == coach.id,
                Student.is_active == True
            )
        )
        students = result.scalars().all()
        
        skipped_count = 0
        for student in students:
            # Check if already marked
            existing = await s.execute(
                select(Lesson).where(
                    Lesson.student_id == student.id,
                    Lesson.date == today
                )
            )
            if existing.scalar_one_or_none():
                continue
            
            # Create skipped lesson
            lesson_time = get_lesson_time_for_day(student, today.weekday())
            lesson = Lesson(
                coach_id=coach.id,
                student_id=student.id,
                date=today,
                time=lesson_time,
                location=student.location,
                notes=f"Отмена: {reason_text}"
            )
            s.add(lesson)
            await s.flush()
            
            # Mark as excused
            att = Attendance(
                lesson_id=lesson.id,
                student_id=student.id,
                status="excused"
            )
            s.add(att)
            skipped_count += 1
        
        await s.commit()
    
    await callback.message.edit_text(
        f"✅ Сохранено\n\n"
        f"Тренировка отменена: {reason_text}\n"
        f"Учеников: {skipped_count}"
    )


@router.callback_query(F.data == "cancel_skip")
async def cb_cancel_skip(callback: CallbackQuery):
    """Cancel skip action."""
    await callback.message.delete()


@router.callback_query(F.data.startswith("quick_group:"))
async def cb_quick_group(callback: CallbackQuery):
    """Open CRM for specific group attendance."""
    coach = await get_coach(callback.from_user.id)
    if not coach:
        await callback.answer("Нет доступа")
        return
    
    time_key = callback.data.split(":")[1]
    
    # Open WebApp with parameter for specific time
    webapp_url = WEBAPP_URL or "https://your-app.up.railway.app"
    
    await callback.message.edit_text(
        f"📋 Тренировка {time_key}\n\n"
        f"Откройте CRM для отметки посещаемости:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="✅ Отметить в CRM",
                web_app=WebAppInfo(url=f"{webapp_url}/?time={time_key}")
            )]
        ])
    )


@router.callback_query(F.data.startswith("skip_group:"))
async def cb_skip_group(callback: CallbackQuery):
    """Skip specific group lesson."""
    coach = await get_coach(callback.from_user.id)
    if not coach:
        await callback.answer("Нет доступа")
        return
    
    time_key = callback.data.split(":")[1]
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎉 Праздник", callback_data=f"skip_group_reason:{time_key}:holiday")],
        [InlineKeyboardButton(text="🤒 Тренер болеет", callback_data=f"skip_group_reason:{time_key}:sick")],
        [InlineKeyboardButton(text="🏠 Другое", callback_data=f"skip_group_reason:{time_key}:other")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_skip")]
    ])
    
    await callback.message.edit_text(
        f"❌ Тренировка {time_key} отменена\n\nВыберите причину:",
        reply_markup=kb
    )


@router.callback_query(F.data.startswith("skip_group_reason:"))
async def cb_skip_group_reason(callback: CallbackQuery):
    """Handle group skip with reason."""
    coach = await get_coach(callback.from_user.id)
    if not coach:
        await callback.answer("Нет доступа")
        return
    
    parts = callback.data.split(":")
    time_key = parts[1]
    reason = parts[2]
    reason_text = {"holiday": "Праздник", "sick": "Тренер болеет", "other": "Другое"}.get(reason, "Другое")
    
    from datetime import date
    today = date.today()
    current_weekday = today.weekday()
    
    async with async_session() as s:
        # Get students for this specific time
        result = await s.execute(
            select(Student).where(
                Student.coach_id == coach.id,
                Student.is_active == True
            )
        )
        students = result.scalars().all()
        
        skipped_count = 0
        for student in students:
            days = student.lesson_days.split(",") if student.lesson_days else []
            if str(current_weekday) in days:
                student_time = get_lesson_time_for_day(student, current_weekday)
                if student_time == time_key:
                    # Check if already marked
                    existing = await s.execute(
                        select(Lesson).where(
                            Lesson.student_id == student.id,
                            Lesson.date == today
                        )
                    )
                    if existing.scalar_one_or_none():
                        continue
                    
                    # Create skipped lesson
                    lesson = Lesson(
                        coach_id=coach.id,
                        student_id=student.id,
                        date=today,
                        time=time_key,
                        location=student.location,
                        notes=f"Отмена: {reason_text}"
                    )
                    s.add(lesson)
                    await s.flush()
                    
                    # Mark as excused
                    att = Attendance(
                        lesson_id=lesson.id,
                        student_id=student.id,
                        status="excused"
                    )
                    s.add(att)
                    skipped_count += 1
        
        await s.commit()
    
    await callback.message.edit_text(
        f"✅ Сохранено\n\n"
        f"Тренировка {time_key} отменена: {reason_text}\n"
        f"Учеников: {skipped_count}"
    )


# === Admin Commands ===

@router.message(Command("coaches"))
async def cmd_coaches(message: Message):
    if not await is_admin(message.from_user.id):
        await message.answer("⛔ Только для администраторов")
        return
    
    async with async_session() as s:
        result = await s.execute(select(Coach).where(Coach.is_active == True))
        coaches = result.scalars().all()
    
    if not coaches:
        await message.answer("Нет зарегистрированных тренеров.")
        return
    
    text = f"<b>👥 Зарегистрированные тренеры ({len(coaches)}):</b>\n\n"
    for c in coaches:
        reg_date = c.created_at.strftime('%d.%m.%Y') if c.created_at else '—'
        text += f"• <b>{c.first_name or 'Без имени'}</b>\n"
        text += f"  ID: <code>{c.telegram_id}</code>\n"
        text += f"  @{c.username or 'нет username'}\n"
        text += f"  Дата: {reg_date}\n\n"
    
    await message.answer(text, parse_mode="HTML")


@router.message(Command("stats"))
async def cmd_stats(message: Message):
    if not await is_admin(message.from_user.id):
        return
    
    async with async_session() as s:
        coaches_count = await s.execute(select(func.count(Coach.id)))
        students_count = await s.execute(select(func.count(Student.id)))
        lessons_count = await s.execute(select(func.count(Lesson.id)))
        payments_total = await s.execute(
            select(func.sum(Payment.amount)).where(Payment.status == "paid")
        )
    
    text = f"""📊 <b>Статистика CRM:</b>

👥 Тренеров: {coaches_count.scalar()}
🎓 Учеников: {students_count.scalar()}
📚 Проведено занятий: {lessons_count.scalar()}
💰 Всего оплачено: {payments_total.scalar() or 0}Br"""
    
    await message.answer(text, parse_mode="HTML")


# === Notification helpers ===

async def notify_coach_payment_due(coach_id: int, student_name: str, days_left: int):
    """Send notification to coach about ending subscription."""
    async with async_session() as s:
        coach = await s.get(Coach, coach_id)
        if not coach:
            return
        
        try:
            if days_left < 0:
                text = f"⚠️ Абонемент <b>{student_name}</b> просрочен!"
            else:
                text = f"⏳ У <b>{student_name}</b> осталось {days_left} дн. абонемента"
            
            await bot.send_message(coach.telegram_id, text, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Failed to notify coach {coach_id}: {e}")


# === Daily Notifications ===

async def should_send_daily_notification(coach_id: int, notification_type: str) -> bool:
    """Check if daily notification was already sent today."""
    from app.models import DailyNotificationLog
    
    today = date.today()
    
    async with async_session() as s:
        result = await s.execute(
            select(DailyNotificationLog).where(
                DailyNotificationLog.coach_id == coach_id,
                DailyNotificationLog.notification_type == notification_type,
                DailyNotificationLog.date == today
            )
        )
        return result.scalar_one_or_none() is None


async def mark_notification_sent(coach_id: int, notification_type: str):
    """Mark notification as sent for today."""
    from app.models import DailyNotificationLog
    
    log = DailyNotificationLog(
        coach_id=coach_id,
        notification_type=notification_type,
        date=date.today()
    )
    async with async_session() as s:
        s.add(log)
        await s.commit()


async def send_daily_summary(coach_id: int = None):
    """Send daily summary to coach(es). If coach_id is None, send to all coaches."""
    from app.models import DailyNotificationLog
    
    today = date.today()
    
    async with async_session() as s:
        if coach_id:
            coaches_result = await s.execute(
                select(Coach).where(Coach.id == coach_id)
            )
        else:
            coaches_result = await s.execute(
                select(Coach).where(Coach.is_active == True)
            )
        
        coaches = coaches_result.scalars().all()
        
        for coach in coaches:
            # Check if already sent today
            already_sent = await s.execute(
                select(DailyNotificationLog).where(
                    DailyNotificationLog.coach_id == coach.id,
                    DailyNotificationLog.notification_type == "daily_summary",
                    DailyNotificationLog.date == today
                )
            )
            if already_sent.scalar_one_or_none():
                continue
            
            # Get all active students
            students_result = await s.execute(
                select(Student).where(
                    Student.coach_id == coach.id,
                    Student.is_active == True
                )
            )
            students = students_result.scalars().all()
            
            # Categorize students
            expired = []       # Subscription ended
            ending_soon = []   # 1-3 days left
            low_lessons = []   # 1-2 lessons remaining
            depleted = []      # 0 lessons remaining
            
            for student in students:
                # Check subscription expiry
                if student.subscription_end:
                    days_left = (student.subscription_end - today).days
                    if days_left < 0:
                        expired.append({
                            "name": student.name,
                            "days": abs(days_left)
                        })
                    elif days_left <= 3:
                        ending_soon.append({
                            "name": student.name,
                            "days": days_left
                        })
                
                # Check lessons remaining (only for non-unlimited subscriptions)
                if not getattr(student, 'is_unlimited', False):
                    if student.lessons_remaining <= 0:
                        depleted.append({
                            "name": student.name,
                            "remaining": 0
                        })
                    elif student.lessons_remaining <= 2:
                        low_lessons.append({
                            "name": student.name,
                            "remaining": student.lessons_remaining
                        })
            
            # Only send if there are alerts
            total_alerts = len(expired) + len(ending_soon) + len(low_lessons) + len(depleted)
            
            if total_alerts > 0 or True:  # Send even if no alerts (for testing)
                text = f"📊 <b>Ежедневная сводка ({today.strftime('%d.%m.%Y')})</b>\n\n"
                
                # Urgent: expired subscriptions
                if expired:
                    text += f"🚨 <b>Просрочена оплата ({len(expired)}):</b>\n"
                    for item in expired:
                        text += f"  • {item['name']} — {item['days']} дн. назад\n"
                    text += "\n"
                
                # Ending soon
                if ending_soon:
                    text += f"⏰ <b>Заканчивается абонемент ({len(ending_soon)}):</b>\n"
                    for item in ending_soon:
                        day_word = "день" if item['days'] == 1 else "дня" if item['days'] < 5 else "дней"
                        text += f"  • {item['name']} — {item['days']} {day_word}\n"
                    text += "\n"
                
                # Depleted lessons
                if depleted:
                    text += f"❌ <b>Закончились занятия ({len(depleted)}):</b>\n"
                    for item in depleted:
                        text += f"  • {item['name']}\n"
                    text += "\n"
                
                # Low lessons
                if low_lessons:
                    text += f"⚠️ <b>Осталось мало занятий ({len(low_lessons)}):</b>\n"
                    for item in low_lessons:
                        lesson_word = "занятие" if item['remaining'] == 1 else "занятия"
                        text += f"  • {item['name']} — {item['remaining']} {lesson_word}\n"
                    text += "\n"
                
                if total_alerts == 0:
                    text += "✅ Все абонементы в порядке!\n\n"
                
                # Add today's schedule
                weekday = today.weekday()
                today_lessons = []
                for student in students:
                    days = student.lesson_days.split(",") if student.lesson_days else []
                    if str(weekday) in days:
                        today_lessons.append({
                            "name": student.name,
                            "time": get_lesson_time_for_day(student, weekday),
                            "remaining": student.lessons_remaining,
                            "is_unlimited": getattr(student, 'is_unlimited', False)
                        })
                
                if today_lessons:
                    # Group by time
                    by_time = {}
                    for lesson in today_lessons:
                        time_key = lesson["time"]
                        if time_key not in by_time:
                            by_time[time_key] = []
                        by_time[time_key].append(lesson)
                    
                    text += f"📅 <b>Сегодняшние тренировки ({len(today_lessons)}):</b>\n"
                    for time_key in sorted(by_time.keys()):
                        lessons = by_time[time_key]
                        text += f"\n🕐 {time_key} ({len(lessons)} учеников)\n"
                        for lesson in lessons[:5]:  # Show max 5 per group
                            if lesson.get("is_unlimited"):
                                status = " ♾️"  # Unlimited symbol
                            elif lesson["remaining"] <= 0:
                                status = " ❌"
                            elif lesson["remaining"] <= 2:
                                status = " ⚠️"
                            else:
                                status = ""
                            text += f"  • {lesson['name']}{status}\n"
                        if len(lessons) > 5:
                            text += f"  ... и ещё {len(lessons) - 5} учеников\n"
                
                # Add action button
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(
                        text="📱 Открыть CRM",
                        web_app=WebAppInfo(url=f"{WEBAPP_URL or 'https://your-app.up.railway.app'}/")
                    )]
                ])
                
                try:
                    await bot.send_message(coach.telegram_id, text, parse_mode="HTML", reply_markup=kb)
                    
                    # Mark as sent
                    log = DailyNotificationLog(
                        coach_id=coach.id,
                        notification_type="daily_summary",
                        date=today
                    )
                    s.add(log)
                    await s.commit()
                    
                    logger.info(f"Daily summary sent to coach {coach.id}")
                except Exception as e:
                    logger.error(f"Failed to send daily summary to coach {coach.id}: {e}")


@router.message(Command("summary"))
async def cmd_summary(message: Message):
    """Manually request daily summary."""
    user_id = message.from_user.id
    
    # Check if coach
    async with async_session() as s:
        result = await s.execute(select(Coach).where(Coach.telegram_id == user_id))
        coach = result.scalar_one_or_none()
    
    if not coach:
        await message.answer("❌ У вас нет доступа")
        return
    
    await message.answer("⏳ Формирую сводку...")
    await send_daily_summary(coach.id)
    await message.answer("✅ Сводка отправлена!")


# === Lesson Reminder Scheduler ===

async def lesson_reminder_scheduler():
    """Background task: check for unmarked lessons every 5 minutes."""
    from app.models import Notification
    
    while True:
        try:
            await asyncio.sleep(300)  # 5 minutes
            
            now = datetime.now()
            current_weekday = now.weekday()
            current_date = now.date()
            current_time = now.strftime("%H:%M")
            
            async with async_session() as s:
                # Get all coaches
                coaches_result = await s.execute(select(Coach).where(Coach.is_active == True))
                coaches = coaches_result.scalars().all()
                
                for coach in coaches:
                    # Get students with lessons at this time
                    students_result = await s.execute(
                        select(Student).where(
                            Student.coach_id == coach.id,
                            Student.is_active == True
                        )
                    )
                    students = students_result.scalars().all()
                    
                    for student in students:
                        days = student.lesson_days.split(",") if student.lesson_days else []
                        if str(current_weekday) not in days:
                            continue
                        
                        # Check if lesson time matches (within 5 min window)
                        lesson_time = get_lesson_time_for_day(student, current_weekday)
                        lesson_hour, lesson_min = map(int, lesson_time.split(":"))
                        lesson_start = lesson_hour * 60 + lesson_min
                        now_total = now.hour * 60 + now.minute
                        
                        # Only notify at lesson start time (within 5 min window)
                        if not (0 <= now_total - lesson_start <= 5):
                            continue
                        
                        # Check if already notified for this lesson
                        existing_notification = await s.execute(
                            select(Notification).where(
                                Notification.coach_id == coach.id,
                                Notification.type == "lesson_reminder",
                                Notification.created_at >= now - timedelta(minutes=30)
                            )
                        )
                        if existing_notification.scalar_one_or_none():
                            continue
                        
                        # Check if lesson already marked
                        existing_lesson = await s.execute(
                            select(Lesson).where(
                                Lesson.student_id == student.id,
                                Lesson.date == current_date
                            )
                        )
                        if existing_lesson.scalar_one_or_none():
                            continue
                        
                        # Send reminder
                        await send_lesson_reminder(coach, [student], lesson_time)
                        
                        # Log notification
                        notification = Notification(
                            coach_id=coach.id,
                            student_id=student.id,
                            type="lesson_reminder",
                            message=f"Напоминание о тренировке {lesson_time}"
                        )
                        s.add(notification)
                        await s.commit()
                        
        except Exception as e:
            logger.error(f"Error in reminder scheduler: {e}")
            await asyncio.sleep(60)


async def send_lesson_reminder(coach: Coach, students: list, time_str: str):
    """Send lesson reminder notification to coach."""
    student_names = ", ".join([s.name for s in students[:3]])
    if len(students) > 3:
        student_names += f" и ещё {len(students) - 3}"
    
    text = f"⏰ <b>Тренировка {time_str}</b>\n\n"
    text += f"👥 Не отмечены ({len(students)}):\n"
    for st in students:
        # Show remaining lessons indicator (skip for unlimited)
        if getattr(st, 'is_unlimited', False):
            text += f"• {st.name} ♾️\n"
            continue
        
        remaining = getattr(st, 'lessons_remaining', None)
        if remaining is not None:
            if remaining <= 0:
                indicator = " ❌"
            elif remaining <= 2:
                indicator = " ⚠️"
            else:
                indicator = ""
            text += f"• {st.name}{indicator}\n"
        else:
            text += f"• {st.name}\n"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 Открыть CRM", web_app=WebAppInfo(url=f"{WEBAPP_URL or 'https://your-app.up.railway.app'}/"))],
        [InlineKeyboardButton(text="❌ Тренировки нет", callback_data="skip_lesson")]
    ])
    
    try:
        await bot.send_message(coach.telegram_id, text, parse_mode="HTML", reply_markup=kb)
    except Exception as e:
        logger.error(f"Failed to send reminder to coach {coach.id}: {e}")


# === Daily Notification Scheduler ===

async def daily_notification_scheduler():
    """Background task: send daily summaries at 9:00 AM."""
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
            
        except Exception as e:
            logger.error(f"Error in daily notification scheduler: {e}")
            await asyncio.sleep(3600)  # Retry in 1 hour
