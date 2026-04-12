import logging
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
    
    # Coach registered - show main menu
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
        "Быстрые действия:",
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
                time_key = st.lesson_time or "18:00"
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
        text += f"• <b>{st.name}</b>\n  📍 {st.location} | 🕐 {days_str} {st.lesson_time}\n  💰 {st.lesson_price} Br/{st.lessons_count} занятий\n\n"
    
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
                lesson_time = st.lesson_time or "18:00"
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
            lesson = Lesson(
                coach_id=coach.id,
                student_id=student.id,
                date=today,
                time=student.lesson_time,
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
💰 Всего оплачено: {payments_total.scalar() or 0}₽"""
    
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
