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
    
    # Check if admin first
    if await is_admin(user_id):
        await message.answer(
            "👑 <b>Админ-панель CRM Break Wave</b>\n\n"
            "Вы администратор системы.\n"
            "Команды:\n"
            "/coaches - список тренеров\n"
            "/add_coach - добавить тренера\n"
            "/stats - общая статистика",
            parse_mode="HTML"
        )
        return
    
    # Check if coach
    coach = await get_coach(user_id)
    if not coach:
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
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Используй: /coach <секретный код>")
        return
    
    if parts[1].strip() != ADMIN_SECRET:
        await message.answer("❌ Неверный код.")
        return
    
    registered = await register_coach(
        message.from_user.id,
        message.from_user.first_name,
        message.from_user.username
    )
    
    if registered:
        await message.answer(
            "✅ <b>Вы зарегистрированы как тренер!</b>\n\n"
            "Теперь вы можете использовать CRM систему.\n"
            "Нажмите /start чтобы открыть приложение."
        )
    else:
        await message.answer("👋 Вы уже зарегистрированы! Нажмите /start")


@router.message(Command("help"))
async def cmd_help(message: Message):
    text = """📋 <b>Помощь по CRM Break Wave</b>

<b>Основные возможности:</b>
• Ученики — база с индивидуальными настройками
• Расписание — дни, время, место занятий
• Посещаемость — отметка на каждом занятии
• Оплата — контроль абонементов

<b>Команды:</b>
/start — Главное меню
/help — Эта справка

Откройте Mini App для полного функционала."""
    await message.answer(text, parse_mode="HTML")


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
        text += f"• <b>{st.name}</b>\n  📍 {st.location} | 🕐 {days_str} {st.lesson_time}\n  💰 {st.lesson_price}₽/{st.lessons_count} занятий\n\n"
    
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


# === Admin Commands ===

@router.message(Command("coaches"))
async def cmd_coaches(message: Message):
    if not await is_admin(message.from_user.id):
        return
    
    async with async_session() as s:
        result = await s.execute(select(Coach).where(Coach.is_active == True))
        coaches = result.scalars().all()
    
    if not coaches:
        await message.answer("Нет зарегистрированных тренеров.")
        return
    
    text = "<b>👥 Тренеры:</b>\n\n"
    for c in coaches:
        text += f"• {c.first_name or 'Без имени'} (@{c.username or 'нет'}) — ID: {c.telegram_id}\n"
    
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
