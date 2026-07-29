import asyncio
import hmac
import logging
import json
import os
from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    WebAppInfo, CallbackQuery, Message
)
from aiogram.exceptions import TelegramForbiddenError
from sqlalchemy import select, and_, desc, or_, func
from sqlalchemy.orm import selectinload
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from app.database import async_session

BELARUS_TZ = ZoneInfo('Europe/Minsk')
from app.models import (
    Coach,
    Student,
    Lesson,
    Attendance,
    Payment,
    AdminUser,
    StudentSchedule,
    Notification,
    ParentAccount,
    RegistrationInvite,
    RegistrationRequest,
    ScheduleRequest,
    TrainingResponse,
    MakeupCredit,
)
from app.config import BOT_TOKEN, ADMIN_IDS, ADMIN_SECRET, WEBAPP_URL
from app.family import (
    ATTENDANCE_STATUSES,
    TARIFFS,
    is_payment_reminder_day,
    month_bounds,
    payment_due_date,
    payment_total,
    reconcile_attendance,
)


def get_remaining_lessons(student: Student) -> int:
    """Normalize remaining lessons for legacy and current records."""
    if getattr(student, "lessons_remaining", None) is None:
        return getattr(student, "lessons_count", 0) or 0
    return student.lessons_remaining

logger = logging.getLogger(__name__)

LESSON_REMINDER_WINDOW_MINUTES = 10
LESSON_REMINDER_POLL_SECONDS = 60
DAILY_SUMMARY_HOUR = 10
DAILY_SUMMARY_MINUTE = 0
PARENT_REMINDER_HOUR = 18
FAMILY_SCHEDULER_POLL_SECONDS = 60


def build_lesson_reminder_log_key(reminder_date: date, time_str: str) -> str:
    """Stable key for deduplicating reminder notifications in the DB."""
    return f"[lesson_reminder:{reminder_date.isoformat()}:{time_str}]"


def get_runtime_version_label() -> str | None:
    """Build a stable version label for deploy/update notifications."""
    commit_sha = os.getenv("RAILWAY_GIT_COMMIT_SHA") or os.getenv("RENDER_GIT_COMMIT")
    if commit_sha:
        return commit_sha[:7]

    deployment_id = os.getenv("RAILWAY_DEPLOYMENT_ID") or os.getenv("APP_VERSION")
    if deployment_id:
        return deployment_id[:12]

    return None


async def notify_version_update() -> None:
    """Notify active coaches once per deployed version that the bot was updated."""
    if not bot:
        return

    version_label = get_runtime_version_label()
    if not version_label:
        logger.info("Skip version update notification: runtime version label not available")
        return

    sent_at = datetime.now(BELARUS_TZ)
    log_key = f"[version_update:{version_label}]"
    message_text = (
        "🔄 CRM Break Wave обновлена\n"
        f"Версия: {version_label}\n"
        f"Время: {sent_at.strftime('%d.%m.%Y %H:%M')}\n"
        "Бот и mini app запущены."
    )

    async with async_session() as s:
        result = await s.execute(
            select(Coach).where(Coach.is_active == True)
        )
        coaches = result.scalars().all()
        if not coaches:
            logger.info("Skip version update notification: no active coaches")
            return

        for coach in coaches:
            existing_notification = await s.execute(
                select(Notification).where(
                    Notification.coach_id == coach.id,
                    Notification.type == "version_update",
                    Notification.message.like(f"{log_key}%")
                )
            )
            if existing_notification.scalar_one_or_none():
                continue

            try:
                await bot.send_message(coach.telegram_id, message_text)
                s.add(Notification(
                    coach_id=coach.id,
                    student_id=None,
                    type="version_update",
                    message=f"{log_key} {message_text}"
                ))
            except Exception as exc:
                logger.warning(f"Failed to send version update notification to coach {coach.id}: {exc}")

        await s.commit()


def create_bot() -> Bot:
    """Create bot instance lazily so module import does not crash without env."""
    global bot
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not configured")
    try:
        bot = Bot(token=BOT_TOKEN)
        return bot
    except Exception as exc:
        raise RuntimeError(f"Invalid BOT_TOKEN: {exc}") from exc


bot = None
dp = Dispatcher()
router = Router()
dp.include_router(router)

# === Helpers ===

async def is_admin(user_id: int) -> bool:
    if ADMIN_IDS and user_id in ADMIN_IDS:
        return True
    async with async_session() as s:
        result = await s.execute(select(AdminUser).where(AdminUser.telegram_id == user_id))
        if result.scalar_one_or_none() is not None:
            return True
        coach_result = await s.execute(
            select(Coach).where(
                Coach.telegram_id == user_id,
                Coach.is_active == True,
                Coach.is_admin == True,
            )
        )
        return coach_result.scalar_one_or_none() is not None


async def get_coach(user_id: int):
    async with async_session() as s:
        result = await s.execute(select(Coach).where(Coach.telegram_id == user_id))
        return result.scalar_one_or_none()


async def get_parent(user_id: int):
    async with async_session() as s:
        result = await s.execute(
            select(ParentAccount).where(ParentAccount.telegram_id == user_id)
        )
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


# === Commands ===

@router.message(CommandStart())
async def cmd_start(message: Message):
    user_id = message.from_user.id
    logger.info(f"Start command from user: {user_id} ({message.from_user.first_name})")

    start_parts = (message.text or "").split(maxsplit=1)
    start_payload = start_parts[1].strip() if len(start_parts) > 1 else ""
    if start_payload.startswith("invite_"):
        invite_token = start_payload.removeprefix("invite_")
        async with async_session() as s:
            invite_result = await s.execute(
                select(RegistrationInvite).where(
                    RegistrationInvite.token == invite_token,
                    RegistrationInvite.status == "active",
                    RegistrationInvite.expires_at > datetime.utcnow(),
                )
            )
            invite = invite_result.scalar_one_or_none()
        if not invite:
            await message.answer(
                "Ссылка уже использована или истекла. Попросите тренера создать новое приглашение."
            )
            return
        webapp_url = WEBAPP_URL or "https://your-app.up.railway.app"
        await message.answer(
            "👋 <b>Регистрация в Break Wave</b>\n\n"
            f"Ребёнок: <b>{invite.preliminary_child_name}</b>\n"
            "Заполните короткую анкету и согласованное с тренером расписание.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="Заполнить анкету",
                    web_app=WebAppInfo(url=f"{webapp_url}/?invite={invite_token}"),
                )
            ]]),
        )
        return

    parent = await get_parent(user_id)
    if parent:
        if parent.bot_blocked_at:
            async with async_session() as s:
                stored_parent = await s.get(ParentAccount, parent.id)
                if stored_parent:
                    stored_parent.bot_blocked_at = None
                    await s.commit()
        webapp_url = WEBAPP_URL or "https://your-app.up.railway.app"
        await message.answer(
            f"👋 {parent.full_name}, семейный кабинет Break Wave готов.\n\n"
            "Здесь доступны расписание, абонементы, оплаты и отработки.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="Открыть семейный кабинет",
                    web_app=WebAppInfo(url=f"{webapp_url}/"),
                )
            ]]),
        )
        return
    
    # Check roles
    is_admin_user = await is_admin(user_id)
    coach = await get_coach(user_id)
    if is_admin_user and not coach:
        await register_coach(
            user_id,
            message.from_user.first_name,
            message.from_user.username,
        )
        coach = await get_coach(user_id)
    
    # Admins get the same operational CRM entry point plus admin commands.
    if is_admin_user:
        admin_text = (
            "👑 <b>Админ-панель CRM Break Wave</b>\n\n"
            "Админ-команды:\n"
            "/coaches - список тренеров\n"
            "/stats - общая статистика\n\n"
        )

        webapp_url = WEBAPP_URL or "https://your-app.up.railway.app"
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="📱 Открыть CRM",
                web_app=WebAppInfo(url=f"{webapp_url}/"),
            )
        ]])

        await message.answer(
            admin_text + "Ученики, расписание, посещаемость и деньги собраны в одном приложении.",
            parse_mode="HTML",
            reply_markup=kb,
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
    now = datetime.now(BELARUS_TZ)
    current_weekday = now.weekday()
    current_date = now.date()
    
    async with async_session() as s:
        # Get students grouped by time
        result = await s.execute(
            select(Student).options(
                selectinload(Student.schedules).selectinload(StudentSchedule.location)
            ).where(
                Student.coach_id == coach.id,
                Student.is_active == True
            )
        )
        students = result.scalars().all()
        
        # Group by time and check for unmarked (using new schedule system)
        groups = {}
        for st in students:
            # Use new schedule system
            schedules = st.get_schedules_for_day(current_weekday)
            for sched_info in schedules:
                time_key = sched_info["time"]
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
                            Lesson.date == current_date,
                            Lesson.time == time_key
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
        )]
    ])
    
    await message.answer(
        f"👋 Привет, {coach.first_name or 'тренер'}!\n\n"
        "<b>CRM Break Wave</b> — ученики, расписание, посещаемость и деньги в одном месте.\n\n"
        f"Сейчас активных тренировок нет · {now.strftime('%H:%M')}",
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

    if not ADMIN_SECRET:
        logger.error("Coach self-registration is disabled because ADMIN_SECRET is not configured")
        await message.answer("❌ Регистрация временно отключена. Обратитесь к администратору.")
        return

    if not hmac.compare_digest(parts[1].strip(), ADMIN_SECRET):
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
        text += "/requests - заявки, расписание и чеки\n"
        text += "/payments - оплаты и долги\n"
        text += "/makeups - отработки\n\n"
    
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


async def _send_admin_mini_app_entry(message: Message, screen: str, title: str) -> None:
    if not await is_admin(message.from_user.id):
        await message.answer("⛔ Только для руководителей школы")
        return
    webapp_url = WEBAPP_URL or "https://your-app.up.railway.app"
    await message.answer(
        title,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Открыть в CRM",
                web_app=WebAppInfo(url=f"{webapp_url}/?screen={screen}"),
            )
        ]]),
    )


@router.message(Command("requests"))
async def cmd_requests(message: Message):
    await _send_admin_mini_app_entry(message, "requests", "Запросы родителей, расписание и чеки")


@router.message(Command("payments"))
async def cmd_payments(message: Message):
    await _send_admin_mini_app_entry(message, "finance", "Оплаты, просрочки и долги")


@router.message(Command("makeups"))
async def cmd_makeups(message: Message):
    await _send_admin_mini_app_entry(message, "requests", "Запросы и сроки отработок")


@router.message(F.photo | F.document)
async def receive_parent_receipt(message: Message):
    """Attach a receipt to the latest online payment waiting for a file."""
    parent = await get_parent(message.from_user.id)
    if not parent:
        return

    file_id = (
        message.photo[-1].file_id
        if message.photo
        else (message.document.file_id if message.document else None)
    )
    if not file_id:
        return

    async with async_session() as s:
        result = await s.execute(
            select(Payment, Student)
            .join(Student)
            .where(
                Student.parent_id == parent.id,
                Payment.status == "awaiting_receipt",
                Payment.payment_method == "online",
            )
            .order_by(desc(Payment.reported_at), desc(Payment.id))
        )
        row = result.first()
        if not row:
            await message.answer(
                "Сейчас нет онлайн-оплаты, ожидающей чек. Сначала выберите оплату в Mini App."
            )
            return

        payment, student = row
        payment.receipt_file_id = file_id
        payment.status = "reported"
        payment.reported_at = datetime.utcnow()

        admins_result = await s.execute(
            select(Coach).where(
                Coach.is_active == True,
                or_(
                    Coach.is_admin == True,
                    Coach.telegram_id.in_(ADMIN_IDS or [-1]),
                ),
            )
        )
        for admin in admins_result.scalars().all():
            receipt_caption = (
                f"💳 Чек на проверку\n"
                f"Ребёнок: {student.name}\n"
                f"Сумма: {payment.amount} Br"
            )
            try:
                if message.photo:
                    await bot.send_photo(admin.telegram_id, file_id, caption=receipt_caption)
                else:
                    await bot.send_document(admin.telegram_id, file_id, caption=receipt_caption)
            except Exception as exc:
                logger.warning("Could not forward receipt to admin %s: %s", admin.id, exc)
            s.add(Notification(
                coach_id=admin.id,
                student_id=student.id,
                type="payment_review",
                message=(
                    f"💳 Новый чек на проверку\n"
                    f"Ребёнок: {student.name}\n"
                    f"Сумма: {payment.amount} Br\n"
                    "Откройте раздел «Запросы» в CRM."
                ),
                recipient_telegram_id=admin.telegram_id,
            ))
        await s.commit()

    await message.answer("✅ Чек прикреплён и отправлен администраторам на проверку.")


@router.message(Command("now"))
async def cmd_now(message: Message):
    """Show current lesson for quick attendance."""
    coach = await get_coach(message.from_user.id)
    if not coach:
        await message.answer("❌ У вас нет доступа")
        return
    
    from datetime import datetime
    now = datetime.now(BELARUS_TZ)
    current_weekday = now.weekday()
    current_date = now.date()
    
    async with async_session() as s:
        result = await s.execute(
            select(Student).options(
                selectinload(Student.schedules).selectinload(StudentSchedule.location)
            ).where(
                Student.coach_id == coach.id,
                Student.is_active == True
            )
        )
        students = result.scalars().all()
        
        # Group by time (using new schedule system)
        groups = {}
        for st in students:
            # Use new schedule system
            schedules = st.get_schedules_for_day(current_weekday)
            for sched_info in schedules:
                time_key = sched_info["time"]
                if time_key not in groups:
                    groups[time_key] = []
                
                # Check if marked
                existing = await s.execute(
                    select(Lesson).where(
                        Lesson.student_id == st.id,
                        Lesson.date == current_date,
                        Lesson.time == time_key
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
                    [InlineKeyboardButton(text="❌ Тренировки нет", callback_data=f"skip_group:{time_key}")]
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
            select(Student).options(
                selectinload(Student.schedules).selectinload(StudentSchedule.location)
            ).where(Student.coach_id == coach.id, Student.is_active == True)
        )
        students = result.scalars().all()
    
    if not students:
        await callback.message.edit_text(
            "У вас пока нет учеников.\n\n"
            "Добавьте первого ученика через Mini App."
        )
        return
    
    text = f"👥 <b>Ваши ученики ({len(students)}):</b>\n\n"
    day_names = {"0":"Пн","1":"Вт","2":"Ср","3":"Чт","4":"Пт","5":"Сб","6":"Вс"}
    for st in students:
        schedules_info = []
        if st.schedules:
            for sched in st.schedules:
                days = sched.days or ""
                days_str = ",".join([day_names.get(d.strip(), d.strip()) for d in days.split(",") if d.strip()])
                try:
                    times = json.loads(sched.times or '{}')
                    time_str = times.get('default', '18:00')
                except:
                    time_str = '18:00'
                loc = sched.location.name if sched.location else (st.location or 'Зал')
                schedules_info.append(f"🕐 {days_str} {time_str} 📍 {loc}")
        else:
            # Legacy fallback
            days = st.lesson_days or "1,3"
            days_str = ",".join([day_names.get(d.strip(), d.strip()) for d in days.split(",") if d.strip()])
            try:
                times = json.loads(st.lesson_times or '{}')
                time_str = times.get('default', '18:00')
            except:
                time_str = '18:00'
            schedules_info.append(f"🕐 {days_str} {time_str} 📍 {st.location or 'Зал'}")
        
        text += f"• <b>{st.name}</b>\n"
        for info in schedules_info:
            text += f"  {info}\n"
        balance = "безлимит" if st.is_unlimited else f"осталось {get_remaining_lessons(st)}"
        text += f"  🎟 {balance}\n\n"
    
    await callback.message.edit_text(text, parse_mode="HTML")


@router.callback_query(F.data == "check_payments")
async def cb_check_payments(callback: CallbackQuery):
    coach = await get_coach(callback.from_user.id)
    if not coach:
        await callback.answer("Нет доступа")
        return
    
    today = datetime.now(BELARUS_TZ).date()
    
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
    now = datetime.now(BELARUS_TZ)
    current_weekday = now.weekday()
    current_date = now.date()
    
    async with async_session() as s:
        result = await s.execute(
            select(Student).options(
                selectinload(Student.schedules).selectinload(StudentSchedule.location)
            ).where(
                Student.coach_id == coach.id,
                Student.is_active == True
            )
        )
        students = result.scalars().all()
        
        # Filter students for current time (using new schedule system)
        current_students = []
        for st in students:
            # Use new schedule system
            schedules = st.get_schedules_for_day(current_weekday)
            for sched_info in schedules:
                lesson_time = sched_info["time"]
                lesson_hour, lesson_min = map(int, lesson_time.split(":"))
                lesson_start = lesson_hour * 60 + lesson_min
                now_total = now.hour * 60 + now.minute
                
                # Within lesson time window
                if -15 <= now_total - lesson_start <= 90:
                    # Check if already marked
                    existing = await s.execute(
                        select(Lesson).where(
                            Lesson.student_id == st.id,
                            Lesson.date == current_date,
                            Lesson.time == lesson_time
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
    
    today = datetime.now(BELARUS_TZ).date()
    current_weekday = today.weekday()
    
    async with async_session() as s:
        # Get all students for this coach
        result = await s.execute(
            select(Student).options(
                selectinload(Student.schedules).selectinload(StudentSchedule.location)
            ).where(
                Student.coach_id == coach.id,
                Student.is_active == True
            )
        )
        students = result.scalars().all()
        
        skipped_count = 0
        for student in students:
            for sched_info in student.get_schedules_for_day(current_weekday):
                lesson_time = sched_info["time"]
                existing = await s.execute(
                    select(Lesson).where(
                        Lesson.student_id == student.id,
                        Lesson.date == today,
                        Lesson.time == lesson_time
                    )
                )
                if existing.scalar_one_or_none():
                    continue
                
                lesson = Lesson(
                    coach_id=coach.id,
                    student_id=student.id,
                    date=today,
                    time=lesson_time,
                    location=sched_info.get("location_name", student.location),
                    location_id=sched_info.get("location_id"),
                    notes=f"Отмена: {reason_text}"
                )
                s.add(lesson)
                await s.flush()
                
                att = Attendance(
                    lesson_id=lesson.id,
                    student_id=student.id,
                    location_id=sched_info.get("location_id"),
                    status="excused",
                    source="coach_cancelled",
                    deducted=False,
                    attendance_date=today,
                    attendance_time=lesson_time
                )
                s.add(att)
                await reconcile_attendance(s, student, att)
                if student.parent_id:
                    parent = await s.get(ParentAccount, student.parent_id)
                    if parent:
                        s.add(Notification(
                            coach_id=student.coach_id,
                            student_id=student.id,
                            type="training_cancelled",
                            message=(
                                f"❌ Тренировка отменена\n{student.name} · "
                                f"{today.strftime('%d.%m.%Y')} в {lesson_time}\n"
                                f"Причина: {reason_text}\n"
                                "Занятие не списано, право на отработку добавлено."
                            ),
                            recipient_telegram_id=parent.telegram_id,
                        ))
                skipped_count += 1
        
        await s.commit()
    
    await callback.message.edit_text(
        f"✅ Сохранено\n\n"
        f"Тренировка отменена: {reason_text}\n"
        f"Отмечено слотов: {skipped_count}"
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
    
    today = datetime.now(BELARUS_TZ).date()
    current_weekday = today.weekday()
    
    async with async_session() as s:
        # Get students for this specific time
        result = await s.execute(
            select(Student).options(
                selectinload(Student.schedules).selectinload(StudentSchedule.location)
            ).where(
                Student.coach_id == coach.id,
                Student.is_active == True
            )
        )
        students = result.scalars().all()
        
        skipped_count = 0
        for student in students:
            # Use new schedule system
            schedules = student.get_schedules_for_day(current_weekday)
            for sched_info in schedules:
                student_time = sched_info["time"]
                if student_time == time_key:
                    # Check if already marked
                    existing = await s.execute(
                        select(Lesson).where(
                            Lesson.student_id == student.id,
                            Lesson.date == today,
                            Lesson.time == time_key
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
                        location=sched_info.get("location_name", student.location),
                        location_id=sched_info.get("location_id"),
                        notes=f"Отмена: {reason_text}"
                    )
                    s.add(lesson)
                    await s.flush()
                    
                    # Mark as excused
                    att = Attendance(
                        lesson_id=lesson.id,
                        student_id=student.id,
                        location_id=sched_info.get("location_id"),
                        status="excused",
                        source="coach_cancelled",
                        deducted=False,
                        attendance_date=today,
                        attendance_time=time_key
                    )
                    s.add(att)
                    await reconcile_attendance(s, student, att)
                    if student.parent_id:
                        parent = await s.get(ParentAccount, student.parent_id)
                        if parent:
                            s.add(Notification(
                                coach_id=student.coach_id,
                                student_id=student.id,
                                type="training_cancelled",
                                message=(
                                    f"❌ Тренировка отменена\n{student.name} · "
                                    f"{today.strftime('%d.%m.%Y')} в {time_key}\n"
                                    f"Причина: {reason_text}\n"
                                    "Занятие не списано, право на отработку добавлено."
                                ),
                                recipient_telegram_id=parent.telegram_id,
                            ))
                    skipped_count += 1
        
        await s.commit()
    
    await callback.message.edit_text(
        f"✅ Сохранено\n\n"
        f"Тренировка {time_key} отменена: {reason_text}\n"
        f"Отмечено слотов: {skipped_count}"
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


# === Daily Notifications ===

async def _deprecated_send_daily_summary(coach_id: int = None):
    """Deprecated duplicate preserved temporarily during migration."""
    if bot is None:
        logger.warning("Bot is not initialized; skipping daily summary")
        return
    from app.models import DailyNotificationLog
    
    today = datetime.now(BELARUS_TZ).date()
    
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
                select(Student).options(
                    selectinload(Student.schedules).selectinload(StudentSchedule.location)
                ).where(
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
                    remaining = getattr(student, 'lessons_remaining', None)
                    if remaining is not None and remaining <= 0:
                        depleted.append({
                            "name": student.name,
                            "remaining": 0
                        })
                    elif remaining is not None and remaining <= 2:
                        low_lessons.append({
                            "name": student.name,
                            "remaining": remaining
                        })
            
            # Only send if there are alerts
            total_alerts = len(expired) + len(ending_soon) + len(low_lessons) + len(depleted)
            
            if total_alerts <= 0:
                continue

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

            # Add today's schedule (using new schedule system)
            weekday = today.weekday()
            today_lessons = []
            for student in students:
                schedules = student.get_schedules_for_day(weekday)
                for sched_info in schedules:
                    today_lessons.append({
                        "name": student.name,
                        "time": sched_info["time"],
                        "remaining": student.lessons_remaining if student.lessons_remaining is not None else 0,
                        "is_unlimited": getattr(student, 'is_unlimited', False)
                    })

            if today_lessons:
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
                    for lesson in lessons[:5]:
                        if lesson.get("is_unlimited"):
                            status = " ♾️"
                        elif lesson["remaining"] <= 0:
                            status = " ❌"
                        elif lesson["remaining"] <= 2:
                            status = " ⚠️"
                        else:
                            status = ""
                        text += f"  • {lesson['name']}{status}\n"
                    if len(lessons) > 5:
                        text += f"  ... и ещё {len(lessons) - 5} учеников\n"

            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="📱 Открыть CRM",
                    web_app=WebAppInfo(url=f"{WEBAPP_URL or 'https://your-app.up.railway.app'}/")
                )]
            ])

            try:
                await bot.send_message(coach.telegram_id, text, parse_mode="HTML", reply_markup=kb)

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


async def send_daily_summary(coach_id: int = None, force: bool = False):
    """Send daily summary to coach(es). If coach_id is None, send to all coaches."""
    if bot is None:
        logger.warning("Bot is not initialized; skipping daily summary")
        return 0
    from app.models import DailyNotificationLog

    today = datetime.now(BELARUS_TZ).date()
    sent_count = 0

    async with async_session() as s:
        if coach_id:
            coaches_result = await s.execute(select(Coach).where(Coach.id == coach_id))
        else:
            coaches_result = await s.execute(select(Coach).where(Coach.is_active == True))

        coaches = coaches_result.scalars().all()

        for coach in coaches:
            already_sent = await s.execute(
                select(DailyNotificationLog).where(
                    DailyNotificationLog.coach_id == coach.id,
                    DailyNotificationLog.notification_type == "daily_summary",
                    DailyNotificationLog.date == today
                )
            )
            already_sent_row = already_sent.scalar_one_or_none()
            if already_sent_row and not force:
                continue

            students_result = await s.execute(
                select(Student).options(
                    selectinload(Student.schedules).selectinload(StudentSchedule.location)
                ).where(
                    Student.coach_id == coach.id,
                    Student.is_active == True
                )
            )
            students = students_result.scalars().all()

            expired = []
            ending_soon = []
            low_lessons = []
            depleted = []

            for student in students:
                if student.subscription_end:
                    days_left = (student.subscription_end - today).days
                    if days_left < 0:
                        expired.append({"name": student.name, "days": abs(days_left)})
                    elif days_left <= 3:
                        ending_soon.append({"name": student.name, "days": days_left})

                if not getattr(student, "is_unlimited", False):
                    remaining = get_remaining_lessons(student)
                    if remaining <= 0:
                        depleted.append({"name": student.name, "remaining": 0})
                    elif remaining <= 2:
                        low_lessons.append({"name": student.name, "remaining": remaining})

            total_alerts = len(expired) + len(ending_soon) + len(low_lessons) + len(depleted)
            text = f"📊 <b>Ежедневная сводка ({today.strftime('%d.%m.%Y')})</b>\n\n"

            if expired:
                text += f"🚨 <b>Просрочена оплата ({len(expired)}):</b>\n"
                for item in expired:
                    text += f"  • {item['name']} — {item['days']} дн. назад\n"
                text += "\n"

            if ending_soon:
                text += f"⏰ <b>Заканчивается абонемент ({len(ending_soon)}):</b>\n"
                for item in ending_soon:
                    day_word = "день" if item["days"] == 1 else "дня" if item["days"] < 5 else "дней"
                    text += f"  • {item['name']} — {item['days']} {day_word}\n"
                text += "\n"

            if depleted:
                text += f"❌ <b>Закончились занятия ({len(depleted)}):</b>\n"
                for item in depleted:
                    text += f"  • {item['name']}\n"
                text += "\n"

            if low_lessons:
                text += f"⚠️ <b>Осталось мало занятий ({len(low_lessons)}):</b>\n"
                for item in low_lessons:
                    lesson_word = "занятие" if item["remaining"] == 1 else "занятия"
                    text += f"  • {item['name']} — {item['remaining']} {lesson_word}\n"
                text += "\n"

            weekday = today.weekday()
            today_lessons = []
            for student in students:
                for sched_info in student.get_schedules_for_day(weekday):
                    today_lessons.append({
                        "name": student.name,
                        "time": sched_info["time"],
                        "remaining": get_remaining_lessons(student),
                        "is_unlimited": getattr(student, "is_unlimited", False)
                    })

            if today_lessons:
                by_time = {}
                for lesson in today_lessons:
                    by_time.setdefault(lesson["time"], []).append(lesson)

                text += f"📅 <b>Сегодняшние тренировки ({len(today_lessons)}):</b>\n"
                for time_key in sorted(by_time.keys()):
                    lessons = by_time[time_key]
                    text += f"\n🕐 {time_key} ({len(lessons)} учеников)\n"
                    for lesson in lessons[:5]:
                        if lesson.get("is_unlimited"):
                            status = " ♾️"
                        elif lesson["remaining"] <= 0:
                            status = " ❌"
                        elif lesson["remaining"] <= 2:
                            status = " ⚠️"
                        else:
                            status = ""
                        text += f"  • {lesson['name']}{status}\n"
                    if len(lessons) > 5:
                        text += f"  ... и ещё {len(lessons) - 5} учеников\n"

            has_today_lessons = len(today_lessons) > 0
            if not (force or total_alerts > 0 or has_today_lessons):
                continue

            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="📱 Открыть CRM",
                    web_app=WebAppInfo(url=f"{WEBAPP_URL or 'https://your-app.up.railway.app'}/")
                )
            ]])

            try:
                await bot.send_message(coach.telegram_id, text, parse_mode="HTML", reply_markup=kb)
                if not already_sent_row:
                    s.add(DailyNotificationLog(
                        coach_id=coach.id,
                        notification_type="daily_summary",
                        date=today
                    ))
                await s.commit()
                sent_count += 1
                logger.info(f"Daily summary sent to coach {coach.id}")
            except Exception as e:
                logger.error(f"Failed to send daily summary to coach {coach.id}: {e}")

    return sent_count


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
    sent_count = await send_daily_summary(coach.id, force=True)
    if sent_count:
        await message.answer("✅ Сводка отправлена!")
    else:
        await message.answer("ℹ️ Сегодня нет данных для сводки.")
    return


# === Lesson Reminder Scheduler ===

async def lesson_reminder_scheduler():
    """Background task: remind about groups whose attendance has not been marked yet."""
    while True:
        try:
            now = datetime.now(BELARUS_TZ)
            current_weekday = now.weekday()
            current_date = now.date()
            
            async with async_session() as s:
                coaches_result = await s.execute(
                    select(Coach).where(Coach.is_active == True)
                )
                coaches = coaches_result.scalars().all()
                
                for coach in coaches:
                    # Group students by lesson time
                    time_groups = {}
                    
                    students_result = await s.execute(
                        select(Student).options(
                            selectinload(Student.schedules).selectinload(StudentSchedule.location)
                        ).where(
                            Student.coach_id == coach.id,
                            Student.is_active == True
                        )
                    )
                    students = students_result.scalars().all()
                    
                    for student in students:
                        # Use new schedule system first, fallback to legacy
                        schedules = student.get_schedules_for_day(current_weekday)
                        
                        for sched_info in schedules:
                            lesson_time = sched_info["time"]
                            lesson_hour, lesson_min = map(int, lesson_time.split(":"))
                            lesson_start = lesson_hour * 60 + lesson_min
                            now_total = now.hour * 60 + now.minute
                            
                            # Keep a small grace window so a restart does not lose the reminder.
                            if not (0 <= now_total - lesson_start <= LESSON_REMINDER_WINDOW_MINUTES):
                                continue
                            
                            # Check if lesson already marked
                            existing_lesson = await s.execute(
                                select(Lesson).where(
                                    Lesson.student_id == student.id,
                                    Lesson.date == current_date,
                                    Lesson.time == lesson_time
                                )
                            )
                            if existing_lesson.scalar_one_or_none():
                                continue
                            
                            # Group by time
                            if lesson_time not in time_groups:
                                time_groups[lesson_time] = []
                            time_groups[lesson_time].append(student)
                    
                    # Send ONE notification per time group
                    for lesson_time, students_list in time_groups.items():
                        # Check if already notified for THIS SPECIFIC TIME
                        existing_notification = await s.execute(
                            select(Notification).where(
                                Notification.coach_id == coach.id,
                                Notification.type == "lesson_reminder",
                                Notification.message.like(f"%тренировке {lesson_time}%"),
                                Notification.created_at >= now - timedelta(minutes=30)
                            )
                        )
                        reminder_key = build_lesson_reminder_log_key(current_date, lesson_time)
                        existing_notification_exact = await s.execute(
                            select(Notification).where(
                                Notification.coach_id == coach.id,
                                Notification.type == "lesson_reminder",
                                Notification.message.like(f"{reminder_key}%")
                            )
                        )
                        if existing_notification.scalar_one_or_none() or existing_notification_exact.scalar_one_or_none():
                            continue
                        
                        # Send reminder for group
                        await send_lesson_reminder(coach, students_list, lesson_time)
                        
                        # Log notification (for first student in group)
                        notification = Notification(
                            coach_id=coach.id,
                            student_id=students_list[0].id,
                            type="lesson_reminder",
                            message=f"{reminder_key} Reminder for lesson {lesson_time} ({len(students_list)} students)"
                        )
                        s.add(notification)
                        await s.commit()
                        
                        logger.info(f"Lesson reminder sent to coach {coach.id} for {lesson_time} ({len(students_list)} students)")
                        
        except Exception as e:
            logger.error(f"Error in reminder scheduler: {e}")
        
        await asyncio.sleep(LESSON_REMINDER_POLL_SECONDS)


async def send_lesson_reminder(coach: Coach, students: list, time_str: str):
    """Send lesson reminder notification to coach."""
    if bot is None:
        logger.warning("Bot is not initialized; skipping lesson reminder")
        return
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
        [InlineKeyboardButton(text="❌ Тренировки нет", callback_data=f"skip_group:{time_str}")]
    ])
    
    try:
        await bot.send_message(coach.telegram_id, text, parse_mode="HTML", reply_markup=kb)
    except Exception as e:
        logger.error(f"Failed to send reminder to coach {coach.id}: {e}")


# === Daily Notification Scheduler ===

async def daily_notification_scheduler():
    """Background task: send daily summaries once per morning."""
    while True:
        try:
            now = datetime.now(BELARUS_TZ)
            
            target_time = now.replace(
                hour=DAILY_SUMMARY_HOUR,
                minute=DAILY_SUMMARY_MINUTE,
                second=0,
                microsecond=0
            )
            if target_time <= now:
                target_time += timedelta(days=1)
            
            wait_seconds = (target_time - now).total_seconds()
            logger.info(
                f"Daily notification scheduler: waiting {wait_seconds/3600:.1f} hours "
                f"until {DAILY_SUMMARY_HOUR:02d}:{DAILY_SUMMARY_MINUTE:02d} (Belarus time)"
            )
            
            await asyncio.sleep(wait_seconds)
            
            # Send daily summaries
            await send_daily_summary()
            
            # Wait a bit to avoid double-sending
            await asyncio.sleep(60)
            
        except Exception as e:
            logger.error(f"Error in daily notification scheduler: {e}")
            await asyncio.sleep(3600)  # Retry in 1 hour


@router.callback_query(F.data.startswith("rsvp|"))
async def parent_training_response(callback: CallbackQuery):
    """Save the optional parent response without changing lesson deduction rules."""
    parts = callback.data.split("|")
    if len(parts) != 7:
        await callback.answer("Не удалось сохранить ответ", show_alert=True)
        return
    _, student_id_raw, date_raw, time_raw, location_raw, response_raw, _version = parts
    parent = await get_parent(callback.from_user.id)
    if not parent:
        await callback.answer("Нет доступа", show_alert=True)
        return
    try:
        student_id = int(student_id_raw)
        training_date = datetime.strptime(date_raw, "%Y%m%d").date()
        training_time = f"{time_raw[:2]}:{time_raw[2:]}"
        location_id = int(location_raw) if location_raw != "0" else None
    except (TypeError, ValueError):
        await callback.answer("Некорректные данные", show_alert=True)
        return
    response = "attending" if response_raw == "yes" else "absent"

    async with async_session() as s:
        student_result = await s.execute(
            select(Student).where(
                Student.id == student_id,
                Student.parent_id == parent.id,
                Student.is_active == True,
            )
        )
        student = student_result.scalar_one_or_none()
        if not student:
            await callback.answer("Ребёнок не найден", show_alert=True)
            return
        response_result = await s.execute(
            select(TrainingResponse).where(
                TrainingResponse.student_id == student_id,
                TrainingResponse.training_date == training_date,
                TrainingResponse.training_time == training_time,
                TrainingResponse.location_id == location_id,
            )
        )
        item = response_result.scalar_one_or_none()
        if item:
            item.response = response
            item.responded_at = datetime.utcnow()
        else:
            s.add(TrainingResponse(
                student_id=student_id,
                training_date=training_date,
                training_time=training_time,
                location_id=location_id,
                response=response,
            ))
        await s.commit()

    label = "Буду" if response == "attending" else "Не буду"
    await callback.answer(f"Ответ сохранён: {label}")
    await callback.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text=f"✓ {label}",
            callback_data=callback.data,
        )
    ]]))


async def _notify_assigned_trainers_about_blocked_parent(
    session,
    parent: ParentAccount,
) -> None:
    coaches_result = await session.execute(
        select(Coach).join(Student).where(
            Student.parent_id == parent.id,
            Student.is_active == True,
            Coach.is_active == True,
        ).distinct()
    )
    for coach in coaches_result.scalars().all():
        try:
            await bot.send_message(
                coach.telegram_id,
                f"⚠️ Родитель {parent.full_name} заблокировал бота. "
                "Уведомления ребёнку не доставляются.",
            )
        except Exception as exc:
            logger.warning("Could not notify coach %s about blocked parent: %s", coach.id, exc)


async def _deliver_outbox() -> None:
    if bot is None:
        return
    async with async_session() as s:
        result = await s.execute(
            select(Notification).where(
                Notification.recipient_telegram_id.is_not(None),
                Notification.sent_at.is_(None),
            ).order_by(Notification.created_at).limit(50)
        )
        for item in result.scalars().all():
            try:
                display_message = item.message
                if display_message.startswith("[") and "] " in display_message:
                    display_message = display_message.split("] ", 1)[1]
                await bot.send_message(item.recipient_telegram_id, display_message)
                item.sent_at = datetime.utcnow()
                item.delivery_error = None
            except TelegramForbiddenError:
                item.sent_at = datetime.utcnow()
                item.delivery_error = "bot_blocked"
                parent_result = await s.execute(
                    select(ParentAccount).where(
                        ParentAccount.telegram_id == item.recipient_telegram_id
                    )
                )
                parent = parent_result.scalar_one_or_none()
                if parent:
                    parent.bot_blocked_at = datetime.utcnow()
                    await _notify_assigned_trainers_about_blocked_parent(s, parent)
            except Exception as exc:
                item.delivery_error = str(exc)[:500]
                logger.warning("Outbox delivery failed for notification %s: %s", item.id, exc)
        await s.commit()


async def _send_parent_training_reminders(reference: datetime) -> None:
    if bot is None:
        return
    training_date = (reference + timedelta(days=1)).date()
    weekday = training_date.weekday()
    async with async_session() as s:
        result = await s.execute(
            select(Student, ParentAccount)
            .join(ParentAccount, Student.parent_id == ParentAccount.id)
            .options(
                selectinload(Student.schedules).selectinload(StudentSchedule.location)
            )
            .where(
                Student.is_active == True,
                Student.training_reminders_enabled == True,
                ParentAccount.training_reminders_enabled == True,
            )
        )
        for student, parent in result.all():
            for schedule in student.get_schedules_for_day(weekday):
                time_str = schedule["time"]
                location_id = schedule.get("location_id") or 0
                key = (
                    f"[parent_training:{student.id}:{training_date.isoformat()}:"
                    f"{time_str}:{location_id}]"
                )
                exists = await s.execute(
                    select(Notification).where(
                        Notification.type == "parent_training_reminder",
                        Notification.message.like(f"{key}%"),
                    )
                )
                if exists.scalar_one_or_none():
                    continue
                callback_prefix = (
                    f"rsvp|{student.id}|{training_date.strftime('%Y%m%d')}|"
                    f"{time_str.replace(':', '')}|{location_id}|"
                )
                markup = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(
                        text="Буду",
                        callback_data=f"{callback_prefix}yes|1",
                    ),
                    InlineKeyboardButton(
                        text="Не буду",
                        callback_data=f"{callback_prefix}no|1",
                    ),
                ]])
                text = (
                    f"⏰ Завтра тренировка\n\n"
                    f"{student.name} · {time_str}\n"
                    f"{schedule.get('location_name') or 'Зал уточняется'}\n\n"
                    "Ответ носит информационный характер. Пропуск всё равно списывает занятие "
                    "и создаёт право на отработку."
                )
                notification = Notification(
                    coach_id=student.coach_id,
                    student_id=student.id,
                    type="parent_training_reminder",
                    message=f"{key} {text}",
                    recipient_telegram_id=parent.telegram_id,
                )
                try:
                    await bot.send_message(parent.telegram_id, text, reply_markup=markup)
                    notification.sent_at = datetime.utcnow()
                    parent.bot_blocked_at = None
                except TelegramForbiddenError:
                    notification.sent_at = datetime.utcnow()
                    notification.delivery_error = "bot_blocked"
                    parent.bot_blocked_at = datetime.utcnow()
                    await _notify_assigned_trainers_about_blocked_parent(s, parent)
                except Exception as exc:
                    notification.delivery_error = str(exc)[:500]
                s.add(notification)
        await s.commit()


async def _ensure_parent_invoices(session, reference: date) -> None:
    month_start, month_end = month_bounds(reference)
    students_result = await session.execute(
        select(Student).where(
            Student.parent_id.is_not(None),
            Student.is_active == True,
            Student.deleted_at.is_(None),
        )
    )
    for student in students_result.scalars().all():
        existing = (
            await session.execute(
                select(Payment).where(
                    Payment.student_id == student.id,
                    Payment.period_start == month_start,
                    Payment.tariff_code.is_not(None),
                )
            )
        ).scalars().first()
        if existing:
            continue
        previous = (
            await session.execute(
                select(Payment).where(
                    Payment.student_id == student.id,
                    Payment.tariff_code.is_not(None),
                    Payment.period_start < month_start,
                ).order_by(desc(Payment.period_start), desc(Payment.id))
            )
        ).scalars().first()
        tariff_code = previous.tariff_code if previous and previous.tariff_code in TARIFFS else "8"
        tariff = TARIFFS[tariff_code]
        session.add(Payment(
            coach_id=student.coach_id,
            student_id=student.id,
            amount=tariff["price"],
            base_amount=tariff["price"],
            late_fee_amount=0,
            tariff_code=tariff_code,
            lessons_count=tariff["lessons"],
            status="pending",
            period_start=month_start,
            period_end=month_end,
            due_date=payment_due_date(month_start),
            notes="Абонемент повторён с прошлого месяца автоматически",
        ))


async def _send_payment_and_makeup_reminders(reference: datetime) -> None:
    if bot is None:
        return
    today = reference.date()
    async with async_session() as s:
        await _ensure_parent_invoices(s, today)
        rows = (
            await s.execute(
                select(Payment, Student, ParentAccount)
                .join(Student, Payment.student_id == Student.id)
                .join(ParentAccount, Student.parent_id == ParentAccount.id)
                .where(
                    Payment.status != "paid",
                    Payment.status != "written_off",
                    Payment.tariff_code.is_not(None),
                )
                .order_by(Student.id, Payment.period_start)
            )
        ).all()
        oldest_by_student = {}
        for payment, student, parent in rows:
            oldest_by_student.setdefault(student.id, (payment, student, parent))

        for payment, student, parent in oldest_by_student.values():
            due_date = payment.due_date or payment_due_date(payment.period_start or today)
            if not is_payment_reminder_day(today, due_date):
                continue
            late_fee, total = (
                (0, payment.base_amount or payment.amount)
                if payment.tariff_code == "single"
                else payment_total(payment.base_amount or payment.amount, due_date, today)
            )
            payment.late_fee_amount = late_fee
            payment.amount = total
            key = f"[payment_reminder:{payment.id}:{today.isoformat()}]"
            exists = await s.execute(
                select(Notification).where(
                    Notification.type == "payment_reminder",
                    Notification.message.like(f"{key}%"),
                )
            )
            if exists.scalar_one_or_none():
                continue
            text = (
                f"💳 Оплата за {payment.period_start.strftime('%m.%Y')}\n\n"
                f"{student.name}\n"
                f"Абонемент: {payment.base_amount or payment.amount} Br\n"
                f"Доплата за просрочку: {late_fee} Br\n"
                f"Итого: {total} Br\n\n"
                "Оплатить и отправить чек можно через Mini App."
            )
            s.add(Notification(
                coach_id=student.coach_id,
                student_id=student.id,
                type="payment_reminder",
                message=f"{key} {text}",
                recipient_telegram_id=parent.telegram_id,
            ))

        scheduled_past = (
            await s.execute(
                select(MakeupCredit).where(
                    MakeupCredit.status == "scheduled",
                    MakeupCredit.scheduled_date < today,
                )
            )
        ).scalars().all()
        for credit in scheduled_past:
            attendance = (
                await s.execute(
                    select(Attendance).where(
                        Attendance.makeup_credit_id == credit.id
                    )
                )
            ).scalar_one_or_none()
            credit.status = "used" if attendance and attendance.status == "present" else "burned"
            credit.used_at = datetime.utcnow()

        expired_credits = (
            await s.execute(
                select(MakeupCredit).where(
                    MakeupCredit.status.in_({"available", "requested"}),
                    MakeupCredit.expires_at < today,
                )
            )
        ).scalars().all()
        for credit in expired_credits:
            credit.status = "expired"

        makeup_rows = (
            await s.execute(
                select(MakeupCredit, Student, ParentAccount)
                .join(Student)
                .join(ParentAccount, Student.parent_id == ParentAccount.id)
                .where(
                    MakeupCredit.status.in_({"available", "requested", "scheduled"}),
                    MakeupCredit.expires_at >= today,
                )
            )
        ).all()
        for credit, student, parent in makeup_rows:
            days_left = (credit.expires_at - today).days
            if days_left not in {14, 7}:
                continue
            key = f"[makeup_expiry:{credit.id}:{days_left}]"
            exists = await s.execute(
                select(Notification).where(
                    Notification.type == "makeup_expiry",
                    Notification.message.like(f"{key}%"),
                )
            )
            if exists.scalar_one_or_none():
                continue
            s.add(Notification(
                coach_id=student.coach_id,
                student_id=student.id,
                type="makeup_expiry",
                message=(
                    f"{key} 🔁 Отработка для {student.name} сгорит через {days_left} дней.\n"
                    f"Срок: {credit.expires_at.strftime('%d.%m.%Y')}"
                ),
                recipient_telegram_id=parent.telegram_id,
            ))

        pending_registrations = (
            await s.execute(
                select(func.count(RegistrationRequest.id)).where(
                    RegistrationRequest.status == "pending"
                )
            )
        ).scalar_one()
        pending_schedules = (
            await s.execute(
                select(func.count(ScheduleRequest.id)).where(
                    ScheduleRequest.status == "pending"
                )
            )
        ).scalar_one()
        pending_makeups = (
            await s.execute(
                select(func.count(MakeupCredit.id)).where(
                    MakeupCredit.status == "requested"
                )
            )
        ).scalar_one()
        pending_payments = (
            await s.execute(
                select(func.count(Payment.id)).where(Payment.status == "reported")
            )
        ).scalar_one()
        overdue_payments = (
            await s.execute(
                select(Payment).where(
                    Payment.status != "paid",
                    Payment.status != "written_off",
                    Payment.tariff_code.is_not(None),
                    Payment.due_date < today,
                )
            )
        ).scalars().all()
        overdue_total = sum(
            payment_total(
                payment.base_amount or payment.amount,
                payment.due_date,
                today,
            )[1]
            for payment in overdue_payments
            if payment.tariff_code != "single"
        )
        admins = (
            await s.execute(
                select(Coach).where(
                    Coach.is_active == True,
                    or_(
                        Coach.is_admin == True,
                        Coach.telegram_id.in_(ADMIN_IDS or [-1]),
                    ),
                )
            )
        ).scalars().all()
        for admin in admins:
            key = f"[family_daily:{today.isoformat()}:{admin.id}]"
            exists = await s.execute(
                select(Notification).where(
                    Notification.type == "family_daily_summary",
                    Notification.message.like(f"{key}%"),
                )
            )
            if exists.scalar_one_or_none():
                continue
            s.add(Notification(
                coach_id=admin.id,
                student_id=None,
                type="family_daily_summary",
                message=(
                    f"{key} 📋 Сводка Break Wave на {today.strftime('%d.%m.%Y')}\n\n"
                    f"Новые анкеты: {pending_registrations}\n"
                    f"Изменения расписания: {pending_schedules}\n"
                    f"Отработки на подтверждение: {pending_makeups}\n"
                    f"Оплаты на проверку: {pending_payments}\n"
                    f"Просроченные счета: {len(overdue_payments)} · {overdue_total} Br"
                ),
                recipient_telegram_id=admin.telegram_id,
            ))
        await s.commit()


async def family_notification_scheduler():
    """Run all parent/admin notifications in the same existing bot process."""
    last_training_reminder_date = None
    last_daily_rules_date = None
    while True:
        try:
            now = datetime.now(BELARUS_TZ)
            await _deliver_outbox()
            if (
                now.hour == PARENT_REMINDER_HOUR
                and now.minute < 2
                and last_training_reminder_date != now.date()
            ):
                await _send_parent_training_reminders(now)
                last_training_reminder_date = now.date()
            if now.hour == DAILY_SUMMARY_HOUR and now.minute < 2 and last_daily_rules_date != now.date():
                await _send_payment_and_makeup_reminders(now)
                last_daily_rules_date = now.date()
        except Exception as exc:
            logger.error("Error in family notification scheduler: %s", exc)
        await asyncio.sleep(FAMILY_SCHEDULER_POLL_SECONDS)
