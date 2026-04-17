from fastapi import FastAPI, UploadFile, File, Query, Request, Form, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from sqlalchemy import select, func, and_, or_, desc, delete, update
from contextlib import asynccontextmanager
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from typing import Optional
import re

BELARUS_TZ = ZoneInfo('Europe/Minsk')
import hmac
import hashlib
import json
import os
import logging
import urllib.parse

from app.database import async_session, init_db
from app.models import Coach, Student, Lesson, Attendance, Payment, Notification, AdminUser, DailyNotificationLog, Location, StudentSchedule
from sqlalchemy.orm import selectinload
from app.config import WEBAPP_DIR, BOT_TOKEN, WEEKDAYS, ADMIN_IDS

# Version for cache busting - auto-generated on server start (timestamp)
import time
APP_VERSION = str(int(time.time()))


class NoCacheMiddleware(BaseHTTPMiddleware):
    """Add cache-busting headers to all responses."""
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        # Disable caching for all responses
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

logger = logging.getLogger(__name__)


def get_student_schedule_for_time(student: Student, day_of_week: int, target_time: str | None = None) -> dict | None:
    """Return matching schedule info for a day/time, or the primary schedule for that day."""
    schedules = student.get_schedules_for_day(day_of_week)
    if not schedules:
        return None
    if target_time:
        for sched in schedules:
            if sched["time"] == target_time:
                return sched
    return schedules[0]


@asynccontextmanager
async def lifespan(application: FastAPI):
    await init_db()
    yield

app = FastAPI(lifespan=lifespan)

# Disable caching middleware
app.add_middleware(NoCacheMiddleware)

# CORS for Telegram WebApp
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files with cache disabled
class NoCacheStaticFiles(StaticFiles):
    def file_response(self, *args, **kwargs):
        response = super().file_response(*args, **kwargs)
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

app.mount("/assets", NoCacheStaticFiles(directory=str(WEBAPP_DIR / "assets")), name="assets")


# === Telegram Auth Helpers ===

def verify_telegram_init_data(init_data: str) -> dict | None:
    """Verify Telegram WebApp initData and extract user info."""
    if not init_data or not BOT_TOKEN:
        return None
    try:
        parsed = dict(urllib.parse.parse_qsl(init_data))
        check_hash = parsed.pop("hash", "")
        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        computed_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        if computed_hash == check_hash:
            user = json.loads(parsed.get("user", "{}"))
            return user
    except Exception as e:
        logger.warning(f"initData verification failed: {e}")
    return None


async def get_current_coach(init_data: str):
    """Get coach by Telegram init data."""
    user = verify_telegram_init_data(init_data)
    if not user:
        return None
    
    async with async_session() as s:
        result = await s.execute(select(Coach).where(Coach.telegram_id == user.get("id")))
        return result.scalar_one_or_none()


async def is_admin_user(user_id: int | None) -> bool:
    """Check whether the current Telegram user has admin access."""
    if user_id is None:
        return False
    if ADMIN_IDS and user_id in ADMIN_IDS:
        return True

    async with async_session() as s:
        result = await s.execute(select(AdminUser).where(AdminUser.telegram_id == user_id))
        return result.scalar_one_or_none() is not None


def get_remaining_lessons(student: Student) -> int:
    """Return normalized remaining lessons value for old and new records."""
    if getattr(student, "lessons_remaining", None) is None:
        return getattr(student, "lessons_count", 0) or 0
    return student.lessons_remaining


def normalize_lesson_times_payload(lesson_times, lesson_days: str, fallback_time: str = "18:00") -> str:
    """Normalize lesson time payloads to JSON stored in the database."""
    if isinstance(lesson_times, str) and lesson_times.strip():
        return lesson_times

    if isinstance(lesson_times, dict):
        return json.dumps({str(day): value for day, value in lesson_times.items() if value})

    days = [day.strip() for day in (lesson_days or "1,3").split(",") if day.strip()]
    normalized = {day: fallback_time for day in days} or {"1": fallback_time}
    return json.dumps(normalized)


def normalize_schedule_days_payload(days, fallback_days: str = "1,3") -> str:
    """Normalize schedule day payloads to comma-separated weekday values."""
    if isinstance(days, list):
        normalized_days = [str(day).strip() for day in days if str(day).strip()]
        return ",".join(normalized_days) or fallback_days
    if isinstance(days, str) and days.strip():
        return ",".join(part.strip() for part in days.split(",") if part.strip()) or fallback_days
    return fallback_days


async def resolve_legacy_schedule_fields(
    session,
    schedules_payload,
    fallback_location: str = "Зал Break Wave",
    fallback_location_id: int | None = None,
    fallback_days: str = "1,3",
    fallback_times: str | None = None,
) -> dict:
    """Derive legacy single-location fields from the primary schedule."""
    if not schedules_payload:
        return {
            "location": fallback_location,
            "location_id": fallback_location_id,
            "lesson_days": fallback_days,
            "lesson_times": fallback_times or normalize_lesson_times_payload(None, fallback_days),
        }

    primary_schedule = next(
        (schedule for schedule in schedules_payload if schedule.get("is_primary")),
        schedules_payload[0],
    )
    lesson_days = normalize_schedule_days_payload(primary_schedule.get("days"), fallback_days)
    lesson_times = normalize_lesson_times_payload(
        primary_schedule.get("times"),
        lesson_days,
        primary_schedule.get("time", "18:00"),
    )
    location_id = primary_schedule.get("location_id") or fallback_location_id
    location_name = fallback_location
    if location_id:
        location = await session.get(Location, location_id)
        if location:
            location_name = location.name

    return {
        "location": location_name,
        "location_id": location_id,
        "lesson_days": lesson_days,
        "lesson_times": lesson_times,
    }


def apply_attendance_to_balance(student: Student, old_status: str | None, new_status: str) -> None:
    """Keep lessons_remaining consistent when attendance changes."""
    if getattr(student, "is_unlimited", False):
        return

    current_remaining = get_remaining_lessons(student)
    student.lessons_remaining = current_remaining

    if new_status == "present" and old_status != "present":
        if current_remaining > 0:
            student.lessons_remaining = current_remaining - 1
    elif new_status != "present" and old_status == "present":
        student.lessons_remaining = current_remaining + 1


# === Routes ===

@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve main HTML file with cache busting."""
    html_file = WEBAPP_DIR / "index.html"
    if html_file.exists():
        content = html_file.read_text()
        # Always bump asset versions on server start so Telegram WebApp does not keep stale JS/CSS.
        content = re.sub(r'href="/assets/style\.css\?v=\d+"', f'href="/assets/style.css?v={APP_VERSION}"', content)
        content = re.sub(r'src="/assets/app\.js\?v=\d+"', f'src="/assets/app.js?v={APP_VERSION}"', content)
        return Response(content=content, media_type="text/html", headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0"
        })
    return HTMLResponse(content="<h1>CRM Break Wave</h1><p>Mini App is loading...</p>")


# === Auth ===

@app.post("/api/auth")
async def api_auth(request: Request):
    """Authenticate user and return coach info."""
    body = await request.json()
    init_data = body.get("initData", "")
    
    coach = await get_current_coach(init_data)
    if not coach:
        return JSONResponse({"error": "not_registered"}, 403)
    
    # Check if admin
    user = verify_telegram_init_data(init_data)
    user_id = user.get("id") if user else None
    is_admin_user = False
    if ADMIN_IDS and user_id in ADMIN_IDS:
        is_admin_user = True
    else:
        async with async_session() as s:
            admin_result = await s.execute(select(AdminUser).where(AdminUser.telegram_id == user_id))
            if admin_result.scalar_one_or_none():
                is_admin_user = True
    
    return {
        "coach_id": coach.id,
        "first_name": coach.first_name,
        "telegram_id": coach.telegram_id,
        "username": coach.username,
        "is_admin": is_admin_user,
    }


# === Dashboard ===

@app.post("/api/dashboard")
async def api_dashboard(request: Request):
    """Get dashboard stats for coach."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    today = datetime.now(BELARUS_TZ).date()
    month_start = today.replace(day=1)
    
    async with async_session() as s:
        # Students count
        students_result = await s.execute(
            select(func.count(Student.id)).where(
                Student.coach_id == coach.id,
                Student.is_active == True
            )
        )
        students_count = students_result.scalar()
        
        # Lessons this month
        lessons_result = await s.execute(
            select(func.count(Lesson.id)).where(
                Lesson.coach_id == coach.id,
                Lesson.date >= month_start
            )
        )
        lessons_count = lessons_result.scalar()
        
        # Attendance rate this month
        attendance_result = await s.execute(
            select(func.count(Attendance.id)).join(Lesson).where(
                Lesson.coach_id == coach.id,
                Lesson.date >= month_start,
                Attendance.status == "present"
            )
        )
        present_count = attendance_result.scalar()
        
        total_attendance_result = await s.execute(
            select(func.count(Attendance.id)).join(Lesson).where(
                Lesson.coach_id == coach.id,
                Lesson.date >= month_start
            )
        )
        total_attendance = total_attendance_result.scalar()
        
        attendance_rate = round(present_count / total_attendance * 100) if total_attendance > 0 else 0
        
        # Overdue payments
        overdue_result = await s.execute(
            select(func.count(Student.id)).where(
                Student.coach_id == coach.id,
                Student.is_active == True,
                Student.subscription_end < today
            )
        )
        overdue_count = overdue_result.scalar()
        
        # Ending soon (3 days or less)
        ending_soon_result = await s.execute(
            select(func.count(Student.id)).where(
                Student.coach_id == coach.id,
                Student.is_active == True,
                Student.subscription_end >= today,
                Student.subscription_end <= today + timedelta(days=3)
            )
        )
        ending_soon_count = ending_soon_result.scalar()
        
        # Monthly revenue
        revenue_result = await s.execute(
            select(func.sum(Payment.amount)).where(
                Payment.coach_id == coach.id,
                Payment.status == "paid",
                Payment.paid_at >= month_start
            )
        )
        monthly_revenue = revenue_result.scalar() or 0
    
    return {
        "students_count": students_count,
        "lessons_this_month": lessons_count,
        "attendance_rate": attendance_rate,
        "overdue_count": overdue_count,
        "ending_soon_count": ending_soon_count,
        "monthly_revenue": monthly_revenue,
    }


@app.post("/api/sync")
async def api_sync(request: Request):
    """Full sync - get all data for offline mode."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    today = datetime.now(BELARUS_TZ).date()
    month_start = today.replace(day=1)
    
    async with async_session() as s:
        # Get all students
        result = await s.execute(
            select(Student).where(Student.coach_id == coach.id).order_by(Student.name)
        )
        students = [{
            "id": st.id,
            "name": st.name,
            "nickname": st.nickname,
            "phone": st.phone,
            "parent_phone": st.parent_phone,
            "age": st.age,
            "location": st.location,
            "lesson_days": st.lesson_days,
            "lesson_times": st.lesson_times,
            "lesson_price": st.lesson_price,
            "lessons_count": st.lessons_count,
            "lessons_remaining": get_remaining_lessons(st),
            "is_unlimited": st.is_unlimited,
            "subscription_start": st.subscription_start.isoformat() if st.subscription_start else None,
            "subscription_end": st.subscription_end.isoformat() if st.subscription_end else None,
            "notes": st.notes,
            "is_active": st.is_active,
        } for st in result.scalars().all()]
        
        # Get all payments
        payments_result = await s.execute(
            select(Payment, Student).join(Student).where(Payment.coach_id == coach.id).order_by(desc(Payment.created_at))
        )
        payments = [{
            "id": p.id,
            "student_id": p.student_id,
            "student_name": st.name,
            "amount": p.amount,
            "lessons_count": p.lessons_count,
            "status": p.status,
            "period_start": p.period_start.isoformat() if p.period_start else None,
            "period_end": p.period_end.isoformat() if p.period_end else None,
            "paid_at": p.paid_at.isoformat() if p.paid_at else None,
            "notes": p.notes,
        } for p, st in payments_result.all()]
        
        # Get dashboard stats
        students_count = len([s for s in students if s["is_active"]])
        
        # Overdue count
        overdue_count = sum(1 for s in students if s["is_active"] and s["subscription_end"] and date.fromisoformat(s["subscription_end"]) < today)
        
        # Ending soon
        ending_soon_count = sum(1 for s in students if s["is_active"] and s["subscription_end"] and 
                               date.fromisoformat(s["subscription_end"]) >= today and 
                               date.fromisoformat(s["subscription_end"]) <= today + timedelta(days=3))
        
        # Monthly revenue
        revenue_result = await s.execute(
            select(func.sum(Payment.amount)).where(
                Payment.coach_id == coach.id,
                Payment.status == "paid",
                Payment.paid_at >= month_start
            )
        )
        monthly_revenue = revenue_result.scalar() or 0
        
        # Lessons this month
        lessons_result = await s.execute(
            select(func.count(Lesson.id)).where(
                Lesson.coach_id == coach.id,
                Lesson.date >= month_start
            )
        )
        lessons_count = lessons_result.scalar()
    
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "students": students,
        "payments": payments,
        "stats": {
            "students_count": students_count,
            "lessons_this_month": lessons_count,
            "overdue_count": overdue_count,
            "ending_soon_count": ending_soon_count,
            "monthly_revenue": monthly_revenue,
        }
    }


# === Coaches ===

@app.post("/api/coaches")
async def api_coaches(request: Request):
    """Get all coaches (for admin) or current coach."""
    body = await request.json()
    user = verify_telegram_init_data(body.get("initData", ""))
    if not user:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    user_id = user.get("id")
    
    async with async_session() as s:
        # Check if admin
        is_admin_user = False
        if ADMIN_IDS and user_id in ADMIN_IDS:
            is_admin_user = True
        else:
            admin_result = await s.execute(select(AdminUser).where(AdminUser.telegram_id == user_id))
            if admin_result.scalar_one_or_none():
                is_admin_user = True
        
        if is_admin_user:
            # Admin sees all coaches
            result = await s.execute(select(Coach).where(Coach.is_active == True).order_by(Coach.first_name))
            coaches = result.scalars().all()
        else:
            # Regular user sees only themselves
            result = await s.execute(select(Coach).where(Coach.telegram_id == user_id))
            coach = result.scalar_one_or_none()
            coaches = [coach] if coach else []
        
        return [{
            "id": c.id,
            "telegram_id": c.telegram_id,
            "first_name": c.first_name,
            "username": c.username,
            "phone": c.phone,
            "is_current": c.telegram_id == user_id
        } for c in coaches]


# === Students ===

@app.post("/api/students")
async def api_students(request: Request):
    """Get all students for coach (or all for admin/shared view)."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    # Check if admin
    user = verify_telegram_init_data(body.get("initData", ""))
    user_id = user.get("id") if user else None
    is_admin_user = False
    if ADMIN_IDS and user_id in ADMIN_IDS:
        is_admin_user = True
    else:
        async with async_session() as s:
            admin_result = await s.execute(select(AdminUser).where(AdminUser.telegram_id == user_id))
            if admin_result.scalar_one_or_none():
                is_admin_user = True
    
    # Get filter parameters
    view_mode = body.get("view_mode", "all")  # "all" or "my"
    coach_filter = body.get("coach_id")  # Filter by specific coach
    
    async with async_session() as s:
        # Build query with eager loading for schedules
        query = select(Student, Coach).join(Coach).options(
            selectinload(Student.schedules).selectinload(StudentSchedule.location)
        )
        
        if coach_filter:
            # Filter by specific coach
            query = query.where(Student.coach_id == coach_filter)
        elif view_mode == "my" and not is_admin_user:
            # Show only my students
            query = query.where(Student.coach_id == coach.id)
        # else: show all students (shared view for coaches)
        
        query = query.where(Student.is_active == True).order_by(Student.name)
        result = await s.execute(query)
        rows = result.all()
        
        # Recalculate subscriptions for all students to ensure consistency
        for st, _ in rows:
            await recalculate_student_subscription(st.id, s)
        await s.commit()
        
        # Clear identity map and reload all students fresh from DB
        s.expunge_all()
        student_ids = [st.id for st, _ in rows]
        if student_ids:
            result = await s.execute(
                select(Student, Coach).join(Coach).options(
                    selectinload(Student.schedules).selectinload(StudentSchedule.location)
                ).where(Student.id.in_(student_ids)).order_by(Student.name)
            )
            rows = result.all()
        
        # Format response with schedules
        result_list = []
        for st, coach_obj in rows:
            # Build schedules list
            schedules_list = []
            for sch in st.schedules:
                loc_name = sch.location.name if sch.location else "Зал"
                schedules_list.append({
                    "id": sch.id,
                    "location_id": sch.location_id,
                    "location_name": loc_name,
                    "days": sch.days,
                    "times": sch.times,
                    "duration": sch.duration,
                    "is_primary": sch.is_primary
                })
            
            result_list.append({
                "id": st.id,
                "coach_id": st.coach_id,
                "coach_name": coach_obj.first_name if coach_obj else "Unknown",
                "coach_username": coach_obj.username if coach_obj else None,
                "is_my_student": st.coach_id == coach.id,
                "name": st.name,
                "nickname": st.nickname,
                "phone": st.phone,
                "parent_phone": st.parent_phone,
                "age": st.age,
                "location": st.location,
                "location_id": st.location_id,
                "lesson_days": st.lesson_days,
                "lesson_times": st.lesson_times,
                "lesson_price": st.lesson_price,
                "lessons_count": st.lessons_count,
                "lessons_remaining": get_remaining_lessons(st),
                "is_unlimited": st.is_unlimited,
                "subscription_start": st.subscription_start.isoformat() if st.subscription_start else None,
                "subscription_end": st.subscription_end.isoformat() if st.subscription_end else None,
                "notes": st.notes,
                "is_active": st.is_active,
                "schedules": schedules_list  # Include schedules for frontend
            })
        
        return result_list


@app.post("/api/students/create")
async def api_create_student(request: Request):
    """Create new student."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    data = body.get("student", {})
    
    user = verify_telegram_init_data(body.get("initData", ""))
    requested_coach_id = data.get("coach_id")
    coach_id = requested_coach_id or coach.id
    if coach_id != coach.id and not await is_admin_user(user.get("id") if user else None):
        return JSONResponse({"error": "forbidden"}, 403)
    
    # Verify coach exists
    async with async_session() as s:
        target_coach = await s.get(Coach, coach_id)
        if not target_coach:
            return JSONResponse({"error": "coach_not_found"}, 400)
        
        schedules_payload = data.get("schedules") or []
        lesson_days = data.get("lesson_days", "1,3")
        lesson_times = normalize_lesson_times_payload(
            data.get("lesson_times"),
            lesson_days,
            data.get("lesson_time", "18:00")
        )
        legacy_fields = await resolve_legacy_schedule_fields(
            s,
            schedules_payload,
            fallback_location=data.get("location", "Р—Р°Р» Break Wave"),
            fallback_location_id=data.get("location_id"),
            fallback_days=lesson_days,
            fallback_times=lesson_times,
        )
        
        student = Student(
            coach_id=coach_id,
            name=data.get("name"),
            nickname=data.get("nickname") or None,
            phone=data.get("phone") or None,
            parent_phone=data.get("parent_phone") or None,
            age=int(data.get("age")) if data.get("age") else None,
            location=data.get("location", "Зал Break Wave"),
            location_id=data.get("location_id"),
            lesson_days=lesson_days,
            lesson_times=lesson_times,
            lesson_price=150,
            lessons_count=0,
            lessons_remaining=0,
            is_unlimited=False,
            subscription_start=None,
            subscription_end=None,
            notes=data.get("notes") or None,
        )
        student.location = legacy_fields["location"]
        student.location_id = legacy_fields["location_id"]
        student.lesson_days = legacy_fields["lesson_days"]
        student.lesson_times = legacy_fields["lesson_times"]
        s.add(student)
        await s.flush()  # Get student.id without committing
        
        # Create primary schedule in new table (for multi-location support)
        # This duplicates data temporarily for migration purposes
        schedule_days = data.get("lesson_days", "1,3")
        schedule_times = lesson_times
        
        # If schedules array provided (new format), use it
        if schedules_payload:
            for idx, sched_data in enumerate(schedules_payload):
                times = sched_data.get("times")
                if not times and sched_data.get("time"):
                    days_list = normalize_schedule_days_payload(sched_data.get("days"), "1,3").split(",")
                    times = json.dumps({d.strip(): sched_data["time"] for d in days_list})
                elif isinstance(times, dict):
                    times = json.dumps(times)
                
                schedule = StudentSchedule(
                    student_id=student.id,
                    location_id=sched_data.get("location_id"),
                    days=normalize_schedule_days_payload(sched_data.get("days"), "1,3"),
                    times=times or '{"1": "18:00"}',
                    duration=sched_data.get("duration", 90),
                    is_primary=bool(sched_data.get("is_primary", idx == 0))
                )
                s.add(schedule)
        else:
            # Create from legacy fields (default behavior)
            schedule = StudentSchedule(
                student_id=student.id,
                location_id=data.get("location_id"),
                days=schedule_days,
                times=schedule_times,
                duration=int(data.get("lesson_duration", 90)),
                is_primary=True
            )
            s.add(schedule)
        
        await s.commit()
        
        return {"success": True, "id": student.id}


@app.post("/api/students/{student_id}")
async def api_get_student(student_id: int, request: Request):
    """Get single student details."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    async with async_session() as s:
        result = await s.execute(
            select(Student).where(Student.id == student_id, Student.coach_id == coach.id)
        )
        st = result.scalar_one_or_none()
        if not st:
            return JSONResponse({"error": "not_found"}, 404)
        
        # Get recent attendance
        attendance_result = await s.execute(
            select(Attendance, Lesson).join(Lesson).where(
                Attendance.student_id == student_id
            ).order_by(desc(Lesson.date)).limit(10)
        )
        attendance = []
        for att, lesson in attendance_result.all():
            attendance.append({
                "date": lesson.date.isoformat(),
                "status": att.status,
            })
        
        # Recalculate subscription to ensure consistency before returning
        await recalculate_student_subscription(student_id, s)
        
        # Get payments
        payments_result = await s.execute(
            select(Payment).where(Payment.student_id == student_id).order_by(desc(Payment.created_at))
        )
        payments = [{
            "id": p.id,
            "amount": p.amount,
            "lessons_count": p.lessons_count,
            "status": p.status,
            "period_start": p.period_start.isoformat() if p.period_start else None,
            "period_end": p.period_end.isoformat() if p.period_end else None,
            "paid_at": p.paid_at.isoformat() if p.paid_at else None,
            "is_unlimited": p.is_unlimited,
            "notes": p.notes,
        } for p in payments_result.scalars().all()]
        
        # Get location info (legacy)
        location_name = st.location
        if st.location_id:
            loc_result = await s.execute(select(Location).where(Location.id == st.location_id))
            loc = loc_result.scalar_one_or_none()
            if loc:
                location_name = loc.name
        
        # Get schedules (new multi-location system)
        schedules_result = await s.execute(
            select(StudentSchedule, Location).outerjoin(
                Location, StudentSchedule.location_id == Location.id
            ).where(StudentSchedule.student_id == student_id)
        )
        schedules = []
        for sched, loc in schedules_result.all():
            schedules.append({
                "id": sched.id,
                "location_id": sched.location_id,
                "location_name": loc.name if loc else "Зал",
                "location_address": loc.address if loc else None,
                "days": sched.days,
                "times": sched.times,
                "duration": sched.duration,
                "is_primary": sched.is_primary,
            })
        
        # If no schedules yet, create fallback from legacy data
        if not schedules and st.lesson_days:
            schedules = [{
                "id": None,
                "location_id": st.location_id,
                "location_name": location_name,
                "days": st.lesson_days,
                "times": st.lesson_times,
                "duration": st.lesson_duration,
                "is_primary": True,
                "is_legacy": True
            }]
        
        await s.commit()
        
        # Clear SQLAlchemy identity map to force fresh DB read
        s.expunge_all()
        
        # Reload fresh student data with schedules after commit
        result = await s.execute(
            select(Student).options(
                selectinload(Student.schedules).selectinload(StudentSchedule.location)
            ).where(Student.id == student_id)
        )
        st = result.scalar_one()
        
        return {
            "id": st.id,
            "coach_id": st.coach_id,
            "name": st.name,
            "nickname": st.nickname,
            "phone": st.phone,
            "parent_phone": st.parent_phone,
            "age": st.age,
            "birthday": st.birthday.isoformat() if st.birthday else None,
            # Legacy fields (for backward compatibility)
            "location": location_name,
            "location_id": st.location_id,
            "lesson_days": st.lesson_days,
            "lesson_times": st.lesson_times,
            # New multi-location system
            "schedules": schedules,
            "lesson_price": st.lesson_price,
            "lessons_count": st.lessons_count,
            "lessons_remaining": get_remaining_lessons(st),
            "is_unlimited": st.is_unlimited,
            "subscription_start": st.subscription_start.isoformat() if st.subscription_start else None,
            "subscription_end": st.subscription_end.isoformat() if st.subscription_end else None,
            "notes": st.notes,
            "is_active": st.is_active,
            "attendance": attendance,
            "payments": payments,
        }


@app.post("/api/students/{student_id}/update")
async def api_update_student(student_id: int, request: Request):
    """Update student."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    data = body.get("student", {})
    
    async with async_session() as s:
        result = await s.execute(
            select(Student).where(Student.id == student_id, Student.coach_id == coach.id)
        )
        student = result.scalar_one_or_none()
        if not student:
            return JSONResponse({"error": "not_found"}, 404)
        
        # Update fields
        if "name" in data:
            student.name = data["name"]
        if "nickname" in data:
            student.nickname = data["nickname"] or None
        if "phone" in data:
            student.phone = data["phone"] or None
        if "parent_phone" in data:
            student.parent_phone = data["parent_phone"] or None
        if "age" in data:
            student.age = int(data["age"]) if data["age"] else None
        if "location" in data:
            student.location = data["location"]
        if "location_id" in data:
            student.location_id = data["location_id"]
        if "lesson_days" in data:
            student.lesson_days = data["lesson_days"]
        if "lesson_times" in data:
            student.lesson_times = normalize_lesson_times_payload(data["lesson_times"], student.lesson_days)
        elif "lesson_time" in data:
            student.lesson_times = normalize_lesson_times_payload(None, student.lesson_days, data["lesson_time"])
        if "lesson_price" in data:
            student.lesson_price = int(data["lesson_price"])
        # Note: subscription fields (lessons_count, lessons_remaining, is_unlimited,
        # subscription_start, subscription_end) are managed via Payments API only
        if "notes" in data:
            student.notes = data["notes"] or None
        if "is_active" in data:
            student.is_active = bool(data["is_active"])
        
        # Update schedules if provided (new multi-location system)
        if "schedules" in data and data["schedules"]:
            schedules_payload = data["schedules"]
            # Get existing schedules
            existing_schedules_result = await s.execute(
                select(StudentSchedule).where(StudentSchedule.student_id == student_id)
            )
            existing_schedules = {sch.id: sch for sch in existing_schedules_result.scalars().all()}
            
            # Process incoming schedules
            for idx, sched_data in enumerate(schedules_payload):
                sched_id = sched_data.get("id")
                normalized_days = normalize_schedule_days_payload(sched_data.get("days"), student.lesson_days or "1,3")
                
                # Handle times JSON
                times = sched_data.get("times")
                if isinstance(times, dict):
                    times = json.dumps(times)
                elif not times and sched_data.get("time"):
                    # Legacy format: create times from single time value
                    days = normalized_days.split(",")
                    times_dict = {d.strip(): sched_data["time"] for d in days}
                    times = json.dumps(times_dict)
                
                if sched_id and sched_id in existing_schedules:
                    # Update existing schedule
                    sch = existing_schedules[sched_id]
                    if "location_id" in sched_data:
                        sch.location_id = sched_data["location_id"]
                    if "days" in sched_data:
                        sch.days = normalized_days
                    if times:
                        sch.times = times
                    if "duration" in sched_data:
                        sch.duration = int(sched_data["duration"])
                    if "is_primary" in sched_data:
                        sch.is_primary = bool(sched_data["is_primary"])
                    # Remove from dict to track which ones to keep
                    del existing_schedules[sched_id]
                else:
                    # Create new schedule
                    new_schedule = StudentSchedule(
                        student_id=student_id,
                        location_id=sched_data.get("location_id"),
                        days=normalized_days,
                        times=times or '{"1": "18:00", "3": "18:00"}',
                        duration=int(sched_data.get("duration", 90)),
                        is_primary=sched_data.get("is_primary", idx == 0)
                    )
                    s.add(new_schedule)
            
            for sch in existing_schedules.values():
                await s.delete(sch)

            legacy_fields = await resolve_legacy_schedule_fields(
                s,
                schedules_payload,
                fallback_location=student.location or "Зал Break Wave",
                fallback_location_id=student.location_id,
                fallback_days=student.lesson_days or "1,3",
                fallback_times=student.lesson_times,
            )
            student.location = legacy_fields["location"]
            student.location_id = legacy_fields["location_id"]
            student.lesson_days = legacy_fields["lesson_days"]
            student.lesson_times = legacy_fields["lesson_times"]
        
        await s.commit()
        return {"success": True}


@app.post("/api/students/{student_id}/delete")
async def api_delete_student(student_id: int, request: Request):
    """Delete (deactivate) student."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    async with async_session() as s:
        result = await s.execute(
            select(Student).where(Student.id == student_id, Student.coach_id == coach.id)
        )
        student = result.scalar_one_or_none()
        if not student:
            return JSONResponse({"error": "not_found"}, 404)
        
        student.is_active = False
        await s.commit()
        return {"success": True}


@app.post("/api/students/{student_id}/destroy")
async def api_destroy_student(student_id: int, request: Request):
    """Permanently delete student and all related data (lessons, attendance, payments, schedules).
    
    WARNING: This action cannot be undone! Use for students who left the school.
    """
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    # Optional: require confirmation
    confirmed = body.get("confirm_destroy", False)
    if not confirmed:
        return JSONResponse({
            "error": "confirmation_required",
            "message": "Это действие необратимо! Для подтверждения отправьте confirm_destroy: true"
        }, 400)
    
    async with async_session() as s:
        # Verify student exists and belongs to coach
        result = await s.execute(
            select(Student).where(Student.id == student_id, Student.coach_id == coach.id)
        )
        student = result.scalar_one_or_none()
        if not student:
            return JSONResponse({"error": "not_found"}, 404)
        
        student_name = student.name
        
        # Delete all related data (cascade delete)
        # 1. Delete attendance records
        await s.execute(
            delete(Attendance).where(Attendance.student_id == student_id)
        )
        
        # 2. Delete lessons
        await s.execute(
            delete(Lesson).where(Lesson.student_id == student_id)
        )
        
        # 3. Delete payments
        await s.execute(
            delete(Payment).where(Payment.student_id == student_id)
        )
        
        # 4. Delete schedules
        await s.execute(
            delete(StudentSchedule).where(StudentSchedule.student_id == student_id)
        )
        
        # 5. Delete notifications
        await s.execute(
            delete(Notification).where(Notification.student_id == student_id)
        )
        
        # 6. Finally delete student
        await s.delete(student)
        await s.commit()
        
        logger.info(f"Student {student_name} (ID: {student_id}) permanently deleted by coach {coach.id}")
        
        return {
            "success": True,
            "message": f"Ученик {student_name} и все связанные данные полностью удалены",
            "student_id": student_id
        }


# === Lessons & Attendance ===

@app.post("/api/lessons")
async def api_lessons(request: Request):
    """Get lessons for date range."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    start_date = body.get("start_date")
    end_date = body.get("end_date")
    student_id = body.get("student_id")
    
    async with async_session() as s:
        query = select(Lesson, Student).join(Student).where(
            Lesson.coach_id == coach.id
        )
        
        if start_date:
            query = query.where(Lesson.date >= date.fromisoformat(start_date))
        if end_date:
            query = query.where(Lesson.date <= date.fromisoformat(end_date))
        if student_id:
            query = query.where(Lesson.student_id == student_id)
        
        query = query.order_by(desc(Lesson.date))
        result = await s.execute(query)
        
        lessons = []
        for lesson, student in result.all():
            # Get attendance
            att_result = await s.execute(
                select(Attendance).where(Attendance.lesson_id == lesson.id)
            )
            att = att_result.scalar_one_or_none()
            
            lessons.append({
                "id": lesson.id,
                "student_id": lesson.student_id,
                "student_name": student.name,
                "date": lesson.date.isoformat(),
                "time": lesson.time,
                "location": lesson.location,
                "topic": lesson.topic,
                "notes": lesson.notes,
                "attendance": att.status if att else None,
            })
        
        return lessons


@app.post("/api/lessons/{lesson_id}")
async def api_lesson_detail(lesson_id: int, request: Request):
    """Get detailed lesson info by id."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)

    async with async_session() as s:
        result = await s.execute(
            select(Lesson, Student).join(Student).where(
                Lesson.id == lesson_id,
                Lesson.coach_id == coach.id
            )
        )
        row = result.one_or_none()
        if not row:
            return JSONResponse({"error": "not_found"}, 404)

        lesson, student = row
        attendance_result = await s.execute(
            select(Attendance).where(Attendance.lesson_id == lesson.id)
        )
        attendance = attendance_result.scalar_one_or_none()

        return {
            "id": lesson.id,
            "student_id": student.id,
            "student_name": student.name,
            "date": lesson.date.isoformat(),
            "time": lesson.time,
            "location": lesson.location,
            "location_id": lesson.location_id,
            "topic": lesson.topic,
            "notes": lesson.notes,
            "attendance": attendance.status if attendance else None,
            "attendance_notes": attendance.notes if attendance else None,
        }


@app.post("/api/lessons/create")
async def api_create_lesson(request: Request):
    """Create new lesson with attendance."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    data = body.get("lesson", {})
    student_id = data.get("student_id")
    
    async with async_session() as s:
        # Verify student belongs to coach
        student_result = await s.execute(
            select(Student).where(Student.id == student_id, Student.coach_id == coach.id)
        )
        student = student_result.scalar_one_or_none()
        if not student:
            return JSONResponse({"error": "student_not_found"}, 404)
        
        lesson_date = date.fromisoformat(data.get("date"))
        weekday = lesson_date.weekday()
        sched_info = get_student_schedule_for_time(student, weekday, data.get("time"))
        lesson_time = data.get("time") or (sched_info["time"] if sched_info else student.get_lesson_time_for_day(weekday))
        location_name = data.get("location") or (sched_info.get("location_name") if sched_info else student.location)
        location_id = sched_info.get("location_id") if sched_info else student.location_id

        lesson_result = await s.execute(
            select(Lesson).where(
                Lesson.student_id == student_id,
                Lesson.coach_id == coach.id,
                Lesson.date == lesson_date,
                Lesson.time == lesson_time
            )
        )
        lesson = lesson_result.scalar_one_or_none()
        if lesson:
            lesson.location = location_name
            lesson.location_id = location_id
            lesson.topic = data.get("topic")
            lesson.notes = data.get("notes")
        else:
            lesson = Lesson(
                coach_id=coach.id,
                student_id=student_id,
                date=lesson_date,
                time=lesson_time,
                location=location_name,
                location_id=location_id,
                topic=data.get("topic"),
                notes=data.get("notes"),
            )
            s.add(lesson)
            await s.flush()

        attendance_result = await s.execute(
            select(Attendance).where(Attendance.lesson_id == lesson.id)
        )
        attendance = attendance_result.scalar_one_or_none()
        old_status = attendance.status if attendance else None
        if attendance:
            attendance.status = data.get("status", "present")
            attendance.notes = data.get("attendance_notes")
            attendance.attendance_date = lesson_date
            attendance.attendance_time = lesson_time
            attendance.location_id = location_id
        else:
            attendance = Attendance(
                lesson_id=lesson.id,
                student_id=student_id,
                location_id=location_id,
                status=data.get("status", "present"),
                attendance_date=lesson_date,
                attendance_time=lesson_time,
                notes=data.get("attendance_notes"),
            )
            s.add(attendance)

        apply_attendance_to_balance(student, old_status, attendance.status)
        await s.commit()
        
        return {"success": True, "id": lesson.id}


@app.post("/api/lessons/{lesson_id}/attendance")
async def api_update_attendance(lesson_id: int, request: Request):
    """Update attendance for lesson."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    async with async_session() as s:
        # Verify lesson belongs to coach
        lesson_result = await s.execute(
            select(Lesson).where(Lesson.id == lesson_id, Lesson.coach_id == coach.id)
        )
        lesson = lesson_result.scalar_one_or_none()
        if not lesson:
            return JSONResponse({"error": "not_found"}, 404)
        
        # Update or create attendance
        att_result = await s.execute(
            select(Attendance).where(Attendance.lesson_id == lesson_id)
        )
        att = att_result.scalar_one_or_none()
        
        if att:
            old_status = att.status
            att.status = body.get("status", "present")
            att.notes = body.get("notes")
            att.attendance_date = lesson.date
            att.attendance_time = lesson.time
            att.location_id = lesson.location_id
        else:
            old_status = None
            att = Attendance(
                lesson_id=lesson_id,
                student_id=lesson.student_id,
                location_id=lesson.location_id,
                status=body.get("status", "present"),
                attendance_date=lesson.date,
                attendance_time=lesson.time,
                notes=body.get("notes"),
            )
            s.add(att)

        student = await s.get(Student, lesson.student_id)
        if student:
            apply_attendance_to_balance(student, old_status, att.status)
        
        await s.commit()
        return {"success": True}


# === Payments ===

async def recalculate_student_subscription(student_id: int, session):
    """Recalculate student's subscription based on all paid payments using raw UPDATE."""
    from sqlalchemy import func
    
    # Get all paid payments for this student (newest first by created_at, then by id)
    payments_result = await session.execute(
        select(Payment).where(
            Payment.student_id == student_id,
            Payment.status == "paid"
        ).order_by(desc(Payment.created_at), desc(Payment.id))
    )
    paid_payments = payments_result.scalars().all()
    
    logger.info(f"Recalculate subscription for student {student_id}: found {len(paid_payments)} paid payments")
    
    if paid_payments:
        last_payment = paid_payments[0]
        logger.info(f"Last payment id={last_payment.id}, is_unlimited={last_payment.is_unlimited}, status={last_payment.status}")
        
        if last_payment.is_unlimited:
            await session.execute(
                update(Student).where(Student.id == student_id).values(
                    is_unlimited=True,
                    lessons_count=0,
                    lessons_remaining=0,
                    subscription_start=last_payment.period_start,
                    subscription_end=last_payment.period_end
                )
            )
            logger.info(f"Student {student_id} set to UNLIMITED")
        else:
            used_result = await session.execute(
                select(func.count(Attendance.id)).where(
                    Attendance.student_id == student_id,
                    Attendance.status == "present"
                )
            )
            used_lessons = used_result.scalar() or 0
            total_lessons = sum(p.lessons_count or 0 for p in paid_payments)
            remaining_lessons = max(0, total_lessons - used_lessons)
            await session.execute(
                update(Student).where(Student.id == student_id).values(
                    is_unlimited=False,
                    lessons_count=total_lessons,
                    lessons_remaining=remaining_lessons,
                    subscription_start=last_payment.period_start,
                    subscription_end=last_payment.period_end
                )
            )
            logger.info(f"Student {student_id} set to regular: {remaining_lessons} remaining")
    else:
        await session.execute(
            update(Student).where(Student.id == student_id).values(
                is_unlimited=False,
                lessons_count=0,
                lessons_remaining=0,
                subscription_start=None,
                subscription_end=None
            )
        )
        logger.info(f"Student {student_id} reset to no subscription")
    
    await session.flush()


@app.post("/api/payments")
async def api_payments(request: Request):
    """Get all payments for coach."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    status_filter = body.get("status")
    student_id = body.get("student_id")
    
    async with async_session() as s:
        query = select(Payment, Student).join(Student).where(Payment.coach_id == coach.id)
        
        if status_filter:
            query = query.where(Payment.status == status_filter)
        if student_id:
            query = query.where(Payment.student_id == student_id)
        
        query = query.order_by(desc(Payment.created_at))
        result = await s.execute(query)
        
        payments = []
        for payment, student in result.all():
            payments.append({
                "id": payment.id,
                "student_id": payment.student_id,
                "student_name": student.name,
                "amount": payment.amount,
                "lessons_count": payment.lessons_count,
                "status": payment.status,
                "period_start": payment.period_start.isoformat() if payment.period_start else None,
                "period_end": payment.period_end.isoformat() if payment.period_end else None,
                "paid_at": payment.paid_at.isoformat() if payment.paid_at else None,
                "is_unlimited": payment.is_unlimited,
                "notes": payment.notes,
            })
        
        return payments


@app.post("/api/payments/create")
async def api_create_payment(request: Request):
    """Create new payment record."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    data = body.get("payment", {})
    
    async with async_session() as s:
        student_result = await s.execute(
            select(Student).where(Student.id == data.get("student_id"), Student.coach_id == coach.id)
        )
        student = student_result.scalar_one_or_none()
        if not student:
            return JSONResponse({"error": "student_not_found"}, 404)

        is_unlimited = bool(data.get("is_unlimited", False))
        lessons_count = int(data.get("lessons_count", 0)) if not is_unlimited else 0
        
        payment = Payment(
            coach_id=coach.id,
            student_id=data.get("student_id"),
            amount=int(data.get("amount", 0)),
            lessons_count=lessons_count,
            status=data.get("status", "pending"),
            period_start=date.fromisoformat(data["period_start"]) if data.get("period_start") else None,
            period_end=date.fromisoformat(data["period_end"]) if data.get("period_end") else None,
            is_unlimited=is_unlimited,
            notes=data.get("notes"),
        )
        
        if payment.status == "paid":
            payment.paid_at = datetime.utcnow()
        
        s.add(payment)
        await s.commit()
        
        # Recalculate student subscription based on all paid payments
        await recalculate_student_subscription(payment.student_id, s)
        await s.commit()
        
        return {"success": True, "id": payment.id}


@app.post("/api/payments/{payment_id}/mark-paid")
async def api_mark_payment_paid(payment_id: int, request: Request):
    """Mark payment as paid."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    async with async_session() as s:
        result = await s.execute(
            select(Payment).where(Payment.id == payment_id, Payment.coach_id == coach.id)
        )
        payment = result.scalar_one_or_none()
        if not payment:
            return JSONResponse({"error": "not_found"}, 404)
        
        payment.status = "paid"
        payment.paid_at = datetime.utcnow()
        
        # Recalculate student subscription based on all paid payments
        await recalculate_student_subscription(payment.student_id, s)
        
        await s.commit()
        return {"success": True}


@app.post("/api/payments/{payment_id}/update")
async def api_update_payment(payment_id: int, request: Request):
    """Update payment record."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    data = body.get("payment", {})
    
    async with async_session() as s:
        result = await s.execute(
            select(Payment).where(Payment.id == payment_id, Payment.coach_id == coach.id)
        )
        payment = result.scalar_one_or_none()
        if not payment:
            return JSONResponse({"error": "not_found"}, 404)
        
        logger.info(f"api_update_payment id={payment_id} received data: {data}")
        
        if "amount" in data:
            payment.amount = int(data["amount"])
        if "lessons_count" in data:
            payment.lessons_count = int(data["lessons_count"])
        if "status" in data:
            old_status = payment.status
            payment.status = data["status"]
            if payment.status == "paid" and old_status != "paid":
                payment.paid_at = datetime.utcnow()
            elif payment.status != "paid" and old_status == "paid":
                payment.paid_at = None
        if "period_start" in data:
            payment.period_start = date.fromisoformat(data["period_start"]) if data["period_start"] else None
        if "period_end" in data:
            payment.period_end = date.fromisoformat(data["period_end"]) if data["period_end"] else None
        if "is_unlimited" in data:
            payment.is_unlimited = bool(data["is_unlimited"])
            if payment.is_unlimited:
                payment.lessons_count = 0
        if "notes" in data:
            payment.notes = data["notes"] or None
        
        await s.commit()
        
        logger.info(f"api_update_payment id={payment_id} after commit: is_unlimited={payment.is_unlimited}, lessons_count={payment.lessons_count}")
        
        # Clear SQLAlchemy identity map to force fresh DB read
        s.expunge_all()
        
        # Recalculate student subscription
        await recalculate_student_subscription(payment.student_id, s)
        await s.commit()
        
        return {"success": True}


@app.post("/api/payments/{payment_id}/delete")
async def api_delete_payment(payment_id: int, request: Request):
    """Delete payment record."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    async with async_session() as s:
        result = await s.execute(
            select(Payment).where(Payment.id == payment_id, Payment.coach_id == coach.id)
        )
        payment = result.scalar_one_or_none()
        if not payment:
            return JSONResponse({"error": "not_found"}, 404)
        
        student_id = payment.student_id
        await s.delete(payment)
        await s.commit()
        
        # Recalculate student subscription
        await recalculate_student_subscription(student_id, s)
        await s.commit()
        
        return {"success": True}


# === Current Lesson & Quick Attendance ===

@app.post("/api/current-lesson")
async def api_current_lesson(request: Request):
    """Get current lesson(s) for coach based on time."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    from datetime import datetime, timedelta
    now = datetime.now(BELARUS_TZ)
    current_weekday = now.weekday()
    current_time = now.strftime("%H:%M")
    current_date = now.date()
    
    async with async_session() as s:
        # Get all active students with schedules eager-loaded
        from sqlalchemy.orm import selectinload
        result = await s.execute(
            select(Student).options(
                selectinload(Student.schedules).selectinload(StudentSchedule.location)
            ).where(
                Student.coach_id == coach.id,
                Student.is_active == True
            )
        )
        all_students = result.scalars().all()
        
        # Find students who have lesson now (within time window)
        # Use new multi-location schedule system
        current_lessons = []
        for student in all_students:
            # Get schedules for today
            schedules = student.get_schedules_for_day(current_weekday)
            for sched_info in schedules:
                # Check if within lesson time (±15 min window)
                lesson_time = sched_info["time"]
                lesson_hour, lesson_min = map(int, lesson_time.split(":"))
                lesson_start = lesson_hour * 60 + lesson_min
                
                now_hour, now_min = now.hour, now.minute
                now_total = now_hour * 60 + now_min
                
                # Window: 15 min before to 30 min after start
                if lesson_start - 15 <= now_total <= lesson_start + 30:
                    # Check if already marked today
                    existing = await s.execute(
                        select(Lesson).where(
                            Lesson.student_id == student.id,
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
                    
                    current_lessons.append({
                        "student_id": student.id,
                        "name": student.name,
                        "nickname": student.nickname,
                        "lesson_time": lesson_time,
                        "lesson_duration": sched_info.get("duration", 90),
                        "location": sched_info.get("location_name", "Зал"),
                        "status": status,
                        "marked": lesson_exists is not None
                    })
        
        # Group by time
        lessons_by_time = {}
        for lesson in current_lessons:
            time_key = lesson["lesson_time"]
            if time_key not in lessons_by_time:
                lessons_by_time[time_key] = []
            lessons_by_time[time_key].append(lesson)
        
        # Sort by time
        sorted_times = sorted(lessons_by_time.keys())
        groups = []
        for time_key in sorted_times:
            groups.append({
                "time": time_key,
                "students": lessons_by_time[time_key],
                "all_marked": all(s["marked"] for s in lessons_by_time[time_key])
            })
        
        return {
            "has_lessons": len(groups) > 0,
            "groups": groups,
            "current_time": current_time,
            "current_date": current_date.isoformat()
        }


@app.post("/api/bulk-attendance")
async def api_bulk_attendance(request: Request):
    """Mark attendance for multiple students at once."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    data = body.get("attendance", [])
    lesson_date = body.get("date", datetime.now(BELARUS_TZ).date().isoformat())
    
    if not data:
        return JSONResponse({"error": "no_data"}, 400)
    
    async with async_session() as s:
        marked_count = 0
        students_with_low_lessons = []
        
        for item in data:
            student_id = item.get("student_id")
            status = item.get("status", "present")
            
            # Get student info with schedules eager-loaded
            student_result = await s.execute(
                select(Student).options(
                    selectinload(Student.schedules).selectinload(StudentSchedule.location)
                ).where(Student.id == student_id, Student.coach_id == coach.id)
            )
            student = student_result.scalar_one_or_none()
            if not student:
                continue
            
            # Check if lesson exists (with optional time filter)
            lesson_time = item.get("time")
            lesson_query = select(Lesson).where(
                Lesson.student_id == student_id,
                Lesson.date == date.fromisoformat(lesson_date)
            )
            if lesson_time:
                lesson_query = lesson_query.where(Lesson.time == lesson_time)
            lesson_result = await s.execute(lesson_query)
            lesson = lesson_result.scalar_one_or_none()
            
            # Track old status for lesson counting
            old_status = None
            
            if lesson:
                # Update existing attendance
                att_result = await s.execute(
                    select(Attendance).where(Attendance.lesson_id == lesson.id)
                )
                att = att_result.scalar_one_or_none()
                if att:
                    old_status = att.status
                    att.status = status
                    att.attendance_date = date.fromisoformat(lesson_date)
                    att.attendance_time = lesson.time
                    att.location_id = lesson.location_id or student.location_id
                else:
                    # Get time for this day (using new schedule system)
                    weekday = date.fromisoformat(lesson_date).weekday()
                    sched_info = get_student_schedule_for_time(student, weekday, lesson.time)
                    lesson_time = lesson.time or (sched_info["time"] if sched_info else "18:00")
                    
                    att = Attendance(
                        lesson_id=lesson.id,
                        student_id=student_id,
                        location_id=lesson.location_id or student.location_id,
                        status=status,
                        attendance_date=date.fromisoformat(lesson_date),
                        attendance_time=lesson_time
                    )
                    s.add(att)
            else:
                # Create new lesson (using new schedule system)
                weekday = date.fromisoformat(lesson_date).weekday()
                
                # Get first schedule for this day (for time and location)
                schedules = student.get_schedules_for_day(weekday)
                if not schedules:
                    continue  # Skip if no schedule for this day
                
                sched_info = get_student_schedule_for_time(student, weekday, item.get("time"))
                if not sched_info:
                    continue
                lesson_time = sched_info["time"]
                location_name = sched_info.get("location_name", student.location or "Зал")
                location_id = sched_info.get("location_id")
                
                lesson = Lesson(
                    coach_id=coach.id,
                    student_id=student_id,
                    date=date.fromisoformat(lesson_date),
                    time=lesson_time,
                    location=location_name,
                    location_id=location_id
                )
                s.add(lesson)
                await s.flush()
                
                # Create attendance
                att = Attendance(
                    lesson_id=lesson.id,
                    student_id=student_id,
                    location_id=location_id,
                    status=status,
                    attendance_date=date.fromisoformat(lesson_date),
                    attendance_time=lesson_time
                )
                s.add(att)
            
            apply_attendance_to_balance(student, old_status, status)
            if not getattr(student, "is_unlimited", False) and get_remaining_lessons(student) <= 2:
                students_with_low_lessons.append({
                    "id": student.id,
                    "name": student.name,
                    "remaining": get_remaining_lessons(student)
                })
            
            marked_count += 1
        
        await s.commit()
        return {
            "success": True, 
            "marked": marked_count,
            "low_lessons_alert": students_with_low_lessons if students_with_low_lessons else None
        }


@app.post("/api/skip-lesson")
async def api_skip_lesson(request: Request):
    """Mark lesson as skipped (no training - holiday, etc.)."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    reason = body.get("reason", "no_training")
    lesson_date = body.get("date", datetime.now(BELARUS_TZ).date().isoformat())
    
    async with async_session() as s:
        # Get all students for this coach with schedules eager-loaded
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
            weekday = date.fromisoformat(lesson_date).weekday()
            schedules = student.get_schedules_for_day(weekday)
            
            # Create skipped lesson for each schedule (if multiple locations)
            for sched_info in schedules:
                # Check if lesson already exists
                existing = await s.execute(
                    select(Lesson).where(
                        Lesson.student_id == student.id,
                        Lesson.date == date.fromisoformat(lesson_date),
                        Lesson.time == sched_info["time"]
                    )
                )
                if existing.scalar_one_or_none():
                    continue  # Already marked
                
                lesson = Lesson(
                    coach_id=coach.id,
                    student_id=student.id,
                    date=date.fromisoformat(lesson_date),
                    time=sched_info["time"],
                    location=sched_info.get("location_name", student.location or "Зал"),
                    location_id=sched_info.get("location_id"),
                    notes=f"Тренировка отменена: {reason}"
                )
                s.add(lesson)
                await s.flush()
                
                # Mark as excused (не влияет на статистику)
                att = Attendance(
                    lesson_id=lesson.id,
                    student_id=student.id,
                    location_id=sched_info.get("location_id"),
                    status="excused",
                    attendance_date=date.fromisoformat(lesson_date),
                    attendance_time=sched_info["time"]
                )
                s.add(att)
                skipped_count += 1
        
        await s.commit()
        return {"success": True, "skipped": skipped_count}


# === Calendar ===

@app.post("/api/calendar")
async def api_calendar(request: Request):
    """Get calendar data with scheduled lessons and attendance status."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    year = body.get("year", datetime.now(BELARUS_TZ).date().year)
    month = body.get("month", datetime.now(BELARUS_TZ).date().month)
    
    # Calculate date range
    month_start = date(year, month, 1)
    if month == 12:
        month_end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        month_end = date(year, month + 1, 1) - timedelta(days=1)
    
    async with async_session() as s:
        # Get all active students with their schedules (eager loading)
        result = await s.execute(
            select(Student).options(
                selectinload(Student.schedules).selectinload(StudentSchedule.location)
            ).where(
                Student.coach_id == coach.id,
                Student.is_active == True
            )
        )
        students = result.scalars().all()
        
        # Get marked lessons for this period (for attendance status)
        lessons_result = await s.execute(
            select(Lesson, Attendance).join(
                Attendance, Lesson.id == Attendance.lesson_id, isouter=True
            ).where(
                Lesson.coach_id == coach.id,
                Lesson.date >= month_start,
                Lesson.date <= month_end
            )
        )
        
        # Build attendance lookup: {(student_id, day, time): status}
        attendance_lookup = {}
        for lesson, att in lessons_result.all():
            key = (lesson.student_id, lesson.date.day, lesson.time or "")
            status = att.status if att else None
            attendance_lookup[key] = {
                "status": status,
                "lesson_id": lesson.id,
                "time": lesson.time
            }
        
        # Build schedule for each day
        days = {}
        current_date = month_start
        while current_date <= month_end:
            weekday = current_date.weekday()
            day = current_date.day
            
            # Find all students with lessons on this day
            day_lessons = []
            for student in students:
                # Use new schedule system
                schedules = student.get_schedules_for_day(weekday)
                
                for sched_info in schedules:
                    # Check if already marked
                    att_key = (student.id, day, sched_info["time"] or "")
                    att_info = attendance_lookup.get(att_key, {})
                    
                    day_lessons.append({
                        "id": student.id,
                        "time": sched_info["time"],
                        "student_name": student.name,
                        "student_id": student.id,
                        "location": sched_info.get("location_name", "Зал"),
                        "status": att_info.get("status"),  # present, absent, sick, or None
                        "lesson_id": att_info.get("lesson_id"),
                        "is_marked": att_info.get("status") is not None
                    })
            
            if day_lessons:
                # Sort by time
                day_lessons.sort(key=lambda x: x["time"])
                days[day] = day_lessons
            
            current_date += timedelta(days=1)
        
        return {"days": days, "month_start": month_start.isoformat()}


# === Extra Attendance (Out-of-schedule lessons) ===

@app.post("/api/extra-attendance")
async def api_extra_attendance(request: Request):
    """Mark attendance for student outside their scheduled time (make-up/extra practice)."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    student_id = body.get("student_id")
    attendance_date = body.get("date", datetime.now(BELARUS_TZ).date().isoformat())
    attendance_time = body.get("time", datetime.now(BELARUS_TZ).strftime("%H:%M"))
    status = body.get("status", "present")
    notes = body.get("notes", "")
    deduct_lesson = body.get("deduct_lesson", True)  # Whether to deduct from subscription
    
    async with async_session() as s:
        # Verify student belongs to coach
        student_result = await s.execute(
            select(Student).where(Student.id == student_id, Student.coach_id == coach.id)
        )
        student = student_result.scalar_one_or_none()
        if not student:
            return JSONResponse({"error": "student_not_found"}, 404)
        
        # Check remaining lessons (skip for unlimited subscriptions)
        remaining_lessons = get_remaining_lessons(student)
        student.lessons_remaining = remaining_lessons
        if deduct_lesson and not getattr(student, 'is_unlimited', False) and remaining_lessons <= 0:
            return JSONResponse({
                "error": "no_lessons_remaining",
                "message": "У ученика не осталось занятий в абонементе"
            }, 400)
        
        duplicate_result = await s.execute(
            select(Attendance).where(
                Attendance.student_id == student_id,
                Attendance.is_extra == True,
                Attendance.attendance_date == date.fromisoformat(attendance_date),
                Attendance.attendance_time == attendance_time
            )
        )
        if duplicate_result.scalar_one_or_none():
            return JSONResponse({"error": "already_marked", "message": "Посещаемость уже отмечена"}, 409)

        # Create extra attendance record (no lesson entry - this is out-of-schedule)
        att = Attendance(
            lesson_id=None,  # No scheduled lesson
            student_id=student_id,
            location_id=student.location_id,
            status=status,
            is_extra=True,
            attendance_date=date.fromisoformat(attendance_date),
            attendance_time=attendance_time,
            notes=notes or "Внеплановое посещение"
        )
        s.add(att)
        
        # Deduct from remaining lessons if present
        if deduct_lesson and status == "present" and not getattr(student, 'is_unlimited', False):
            student.lessons_remaining = max(0, remaining_lessons - 1)
        
        await s.commit()
        
        return {
            "success": True,
            "lessons_remaining": student.lessons_remaining,
            "message": f"Отмечено: {student.name}. Осталось занятий: {student.lessons_remaining}"
        }


@app.post("/api/students/{student_id}/attendance-history")
async def api_student_attendance_history(student_id: int, request: Request):
    """Get full attendance history for a student (both scheduled and extra)."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    async with async_session() as s:
        # Verify student belongs to coach
        student_result = await s.execute(
            select(Student).where(Student.id == student_id, Student.coach_id == coach.id)
        )
        student = student_result.scalar_one_or_none()
        if not student:
            return JSONResponse({"error": "not_found"}, 404)
        
        # Get all attendance records (including extra)
        attendance_result = await s.execute(
            select(Attendance).where(
                Attendance.student_id == student_id
            ).order_by(desc(Attendance.attendance_date), desc(Attendance.created_at))
        )
        
        attendance = []
        for att in attendance_result.scalars().all():
            record = {
                "id": att.id,
                "date": att.attendance_date.isoformat(),
                "time": att.attendance_time,
                "status": att.status,
                "is_extra": att.is_extra,
                "notes": att.notes,
            }
            
            # If it's a scheduled lesson, get lesson details
            if att.lesson_id and not att.is_extra:
                lesson_result = await s.execute(
                    select(Lesson).where(Lesson.id == att.lesson_id)
                )
                lesson = lesson_result.scalar_one_or_none()
                if lesson:
                    record["scheduled_time"] = lesson.time
                    record["location"] = lesson.location
            
            attendance.append(record)
        
        # Calculate stats
        counted_attendance = [a for a in attendance if a["status"] != "excused"]
        total_lessons = len([a for a in counted_attendance if not a["is_extra"]])
        extra_lessons = len([a for a in attendance if a["is_extra"]])
        present_count = len([a for a in counted_attendance if a["status"] == "present"])
        
        return {
            "student": {
                "id": student.id,
                "name": student.name,
                "lessons_count": student.lessons_count,
                "lessons_remaining": student.lessons_remaining,
            },
            "attendance": attendance,
            "stats": {
                "total_scheduled": total_lessons,
                "extra_lessons": extra_lessons,
                "total_present": present_count,
                "attendance_rate": round(present_count / len(counted_attendance) * 100) if counted_attendance else 0
            }
        }


# === Subscription Management ===

@app.post("/api/students/{student_id}/subscription")
async def api_update_subscription(student_id: int, request: Request):
    """Update student subscription (new payment/renewal)."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    async with async_session() as s:
        result = await s.execute(
            select(Student).where(Student.id == student_id, Student.coach_id == coach.id)
        )
        student = result.scalar_one_or_none()
        if not student:
            return JSONResponse({"error": "not_found"}, 404)
        
        data = body.get("subscription", {})
        
        # Update subscription
        if "lessons_count" in data:
            student.lessons_count = int(data["lessons_count"])
        if "lessons_remaining" in data:
            student.lessons_remaining = int(data["lessons_remaining"])
        elif "add_lessons" in data:
            # Add lessons to existing count
            add_lessons = int(data["add_lessons"])
            current_remaining = get_remaining_lessons(student)
            student.lessons_remaining = current_remaining + add_lessons
            student.lessons_count = (student.lessons_count or 0) + add_lessons
        
        if "subscription_start" in data:
            student.subscription_start = date.fromisoformat(data["subscription_start"]) if data["subscription_start"] else None
        if "subscription_end" in data:
            student.subscription_end = date.fromisoformat(data["subscription_end"]) if data["subscription_end"] else None
        
        await s.commit()
        
        return {
            "success": True,
            "lessons_count": student.lessons_count,
            "lessons_remaining": student.lessons_remaining,
            "subscription_end": student.subscription_end.isoformat() if student.subscription_end else None
        }


@app.post("/api/students/{student_id}/subscription-status")
async def api_subscription_status(student_id: int, request: Request):
    """Get detailed subscription status for a student."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    async with async_session() as s:
        result = await s.execute(
            select(Student).where(Student.id == student_id, Student.coach_id == coach.id)
        )
        student = result.scalar_one_or_none()
        if not student:
            return JSONResponse({"error": "not_found"}, 404)
        
        today = datetime.now(BELARUS_TZ).date()
        
        # Check subscription status
        days_until_expiry = None
        status = "active"
        
        if student.subscription_end:
            days_until_expiry = (student.subscription_end - today).days
            if days_until_expiry < 0:
                status = "expired"
            elif days_until_expiry <= 3:
                status = "ending_soon"
        
        # Check lessons remaining
        lessons_status = "ok"
        remaining = get_remaining_lessons(student)
        if remaining <= 0:
            lessons_status = "depleted"
        elif remaining <= 2:
            lessons_status = "low"
        
        return {
            "student_id": student.id,
            "name": student.name,
            "subscription": {
                "total_lessons": student.lessons_count,
                "remaining": remaining,
                "used": max(0, student.lessons_count - remaining),
                "start_date": student.subscription_start.isoformat() if student.subscription_start else None,
                "end_date": student.subscription_end.isoformat() if student.subscription_end else None,
                "days_until_expiry": days_until_expiry,
            },
            "status": {
                "subscription": status,
                "lessons": lessons_status
            },
            "alerts": {
                "payment_needed": status in ("expired", "ending_soon") or lessons_status in ("depleted", "low"),
                "urgent": status == "expired" or lessons_status == "depleted"
            }
        }


# === Group Management ===

@app.post("/api/current-lesson/add-student")
async def api_add_student_to_current_lesson(request: Request):
    """Add a student to current/next lesson group on-the-fly."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    student_id = body.get("student_id")
    target_time = body.get("target_time")  # If None, use current time
    lesson_date = body.get("date", datetime.now(BELARUS_TZ).date().isoformat())
    
    async with async_session() as s:
        # Verify student belongs to coach
        student_result = await s.execute(
            select(Student).where(Student.id == student_id, Student.coach_id == coach.id)
        )
        student = student_result.scalar_one_or_none()
        if not student:
            return JSONResponse({"error": "student_not_found"}, 404)
        
        # If target_time specified, temporarily update student's schedule
        lesson_day = date.fromisoformat(lesson_date).weekday()
        original_time = student.get_lesson_time_for_day(lesson_day)
        if target_time and target_time != original_time:
            duplicate_result = await s.execute(
                select(Attendance).where(
                    Attendance.student_id == student_id,
                    Attendance.is_extra == True,
                    Attendance.attendance_date == date.fromisoformat(lesson_date),
                    Attendance.attendance_time == target_time
                )
            )
            if duplicate_result.scalar_one_or_none():
                return JSONResponse({"error": "already_marked"}, 409)

            # Create extra attendance for this specific session
            att = Attendance(
                lesson_id=None,
                student_id=student_id,
                location_id=student.location_id,
                status="present",
                is_extra=True,
                attendance_date=date.fromisoformat(lesson_date),
                attendance_time=target_time,
                notes=f"Добавлен к группе {target_time} (обычное время: {original_time})"
            )
            s.add(att)
            
            # Deduct lesson if present (skip for unlimited)
            remaining_lessons = get_remaining_lessons(student)
            student.lessons_remaining = remaining_lessons
            if not getattr(student, 'is_unlimited', False) and remaining_lessons > 0:
                student.lessons_remaining = remaining_lessons - 1
            
            await s.commit()
            
            return {
                "success": True,
                "message": f"{student.name} добавлен к группе {target_time}",
                "lessons_remaining": student.lessons_remaining
            }
        else:
            # Regular attendance at student's scheduled time
            return JSONResponse({"error": "use_regular_attendance"}, 400)


@app.post("/api/groups")
async def api_get_groups(request: Request):
    """Get all student groups organized by time slot."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    async with async_session() as s:
        result = await s.execute(
            select(Student).options(
                selectinload(Student.schedules).selectinload(StudentSchedule.location)
            ).where(
                Student.coach_id == coach.id,
                Student.is_active == True
            ).order_by(Student.name)
        )
        students = result.scalars().all()
        
        # Group by time
        groups = {}
        for student in students:
            weekday_times = set()
            for day in range(7):
                for sched_info in student.get_schedules_for_day(day):
                    weekday_times.add(sched_info["time"])

            fallback_time = student.get_lesson_time_for_day(0)
            for time_key in sorted(weekday_times or {fallback_time}):
                if time_key not in groups:
                    groups[time_key] = {
                        "time": time_key,
                        "students": [],
                        "count": 0,
                        "low_lessons": 0
                    }

                remaining = get_remaining_lessons(student)
                groups[time_key]["students"].append({
                    "id": student.id,
                    "name": student.name,
                    "lessons_remaining": remaining,
                    "subscription_end": student.subscription_end.isoformat() if student.subscription_end else None
                })
                groups[time_key]["count"] += 1

                if not getattr(student, "is_unlimited", False) and remaining <= 2:
                    groups[time_key]["low_lessons"] += 1
        
        # Sort by time
        sorted_groups = sorted(groups.values(), key=lambda g: g["time"])
        
        return {
            "groups": sorted_groups,
            "total_students": len(students)
        }


# === Daily Notifications for Coach ===

@app.post("/api/coach/daily-summary")
async def api_daily_summary(request: Request):
    """Get daily summary for coach: payments due, low lessons, etc."""
    try:
        body = await request.json()
        coach = await get_current_coach(body.get("initData", ""))
        if not coach:
            return JSONResponse({"error": "unauthorized"}, 403)
        
        today = datetime.now(BELARUS_TZ).date()
        logger.info(f"Daily summary requested for coach {coach.id}")
        
        async with async_session() as s:
            # Get all active students with schedules eager-loaded
            from sqlalchemy.orm import selectinload
            result = await s.execute(
                select(Student).options(
                    selectinload(Student.schedules).selectinload(StudentSchedule.location)
                ).where(
                    Student.coach_id == coach.id,
                    Student.is_active == True
                )
            )
            students = result.scalars().all()
            logger.info(f"Found {len(students)} active students")
            
            # Categorize students
            payments_due = []  # Subscription ended or ending within 3 days
            low_lessons = []   # 2 or fewer lessons remaining
            depleted = []      # No lessons remaining
            
            for idx, student in enumerate(students):
                try:
                    lessons_remaining = get_remaining_lessons(student)
                    
                    # Check subscription expiry
                    if student.subscription_end:
                        days_left = (student.subscription_end - today).days
                        if days_left < 0:
                            payments_due.append({
                                "id": student.id,
                                "name": student.name,
                                "reason": "subscription_expired",
                                "days_overdue": abs(days_left)
                            })
                        elif days_left <= 3:
                            payments_due.append({
                                "id": student.id,
                                "name": student.name,
                                "reason": "subscription_ending",
                                "days_left": days_left
                            })
                    
                    # Check lessons remaining
                    if not getattr(student, "is_unlimited", False) and lessons_remaining <= 0:
                        depleted.append({
                            "id": student.id,
                            "name": student.name,
                            "lessons_remaining": 0
                        })
                    elif not getattr(student, "is_unlimited", False) and lessons_remaining <= 2:
                        low_lessons.append({
                            "id": student.id,
                            "name": student.name,
                            "lessons_remaining": lessons_remaining,
                            "is_unlimited": getattr(student, "is_unlimited", False)
                        })
                except Exception as e:
                    logger.error(f"Error processing student {student.id}: {e}")
                    continue
            
            # Get today's lessons (using new schedule system)
            weekday = today.weekday()
            today_lessons = []
            for student in students:
                try:
                    # Use new multi-location schedule system
                    schedules = student.get_schedules_for_day(weekday)
                    for sched_info in schedules:
                        lessons_remaining = get_remaining_lessons(student)
                        today_lessons.append({
                            "id": student.id,
                            "name": student.name,
                            "time": sched_info["time"],
                            "location": sched_info.get("location_name", "Зал"),
                            "lessons_remaining": lessons_remaining
                        })
                except Exception as e:
                    logger.error(f"Error processing today's lesson for student {student.id}: {e}")
                    continue
            
            # Group today's lessons by time
            lessons_by_time = {}
            for lesson in today_lessons:
                time_key = lesson["time"]
                if time_key not in lessons_by_time:
                    lessons_by_time[time_key] = []
                lessons_by_time[time_key].append(lesson)
            
            return {
                "date": today.isoformat(),
                "summary": {
                    "total_students": len(students),
                    "payments_due_count": len(payments_due),
                    "low_lessons_count": len(low_lessons),
                    "depleted_count": len(depleted),
                    "today_lessons_count": len(today_lessons)
                },
                "alerts": {
                    "payments_due": payments_due,
                    "low_lessons": low_lessons,
                    "depleted": depleted
                },
                "today_schedule": lessons_by_time
            }
    except Exception as e:
        logger.error(f"Daily summary error: {e}", exc_info=True)
        return JSONResponse({"error": "internal_error", "message": str(e)}, 500)


# === Locations ===

@app.post("/api/locations")
async def api_locations(request: Request):
    """Get all locations for coach."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    async with async_session() as s:
        result = await s.execute(
            select(Location).where(
                Location.coach_id == coach.id,
                Location.is_active == True
            ).order_by(Location.name)
        )
        locations = result.scalars().all()
    
    return [{
        "id": loc.id,
        "name": loc.name,
        "address": loc.address,
    } for loc in locations]


@app.post("/api/locations/create")
async def api_create_location(request: Request):
    """Create new location."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    data = body.get("location", {})
    
    async with async_session() as s:
        location = Location(
            coach_id=coach.id,
            name=data.get("name"),
            address=data.get("address"),
        )
        s.add(location)
        await s.commit()
        
        return {"success": True, "id": location.id}


@app.post("/api/locations/{location_id}/update")
async def api_update_location(location_id: int, request: Request):
    """Update location."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    data = body.get("location", {})
    
    async with async_session() as s:
        result = await s.execute(
            select(Location).where(Location.id == location_id, Location.coach_id == coach.id)
        )
        location = result.scalar_one_or_none()
        if not location:
            return JSONResponse({"error": "not_found"}, 404)
        
        if "name" in data:
            location.name = data["name"]
        if "address" in data:
            location.address = data["address"]
        if "is_active" in data:
            location.is_active = data["is_active"]
        
        await s.commit()
        return {"success": True}


@app.post("/api/locations/{location_id}/delete")
async def api_delete_location(location_id: int, request: Request):
    """Delete (deactivate) location."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    async with async_session() as s:
        result = await s.execute(
            select(Location).where(Location.id == location_id, Location.coach_id == coach.id)
        )
        location = result.scalar_one_or_none()
        if not location:
            return JSONResponse({"error": "not_found"}, 404)
        
        location.is_active = False
        await s.commit()
        return {"success": True}


# === Statistics ===

@app.post("/api/statistics")
async def api_statistics(request: Request):
    """Get detailed statistics."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    # Get filter parameters
    period = body.get("period", "month")  # week, month, year, all
    location_id = body.get("location_id")
    
    today = datetime.now(BELARUS_TZ).date()
    
    if period == "week":
        start_date = today - timedelta(days=today.weekday())
    elif period == "month":
        start_date = today.replace(day=1)
    elif period == "year":
        start_date = today.replace(month=1, day=1)
    else:
        start_date = date(2000, 1, 1)
    
    async with async_session() as s:
        # Base query for students
        students_query = select(Student).where(
            Student.coach_id == coach.id,
            Student.is_active == True
        )
        if location_id:
            students_query = students_query.where(Student.location_id == location_id)
        
        students_result = await s.execute(students_query)
        students = students_result.scalars().all()
        
        # Attendance statistics
        attendance_query = select(Attendance, Student).join(Student).where(
            Student.coach_id == coach.id,
            Attendance.attendance_date >= start_date
        )
        if location_id:
            attendance_query = attendance_query.where(Attendance.location_id == location_id)
        
        attendance_result = await s.execute(attendance_query)
        attendance_records = attendance_result.all()
        
        # Calculate stats
        counted_attendance = [(att, student) for att, student in attendance_records if att.status != "excused"]
        total_present = sum(1 for att, _ in counted_attendance if att.status == "present")
        total_absent = sum(1 for att, _ in counted_attendance if att.status == "absent")
        total_sick = sum(1 for att, _ in counted_attendance if att.status == "sick")
        total_lessons = len(counted_attendance)
        
        attendance_rate = round(total_present / total_lessons * 100) if total_lessons > 0 else 0
        
        # By day of week
        by_day_of_week = {}
        for att, student in counted_attendance:
            day = att.attendance_date.weekday()
            if day not in by_day_of_week:
                by_day_of_week[day] = {"total": 0, "present": 0}
            by_day_of_week[day]["total"] += 1
            if att.status == "present":
                by_day_of_week[day]["present"] += 1
        
        # By location
        by_location = {}
        locations_result = await s.execute(
            select(Location).where(Location.coach_id == coach.id, Location.is_active == True)
        )
        locations = {loc.id: loc.name for loc in locations_result.scalars().all()}
        locations[None] = "Без зала"
        
        for att, student in counted_attendance:
            loc_name = locations.get(att.location_id, "Другой")
            if loc_name not in by_location:
                by_location[loc_name] = {"total": 0, "present": 0}
            by_location[loc_name]["total"] += 1
            if att.status == "present":
                by_location[loc_name]["present"] += 1
        
        # By age groups
        age_groups = {"Дети (5-12)": 0, "Подростки (13-17)": 0, "Взрослые (18+)": 0, "Не указан": 0}
        for student in students:
            if student.age is None:
                age_groups["Не указан"] += 1
            elif student.age <= 12:
                age_groups["Дети (5-12)"] += 1
            elif student.age <= 17:
                age_groups["Подростки (13-17)"] += 1
            else:
                age_groups["Взрослые (18+)"] += 1
        
        # Monthly trend (last 6 months) - correct month calculation
        monthly_trend = []
        for i in range(5, -1, -1):
            # Calculate month correctly (handle year boundaries)
            total_months = today.year * 12 + today.month - 1 - i
            year = total_months // 12
            month = (total_months % 12) + 1
            month_date = date(year, month, 1)
            
            month_start = month_date
            if month == 12:
                month_end = date(year + 1, 1, 1)
            else:
                month_end = date(year, month + 1, 1)
            
            month_attendance = await s.execute(
                select(func.count(Attendance.id)).join(Student).where(
                    Student.coach_id == coach.id,
                    Attendance.attendance_date >= month_start,
                    Attendance.attendance_date < month_end,
                    Attendance.status == "present"
                )
            )
            monthly_trend.append({
                "month": month_date.strftime("%b"),
                "count": month_attendance.scalar() or 0
            })
        
        return {
            "period": period,
            "summary": {
                "total_students": len(students),
                "total_lessons": total_lessons,
                "attendance_rate": attendance_rate,
                "total_present": total_present,
                "total_absent": total_absent,
                "total_sick": total_sick,
            },
            "by_day_of_week": {
                day: {"total": data["total"], "rate": round(data["present"] / data["total"] * 100) if data["total"] > 0 else 0}
                for day, data in by_day_of_week.items()
            },
            "by_location": {
                loc: {"total": data["total"], "rate": round(data["present"] / data["total"] * 100) if data["total"] > 0 else 0}
                for loc, data in by_location.items()
            },
            "age_groups": age_groups,
            "monthly_trend": monthly_trend,
        }


# === Search ===

@app.post("/api/search")
async def api_search(request: Request):
    """Search students by name, phone, or nickname."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    query = body.get("query", "").strip().lower()
    if not query or len(query) < 2:
        return {"results": [], "count": 0}
    
    async with async_session() as s:
        # Search in students
        students_result = await s.execute(
            select(Student).where(
                Student.coach_id == coach.id,
                Student.is_active == True,
                or_(
                    func.lower(Student.name).contains(query),
                    func.lower(Student.nickname).contains(query),
                    Student.phone.contains(query),
                    Student.parent_phone.contains(query)
                )
            ).order_by(Student.name).limit(20)
        )
        students = students_result.scalars().all()
        
        results = [{
            "type": "student",
            "id": st.id,
            "name": st.name,
            "nickname": st.nickname,
            "phone": st.phone,
            "parent_phone": st.parent_phone,
            "age": st.age,
            "lessons_remaining": get_remaining_lessons(st),
            "is_unlimited": st.is_unlimited,
            "location": st.location,
        } for st in students]
        
        return {
            "results": results,
            "count": len(results)
        }


# === Student Schedules (Multiple Locations) ===

@app.post("/api/students/{student_id}/schedules")
async def api_get_student_schedules(student_id: int, request: Request):
    """Get all schedules (locations) for a student."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    async with async_session() as s:
        # Verify student exists and belongs to coach
        student_result = await s.execute(
            select(Student).where(Student.id == student_id)
        )
        student = student_result.scalar_one_or_none()
        if not student:
            return JSONResponse({"error": "not_found"}, 404)
        
        # Get all schedules with location info
        schedules_result = await s.execute(
            select(StudentSchedule, Location).outerjoin(
                Location, StudentSchedule.location_id == Location.id
            ).where(StudentSchedule.student_id == student_id)
        )
        
        schedules = []
        for sched, loc in schedules_result.all():
            schedules.append({
                "id": sched.id,
                "location_id": sched.location_id,
                "location_name": loc.name if loc else "Зал",
                "location_address": loc.address if loc else None,
                "days": sched.days,
                "times": sched.times,
                "duration": sched.duration,
                "is_primary": sched.is_primary,
            })
        
        return {
            "student_id": student_id,
            "student_name": student.name,
            "schedules": schedules
        }


@app.post("/api/students/{student_id}/schedules/create")
async def api_create_student_schedule(student_id: int, request: Request):
    """Add a new schedule (location) for a student."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    data = body.get("schedule", {})
    
    async with async_session() as s:
        # Verify student belongs to coach
        student_result = await s.execute(
            select(Student).where(Student.id == student_id, Student.coach_id == coach.id)
        )
        student = student_result.scalar_one_or_none()
        if not student:
            return JSONResponse({"error": "not_found"}, 404)
        
        # Verify location belongs to coach
        location_id = data.get("location_id")
        if location_id:
            loc_result = await s.execute(
                select(Location).where(Location.id == location_id, Location.coach_id == coach.id)
            )
            if not loc_result.scalar_one_or_none():
                return JSONResponse({"error": "location_not_found"}, 404)
        
        # If this is the first schedule, mark as primary
        existing_count = await s.execute(
            select(func.count(StudentSchedule.id)).where(StudentSchedule.student_id == student_id)
        )
        is_primary = existing_count.scalar() == 0
        
        # Handle times JSON
        times = data.get("times")
        if not times and data.get("time"):
            # Legacy format: single time for all days
            days = data.get("days", "1,3").split(",")
            times = json.dumps({day.strip(): data["time"] for day in days})
        elif isinstance(times, dict):
            times = json.dumps(times)
        
        schedule = StudentSchedule(
            student_id=student_id,
            location_id=location_id,
            days=data.get("days", "1,3"),
            times=times or '{"1": "18:00", "3": "18:00"}',
            duration=int(data.get("duration", 90)),
            is_primary=data.get("is_primary", is_primary)
        )
        s.add(schedule)
        await s.commit()
        
        return {"success": True, "id": schedule.id}


@app.post("/api/schedules/{schedule_id}/update")
async def api_update_schedule(schedule_id: int, request: Request):
    """Update a student schedule."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    data = body.get("schedule", {})
    
    async with async_session() as s:
        # Get schedule and verify ownership through student
        sched_result = await s.execute(
            select(StudentSchedule, Student).join(Student).where(
                StudentSchedule.id == schedule_id,
                Student.coach_id == coach.id
            )
        )
        row = sched_result.one_or_none()
        if not row:
            return JSONResponse({"error": "not_found"}, 404)
        
        schedule, student = row
        
        # Update fields
        if "location_id" in data:
            schedule.location_id = data["location_id"]
        if "days" in data:
            schedule.days = data["days"]
        if "times" in data:
            schedule.times = json.dumps(data["times"]) if isinstance(data["times"], dict) else data["times"]
        elif "time" in data:
            # Update time for all days
            days = schedule.days.split(",") if schedule.days else []
            times = {}
            for day in days:
                times[day.strip()] = data["time"]
            schedule.times = json.dumps(times)
        if "duration" in data:
            schedule.duration = int(data["duration"])
        if "is_primary" in data:
            schedule.is_primary = bool(data["is_primary"])
        
        await s.commit()
        return {"success": True}


@app.post("/api/schedules/{schedule_id}/delete")
async def api_delete_schedule(schedule_id: int, request: Request):
    """Delete a student schedule."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    async with async_session() as s:
        # Get schedule and verify ownership
        sched_result = await s.execute(
            select(StudentSchedule, Student).join(Student).where(
                StudentSchedule.id == schedule_id,
                Student.coach_id == coach.id
            )
        )
        row = sched_result.one_or_none()
        if not row:
            return JSONResponse({"error": "not_found"}, 404)
        
        schedule, student = row
        
        # Check if it's the only schedule
        count_result = await s.execute(
            select(func.count(StudentSchedule.id)).where(StudentSchedule.student_id == student.id)
        )
        if count_result.scalar() <= 1:
            return JSONResponse({"error": "cannot_delete_only_schedule"}, 400)
        
        await s.delete(schedule)
        await s.commit()
        return {"success": True}


@app.post("/api/students/{student_id}/schedules/set-primary")
async def api_set_primary_schedule(student_id: int, request: Request):
    """Set a schedule as primary (and unset others)."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    primary_schedule_id = body.get("schedule_id")
    
    async with async_session() as s:
        # Verify student belongs to coach
        student_result = await s.execute(
            select(Student).where(Student.id == student_id, Student.coach_id == coach.id)
        )
        if not student_result.scalar_one_or_none():
            return JSONResponse({"error": "not_found"}, 404)
        
        # Unset all primary for this student
        await s.execute(
            select(StudentSchedule).where(StudentSchedule.student_id == student_id)
        )
        schedules = await s.execute(
            select(StudentSchedule).where(StudentSchedule.student_id == student_id)
        )
        for sched in schedules.scalars().all():
            sched.is_primary = (sched.id == primary_schedule_id)
        
        await s.commit()
        return {"success": True}


# === Finance Reports ===

@app.post("/api/finance/summary")
async def api_finance_summary(request: Request):
    """Get financial summary for period."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    period = body.get("period", "month")  # month, year, all
    location_id = body.get("location_id")  # Filter by location
    
    today = datetime.now(BELARUS_TZ).date()
    
    if period == "month":
        start_date = today.replace(day=1)
    elif period == "year":
        start_date = today.replace(month=1, day=1)
    else:
        start_date = date(2000, 1, 1)
    
    async with async_session() as s:
        # Base query
        base_query = select(Payment).where(Payment.coach_id == coach.id)
        
        if location_id:
            # Join with students to filter by location
            base_query = base_query.join(Student).where(Student.location_id == location_id)
        
        # Total revenue
        revenue_result = await s.execute(
            select(func.sum(Payment.amount)).where(
                Payment.coach_id == coach.id,
                Payment.status == "paid",
                Payment.paid_at >= start_date
            )
        )
        total_revenue = revenue_result.scalar() or 0
        
        # Revenue by coach (for shared view)
        revenue_by_coach = []
        coaches_result = await s.execute(select(Coach).where(Coach.is_active == True))
        all_coaches = coaches_result.scalars().all()
        
        for c in all_coaches:
            coach_revenue = await s.execute(
                select(func.sum(Payment.amount)).where(
                    Payment.coach_id == c.id,
                    Payment.status == "paid",
                    Payment.paid_at >= start_date
                )
            )
            rev = coach_revenue.scalar() or 0
            if rev > 0:
                revenue_by_coach.append({
                    "coach_id": c.id,
                    "coach_name": c.first_name,
                    "revenue": rev
                })
        
        # Revenue by location
        revenue_by_location = []
        locations_result = await s.execute(
            select(Location).where(Location.is_active == True)
        )
        all_locations = locations_result.scalars().all()
        
        for loc in all_locations:
            # Get payments for students in this location
            loc_revenue = await s.execute(
                select(func.sum(Payment.amount)).join(Student).where(
                    Student.location_id == loc.id,
                    Payment.status == "paid",
                    Payment.paid_at >= start_date
                )
            )
            rev = loc_revenue.scalar() or 0
            if rev > 0:
                revenue_by_location.append({
                    "location_id": loc.id,
                    "location_name": loc.name,
                    "revenue": rev
                })
        
        # Pending payments (overdue + pending)
        pending_result = await s.execute(
            select(func.sum(Payment.amount)).where(
                Payment.coach_id == coach.id,
                Payment.status.in_(["pending", "overdue"]),
                Payment.created_at >= start_date
            )
        )
        pending_amount = pending_result.scalar() or 0
        
        # Overdue breakdown
        overdue_result = await s.execute(
            select(Payment, Student).join(Student).where(
                Payment.coach_id == coach.id,
                Payment.status == "overdue"
            ).order_by(desc(Payment.created_at))
        )
        
        overdue_list = []
        for payment, student in overdue_result.all():
            overdue_list.append({
                "id": payment.id,
                "student_name": student.name,
                "amount": payment.amount,
                "period_end": payment.period_end.isoformat() if payment.period_end else None,
                "days_overdue": (today - payment.period_end).days if payment.period_end else 0
            })
        
        # Monthly trend (last 6 months) - correct month calculation
        monthly_trend = []
        for i in range(5, -1, -1):
            # Calculate month correctly (handle year boundaries)
            total_months = today.year * 12 + today.month - 1 - i
            year = total_months // 12
            month = (total_months % 12) + 1
            month_date = date(year, month, 1)
            
            month_start = month_date
            if month == 12:
                month_end = date(year + 1, 1, 1)
            else:
                month_end = date(year, month + 1, 1)
            
            month_revenue = await s.execute(
                select(func.sum(Payment.amount)).where(
                    Payment.status == "paid",
                    Payment.paid_at >= month_start,
                    Payment.paid_at < month_end
                )
            )
            monthly_trend.append({
                "month": month_date.strftime("%b"),
                "revenue": month_revenue.scalar() or 0
            })
        
        return {
            "period": period,
            "summary": {
                "total_revenue": total_revenue,
                "pending_amount": pending_amount,
                "overdue_count": len(overdue_list),
                "overdue_total": sum(o["amount"] for o in overdue_list)
            },
            "by_coach": revenue_by_coach,
            "by_location": revenue_by_location,
            "overdue_payments": overdue_list[:10],  # Top 10
            "monthly_trend": monthly_trend
        }


@app.post("/api/finance/debtors")
async def api_finance_debtors(request: Request):
    """Get list of students with payment issues."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    today = datetime.now(BELARUS_TZ).date()
    
    async with async_session() as s:
        # Get all active students with subscription info
        result = await s.execute(
            select(Student).where(
                Student.coach_id == coach.id,
                Student.is_active == True
            ).order_by(Student.name)
        )
        students = result.scalars().all()
        
        debtors = {
            "expired_subscription": [],  # Subscription ended
            "ending_soon": [],           # 1-3 days left
            "no_lessons": [],            # 0 lessons remaining
            "low_lessons": []            # 1-2 lessons remaining
        }
        
        for student in students:
            remaining = student.lessons_remaining if student.lessons_remaining is not None else student.lessons_count
            
            # Check lessons
            if remaining <= 0:
                debtors["no_lessons"].append({
                    "id": student.id,
                    "name": student.name,
                    "coach_id": student.coach_id,
                    "reason": "no_lessons",
                    "remaining": 0
                })
            elif remaining <= 2:
                debtors["low_lessons"].append({
                    "id": student.id,
                    "name": student.name,
                    "coach_id": student.coach_id,
                    "reason": "low_lessons",
                    "remaining": remaining
                })
            
            # Check subscription
            if student.subscription_end:
                days_left = (student.subscription_end - today).days
                if days_left < 0:
                    debtors["expired_subscription"].append({
                        "id": student.id,
                        "name": student.name,
                        "coach_id": student.coach_id,
                        "reason": "expired",
                        "days_overdue": abs(days_left)
                    })
                elif days_left <= 3:
                    debtors["ending_soon"].append({
                        "id": student.id,
                        "name": student.name,
                        "coach_id": student.coach_id,
                        "reason": "ending_soon",
                        "days_left": days_left
                    })
        
        return {
            "counts": {
                "expired": len(debtors["expired_subscription"]),
                "ending_soon": len(debtors["ending_soon"]),
                "no_lessons": len(debtors["no_lessons"]),
                "low_lessons": len(debtors["low_lessons"]),
                "total": sum(len(v) for v in debtors.values())
            },
            "debtors": debtors
        }
