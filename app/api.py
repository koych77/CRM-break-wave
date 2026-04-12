from fastapi import FastAPI, UploadFile, File, Query, Request, Form, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from sqlalchemy import select, func, and_, or_, desc
from contextlib import asynccontextmanager
from datetime import datetime, date, timedelta
from typing import Optional
import hmac
import hashlib
import json
import os
import logging
import urllib.parse

from app.database import async_session, init_db
from app.models import Coach, Student, Lesson, Attendance, Payment, Notification, AdminUser, DailyNotificationLog, Location
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


# === Routes ===

@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve main HTML file with cache busting."""
    html_file = WEBAPP_DIR / "index.html"
    if html_file.exists():
        content = html_file.read_text()
        # Replace version placeholder or add version to assets
        content = content.replace('href="/assets/style.css?v=2"', f'href="/assets/style.css?v={APP_VERSION}"')
        content = content.replace('src="/assets/app.js?v=2"', f'src="/assets/app.js?v={APP_VERSION}"')
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
    
    today = date.today()
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
    
    today = date.today()
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
            "lesson_time": st.lesson_time,
            "lesson_price": st.lesson_price,
            "lessons_count": st.lessons_count,
            "lessons_remaining": st.lessons_remaining,
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
    """Get all students for coach (or all for admin)."""
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
    
    async with async_session() as s:
        if is_admin_user:
            # Admin sees all students
            result = await s.execute(
                select(Student).order_by(Student.name)
            )
        else:
            # Regular coach sees only their students
            result = await s.execute(
                select(Student).where(Student.coach_id == coach.id).order_by(Student.name)
            )
        students = result.scalars().all()
    
    return [{
        "id": st.id,
        "coach_id": st.coach_id,
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
        "lessons_remaining": st.lessons_remaining,
        "subscription_start": st.subscription_start.isoformat() if st.subscription_start else None,
        "subscription_end": st.subscription_end.isoformat() if st.subscription_end else None,
        "notes": st.notes,
        "is_active": st.is_active,
    } for st in students]


@app.post("/api/students/create")
async def api_create_student(request: Request):
    """Create new student."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    data = body.get("student", {})
    
    # Determine coach_id (admin can assign to any coach)
    coach_id = data.get("coach_id", coach.id)
    
    # Verify coach exists
    async with async_session() as s:
        target_coach = await s.get(Coach, coach_id)
        if not target_coach:
            return JSONResponse({"error": "coach_not_found"}, 400)
        
        lessons_count = int(data.get("lessons_count", 8))
        
        # Handle lesson_times (JSON format: {"1": "18:00", "3": "19:30"})
        lesson_times = data.get("lesson_times")
        if not lesson_times:
            # Convert from old format or use default
            days = data.get("lesson_days", "1,3").split(",")
            time = data.get("lesson_time", "18:00")
            lesson_times = json.dumps({day.strip(): time for day in days})
        
        student = Student(
            coach_id=coach_id,
            name=data.get("name"),
            nickname=data.get("nickname") or None,
            phone=data.get("phone") or None,
            parent_phone=data.get("parent_phone") or None,
            age=int(data.get("age")) if data.get("age") else None,
            location=data.get("location", "Зал Break Wave"),
            location_id=data.get("location_id"),
            lesson_days=data.get("lesson_days", "1,3"),
            lesson_times=lesson_times,
            lesson_price=int(data.get("lesson_price", 150)),
            lessons_count=lessons_count,
            lessons_remaining=lessons_count,
            notes=data.get("notes") or None,
        )
        s.add(student)
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
        
        # Get payments
        payments_result = await s.execute(
            select(Payment).where(Payment.student_id == student_id).order_by(desc(Payment.created_at))
        )
        payments = [{
            "id": p.id,
            "amount": p.amount,
            "status": p.status,
            "period_start": p.period_start.isoformat() if p.period_start else None,
            "period_end": p.period_end.isoformat() if p.period_end else None,
            "paid_at": p.paid_at.isoformat() if p.paid_at else None,
        } for p in payments_result.scalars().all()]
        
        # Get location info
        location_name = st.location
        if st.location_id:
            loc_result = await s.execute(select(Location).where(Location.id == st.location_id))
            loc = loc_result.scalar_one_or_none()
            if loc:
                location_name = loc.name
        
        return {
            "id": st.id,
            "name": st.name,
            "nickname": st.nickname,
            "phone": st.phone,
            "parent_phone": st.parent_phone,
            "age": st.age,
            "birthday": st.birthday.isoformat() if st.birthday else None,
            "location": location_name,
            "location_id": st.location_id,
            "lesson_days": st.lesson_days,
            "lesson_times": st.lesson_times,
            "lesson_price": st.lesson_price,
            "lessons_count": st.lessons_count,
            "lessons_remaining": st.lessons_remaining,
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
            student.lesson_times = json.dumps(data["lesson_times"]) if isinstance(data["lesson_times"], dict) else data["lesson_times"]
        elif "lesson_time" in data:
            # Backward compatibility: update times for all days
            days = student.lesson_days.split(",") if student.lesson_days else ["1"]
            times = {}
            for day in days:
                times[day.strip()] = data["lesson_time"]
            student.lesson_times = json.dumps(times)
        if "lesson_price" in data:
            student.lesson_price = int(data["lesson_price"])
        if "lessons_count" in data:
            student.lessons_count = int(data["lessons_count"])
        if "subscription_start" in data:
            student.subscription_start = date.fromisoformat(data["subscription_start"]) if data["subscription_start"] else None
        if "subscription_end" in data:
            student.subscription_end = date.fromisoformat(data["subscription_end"]) if data["subscription_end"] else None
        if "notes" in data:
            student.notes = data["notes"] or None
        if "is_active" in data:
            student.is_active = bool(data["is_active"])
        
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
        
        lesson = Lesson(
            coach_id=coach.id,
            student_id=student_id,
            date=date.fromisoformat(data.get("date")),
            time=data.get("time", student.lesson_time),
            location=data.get("location", student.location),
            topic=data.get("topic"),
            notes=data.get("notes"),
        )
        s.add(lesson)
        await s.flush()
        
        # Create attendance record
        attendance = Attendance(
            lesson_id=lesson.id,
            student_id=student_id,
            status=data.get("status", "present"),
            notes=data.get("attendance_notes"),
        )
        s.add(attendance)
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
            att.status = body.get("status", "present")
            att.notes = body.get("notes")
        else:
            att = Attendance(
                lesson_id=lesson_id,
                student_id=lesson.student_id,
                status=body.get("status", "present"),
                notes=body.get("notes"),
            )
            s.add(att)
        
        await s.commit()
        return {"success": True}


# === Payments ===

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
        payment = Payment(
            coach_id=coach.id,
            student_id=data.get("student_id"),
            amount=int(data.get("amount", 0)),
            lessons_count=int(data.get("lessons_count", 8)),
            status=data.get("status", "pending"),
            period_start=date.fromisoformat(data["period_start"]) if data.get("period_start") else None,
            period_end=date.fromisoformat(data["period_end"]) if data.get("period_end") else None,
            notes=data.get("notes"),
        )
        
        if payment.status == "paid":
            payment.paid_at = datetime.utcnow()
        
        s.add(payment)
        await s.commit()
        
        # Update student subscription dates and lessons if paid
        if payment.status == "paid":
            student_result = await s.execute(
                select(Student).where(Student.id == payment.student_id)
            )
            student = student_result.scalar_one_or_none()
            if student:
                if payment.period_end:
                    student.subscription_end = payment.period_end
                    if not student.subscription_start:
                        student.subscription_start = payment.period_start
                
                # Add lessons to subscription
                if payment.lessons_count:
                    student.lessons_count += payment.lessons_count
                    student.lessons_remaining += payment.lessons_count
        
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
        
        # Update student subscription
        student_result = await s.execute(
            select(Student).where(Student.id == payment.student_id)
        )
        student = student_result.scalar_one_or_none()
        if student and payment.period_end:
            student.subscription_end = payment.period_end
            if not student.subscription_start:
                student.subscription_start = payment.period_start
        
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
    now = datetime.now()
    current_weekday = now.weekday()
    current_time = now.strftime("%H:%M")
    current_date = now.date()
    
    async with async_session() as s:
        # Get all active students for this coach
        result = await s.execute(
            select(Student).where(
                Student.coach_id == coach.id,
                Student.is_active == True
            )
        )
        all_students = result.scalars().all()
        
        # Find students who have lesson now (within time window)
        current_lessons = []
        for student in all_students:
            days = student.lesson_days.split(",") if student.lesson_days else []
            if str(current_weekday) in days:
                # Check if within lesson time (±15 min window)
                lesson_time = student.get_lesson_time_for_day(current_weekday)
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
                    
                    current_lessons.append({
                        "student_id": student.id,
                        "name": student.name,
                        "nickname": student.nickname,
                        "lesson_time": lesson_time,
                        "lesson_duration": student.lesson_duration,
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
    lesson_date = body.get("date", datetime.now().date().isoformat())
    
    if not data:
        return JSONResponse({"error": "no_data"}, 400)
    
    async with async_session() as s:
        marked_count = 0
        students_with_low_lessons = []
        
        for item in data:
            student_id = item.get("student_id")
            status = item.get("status", "present")
            
            # Get student info
            student_result = await s.execute(
                select(Student).where(Student.id == student_id, Student.coach_id == coach.id)
            )
            student = student_result.scalar_one_or_none()
            if not student:
                continue
            
            # Check if lesson exists
            lesson_result = await s.execute(
                select(Lesson).where(
                    Lesson.student_id == student_id,
                    Lesson.date == date.fromisoformat(lesson_date)
                )
            )
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
                else:
                    att = Attendance(
                        lesson_id=lesson.id,
                        student_id=student_id,
                        status=status,
                        attendance_date=date.fromisoformat(lesson_date),
                        attendance_time=student.lesson_time
                    )
                    s.add(att)
            else:
                # Create new lesson
                lesson = Lesson(
                    coach_id=coach.id,
                    student_id=student_id,
                    date=date.fromisoformat(lesson_date),
                    time=student.lesson_time,
                    location=student.location,
                )
                s.add(lesson)
                await s.flush()
                
                # Create attendance
                att = Attendance(
                    lesson_id=lesson.id,
                    student_id=student_id,
                    status=status,
                    attendance_date=date.fromisoformat(lesson_date),
                    attendance_time=student.lesson_time
                )
                s.add(att)
            
            # Update lessons_remaining based on status change
            # Only deduct for "present" status, restore if changed from present to absent/sick
            if status == "present" and old_status != "present":
                # Deduct a lesson
                if student.lessons_remaining > 0:
                    student.lessons_remaining -= 1
                # Check if now low on lessons
                if student.lessons_remaining <= 2:
                    students_with_low_lessons.append({
                        "id": student.id,
                        "name": student.name,
                        "remaining": student.lessons_remaining
                    })
            elif status != "present" and old_status == "present":
                # Restore a lesson (changed from present to absent/sick)
                student.lessons_remaining += 1
            
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
    lesson_date = body.get("date", datetime.now().date().isoformat())
    
    async with async_session() as s:
        # Get all students for this coach
        result = await s.execute(
            select(Student).where(
                Student.coach_id == coach.id,
                Student.is_active == True
            )
        )
        students = result.scalars().all()
        
        for student in students:
            # Check if lesson already exists
            existing = await s.execute(
                select(Lesson).where(
                    Lesson.student_id == student.id,
                    Lesson.date == date.fromisoformat(lesson_date)
                )
            )
            if existing.scalar_one_or_none():
                continue  # Already marked
            
            # Create skipped lesson
            lesson = Lesson(
                coach_id=coach.id,
                student_id=student.id,
                date=date.fromisoformat(lesson_date),
                time=student.lesson_time,
                location=student.location,
                notes=f"Тренировка отменена: {reason}"
            )
            s.add(lesson)
            await s.flush()
            
            # Mark as excused (не влияет на статистику)
            att = Attendance(
                lesson_id=lesson.id,
                student_id=student.id,
                status="excused"
            )
            s.add(att)
        
        await s.commit()
        return {"success": True, "skipped": len(students)}


# === Calendar ===

@app.post("/api/calendar")
async def api_calendar(request: Request):
    """Get calendar data with lessons."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    year = body.get("year", date.today().year)
    month = body.get("month", date.today().month)
    
    # Calculate date range
    month_start = date(year, month, 1)
    if month == 12:
        month_end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        month_end = date(year, month + 1, 1) - timedelta(days=1)
    
    async with async_session() as s:
        result = await s.execute(
            select(Lesson, Student).join(Student).where(
                Lesson.coach_id == coach.id,
                Lesson.date >= month_start,
                Lesson.date <= month_end
            ).order_by(Lesson.date, Lesson.time)
        )
        
        days = {}
        for lesson, student in result.all():
            day = lesson.date.day
            if day not in days:
                days[day] = []
            days[day].append({
                "id": lesson.id,
                "time": lesson.time,
                "student_name": student.name,
                "student_id": student.id,
            })
        
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
    attendance_date = body.get("date", datetime.now().date().isoformat())
    attendance_time = body.get("time", datetime.now().strftime("%H:%M"))
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
        
        # Check remaining lessons
        if deduct_lesson and student.lessons_remaining <= 0:
            return JSONResponse({
                "error": "no_lessons_remaining",
                "message": "У ученика не осталось занятий в абонементе"
            }, 400)
        
        # Create extra attendance record (no lesson entry - this is out-of-schedule)
        att = Attendance(
            lesson_id=None,  # No scheduled lesson
            student_id=student_id,
            status=status,
            is_extra=True,
            attendance_date=date.fromisoformat(attendance_date),
            attendance_time=attendance_time,
            notes=notes or "Внеплановое посещение"
        )
        s.add(att)
        
        # Deduct from remaining lessons if present
        if deduct_lesson and status == "present":
            student.lessons_remaining = max(0, student.lessons_remaining - 1)
        
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
        total_lessons = len([a for a in attendance if not a["is_extra"]])
        extra_lessons = len([a for a in attendance if a["is_extra"]])
        present_count = len([a for a in attendance if a["status"] == "present"])
        
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
                "attendance_rate": round(present_count / len(attendance) * 100) if attendance else 0
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
            student.lessons_remaining += int(data["add_lessons"])
            student.lessons_count += int(data["add_lessons"])
        
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
        
        today = date.today()
        
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
        if student.lessons_remaining <= 0:
            lessons_status = "depleted"
        elif student.lessons_remaining <= 2:
            lessons_status = "low"
        
        return {
            "student_id": student.id,
            "name": student.name,
            "subscription": {
                "total_lessons": student.lessons_count,
                "remaining": student.lessons_remaining,
                "used": student.lessons_count - student.lessons_remaining,
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
    lesson_date = body.get("date", datetime.now().date().isoformat())
    
    async with async_session() as s:
        # Verify student belongs to coach
        student_result = await s.execute(
            select(Student).where(Student.id == student_id, Student.coach_id == coach.id)
        )
        student = student_result.scalar_one_or_none()
        if not student:
            return JSONResponse({"error": "student_not_found"}, 404)
        
        # If target_time specified, temporarily update student's schedule
        if target_time and target_time != student.lesson_time:
            # Store original time for reference
            original_time = student.lesson_time
            
            # Create extra attendance for this specific session
            att = Attendance(
                lesson_id=None,
                student_id=student_id,
                status="present",
                is_extra=True,
                attendance_date=date.fromisoformat(lesson_date),
                attendance_time=target_time,
                notes=f"Добавлен к группе {target_time} (обычное время: {original_time})"
            )
            s.add(att)
            
            # Deduct lesson if present
            if student.lessons_remaining > 0:
                student.lessons_remaining -= 1
            
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
            select(Student).where(
                Student.coach_id == coach.id,
                Student.is_active == True
            ).order_by(Student.lesson_time, Student.name)
        )
        students = result.scalars().all()
        
        # Group by time
        groups = {}
        for student in students:
            time_key = student.lesson_time or "18:00"
            if time_key not in groups:
                groups[time_key] = {
                    "time": time_key,
                    "students": [],
                    "count": 0,
                    "low_lessons": 0
                }
            
            groups[time_key]["students"].append({
                "id": student.id,
                "name": student.name,
                "lessons_remaining": student.lessons_remaining,
                "subscription_end": student.subscription_end.isoformat() if student.subscription_end else None
            })
            groups[time_key]["count"] += 1
            
            if student.lessons_remaining <= 2:
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
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    today = date.today()
    
    async with async_session() as s:
        # Get all active students
        result = await s.execute(
            select(Student).where(
                Student.coach_id == coach.id,
                Student.is_active == True
            )
        )
        students = result.scalars().all()
        
        # Categorize students
        payments_due = []  # Subscription ended or ending within 3 days
        low_lessons = []   # 2 or fewer lessons remaining
        depleted = []      # No lessons remaining
        
        for student in students:
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
            if student.lessons_remaining <= 0:
                depleted.append({
                    "id": student.id,
                    "name": student.name,
                    "lessons_remaining": 0
                })
            elif student.lessons_remaining <= 2:
                low_lessons.append({
                    "id": student.id,
                    "name": student.name,
                    "lessons_remaining": student.lessons_remaining
                })
        
        # Get today's lessons
        weekday = today.weekday()
        today_lessons = []
        for student in students:
            days = student.lesson_days.split(",") if student.lesson_days else []
            if str(weekday) in days:
                today_lessons.append({
                    "id": student.id,
                    "name": student.name,
                    "time": student.lesson_time,
                    "lessons_remaining": student.lessons_remaining
                })
        
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
    
    today = date.today()
    
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
        total_present = sum(1 for att, _ in attendance_records if att.status == "present")
        total_absent = sum(1 for att, _ in attendance_records if att.status == "absent")
        total_sick = sum(1 for att, _ in attendance_records if att.status == "sick")
        total_lessons = len(attendance_records)
        
        attendance_rate = round(total_present / total_lessons * 100) if total_lessons > 0 else 0
        
        # By day of week
        by_day_of_week = {}
        for att, student in attendance_records:
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
        
        for att, student in attendance_records:
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
        
        # Monthly trend (last 6 months)
        monthly_trend = []
        for i in range(5, -1, -1):
            month_date = today.replace(day=1) - timedelta(days=i*30)
            month_start = month_date.replace(day=1)
            if month_date.month == 12:
                month_end = month_date.replace(year=month_date.year + 1, month=1, day=1)
            else:
                month_end = month_date.replace(month=month_date.month + 1, day=1)
            
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
            "lessons_remaining": st.lessons_remaining,
            "location": st.location,
        } for st in students]
        
        return {
            "results": results,
            "count": len(results)
        }
