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
from app.models import Coach, Student, Lesson, Attendance, Payment, Notification
from app.config import WEBAPP_DIR, BOT_TOKEN, WEEKDAYS

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
    
    return {
        "coach_id": coach.id,
        "first_name": coach.first_name,
        "telegram_id": coach.telegram_id,
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


# === Students ===

@app.post("/api/students")
async def api_students(request: Request):
    """Get all students for coach."""
    body = await request.json()
    coach = await get_current_coach(body.get("initData", ""))
    if not coach:
        return JSONResponse({"error": "unauthorized"}, 403)
    
    async with async_session() as s:
        result = await s.execute(
            select(Student).where(Student.coach_id == coach.id).order_by(Student.name)
        )
        students = result.scalars().all()
    
    return [{
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
    
    async with async_session() as s:
        student = Student(
            coach_id=coach.id,
            name=data.get("name"),
            nickname=data.get("nickname") or None,
            phone=data.get("phone") or None,
            parent_phone=data.get("parent_phone") or None,
            age=int(data.get("age")) if data.get("age") else None,
            location=data.get("location", "Зал Break Wave"),
            lesson_days=data.get("lesson_days", "1,3"),
            lesson_time=data.get("lesson_time", "18:00"),
            lesson_price=int(data.get("lesson_price", 5000)),
            lessons_count=int(data.get("lessons_count", 8)),
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
        
        return {
            "id": st.id,
            "name": st.name,
            "nickname": st.nickname,
            "phone": st.phone,
            "parent_phone": st.parent_phone,
            "age": st.age,
            "birthday": st.birthday.isoformat() if st.birthday else None,
            "location": st.location,
            "lesson_days": st.lesson_days,
            "lesson_time": st.lesson_time,
            "lesson_price": st.lesson_price,
            "lessons_count": st.lessons_count,
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
        if "lesson_days" in data:
            student.lesson_days = data["lesson_days"]
        if "lesson_time" in data:
            student.lesson_time = data["lesson_time"]
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
        
        # Update student subscription dates if paid
        if payment.status == "paid" and payment.period_end:
            student_result = await s.execute(
                select(Student).where(Student.id == payment.student_id)
            )
            student = student_result.scalar_one_or_none()
            if student:
                student.subscription_end = payment.period_end
                if not student.subscription_start:
                    student.subscription_start = payment.period_start
        
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
