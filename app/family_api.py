"""Parent and administrator workflows for the existing CRM Mini App."""

from __future__ import annotations

import json
import os
import secrets
from datetime import date, datetime, timedelta

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from sqlalchemy import delete, desc, func, or_, select
from sqlalchemy.orm import selectinload

from app.api import (
    BELARUS_TZ,
    add_audit_log,
    ensure_monthly_invoice,
    get_current_coach,
    get_current_parent,
    get_parent_student,
    is_admin_user,
    verify_telegram_init_data,
)
from app.config import ADMIN_IDS, BOT_USERNAME, WEBAPP_URL
from app.database import async_session
from app.family import (
    ABSENCE_STATUSES,
    ATTENDANCE_STATUSES,
    TARIFFS,
    month_bounds,
    payment_due_date,
    payment_total,
    tariff_details,
)
from app.models import (
    Attendance,
    Coach,
    Location,
    MakeupCredit,
    Notification,
    ParentAccount,
    Payment,
    RegistrationInvite,
    RegistrationRequest,
    ScheduleRequest,
    Student,
    StudentSchedule,
    TrainingGroup,
)


router = APIRouter(prefix="/api")


def _invitation_url(token: str) -> str:
    bot_username = (os.getenv("BOT_USERNAME") or BOT_USERNAME).strip().lstrip("@")
    if bot_username:
        return f"https://t.me/{bot_username}?start=invite_{token}"
    return f"{WEBAPP_URL}/?invite={token}"


async def _admin_actor(body: dict) -> tuple[Coach | None, dict | None]:
    init_data = body.get("initData", "")
    user = verify_telegram_init_data(init_data)
    coach = await get_current_coach(init_data)
    if not user or not coach or not await is_admin_user(user.get("id")):
        return None, user
    return coach, user


async def _queue_admin_notification(
    session,
    notification_type: str,
    message: str,
    student_id: int | None = None,
) -> None:
    query = select(Coach).where(
        Coach.is_active == True,
        or_(
            Coach.is_admin == True,
            Coach.telegram_id.in_(ADMIN_IDS or [-1]),
        ),
    )
    admins = (await session.execute(query)).scalars().all()
    for admin in admins:
        session.add(Notification(
            coach_id=admin.id,
            student_id=student_id,
            type=notification_type,
            message=message,
            recipient_telegram_id=admin.telegram_id,
        ))


async def _queue_parent_notification(
    session,
    student: Student,
    parent: ParentAccount,
    notification_type: str,
    message: str,
) -> None:
    session.add(Notification(
        coach_id=student.coach_id,
        student_id=student.id,
        type=notification_type,
        message=message,
        recipient_telegram_id=parent.telegram_id,
    ))


def _schedule_payload(value) -> list[dict]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []
    if not isinstance(value, list):
        return []
    normalized = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            continue
        days = item.get("days", "")
        if isinstance(days, list):
            days = ",".join(str(day) for day in days)
        times = item.get("times", {})
        if isinstance(times, str):
            try:
                json.loads(times)
            except json.JSONDecodeError:
                times = {}
        if isinstance(times, dict):
            times = json.dumps(times, ensure_ascii=False)
        normalized.append({
            "location_id": int(item["location_id"]) if item.get("location_id") else None,
            "group_id": int(item["group_id"]) if item.get("group_id") else None,
            "days": str(days or ""),
            "times": str(times or "{}"),
            "duration": int(item.get("duration") or 90),
            "is_primary": bool(item.get("is_primary", index == 0)),
        })
    if normalized and not any(item["is_primary"] for item in normalized):
        normalized[0]["is_primary"] = True
    return normalized


async def _replace_student_schedules(session, student: Student, schedules: list[dict]) -> None:
    await session.execute(
        delete(StudentSchedule).where(StudentSchedule.student_id == student.id)
    )
    for item in schedules:
        session.add(StudentSchedule(student_id=student.id, **item))

    if schedules:
        primary = next((item for item in schedules if item["is_primary"]), schedules[0])
        student.location_id = primary["location_id"]
        student.lesson_days = primary["days"] or "1,3"
        student.lesson_times = primary["times"] or '{"1":"18:00","3":"18:00"}'
        if primary["location_id"]:
            location = await session.get(Location, primary["location_id"])
            if location:
                student.location = location.name


def _invoice_json(payment: Payment, today: date) -> dict:
    base_amount = payment.base_amount or payment.amount
    late_fee, total = (
        (0, base_amount)
        if payment.tariff_code == "single" or payment.status == "paid"
        else payment_total(base_amount, payment.due_date, today)
    )
    effective_status = payment.status
    if effective_status == "pending" and payment.due_date and payment.due_date < today:
        effective_status = "overdue"
    return {
        "id": payment.id,
        "tariff_code": payment.tariff_code,
        "tariff": tariff_details(payment.tariff_code),
        "base_amount": base_amount,
        "late_fee_amount": payment.late_fee_amount if payment.status == "paid" else late_fee,
        "amount": payment.amount if payment.status == "paid" else total,
        "status": effective_status,
        "stored_status": payment.status,
        "payment_method": payment.payment_method,
        "receipt_attached": bool(payment.receipt_file_id),
        "rejection_reason": payment.rejection_reason,
        "period_start": payment.period_start.isoformat() if payment.period_start else None,
        "period_end": payment.period_end.isoformat() if payment.period_end else None,
        "due_date": payment.due_date.isoformat() if payment.due_date else None,
    }


@router.post("/parent/register")
async def parent_register(request: Request):
    body = await request.json()
    user = verify_telegram_init_data(body.get("initData", ""))
    token = str(body.get("invite_token") or "").removeprefix("invite_")
    if not user or not token:
        return JSONResponse({"error": "unauthorized"}, 403)

    parent_data = body.get("parent") or {}
    child_data = body.get("child") or {}
    schedule = _schedule_payload(body.get("proposed_schedule"))
    required = [
        str(parent_data.get("full_name") or "").strip(),
        str(parent_data.get("phone") or "").strip(),
        str(child_data.get("name") or "").strip(),
        str(child_data.get("birthday") or "").strip(),
    ]
    if not all(required) or not schedule:
        return JSONResponse({"error": "required_fields"}, 400)

    try:
        birthday = date.fromisoformat(required[3])
    except ValueError:
        return JSONResponse({"error": "invalid_birthday"}, 400)

    async with async_session() as session:
        invite_result = await session.execute(
            select(RegistrationInvite).where(
                RegistrationInvite.token == token,
                RegistrationInvite.status == "active",
                RegistrationInvite.expires_at > datetime.utcnow(),
            )
        )
        invite = invite_result.scalar_one_or_none()
        if not invite:
            return JSONResponse({"error": "invite_expired"}, 410)

        parent_result = await session.execute(
            select(ParentAccount).where(ParentAccount.telegram_id == user["id"])
        )
        parent = parent_result.scalar_one_or_none()
        if parent:
            parent.full_name = required[0]
            parent.phone = required[1]
            parent.bot_blocked_at = None
        else:
            parent = ParentAccount(
                telegram_id=user["id"],
                full_name=required[0],
                phone=required[1],
                training_reminders_enabled=True,
            )
            session.add(parent)
            await session.flush()

        registration = RegistrationRequest(
            invite_id=invite.id,
            parent_id=parent.id,
            child_name=required[2],
            child_birthday=birthday,
            child_phone=str(child_data.get("phone") or "").strip() or None,
            proposed_schedule=json.dumps(schedule, ensure_ascii=False),
            status="pending",
        )
        session.add(registration)
        invite.status = "used"
        invite.used_at = datetime.utcnow()
        await session.flush()
        await _queue_admin_notification(
            session,
            "registration_request",
            f"🆕 Новая заявка на регистрацию\nРебёнок: {registration.child_name}\nРодитель: {parent.full_name}",
        )
        await add_audit_log(
            session,
            user["id"],
            "registration_submitted",
            "registration_request",
            registration.id,
        )
        await session.commit()
        return {"success": True, "request_id": registration.id, "status": "pending"}


@router.post("/parent/context")
async def parent_context(request: Request):
    body = await request.json()
    parent = await get_current_parent(body.get("initData", ""))
    if not parent:
        return JSONResponse({"error": "unauthorized"}, 403)

    today = datetime.now(BELARUS_TZ).date()
    async with async_session() as session:
        missed_makeups = (
            await session.execute(
                select(MakeupCredit).where(
                    MakeupCredit.status == "scheduled",
                    MakeupCredit.scheduled_date < today,
                )
            )
        ).scalars().all()
        for credit in missed_makeups:
            attendance = (
                await session.execute(
                    select(Attendance).where(Attendance.makeup_credit_id == credit.id)
                )
            ).scalar_one_or_none()
            credit.status = "used" if attendance and attendance.status == "present" else "burned"
            credit.used_at = datetime.utcnow()

        expired_makeups = (
            await session.execute(
                select(MakeupCredit).where(
                    MakeupCredit.status.in_({"available", "requested", "scheduled"}),
                    MakeupCredit.expires_at < today,
                )
            )
        ).scalars().all()
        for credit in expired_makeups:
            credit.status = "expired"

        students_result = await session.execute(
            select(Student).options(
                selectinload(Student.coach),
                selectinload(Student.schedules).selectinload(StudentSchedule.location),
                selectinload(Student.schedules).selectinload(StudentSchedule.group),
            ).where(
                Student.parent_id == parent.id,
                Student.is_active == True,
                Student.deleted_at.is_(None),
            ).order_by(Student.name)
        )
        students = students_result.scalars().unique().all()
        response_students = []
        for student in students:
            invoice = await ensure_monthly_invoice(session, student, today)
            makeup_result = await session.execute(
                select(MakeupCredit).where(
                    MakeupCredit.student_id == student.id,
                    MakeupCredit.status.in_({"available", "requested", "scheduled"}),
                ).order_by(MakeupCredit.expires_at)
            )
            makeups = makeup_result.scalars().all()
            attendance_count = (
                await session.execute(
                    select(func.count(Attendance.id)).where(
                        Attendance.student_id == student.id,
                        Attendance.attendance_date >= today.replace(day=1),
                        Attendance.status.in_(ATTENDANCE_STATUSES),
                    )
                )
            ).scalar_one()
            response_students.append({
                "id": student.id,
                "name": student.name,
                "birthday": student.birthday.isoformat() if student.birthday else None,
                "phone": student.phone,
                "coach_name": student.coach.first_name if student.coach else None,
                "lessons_remaining": student.lessons_remaining or 0,
                "lessons_count": student.lessons_count or 0,
                "attendance_this_month": attendance_count,
                "training_reminders_enabled": bool(student.training_reminders_enabled),
                "subscription_start": student.subscription_start.isoformat() if student.subscription_start else None,
                "subscription_end": student.subscription_end.isoformat() if student.subscription_end else None,
                "schedules": [{
                    "id": item.id,
                    "location": item.location.name if item.location else "Зал не назначен",
                    "location_id": item.location_id,
                    "group": item.group.name if item.group else None,
                    "group_id": item.group_id,
                    "days": item.days,
                    "times": json.loads(item.times or "{}"),
                    "duration": item.duration,
                    "is_primary": item.is_primary,
                } for item in student.schedules],
                "invoice": _invoice_json(invoice, today),
                "makeups": [{
                    "id": item.id,
                    "source_date": item.source_date.isoformat(),
                    "source_type": item.source_type,
                    "expires_at": item.expires_at.isoformat(),
                    "status": item.status,
                    "requested_date": item.requested_date.isoformat() if item.requested_date else None,
                    "scheduled_date": item.scheduled_date.isoformat() if item.scheduled_date else None,
                    "scheduled_time": item.scheduled_time,
                    "rejection_reason": item.rejection_reason,
                } for item in makeups],
            })

        registrations = (
            await session.execute(
                select(RegistrationRequest).where(
                    RegistrationRequest.parent_id == parent.id
                ).order_by(desc(RegistrationRequest.created_at))
            )
        ).scalars().all()
        schedule_requests = (
            await session.execute(
                select(ScheduleRequest).where(
                    ScheduleRequest.parent_id == parent.id
                ).order_by(desc(ScheduleRequest.created_at)).limit(10)
            )
        ).scalars().all()
        await session.commit()
        return {
            "parent": {
                "id": parent.id,
                "full_name": parent.full_name,
                "phone": parent.phone,
                "training_reminders_enabled": parent.training_reminders_enabled,
            },
            "students": response_students,
            "tariffs": TARIFFS,
            "registration_requests": [{
                "id": item.id,
                "child_name": item.child_name,
                "status": item.status,
                "rejection_reason": item.rejection_reason,
            } for item in registrations],
            "schedule_requests": [{
                "id": item.id,
                "student_id": item.student_id,
                "status": item.status,
                "rejection_reason": item.rejection_reason,
            } for item in schedule_requests],
        }


@router.post("/parent/reminders")
async def parent_reminders(request: Request):
    body = await request.json()
    parent = await get_current_parent(body.get("initData", ""))
    if not parent:
        return JSONResponse({"error": "unauthorized"}, 403)
    enabled = bool(body.get("enabled"))
    student_id = body.get("student_id")
    async with async_session() as session:
        student = await get_parent_student(session, parent.id, int(student_id))
        if not student:
            return JSONResponse({"error": "student_not_found"}, 404)
        student.training_reminders_enabled = enabled
        await session.commit()
        return {"success": True, "enabled": enabled}


@router.post("/parent/schedule-request")
async def parent_schedule_request(request: Request):
    body = await request.json()
    parent = await get_current_parent(body.get("initData", ""))
    schedule = _schedule_payload(body.get("proposed_schedule"))
    if not parent:
        return JSONResponse({"error": "unauthorized"}, 403)
    if not schedule:
        return JSONResponse({"error": "schedule_required"}, 400)

    async with async_session() as session:
        student = await get_parent_student(session, parent.id, int(body.get("student_id")))
        if not student:
            return JSONResponse({"error": "student_not_found"}, 404)
        pending = (
            await session.execute(
                select(ScheduleRequest).where(
                    ScheduleRequest.student_id == student.id,
                    ScheduleRequest.status == "pending",
                )
            )
        ).scalar_one_or_none()
        if pending:
            pending.proposed_schedule = json.dumps(schedule, ensure_ascii=False)
            request_item = pending
        else:
            request_item = ScheduleRequest(
                student_id=student.id,
                parent_id=parent.id,
                proposed_schedule=json.dumps(schedule, ensure_ascii=False),
                status="pending",
            )
            session.add(request_item)
        await session.flush()
        await _queue_admin_notification(
            session,
            "schedule_request",
            f"🗓 Запрос на изменение расписания\nРебёнок: {student.name}",
            student.id,
        )
        await session.commit()
        return {"success": True, "request_id": request_item.id}


@router.post("/parent/tariff")
async def parent_tariff(request: Request):
    body = await request.json()
    parent = await get_current_parent(body.get("initData", ""))
    tariff = tariff_details(body.get("tariff_code"))
    if not parent:
        return JSONResponse({"error": "unauthorized"}, 403)
    if not tariff:
        return JSONResponse({"error": "invalid_tariff"}, 400)

    month_value = date.fromisoformat(body.get("month") or date.today().replace(day=1).isoformat())
    month_start, month_end = month_bounds(month_value)
    async with async_session() as session:
        student = await get_parent_student(session, parent.id, int(body.get("student_id")))
        if not student:
            return JSONResponse({"error": "student_not_found"}, 404)
        old_debt = (
            await session.execute(
                select(Payment).where(
                    Payment.student_id == student.id,
                    Payment.period_start < month_start,
                    Payment.status != "paid",
                    Payment.tariff_code.is_not(None),
                ).order_by(Payment.period_start)
            )
        ).scalars().first()
        if old_debt:
            return JSONResponse({
                "error": "old_debt_required",
                "payment_id": old_debt.id,
                "message": "Сначала необходимо закрыть самый старый долг",
            }, 409)

        invoice = await ensure_monthly_invoice(session, student, month_start)
        if invoice.status not in {"pending", "rejected"}:
            return JSONResponse({"error": "payment_in_review"}, 409)
        invoice.tariff_code = str(body.get("tariff_code"))
        invoice.base_amount = tariff["price"]
        invoice.amount = tariff["price"]
        invoice.lessons_count = tariff["lessons"]
        invoice.period_start = month_start
        invoice.period_end = month_end
        invoice.due_date = payment_due_date(month_start)
        invoice.status = "pending"
        invoice.rejection_reason = None
        invoice.notes = (
            "Тариф выбран родителем"
            if date.today().day <= 5
            else "Тариф выбран родителем после 5 числа"
        )
        await session.commit()
        return {"success": True, "invoice": _invoice_json(invoice, date.today())}


@router.post("/parent/payments/{payment_id}/report")
async def parent_report_payment(payment_id: int, request: Request):
    body = await request.json()
    parent = await get_current_parent(body.get("initData", ""))
    method = str(body.get("payment_method") or "")
    if not parent:
        return JSONResponse({"error": "unauthorized"}, 403)
    if method not in {"online", "cash"}:
        return JSONResponse({"error": "invalid_payment_method"}, 400)

    async with async_session() as session:
        result = await session.execute(
            select(Payment, Student).join(Student).where(
                Payment.id == payment_id,
                Student.parent_id == parent.id,
            )
        )
        row = result.one_or_none()
        if not row:
            return JSONResponse({"error": "payment_not_found"}, 404)
        payment, student = row
        if payment.status == "paid":
            return JSONResponse({"error": "already_paid"}, 409)
        payment.payment_method = method
        payment.reported_at = datetime.utcnow()
        payment.rejection_reason = None
        if method == "online" and not payment.receipt_file_id:
            payment.status = "awaiting_receipt"
            message = "Пришлите чек фотографией или файлом в этот чат с ботом."
        else:
            payment.status = "reported"
            message = "Оплата отправлена тренерам на проверку."
            await _queue_admin_notification(
                session,
                "payment_review",
                f"💳 Оплата на проверку\nРебёнок: {student.name}\nСпособ: {'наличные' if method == 'cash' else 'онлайн'}",
                student.id,
            )
        await session.commit()
        return {"success": True, "status": payment.status, "message": message}


@router.post("/parent/makeups/{makeup_id}/request")
async def parent_request_makeup(makeup_id: int, request: Request):
    body = await request.json()
    parent = await get_current_parent(body.get("initData", ""))
    if not parent:
        return JSONResponse({"error": "unauthorized"}, 403)
    try:
        requested_date = date.fromisoformat(body.get("requested_date"))
    except (TypeError, ValueError):
        return JSONResponse({"error": "invalid_date"}, 400)

    async with async_session() as session:
        result = await session.execute(
            select(MakeupCredit, Student).join(Student).where(
                MakeupCredit.id == makeup_id,
                Student.parent_id == parent.id,
                MakeupCredit.status.in_({"available", "requested"}),
            )
        )
        row = result.one_or_none()
        if not row:
            return JSONResponse({"error": "makeup_not_found"}, 404)
        credit, student = row
        if requested_date > credit.expires_at:
            return JSONResponse({"error": "after_expiry"}, 400)
        credit.requested_date = requested_date
        credit.status = "requested"
        credit.rejection_reason = None
        await _queue_admin_notification(
            session,
            "makeup_request",
            f"🔁 Запрос на отработку\nРебёнок: {student.name}\nЖелаемая дата: {requested_date.strftime('%d.%m.%Y')}",
            student.id,
        )
        await session.commit()
        return {"success": True}


@router.post("/admin/invitations")
async def admin_invitations(request: Request):
    body = await request.json()
    coach, user = await _admin_actor(body)
    if not coach:
        return JSONResponse({"error": "forbidden"}, 403)
    async with async_session() as session:
        invites = (
            await session.execute(
                select(RegistrationInvite).order_by(desc(RegistrationInvite.created_at)).limit(50)
            )
        ).scalars().all()
        return [{
            "id": item.id,
            "child_name": item.preliminary_child_name,
            "status": "expired" if item.status == "active" and item.expires_at <= datetime.utcnow() else item.status,
            "expires_at": item.expires_at.isoformat(),
            "invite_url": _invitation_url(item.token),
        } for item in invites]


@router.post("/admin/invitations/create")
async def admin_create_invitation(request: Request):
    body = await request.json()
    coach, user = await _admin_actor(body)
    child_name = str(body.get("child_name") or "").strip()
    if not coach:
        return JSONResponse({"error": "forbidden"}, 403)
    if not child_name:
        return JSONResponse({"error": "child_name_required"}, 400)
    async with async_session() as session:
        invite = RegistrationInvite(
            token=secrets.token_urlsafe(24),
            preliminary_child_name=child_name,
            created_by_coach_id=coach.id,
            expires_at=datetime.utcnow() + timedelta(days=7),
            status="active",
        )
        session.add(invite)
        await session.flush()
        await add_audit_log(
            session, user["id"], "invite_created", "registration_invite", invite.id
        )
        await session.commit()
        invite_url = _invitation_url(invite.token)
        return {
            "success": True,
            "id": invite.id,
            "invite_url": invite_url,
            "expires_at": invite.expires_at.isoformat(),
        }


@router.post("/admin/requests")
async def admin_requests(request: Request):
    body = await request.json()
    coach, _ = await _admin_actor(body)
    if not coach:
        return JSONResponse({"error": "forbidden"}, 403)
    async with async_session() as session:
        registrations = (
            await session.execute(
                select(RegistrationRequest, ParentAccount)
                .join(ParentAccount)
                .where(RegistrationRequest.status == "pending")
                .order_by(RegistrationRequest.created_at)
            )
        ).all()
        schedules = (
            await session.execute(
                select(ScheduleRequest, Student)
                .join(Student)
                .where(ScheduleRequest.status == "pending")
                .order_by(ScheduleRequest.created_at)
            )
        ).all()
        makeups = (
            await session.execute(
                select(MakeupCredit, Student)
                .join(Student)
                .where(MakeupCredit.status == "requested")
                .order_by(MakeupCredit.created_at)
            )
        ).all()
        payments = (
            await session.execute(
                select(Payment, Student)
                .join(Student)
                .where(Payment.status == "reported")
                .order_by(Payment.reported_at)
            )
        ).all()
        coaches = (
            await session.execute(
                select(Coach).where(Coach.is_active == True).order_by(Coach.first_name)
            )
        ).scalars().all()
        locations = (
            await session.execute(
                select(Location, Coach)
                .join(Coach)
                .where(Location.is_active == True)
                .order_by(Location.name)
            )
        ).all()
        groups = (
            await session.execute(
                select(TrainingGroup).where(TrainingGroup.is_active == True).order_by(TrainingGroup.name)
            )
        ).scalars().all()
        return {
            "registrations": [{
                "id": item.id,
                "child_name": item.child_name,
                "birthday": item.child_birthday.isoformat(),
                "child_phone": item.child_phone,
                "parent_name": parent.full_name,
                "parent_phone": parent.phone,
                "proposed_schedule": json.loads(item.proposed_schedule),
            } for item, parent in registrations],
            "schedules": [{
                "id": item.id,
                "student_id": student.id,
                "student_name": student.name,
                "proposed_schedule": json.loads(item.proposed_schedule),
            } for item, student in schedules],
            "makeups": [{
                "id": item.id,
                "student_id": student.id,
                "student_name": student.name,
                "coach_id": student.coach_id,
                "requested_date": item.requested_date.isoformat() if item.requested_date else None,
                "expires_at": item.expires_at.isoformat(),
            } for item, student in makeups],
            "payments": [{
                "id": payment.id,
                "student_id": student.id,
                "student_name": student.name,
                "method": payment.payment_method,
                "receipt_attached": bool(payment.receipt_file_id),
                "amount": payment.amount,
            } for payment, student in payments],
            "resources": {
                "coaches": [{
                    "id": item.id,
                    "name": item.first_name,
                    "is_admin": bool(item.is_admin or item.telegram_id in ADMIN_IDS),
                    "is_manager": bool(item.is_manager or item.telegram_id in ADMIN_IDS),
                    "is_configured_owner": item.telegram_id in ADMIN_IDS,
                } for item in coaches],
                "locations": [{
                    "id": item.id,
                    "name": item.name,
                    "coach_id": owner.id,
                } for item, owner in locations],
                "groups": [{
                    "id": item.id,
                    "name": item.name,
                    "coach_id": item.coach_id,
                    "location_id": item.location_id,
                } for item in groups],
            },
        }


@router.post("/admin/registrations/{request_id}/review")
async def admin_review_registration(request_id: int, request: Request):
    body = await request.json()
    coach, user = await _admin_actor(body)
    decision = body.get("decision")
    if not coach:
        return JSONResponse({"error": "forbidden"}, 403)
    if decision not in {"approve", "reject"}:
        return JSONResponse({"error": "invalid_decision"}, 400)

    async with async_session() as session:
        result = await session.execute(
            select(RegistrationRequest, ParentAccount)
            .join(ParentAccount)
            .where(RegistrationRequest.id == request_id)
        )
        row = result.one_or_none()
        if not row:
            return JSONResponse({"error": "request_not_found"}, 404)
        item, parent = row
        if item.status != "pending":
            return JSONResponse({"error": "already_reviewed"}, 409)
        if decision == "reject":
            reason = str(body.get("reason") or "").strip()
            if not reason:
                return JSONResponse({"error": "reason_required"}, 400)
            item.status = "rejected"
            item.rejection_reason = reason
            item.reviewed_by_coach_id = coach.id
            item.reviewed_at = datetime.utcnow()
            await _queue_parent_notification(
                session,
                Student(coach_id=coach.id, id=None, name=item.child_name),
                parent,
                "registration_rejected",
                f"❌ Заявка на регистрацию отклонена\nПричина: {reason}",
            )
        else:
            assigned_coach_id = int(body.get("coach_id") or coach.id)
            assigned_coach = await session.get(Coach, assigned_coach_id)
            if not assigned_coach or not assigned_coach.is_active:
                return JSONResponse({"error": "coach_not_found"}, 404)
            schedules = _schedule_payload(body.get("approved_schedule")) or json.loads(item.proposed_schedule)
            selected_location_id = int(body["location_id"]) if body.get("location_id") else None
            selected_group_id = int(body["group_id"]) if body.get("group_id") else None
            if selected_location_id:
                location = await session.get(Location, selected_location_id)
                if not location or location.coach_id != assigned_coach.id:
                    return JSONResponse({"error": "location_not_found"}, 404)
            if selected_group_id:
                group = await session.get(TrainingGroup, selected_group_id)
                if not group or group.coach_id != assigned_coach.id:
                    return JSONResponse({"error": "group_not_found"}, 404)
            for schedule in schedules:
                if selected_location_id:
                    schedule["location_id"] = selected_location_id
                if selected_group_id:
                    schedule["group_id"] = selected_group_id
            try:
                training_start_date = date.fromisoformat(
                    body.get("training_start_date") or date.today().isoformat()
                )
            except ValueError:
                return JSONResponse({"error": "invalid_start_date"}, 400)
            student = Student(
                coach_id=assigned_coach.id,
                parent_id=parent.id,
                name=item.child_name,
                phone=item.child_phone,
                parent_phone=parent.phone,
                birthday=item.child_birthday,
                lessons_count=8,
                lessons_remaining=0,
                lesson_price=140,
                training_start_date=training_start_date,
                training_reminders_enabled=True,
                is_active=True,
            )
            session.add(student)
            await session.flush()
            await _replace_student_schedules(session, student, schedules)
            item.student_id = student.id
            item.status = "approved"
            item.reviewed_by_coach_id = coach.id
            item.reviewed_at = datetime.utcnow()
            await ensure_monthly_invoice(session, student, date.today())
            await _queue_parent_notification(
                session,
                student,
                parent,
                "registration_approved",
                f"✅ {student.name} зарегистрирован(а) в Break Wave.\nРасписание и оплата доступны в Mini App.",
            )
        await add_audit_log(
            session,
            user["id"],
            f"registration_{decision}d",
            "registration_request",
            item.id,
        )
        await session.commit()
        return {"success": True, "status": item.status, "student_id": item.student_id}


@router.post("/admin/schedule-requests/{request_id}/review")
async def admin_review_schedule(request_id: int, request: Request):
    body = await request.json()
    coach, user = await _admin_actor(body)
    decision = body.get("decision")
    if not coach:
        return JSONResponse({"error": "forbidden"}, 403)
    async with async_session() as session:
        result = await session.execute(
            select(ScheduleRequest, Student, ParentAccount)
            .join(Student, ScheduleRequest.student_id == Student.id)
            .join(ParentAccount, ScheduleRequest.parent_id == ParentAccount.id)
            .where(ScheduleRequest.id == request_id)
        )
        row = result.one_or_none()
        if not row:
            return JSONResponse({"error": "request_not_found"}, 404)
        item, student, parent = row
        if decision == "reject":
            reason = str(body.get("reason") or "").strip()
            if not reason:
                return JSONResponse({"error": "reason_required"}, 400)
            item.status = "rejected"
            item.rejection_reason = reason
            message = f"❌ Расписание для {student.name} не изменено.\nПричина: {reason}"
        elif decision == "approve":
            schedules = _schedule_payload(body.get("approved_schedule")) or json.loads(item.proposed_schedule)
            current_schedules = (
                await session.execute(
                    select(StudentSchedule).where(
                        StudentSchedule.student_id == student.id
                    ).order_by(desc(StudentSchedule.is_primary), StudentSchedule.id)
                )
            ).scalars().all()
            fallback_location_id = current_schedules[0].location_id if current_schedules else None
            fallback_group_id = current_schedules[0].group_id if current_schedules else None
            for schedule in schedules:
                if not schedule.get("location_id"):
                    schedule["location_id"] = fallback_location_id
                if not schedule.get("group_id"):
                    schedule["group_id"] = fallback_group_id
            await _replace_student_schedules(session, student, schedules)
            item.status = "approved"
            item.rejection_reason = None
            message = f"✅ Новое расписание для {student.name} подтверждено."
        else:
            return JSONResponse({"error": "invalid_decision"}, 400)
        item.reviewed_by_coach_id = coach.id
        item.reviewed_at = datetime.utcnow()
        await _queue_parent_notification(session, student, parent, "schedule_review", message)
        await add_audit_log(
            session, user["id"], f"schedule_{decision}d", "schedule_request", item.id
        )
        await session.commit()
        return {"success": True, "status": item.status}


@router.post("/admin/makeups/{makeup_id}/review")
async def admin_review_makeup(makeup_id: int, request: Request):
    body = await request.json()
    coach, user = await _admin_actor(body)
    decision = body.get("decision")
    if not coach:
        return JSONResponse({"error": "forbidden"}, 403)
    async with async_session() as session:
        result = await session.execute(
            select(MakeupCredit, Student, ParentAccount)
            .join(Student, MakeupCredit.student_id == Student.id)
            .join(ParentAccount, Student.parent_id == ParentAccount.id)
            .where(MakeupCredit.id == makeup_id)
        )
        row = result.one_or_none()
        if not row:
            return JSONResponse({"error": "makeup_not_found"}, 404)
        credit, student, parent = row
        if decision == "reject":
            reason = str(body.get("reason") or "").strip()
            if not reason:
                return JSONResponse({"error": "reason_required"}, 400)
            credit.status = "available"
            credit.rejection_reason = reason
            message = f"❌ Отработка для {student.name} не подтверждена.\nПричина: {reason}"
        elif decision == "approve":
            try:
                scheduled_date = date.fromisoformat(body.get("scheduled_date") or credit.requested_date.isoformat())
            except (AttributeError, ValueError):
                return JSONResponse({"error": "date_required"}, 400)
            if scheduled_date > credit.expires_at:
                return JSONResponse({"error": "after_expiry"}, 400)
            location_id = int(body["location_id"]) if body.get("location_id") else None
            group_id = int(body["group_id"]) if body.get("group_id") else None
            if location_id:
                location = await session.get(Location, location_id)
                if not location or location.coach_id != student.coach_id:
                    return JSONResponse({"error": "location_not_found"}, 404)
            if group_id:
                group = await session.get(TrainingGroup, group_id)
                if not group or group.coach_id != student.coach_id:
                    return JSONResponse({"error": "group_not_found"}, 404)
            credit.status = "scheduled"
            credit.scheduled_date = scheduled_date
            credit.scheduled_time = str(body.get("scheduled_time") or "18:00")
            credit.location_id = location_id
            credit.group_id = group_id
            credit.approved_by_coach_id = coach.id
            credit.rejection_reason = None
            message = (
                f"✅ Отработка для {student.name} подтверждена\n"
                f"{scheduled_date.strftime('%d.%m.%Y')} в {credit.scheduled_time}"
            )
        else:
            return JSONResponse({"error": "invalid_decision"}, 400)
        await _queue_parent_notification(session, student, parent, "makeup_review", message)
        await add_audit_log(
            session, user["id"], f"makeup_{decision}d", "makeup_credit", credit.id
        )
        await session.commit()
        return {"success": True, "status": credit.status}


@router.post("/admin/payments/{payment_id}/review")
async def admin_review_payment(payment_id: int, request: Request):
    body = await request.json()
    coach, user = await _admin_actor(body)
    decision = body.get("decision")
    if not coach:
        return JSONResponse({"error": "forbidden"}, 403)
    async with async_session() as session:
        result = await session.execute(
            select(Payment, Student, ParentAccount)
            .join(Student, Payment.student_id == Student.id)
            .join(ParentAccount, Student.parent_id == ParentAccount.id)
            .where(Payment.id == payment_id)
        )
        row = result.one_or_none()
        if not row:
            return JSONResponse({"error": "payment_not_found"}, 404)
        payment, student, parent = row
        if decision == "reject":
            reason = str(body.get("reason") or "").strip()
            if not reason:
                return JSONResponse({"error": "reason_required"}, 400)
            payment.status = "rejected"
            payment.rejection_reason = reason
            message = f"❌ Оплата для {student.name} отклонена.\nПричина: {reason}"
        elif decision == "approve":
            older = (
                await session.execute(
                    select(Payment).where(
                        Payment.student_id == student.id,
                        Payment.period_start < payment.period_start,
                        Payment.status != "paid",
                        Payment.tariff_code.is_not(None),
                    ).order_by(Payment.period_start)
                )
            ).scalars().first()
            if older:
                return JSONResponse({"error": "oldest_debt_first", "payment_id": older.id}, 409)
            late_fee, total = (
                (0, payment.base_amount or payment.amount)
                if payment.tariff_code == "single"
                else payment_total(payment.base_amount or payment.amount, payment.due_date, date.today())
            )
            payment.late_fee_amount = late_fee
            payment.amount = total
            payment.status = "paid"
            payment.paid_at = datetime.utcnow()
            payment.confirmed_by_coach_id = coach.id
            payment.rejection_reason = None
            if payment.payment_method == "cash":
                receiver_id = int(body.get("received_by_coach_id") or coach.id)
                receiver = await session.get(Coach, receiver_id)
                if not receiver or not receiver.is_active:
                    return JSONResponse({"error": "receiver_not_found"}, 404)
                payment.cash_received_by_coach_id = receiver_id

            consumed = (
                await session.execute(
                    select(func.count(Attendance.id)).where(
                        Attendance.student_id == student.id,
                        Attendance.attendance_date >= payment.period_start,
                        Attendance.attendance_date <= payment.period_end,
                        Attendance.status.in_(ATTENDANCE_STATUSES),
                        Attendance.source.notin_({"makeup", "coach_cancelled"}),
                    )
                )
            ).scalar_one()
            student.lessons_count = payment.lessons_count
            student.lesson_price = payment.base_amount or payment.amount
            student.lessons_remaining = max((payment.lessons_count or 0) - consumed, 0)
            student.subscription_start = payment.period_start
            student.subscription_end = payment.period_end
            student.is_unlimited = False
            message = (
                f"✅ Оплата для {student.name} подтверждена\n"
                f"Сумма: {payment.amount} Br · Осталось занятий: {student.lessons_remaining}"
            )
        else:
            return JSONResponse({"error": "invalid_decision"}, 400)
        await _queue_parent_notification(session, student, parent, "payment_review", message)
        await add_audit_log(
            session, user["id"], f"payment_{decision}d", "payment", payment.id,
            {"amount": payment.amount},
        )
        await session.commit()
        return {"success": True, "status": payment.status, "amount": payment.amount}


@router.post("/training-groups")
async def training_groups(request: Request):
    body = await request.json()
    coach, _ = await _admin_actor(body)
    if not coach:
        return JSONResponse({"error": "forbidden"}, 403)
    async with async_session() as session:
        groups = (
            await session.execute(
                select(TrainingGroup, Coach, Location)
                .join(Coach, TrainingGroup.coach_id == Coach.id)
                .join(Location, TrainingGroup.location_id == Location.id, isouter=True)
                .where(TrainingGroup.is_active == True)
                .order_by(TrainingGroup.name)
            )
        ).all()
        return [{
            "id": group.id,
            "name": group.name,
            "coach_id": owner.id,
            "coach_name": owner.first_name,
            "location_id": location.id if location else None,
            "location_name": location.name if location else None,
        } for group, owner, location in groups]


@router.post("/training-groups/create")
async def create_training_group(request: Request):
    body = await request.json()
    coach, user = await _admin_actor(body)
    if not coach:
        return JSONResponse({"error": "forbidden"}, 403)
    name = str(body.get("name") or "").strip()
    if not name:
        return JSONResponse({"error": "name_required"}, 400)
    owner_id = int(body.get("coach_id") or coach.id)
    async with async_session() as session:
        group = TrainingGroup(
            name=name,
            coach_id=owner_id,
            location_id=int(body["location_id"]) if body.get("location_id") else None,
            is_active=True,
        )
        session.add(group)
        await session.flush()
        await add_audit_log(session, user["id"], "group_created", "training_group", group.id)
        await session.commit()
        return {"success": True, "id": group.id}


@router.post("/admin/locations/create")
async def admin_create_location(request: Request):
    body = await request.json()
    coach, user = await _admin_actor(body)
    if not coach:
        return JSONResponse({"error": "forbidden"}, 403)
    name = str(body.get("name") or "").strip()
    if not name:
        return JSONResponse({"error": "name_required"}, 400)
    owner_id = int(body.get("coach_id") or coach.id)
    async with async_session() as session:
        owner = await session.get(Coach, owner_id)
        if not owner or not owner.is_active:
            return JSONResponse({"error": "coach_not_found"}, 404)
        location = Location(
            coach_id=owner_id,
            name=name,
            address=str(body.get("address") or "").strip() or None,
            is_active=True,
        )
        session.add(location)
        await session.flush()
        await add_audit_log(session, user["id"], "location_created", "location", location.id)
        await session.commit()
        return {"success": True, "id": location.id}


@router.post("/admin/coaches/{coach_id}/roles")
async def admin_update_coach_roles(coach_id: int, request: Request):
    body = await request.json()
    coach, user = await _admin_actor(body)
    if not coach:
        return JSONResponse({"error": "forbidden"}, 403)
    async with async_session() as session:
        target = await session.get(Coach, coach_id)
        if not target or not target.is_active:
            return JSONResponse({"error": "coach_not_found"}, 404)
        if target.telegram_id in ADMIN_IDS:
            target.is_admin = True
            target.is_manager = True
        else:
            target.is_admin = bool(body.get("is_admin"))
            target.is_manager = bool(body.get("is_manager"))
        await add_audit_log(
            session,
            user["id"],
            "coach_roles_updated",
            "coach",
            target.id,
            {"is_admin": target.is_admin, "is_manager": target.is_manager},
        )
        await session.commit()
        return {
            "success": True,
            "is_admin": target.is_admin,
            "is_manager": target.is_manager,
        }
