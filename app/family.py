"""Business rules shared by the Mini App API and the Telegram bot."""

from __future__ import annotations

from calendar import monthrange
from datetime import date
from math import ceil

from dateutil.relativedelta import relativedelta
from sqlalchemy import func, select

from app.models import MakeupCredit


TARIFFS = {
    "single": {"label": "Разовое занятие", "lessons": 1, "price": 40},
    "8": {"label": "8 занятий", "lessons": 8, "price": 140},
    "12": {"label": "12 занятий", "lessons": 12, "price": 170},
    "16": {"label": "16 занятий", "lessons": 16, "price": 200},
}

PAYMENT_DUE_DAY = 10
TARIFF_SELECTION_DEADLINE_DAY = 5
LATE_FEE_STEP_DAYS = 7
LATE_FEE_STEP_AMOUNT = 10
MAKEUP_VALID_MONTHS = 2
MONTHLY_MAKEUP_LIMIT = 8
TRAINING_REMINDER_HOUR = 18

ATTENDANCE_STATUSES = {"present", "absent", "sick", "excused"}
ABSENCE_STATUSES = {"absent", "sick", "excused"}


def month_bounds(value: date) -> tuple[date, date]:
    """Return the first and last day of a calendar month."""
    start = value.replace(day=1)
    end = value.replace(day=monthrange(value.year, value.month)[1])
    return start, end


def payment_due_date(value: date) -> date:
    """Payment is due on the tenth day of the selected calendar month."""
    return value.replace(day=PAYMENT_DUE_DAY)


def can_select_tariff(on_date: date | None = None) -> bool:
    """Return whether a parent may still change the tariff for the current month."""
    reference = on_date or date.today()
    return reference.day <= TARIFF_SELECTION_DEADLINE_DAY


def calculate_late_fee(due_date: date | None, on_date: date | None = None) -> int:
    """Add 10 Br on the 11th and another 10 Br every seven overdue days."""
    if not due_date:
        return 0
    reference = on_date or date.today()
    days_overdue = (reference - due_date).days
    if days_overdue <= 0:
        return 0
    return ceil(days_overdue / LATE_FEE_STEP_DAYS) * LATE_FEE_STEP_AMOUNT


def tariff_details(tariff_code: str | int | None) -> dict | None:
    """Resolve a safe public copy of a supported tariff."""
    tariff = TARIFFS.get(str(tariff_code or ""))
    return dict(tariff) if tariff else None


def payment_total(base_amount: int, due_date: date | None, on_date: date | None = None) -> tuple[int, int]:
    """Return late fee and total for an unpaid monthly invoice."""
    late_fee = calculate_late_fee(due_date, on_date)
    return late_fee, int(base_amount or 0) + late_fee


def makeup_expiry(source_date: date) -> date:
    """Makeup rights expire two calendar months after the missed training."""
    return source_date + relativedelta(months=MAKEUP_VALID_MONTHS)


def attendance_consumes_lesson(status: str | None, source: str | None = "scheduled") -> bool:
    """Every held slot consumes a lesson; trainer cancellations and makeups do not."""
    return (source or "scheduled") not in {"makeup", "coach_cancelled"} and status in ATTENDANCE_STATUSES


def absence_creates_makeup(status: str | None, source: str | None = "scheduled") -> bool:
    """Any scheduled child absence creates a makeup right."""
    return (source or "scheduled") == "scheduled" and status in ABSENCE_STATUSES


def is_payment_reminder_day(reference: date, due_date: date) -> bool:
    """Reminder cadence: 7/3 days before, due day, then every seven days from the 11th."""
    delta = (due_date - reference).days
    if delta in {7, 3, 0}:
        return True
    overdue_days = (reference - due_date).days
    return overdue_days > 0 and (overdue_days - 1) % LATE_FEE_STEP_DAYS == 0


async def reconcile_attendance(session, student, attendance) -> MakeupCredit | None:
    """Apply lesson balance and makeup rules exactly once for an attendance row."""
    consumes = attendance_consumes_lesson(attendance.status, attendance.source)
    already_deducted = bool(attendance.deducted)

    if not getattr(student, "is_unlimited", False):
        remaining = (
            student.lessons_remaining
            if student.lessons_remaining is not None
            else (student.lessons_count or 0)
        )
        if consumes and not already_deducted:
            student.lessons_remaining = max(remaining - 1, 0)
            attendance.deducted = True
        elif not consumes and already_deducted:
            student.lessons_remaining = remaining + 1
            attendance.deducted = False

    await session.flush()

    existing_result = await session.execute(
        select(MakeupCredit).where(MakeupCredit.source_attendance_id == attendance.id)
    )
    existing = existing_result.scalar_one_or_none()

    needs_makeup = absence_creates_makeup(attendance.status, attendance.source)
    source_type = "absence"
    if attendance.source == "coach_cancelled":
        needs_makeup = True
        source_type = "coach_cancelled"

    if not needs_makeup:
        if existing and existing.status in {"available", "requested"}:
            existing.status = "cancelled"
        if attendance.source == "makeup" and attendance.makeup_credit_id:
            credit = await session.get(MakeupCredit, attendance.makeup_credit_id)
            if credit:
                credit.status = "used" if attendance.status == "present" else "burned"
                credit.used_at = attendance.created_at
        return existing

    if existing:
        return existing

    source_date = attendance.attendance_date
    if source_type == "absence":
        month_start, month_end = month_bounds(source_date)
        count_result = await session.execute(
            select(func.count(MakeupCredit.id)).where(
                MakeupCredit.student_id == student.id,
                MakeupCredit.source_type == "absence",
                MakeupCredit.source_date >= month_start,
                MakeupCredit.source_date <= month_end,
            )
        )
        if count_result.scalar_one() >= MONTHLY_MAKEUP_LIMIT:
            return None

    credit = MakeupCredit(
        student_id=student.id,
        source_attendance_id=attendance.id,
        source_date=source_date,
        source_type=source_type,
        expires_at=makeup_expiry(source_date),
        status="available",
    )
    session.add(credit)
    return credit
