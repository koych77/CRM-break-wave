import asyncio
import hashlib
import hmac
import json
import os
import tempfile
import time
import urllib.parse
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest


_test_directory = tempfile.TemporaryDirectory()
_database_path = Path(_test_directory.name) / "crm-test.db"
os.environ["BOT_TOKEN"] = "123456789:TEST_TOKEN_FOR_SIGNATURES_ONLY"
os.environ["ADMIN_IDS"] = ""
os.environ["ADMIN_SECRET"] = "test-only-registration-secret"
os.environ["TELEGRAM_AUTH_MAX_AGE_SECONDS"] = "3600"
os.environ["DATABASE_URL"] = (
    "sqlite+aiosqlite:///" + _database_path.as_posix()
)

from fastapi.testclient import TestClient
from sqlalchemy import select, text

import app.api as api_module
from app.api import app, get_effective_payment_status, verify_telegram_init_data
from app.database import async_session, engine, init_db, run_migrations
from app.family import calculate_late_fee, reconcile_attendance
from app.models import (
    Attendance,
    Coach,
    Location,
    MakeupCredit,
    ParentAccount,
    Payment,
    RegistrationInvite,
    RegistrationRequest,
    Student,
    StudentSchedule,
)


SEEDED = {}


def telegram_init_data(user_id: int, auth_date: int | None = None) -> str:
    params = {
        "auth_date": str(auth_date if auth_date is not None else int(time.time())),
        "query_id": f"test-query-{user_id}",
        "user": json.dumps(
            {"id": user_id, "first_name": "Test"},
            separators=(",", ":"),
        ),
    }
    data_check_string = "\n".join(f"{key}={value}" for key, value in sorted(params.items()))
    secret_key = hmac.new(
        b"WebAppData",
        os.environ["BOT_TOKEN"].encode(),
        hashlib.sha256,
    ).digest()
    params["hash"] = hmac.new(
        secret_key,
        data_check_string.encode(),
        hashlib.sha256,
    ).hexdigest()
    return urllib.parse.urlencode(params)


async def seed_database():
    async with async_session() as session:
        own_coach = Coach(telegram_id=1001, first_name="Own coach")
        other_coach = Coach(telegram_id=1002, first_name="Other coach")
        session.add_all([own_coach, other_coach])
        await session.flush()

        own_location = Location(coach_id=own_coach.id, name="Own hall")
        other_location = Location(coach_id=other_coach.id, name="Other hall")
        session.add_all([own_location, other_location])
        await session.flush()

        own_student = Student(
            coach_id=own_coach.id,
            name="Own student",
            location_id=own_location.id,
        )
        other_student = Student(
            coach_id=other_coach.id,
            name="Other student",
            location_id=other_location.id,
        )
        session.add_all([own_student, other_student])
        await session.flush()

        session.add_all([
            StudentSchedule(
                student_id=own_student.id,
                location_id=own_location.id,
                days="1,3",
            ),
            StudentSchedule(
                student_id=other_student.id,
                location_id=other_location.id,
                days="2,4",
            ),
            Payment(
                coach_id=own_coach.id,
                student_id=own_student.id,
                amount=100,
                status="paid",
                paid_at=datetime.utcnow(),
                period_start=date.today(),
            ),
            Payment(
                coach_id=other_coach.id,
                student_id=other_student.id,
                amount=900,
                status="paid",
                paid_at=datetime.utcnow(),
                period_start=date.today(),
            ),
        ])
        await session.commit()

        SEEDED.update({
            "own_coach_id": own_coach.id,
            "other_coach_id": other_coach.id,
            "own_location_id": own_location.id,
            "own_student_id": own_student.id,
            "other_student_id": other_student.id,
        })


@pytest.fixture(scope="module", autouse=True)
def prepared_database():
    asyncio.run(init_db())
    asyncio.run(seed_database())
    yield
    asyncio.run(engine.dispose())
    _test_directory.cleanup()


def test_telegram_init_data_accepts_fresh_signature():
    verified = verify_telegram_init_data(telegram_init_data(1001))
    assert verified is not None
    assert verified["id"] == 1001


def test_legacy_webapp_route_redirects_to_current_mini_app():
    with TestClient(app) as client:
        response = client.get("/webapp/", follow_redirects=False)

    assert response.status_code == 307
    assert response.headers["location"] == "/"
    assert response.headers["cache-control"] == "no-cache, no-store, must-revalidate"


def test_healthcheck_verifies_database():
    with TestClient(app) as client:
        response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "database": "ok"}


def test_admin_is_provisioned_as_coach_on_first_open():
    admin_id = 909001
    api_module.ADMIN_IDS.append(admin_id)

    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/auth",
                json={"initData": telegram_init_data(admin_id)},
            )

        assert response.status_code == 200
        assert response.json()["is_admin"] is True
        assert response.json()["telegram_id"] == admin_id
    finally:
        api_module.ADMIN_IDS.remove(admin_id)

        async def remove_admin_coach():
            async with async_session() as session:
                coach_result = await session.execute(
                    text("SELECT id FROM coaches WHERE telegram_id = :telegram_id"),
                    {"telegram_id": admin_id},
                )
                coach_id = coach_result.scalar_one_or_none()
                if coach_id:
                    coach = await session.get(Coach, coach_id)
                    await session.delete(coach)
                    await session.commit()

        asyncio.run(remove_admin_coach())


def test_telegram_init_data_rejects_stale_signature():
    stale = telegram_init_data(1001, int(time.time()) - 3601)
    assert verify_telegram_init_data(stale) is None


def test_regular_coach_cannot_list_another_coachs_students():
    with TestClient(app) as client:
        response = client.post(
            "/api/students",
            json={
                "initData": telegram_init_data(1001),
                "coach_id": SEEDED["other_coach_id"],
            },
        )

    assert response.status_code == 200
    assert [student["id"] for student in response.json()] == [SEEDED["own_student_id"]]


def test_regular_coach_cannot_read_another_students_schedule():
    with TestClient(app) as client:
        response = client.post(
            f"/api/students/{SEEDED['other_student_id']}/schedules",
            json={"initData": telegram_init_data(1001)},
        )

    assert response.status_code == 404


def test_invalid_location_identifier_is_rejected_without_server_error():
    with TestClient(app) as client:
        response = client.post(
            f"/api/students/{SEEDED['own_student_id']}/update",
            json={
                "initData": telegram_init_data(1001),
                "student": {"location_id": "not-an-id"},
            },
        )

    assert response.status_code == 404
    assert response.json() == {"error": "location_not_found"}


def test_finance_summary_is_scoped_to_current_coach():
    with TestClient(app) as client:
        response = client.post(
            "/api/finance/summary",
            json={"initData": telegram_init_data(1001), "period": "all"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["total_revenue"] == 100
    assert [item["coach_id"] for item in payload["by_coach"]] == [SEEDED["own_coach_id"]]
    assert [item["location_name"] for item in payload["by_location"]] == ["Own hall"]


def test_search_handles_cyrillic_casefolding():
    async def rename_student(name):
        async with async_session() as session:
            student = await session.get(Student, SEEDED["own_student_id"])
            student.name = name
            await session.commit()

    asyncio.run(rename_student("Мария Александрова"))
    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/search",
                json={"initData": telegram_init_data(1001), "query": "Мария"},
            )

        assert response.status_code == 200
        assert [item["id"] for item in response.json()["results"]] == [
            SEEDED["own_student_id"]
        ]
    finally:
        asyncio.run(rename_student("Own student"))


def test_unlimited_student_is_not_reported_as_out_of_lessons():
    async def set_subscription(is_unlimited, remaining):
        async with async_session() as session:
            student = await session.get(Student, SEEDED["own_student_id"])
            student.is_unlimited = is_unlimited
            student.lessons_remaining = remaining
            await session.commit()

    asyncio.run(set_subscription(True, 0))
    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/finance/debtors",
                json={"initData": telegram_init_data(1001)},
            )

        assert response.status_code == 200
        debtors = response.json()["debtors"]
        reported_ids = {
            item["id"]
            for category in ("no_lessons", "low_lessons")
            for item in debtors[category]
        }
        assert SEEDED["own_student_id"] not in reported_ids
    finally:
        asyncio.run(set_subscription(False, 8))


def test_payment_overdue_status_is_derived_from_period_end():
    payment = Payment(status="pending", period_end=date.today() - timedelta(days=1))
    assert get_effective_payment_status(payment) == "overdue"

    payment.status = "paid"
    assert get_effective_payment_status(payment) == "paid"


def test_finance_debtors_count_each_student_once():
    async def set_multiple_attention_reasons():
        async with async_session() as session:
            student = await session.get(Student, SEEDED["own_student_id"])
            student.subscription_end = date.today() - timedelta(days=5)
            student.lessons_remaining = 0
            await session.commit()

    asyncio.run(set_multiple_attention_reasons())
    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/finance/debtors",
                json={"initData": telegram_init_data(1001)},
            )

        assert response.status_code == 200
        payload = response.json()
        assert payload["counts"]["total"] == 1
        assert payload["items"][0]["id"] == SEEDED["own_student_id"]
        assert set(payload["items"][0]["reasons"]) == {"expired", "no_lessons"}
    finally:
        async def restore_subscription():
            async with async_session() as session:
                student = await session.get(Student, SEEDED["own_student_id"])
                student.subscription_end = None
                student.lessons_remaining = 8
                await session.commit()

        asyncio.run(restore_subscription())


def test_lesson_create_static_route_is_not_shadowed_by_detail_route():
    with TestClient(app) as client:
        response = client.post(
            "/api/lessons/create",
            json={
                "initData": telegram_init_data(1001),
                "lesson": {
                    "student_id": SEEDED["own_student_id"],
                    "date": date.today().isoformat(),
                    "time": "12:34",
                    "location": "Own hall",
                    "status": "absent",
                },
            },
        )

    assert response.status_code == 200
    assert response.json()["success"] is True


def test_sqlite_schema_allows_schedule_without_location_and_enforces_foreign_keys():
    async def inspect_and_insert():
        async with engine.begin() as connection:
            foreign_keys = await connection.execute(text("PRAGMA foreign_keys"))
            table_info = await connection.execute(text("PRAGMA table_info(student_schedules)"))
            location_column = next(row for row in table_info.fetchall() if row[1] == "location_id")
            await connection.execute(
                text("""
                    INSERT INTO student_schedules
                        (student_id, location_id, days, times, duration, is_primary)
                    VALUES (:student_id, NULL, '1', '{"1": "18:00"}', 90, 0)
                """),
                {"student_id": SEEDED["own_student_id"]},
            )
            return foreign_keys.scalar_one(), location_column

    foreign_keys, location_column = asyncio.run(inspect_and_insert())
    assert foreign_keys == 1
    assert location_column[3] == 0


def test_legacy_schedule_schema_is_rebuilt_without_data_loss():
    async def recreate_legacy_schema_and_migrate():
        async with engine.begin() as connection:
            await connection.execute(text("""
                CREATE TABLE student_schedules_legacy (
                    id INTEGER PRIMARY KEY,
                    student_id INTEGER NOT NULL REFERENCES students(id),
                    location_id INTEGER NOT NULL REFERENCES locations(id),
                    days VARCHAR(100) DEFAULT '1,3',
                    times VARCHAR(500) DEFAULT '{"1": "18:00", "3": "18:00"}',
                    duration INTEGER DEFAULT 90,
                    is_primary BOOLEAN DEFAULT 1,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """))
            await connection.execute(text("""
                INSERT INTO student_schedules_legacy
                    (id, student_id, location_id, days, times, duration, is_primary, created_at)
                SELECT id, student_id, location_id, days, times, duration, is_primary, created_at
                FROM student_schedules
                WHERE location_id IS NOT NULL
            """))
            expected_rows = (
                await connection.execute(text("SELECT COUNT(*) FROM student_schedules_legacy"))
            ).scalar_one()
            await connection.execute(text("DROP TABLE student_schedules"))
            await connection.execute(text(
                "ALTER TABLE student_schedules_legacy RENAME TO student_schedules"
            ))

        await run_migrations()

        async with engine.begin() as connection:
            table_info = await connection.execute(text("PRAGMA table_info(student_schedules)"))
            location_column = next(row for row in table_info.fetchall() if row[1] == "location_id")
            actual_rows = (
                await connection.execute(text("SELECT COUNT(*) FROM student_schedules"))
            ).scalar_one()
            foreign_key_violations = (
                await connection.execute(text("PRAGMA foreign_key_check"))
            ).fetchall()
            return expected_rows, actual_rows, location_column, foreign_key_violations

    expected_rows, actual_rows, location_column, violations = asyncio.run(
        recreate_legacy_schema_and_migrate()
    )
    assert expected_rows > 0
    assert actual_rows == expected_rows
    assert location_column[3] == 0
    assert violations == []


def test_late_fee_grows_by_ten_rubles_every_seven_days_without_cap():
    due = date(2026, 7, 10)
    assert calculate_late_fee(due, date(2026, 7, 10)) == 0
    assert calculate_late_fee(due, date(2026, 7, 11)) == 10
    assert calculate_late_fee(due, date(2026, 7, 17)) == 10
    assert calculate_late_fee(due, date(2026, 7, 18)) == 20
    assert calculate_late_fee(due, date(2026, 8, 15)) == 60


def test_parent_registration_approval_and_family_invoice_use_same_mini_app():
    parent_telegram_id = 3001
    token = "test-parent-invite"

    async def create_invite():
        async with async_session() as session:
            invite = RegistrationInvite(
                token=token,
                preliminary_child_name="Новый ребёнок",
                created_by_coach_id=SEEDED["own_coach_id"],
                expires_at=datetime.utcnow() + timedelta(days=7),
                status="active",
            )
            session.add(invite)
            await session.commit()

    asyncio.run(create_invite())
    api_module.ADMIN_IDS.append(1001)
    try:
        with TestClient(app) as client:
            registration = client.post(
                "/api/parent/register",
                json={
                    "initData": telegram_init_data(parent_telegram_id),
                    "invite_token": token,
                    "parent": {
                        "full_name": "Мария Иванова",
                        "phone": "+375291111111",
                    },
                    "child": {
                        "name": "Иван Иванов",
                        "birthday": "2015-05-12",
                        "phone": "",
                    },
                    "proposed_schedule": [{
                        "days": "0",
                        "times": {"0": "18:00"},
                        "duration": 90,
                        "is_primary": True,
                    }],
                },
            )
            assert registration.status_code == 200
            request_id = registration.json()["request_id"]

            parent_auth = client.post(
                "/api/auth",
                json={"initData": telegram_init_data(parent_telegram_id)},
            )
            assert parent_auth.status_code == 200
            assert parent_auth.json()["role"] == "parent"

            review = client.post(
                f"/api/admin/registrations/{request_id}/review",
                json={
                    "initData": telegram_init_data(1001),
                    "decision": "approve",
                    "coach_id": SEEDED["own_coach_id"],
                },
            )
            assert review.status_code == 200

            context = client.post(
                "/api/parent/context",
                json={"initData": telegram_init_data(parent_telegram_id)},
            )

        assert context.status_code == 200
        payload = context.json()
        assert payload["students"][0]["name"] == "Иван Иванов"
        assert payload["students"][0]["invoice"]["tariff_code"] == "8"
        assert payload["students"][0]["invoice"]["base_amount"] == 140
        assert payload["tariffs"]["16"]["price"] == 200
    finally:
        api_module.ADMIN_IDS.remove(1001)


def test_absence_deducts_lesson_and_creates_makeup_but_coach_cancellation_does_not_deduct():
    async def exercise_rules():
        async with async_session() as session:
            student = Student(
                coach_id=SEEDED["own_coach_id"],
                name="Правила посещения",
                lessons_count=8,
                lessons_remaining=8,
            )
            session.add(student)
            await session.flush()

            absence = Attendance(
                student_id=student.id,
                status="absent",
                source="scheduled",
                attendance_date=date(2026, 7, 6),
                attendance_time="18:00",
            )
            session.add(absence)
            await reconcile_attendance(session, student, absence)
            remaining_after_absence = student.lessons_remaining

            cancellation = Attendance(
                student_id=student.id,
                status="excused",
                source="coach_cancelled",
                attendance_date=date(2026, 7, 8),
                attendance_time="18:00",
            )
            session.add(cancellation)
            await reconcile_attendance(session, student, cancellation)
            remaining_after_cancellation = student.lessons_remaining
            credits = (
                await session.execute(
                    select(MakeupCredit).where(MakeupCredit.student_id == student.id)
                )
            ).scalars().all()
            return remaining_after_absence, remaining_after_cancellation, credits

    after_absence, after_cancellation, credits = asyncio.run(exercise_rules())
    assert after_absence == 7
    assert after_cancellation == 7
    assert {credit.source_type for credit in credits} == {"absence", "coach_cancelled"}


def test_admin_can_approve_parent_makeup_and_reported_payment():
    async def prepare_requests():
        async with async_session() as session:
            parent = (
                await session.execute(
                    select(ParentAccount).where(ParentAccount.telegram_id == 3001)
                )
            ).scalar_one()
            student = (
                await session.execute(
                    select(Student).where(Student.parent_id == parent.id)
                )
            ).scalar_one()
            credit = MakeupCredit(
                student_id=student.id,
                source_date=date.today() - timedelta(days=1),
                source_type="absence",
                expires_at=date.today() + timedelta(days=60),
                status="requested",
                requested_date=date.today() + timedelta(days=7),
            )
            session.add(credit)
            payment = (
                await session.execute(
                    select(Payment)
                    .where(Payment.student_id == student.id)
                    .order_by(Payment.id)
                )
            ).scalars().first()
            payment.status = "reported"
            payment.payment_method = "cash"
            payment.reported_at = datetime.utcnow()
            await session.commit()
            return credit.id, payment.id

    makeup_id, payment_id = asyncio.run(prepare_requests())
    api_module.ADMIN_IDS.append(1001)
    try:
        with TestClient(app) as client:
            makeup_review = client.post(
                f"/api/admin/makeups/{makeup_id}/review",
                json={
                    "initData": telegram_init_data(1001),
                    "decision": "approve",
                    "scheduled_date": (date.today() + timedelta(days=7)).isoformat(),
                    "scheduled_time": "18:00",
                    "location_id": SEEDED["own_location_id"],
                },
            )
            payment_review = client.post(
                f"/api/admin/payments/{payment_id}/review",
                json={
                    "initData": telegram_init_data(1001),
                    "decision": "approve",
                    "received_by_coach_id": SEEDED["own_coach_id"],
                },
            )
    finally:
        api_module.ADMIN_IDS.remove(1001)

    assert makeup_review.status_code == 200
    assert makeup_review.json()["status"] == "scheduled"
    assert payment_review.status_code == 200
    assert payment_review.json()["status"] == "paid"


def test_student_deletion_removes_personal_data_and_receipts_but_keeps_anonymized_revenue():
    async def prepare_deletion():
        async with async_session() as session:
            parent = (
                await session.execute(
                    select(ParentAccount).where(ParentAccount.telegram_id == 3001)
                )
            ).scalar_one()
            student = (
                await session.execute(
                    select(Student).where(Student.parent_id == parent.id)
                )
            ).scalar_one()
            payment = (
                await session.execute(
                    select(Payment).where(Payment.student_id == student.id)
                )
            ).scalars().first()
            payment.status = "paid"
            payment.receipt_file_id = "telegram-receipt-file"
            payment.paid_at = datetime.utcnow()
            session.add(Payment(
                coach_id=student.coach_id,
                student_id=student.id,
                amount=140,
                base_amount=140,
                lessons_count=8,
                tariff_code="8",
                status="pending",
                receipt_file_id="unconfirmed-receipt",
                period_start=date.today() + timedelta(days=31),
            ))
            absence = Attendance(
                student_id=student.id,
                status="absent",
                source="scheduled",
                attendance_date=date.today() - timedelta(days=3),
                attendance_time="18:00",
            )
            session.add(absence)
            await reconcile_attendance(session, student, absence)
            credit = (
                await session.execute(
                    select(MakeupCredit).where(
                        MakeupCredit.student_id == student.id,
                        MakeupCredit.source_attendance_id == absence.id,
                    )
                )
            ).scalar_one()
            session.add(Attendance(
                student_id=student.id,
                status="present",
                source="makeup",
                makeup_credit_id=credit.id,
                attendance_date=date.today() - timedelta(days=1),
                attendance_time="18:00",
            ))
            await session.commit()
            return student.id, parent.id

    student_id, parent_id = asyncio.run(prepare_deletion())
    with TestClient(app) as client:
        response = client.post(
            f"/api/students/{student_id}/destroy",
            json={
                "initData": telegram_init_data(1001),
                "confirm_destroy": True,
            },
        )
    assert response.status_code == 200

    async def inspect_deletion():
        async with async_session() as session:
            student = await session.get(Student, student_id)
            parent = await session.get(ParentAccount, parent_id)
            payments = (
                await session.execute(
                    select(Payment).where(Payment.student_id == student_id).order_by(Payment.id)
                )
            ).scalars().all()
            return student, parent, payments

    student, parent, payments = asyncio.run(inspect_deletion())
    assert student.name == f"Удалённый ученик #{student_id}"
    assert student.phone is None
    assert student.parent_id is None
    assert parent is None
    assert [payment.status for payment in payments] == ["paid", "written_off"]
    assert all(payment.receipt_file_id is None for payment in payments)
