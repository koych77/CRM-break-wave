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
from sqlalchemy import text

import app.api as api_module
from app.api import app, get_effective_payment_status, verify_telegram_init_data
from app.database import async_session, engine, init_db, run_migrations
from app.models import Coach, Location, Payment, Student, StudentSchedule


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
