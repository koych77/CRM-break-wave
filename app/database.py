from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import event, text
from sqlalchemy.engine import make_url
from app.config import DATABASE_URL
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

database_url = make_url(DATABASE_URL)
is_sqlite = database_url.drivername.startswith("sqlite")
logger.info("Database backend: %s", database_url.drivername)

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


if is_sqlite:
    @event.listens_for(engine.sync_engine, "connect")
    def _enable_sqlite_foreign_keys(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


class Base(DeclarativeBase):
    pass


async def init_db():
    """Initialize database tables."""
    logger.info("Initializing database...")
    
    # Ensure the SQLite parent directory exists without treating non-SQLite URLs as paths.
    if is_sqlite and database_url.database and database_url.database != ":memory:":
        database_path = Path(database_url.database)
        if not database_path.is_absolute():
            database_path = Path.cwd() / database_path
        database_path.parent.mkdir(parents=True, exist_ok=True)
    
    async with engine.begin() as conn:
        # Coaches table
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS coaches (
                id INTEGER PRIMARY KEY,
                telegram_id BIGINT UNIQUE NOT NULL,
                first_name VARCHAR(200),
                username VARCHAR(200),
                phone VARCHAR(50),
                is_active BOOLEAN DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Locations table
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS locations (
                id INTEGER PRIMARY KEY,
                coach_id INTEGER NOT NULL REFERENCES coaches(id),
                name VARCHAR(200) NOT NULL,
                address VARCHAR(500),
                is_active BOOLEAN DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Students table - individual settings per student
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS students (
                id INTEGER PRIMARY KEY,
                coach_id INTEGER NOT NULL REFERENCES coaches(id),
                name VARCHAR(200) NOT NULL,
                nickname VARCHAR(100),
                phone VARCHAR(50),
                parent_phone VARCHAR(50),
                age INTEGER,
                birthday DATE,
                notes TEXT,
                location VARCHAR(200) DEFAULT 'Зал Break Wave',
                location_id INTEGER REFERENCES locations(id),
                lesson_days VARCHAR(100) DEFAULT '1,3',
                lesson_times VARCHAR(500) DEFAULT '{"1": "18:00", "3": "18:00"}',
                lesson_duration INTEGER DEFAULT 90,
                lesson_price INTEGER DEFAULT 150,
                lessons_count INTEGER DEFAULT 8,
                lessons_remaining INTEGER DEFAULT 8,
                subscription_start DATE,
                subscription_end DATE,
                is_active BOOLEAN DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Lessons table
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS lessons (
                id INTEGER PRIMARY KEY,
                coach_id INTEGER NOT NULL REFERENCES coaches(id),
                student_id INTEGER NOT NULL REFERENCES students(id),
                date DATE NOT NULL,
                time VARCHAR(10),
                location VARCHAR(200),
                location_id INTEGER REFERENCES locations(id),
                topic VARCHAR(200),
                notes TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Attendance table (supports both scheduled and extra lessons)
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS attendance (
                id INTEGER PRIMARY KEY,
                lesson_id INTEGER REFERENCES lessons(id),
                student_id INTEGER NOT NULL REFERENCES students(id),
                location_id INTEGER REFERENCES locations(id),
                status VARCHAR(20) DEFAULT 'present',
                is_extra BOOLEAN DEFAULT 0,
                attendance_date DATE NOT NULL,
                attendance_time VARCHAR(10),
                notes VARCHAR(500),
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Payments table
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY,
                coach_id INTEGER NOT NULL REFERENCES coaches(id),
                student_id INTEGER NOT NULL REFERENCES students(id),
                amount INTEGER NOT NULL,
                lessons_count INTEGER DEFAULT 8,
                status VARCHAR(20) DEFAULT 'pending',
                period_start DATE,
                period_end DATE,
                paid_at DATETIME,
                notes VARCHAR(500),
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Admin users table
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS admin_users (
                id INTEGER PRIMARY KEY,
                telegram_id BIGINT UNIQUE NOT NULL,
                first_name VARCHAR(200),
                username VARCHAR(200),
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Notifications log
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY,
                coach_id INTEGER NOT NULL REFERENCES coaches(id),
                student_id INTEGER REFERENCES students(id),
                type VARCHAR(50) NOT NULL,
                message TEXT NOT NULL,
                is_read BOOLEAN DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Daily notification logs (to prevent duplicate daily notifications)
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS daily_notification_logs (
                id INTEGER PRIMARY KEY,
                coach_id INTEGER NOT NULL REFERENCES coaches(id),
                notification_type VARCHAR(50) NOT NULL,
                sent_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                date DATE NOT NULL
            )
        """))

        # Keep the legacy schema compatible and let SQLAlchemy create only the
        # additive tables introduced for parents, requests, groups and makeups.
        from app import models as _models  # noqa: F401
        await conn.run_sync(Base.metadata.create_all)
    
    # Run migrations for existing databases
    await run_migrations()
    
    # Create indexes for performance
    await create_indexes()


async def run_migrations():
    """Run migrations for schema updates on existing databases."""
    import json
    
    async with engine.begin() as conn:
        # Migration: Add missing columns to existing tables
        
        # 1. Check and add location_id to students
        try:
            await conn.execute(text("SELECT location_id FROM students LIMIT 1"))
        except:
            logger.info("Migrating: Adding location_id to students")
            await conn.execute(text("ALTER TABLE students ADD COLUMN location_id INTEGER"))
        
        # 2. Check and add lesson_times to students
        try:
            await conn.execute(text("SELECT lesson_times FROM students LIMIT 1"))
        except:
            logger.info("Migrating: Adding lesson_times to students")
            await conn.execute(text('ALTER TABLE students ADD COLUMN lesson_times VARCHAR(500)'))
            # Populate with default values from legacy data if available
            try:
                result = await conn.execute(text("SELECT id, lesson_days, lesson_time FROM students"))
                rows = result.fetchall()
                for row in rows:
                    student_id, days, time = row
                    if days:
                        days_list = [d.strip() for d in days.split(',')]
                        times_dict = {d: (time or '18:00') for d in days_list}
                        times_json = json.dumps(times_dict)
                        await conn.execute(
                            text("UPDATE students SET lesson_times = :times WHERE id = :id"),
                            {"times": times_json, "id": student_id}
                        )
            except:
                # lesson_time column doesn't exist, use default values
                logger.info("Migrating: lesson_time column not found, using defaults")
                result = await conn.execute(text("SELECT id, lesson_days FROM students"))
                rows = result.fetchall()
                for row in rows:
                    student_id, days = row
                    if days:
                        days_list = [d.strip() for d in days.split(',')]
                        times_dict = {d: '18:00' for d in days_list}
                        times_json = json.dumps(times_dict)
                        await conn.execute(
                            text("UPDATE students SET lesson_times = :times WHERE id = :id"),
                            {"times": times_json, "id": student_id}
                        )
        
        # 3. Check and add lessons_remaining to students
        try:
            await conn.execute(text("SELECT lessons_remaining FROM students LIMIT 1"))
        except:
            logger.info("Migrating: Adding lessons_remaining to students")
            await conn.execute(text("ALTER TABLE students ADD COLUMN lessons_remaining INTEGER"))
            await conn.execute(text("UPDATE students SET lessons_remaining = lessons_count"))
        
        # 4. Check and add location_id to lessons
        try:
            await conn.execute(text("SELECT location_id FROM lessons LIMIT 1"))
        except:
            logger.info("Migrating: Adding location_id to lessons")
            await conn.execute(text("ALTER TABLE lessons ADD COLUMN location_id INTEGER"))
        
        # 5. Check and add location_id to attendance
        try:
            await conn.execute(text("SELECT location_id FROM attendance LIMIT 1"))
        except:
            logger.info("Migrating: Adding location_id to attendance")
            await conn.execute(text("ALTER TABLE attendance ADD COLUMN location_id INTEGER"))
        
        # 5c. Check and add attendance_date to attendance
        try:
            await conn.execute(text("SELECT attendance_date FROM attendance LIMIT 1"))
        except:
            logger.info("Migrating: Adding attendance_date to attendance")
            await conn.execute(text("ALTER TABLE attendance ADD COLUMN attendance_date DATE"))
            # Populate from lesson dates
            await conn.execute(text("""
                UPDATE attendance 
                SET attendance_date = (SELECT date FROM lessons WHERE lessons.id = attendance.lesson_id)
                WHERE attendance_date IS NULL
            """))
        
        # 5d. Check and add attendance_time to attendance
        try:
            await conn.execute(text("SELECT attendance_time FROM attendance LIMIT 1"))
        except:
            logger.info("Migrating: Adding attendance_time to attendance")
            await conn.execute(text("ALTER TABLE attendance ADD COLUMN attendance_time VARCHAR(10)"))

        # 5e. Check and add is_extra to attendance
        try:
            await conn.execute(text("SELECT is_extra FROM attendance LIMIT 1"))
        except:
            logger.info("Migrating: Adding is_extra to attendance")
            await conn.execute(text("ALTER TABLE attendance ADD COLUMN is_extra BOOLEAN DEFAULT 0"))
            await conn.execute(text("UPDATE attendance SET is_extra = 0 WHERE is_extra IS NULL"))

        # 6. Check and add lesson_duration to students
        try:
            await conn.execute(text("SELECT lesson_duration FROM students LIMIT 1"))
        except:
            logger.info("Migrating: Adding lesson_duration to students")
            await conn.execute(text("ALTER TABLE students ADD COLUMN lesson_duration INTEGER DEFAULT 90"))
        
        # 7. Check and add is_extra to lessons (if not exists)
        try:
            await conn.execute(text("SELECT topic FROM lessons LIMIT 1"))
        except:
            logger.info("Migrating: Adding topic to lessons")
            await conn.execute(text("ALTER TABLE lessons ADD COLUMN topic VARCHAR(200)"))
        
        # 6. Create locations table if not exists
        try:
            await conn.execute(text("SELECT id FROM locations LIMIT 1"))
        except:
            logger.info("Migrating: Creating locations table")
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS locations (
                    id INTEGER PRIMARY KEY,
                    coach_id INTEGER NOT NULL REFERENCES coaches(id),
                    name VARCHAR(200) NOT NULL,
                    address VARCHAR(500),
                    is_active BOOLEAN DEFAULT 1,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """))
        
        # 7. Check and add is_unlimited to students
        try:
            await conn.execute(text("SELECT is_unlimited FROM students LIMIT 1"))
        except:
            logger.info("Migrating: Adding is_unlimited to students")
            await conn.execute(text("ALTER TABLE students ADD COLUMN is_unlimited BOOLEAN DEFAULT 0"))
        
        # 8. Check and add birthday to students
        try:
            await conn.execute(text("SELECT birthday FROM students LIMIT 1"))
        except:
            logger.info("Migrating: Adding birthday to students")
            await conn.execute(text("ALTER TABLE students ADD COLUMN birthday DATE"))
        
        # 9. Create student_schedules table if not exists
        try:
            await conn.execute(text("SELECT id FROM student_schedules LIMIT 1"))
        except:
            logger.info("Migrating: Creating student_schedules table")
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS student_schedules (
                    id INTEGER PRIMARY KEY,
                    student_id INTEGER NOT NULL REFERENCES students(id),
                    location_id INTEGER REFERENCES locations(id),
                    days VARCHAR(100) DEFAULT '1,3',
                    times VARCHAR(500) DEFAULT '{"1": "18:00", "3": "18:00"}',
                    duration INTEGER DEFAULT 90,
                    is_primary BOOLEAN DEFAULT 1,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """))

        # Older databases created location_id as NOT NULL even though the API permits
        # schedules without a selected hall. Rebuild the child table without data loss.
        if is_sqlite:
            table_info = await conn.execute(text("PRAGMA table_info(student_schedules)"))
            location_column = next(
                (row for row in table_info.fetchall() if row[1] == "location_id"),
                None,
            )
            if location_column and location_column[3]:
                logger.info("Migrating: Allowing null location_id in student_schedules")
                await conn.execute(text("DROP TABLE IF EXISTS student_schedules_new"))
                await conn.execute(text("""
                    CREATE TABLE student_schedules_new (
                        id INTEGER PRIMARY KEY,
                        student_id INTEGER NOT NULL REFERENCES students(id),
                        location_id INTEGER REFERENCES locations(id),
                        days VARCHAR(100) DEFAULT '1,3',
                        times VARCHAR(500) DEFAULT '{"1": "18:00", "3": "18:00"}',
                        duration INTEGER DEFAULT 90,
                        is_primary BOOLEAN DEFAULT 1,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                await conn.execute(text("""
                    INSERT INTO student_schedules_new
                        (id, student_id, location_id, days, times, duration, is_primary, created_at)
                    SELECT id, student_id, location_id, days, times, duration, is_primary, created_at
                    FROM student_schedules
                """))
                await conn.execute(text("DROP TABLE student_schedules"))
                await conn.execute(text(
                    "ALTER TABLE student_schedules_new RENAME TO student_schedules"
                ))
        
        # 10. Check and add is_unlimited to payments
        try:
            await conn.execute(text("SELECT is_unlimited FROM payments LIMIT 1"))
        except:
            logger.info("Migrating: Adding is_unlimited to payments")
            try:
                await conn.execute(text("ALTER TABLE payments ADD COLUMN is_unlimited BOOLEAN DEFAULT 0"))
            except Exception:
                # Column may have been added by another concurrent process
                pass

        additive_columns = {
            "coaches": [
                ("is_admin", "BOOLEAN DEFAULT 0"),
                ("is_manager", "BOOLEAN DEFAULT 0"),
            ],
            "students": [
                ("parent_id", "INTEGER"),
                ("training_reminders_enabled", "BOOLEAN DEFAULT 1"),
                ("training_start_date", "DATE"),
                ("deleted_at", "DATETIME"),
            ],
            "student_schedules": [
                ("group_id", "INTEGER"),
            ],
            "attendance": [
                ("source", "VARCHAR(20) DEFAULT 'scheduled'"),
                ("makeup_credit_id", "INTEGER"),
                ("deducted", "BOOLEAN DEFAULT 0"),
            ],
            "payments": [
                ("tariff_code", "VARCHAR(20)"),
                ("base_amount", "INTEGER"),
                ("late_fee_amount", "INTEGER DEFAULT 0"),
                ("due_date", "DATE"),
                ("payment_method", "VARCHAR(20)"),
                ("receipt_file_id", "VARCHAR(500)"),
                ("reported_at", "DATETIME"),
                ("confirmed_by_coach_id", "INTEGER"),
                ("cash_received_by_coach_id", "INTEGER"),
                ("rejection_reason", "VARCHAR(500)"),
            ],
            "notifications": [
                ("recipient_telegram_id", "BIGINT"),
                ("sent_at", "DATETIME"),
                ("delivery_error", "VARCHAR(500)"),
            ],
        }

        for table_name, columns in additive_columns.items():
            for column_name, column_type in columns:
                try:
                    await conn.execute(text(
                        f"SELECT {column_name} FROM {table_name} LIMIT 1"
                    ))
                except Exception:
                    logger.info("Migrating: Adding %s.%s", table_name, column_name)
                    await conn.execute(text(
                        f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
                    ))

        await conn.execute(text(
            "UPDATE students SET training_reminders_enabled = 1 "
            "WHERE training_reminders_enabled IS NULL"
        ))
        await conn.execute(text(
            "UPDATE attendance SET source = 'scheduled' WHERE source IS NULL"
        ))
        await conn.execute(text(
            "UPDATE attendance SET deducted = 1 "
            "WHERE deducted IS NULL AND status = 'present'"
        ))
        await conn.execute(text(
            "UPDATE attendance SET deducted = 0 WHERE deducted IS NULL"
        ))
        await conn.execute(text(
            "UPDATE payments SET late_fee_amount = 0 WHERE late_fee_amount IS NULL"
        ))

        # Public family registration is authenticated by signed Telegram initData
        # and therefore does not require a one-time administrator invitation.
        if is_sqlite:
            table_info = await conn.execute(text("PRAGMA table_info(registration_requests)"))
            invite_column = next(
                (row for row in table_info.fetchall() if row[1] == "invite_id"),
                None,
            )
            if invite_column and invite_column[3]:
                logger.info("Migrating: Allowing public registration requests without invites")
                await conn.execute(text("DROP TABLE IF EXISTS registration_requests_new"))
                await conn.execute(text("""
                    CREATE TABLE registration_requests_new (
                        id INTEGER PRIMARY KEY,
                        invite_id INTEGER UNIQUE REFERENCES registration_invites(id),
                        parent_id INTEGER NOT NULL REFERENCES parent_accounts(id),
                        student_id INTEGER REFERENCES students(id),
                        child_name VARCHAR(200) NOT NULL,
                        child_birthday DATE NOT NULL,
                        child_phone VARCHAR(50),
                        proposed_schedule TEXT NOT NULL,
                        status VARCHAR(20) DEFAULT 'pending',
                        rejection_reason VARCHAR(500),
                        reviewed_by_coach_id INTEGER REFERENCES coaches(id),
                        reviewed_at DATETIME,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                await conn.execute(text("""
                    INSERT INTO registration_requests_new (
                        id, invite_id, parent_id, student_id, child_name,
                        child_birthday, child_phone, proposed_schedule, status,
                        rejection_reason, reviewed_by_coach_id, reviewed_at, created_at
                    )
                    SELECT
                        id, invite_id, parent_id, student_id, child_name,
                        child_birthday, child_phone, proposed_schedule, status,
                        rejection_reason, reviewed_by_coach_id, reviewed_at, created_at
                    FROM registration_requests
                """))
                await conn.execute(text("DROP TABLE registration_requests"))
                await conn.execute(text(
                    "ALTER TABLE registration_requests_new RENAME TO registration_requests"
                ))
        else:
            await conn.execute(text(
                "ALTER TABLE registration_requests ALTER COLUMN invite_id DROP NOT NULL"
            ))
        
        logger.info("Migrations completed")


async def create_indexes():
    """Create database indexes for better performance."""
    async with engine.begin() as conn:
        # Attendance indexes
        try:
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_attendance_date ON attendance(attendance_date)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_attendance_student ON attendance(student_id)"))
            logger.info("Created attendance indexes")
        except Exception as e:
            logger.warning(f"Could not create attendance indexes: {e}")
        
        # Lessons indexes
        try:
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_lessons_date ON lessons(date)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_lessons_student ON lessons(student_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_lessons_coach ON lessons(coach_id)"))
            logger.info("Created lessons indexes")
        except Exception as e:
            logger.warning(f"Could not create lessons indexes: {e}")
        
        # Payments indexes
        try:
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(status)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_payments_student ON payments(student_id)"))
            logger.info("Created payments indexes")
        except Exception as e:
            logger.warning(f"Could not create payments indexes: {e}")
        
        # Student schedules indexes
        try:
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_schedules_student ON student_schedules(student_id)"))
            logger.info("Created schedules indexes")
        except Exception as e:
            logger.warning(f"Could not create schedules indexes: {e}")

        # Parent, request and makeup workflow indexes
        try:
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_students_parent ON students(parent_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_registration_status ON registration_requests(status)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_schedule_requests_status ON schedule_requests(status)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_makeups_student_status ON makeup_credits(student_id, status)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_training_response_date ON training_responses(training_date)"))
            logger.info("Created parent workflow indexes")
        except Exception as e:
            logger.warning(f"Could not create parent workflow indexes: {e}")
        
        # Notification logs indexes
        try:
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_notif_logs_coach_date ON daily_notification_logs(coach_id, date)"))
            logger.info("Created notification logs indexes")
        except Exception as e:
            logger.warning(f"Could not create notification logs indexes: {e}")
        
        logger.info("Indexes creation completed")


async def get_session() -> AsyncSession:
    async with async_session() as session:
        yield session
