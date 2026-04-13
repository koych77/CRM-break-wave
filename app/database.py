from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import text
from app.config import DATABASE_URL
import logging

logger = logging.getLogger(__name__)

logger.info(f"Database URL: {DATABASE_URL}")

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def init_db():
    """Initialize database tables."""
    logger.info("Initializing database...")
    
    # Ensure data directory exists (for Railway volume)
    import os
    data_dir = os.path.dirname(DATABASE_URL.replace('sqlite+aiosqlite:///', ''))
    if data_dir and not os.path.exists(data_dir):
        os.makedirs(data_dir, exist_ok=True)
        logger.info(f"Created data directory: {data_dir}")
    
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
                    location_id INTEGER NOT NULL REFERENCES locations(id),
                    days VARCHAR(100) DEFAULT '1,3',
                    times VARCHAR(500) DEFAULT '{"1": "18:00", "3": "18:00"}',
                    duration INTEGER DEFAULT 90,
                    is_primary BOOLEAN DEFAULT 1,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """))
        
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
