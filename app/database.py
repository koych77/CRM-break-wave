from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import text
from app.config import DATABASE_URL

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def init_db():
    """Initialize database tables."""
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
                -- Individual settings
                location VARCHAR(200) DEFAULT 'Зал Break Wave',
                lesson_days VARCHAR(100) DEFAULT '1,3',
                lesson_time VARCHAR(10) DEFAULT '18:00',
                lesson_price INTEGER DEFAULT 5000,
                lessons_count INTEGER DEFAULT 8,
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
                topic VARCHAR(200),
                notes TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Attendance table
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS attendance (
                id INTEGER PRIMARY KEY,
                lesson_id INTEGER NOT NULL REFERENCES lessons(id),
                student_id INTEGER NOT NULL REFERENCES students(id),
                status VARCHAR(20) DEFAULT 'present',
                notes VARCHAR(500),
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(lesson_id, student_id)
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


async def get_session() -> AsyncSession:
    async with async_session() as session:
        yield session
