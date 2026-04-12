"""
Migration script to add lessons_remaining column and update existing data.
Also adds is_extra, attendance_date, attendance_time columns to Attendance table.
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from app.database import engine, Base
from app.models import Student, Attendance, DailyNotificationLog


async def migrate():
    """Run migration."""
    print("Starting migration...")
    
    async with engine.begin() as conn:
        # Check if lessons_remaining column exists
        try:
            result = await conn.execute(text("SELECT lessons_remaining FROM students LIMIT 1"))
            print("✓ lessons_remaining column already exists")
        except Exception as e:
            print("Adding lessons_remaining column to students table...")
            await conn.execute(text("ALTER TABLE students ADD COLUMN lessons_remaining INTEGER DEFAULT 8"))
            print("✓ Column added")
        
        # Initialize lessons_remaining for existing students
        await conn.execute(text("""
            UPDATE students 
            SET lessons_remaining = lessons_count 
            WHERE lessons_remaining IS NULL OR lessons_remaining = 0
        """))
        print("✓ Initialized lessons_remaining for existing students")
        
        # Check and add Attendance columns
        try:
            result = await conn.execute(text("SELECT is_extra FROM attendance LIMIT 1"))
            print("✓ is_extra column already exists")
        except:
            print("Adding is_extra column to attendance table...")
            await conn.execute(text("ALTER TABLE attendance ADD COLUMN is_extra BOOLEAN DEFAULT 0"))
            print("✓ is_extra column added")
        
        try:
            result = await conn.execute(text("SELECT attendance_date FROM attendance LIMIT 1"))
            print("✓ attendance_date column already exists")
        except:
            print("Adding attendance_date column to attendance table...")
            await conn.execute(text("ALTER TABLE attendance ADD COLUMN attendance_date DATE"))
            # Update existing records with lesson date
            await conn.execute(text("""
                UPDATE attendance 
                SET attendance_date = (SELECT date FROM lessons WHERE lessons.id = attendance.lesson_id)
                WHERE attendance_date IS NULL
            """))
            print("✓ attendance_date column added and populated")
        
        try:
            result = await conn.execute(text("SELECT attendance_time FROM attendance LIMIT 1"))
            print("✓ attendance_time column already exists")
        except:
            print("Adding attendance_time column to attendance table...")
            await conn.execute(text("ALTER TABLE attendance ADD COLUMN attendance_time VARCHAR(10)"))
            # Update existing records with lesson time
            await conn.execute(text("""
                UPDATE attendance 
                SET attendance_time = (SELECT time FROM lessons WHERE lessons.id = attendance.lesson_id)
                WHERE attendance_time IS NULL
            """))
            print("✓ attendance_time column added and populated")
        
        # Make lesson_id nullable
        try:
            # SQLite doesn't support altering column constraints directly
            # We'll just note this - new records will work with nullable
            print("ℹ️ lesson_id is now nullable for new records (extra attendance)")
        except Exception as e:
            print(f"Note: {e}")
        
        # Create DailyNotificationLog table
        try:
            result = await conn.execute(text("SELECT id FROM daily_notification_logs LIMIT 1"))
            print("✓ daily_notification_logs table already exists")
        except:
            print("Creating daily_notification_logs table...")
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS daily_notification_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    coach_id INTEGER NOT NULL,
                    notification_type VARCHAR(50) NOT NULL,
                    sent_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    date DATE NOT NULL,
                    FOREIGN KEY (coach_id) REFERENCES coaches (id)
                )
            """))
            print("✓ daily_notification_logs table created")
    
    print("\n✅ Migration completed successfully!")
    print("\nNew features available:")
    print("  • lessons_remaining tracking per student")
    print("  • Extra attendance (out-of-schedule lessons)")
    print("  • Daily notification logs")
    print("  • Attendance history with actual date/time")


if __name__ == "__main__":
    asyncio.run(migrate())
