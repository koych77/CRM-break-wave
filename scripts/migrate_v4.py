#!/usr/bin/env python3
"""
Migration script for v4.0 - Add locations and update student schema
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from app.database import engine


async def migrate():
    """Run migration for v4.0"""
    print("Starting v4.0 migration...")
    
    async with engine.begin() as conn:
        # 1. Create locations table
        print("Creating locations table...")
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS locations (
                id INTEGER PRIMARY KEY,
                coach_id INTEGER NOT NULL,
                name VARCHAR(200) NOT NULL,
                address VARCHAR(500),
                is_active BOOLEAN DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (coach_id) REFERENCES coaches (id)
            )
        """))
        print("✓ locations table created")
        
        # 2. Add location_id to students
        try:
            await conn.execute(text("SELECT location_id FROM students LIMIT 1"))
            print("✓ location_id already exists")
        except:
            print("Adding location_id to students...")
            await conn.execute(text("ALTER TABLE students ADD COLUMN location_id INTEGER"))
            print("✓ location_id added")
        
        # 3. Add lesson_times to students
        try:
            await conn.execute(text("SELECT lesson_times FROM students LIMIT 1"))
            print("✓ lesson_times already exists")
        except:
            print("Adding lesson_times to students...")
            await conn.execute(text('''ALTER TABLE students ADD COLUMN lesson_times VARCHAR(500) DEFAULT '{"1": "18:00", "3": "18:00"}''''))
            # Update existing records with default times based on lesson_days
            result = await conn.execute(text("SELECT id, lesson_days, lesson_time FROM students WHERE lesson_times IS NULL"))
            rows = result.fetchall()
            for row in rows:
                student_id, days, time = row
                if days:
                    days_list = [d.strip() for d in days.split(',')]
                    times_dict = {d: (time or '18:00') for d in days_list}
                    import json
                    times_json = json.dumps(times_dict)
                    await conn.execute(
                        text("UPDATE students SET lesson_times = :times WHERE id = :id"),
                        {"times": times_json, "id": student_id}
                    )
            print("✓ lesson_times added and populated")
        
        # 4. Add lessons_remaining to students
        try:
            await conn.execute(text("SELECT lessons_remaining FROM students LIMIT 1"))
            print("✓ lessons_remaining already exists")
        except:
            print("Adding lessons_remaining to students...")
            await conn.execute(text("ALTER TABLE students ADD COLUMN lessons_remaining INTEGER"))
            await conn.execute(text("UPDATE students SET lessons_remaining = lessons_count WHERE lessons_remaining IS NULL"))
            print("✓ lessons_remaining added and populated")
        
        # 5. Add location_id to lessons
        try:
            await conn.execute(text("SELECT location_id FROM lessons LIMIT 1"))
            print("✓ lessons.location_id already exists")
        except:
            print("Adding location_id to lessons...")
            await conn.execute(text("ALTER TABLE lessons ADD COLUMN location_id INTEGER"))
            print("✓ lessons.location_id added")
        
        # 6. Add location_id to attendance
        try:
            await conn.execute(text("SELECT location_id FROM attendance LIMIT 1"))
            print("✓ attendance.location_id already exists")
        except:
            print("Adding location_id to attendance...")
            await conn.execute(text("ALTER TABLE attendance ADD COLUMN location_id INTEGER"))
            print("✓ attendance.location_id added")
        
        # 7. Create daily_notification_logs table
        try:
            await conn.execute(text("SELECT id FROM daily_notification_logs LIMIT 1"))
            print("✓ daily_notification_logs already exists")
        except:
            print("Creating daily_notification_logs table...")
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS daily_notification_logs (
                    id INTEGER PRIMARY KEY,
                    coach_id INTEGER NOT NULL,
                    notification_type VARCHAR(50) NOT NULL,
                    sent_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    date DATE NOT NULL,
                    FOREIGN KEY (coach_id) REFERENCES coaches (id)
                )
            """))
            print("✓ daily_notification_logs created")
        
        print("\n✅ Migration completed successfully!")
        print("\nNew features available:")
        print("  • Multiple training locations")
        print("  • Different times per day of week")
        print("  • Lesson counter tracking")
        print("  • Daily notification logs")


if __name__ == "__main__":
    asyncio.run(migrate())
