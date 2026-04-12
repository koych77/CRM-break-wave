#!/usr/bin/env python3
"""
Migration script: Move student schedule data from legacy fields to student_schedules table.

This allows students to have multiple locations with different schedules.
Legacy fields (lesson_days, lesson_times, location_id) are kept for backward compatibility.
"""

import asyncio
import sys
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import engine, Base, async_session
from app.models import Student, StudentSchedule, Location
from sqlalchemy import select


async def migrate_schedules():
    """Migrate existing students to new schedule structure."""
    print("🔄 Starting migration to student_schedules...")
    
    # Create new table if not exists
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("✅ Tables created/verified")
    
    async with async_session() as session:
        # Get all students
        result = await session.execute(select(Student))
        students = result.scalars().all()
        
        migrated = 0
        skipped = 0
        errors = 0
        
        for student in students:
            try:
                # Check if student already has schedules
                if student.schedules:
                    print(f"⏭️  Student {student.name} (ID: {student.id}) already has schedules, skipping")
                    skipped += 1
                    continue
                
                # Check if legacy data exists
                if not student.lesson_days:
                    print(f"⏭️  Student {student.name} (ID: {student.id}) has no lesson_days, skipping")
                    skipped += 1
                    continue
                
                # Get location_id (try to find matching location or use None)
                location_id = student.location_id
                
                # If location_id not set but location name exists, try to find it
                if not location_id and student.location:
                    loc_result = await session.execute(
                        select(Location).where(
                            Location.name == student.location,
                            Location.coach_id == student.coach_id
                        )
                    )
                    loc = loc_result.scalar_one_or_none()
                    if loc:
                        location_id = loc.id
                
                # Create primary schedule from legacy data
                schedule = StudentSchedule(
                    student_id=student.id,
                    location_id=location_id,
                    days=student.lesson_days,
                    times=student.lesson_times or '{"1": "18:00", "3": "18:00"}',
                    duration=student.lesson_duration or 90,
                    is_primary=True
                )
                session.add(schedule)
                migrated += 1
                print(f"✅ Migrated: {student.name} (ID: {student.id}) - days: {student.lesson_days}")
                
            except Exception as e:
                print(f"❌ Error migrating student {student.id}: {e}")
                errors += 1
                continue
        
        await session.commit()
    
    print(f"\n📊 Migration complete:")
    print(f"   Migrated: {migrated}")
    print(f"   Skipped: {skipped}")
    print(f"   Errors: {errors}")
    print(f"\n✨ Students can now have multiple locations with different schedules!")


async def verify_migration():
    """Verify the migration worked correctly."""
    print("\n🔍 Verifying migration...")
    
    async with async_session() as session:
        # Count students with schedules
        result = await session.execute(select(StudentSchedule))
        schedules = result.scalars().all()
        
        print(f"   Total schedules in database: {len(schedules)}")
        
        # Show sample
        for sched in schedules[:5]:
            student = await session.get(Student, sched.student_id)
            loc = await session.get(Location, sched.location_id) if sched.location_id else None
            loc_name = loc.name if loc else "Unknown"
            print(f"   - {student.name if student else 'Unknown'}: {loc_name}, days: {sched.days}")


if __name__ == "__main__":
    print("=" * 60)
    print("CRM Break Wave - Schedule Migration Tool")
    print("=" * 60)
    
    asyncio.run(migrate_schedules())
    asyncio.run(verify_migration())
