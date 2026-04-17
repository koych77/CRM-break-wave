#!/usr/bin/env python3
"""
Migration: Add is_unlimited column to students table
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import engine
from sqlalchemy import text


async def migrate():
    print("🔄 Adding is_unlimited column...")
    
    async with engine.begin() as conn:
        # Check if column exists
        try:
            await conn.execute(text("SELECT is_unlimited FROM students LIMIT 1"))
            print("✅ Column already exists")
        except:
            # Add column
            await conn.execute(text("ALTER TABLE students ADD COLUMN is_unlimited BOOLEAN DEFAULT 0"))
            print("✅ Column added")
    
    print("✅ Migration complete!")


if __name__ == "__main__":
    asyncio.run(migrate())
