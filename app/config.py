import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
WEBAPP_DIR = BASE_DIR / "app" / "webapp"
DATA_DIR.mkdir(exist_ok=True)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = list(map(int, os.getenv("ADMIN_IDS", "").split(","))) if os.getenv("ADMIN_IDS") else []
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "bwcoach2026")

DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite+aiosqlite:///{DATA_DIR / 'coach_crm.db'}")
WEBAPP_URL = os.getenv("WEBAPP_URL", "")

# Constants
LESSON_STATUS = {
    "present": "✅ Был",
    "absent": "❌ Не был",
    "sick": "🤒 Болеет",
    "excused": "📝 Уважительная",
}

PAYMENT_STATUS = {
    "paid": "✅ Оплачено",
    "pending": "⏳ Ожидает",
    "overdue": "⚠️ Просрочено",
}

WEEKDAYS = {
    0: "Пн",
    1: "Вт",
    2: "Ср",
    3: "Чт",
    4: "Пт",
    5: "Сб",
    6: "Вс",
}
