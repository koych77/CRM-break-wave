CREATE TABLE coaches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  telegram_id INTEGER NOT NULL UNIQUE,
  first_name TEXT,
  username TEXT,
  is_admin INTEGER NOT NULL DEFAULT 0,
  is_manager INTEGER NOT NULL DEFAULT 0,
  is_active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL
);

CREATE TABLE locations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE training_groups (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE parents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  telegram_id INTEGER NOT NULL UNIQUE,
  full_name TEXT NOT NULL,
  phone TEXT NOT NULL,
  bot_blocked_at TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE students (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  coach_id INTEGER NOT NULL REFERENCES coaches(id),
  parent_id INTEGER REFERENCES parents(id),
  name TEXT NOT NULL,
  phone TEXT,
  parent_phone TEXT,
  birthday TEXT,
  notes TEXT,
  lessons_count INTEGER NOT NULL DEFAULT 0,
  lessons_remaining INTEGER NOT NULL DEFAULT 0,
  training_start_date TEXT,
  reminders_enabled INTEGER NOT NULL DEFAULT 1,
  is_active INTEGER NOT NULL DEFAULT 1,
  deleted_at TEXT,
  created_at TEXT NOT NULL
);
CREATE INDEX idx_students_coach ON students(coach_id, is_active);
CREATE INDEX idx_students_parent ON students(parent_id);

-- One row per weekly training; weekday 0 is Monday.
CREATE TABLE schedules (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  student_id INTEGER NOT NULL REFERENCES students(id) ON DELETE CASCADE,
  weekday INTEGER NOT NULL CHECK (weekday BETWEEN 0 AND 6),
  time TEXT NOT NULL,
  location_id INTEGER REFERENCES locations(id),
  group_id INTEGER REFERENCES training_groups(id)
);
CREATE INDEX idx_schedules_student ON schedules(student_id);
CREATE INDEX idx_schedules_weekday ON schedules(weekday);

CREATE TABLE makeups (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  student_id INTEGER NOT NULL REFERENCES students(id) ON DELETE CASCADE,
  source_attendance_id INTEGER UNIQUE,
  source_date TEXT NOT NULL,
  source_type TEXT NOT NULL DEFAULT 'absence',
  expires_at TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'available',
  requested_date TEXT,
  requested_time TEXT,
  requested_location_id INTEGER,
  requested_group_id INTEGER,
  scheduled_date TEXT,
  scheduled_time TEXT,
  location_id INTEGER,
  group_id INTEGER,
  rejection_reason TEXT,
  used_at TEXT,
  created_at TEXT NOT NULL
);
CREATE INDEX idx_makeups_student ON makeups(student_id, status);
CREATE INDEX idx_makeups_scheduled ON makeups(scheduled_date);

CREATE TABLE attendance (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  student_id INTEGER NOT NULL REFERENCES students(id) ON DELETE CASCADE,
  date TEXT NOT NULL,
  time TEXT NOT NULL,
  status TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT 'scheduled',
  makeup_id INTEGER,
  deducted INTEGER NOT NULL DEFAULT 0,
  marked_by_coach_id INTEGER,
  created_at TEXT NOT NULL,
  UNIQUE (student_id, date, time)
);
CREATE INDEX idx_attendance_date ON attendance(date);

CREATE TABLE payments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  student_id INTEGER NOT NULL REFERENCES students(id),
  coach_id INTEGER NOT NULL,
  amount INTEGER NOT NULL,
  lessons_count INTEGER NOT NULL,
  tariff_code TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  period_start TEXT NOT NULL,
  period_end TEXT NOT NULL,
  due_date TEXT NOT NULL,
  method TEXT,
  receipt_file_id TEXT,
  reported_at TEXT,
  paid_at TEXT,
  confirmed_by_coach_id INTEGER,
  cash_received_by_coach_id INTEGER,
  rejection_reason TEXT,
  created_at TEXT NOT NULL,
  UNIQUE (student_id, period_start)
);
CREATE INDEX idx_payments_status ON payments(status);

CREATE TABLE registration_requests (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  parent_id INTEGER NOT NULL REFERENCES parents(id),
  student_id INTEGER,
  child_name TEXT NOT NULL,
  child_birthday TEXT NOT NULL,
  child_phone TEXT,
  proposed_schedule TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  rejection_reason TEXT,
  reviewed_by_coach_id INTEGER,
  created_at TEXT NOT NULL
);

CREATE TABLE schedule_requests (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  student_id INTEGER NOT NULL REFERENCES students(id) ON DELETE CASCADE,
  proposed_schedule TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  rejection_reason TEXT,
  reviewed_by_coach_id INTEGER,
  created_at TEXT NOT NULL
);

CREATE TABLE training_responses (
  student_id INTEGER NOT NULL REFERENCES students(id) ON DELETE CASCADE,
  date TEXT NOT NULL,
  time TEXT NOT NULL,
  response TEXT NOT NULL,
  responded_at TEXT NOT NULL,
  PRIMARY KEY (student_id, date, time)
);

-- Outgoing Telegram messages. The unique key makes every scheduled job safe to repeat.
CREATE TABLE outbox (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  dedupe_key TEXT UNIQUE,
  chat_id INTEGER NOT NULL,
  text TEXT NOT NULL,
  reply_markup TEXT,
  sent_at TEXT,
  error TEXT,
  created_at TEXT NOT NULL
);
CREATE INDEX idx_outbox_pending ON outbox(sent_at, id);

CREATE TABLE audit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  actor_telegram_id INTEGER,
  action TEXT NOT NULL,
  entity TEXT,
  entity_id INTEGER,
  details TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE job_runs (
  key TEXT PRIMARY KEY,
  ran_at TEXT NOT NULL
);
