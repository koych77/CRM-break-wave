-- The balance is now calculated per calendar month, so stored counters go away.
-- A month can hold several payments: the subscription plus any upgrades and single lessons.
CREATE TABLE payments_new (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  student_id INTEGER NOT NULL REFERENCES students(id),
  coach_id INTEGER NOT NULL,
  kind TEXT NOT NULL DEFAULT 'subscription',
  amount INTEGER NOT NULL,
  lessons_count INTEGER NOT NULL,
  tariff_code TEXT NOT NULL,
  is_partial INTEGER NOT NULL DEFAULT 0,
  attended INTEGER NOT NULL DEFAULT 0,
  adjustment INTEGER NOT NULL DEFAULT 0,
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
  created_at TEXT NOT NULL
);
INSERT INTO payments_new (id, student_id, coach_id, amount, lessons_count, tariff_code, status, period_start, period_end,
  due_date, method, receipt_file_id, reported_at, paid_at, confirmed_by_coach_id, cash_received_by_coach_id, rejection_reason, created_at)
SELECT id, student_id, coach_id, amount, lessons_count, tariff_code, status, period_start, period_end,
  due_date, method, receipt_file_id, reported_at, paid_at, confirmed_by_coach_id, cash_received_by_coach_id, rejection_reason, created_at
FROM payments;
DROP TABLE payments;
ALTER TABLE payments_new RENAME TO payments;
CREATE INDEX idx_payments_status ON payments(status);
CREATE INDEX idx_payments_period ON payments(student_id, period_start);
CREATE UNIQUE INDEX idx_payments_subscription ON payments(student_id, period_start) WHERE kind = 'subscription';

ALTER TABLE students DROP COLUMN lessons_count;
ALTER TABLE students DROP COLUMN lessons_remaining;
ALTER TABLE attendance DROP COLUMN deducted;
