// Shared lesson, makeup and invoice operations.
import { all, first, placeholders, run } from './db.js';
import {
  DEFAULT_TARIFF,
  MONTHLY_MAKEUP_LIMIT,
  TARIFFS,
  consumesLesson,
  dueDate,
  isoNow,
  makeupExpiry,
  monthBounds,
} from './rules.js';

export function tariffOf(code) {
  return TARIFFS[String(code)] ?? null;
}

export async function ensureInvoice(db, student, date) {
  const { start, end } = monthBounds(date);
  const existing = await first(db, 'SELECT * FROM payments WHERE student_id = ? AND period_start = ?', student.id, start);
  if (existing) return existing;
  const previous = await first(
    db,
    'SELECT tariff_code FROM payments WHERE student_id = ? AND period_start < ? ORDER BY period_start DESC LIMIT 1',
    student.id,
    start,
  );
  const code = tariffOf(previous?.tariff_code) ? previous.tariff_code : DEFAULT_TARIFF;
  const tariff = tariffOf(code);
  await run(
    db,
    `INSERT OR IGNORE INTO payments
       (student_id, coach_id, amount, lessons_count, tariff_code, status, period_start, period_end, due_date, created_at)
     VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?)`,
    student.id,
    student.coach_id,
    tariff.price,
    tariff.lessons,
    code,
    start,
    end,
    dueDate(start),
    isoNow(),
  );
  return first(db, 'SELECT * FROM payments WHERE student_id = ? AND period_start = ?', student.id, start);
}

export async function ensureAllInvoices(db, date) {
  const { start } = monthBounds(date);
  const missing = await all(
    db,
    `SELECT s.* FROM students s
     WHERE s.is_active = 1 AND s.deleted_at IS NULL
       AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.student_id = s.id AND p.period_start = ?)`,
    start,
  );
  for (const student of missing) await ensureInvoice(db, student, date);
}

export function oldestOpenInvoice(db, studentId) {
  return first(
    db,
    `SELECT * FROM payments WHERE student_id = ? AND status NOT IN ('paid', 'written_off')
     ORDER BY period_start, id LIMIT 1`,
    studentId,
  );
}

// Payment confirms the month's subscription; lessons already held that month count against it.
export async function markPaid(db, payment, student, coachId, receivedByCoachId = null) {
  const consumed = await first(
    db,
    `SELECT COUNT(*) AS count FROM attendance
     WHERE student_id = ? AND date BETWEEN ? AND ? AND status IN ('present', 'absent')
       AND source NOT IN ('makeup', 'coach_cancelled')`,
    student.id,
    payment.period_start,
    payment.period_end,
  );
  const left = Math.max(payment.lessons_count - consumed.count, 0);
  await db.batch([
    db.prepare(
      `UPDATE payments SET status = 'paid', paid_at = ?, confirmed_by_coach_id = ?, cash_received_by_coach_id = ?,
         rejection_reason = NULL WHERE id = ?`,
    ).bind(isoNow(), coachId, receivedByCoachId, payment.id),
    db.prepare('UPDATE students SET lessons_count = ?, lessons_remaining = ? WHERE id = ?')
      .bind(payment.lessons_count, left, student.id),
  ]);
  return left;
}

async function createMakeup(db, student, attendance, sourceType) {
  const date = attendance.date;
  if (sourceType === 'absence') {
    const { start, end } = monthBounds(date);
    const used = await first(
      db,
      `SELECT COUNT(*) AS count FROM makeups
       WHERE student_id = ? AND source_type = 'absence' AND source_date BETWEEN ? AND ?`,
      student.id,
      start,
      end,
    );
    if (used.count >= MONTHLY_MAKEUP_LIMIT) return;
  }
  await run(
    db,
    `INSERT OR IGNORE INTO makeups (student_id, source_attendance_id, source_date, source_type, expires_at, status, created_at)
     VALUES (?, ?, ?, ?, ?, 'available', ?)`,
    student.id,
    attendance.id,
    date,
    sourceType,
    makeupExpiry(date),
    isoNow(),
  );
}

// Applies balance and makeup rules so that repeated saves change nothing.
export async function reconcileAttendance(db, student, attendance) {
  const consumes = consumesLesson(attendance.status, attendance.source);
  if (consumes && !attendance.deducted) {
    await db.batch([
      db.prepare('UPDATE students SET lessons_remaining = MAX(lessons_remaining - 1, 0) WHERE id = ?').bind(student.id),
      db.prepare('UPDATE attendance SET deducted = 1 WHERE id = ?').bind(attendance.id),
    ]);
  } else if (!consumes && attendance.deducted) {
    await db.batch([
      db.prepare('UPDATE students SET lessons_remaining = lessons_remaining + 1 WHERE id = ?').bind(student.id),
      db.prepare('UPDATE attendance SET deducted = 0 WHERE id = ?').bind(attendance.id),
    ]);
  }

  if (attendance.source === 'makeup') {
    if (attendance.makeup_id) {
      await run(
        db,
        'UPDATE makeups SET status = ?, used_at = ? WHERE id = ?',
        attendance.status === 'present' ? 'used' : 'burned',
        isoNow(),
        attendance.makeup_id,
      );
    }
    return;
  }

  const absence = attendance.source === 'scheduled' && attendance.status === 'absent';
  const cancelled = attendance.source === 'coach_cancelled';
  if (absence || cancelled) {
    await run(
      db,
      `UPDATE makeups SET source_type = ? WHERE source_attendance_id = ? AND status IN ('available', 'requested')`,
      cancelled ? 'coach_cancelled' : 'absence',
      attendance.id,
    );
    await run(
      db,
      `UPDATE makeups SET status = 'available' WHERE source_attendance_id = ? AND status = 'cancelled'`,
      attendance.id,
    );
    const existing = await first(db, 'SELECT id FROM makeups WHERE source_attendance_id = ?', attendance.id);
    if (!existing) await createMakeup(db, student, attendance, cancelled ? 'coach_cancelled' : 'absence');
  } else {
    await run(
      db,
      `UPDATE makeups SET status = 'cancelled' WHERE source_attendance_id = ? AND status IN ('available', 'requested')`,
      attendance.id,
    );
  }
}

export async function saveAttendance(db, student, { date, time, status, source, makeupId = null, coachId = null }) {
  const row = await first(
    db,
    `INSERT INTO attendance (student_id, date, time, status, source, makeup_id, marked_by_coach_id, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT (student_id, date, time) DO UPDATE SET
       status = excluded.status, source = excluded.source, makeup_id = excluded.makeup_id,
       marked_by_coach_id = excluded.marked_by_coach_id
     RETURNING *`,
    student.id,
    date,
    time,
    status,
    source,
    makeupId,
    coachId,
    isoNow(),
  );
  await reconcileAttendance(db, student, row);
  return row;
}

export async function expireMakeups(db, today) {
  await run(
    db,
    `UPDATE makeups SET status = CASE
        WHEN EXISTS (SELECT 1 FROM attendance a WHERE a.makeup_id = makeups.id AND a.status = 'present') THEN 'used'
        ELSE 'burned' END, used_at = ?
     WHERE status = 'scheduled' AND scheduled_date < ?`,
    isoNow(),
    today,
  );
  await run(
    db,
    `UPDATE makeups SET status = 'expired' WHERE status IN ('available', 'requested') AND expires_at < ?`,
    today,
  );
}

export async function schedulesFor(db, studentIds) {
  if (!studentIds.length) return new Map();
  const rows = await all(
    db,
    `SELECT sc.*, l.name AS location, g.name AS group_name
     FROM schedules sc
     LEFT JOIN locations l ON l.id = sc.location_id
     LEFT JOIN training_groups g ON g.id = sc.group_id
     WHERE sc.student_id IN (${placeholders(studentIds)})
     ORDER BY sc.weekday, sc.time`,
    ...studentIds,
  );
  const result = new Map(studentIds.map((id) => [id, []]));
  for (const row of rows) result.get(row.student_id).push(row);
  return result;
}

export async function replaceSchedule(db, studentId, rows) {
  await db.batch([
    db.prepare('DELETE FROM schedules WHERE student_id = ?').bind(studentId),
    ...rows.map((row) =>
      db.prepare('INSERT INTO schedules (student_id, weekday, time, location_id, group_id) VALUES (?, ?, ?, ?, ?)')
        .bind(studentId, row.weekday, row.time, row.location_id ?? null, row.group_id ?? null)),
  ]);
}
