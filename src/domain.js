// Shared lesson, makeup and invoice operations.
import { all, first, placeholders, run } from './db.js';
import {
  MONTHLY_MAKEUP_LIMIT,
  TARIFFS,
  countTrainings,
  dueDate,
  firstTrainingDate,
  isMonthlyTariff,
  isoNow,
  makeupExpiry,
  minskNow,
  monthBounds,
  partialPrice,
  tariffForSchedule,
} from './rules.js';

export function tariffOf(code) {
  return TARIFFS[String(code)] ?? null;
}

const subscriptionSql = `SELECT * FROM payments WHERE student_id = ? AND period_start = ? AND kind = 'subscription'`;

function monthSubscription(db, studentId, start) {
  return first(db, subscriptionSql, studentId, start);
}

// Returns the month's subscription invoice, or null while the child has no training in that month.
export async function ensureInvoice(db, student, date) {
  const { start, end } = monthBounds(date);
  const existing = await monthSubscription(db, student.id, start);
  if (existing) return existing;
  const startDate = student.training_start_date;
  if (startDate && startDate > end) return null;
  const previous = await first(
    db,
    `SELECT tariff_code, is_partial FROM payments WHERE student_id = ? AND period_start < ? AND kind = 'subscription'
     ORDER BY period_start DESC LIMIT 1`,
    student.id,
    start,
  );
  const schedule = await all(db, 'SELECT weekday FROM schedules WHERE student_id = ?', student.id);
  let code = previous && !previous.is_partial && isMonthlyTariff(previous.tariff_code)
    ? previous.tariff_code
    : tariffForSchedule(schedule.length);
  let { price: amount, lessons } = tariffOf(code);
  let isPartial = 0;
  let due = startDate && startDate > dueDate(start) && startDate <= end ? startDate : dueDate(start);
  if (!previous && startDate && startDate > start && schedule.length) {
    lessons = countTrainings(schedule, startDate, end);
    if (!lessons) return null;
    ({ code, amount } = partialPrice(lessons));
    isPartial = 1;
    due = firstTrainingDate(schedule, startDate, end);
  }
  await run(
    db,
    `INSERT OR IGNORE INTO payments
       (student_id, coach_id, amount, lessons_count, tariff_code, is_partial, status, period_start, period_end, due_date, created_at)
     VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?)`,
    student.id,
    student.coach_id,
    amount,
    lessons,
    code,
    isPartial,
    start,
    end,
    due,
    isoNow(),
  );
  await refreshAttended(db, student.id, start);
  return monthSubscription(db, student.id, start);
}

// Recreates an untouched first invoice after the start date or the schedule changes.
export async function refreshFirstInvoice(db, student, date) {
  const { start } = monthBounds(date);
  const invoice = await monthSubscription(db, student.id, start);
  if (!invoice || invoice.status !== 'pending' || invoice.attended) return;
  const earlier = await first(
    db,
    `SELECT id FROM payments WHERE student_id = ? AND period_start < ? AND kind = 'subscription' LIMIT 1`,
    student.id,
    start,
  );
  if (earlier) return;
  await run(db, 'DELETE FROM payments WHERE id = ?', invoice.id);
  await ensureInvoice(db, student, date);
}

// Marks whether the child trained in the month beyond paid single lessons; only then an unpaid month is a debt.
export async function refreshAttended(db, studentId, date) {
  const { start, end } = monthBounds(date);
  const present = await first(
    db,
    `SELECT COUNT(*) AS count FROM attendance
     WHERE student_id = ? AND date BETWEEN ? AND ? AND status = 'present' AND source = 'scheduled'`,
    studentId,
    start,
    end,
  );
  const singles = await first(
    db,
    `SELECT COALESCE(SUM(lessons_count), 0) AS count FROM payments
     WHERE student_id = ? AND period_start = ? AND kind = 'single' AND status = 'paid'`,
    studentId,
    start,
  );
  const attended = present.count > singles.count ? 1 : 0;
  await run(
    db,
    `UPDATE payments SET attended = ?, status = CASE WHEN ? = 1 AND status = 'skipped' THEN 'pending' ELSE status END
     WHERE student_id = ? AND period_start = ? AND kind = 'subscription'`,
    attended,
    attended,
    studentId,
    start,
  );
}

// The balance of one calendar month: lessons bought for it minus lessons used in it.
export async function balancesFor(db, studentIds, date) {
  const result = new Map(studentIds.map((id) => [id, {
    total: 0, used: 0, left: 0, over: 0, paid: false, hasInvoice: false,
  }]));
  if (!studentIds.length) return result;
  const { start, end } = monthBounds(date);
  const payments = await all(
    db,
    `SELECT student_id, kind, status, lessons_count, adjustment FROM payments
     WHERE period_start = ? AND student_id IN (${placeholders(studentIds)})`,
    start,
    ...studentIds,
  );
  for (const payment of payments) {
    const balance = result.get(payment.student_id);
    if (payment.kind === 'subscription') {
      balance.hasInvoice = true;
      balance.total += payment.lessons_count + payment.adjustment;
    } else if (payment.status === 'paid') {
      balance.total += payment.lessons_count;
    }
    if (payment.status === 'paid') balance.paid = true;
  }
  const used = await all(
    db,
    `SELECT student_id, COUNT(*) AS count FROM attendance
     WHERE date BETWEEN ? AND ? AND status IN ('present', 'absent') AND source NOT IN ('makeup', 'coach_cancelled')
       AND student_id IN (${placeholders(studentIds)})
     GROUP BY student_id`,
    start,
    end,
    ...studentIds,
  );
  for (const row of used) result.get(row.student_id).used = row.count;
  for (const balance of result.values()) {
    balance.left = Math.max(balance.total - balance.used, 0);
    balance.over = Math.max(balance.used - balance.total, 0);
  }
  return result;
}

export async function balanceOf(db, studentId, date) {
  return (await balancesFor(db, [studentId], date)).get(studentId);
}

export async function ensureAllInvoices(db, date) {
  const { start } = monthBounds(date);
  const missing = await all(
    db,
    `SELECT s.* FROM students s
     WHERE s.is_active = 1 AND s.deleted_at IS NULL
       AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.student_id = s.id AND p.period_start = ? AND p.kind = 'subscription')`,
    start,
  );
  for (const student of missing) await ensureInvoice(db, student, date);
}

// The earliest month to settle. A past month without training is not a debt and does not block paying.
export function oldestOpenInvoice(db, studentId) {
  return first(
    db,
    `SELECT * FROM payments WHERE student_id = ? AND kind = 'subscription'
       AND status NOT IN ('paid', 'written_off', 'skipped') AND (attended = 1 OR period_end >= ?)
     ORDER BY period_start, id LIMIT 1`,
    studentId,
    minskNow().date,
  );
}

// Unpaid months nobody trained in, and offers nobody took up, close when the month ends.
export async function closeUnusedMonths(db, today) {
  await run(
    db,
    `UPDATE payments SET status = 'skipped'
     WHERE status IN ('pending', 'rejected') AND period_end < ? AND (kind != 'subscription' OR attended = 0)`,
    today,
  );
}

// Payment confirms lessons of its own month only; the balance is always calculated from attendance.
export async function markPaid(db, payment, student, coachId, receivedByCoachId = null) {
  await run(
    db,
    `UPDATE payments SET status = 'paid', paid_at = ?, confirmed_by_coach_id = ?, cash_received_by_coach_id = ?,
       rejection_reason = NULL WHERE id = ?`,
    isoNow(),
    coachId,
    receivedByCoachId,
    payment.id,
  );
  await refreshAttended(db, student.id, payment.period_start);
  // Missed lessons of an unpaid month earn their makeups once the month is paid.
  const missed = await all(
    db,
    `SELECT * FROM attendance WHERE student_id = ? AND date BETWEEN ? AND ?
       AND (source = 'coach_cancelled' OR (source = 'scheduled' AND status = 'absent'))`,
    student.id,
    payment.period_start,
    payment.period_end,
  );
  for (const attendance of missed) await reconcileAttendance(db, student, attendance);
  return (await balanceOf(db, student.id, payment.period_start)).left;
}

async function isMonthPaid(db, studentId, date) {
  const { start } = monthBounds(date);
  const paid = await first(
    db,
    `SELECT id FROM payments WHERE student_id = ? AND period_start = ? AND status = 'paid' LIMIT 1`,
    studentId,
    start,
  );
  return Boolean(paid);
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

// Applies makeup rules so that repeated saves change nothing.
export async function reconcileAttendance(db, student, attendance) {
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
    if (!existing && await isMonthPaid(db, student.id, attendance.date)) {
      await createMakeup(db, student, attendance, cancelled ? 'coach_cancelled' : 'absence');
    }
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
  await refreshAttended(db, student.id, date);
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
