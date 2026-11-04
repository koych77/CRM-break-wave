import { coachOptions, requireAdmin, requireCoach } from './auth.js';
import { all, audit, first, insert, run } from './db.js';
import {
  balanceOf,
  balancesFor,
  ensureAllInvoices,
  ensureInvoice,
  markPaid,
  oldestOpenInvoice,
  refreshFirstInvoice,
  replaceSchedule,
  saveAttendance,
  schedulesFor,
  tariffOf,
} from './domain.js';
import { ApiError, optionalInt, positiveInt, text } from './http.js';
import {
  LOW_LESSONS,
  coveringTariff,
  formatDate,
  invoiceStatus,
  isMonthlyTariff,
  isoNow,
  minskNow,
  monthBounds,
  validDate,
  validTime,
  weekdayOf,
} from './rules.js';
import { parseScheduleRows } from './parent.js';
import { callTelegram, enqueue, notifyParentOfStudent } from './telegram.js';

const scopeSql = (coach, alias = 's') => (coach.can_admin ? '' : ` AND ${alias}.coach_id = ${Number(coach.id)}`);

async function visibleStudent(ctx, coach, studentId) {
  const student = await first(
    ctx.db,
    `SELECT * FROM students s WHERE s.id = ? AND s.deleted_at IS NULL${scopeSql(coach)}`,
    positiveInt(studentId),
  );
  if (!student) throw new ApiError(404, 'student_not_found', 'Ученик не найден');
  return student;
}

function studentFlag(balance, payStatus) {
  if (payStatus === 'overdue') return 'debt';
  if (balance.hasInvoice && balance.left <= LOW_LESSONS) return 'low';
  return null;
}

export function resources(ctx) {
  const coach = requireCoach(ctx.actor);
  return loadResources(ctx.db, coach);
}

async function loadResources(db, coach) {
  const [locations, groups, coaches] = await Promise.all([
    all(db, 'SELECT id, name FROM locations WHERE is_active = 1 ORDER BY name'),
    all(db, 'SELECT id, name FROM training_groups WHERE is_active = 1 ORDER BY name'),
    coach.can_admin ? all(db, 'SELECT id, first_name FROM coaches WHERE is_active = 1 ORDER BY first_name') : [],
  ]);
  return { locations, groups, coaches };
}

// A session is one training slot: all students sharing a time, hall and group that day.
async function buildDay(db, coach, date) {
  const weekday = weekdayOf(date);
  const scheduled = await all(
    db,
    `SELECT sc.time, sc.location_id, sc.group_id, l.name AS location, g.name AS group_name,
            s.id AS student_id, s.name, s.nickname, s.coach_id
     FROM schedules sc
     JOIN students s ON s.id = sc.student_id
     LEFT JOIN locations l ON l.id = sc.location_id
     LEFT JOIN training_groups g ON g.id = sc.group_id
     WHERE sc.weekday = ? AND s.is_active = 1 AND s.deleted_at IS NULL
       AND (s.training_start_date IS NULL OR s.training_start_date <= ?)${scopeSql(coach)}
     ORDER BY sc.time, s.name`,
    weekday,
    date,
  );
  const makeups = await all(
    db,
    `SELECT m.id AS makeup_id, m.scheduled_time AS time, m.location_id, m.group_id, l.name AS location,
            g.name AS group_name, s.id AS student_id, s.name, s.nickname
     FROM makeups m
     JOIN students s ON s.id = m.student_id
     LEFT JOIN locations l ON l.id = m.location_id
     LEFT JOIN training_groups g ON g.id = m.group_id
     WHERE m.scheduled_date = ? AND s.is_active = 1 AND s.deleted_at IS NULL${scopeSql(coach)}`,
    date,
  );
  const marks = await all(
    db,
    'SELECT student_id, time, status, source FROM attendance WHERE date = ?',
    date,
  );
  const markBy = new Map(marks.map((row) => [`${row.student_id}|${row.time}`, row]));
  const responses = await all(db, 'SELECT student_id, time, response FROM training_responses WHERE date = ?', date);
  const responseBy = new Map(responses.map((row) => [`${row.student_id}|${row.time}`, row.response]));
  const overdue = await all(
    db,
    `SELECT DISTINCT student_id FROM payments
     WHERE status = 'pending' AND due_date < ? AND (kind != 'subscription' OR attended = 1)`,
    minskNow().date,
  );
  const debtors = new Set(overdue.map((row) => row.student_id));
  const balances = await balancesFor(db, [...new Set([...scheduled, ...makeups].map((row) => row.student_id))], date);

  const sessions = new Map();
  const add = (row, source, makeupId = null) => {
    const key = `${row.time}|${row.location_id ?? ''}|${row.group_id ?? ''}`;
    if (!sessions.has(key)) {
      sessions.set(key, {
        key,
        time: row.time,
        location_id: row.location_id,
        group_id: row.group_id,
        location: row.location,
        group: row.group_name,
        roster: [],
      });
    }
    const mark = markBy.get(`${row.student_id}|${row.time}`);
    const response = responseBy.get(`${row.student_id}|${row.time}`) ?? null;
    let tag = null;
    if (source === 'makeup') tag = 'Отработка';
    else if (response === 'absent') tag = 'Родитель: не придёт';
    else if (debtors.has(row.student_id)) tag = 'Долг по оплате';
    else if (balances.get(row.student_id).hasInvoice && balances.get(row.student_id).left === 0) tag = 'Занятия в абонементе закончились';
    else if (balances.get(row.student_id).hasInvoice && balances.get(row.student_id).left <= LOW_LESSONS) tag = `Осталось занятий: ${balances.get(row.student_id).left}`;
    sessions.get(key).roster.push({
      student_id: row.student_id,
      name: row.name,
      nickname: row.nickname,
      source,
      makeup_id: makeupId,
      tag,
      status: mark?.status ?? (response === 'absent' ? 'absent' : 'present'),
      marked: Boolean(mark),
      cancelled: mark?.source === 'coach_cancelled',
    });
  };
  for (const row of scheduled) add(row, 'scheduled');
  for (const row of makeups) add(row, 'makeup', row.makeup_id);

  return [...sessions.values()]
    .map((session) => ({
      ...session,
      saved: session.roster.every((item) => item.marked),
      cancelled: session.roster.length > 0 && session.roster.every((item) => item.cancelled),
    }))
    .sort((a, b) => a.time.localeCompare(b.time));
}

export async function day(ctx) {
  const coach = requireCoach(ctx.actor);
  const date = ctx.body.date || minskNow().date;
  if (!validDate(date)) throw new ApiError(400, 'invalid_date', 'Неверная дата');
  return { date, today: minskNow().date, sessions: await buildDay(ctx.db, coach, date) };
}

function assertMarkable(date) {
  if (!validDate(date)) throw new ApiError(400, 'invalid_date', 'Неверная дата');
  if (date > minskNow().date) throw new ApiError(409, 'future_date', 'Эта тренировка ещё не прошла');
}

function findSession(sessions, key) {
  const session = sessions.find((item) => item.key === key);
  if (!session) throw new ApiError(404, 'session_not_found', 'Тренировка не найдена');
  return session;
}

export async function saveMarks(ctx) {
  const coach = requireCoach(ctx.actor);
  const { db, body } = ctx;
  assertMarkable(body.date);
  const session = findSession(await buildDay(db, coach, body.date), body.key);
  const statuses = new Map((body.marks || []).map((mark) => [Number(mark.student_id), mark.status]));
  for (const item of session.roster) {
    const status = statuses.get(item.student_id) ?? item.status;
    if (!['present', 'absent'].includes(status)) throw new ApiError(400, 'invalid_status', 'Неверная отметка');
    const student = await first(db, 'SELECT * FROM students WHERE id = ?', item.student_id);
    await saveAttendance(db, student, {
      date: body.date,
      time: session.time,
      status,
      source: item.source,
      makeupId: item.makeup_id,
      coachId: coach.id,
    });
  }
  const balances = await balancesFor(db, session.roster.map((item) => item.student_id), body.date);
  for (const item of session.roster) {
    if (!balances.get(item.student_id).over) continue;
    const student = await first(db, 'SELECT * FROM students WHERE id = ?', item.student_id);
    await notifyParentOfStudent(
      ctx.env,
      student,
      `${student.name}: занятий в этом месяце больше, чем оплачено.\nДополнительные занятия можно оплатить в кабинете.`,
      `over:${student.id}:${body.date.slice(0, 7)}`,
    );
  }
  await audit(db, ctx.actor.user.id, 'attendance_saved', 'session', null, { date: body.date, key: body.key });
  return { success: true };
}

export async function cancelSession(ctx) {
  const coach = requireCoach(ctx.actor);
  const { db, body, env } = ctx;
  if (!validDate(body.date)) throw new ApiError(400, 'invalid_date', 'Неверная дата');
  const session = findSession(await buildDay(db, coach, body.date), body.key);
  for (const item of session.roster) {
    const student = await first(db, 'SELECT * FROM students WHERE id = ?', item.student_id);
    if (item.source === 'makeup') {
      await run(
        db,
        `UPDATE makeups SET status = 'available', scheduled_date = NULL, scheduled_time = NULL WHERE id = ?`,
        item.makeup_id,
      );
    } else {
      await saveAttendance(db, student, {
        date: body.date,
        time: session.time,
        status: 'absent',
        source: 'coach_cancelled',
        coachId: coach.id,
      });
    }
    await notifyParentOfStudent(
      env,
      student,
      `Тренировка ${formatDate(body.date)} в ${session.time} отменена.\n${student.name}: занятие не спишется, в оплаченном месяце появится отработка.`,
      `cancel:${student.id}:${body.date}:${session.time}`,
    );
  }
  await audit(db, ctx.actor.user.id, 'session_cancelled', 'session', null, { date: body.date, key: body.key });
  return { success: true };
}

export async function students(ctx) {
  const coach = requireCoach(ctx.actor);
  const { db } = ctx;
  const today = minskNow().date;
  await ensureAllInvoices(db, today);
  const rows = await all(
    db,
    `SELECT s.*, p.full_name AS parent_name, p.phone AS parent_account_phone
     FROM students s LEFT JOIN parents p ON p.id = s.parent_id
     WHERE s.is_active = 1 AND s.deleted_at IS NULL${scopeSql(coach)} ORDER BY s.name`,
  );
  const schedules = await schedulesFor(db, rows.map((row) => row.id));
  const balances = await balancesFor(db, rows.map((row) => row.id), today);
  const open = await all(
    db,
    `SELECT student_id, kind, attended, status, due_date FROM payments WHERE status NOT IN ('paid', 'written_off', 'skipped')`,
  );
  const overdueIds = new Set(open.filter((item) => invoiceStatus(item, today) === 'overdue').map((item) => item.student_id));
  return {
    students: rows.map((row) => ({
      id: row.id,
      name: row.name,
      nickname: row.nickname,
      phone: row.phone,
      parent_name: row.parent_name,
      parent_phone: row.parent_account_phone || row.parent_phone,
      lessons_remaining: balances.get(row.id).left,
      lessons_count: balances.get(row.id).total,
      group: schedules.get(row.id)[0]?.group_name ?? null,
      flag: studentFlag(balances.get(row.id), overdueIds.has(row.id) ? 'overdue' : null),
    })),
  };
}

export async function studentDetail(ctx) {
  const coach = requireCoach(ctx.actor);
  const { db } = ctx;
  const student = await visibleStudent(ctx, coach, ctx.params.id);
  const today = minskNow().date;
  const invoice = await ensureInvoice(db, student, today);
  const balance = await balanceOf(db, student.id, today);
  const schedule = (await schedulesFor(db, [student.id])).get(student.id);
  const parent = student.parent_id ? await first(db, 'SELECT full_name, phone FROM parents WHERE id = ?', student.parent_id) : null;
  const makeups = await first(
    db,
    `SELECT COUNT(*) AS count FROM makeups WHERE student_id = ? AND status IN ('available', 'requested', 'scheduled')`,
    student.id,
  );
  const history = await all(
    db,
    'SELECT date, time, status, source FROM attendance WHERE student_id = ? ORDER BY date DESC, time DESC LIMIT 10',
    student.id,
  );
  return {
    student: {
      id: student.id,
      name: student.name,
      nickname: student.nickname,
      phone: student.phone,
      birthday: student.birthday,
      notes: student.notes,
      coach_id: student.coach_id,
      lessons_count: balance.total,
      lessons_remaining: balance.left,
      lessons_used: balance.marked,
      lessons_over: balance.over,
      tariff_code: student.tariff_code,
      adjustment: invoice?.adjustment ?? 0,
      training_start_date: student.training_start_date,
      parent_name: parent?.full_name ?? null,
      parent_phone: parent?.phone ?? student.parent_phone,
      makeups: makeups.count,
      invoice: invoice ? {
        id: invoice.id,
        amount: invoice.amount,
        status: invoiceStatus(invoice, today),
        tariff_code: invoice.tariff_code,
        is_partial: Boolean(invoice.is_partial),
        lessons_count: invoice.lessons_count,
        stored_status: invoice.status,
        period_start: invoice.period_start,
        due_date: invoice.due_date,
      } : null,
      schedule: schedule.map((row) => ({
        weekday: row.weekday,
        time: row.time,
        location_id: row.location_id,
        group_id: row.group_id,
        location: row.location,
        group: row.group_name,
      })),
      history,
    },
  };
}

function parseStaffSchedule(rows) {
  if (!Array.isArray(rows)) return [];
  return rows.map((row) => {
    const weekday = Number(row.weekday);
    if (!Number.isInteger(weekday) || weekday < 0 || weekday > 6 || !validTime(row.time)) {
      throw new ApiError(400, 'invalid_schedule', 'Проверьте дни и время');
    }
    return { weekday, time: row.time, location_id: optionalInt(row.location_id), group_id: optionalInt(row.group_id) };
  });
}

function parseStudentFields(body) {
  const fields = body.student || {};
  const name = text(fields.name);
  if (name.length < 2) throw new ApiError(400, 'name_required', 'Укажите имя ученика');
  const birthday = fields.birthday || null;
  if (birthday && !validDate(birthday)) throw new ApiError(400, 'invalid_date', 'Проверьте дату рождения');
  const startDate = fields.training_start_date || null;
  if (startDate && !validDate(startDate)) throw new ApiError(400, 'invalid_date', 'Проверьте дату начала');
  const tariffCode = fields.tariff_code ? String(fields.tariff_code) : null;
  if (tariffCode && !isMonthlyTariff(tariffCode)) throw new ApiError(400, 'invalid_tariff', 'Такого абонемента нет');
  return {
    name,
    nickname: text(fields.nickname, 50) || null,
    phone: text(fields.phone, 50) || null,
    parent_phone: text(fields.parent_phone, 50) || null,
    birthday,
    notes: text(fields.notes, 2000) || null,
    training_start_date: startDate,
    tariff_code: tariffCode,
    coach_id: optionalInt(fields.coach_id),
  };
}

async function resolveCoachId(ctx, coach, requested) {
  if (!coach.can_admin || !requested) return coach.id;
  const target = await first(ctx.db, 'SELECT id FROM coaches WHERE id = ? AND is_active = 1', requested);
  if (!target) throw new ApiError(404, 'coach_not_found', 'Тренер не найден');
  return target.id;
}

// The chosen subscription is kept for the next months; the current month follows it only while unpaid.
async function applyTariff(db, student, code) {
  if (!code) return;
  await run(db, 'UPDATE students SET tariff_code = ? WHERE id = ?', code, student.id);
  const today = minskNow().date;
  const invoice = await ensureInvoice(db, student, today);
  if (!invoice || invoice.is_partial || !['pending', 'rejected'].includes(invoice.status)) return;
  const tariff = tariffOf(code);
  await run(db, 'UPDATE payments SET tariff_code = ?, amount = ?, lessons_count = ? WHERE id = ?', code, tariff.price, tariff.lessons, invoice.id);
}

export async function createStudent(ctx) {
  const coach = requireCoach(ctx.actor);
  const fields = parseStudentFields(ctx.body);
  const schedule = parseStaffSchedule(ctx.body.schedule);
  const coachId = await resolveCoachId(ctx, coach, fields.coach_id);
  const id = await insert(
    ctx.db,
    `INSERT INTO students (coach_id, name, nickname, phone, parent_phone, birthday, notes, training_start_date, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    coachId,
    fields.name,
    fields.nickname,
    fields.phone,
    fields.parent_phone,
    fields.birthday,
    fields.notes,
    fields.training_start_date,
    isoNow(),
  );
  await replaceSchedule(ctx.db, id, schedule);
  await applyTariff(ctx.db, await first(ctx.db, 'SELECT * FROM students WHERE id = ?', id), fields.tariff_code);
  await audit(ctx.db, ctx.actor.user.id, 'student_created', 'student', id);
  return { success: true, id };
}

export async function updateStudent(ctx) {
  const coach = requireCoach(ctx.actor);
  const student = await visibleStudent(ctx, coach, ctx.params.id);
  const fields = parseStudentFields(ctx.body);
  const schedule = parseStaffSchedule(ctx.body.schedule);
  const coachId = coach.can_admin && fields.coach_id ? await resolveCoachId(ctx, coach, fields.coach_id) : student.coach_id;
  const scheduleKey = (rows) => rows.map((row) => `${row.weekday}|${row.time}`).sort().join(',');
  const oldSchedule = (await schedulesFor(ctx.db, [student.id])).get(student.id);
  await run(
    ctx.db,
    `UPDATE students SET coach_id = ?, name = ?, nickname = ?, phone = ?, parent_phone = ?, birthday = ?, notes = ?,
       training_start_date = ? WHERE id = ?`,
    coachId,
    fields.name,
    fields.nickname,
    fields.phone,
    fields.parent_phone,
    fields.birthday,
    fields.notes,
    fields.training_start_date,
    student.id,
  );
  await replaceSchedule(ctx.db, student.id, schedule);
  const updated = await first(ctx.db, 'SELECT * FROM students WHERE id = ?', student.id);
  if (student.training_start_date !== updated.training_start_date || scheduleKey(oldSchedule) !== scheduleKey(schedule)) {
    await refreshFirstInvoice(ctx.db, updated, minskNow().date);
  }
  await applyTariff(ctx.db, updated, fields.tariff_code);
  await audit(ctx.db, ctx.actor.user.id, 'student_updated', 'student', student.id);
  return { success: true };
}

// The admin's direct edit of the current month: subscription, price, payment and lessons left.
// It is for setting up children who trained before the bot and for correcting mistakes.
export async function editMonth(ctx) {
  const coach = requireAdmin(ctx.actor);
  const { db, body } = ctx;
  const student = await first(db, 'SELECT * FROM students WHERE id = ? AND deleted_at IS NULL', positiveInt(ctx.params.id));
  if (!student) throw new ApiError(404, 'student_not_found', 'Ученик не найден');
  const today = minskNow().date;
  const invoice = await ensureInvoice(db, student, today);
  if (!invoice) throw new ApiError(409, 'no_invoice', 'В этом месяце занятий нет');
  const isPartial = body.tariff_code === 'partial';
  if (!isPartial && !isMonthlyTariff(body.tariff_code)) throw new ApiError(400, 'invalid_tariff', 'Выберите абонемент');
  const lessons = isPartial ? Number(body.lessons) : tariffOf(body.tariff_code).lessons;
  if (!Number.isInteger(lessons) || lessons < 1 || lessons > 31) throw new ApiError(400, 'invalid_lessons', 'Укажите число занятий от 1 до 31');
  const amount = Number(body.amount);
  if (!Number.isInteger(amount) || amount < 0 || amount > 2000) throw new ApiError(400, 'invalid_amount', 'Укажите сумму в рублях');
  const left = body.lessons_left === '' || body.lessons_left == null ? null : Number(body.lessons_left);
  if (left !== null && (!Number.isInteger(left) || left < 0 || left > 31)) throw new ApiError(400, 'invalid_left', 'Укажите, сколько занятий осталось');
  const code = isPartial ? coveringTariff(lessons) : String(body.tariff_code);

  await run(
    db,
    'UPDATE payments SET tariff_code = ?, lessons_count = ?, is_partial = ?, amount = ? WHERE id = ?',
    code,
    lessons,
    isPartial ? 1 : 0,
    amount,
    invoice.id,
  );
  if (!isPartial) await run(db, 'UPDATE students SET tariff_code = ? WHERE id = ?', code, student.id);
  if (body.paid === true && !['paid', 'written_off'].includes(invoice.status)) {
    await run(db, `UPDATE payments SET method = COALESCE(method, 'cash') WHERE id = ?`, invoice.id);
    await markPaid(db, { ...invoice, amount }, student, coach.id, coach.id);
  } else if (body.paid === false && invoice.status === 'paid') {
    await run(db, `UPDATE payments SET status = 'pending', paid_at = NULL WHERE id = ?`, invoice.id);
  }
  // Lessons left are stored as a correction on top of what the bot counts from attendance.
  if (left !== null) {
    const balance = await balanceOf(db, student.id, today);
    if (balance.left !== left) await run(db, 'UPDATE payments SET adjustment = adjustment + ? WHERE id = ?', left - balance.left, invoice.id);
  }
  await audit(db, ctx.actor.user.id, 'month_edited', 'student', student.id, {
    month: invoice.period_start,
    before: { tariff: invoice.tariff_code, lessons: invoice.lessons_count, partial: invoice.is_partial, amount: invoice.amount, status: invoice.status },
    after: { tariff: code, lessons, partial: isPartial, amount, paid: body.paid, left },
  });
  return { success: true };
}

// A manual correction of the month's balance, for mistakes the attendance cannot fix.
export async function adjustLessons(ctx) {
  requireAdmin(ctx.actor);
  const { db, body } = ctx;
  const student = await first(db, 'SELECT * FROM students WHERE id = ? AND deleted_at IS NULL', positiveInt(ctx.params.id));
  if (!student) throw new ApiError(404, 'student_not_found', 'Ученик не найден');
  const delta = Number(body.delta);
  const reason = text(body.reason, 300);
  if (!Number.isInteger(delta) || !delta || Math.abs(delta) > 31) throw new ApiError(400, 'invalid_delta', 'Укажите, на сколько занятий изменить остаток');
  if (!reason) throw new ApiError(400, 'reason_required', 'Укажите причину');
  const invoice = await ensureInvoice(db, student, minskNow().date);
  if (!invoice) throw new ApiError(409, 'no_invoice', 'В этом месяце занятий нет');
  await run(db, 'UPDATE payments SET adjustment = adjustment + ? WHERE id = ?', delta, invoice.id);
  await audit(db, ctx.actor.user.id, 'lessons_adjusted', 'student', student.id, { delta, reason, month: invoice.period_start });
  return { success: true };
}

// Removes personal data but keeps paid amounts so past revenue stays correct.
export async function deleteStudent(ctx) {
  requireAdmin(ctx.actor);
  const { db } = ctx;
  const id = positiveInt(ctx.params.id);
  const student = await first(db, 'SELECT id FROM students WHERE id = ? AND deleted_at IS NULL', id);
  if (!student) throw new ApiError(404, 'student_not_found', 'Ученик не найден');
  await db.batch([
    db.prepare(
      `UPDATE students SET name = 'Удалённый ученик', nickname = NULL, phone = NULL, parent_phone = NULL, birthday = NULL, notes = NULL,
         parent_id = NULL, is_active = 0, deleted_at = ? WHERE id = ?`,
    ).bind(isoNow(), id),
    db.prepare('DELETE FROM schedules WHERE student_id = ?').bind(id),
    db.prepare('DELETE FROM makeups WHERE student_id = ?').bind(id),
    db.prepare('DELETE FROM attendance WHERE student_id = ?').bind(id),
    db.prepare('DELETE FROM training_responses WHERE student_id = ?').bind(id),
    db.prepare('DELETE FROM schedule_requests WHERE student_id = ?').bind(id),
    db.prepare(`DELETE FROM payments WHERE student_id = ? AND status != 'paid'`).bind(id),
    db.prepare('UPDATE payments SET receipt_file_id = NULL WHERE student_id = ?').bind(id),
    db.prepare('UPDATE registration_requests SET student_id = NULL WHERE student_id = ?').bind(id),
  ]);
  await audit(db, ctx.actor.user.id, 'student_deleted', 'student', id);
  return { success: true };
}

export async function requests(ctx) {
  const coach = requireCoach(ctx.actor);
  const { db } = ctx;
  const registrations = await all(
    db,
    `SELECT r.*, p.full_name AS parent_name, p.phone AS parent_phone FROM registration_requests r
     JOIN parents p ON p.id = r.parent_id WHERE r.status = 'pending' ORDER BY r.id`,
  );
  const makeups = await all(
    db,
    `SELECT m.id, m.requested_date, m.requested_time, m.expires_at, s.name AS student_name
     FROM makeups m JOIN students s ON s.id = m.student_id
     WHERE m.status = 'requested'${scopeSql(coach)} ORDER BY m.requested_date`,
  );
  const scheduleChanges = await all(
    db,
    `SELECT r.id, r.student_id, r.proposed_schedule, s.name AS student_name
     FROM schedule_requests r JOIN students s ON s.id = r.student_id
     WHERE r.status = 'pending'${scopeSql(coach)} ORDER BY r.id`,
  );
  const current = await schedulesFor(db, scheduleChanges.map((item) => item.student_id));
  const payments = coach.can_admin
    ? await all(
        db,
        `SELECT p.id, p.kind, p.tariff_code, p.amount, p.method, p.period_start, p.receipt_file_id, s.name AS student_name
         FROM payments p JOIN students s ON s.id = p.student_id
         WHERE p.status = 'reported' ORDER BY p.period_start, p.id`,
      )
    : [];
  const linkable = await all(
    db,
    `SELECT s.id, s.name FROM students s WHERE s.parent_id IS NULL AND s.is_active = 1 AND s.deleted_at IS NULL${scopeSql(coach)} ORDER BY s.name`,
  );
  const options = coachOptions(ctx.env);
  const accounts = await all(db, 'SELECT id, username FROM coaches WHERE is_active = 1');
  const sameName = (left, right) => Boolean(left && right) && left.toLowerCase() === right.toLowerCase();
  return {
    registrations: registrations.map((item) => ({
      id: item.id,
      child_name: item.child_name,
      child_nickname: item.child_nickname,
      child_birthday: item.child_birthday,
      preferred_tariff: item.preferred_tariff,
      proposed_days: JSON.parse(item.proposed_schedule).length,
      preferred_coach_name: options.find((option) => sameName(option.username, item.preferred_coach))?.name ?? null,
      preferred_coach_id: accounts.find((account) => sameName(account.username, item.preferred_coach))?.id ?? null,
      parent_name: item.parent_name,
      parent_phone: item.parent_phone,
      proposed_schedule: JSON.parse(item.proposed_schedule),
    })),
    makeups,
    schedule_changes: scheduleChanges.map((item) => ({
      id: item.id,
      student_name: item.student_name,
      proposed_schedule: JSON.parse(item.proposed_schedule),
      current_schedule: current.get(item.student_id).map((row) => ({ weekday: row.weekday, time: row.time })),
    })),
    payments: payments.map((item) => ({
      id: item.id,
      kind: item.kind,
      amount: item.amount,
      method: item.method,
      period_start: item.period_start,
      student_name: item.student_name,
      has_receipt: Boolean(item.receipt_file_id),
    })),
    linkable_students: linkable,
    ...(await loadResources(db, coach)),
  };
}

function rejectionReason(body) {
  const reason = text(body.reason, 500);
  if (!reason) throw new ApiError(400, 'reason_required', 'Укажите причину');
  return reason;
}

export async function reviewRegistration(ctx) {
  const coach = requireCoach(ctx.actor);
  const { db, body, env } = ctx;
  const request = await first(
    db,
    `SELECT r.*, p.telegram_id, p.phone AS parent_phone FROM registration_requests r JOIN parents p ON p.id = r.parent_id
     WHERE r.id = ? AND r.status = 'pending'`,
    positiveInt(ctx.params.id),
  );
  if (!request) throw new ApiError(404, 'request_not_found', 'Заявка уже разобрана');
  if (body.decision === 'reject') {
    const reason = rejectionReason(body);
    await run(
      db,
      `UPDATE registration_requests SET status = 'rejected', rejection_reason = ?, reviewed_by_coach_id = ? WHERE id = ?`,
      reason,
      coach.id,
      request.id,
    );
    await enqueue(env, request.telegram_id, `Анкета на ${request.child_name} отклонена.\nПричина: ${reason}`, { buttonLabel: 'Открыть кабинет' });
    return { success: true, status: 'rejected' };
  }
  if (body.decision !== 'approve') throw new ApiError(400, 'invalid_decision', 'Неверное решение');
  const startDate = body.start_date;
  if (!validDate(startDate)) throw new ApiError(400, 'invalid_date', 'Укажите дату начала');
  const groupId = optionalInt(body.group_id);
  const locationId = optionalInt(body.location_id);
  const proposed = parseScheduleRows(JSON.parse(request.proposed_schedule));
  const schedule = proposed.map((row) => ({ ...row, group_id: groupId, location_id: locationId }));
  const coachId = await resolveCoachId(ctx, coach, optionalInt(body.coach_id));
  const tariffCode = isMonthlyTariff(body.tariff_code) ? String(body.tariff_code)
    : isMonthlyTariff(request.preferred_tariff) ? request.preferred_tariff : null;

  let studentId = optionalInt(body.link_student_id);
  if (studentId) {
    const existing = await first(
      db,
      `SELECT id FROM students s WHERE s.id = ? AND s.parent_id IS NULL AND s.is_active = 1${scopeSql(coach)}`,
      studentId,
    );
    if (!existing) throw new ApiError(404, 'student_not_found', 'Ученик не найден');
    await run(
      db,
      `UPDATE students SET parent_id = ?, parent_phone = ?, birthday = COALESCE(birthday, ?), nickname = COALESCE(nickname, ?),
         tariff_code = COALESCE(?, tariff_code) WHERE id = ?`,
      request.parent_id,
      request.parent_phone,
      request.child_birthday,
      request.child_nickname,
      tariffCode,
      studentId,
    );
  } else {
    studentId = await insert(
      db,
      `INSERT INTO students (coach_id, parent_id, name, nickname, phone, parent_phone, birthday, training_start_date, tariff_code, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      coachId,
      request.parent_id,
      request.child_name,
      request.child_nickname,
      request.child_phone,
      request.parent_phone,
      request.child_birthday,
      startDate,
      tariffCode,
      isoNow(),
    );
    await replaceSchedule(db, studentId, schedule);
  }
  await run(
    db,
    `UPDATE registration_requests SET status = 'approved', student_id = ?, rejection_reason = NULL, reviewed_by_coach_id = ? WHERE id = ?`,
    studentId,
    coach.id,
    request.id,
  );
  await enqueue(env, request.telegram_id, `Анкета на ${request.child_name} подтверждена. Начало занятий: ${formatDate(startDate)}.`, { buttonLabel: 'Открыть кабинет' });
  await audit(db, ctx.actor.user.id, 'registration_approved', 'student', studentId);
  return { success: true, status: 'approved', student_id: studentId };
}

export async function reviewMakeup(ctx) {
  const coach = requireCoach(ctx.actor);
  const { db, body, env } = ctx;
  const credit = await first(
    db,
    `SELECT m.*, s.name AS student_name, s.parent_id FROM makeups m JOIN students s ON s.id = m.student_id
     WHERE m.id = ? AND m.status = 'requested'${scopeSql(coach)}`,
    positiveInt(ctx.params.id),
  );
  if (!credit) throw new ApiError(404, 'request_not_found', 'Заявка уже разобрана');
  const student = { parent_id: credit.parent_id };
  if (body.decision === 'reject') {
    const reason = rejectionReason(body);
    await run(db, `UPDATE makeups SET status = 'available', rejection_reason = ? WHERE id = ?`, reason, credit.id);
    await notifyParentOfStudent(env, student, `Отработка для ${credit.student_name} не подтверждена.\nПричина: ${reason}`);
    return { success: true, status: 'available' };
  }
  if (body.decision !== 'approve') throw new ApiError(400, 'invalid_decision', 'Неверное решение');
  const date = body.scheduled_date || credit.requested_date;
  const time = body.scheduled_time || credit.requested_time;
  if (!validDate(date) || !validTime(time)) throw new ApiError(400, 'date_required', 'Укажите дату и время');
  if (date > credit.expires_at) throw new ApiError(400, 'after_expiry', 'Эта дата позже срока отработки');
  await run(
    db,
    `UPDATE makeups SET status = 'scheduled', scheduled_date = ?, scheduled_time = ?, location_id = ?, group_id = ?, rejection_reason = NULL WHERE id = ?`,
    date,
    time,
    optionalInt(body.location_id) ?? credit.requested_location_id,
    optionalInt(body.group_id) ?? credit.requested_group_id,
    credit.id,
  );
  await notifyParentOfStudent(env, student, `Отработка для ${credit.student_name} подтверждена: ${formatDate(date)} в ${time}.`);
  await audit(db, ctx.actor.user.id, 'makeup_approved', 'makeup', credit.id);
  return { success: true, status: 'scheduled' };
}

export async function reviewSchedule(ctx) {
  const coach = requireCoach(ctx.actor);
  const { db, body, env } = ctx;
  const request = await first(
    db,
    `SELECT r.*, s.name AS student_name, s.parent_id FROM schedule_requests r JOIN students s ON s.id = r.student_id
     WHERE r.id = ? AND r.status = 'pending'${scopeSql(coach)}`,
    positiveInt(ctx.params.id),
  );
  if (!request) throw new ApiError(404, 'request_not_found', 'Заявка уже разобрана');
  const student = { parent_id: request.parent_id };
  if (body.decision === 'reject') {
    const reason = rejectionReason(body);
    await run(
      db,
      `UPDATE schedule_requests SET status = 'rejected', rejection_reason = ?, reviewed_by_coach_id = ? WHERE id = ?`,
      reason,
      coach.id,
      request.id,
    );
    await notifyParentOfStudent(env, student, `Новое расписание для ${request.student_name} не подтверждено.\nПричина: ${reason}`);
    return { success: true, status: 'rejected' };
  }
  if (body.decision !== 'approve') throw new ApiError(400, 'invalid_decision', 'Неверное решение');
  const current = (await schedulesFor(db, [request.student_id])).get(request.student_id);
  const keep = { location_id: current[0]?.location_id ?? null, group_id: current[0]?.group_id ?? null };
  const rows = parseScheduleRows(JSON.parse(request.proposed_schedule)).map((row) => ({ ...row, ...keep }));
  await replaceSchedule(db, request.student_id, rows);
  await run(db, `UPDATE schedule_requests SET status = 'approved', reviewed_by_coach_id = ? WHERE id = ?`, coach.id, request.id);
  await refreshFirstInvoice(db, await first(db, 'SELECT * FROM students WHERE id = ?', request.student_id), minskNow().date);
  await notifyParentOfStudent(env, student, `Новое расписание для ${request.student_name} подтверждено.`);
  await audit(db, ctx.actor.user.id, 'schedule_approved', 'student', request.student_id);
  return { success: true, status: 'approved' };
}

export async function reviewPayment(ctx) {
  const coach = requireAdmin(ctx.actor);
  const { db, body, env } = ctx;
  const payment = await first(db, 'SELECT * FROM payments WHERE id = ?', positiveInt(ctx.params.id));
  if (!payment) throw new ApiError(404, 'payment_not_found', 'Счёт не найден');
  if (payment.status !== 'reported') throw new ApiError(409, 'payment_not_reported', 'Эта оплата не на проверке');
  const student = await first(db, 'SELECT * FROM students WHERE id = ?', payment.student_id);
  if (body.decision === 'reject') {
    const reason = rejectionReason(body);
    await run(
      db,
      `UPDATE payments SET status = 'rejected', rejection_reason = ?, reported_at = NULL, receipt_file_id = NULL WHERE id = ?`,
      reason,
      payment.id,
    );
    await notifyParentOfStudent(env, student, `Оплата для ${student.name} отклонена.\nПричина: ${reason}`);
    return { success: true, status: 'rejected' };
  }
  if (body.decision !== 'approve') throw new ApiError(400, 'invalid_decision', 'Неверное решение');
  if (payment.method === 'online' && !payment.receipt_file_id) {
    throw new ApiError(409, 'receipt_required', 'Для онлайн-оплаты нужен чек');
  }
  if (payment.kind === 'subscription') {
    const oldest = await oldestOpenInvoice(db, payment.student_id);
    if (oldest && oldest.id !== payment.id) {
      throw new ApiError(409, 'oldest_debt_first', 'Сначала подтвердите самый ранний неоплаченный месяц');
    }
  }
  let receivedBy = null;
  if (payment.method === 'cash') {
    receivedBy = optionalInt(body.received_by_coach_id) ?? coach.id;
    const receiver = await first(db, 'SELECT id FROM coaches WHERE id = ? AND is_active = 1', receivedBy);
    if (!receiver) throw new ApiError(404, 'receiver_not_found', 'Сотрудник не найден');
  }
  const left = await markPaid(db, payment, student, coach.id, receivedBy);
  await notifyParentOfStudent(env, student, `Оплата для ${student.name} подтверждена.\nСумма: ${payment.amount} Br. Осталось занятий: ${left}.`);
  await audit(db, ctx.actor.user.id, 'payment_approved', 'payment', payment.id, { amount: payment.amount });
  return { success: true, status: 'paid', amount: payment.amount };
}

export async function viewReceipt(ctx) {
  requireAdmin(ctx.actor);
  const payment = await first(ctx.db, 'SELECT receipt_file_id FROM payments WHERE id = ?', positiveInt(ctx.params.id));
  if (!payment?.receipt_file_id) throw new ApiError(404, 'receipt_not_found', 'Чека нет');
  const file = await callTelegram(ctx.env, 'getFile', { file_id: payment.receipt_file_id });
  const response = await fetch(`https://api.telegram.org/file/bot${ctx.env.BOT_TOKEN}/${file.file_path}`);
  if (!response.ok) throw new ApiError(502, 'receipt_unavailable', 'Не удалось загрузить чек');
  const extension = file.file_path.split('.').pop().toLowerCase();
  const types = { jpg: 'image/jpeg', jpeg: 'image/jpeg', png: 'image/png', webp: 'image/webp', heic: 'image/heic', pdf: 'application/pdf' };
  return new Response(response.body, {
    headers: {
      'Content-Type': types[extension] || 'application/octet-stream',
      'Cache-Control': 'no-store',
      'X-Content-Type-Options': 'nosniff',
    },
  });
}

export async function money(ctx) {
  requireAdmin(ctx.actor);
  const { db } = ctx;
  const today = minskNow().date;
  await ensureAllInvoices(db, today);
  const { start, end } = monthBounds(ctx.body.month && validDate(`${ctx.body.month}-01`) ? `${ctx.body.month}-01` : today);
  const collected = await first(
    db,
    `SELECT COALESCE(SUM(amount), 0) AS total FROM payments WHERE status = 'paid' AND period_start BETWEEN ? AND ?`,
    start,
    end,
  );
  const open = await all(
    db,
    `SELECT p.*, s.name AS student_name, s.parent_id FROM payments p JOIN students s ON s.id = p.student_id
     WHERE p.status NOT IN ('paid', 'written_off', 'skipped') AND s.is_active = 1 AND s.deleted_at IS NULL
     ORDER BY p.period_start, s.name`,
  );
  const debtors = open
    .filter((item) => item.status !== 'reported')
    .map((item) => ({
      id: item.id,
      kind: item.kind,
      student_name: item.student_name,
      amount: item.amount,
      status: invoiceStatus(item, today),
      not_training: item.kind === 'subscription' && !item.attended && item.due_date < today,
      period_start: item.period_start,
      due_date: item.due_date,
      can_remind: Boolean(item.parent_id),
    }));
  return {
    month: start.slice(0, 7),
    collected: collected.total,
    waiting: open.reduce((sum, item) => sum + item.amount, 0),
    in_review: open.filter((item) => item.status === 'reported').length,
    debtors,
  };
}

export async function takeCash(ctx) {
  const coach = requireAdmin(ctx.actor);
  const { db, env, body } = ctx;
  const payment = await first(db, 'SELECT * FROM payments WHERE id = ?', positiveInt(ctx.params.id));
  if (!payment) throw new ApiError(404, 'payment_not_found', 'Счёт не найден');
  if (['paid', 'written_off'].includes(payment.status)) throw new ApiError(409, 'already_paid', 'Этот месяц уже оплачен');
  if (payment.status === 'skipped') throw new ApiError(409, 'month_closed', 'Этот месяц закрыт: занятий не было');
  if (payment.kind === 'subscription') {
    const oldest = await oldestOpenInvoice(db, payment.student_id);
    if (oldest && oldest.id !== payment.id) {
      throw new ApiError(409, 'oldest_debt_first', 'Сначала закройте самый ранний неоплаченный месяц');
    }
  }
  const receivedBy = optionalInt(body.received_by_coach_id) ?? coach.id;
  await run(db, `UPDATE payments SET method = 'cash' WHERE id = ?`, payment.id);
  const student = await first(db, 'SELECT * FROM students WHERE id = ?', payment.student_id);
  const left = await markPaid(db, payment, student, coach.id, receivedBy);
  await notifyParentOfStudent(env, student, `Оплата для ${student.name} подтверждена.\nСумма: ${payment.amount} Br. Осталось занятий: ${left}.`);
  await audit(db, ctx.actor.user.id, 'cash_taken', 'payment', payment.id, { amount: payment.amount });
  return { success: true };
}

export async function remindPayment(ctx) {
  requireAdmin(ctx.actor);
  const { db, env } = ctx;
  const payment = await first(db, `SELECT * FROM payments WHERE id = ? AND status NOT IN ('paid', 'written_off', 'skipped')`, positiveInt(ctx.params.id));
  if (!payment) throw new ApiError(404, 'payment_not_found', 'Счёт не найден');
  const student = await first(db, 'SELECT * FROM students WHERE id = ?', payment.student_id);
  await notifyParentOfStudent(
    env,
    student,
    `Напоминание об оплате\n${student.name}: ${payment.amount} Br, срок до ${formatDate(payment.due_date)}.`,
    `manual_reminder:${payment.id}:${minskNow().date}`,
  );
  return { success: true };
}

export async function team(ctx) {
  requireAdmin(ctx.actor);
  const coaches = await all(ctx.db, 'SELECT * FROM coaches WHERE is_active = 1 ORDER BY first_name');
  const superIds = String(ctx.env.ADMIN_IDS || '').split(/[,;]/).map((value) => Number(value.trim()));
  const me = await callTelegram(ctx.env, 'getMe').catch(() => null);
  const { locations, groups } = await loadResources(ctx.db, ctx.actor.coach);
  return {
    coaches: coaches.map((coach) => ({
      id: coach.id,
      name: coach.first_name || coach.username || `Тренер ${coach.id}`,
      is_admin: Boolean(coach.is_admin),
      is_manager: Boolean(coach.is_manager),
      locked: superIds.includes(coach.telegram_id),
    })),
    registration_link: me?.username ? `https://t.me/${me.username}?start=registration` : `${ctx.env.WEBAPP_URL}/`,
    locations,
    groups,
  };
}

export async function setRoles(ctx) {
  requireAdmin(ctx.actor);
  const { db, body } = ctx;
  const target = await first(db, 'SELECT * FROM coaches WHERE id = ? AND is_active = 1', positiveInt(ctx.params.id));
  if (!target) throw new ApiError(404, 'coach_not_found', 'Сотрудник не найден');
  const superIds = String(ctx.env.ADMIN_IDS || '').split(/[,;]/).map((value) => Number(value.trim()));
  if (superIds.includes(target.telegram_id)) throw new ApiError(409, 'role_locked', 'Роли этого аккаунта менять нельзя');
  await run(db, 'UPDATE coaches SET is_admin = ?, is_manager = ? WHERE id = ?', body.is_admin ? 1 : 0, body.is_manager ? 1 : 0, target.id);
  await audit(db, ctx.actor.user.id, 'roles_changed', 'coach', target.id, { is_admin: Boolean(body.is_admin), is_manager: Boolean(body.is_manager) });
  return { success: true };
}

function resourceHandlers(table) {
  return {
    async create(ctx) {
      requireAdmin(ctx.actor);
      const name = text(ctx.body.name, 100);
      if (name.length < 1) throw new ApiError(400, 'name_required', 'Укажите название');
      const id = await insert(ctx.db, `INSERT INTO ${table} (name) VALUES (?)`, name);
      return { success: true, id };
    },
    async remove(ctx) {
      requireAdmin(ctx.actor);
      await run(ctx.db, `UPDATE ${table} SET is_active = 0 WHERE id = ?`, positiveInt(ctx.params.id));
      return { success: true };
    },
  };
}

export const locationHandlers = resourceHandlers('locations');
export const groupHandlers = resourceHandlers('training_groups');
