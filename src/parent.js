import { coachOptions, requireParent } from './auth.js';
import { all, audit, first, insert, run } from './db.js';
import { balancesFor, ensureInvoice, oldestOpenInvoice, schedulesFor, tariffOf } from './domain.js';
import { ApiError, optionalInt, positiveInt, text } from './http.js';
import {
  addDays,
  canSelectTariff,
  countTrainings,
  daysBetween,
  invoiceStatus,
  isMonthlyTariff,
  isoNow,
  minskNow,
  monthBounds,
  nextTariff,
  TARIFFS,
  validDate,
  validTime,
  weekdayOf,
} from './rules.js';
import { notifyAdmins, storeReceipt, TelegramError } from './telegram.js';

const MAKEUP_SLOT_DAYS = 21;
const MAKEUP_SLOT_LIMIT = 8;
const RECEIPT_MAX_BYTES = 10 * 1024 * 1024;
const RECEIPT_EXTENSIONS = {
  'image/jpeg': 'jpg',
  'image/png': 'png',
  'image/webp': 'webp',
  'image/heic': 'heic',
  'application/pdf': 'pdf',
};

export function parseScheduleRows(rows) {
  if (!Array.isArray(rows) || !rows.length || rows.length > 14) {
    throw new ApiError(400, 'schedule_required', 'Укажите хотя бы один день');
  }
  return rows.map((row) => {
    const weekday = Number(row.weekday);
    if (!Number.isInteger(weekday) || weekday < 0 || weekday > 6 || !validTime(row.time)) {
      throw new ApiError(400, 'invalid_schedule', 'Проверьте дни и время');
    }
    return { weekday, time: row.time };
  });
}

function invoiceLabel(payment) {
  if (payment.kind === 'single') return 'Разовое занятие';
  if (payment.kind === 'upgrade') return `Доплата до абонемента на ${tariffOf(payment.tariff_code).lessons} занятий`;
  if (payment.is_partial) return `Занятий до конца месяца: ${payment.lessons_count}`;
  return tariffOf(payment.tariff_code)?.label ?? 'Абонемент';
}

function invoiceJson(payment, today) {
  return {
    id: payment.id,
    kind: payment.kind,
    is_partial: Boolean(payment.is_partial),
    label: invoiceLabel(payment),
    lessons_count: payment.lessons_count,
    tariff_code: payment.tariff_code,
    tariff: tariffOf(payment.tariff_code),
    amount: payment.amount,
    status: invoiceStatus(payment, today),
    stored_status: payment.status,
    payment_method: payment.method,
    rejection_reason: payment.rejection_reason,
    period_start: payment.period_start,
    period_end: payment.period_end,
    due_date: payment.due_date,
    paid_at: payment.paid_at,
  };
}

// What a parent may do next when the month has more trainings than lessons bought.
function upgradeOffer(invoice, paidUpgrades) {
  if (!invoice || invoice.is_partial || invoice.status !== 'paid') return null;
  const current = paidUpgrades.reduce((best, item) => (tariffOf(item.tariff_code).lessons > tariffOf(best).lessons ? item.tariff_code : best), invoice.tariff_code);
  const code = nextTariff(current);
  if (!code) return null;
  return {
    tariff_code: code,
    lessons: tariffOf(code).lessons,
    extra_lessons: tariffOf(code).lessons - tariffOf(current).lessons,
    amount: tariffOf(code).price - tariffOf(current).price,
  };
}

function nextTraining(schedule, now) {
  for (let offset = 0; offset < 14; offset += 1) {
    const date = addDays(now.date, offset);
    const weekday = weekdayOf(date);
    for (const row of schedule) {
      if (row.weekday === weekday && (offset > 0 || row.time > now.time)) {
        return { date, time: row.time, location_id: row.location_id, location: row.location, group: row.group_name };
      }
    }
  }
  return null;
}

async function makeupSlots(db, student, schedule, today, until) {
  const rows = await all(
    db,
    `SELECT DISTINCT sc.weekday, sc.time, sc.location_id, sc.group_id, l.name AS location, g.name AS group_name
     FROM schedules sc
     JOIN students s ON s.id = sc.student_id
     LEFT JOIN locations l ON l.id = sc.location_id
     LEFT JOIN training_groups g ON g.id = sc.group_id
     WHERE s.coach_id = ? AND s.is_active = 1 AND s.deleted_at IS NULL`,
    student.coach_id,
  );
  const lastDay = [until, addDays(today, MAKEUP_SLOT_DAYS)].sort()[0];
  const own = new Set(schedule.map((row) => `${row.weekday}|${row.time}`));
  const datesFor = (weekly) => {
    const slots = [];
    for (let date = addDays(today, 1); date <= lastDay; date = addDays(date, 1)) {
      const weekday = weekdayOf(date);
      for (const row of weekly) {
        if (row.weekday === weekday && !own.has(`${row.weekday}|${row.time}`)) {
          slots.push({
            date,
            time: row.time,
            location_id: row.location_id,
            location: row.location,
            group_id: row.group_id,
            group: row.group_name,
          });
        }
      }
    }
    return slots.sort((a, b) => `${a.date}${a.time}`.localeCompare(`${b.date}${b.time}`)).slice(0, MAKEUP_SLOT_LIMIT);
  };
  const ownGroups = new Set(schedule.map((row) => row.group_id).filter(Boolean));
  const sameGroup = datesFor(rows.filter((row) => ownGroups.has(row.group_id)));
  return sameGroup.length ? sameGroup : datesFor(rows);
}

async function ownStudent(ctx, parent, studentId) {
  const student = await first(
    ctx.db,
    'SELECT * FROM students WHERE id = ? AND parent_id = ? AND is_active = 1 AND deleted_at IS NULL',
    positiveInt(studentId),
    parent.id,
  );
  if (!student) throw new ApiError(404, 'student_not_found', 'Ребёнок не найден');
  return student;
}

async function ownPayment(ctx, parent, paymentId) {
  const payment = await first(
    ctx.db,
    `SELECT p.* FROM payments p JOIN students s ON s.id = p.student_id WHERE p.id = ? AND s.parent_id = ?`,
    positiveInt(paymentId),
    parent.id,
  );
  if (!payment) throw new ApiError(404, 'payment_not_found', 'Счёт не найден');
  return payment;
}

export async function context(ctx) {
  const parent = requireParent(ctx.actor);
  const { db } = ctx;
  const now = minskNow();
  const today = now.date;
  const students = await all(
    db,
    'SELECT * FROM students WHERE parent_id = ? AND is_active = 1 AND deleted_at IS NULL ORDER BY name',
    parent.id,
  );
  const schedules = await schedulesFor(db, students.map((student) => student.id));
  for (const student of students) await ensureInvoice(db, student, today);
  const balances = await balancesFor(db, students.map((student) => student.id), today);
  const { end: monthEnd } = monthBounds(today);
  const response = [];
  for (const student of students) {
    const invoice = await ensureInvoice(db, student, today);
    const invoices = await all(
      db,
      `SELECT * FROM payments WHERE student_id = ? AND status != 'written_off' ORDER BY period_start DESC`,
      student.id,
    );
    const oldest = await oldestOpenInvoice(db, student.id);
    const makeups = await all(
      db,
      `SELECT * FROM makeups WHERE student_id = ? AND status IN ('available', 'requested', 'scheduled')
         AND (expires_at >= ? OR status = 'scheduled') ORDER BY expires_at`,
      student.id,
      today,
    );
    const schedule = schedules.get(student.id);
    const training = nextTraining(schedule, now);
    if (training) {
      const saved = await first(
        db,
        'SELECT response FROM training_responses WHERE student_id = ? AND date = ? AND time = ?',
        student.id,
        training.date,
        training.time,
      );
      training.response = saved?.response ?? null;
    }
    const available = makeups.filter((item) => item.status === 'available');
    const balance = balances.get(student.id);
    const monthRows = invoices.filter((item) => item.period_start === monthBounds(today).start);
    const paidUpgrades = monthRows.filter((item) => item.kind === 'upgrade' && item.status === 'paid');
    const trainingsLeft = countTrainings(schedule, addDays(today, 1), monthEnd)
      + schedule.filter((row) => row.weekday === weekdayOf(today) && row.time > now.time).length;
    const offer = upgradeOffer(invoice, paidUpgrades);
    const canUpgrade = Boolean(invoice) && !invoice.is_partial && ['pending', 'rejected'].includes(invoice.status);
    response.push({
      id: student.id,
      name: student.name,
      lessons_remaining: balance.left,
      lessons_count: balance.total,
      lessons_over: balance.over,
      trainings_left: trainingsLeft,
      starts_on: invoice ? null : student.training_start_date,
      upgrade_offer: balance.left < trainingsLeft || balance.over ? offer : null,
      open_extras: invoices
        .filter((item) => item.kind !== 'subscription' && !['paid', 'written_off', 'skipped'].includes(item.status))
        .map((item) => invoiceJson(item, today)),
      training_reminders_enabled: Boolean(student.reminders_enabled),
      next_training: training,
      makeup_slots: available.length ? await makeupSlots(db, student, schedule, today, available[0].expires_at) : [],
      invoice: invoice ? invoiceJson(invoice, today) : null,
      invoices: invoices.map((item) => invoiceJson(item, today)),
      oldest_open_invoice_id: oldest?.id ?? null,
      tariff_change_allowed: canUpgrade,
      tariff_upgrade_only: !canSelectTariff(today),
      schedules: schedule.map((row) => ({
        days: String(row.weekday),
        times: { [row.weekday]: row.time },
        location: row.location,
        group: row.group_name,
      })),
      makeups: makeups.map((item) => ({
        id: item.id,
        source_date: item.source_date,
        source_type: item.source_type,
        expires_at: item.expires_at,
        status: item.status,
        requested_date: item.requested_date,
        requested_time: item.requested_time,
        scheduled_date: item.scheduled_date,
        scheduled_time: item.scheduled_time,
        rejection_reason: item.rejection_reason,
      })),
    });
  }
  const registrations = await all(
    db,
    'SELECT id, child_name, status, rejection_reason FROM registration_requests WHERE parent_id = ? ORDER BY id DESC',
    parent.id,
  );
  const scheduleRequests = await all(
    db,
    `SELECT r.id, r.student_id, r.status, r.rejection_reason FROM schedule_requests r
     JOIN students s ON s.id = r.student_id WHERE s.parent_id = ? ORDER BY r.id DESC LIMIT 10`,
    parent.id,
  );
  return {
    parent: { id: parent.id, full_name: parent.full_name, phone: parent.phone },
    students: response,
    tariffs: TARIFFS,
    payment_details: ctx.env.PAYMENT_DETAILS || '',
    registration_requests: registrations,
    schedule_requests: scheduleRequests,
  };
}

export async function register(ctx) {
  const { db, body, actor } = ctx;
  if (actor.coach) throw new ApiError(409, 'already_staff', 'Этот аккаунт уже работает как тренер');
  const fullName = text(body.parent?.full_name);
  const phone = text(body.parent?.phone, 50);
  if (fullName.length < 2 || phone.length < 5) {
    throw new ApiError(400, 'parent_required', 'Укажите ФИО и телефон родителя');
  }
  const children = Array.isArray(body.children) ? body.children : [];
  if (!children.length || children.length > 10) throw new ApiError(400, 'children_required', 'Добавьте ребёнка');
  const parsed = children.map((child) => {
    const name = text(child.name);
    if (name.length < 2) throw new ApiError(400, 'child_name_required', 'Укажите имя ребёнка');
    const nickname = text(child.nickname, 50);
    if (!nickname) throw new ApiError(400, 'child_nickname_required', 'Укажите никнейм ребёнка');
    if (!validDate(child.birthday) || child.birthday > minskNow().date) {
      throw new ApiError(400, 'child_birthday_required', 'Укажите дату рождения ребёнка');
    }
    const options = coachOptions(ctx.env);
    if (options.length && !options.some((item) => item.username === child.coach)) {
      throw new ApiError(400, 'child_coach_required', 'Выберите тренера');
    }
    return { name, nickname, coach: options.length ? child.coach : null, birthday: child.birthday, phone: text(child.phone, 50) || null, schedule: parseScheduleRows(child.schedule) };
  });

  const parent = await first(
    db,
    `INSERT INTO parents (telegram_id, full_name, phone, created_at) VALUES (?, ?, ?, ?)
     ON CONFLICT (telegram_id) DO UPDATE SET full_name = excluded.full_name, phone = excluded.phone
     RETURNING *`,
    actor.user.id,
    fullName,
    phone,
    isoNow(),
  );
  for (const child of parsed) {
    const duplicate = await first(
      db,
      `SELECT id FROM registration_requests WHERE parent_id = ? AND status = 'pending' AND lower(child_name) = lower(?)`,
      parent.id,
      child.name,
    );
    if (duplicate) throw new ApiError(409, 'registration_exists', `Анкета на ${child.name} уже отправлена`);
  }
  for (const child of parsed) {
    await insert(
      db,
      `INSERT INTO registration_requests (parent_id, child_name, child_nickname, preferred_coach, child_birthday, child_phone, proposed_schedule, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      parent.id,
      child.name,
      child.nickname,
      child.coach,
      child.birthday,
      child.phone,
      JSON.stringify(child.schedule),
      isoNow(),
    );
  }
  await notifyAdmins(ctx.env, `Новая анкета: ${parsed.map((child) => child.name).join(', ')}\nРодитель: ${fullName}`);
  return { success: true, children_count: parsed.length };
}

export async function trainingResponse(ctx) {
  const parent = requireParent(ctx.actor);
  const { db, body } = ctx;
  const student = await ownStudent(ctx, parent, body.student_id);
  if (!validDate(body.date) || !validTime(body.time)) throw new ApiError(400, 'invalid_date', 'Неверная дата');
  if (body.date < minskNow().date) throw new ApiError(409, 'training_passed', 'Эта тренировка уже прошла');
  if (![null, undefined, 'attending', 'absent'].includes(body.response)) {
    throw new ApiError(400, 'invalid_response', 'Неверный ответ');
  }
  const scheduled = await first(
    db,
    'SELECT id FROM schedules WHERE student_id = ? AND weekday = ? AND time = ?',
    student.id,
    weekdayOf(body.date),
    body.time,
  );
  if (!scheduled) throw new ApiError(404, 'training_not_found', 'Такой тренировки нет в расписании');
  await saveTrainingResponse(db, student.id, body.date, body.time, body.response ?? null);
  return { success: true, response: body.response ?? null };
}

export async function saveTrainingResponse(db, studentId, date, time, response) {
  if (response === null) {
    await run(db, 'DELETE FROM training_responses WHERE student_id = ? AND date = ? AND time = ?', studentId, date, time);
    return;
  }
  await run(
    db,
    `INSERT INTO training_responses (student_id, date, time, response, responded_at) VALUES (?, ?, ?, ?, ?)
     ON CONFLICT (student_id, date, time) DO UPDATE SET response = excluded.response, responded_at = excluded.responded_at`,
    studentId,
    date,
    time,
    response,
    isoNow(),
  );
}

export async function reminders(ctx) {
  const parent = requireParent(ctx.actor);
  const student = await ownStudent(ctx, parent, ctx.body.student_id);
  const enabled = Boolean(ctx.body.enabled);
  await run(ctx.db, 'UPDATE students SET reminders_enabled = ? WHERE id = ?', enabled ? 1 : 0, student.id);
  return { success: true, enabled };
}

export async function chooseTariff(ctx) {
  const parent = requireParent(ctx.actor);
  const { db, body } = ctx;
  const student = await ownStudent(ctx, parent, body.student_id);
  if (!isMonthlyTariff(body.tariff_code)) throw new ApiError(400, 'invalid_tariff', 'Такого абонемента нет');
  const tariff = tariffOf(body.tariff_code);
  const today = minskNow().date;
  const invoice = await ensureInvoice(db, student, today);
  if (!invoice) throw new ApiError(409, 'no_invoice', 'В этом месяце занятий нет');
  if (invoice.is_partial) throw new ApiError(409, 'tariff_partial', 'В первый месяц оплачиваются занятия до конца месяца');
  if (!['pending', 'rejected'].includes(invoice.status)) {
    throw new ApiError(409, 'payment_in_review', 'Оплата уже на проверке');
  }
  if (!canSelectTariff(today) && tariff.lessons < invoice.lessons_count) {
    throw new ApiError(409, 'tariff_deadline_passed', 'Уменьшить абонемент можно до 5 числа');
  }
  await run(
    db,
    `UPDATE payments SET tariff_code = ?, amount = ?, lessons_count = ?, status = 'pending', rejection_reason = NULL WHERE id = ?`,
    String(body.tariff_code),
    tariff.price,
    tariff.lessons,
    invoice.id,
  );
  return { success: true };
}

async function openExtra(ctx, student, kind, tariffCode, amount, lessons) {
  const { db } = ctx;
  const today = minskNow().date;
  const { start, end } = monthBounds(today);
  const existing = await first(
    db,
    `SELECT id, status FROM payments WHERE student_id = ? AND period_start = ? AND kind = ? AND status NOT IN ('paid', 'written_off', 'skipped')`,
    student.id,
    start,
    kind,
  );
  if (existing) {
    await run(db, `UPDATE payments SET amount = ?, lessons_count = ?, tariff_code = ? WHERE id = ? AND status IN ('pending', 'rejected')`, amount, lessons, tariffCode, existing.id);
    return { id: existing.id };
  }
  const id = await insert(
    db,
    `INSERT INTO payments (student_id, coach_id, kind, amount, lessons_count, tariff_code, status, period_start, period_end, due_date, created_at)
     VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?)`,
    student.id,
    student.coach_id,
    kind,
    amount,
    lessons,
    tariffCode,
    start,
    end,
    end,
    isoNow(),
  );
  return { id };
}

export async function requestUpgrade(ctx) {
  const parent = requireParent(ctx.actor);
  const { db, body } = ctx;
  const student = await ownStudent(ctx, parent, body.student_id);
  const today = minskNow().date;
  const invoice = await ensureInvoice(db, student, today);
  const paidUpgrades = await all(
    db,
    `SELECT tariff_code FROM payments WHERE student_id = ? AND period_start = ? AND kind = 'upgrade' AND status = 'paid'`,
    student.id,
    monthBounds(today).start,
  );
  const offer = upgradeOffer(invoice, paidUpgrades);
  if (!offer) throw new ApiError(409, 'upgrade_unavailable', 'Перейти на больший абонемент сейчас нельзя');
  return openExtra(ctx, student, 'upgrade', offer.tariff_code, offer.amount, offer.extra_lessons);
}

export async function requestSingle(ctx) {
  const parent = requireParent(ctx.actor);
  const student = await ownStudent(ctx, parent, ctx.body.student_id);
  return openExtra(ctx, student, 'single', 'single', TARIFFS.single.price, TARIFFS.single.lessons);
}

export async function reportPayment(ctx) {
  const parent = requireParent(ctx.actor);
  const { db, body, env } = ctx;
  const method = body.payment_method;
  if (!['online', 'cash'].includes(method)) throw new ApiError(400, 'invalid_payment_method', 'Выберите способ оплаты');
  const payment = await ownPayment(ctx, parent, ctx.params.id);
  if (payment.status === 'paid') throw new ApiError(409, 'already_paid', 'Этот месяц уже оплачен');
  if (payment.status === 'reported') throw new ApiError(409, 'payment_in_review', 'Оплата уже на проверке');
  if (payment.status === 'skipped') throw new ApiError(409, 'month_closed', 'Этот месяц закрыт: занятий не было');
  if (payment.kind === 'subscription') {
    const oldest = await oldestOpenInvoice(db, payment.student_id);
    if (oldest && oldest.id !== payment.id) {
      throw new ApiError(409, 'oldest_debt_first', 'Сначала оплатите самый ранний неоплаченный месяц');
    }
  }
  if (method === 'online') {
    const waiting = await first(
      db,
      `SELECT p.id FROM payments p JOIN students s ON s.id = p.student_id
       WHERE s.parent_id = ? AND p.status = 'awaiting_receipt' AND p.id != ?`,
      parent.id,
      payment.id,
    );
    if (waiting) throw new ApiError(409, 'receipt_pending', 'Сначала пришлите чек для выбранной раньше оплаты');
    await run(
      db,
      `UPDATE payments SET method = 'online', status = 'awaiting_receipt', reported_at = NULL, rejection_reason = NULL WHERE id = ?`,
      payment.id,
    );
    return { success: true, status: 'awaiting_receipt' };
  }
  await run(
    db,
    `UPDATE payments SET method = 'cash', status = 'reported', reported_at = ?, rejection_reason = NULL WHERE id = ?`,
    isoNow(),
    payment.id,
  );
  const student = await first(db, 'SELECT name FROM students WHERE id = ?', payment.student_id);
  await notifyAdmins(env, `Наличные на проверку\nРебёнок: ${student.name}\nСумма: ${payment.amount} Br`);
  await audit(db, ctx.actor.user.id, 'payment_reported', 'payment', payment.id, { method, amount: payment.amount });
  return { success: true, status: 'reported' };
}

export async function uploadReceipt(ctx) {
  const parent = requireParent(ctx.actor);
  const file = ctx.form.get('file');
  if (!file || typeof file === 'string') throw new ApiError(400, 'empty_receipt', 'Прикрепите файл');
  const extension = RECEIPT_EXTENSIONS[file.type];
  if (!extension) throw new ApiError(400, 'invalid_receipt_type', 'Прикрепите фото или PDF');
  if (!file.size) throw new ApiError(400, 'empty_receipt', 'Файл пустой');
  if (file.size > RECEIPT_MAX_BYTES) throw new ApiError(413, 'receipt_too_large', 'Файл больше 10 МБ');
  const payment = await ownPayment(ctx, parent, ctx.params.id);
  if (payment.status !== 'awaiting_receipt') {
    throw new ApiError(409, 'receipt_not_expected', 'Для этой оплаты чек сейчас не нужен');
  }
  const student = await first(ctx.db, 'SELECT name FROM students WHERE id = ?', payment.student_id);
  try {
    const caption = `Чек на проверку\nРебёнок: ${student.name}\nСумма: ${payment.amount} Br`;
    const fileId = await storeReceipt(ctx.env, parent.telegram_id, file, `receipt-${payment.id}.${extension}`, caption);
    await run(
      ctx.db,
      `UPDATE payments SET receipt_file_id = ?, status = 'reported', reported_at = ? WHERE id = ?`,
      fileId,
      isoNow(),
      payment.id,
    );
  } catch (failure) {
    if (failure instanceof TelegramError) {
      throw new ApiError(502, 'receipt_delivery_failed', 'Не удалось отправить чек. Пришлите его в чат с ботом.');
    }
    throw failure;
  }
  await audit(ctx.db, ctx.actor.user.id, 'payment_reported', 'payment', payment.id, { method: 'online', amount: payment.amount });
  return { success: true, status: 'reported' };
}

export async function requestMakeup(ctx) {
  const parent = requireParent(ctx.actor);
  const { db, body, env } = ctx;
  if (!validDate(body.requested_date)) throw new ApiError(400, 'invalid_date', 'Выберите дату');
  const credit = await first(
    db,
    `SELECT m.*, s.name AS student_name FROM makeups m JOIN students s ON s.id = m.student_id
     WHERE m.id = ? AND s.parent_id = ? AND m.status IN ('available', 'requested')`,
    positiveInt(ctx.params.id),
    parent.id,
  );
  if (!credit) throw new ApiError(404, 'makeup_not_found', 'Отработка не найдена');
  if (body.requested_date > credit.expires_at) throw new ApiError(400, 'after_expiry', 'Эта дата позже срока отработки');
  if (daysBetween(minskNow().date, body.requested_date) < 0) throw new ApiError(400, 'invalid_date', 'Эта дата уже прошла');
  await run(
    db,
    `UPDATE makeups SET status = 'requested', requested_date = ?, requested_time = ?, requested_location_id = ?,
       requested_group_id = ?, rejection_reason = NULL WHERE id = ?`,
    body.requested_date,
    validTime(body.requested_time) ? body.requested_time : null,
    optionalInt(body.requested_location_id),
    optionalInt(body.requested_group_id),
    credit.id,
  );
  await notifyAdmins(env, `Заявка на отработку\nРебёнок: ${credit.student_name}\nДата: ${body.requested_date}`);
  return { success: true };
}

export async function requestSchedule(ctx) {
  const parent = requireParent(ctx.actor);
  const { db, body, env } = ctx;
  const student = await ownStudent(ctx, parent, body.student_id);
  const rows = parseScheduleRows(body.proposed_schedule);
  const value = JSON.stringify(rows);
  const pending = await first(db, `SELECT id FROM schedule_requests WHERE student_id = ? AND status = 'pending'`, student.id);
  if (pending) {
    await run(db, 'UPDATE schedule_requests SET proposed_schedule = ? WHERE id = ?', value, pending.id);
  } else {
    await insert(db, 'INSERT INTO schedule_requests (student_id, proposed_schedule, created_at) VALUES (?, ?, ?)', student.id, value, isoNow());
  }
  await notifyAdmins(env, `Заявка на смену расписания\nРебёнок: ${student.name}`);
  return { success: true };
}
