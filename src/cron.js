// Scheduled jobs. Each daily job runs once, on the first tick after its hour,
// so a missed tick delays a job instead of losing it.
import { all, first, run } from './db.js';
import { ensureAllInvoices, expireMakeups } from './domain.js';
import {
  addDays,
  daysBetween,
  formatDate,
  isPaymentReminderDay,
  isoNow,
  minskNow,
  validTime,
  weekdayOf,
} from './rules.js';
import { adminChatIds, enqueue, flushOutbox, openButton } from './telegram.js';

const DAILY_HOUR = 10;
const TRAINING_REMINDER_HOUR = 18;
const UNMARKED_AFTER_MINUTES = 15;
const UNMARKED_WINDOW_MINUTES = 180;

async function once(db, key, job) {
  const claimed = await run(db, 'INSERT OR IGNORE INTO job_runs (key, ran_at) VALUES (?, ?)', key, isoNow());
  if (claimed.meta.changes) await job();
}

async function paymentReminders(env, today) {
  const rows = await all(
    env.DB,
    `SELECT p.*, s.name AS student_name, par.telegram_id
     FROM payments p
     JOIN students s ON s.id = p.student_id
     JOIN parents par ON par.id = s.parent_id
     WHERE p.status NOT IN ('paid', 'written_off', 'reported') AND s.is_active = 1
     ORDER BY s.id, p.period_start`,
  );
  const seen = new Set();
  for (const payment of rows) {
    if (seen.has(payment.student_id)) continue;
    seen.add(payment.student_id);
    if (!isPaymentReminderDay(today, payment.due_date)) continue;
    await enqueue(
      env,
      payment.telegram_id,
      `Оплата абонемента\n${payment.student_name}: ${payment.amount} Br, срок до ${formatDate(payment.due_date)}.`,
      { key: `payment:${payment.id}:${today}`, buttonLabel: 'Оплатить' },
    );
  }
}

async function makeupExpiryReminders(env, today) {
  const rows = await all(
    env.DB,
    `SELECT m.id, m.expires_at, s.name AS student_name, par.telegram_id
     FROM makeups m
     JOIN students s ON s.id = m.student_id
     JOIN parents par ON par.id = s.parent_id
     WHERE m.status = 'available' AND m.expires_at >= ?`,
    today,
  );
  for (const row of rows) {
    const left = daysBetween(today, row.expires_at);
    if (left !== 14 && left !== 7) continue;
    await enqueue(
      env,
      row.telegram_id,
      `Отработка для ${row.student_name} сгорит через ${left} дней: срок до ${formatDate(row.expires_at)}.`,
      { key: `makeup_expiry:${row.id}:${left}`, buttonLabel: 'Записаться' },
    );
  }
}

async function adminSummary(env, today) {
  const count = async (sql) => (await first(env.DB, sql)).count;
  const registrations = await count(`SELECT COUNT(*) AS count FROM registration_requests WHERE status = 'pending'`);
  const schedules = await count(`SELECT COUNT(*) AS count FROM schedule_requests WHERE status = 'pending'`);
  const makeups = await count(`SELECT COUNT(*) AS count FROM makeups WHERE status = 'requested'`);
  const payments = await count(`SELECT COUNT(*) AS count FROM payments WHERE status = 'reported'`);
  const overdue = await first(
    env.DB,
    `SELECT COUNT(*) AS count, COALESCE(SUM(p.amount), 0) AS total FROM payments p JOIN students s ON s.id = p.student_id
     WHERE p.status NOT IN ('paid', 'written_off', 'reported') AND p.due_date < ? AND s.is_active = 1`,
    today,
  );
  if (!registrations && !schedules && !makeups && !payments && !overdue.count) return;
  const text = [
    `Сводка на ${formatDate(today)}`,
    `Заявки: анкеты ${registrations}, расписание ${schedules}, отработки ${makeups}`,
    `Оплаты на проверке: ${payments}`,
    `Просрочено: ${overdue.count} на ${overdue.total} Br`,
  ].join('\n');
  for (const chatId of await adminChatIds(env)) {
    await enqueue(env, chatId, text, { key: `admin_summary:${chatId}:${today}`, buttonLabel: 'Открыть CRM' });
  }
}

async function coachSummaries(env, today) {
  const coaches = await all(env.DB, 'SELECT id, telegram_id FROM coaches WHERE is_active = 1');
  for (const coach of coaches) {
    const sessions = await all(
      env.DB,
      `SELECT DISTINCT sc.time FROM schedules sc JOIN students s ON s.id = sc.student_id
       WHERE s.coach_id = ? AND s.is_active = 1 AND s.deleted_at IS NULL AND sc.weekday = ?
         AND (s.training_start_date IS NULL OR s.training_start_date <= ?)
       ORDER BY sc.time`,
      coach.id,
      weekdayOf(today),
      today,
    );
    if (!sessions.length) continue;
    await enqueue(
      env,
      coach.telegram_id,
      `Тренировки сегодня: ${sessions.map((row) => row.time).join(', ')}`,
      { key: `coach_day:${coach.id}:${today}`, buttonLabel: 'Открыть CRM' },
    );
  }
}

async function trainingReminders(env, today) {
  const date = addDays(today, 1);
  const rows = await all(
    env.DB,
    `SELECT s.id, s.name, sc.time, l.name AS location, par.telegram_id
     FROM schedules sc
     JOIN students s ON s.id = sc.student_id
     JOIN parents par ON par.id = s.parent_id
     LEFT JOIN locations l ON l.id = sc.location_id
     WHERE sc.weekday = ? AND s.is_active = 1 AND s.deleted_at IS NULL AND s.reminders_enabled = 1
       AND (s.training_start_date IS NULL OR s.training_start_date <= ?)`,
    weekdayOf(date),
    date,
  );
  for (const row of rows) {
    const answer = (value) => `rsvp|${row.id}|${date}|${row.time}|${value}`;
    await enqueue(
      env,
      row.telegram_id,
      `Завтра тренировка\n${row.name}, ${row.time}${row.location ? `, ${row.location}` : ''}\n\nПропуск всё равно списывает занятие, вместо него появится отработка.`,
      {
        key: `training:${row.id}:${date}:${row.time}`,
        markup: {
          inline_keyboard: [
            [
              { text: 'Придёт', callback_data: answer('yes') },
              { text: 'Не придёт', callback_data: answer('no') },
            ],
            [openButton(env, 'Открыть кабинет')],
          ],
        },
      },
    );
  }
}

function minutesOf(time) {
  return Number(time.slice(0, 2)) * 60 + Number(time.slice(3, 5));
}

async function unmarkedReminders(env, now) {
  const rows = await all(
    env.DB,
    `SELECT DISTINCT sc.time, c.id AS coach_id, c.telegram_id
     FROM schedules sc
     JOIN students s ON s.id = sc.student_id
     JOIN coaches c ON c.id = s.coach_id
     WHERE sc.weekday = ? AND s.is_active = 1 AND s.deleted_at IS NULL AND c.is_active = 1
       AND (s.training_start_date IS NULL OR s.training_start_date <= ?)
       AND NOT EXISTS (
         SELECT 1 FROM attendance a WHERE a.student_id = s.id AND a.date = ? AND a.time = sc.time
       )`,
    weekdayOf(now.date),
    now.date,
    now.date,
  );
  const current = minutesOf(now.time);
  for (const row of rows) {
    if (!validTime(row.time)) continue;
    const passed = current - minutesOf(row.time);
    if (passed < UNMARKED_AFTER_MINUTES || passed > UNMARKED_WINDOW_MINUTES) continue;
    await enqueue(env, row.telegram_id, `Не отмечена тренировка ${row.time}.`, {
      key: `unmarked:${row.coach_id}:${now.date}:${row.time}`,
      buttonLabel: 'Отметить',
    });
  }
}

export async function runTick(env, date = new Date()) {
  const now = minskNow(date);
  const { DB: db } = env;
  await unmarkedReminders(env, now);
  if (now.hour >= DAILY_HOUR) {
    await once(db, `daily:${now.date}`, async () => {
      await expireMakeups(db, now.date);
      await ensureAllInvoices(db, now.date);
      await paymentReminders(env, now.date);
      await makeupExpiryReminders(env, now.date);
      await coachSummaries(env, now.date);
      await adminSummary(env, now.date);
    });
  }
  if (now.hour >= TRAINING_REMINDER_HOUR) {
    await once(db, `training:${now.date}`, () => trainingReminders(env, now.date));
  }
  await flushOutbox(env);
}
