import assert from 'node:assert/strict';
import { test } from 'node:test';

import { runTick } from '../src/cron.js';
import {
  TARIFFS,
  addDays,
  canSelectTariff,
  isPaymentReminderDay,
  makeupExpiry,
  minskNow,
  monthBounds,
  weekdayOf,
} from '../src/rules.js';
import { SUPER_ADMIN, addCoach, addParent, addStudent, createApp, signInitData } from './helpers.js';

const today = () => minskNow().date;
const ALL_WEEK = (time) => [0, 1, 2, 3, 4, 5, 6].map((weekday) => ({ weekday, time }));
const rowCount = async (app, sql, ...args) => (await app.db.prepare(sql).bind(...args).first()).count;

test('tariffs and rule helpers match the school rules', () => {
  assert.deepEqual(Object.fromEntries(Object.entries(TARIFFS).map(([code, item]) => [code, item.price])), {
    8: 150,
    12: 180,
    16: 200,
    single: 40,
  });
  assert.equal(canSelectTariff('2026-08-05'), true);
  assert.equal(canSelectTariff('2026-08-06'), false);
  assert.equal(makeupExpiry('2026-12-31'), '2027-02-28');
  assert.equal(weekdayOf('2026-11-02'), 0);
  assert.equal(minskNow(new Date('2026-11-02T21:30:00Z')).date, '2026-11-03');
  assert.equal(isPaymentReminderDay('2026-07-03', '2026-07-10'), true);
  assert.equal(isPaymentReminderDay('2026-07-11', '2026-07-10'), true);
  assert.equal(isPaymentReminderDay('2026-07-12', '2026-07-10'), false);
  assert.equal(isPaymentReminderDay('2026-07-18', '2026-07-10'), true);
});

test('authentication rejects forged and stale sign-ins and routes every role', async () => {
  const app = createApp();
  const stale = signInitData(5, { authDate: Math.floor(Date.now() / 1000) - 3 * 86400 });
  const forged = signInitData(5, { token: 'other-token' });
  assert.equal((await app.post('/api/auth', stale)).status, 403);
  assert.equal((await app.post('/api/auth', forged)).status, 403);
  assert.equal((await app.post('/api/auth', 'hash=abc')).status, 403);

  assert.equal((await app.post('/api/auth', 5)).data.role, 'guest');
  const admin = await app.post('/api/auth', SUPER_ADMIN);
  assert.equal(admin.data.role, 'admin');
  await addCoach(app, 6);
  assert.equal((await app.post('/api/auth', 6)).data.role, 'coach');
  await addParent(app, 7);
  assert.equal((await app.post('/api/auth', 7)).data.role, 'parent');
});

test('roles cannot reach each others data', async () => {
  const app = createApp();
  const ownCoach = await addCoach(app, 10);
  const otherCoach = await addCoach(app, 11);
  const parentId = await addParent(app, 12);
  await addParent(app, 13);
  const own = await addStudent(app, { coachId: ownCoach, parentId, name: 'Свой' });
  const foreign = await addStudent(app, { coachId: otherCoach, name: 'Чужой' });

  const list = await app.post('/api/staff/students', 10);
  assert.deepEqual(list.data.students.map((item) => item.name), ['Свой']);
  assert.equal((await app.post(`/api/staff/students/${foreign}`, 10)).status, 404);
  assert.equal((await app.post(`/api/staff/students/${foreign}/update`, 10, { student: { name: 'Взлом' } })).status, 404);
  assert.equal((await app.post('/api/admin/money', 10)).status, 403);
  assert.equal((await app.post(`/api/staff/students/${own}/delete`, 10)).status, 403);
  assert.equal((await app.post('/api/staff/students', 12)).status, 403);

  const stranger = await app.post('/api/parent/reminders', 13, { student_id: own, enabled: false });
  assert.equal(stranger.status, 404);
  const adminList = await app.post('/api/staff/students', SUPER_ADMIN);
  assert.equal(adminList.data.students.length, 2);
});

test('family registration creates a student or links an existing one', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 20);
  const existing = await addStudent(app, { coachId, name: 'Есть В Списке', left: 5 });
  const child = (name) => ({ name, birthday: '2018-03-04', schedule: [{ weekday: 1, time: '18:00' }] });

  assert.equal((await app.post('/api/parent/register', 21, { parent: { full_name: '', phone: '' }, children: [child('Лев')] })).status, 400);
  const parent = { full_name: 'Анна Иванова', phone: '+375291112233' };
  assert.equal((await app.post('/api/parent/register', 21, { parent, children: [child('Лев'), child('Ева')] })).status, 200);
  assert.equal((await app.post('/api/parent/register', 21, { parent, children: [child('Лев')] })).status, 409);
  assert.equal((await app.post('/api/auth', 21)).data.role, 'parent');

  const requests = await app.post('/api/staff/requests', 20);
  assert.equal(requests.data.registrations.length, 2);
  const [first, second] = requests.data.registrations;
  assert.equal((await app.post(`/api/staff/registrations/${first.id}/review`, 20, { decision: 'approve' })).status, 400);
  const approved = await app.post(`/api/staff/registrations/${first.id}/review`, 20, { decision: 'approve', start_date: today() });
  assert.equal(approved.status, 200);
  const linked = await app.post(`/api/staff/registrations/${second.id}/review`, 20, {
    decision: 'approve',
    start_date: today(),
    link_student_id: existing,
  });
  assert.equal(linked.data.student_id, existing);

  const context = await app.post('/api/parent/context', 21);
  assert.deepEqual(context.data.students.map((item) => item.name).sort(), ['Есть В Списке', 'Лев']);
  const kept = context.data.students.find((item) => item.name === 'Есть В Списке');
  assert.equal(kept.lessons_remaining, 5);
  const created = context.data.students.find((item) => item.name === 'Лев');
  assert.equal(created.invoice.amount, 150);
  assert.equal(created.schedules[0].times[1], '18:00');

  assert.equal((await app.post(`/api/staff/registrations/${first.id}/review`, 20, { decision: 'reject', reason: 'x' })).status, 404);
});

test('rejecting requires a reason that reaches the parent', async () => {
  const app = createApp();
  await addCoach(app, 25);
  const child = { name: 'Отказ', birthday: '2018-01-01', schedule: [{ weekday: 0, time: '10:00' }] };
  await app.post('/api/parent/register', 26, { parent: { full_name: 'Мама Отказа', phone: '+375290000001' }, children: [child] });
  const { data } = await app.post('/api/staff/requests', 25);
  const id = data.registrations[0].id;
  assert.equal((await app.post(`/api/staff/registrations/${id}/review`, 25, { decision: 'reject' })).status, 400);
  assert.equal((await app.post(`/api/staff/registrations/${id}/review`, 25, { decision: 'reject', reason: 'Нет мест' })).status, 200);
  const context = await app.post('/api/parent/context', 26);
  assert.equal(context.data.registration_requests[0].rejection_reason, 'Нет мест');
  assert.ok(app.telegram.some((call) => call.method === 'sendMessage' && call.payload.text?.includes('Нет мест')));
});

test('parent sees and answers the next training', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 30);
  const parentId = await addParent(app, 31);
  const studentId = await addStudent(app, { coachId, parentId, schedule: ALL_WEEK('23:59') });
  const context = await app.post('/api/parent/context', 31);
  const training = context.data.students[0].next_training;
  assert.equal(training.time, '23:59');
  assert.equal(training.response, null);
  const answer = { student_id: studentId, date: training.date, time: training.time };
  assert.equal((await app.post('/api/parent/training-response', 31, { ...answer, response: 'absent' })).status, 200);
  const saved = await app.post('/api/parent/context', 31);
  assert.equal(saved.data.students[0].next_training.response, 'absent');
  assert.equal((await app.post('/api/parent/training-response', 31, { ...answer, response: null })).status, 200);
  assert.equal((await app.post('/api/parent/training-response', 31, { ...answer, time: '12:34', response: 'absent' })).status, 404);
});

test('attendance applies lesson and makeup rules once', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 40);
  const parentId = await addParent(app, 41);
  const date = today();
  const weekday = weekdayOf(date);
  const studentId = await addStudent(app, { coachId, parentId, left: 8, schedule: [{ weekday, time: '00:01' }] });
  const left = async () => (await app.db.prepare('SELECT lessons_remaining AS value FROM students WHERE id = ?').bind(studentId).first()).value;
  const credits = () => rowCount(app, `SELECT COUNT(*) AS count FROM makeups WHERE student_id = ? AND status = 'available'`, studentId);

  const day = await app.post('/api/staff/day', 40, { date });
  const session = day.data.sessions[0];
  assert.equal(session.saved, false);
  const save = (status) => app.post('/api/staff/attendance', 40, { date, key: session.key, marks: [{ student_id: studentId, status }] });

  await save('present');
  assert.equal(await left(), 7);
  assert.equal(await credits(), 0);
  await save('present');
  assert.equal(await left(), 7);

  await save('absent');
  assert.equal(await left(), 7);
  assert.equal(await credits(), 1);
  await save('absent');
  assert.equal(await credits(), 1);

  await save('present');
  assert.equal(await credits(), 0);
  await save('absent');
  assert.equal(await credits(), 1);

  const cancelled = await app.post('/api/staff/cancel-session', 40, { date, key: session.key });
  assert.equal(cancelled.status, 200);
  assert.equal(await left(), 8);
  assert.equal(await credits(), 1);
  const after = await app.post('/api/staff/day', 40, { date });
  assert.equal(after.data.sessions[0].cancelled, true);
  assert.ok(app.telegram.some((call) => call.payload?.text?.includes('отменена')));

  assert.equal((await app.post('/api/staff/attendance', 40, { date: addDays(date, 1), key: session.key })).status, 409);
});

test('monthly limit stops new makeups for absences but not for cancellations', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 45);
  const studentId = await addStudent(app, { coachId, schedule: ALL_WEEK('00:01') });
  const { start } = monthBounds(today());
  for (let index = 0; index < 8; index += 1) {
    await app.db.prepare(
      `INSERT INTO makeups (student_id, source_date, source_type, expires_at, status, created_at)
       VALUES (?, ?, 'absence', ?, 'used', '2026-01-01')`,
    ).bind(studentId, start, addDays(start, 60)).run();
  }
  const date = today();
  const { data } = await app.post('/api/staff/day', 45, { date });
  const { key } = data.sessions[0];
  await app.post('/api/staff/attendance', 45, { date, key, marks: [{ student_id: studentId, status: 'absent' }] });
  const available = () => rowCount(app, `SELECT COUNT(*) AS count FROM makeups WHERE student_id = ? AND status = 'available'`, studentId);
  assert.equal(await available(), 0);
  await app.post('/api/staff/cancel-session', 45, { date, key });
  assert.equal(await available(), 1);
});

test('makeup request carries its slot to the approved session', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 50);
  const parentId = await addParent(app, 51);
  const group = await app.db.prepare(`INSERT INTO training_groups (name) VALUES ('Своя') RETURNING id`).first();
  const other = await app.db.prepare(`INSERT INTO training_groups (name) VALUES ('Чужая') RETURNING id`).first();
  const childWeekday = weekdayOf(today());
  const studentId = await addStudent(app, { coachId, parentId, schedule: [{ weekday: childWeekday, time: '09:00', group_id: group.id }] });
  await addStudent(app, { coachId, name: 'Одногруппник', schedule: ALL_WEEK('09:30').map((row) => ({ ...row, group_id: group.id })) });
  await addStudent(app, { coachId, name: 'Другая группа', schedule: ALL_WEEK('09:45').map((row) => ({ ...row, group_id: other.id })) });
  await app.db.prepare(
    `INSERT INTO makeups (student_id, source_date, expires_at, status, created_at) VALUES (?, ?, ?, 'available', '2026-01-01')`,
  ).bind(studentId, today(), addDays(today(), 30)).run();

  const context = await app.post('/api/parent/context', 51);
  const [student] = context.data.students;
  assert.ok(student.makeup_slots.length);
  assert.deepEqual([...new Set(student.makeup_slots.map((slot) => slot.time))], ['09:30']);
  const slot = student.makeup_slots[0];
  const credit = student.makeups[0];
  const body = { requested_date: slot.date, requested_time: slot.time, requested_group_id: slot.group_id };
  assert.equal((await app.post(`/api/parent/makeups/${credit.id}/request`, 51, { ...body, requested_date: addDays(today(), 90) })).status, 400);
  assert.equal((await app.post(`/api/parent/makeups/${credit.id}/request`, 51, body)).status, 200);
  assert.equal((await app.post(`/api/staff/makeups/${credit.id}/review`, 50, { decision: 'approve' })).status, 200);
  const stored = await app.db.prepare('SELECT * FROM makeups WHERE id = ?').bind(credit.id).first();
  assert.equal(stored.status, 'scheduled');
  assert.equal(stored.scheduled_time, '09:30');
  assert.equal(stored.group_id, group.id);

  const day = await app.post('/api/staff/day', 50, { date: slot.date });
  const session = day.data.sessions.find((item) => item.time === '09:30');
  const entry = session.roster.find((item) => item.student_id === studentId);
  assert.equal(entry.source, 'makeup');
  assert.equal(entry.tag, 'Отработка');
});

test('payment flow: cash, oldest debt first, no late fee', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 60);
  const parentId = await addParent(app, 61);
  const studentId = await addStudent(app, { coachId, parentId, left: 0 });
  const context = await app.post('/api/parent/context', 61);
  const current = context.data.students[0].invoice;
  assert.equal(current.amount, 150);

  const previousStart = monthBounds(addDays(monthBounds(today()).start, -1));
  const { id: oldId } = await app.db.prepare(
    `INSERT INTO payments (student_id, coach_id, amount, lessons_count, tariff_code, status, period_start, period_end, due_date, created_at)
     VALUES (?, ?, 150, 8, '8', 'pending', ?, ?, ?, '2026-01-01') RETURNING id`,
  ).bind(studentId, coachId, previousStart.start, previousStart.end, `${previousStart.start.slice(0, 8)}10`).first();

  const overdue = await app.post('/api/parent/context', 61);
  assert.equal(overdue.data.students[0].oldest_open_invoice_id, oldId);
  const oldInvoice = overdue.data.students[0].invoices.find((item) => item.id === oldId);
  assert.equal(oldInvoice.status, 'overdue');
  assert.equal(oldInvoice.amount, 150);

  const newerFirst = await app.post(`/api/parent/payments/${current.id}/report`, 61, { payment_method: 'cash' });
  assert.equal(newerFirst.data.error, 'oldest_debt_first');
  assert.equal((await app.post(`/api/parent/payments/${oldId}/report`, 61, { payment_method: 'cash' })).data.status, 'reported');
  assert.equal((await app.post(`/api/parent/payments/${oldId}/report`, 61, { payment_method: 'cash' })).status, 409);

  assert.equal((await app.post(`/api/admin/payments/${oldId}/review`, 60, { decision: 'approve' })).status, 403);
  const approved = await app.post(`/api/admin/payments/${oldId}/review`, SUPER_ADMIN, { decision: 'approve' });
  assert.equal(approved.data.amount, 150);
  const student = await app.db.prepare('SELECT lessons_remaining FROM students WHERE id = ?').bind(studentId).first();
  assert.equal(student.lessons_remaining, 8);
  assert.equal((await app.post(`/api/admin/payments/${current.id}/review`, SUPER_ADMIN, { decision: 'approve' })).data.error, 'payment_not_reported');
});

test('online payment needs a receipt uploaded through the app', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 70);
  const parentId = await addParent(app, 71);
  await addParent(app, 72);
  await addStudent(app, { coachId, parentId });
  const { data } = await app.post('/api/parent/context', 71);
  const { id } = data.students[0].invoice;
  const url = `/api/parent/payments/${id}/receipt`;
  const image = new File(['receipt'], 'check.jpg', { type: 'image/jpeg' });

  assert.equal((await app.upload(url, 71, image)).data.error, 'receipt_not_expected');
  assert.equal((await app.post(`/api/parent/payments/${id}/report`, 71, { payment_method: 'online' })).data.status, 'awaiting_receipt');
  assert.equal((await app.upload(url, 71, new File(['x'], 'a.exe', { type: 'application/octet-stream' }))).status, 400);
  assert.equal((await app.upload(url, 71, null)).status, 400);
  assert.equal((await app.upload(url, 72, image)).status, 404);

  app.failures.sendDocument = 403;
  const failed = await app.upload(url, 71, image);
  assert.equal(failed.status, 502);
  assert.equal((await app.db.prepare('SELECT status FROM payments WHERE id = ?').bind(id).first()).status, 'awaiting_receipt');
  delete app.failures.sendDocument;

  assert.equal((await app.upload(url, 71, image)).data.status, 'reported');
  const stored = await app.db.prepare('SELECT * FROM payments WHERE id = ?').bind(id).first();
  assert.equal(stored.receipt_file_id, 'telegram-file-id');
  assert.equal((await app.post(`/api/admin/payments/${id}/review`, SUPER_ADMIN, { decision: 'approve' })).status, 200);
});

test('online report without a receipt cannot be approved', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 75);
  const parentId = await addParent(app, 76);
  await addStudent(app, { coachId, parentId });
  const { data } = await app.post('/api/parent/context', 76);
  const { id } = data.students[0].invoice;
  await app.db.prepare(`UPDATE payments SET method = 'online', status = 'reported' WHERE id = ?`).bind(id).run();
  assert.equal((await app.post(`/api/admin/payments/${id}/review`, SUPER_ADMIN, { decision: 'approve' })).data.error, 'receipt_required');
});

test('tariff change is limited to the first five days and open invoices', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 80);
  const parentId = await addParent(app, 81);
  const studentId = await addStudent(app, { coachId, parentId });
  const result = await app.post('/api/parent/tariff', 81, { student_id: studentId, tariff_code: '12' });
  if (canSelectTariff(today())) {
    assert.equal(result.status, 200);
    const { data } = await app.post('/api/parent/context', 81);
    assert.equal(data.students[0].invoice.amount, 180);
    assert.equal(data.students[0].invoice.tariff_code, '12');
  } else {
    assert.equal(result.data.error, 'tariff_deadline_passed');
  }
  assert.equal((await app.post('/api/parent/tariff', 81, { student_id: studentId, tariff_code: '99' })).status, 400);
});

test('money screen lists debtors and takes cash', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 90);
  const parentId = await addParent(app, 91);
  const studentId = await addStudent(app, { coachId, parentId, left: 0 });
  const before = await app.post('/api/admin/money', SUPER_ADMIN);
  assert.equal(before.data.waiting, 150);
  assert.equal(before.data.debtors[0].student_name, 'Ученик');
  const { id } = before.data.debtors[0];
  assert.equal((await app.post(`/api/admin/payments/${id}/remind`, SUPER_ADMIN)).status, 200);
  assert.ok(app.telegram.some((call) => call.payload?.text?.includes('Напоминание об оплате')));
  assert.equal((await app.post(`/api/admin/payments/${id}/cash`, SUPER_ADMIN)).status, 200);
  const after = await app.post('/api/admin/money', SUPER_ADMIN);
  assert.equal(after.data.collected, 150);
  assert.equal(after.data.waiting, 0);
  const student = await app.db.prepare('SELECT lessons_remaining FROM students WHERE id = ?').bind(studentId).first();
  assert.equal(student.lessons_remaining, 8);
});

test('deleting a student erases personal data but keeps revenue', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 95);
  const parentId = await addParent(app, 96);
  const studentId = await addStudent(app, { coachId, parentId, name: 'Удаляемый', schedule: ALL_WEEK('10:00') });
  const money = await app.post('/api/admin/money', SUPER_ADMIN);
  await app.post(`/api/admin/payments/${money.data.debtors[0].id}/cash`, SUPER_ADMIN);
  assert.equal((await app.post(`/api/staff/students/${studentId}/delete`, SUPER_ADMIN)).status, 200);
  const row = await app.db.prepare('SELECT * FROM students WHERE id = ?').bind(studentId).first();
  assert.equal(row.name, 'Удалённый ученик');
  assert.equal(row.parent_id, null);
  assert.equal(await rowCount(app, 'SELECT COUNT(*) AS count FROM schedules WHERE student_id = ?', studentId), 0);
  assert.equal((await app.post('/api/admin/money', SUPER_ADMIN)).data.collected, 150);
  assert.equal((await app.post('/api/staff/students', SUPER_ADMIN)).data.students.length, 0);
});

test('team roles protect the main administrator', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 100);
  await app.post('/api/auth', SUPER_ADMIN);
  const team = await app.post('/api/admin/team', SUPER_ADMIN);
  assert.equal(team.data.registration_link, 'https://t.me/crm_test_bot?start=registration');
  const locked = team.data.coaches.find((item) => item.locked);
  assert.equal((await app.post(`/api/admin/coaches/${locked.id}/roles`, SUPER_ADMIN, { is_admin: false })).status, 409);
  assert.equal((await app.post('/api/admin/money', 100)).status, 403);
  assert.equal((await app.post(`/api/admin/coaches/${coachId}/roles`, SUPER_ADMIN, { is_admin: true })).status, 200);
  assert.equal((await app.post('/api/admin/money', 100)).status, 200);
  const created = await app.post('/api/admin/locations/create', SUPER_ADMIN, { name: 'Зал 1' });
  assert.equal(created.status, 200);
  assert.equal((await app.post('/api/admin/team', SUPER_ADMIN)).data.locations[0].name, 'Зал 1');
  assert.equal((await app.post(`/api/admin/locations/${created.data.id}/delete`, SUPER_ADMIN)).status, 200);
  assert.equal((await app.post('/api/admin/team', SUPER_ADMIN)).data.locations.length, 0);
});

test('webhook needs the secret and every bot reply opens the app', async () => {
  const app = createApp();
  const from = { id: 200, first_name: 'Новый' };
  const message = (text) => ({ message: { from, chat: { id: 200, type: 'private' }, text } });
  assert.equal((await app.webhook(message('/start'), 'wrong')).status, 403);
  assert.equal(app.telegram.length, 0);

  await app.webhook(message('/start'));
  await app.webhook(message('/help'));
  await app.webhook(message('что угодно'));
  await app.webhook(message('/coach неверный'));
  const replies = app.telegram.filter((call) => call.method === 'sendMessage');
  assert.equal(replies.length, 4);
  for (const reply of replies) {
    assert.equal(reply.payload.reply_markup.inline_keyboard[0][0].web_app.url, 'https://crm.example.dev/');
  }

  await app.webhook(message('/coach coach-code'));
  assert.equal((await app.post('/api/auth', 200)).data.role, 'coach');
});

test('receipt sent to the bot chat attaches to the waiting payment', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 210);
  const parentId = await addParent(app, 211);
  await addStudent(app, { coachId, parentId });
  const { data } = await app.post('/api/parent/context', 211);
  const { id } = data.students[0].invoice;
  await app.post(`/api/parent/payments/${id}/report`, 211, { payment_method: 'online' });
  await app.webhook({
    message: { from: { id: 211 }, chat: { id: 211, type: 'private' }, photo: [{ file_id: 'small' }, { file_id: 'large' }] },
  });
  const stored = await app.db.prepare('SELECT * FROM payments WHERE id = ?').bind(id).first();
  assert.equal(stored.status, 'reported');
  assert.equal(stored.receipt_file_id, 'large');
});

test('training answer from a bot button is saved only for the parents own child', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 220);
  const parentId = await addParent(app, 221);
  await addParent(app, 222);
  const studentId = await addStudent(app, { coachId, parentId });
  const date = addDays(today(), 1);
  const callback = (userId) => ({
    callback_query: { id: 'c1', from: { id: userId }, data: `rsvp|${studentId}|${date}|18:00|no` },
  });
  await app.webhook(callback(222));
  assert.equal(await rowCount(app, 'SELECT COUNT(*) AS count FROM training_responses'), 0);
  await app.webhook(callback(221));
  const row = await app.db.prepare('SELECT response FROM training_responses').first();
  assert.equal(row.response, 'absent');
});

test('scheduled jobs run once a day and survive repeated ticks', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 300);
  const parentId = await addParent(app, 301);
  // Monday 2 November 2026; 7 and 3 days before the due date fall on 3 and 7 November.
  const studentId = await addStudent(app, { coachId, parentId, schedule: [{ weekday: 1, time: '18:00' }] });

  const morning = new Date('2026-11-03T07:30:00Z');
  await runTick(app.env, morning);
  await runTick(app.env, new Date('2026-11-03T07:35:00Z'));
  const payment = await app.db.prepare('SELECT * FROM payments WHERE student_id = ?').bind(studentId).first();
  assert.equal(payment.amount, 150);
  const reminders = app.telegram.filter((call) => call.payload?.text?.includes('Оплата абонемента'));
  assert.equal(reminders.length, 1);
  assert.equal(reminders[0].payload.reply_markup.inline_keyboard[0][0].web_app.url, 'https://crm.example.dev/');

  const evening = new Date('2026-11-02T15:05:00Z');
  await runTick(app.env, evening);
  await runTick(app.env, new Date('2026-11-02T15:10:00Z'));
  const training = app.telegram.filter((call) => call.payload?.text?.startsWith('Завтра тренировка'));
  assert.equal(training.length, 1);
  assert.equal(training[0].payload.reply_markup.inline_keyboard[0][0].callback_data, `rsvp|${studentId}|2026-11-03|18:00|yes`);

  await runTick(app.env, new Date('2026-11-03T15:20:00Z'));
  const unmarked = app.telegram.filter((call) => call.payload?.text?.startsWith('Не отмечена'));
  assert.equal(unmarked.length, 1);
  await runTick(app.env, new Date('2026-11-03T15:25:00Z'));
  assert.equal(app.telegram.filter((call) => call.payload?.text?.startsWith('Не отмечена')).length, 1);
});

test('outbox keeps going when a parent has blocked the bot', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 310);
  const parentId = await addParent(app, 311);
  await addStudent(app, { coachId, parentId });
  app.failures.sendMessage = 403;
  await app.post('/api/admin/money', SUPER_ADMIN);
  const { data } = await app.post('/api/admin/money', SUPER_ADMIN);
  await app.post(`/api/admin/payments/${data.debtors[0].id}/remind`, SUPER_ADMIN);
  const parent = await app.db.prepare('SELECT bot_blocked_at FROM parents WHERE id = ?').bind(parentId).first();
  assert.ok(parent.bot_blocked_at);
  assert.equal(await rowCount(app, 'SELECT COUNT(*) AS count FROM outbox WHERE sent_at IS NULL'), 0);
});
