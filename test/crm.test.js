import assert from 'node:assert/strict';
import { test } from 'node:test';
import { readFileSync } from 'node:fs';

import { runTick } from '../src/cron.js';
import { balanceOf, closeUnusedMonths, ensureInvoice, markPaid, refreshFirstInvoice, saveAttendance } from '../src/domain.js';
import {
  TARIFFS,
  addDays,
  canSelectTariff,
  isPaymentReminderDay,
  makeupExpiry,
  minskNow,
  monthBounds,
  firstMonth,
  tariffForSchedule,
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
  const child = (name) => ({ name, nickname: `${name}-ник`, birthday: '2018-03-04', schedule: [{ weekday: 1, time: '18:00' }] });

  assert.equal((await app.post('/api/parent/register', 21, { parent: { full_name: '', phone: '' }, children: [child('Лев')] })).status, 400);
  const parent = { full_name: 'Анна Иванова', phone: '+375291112233' };
  assert.equal((await app.post('/api/parent/register', 21, { parent, children: [child('Лев'), child('Ева')] })).status, 200);
  assert.equal((await app.post('/api/parent/register', 21, { parent, children: [child('Лев')] })).status, 409);
  assert.equal((await app.post('/api/auth', 21)).data.role, 'parent');

  const requests = await app.post('/api/staff/requests', 20);
  assert.equal(requests.data.registrations.length, 2);
  const [first, second] = requests.data.registrations;
  assert.equal((await app.post(`/api/staff/registrations/${first.id}/review`, 20, { decision: 'approve' })).status, 400);
  const approved = await app.post(`/api/staff/registrations/${first.id}/review`, 20, { decision: 'approve', start_date: monthBounds(today()).start });
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
  assert.equal(kept.lessons_remaining, 8);
  const created = context.data.students.find((item) => item.name === 'Лев');
  assert.equal(created.invoice.amount, 150);
  assert.equal(created.schedules[0].times[1], '18:00');

  assert.equal((await app.post(`/api/staff/registrations/${first.id}/review`, 20, { decision: 'reject', reason: 'x' })).status, 404);
});

test('the family form requires a nickname and a real birth date', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 27);
  const parent = { full_name: 'Анна Иванова', phone: '+375291112233' };
  const child = { name: 'Лев Иванов', nickname: 'Лёва', birthday: '2018-03-04', schedule: [{ weekday: 1, time: '18:00' }] };
  const send = (changes) => app.post('/api/parent/register', 28, { parent, children: [{ ...child, ...changes }] });

  assert.equal((await send({ nickname: '  ' })).data.error, 'child_nickname_required');
  assert.equal((await send({ birthday: '' })).data.error, 'child_birthday_required');
  assert.equal((await send({ birthday: '2999-01-01' })).data.error, 'child_birthday_required');
  assert.equal((await send({})).status, 200);

  const { data } = await app.post('/api/staff/requests', 27);
  assert.equal(data.registrations[0].child_nickname, 'Лёва');
  assert.equal(data.registrations[0].child_birthday, '2018-03-04');
  const approved = await app.post(`/api/staff/registrations/${data.registrations[0].id}/review`, 27, { decision: 'approve', start_date: today() });
  const student = await app.post(`/api/staff/students/${approved.data.student_id}`, 27);
  assert.equal(student.data.student.nickname, 'Лёва');
  assert.equal(student.data.student.birthday, '2018-03-04');

  const existing = await addStudent(app, { coachId, name: 'Старый Ученик' });
  await app.post('/api/parent/register', 29, { parent, children: [{ ...child, name: 'Старый Ученик', nickname: 'Старик' }] });
  const second = await app.post('/api/staff/requests', 27);
  await app.post(`/api/staff/registrations/${second.data.registrations[0].id}/review`, 27, { decision: 'approve', start_date: today(), link_student_id: existing });
  assert.equal((await app.post(`/api/staff/students/${existing}`, 27)).data.student.nickname, 'Старик');
  const list = await app.post('/api/staff/students', 27);
  assert.deepEqual(list.data.students.map((item) => item.nickname).sort(), ['Лёва', 'Старик']);
});

test('parents choose a coach and the request suggests that coach', async () => {
  const app = createApp();
  app.env.COACHES = [
    { name: 'Койпиш Максим', username: 'First_Coach' },
    { name: 'Койпиш Юрий', username: 'Second_Coach' },
  ];
  app.env.ADMIN_USERNAMES = 'First_Coach,Second_Coach';
  await addCoach(app, 600);
  const parent = { full_name: 'Анна Иванова', phone: '+375291112233' };
  const child = { name: 'Лев Иванов', nickname: 'Лёва', birthday: '2018-03-04', schedule: [{ weekday: 1, time: '18:00' }] };
  const send = (coach) => app.post('/api/parent/register', 601, { parent, children: [{ ...child, coach }] });

  assert.deepEqual((await app.post('/api/auth', 601)).data.coach_options.map((item) => item.name), ['Койпиш Максим', 'Койпиш Юрий']);
  assert.equal((await send('')).data.error, 'child_coach_required');
  assert.equal((await send('Someone_Else')).data.error, 'child_coach_required');
  assert.equal((await send('Second_Coach')).status, 200);

  let [request] = (await app.post('/api/staff/requests', 600)).data.registrations;
  assert.equal(request.preferred_coach_name, 'Койпиш Юрий');
  assert.equal(request.preferred_coach_id, null);

  // The coach opens the bot later, under the name from the list, and the request now points at them.
  await app.webhook({ message: { text: '/start', chat: { id: 602, type: 'private' }, from: { id: 602, first_name: 'Юра', username: 'second_coach' } } });
  const account = await app.db.prepare('SELECT id, first_name FROM coaches WHERE telegram_id = 602').first();
  assert.equal(account.first_name, 'Койпиш Юрий');
  [request] = (await app.post('/api/staff/requests', 600)).data.registrations;
  assert.equal(request.preferred_coach_id, account.id);
});

test('rejecting requires a reason that reaches the parent', async () => {
  const app = createApp();
  await addCoach(app, 25);
  const child = { name: 'Отказ', nickname: 'Отказник', birthday: '2018-01-01', schedule: [{ weekday: 0, time: '10:00' }] };
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
  const studentId = await addStudent(app, { coachId, parentId, schedule: [{ weekday, time: '00:01' }] });
  const left = async () => (await app.post(`/api/staff/students/${studentId}`, 40)).data.student.lessons_remaining;
  const money = await app.post('/api/admin/money', SUPER_ADMIN);
  await app.post(`/api/admin/payments/${money.data.debtors[0].id}/cash`, SUPER_ADMIN);
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
  const money = await app.post('/api/admin/money', SUPER_ADMIN);
  await app.post(`/api/admin/payments/${money.data.debtors[0].id}/cash`, SUPER_ADMIN);
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
  const studentId = await addStudent(app, { coachId, parentId });
  const context = await app.post('/api/parent/context', 61);
  const current = context.data.students[0].invoice;
  assert.equal(current.amount, 150);

  const previousStart = monthBounds(addDays(monthBounds(today()).start, -1));
  const { id: oldId } = await app.db.prepare(
    `INSERT INTO payments (student_id, coach_id, amount, lessons_count, tariff_code, attended, status, period_start, period_end, due_date, created_at)
     VALUES (?, ?, 150, 8, '8', 1, 'pending', ?, ?, ?, '2026-01-01') RETURNING id`,
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
  assert.equal((await app.post(`/api/staff/students/${studentId}`, 60)).data.student.lessons_remaining, 8);
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
  // Moving up is allowed all month, moving down only until the 5th.
  assert.equal((await app.post('/api/parent/tariff', 81, { student_id: studentId, tariff_code: '12' })).status, 200);
  const { data } = await app.post('/api/parent/context', 81);
  assert.equal(data.students[0].invoice.amount, 180);
  assert.equal(data.students[0].invoice.tariff_code, '12');
  const down = await app.post('/api/parent/tariff', 81, { student_id: studentId, tariff_code: '8' });
  if (canSelectTariff(today())) assert.equal(down.status, 200);
  else assert.equal(down.data.error, 'tariff_deadline_passed');
  assert.equal((await app.post('/api/parent/tariff', 81, { student_id: studentId, tariff_code: 'single' })).status, 400);
  assert.equal((await app.post('/api/parent/tariff', 81, { student_id: studentId, tariff_code: '99' })).status, 400);
});

test('money screen lists debtors and takes cash', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 90);
  const parentId = await addParent(app, 91);
  const studentId = await addStudent(app, { coachId, parentId });
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
  assert.equal((await app.post(`/api/staff/students/${studentId}`, 90)).data.student.lessons_remaining, 8);
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
  assert.equal((await app.post('/api/admin/money', SUPER_ADMIN)).data.collected, 200);
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

test('a listed username becomes an administrator on first contact', async () => {
  const app = createApp();
  app.env.ADMIN_USERNAMES = 'Some_Admin, Other_Admin';
  const start = (id, username) => app.webhook({
    message: { text: '/start', chat: { id, type: 'private' }, from: { id, first_name: 'Тест', username } },
  });
  await start(501, 'other_admin');
  await start(502, 'stranger');
  await start(503, undefined);
  const rows = (await app.db.prepare('SELECT telegram_id, is_admin, is_manager FROM coaches').all()).results;
  assert.deepEqual(rows.map((row) => ({ ...row })), [{ telegram_id: 501, is_admin: 1, is_manager: 1 }]);
});

test('scheduled tick connects the webhook only when it is missing', async () => {
  const app = createApp();
  const tick = new Date('2026-11-03T07:30:00Z');
  await runTick(app.env, tick);
  const setup = app.telegram.filter((call) => call.method === 'setWebhook');
  assert.equal(setup.length, 1);
  assert.equal(setup[0].payload.url, 'https://crm.example.dev/telegram/webhook');
  assert.equal(setup[0].payload.secret_token, app.env.TELEGRAM_WEBHOOK_SECRET);

  const mocked = globalThis.fetch;
  globalThis.fetch = async (url, options) => (String(url).endsWith('/getWebhookInfo')
    ? new Response(JSON.stringify({ ok: true, result: { url: setup[0].payload.url } }))
    : mocked(url, options));
  await runTick(app.env, tick);
  assert.equal(app.telegram.filter((call) => call.method === 'setWebhook').length, 1);
  globalThis.fetch = mocked;
});

test('a webhook failure does not stop the scheduled jobs', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 305);
  const parentId = await addParent(app, 306);
  await addStudent(app, { coachId, parentId, schedule: [{ weekday: 1, time: '18:00' }] });
  app.failures.getWebhookInfo = 401;
  await runTick(app.env, new Date('2026-11-03T07:30:00Z'));
  assert.equal(await rowCount(app, 'SELECT COUNT(*) AS count FROM payments'), 1);
  assert.equal(app.telegram.filter((call) => call.method === 'setWebhook').length, 0);
});

const studentRow = (app, id) => app.db.prepare('SELECT * FROM students WHERE id = ?').bind(id).first();
const mark = (app, student, date, status = 'present', source = 'scheduled') => saveAttendance(app.db, student, { date, time: '18:00', status, source });

test('the balance belongs to its calendar month', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 700);
  const id = await addStudent(app, { coachId, schedule: [{ weekday: 0, time: '18:00' }, { weekday: 2, time: '18:00' }] });
  const student = await studentRow(app, id);
  const october = await ensureInvoice(app.db, student, '2026-10-01');
  await mark(app, student, '2026-10-05');
  await mark(app, student, '2026-10-07');
  // Paying late does not matter: lessons held earlier in the month count.
  assert.equal(await markPaid(app.db, october, student, coachId), 6);

  // A new month starts from its own subscription, whatever was left or paid before.
  const november = await ensureInvoice(app.db, student, '2026-11-01');
  assert.equal(november.tariff_code, '8');
  assert.deepEqual(
    (({ total, used, left }) => ({ total, used, left }))(await balanceOf(app.db, id, '2026-11-15')),
    { total: 8, used: 0, left: 8 },
  );
  await mark(app, student, '2026-11-02');
  assert.equal((await balanceOf(app.db, id, '2026-11-15')).left, 7);

  // A late correction of October leaves November alone.
  await mark(app, student, '2026-10-12');
  assert.equal((await balanceOf(app.db, id, '2026-11-15')).left, 7);
  assert.equal((await balanceOf(app.db, id, '2026-10-15')).left, 5);

  // Makeups and trainer cancellations use no lesson.
  await mark(app, student, '2026-11-04', 'present', 'makeup');
  await mark(app, student, '2026-11-09', 'absent', 'coach_cancelled');
  assert.equal((await balanceOf(app.db, id, '2026-11-15')).used, 1);
});

test('paying an old debt does not change the current month', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 705);
  const id = await addStudent(app, { coachId });
  const student = await studentRow(app, id);
  const october = await ensureInvoice(app.db, student, '2026-10-01');
  await mark(app, student, '2026-10-05');
  await ensureInvoice(app.db, student, '2026-11-01');
  await mark(app, student, '2026-11-02');
  await mark(app, student, '2026-11-03');
  assert.equal(await markPaid(app.db, october, student, coachId), 7);
  assert.equal((await balanceOf(app.db, id, '2026-11-15')).left, 6);
});

test('a new student gets the subscription that fits the schedule', () => {
  assert.equal(tariffForSchedule(1), '8');
  assert.equal(tariffForSchedule(2), '8');
  assert.equal(tariffForSchedule(3), '12');
  assert.equal(tariffForSchedule(5), '16');
  // Fewer trainings than a subscription is worth are paid one by one; otherwise it is the whole subscription.
  assert.deepEqual(firstMonth(3, '8'), { partial: true, amount: 120, lessons: 3 });
  assert.deepEqual(firstMonth(4, '8'), { partial: false, amount: 150, lessons: 8 });
  assert.deepEqual(firstMonth(11, '12'), { partial: false, amount: 180, lessons: 12 });
  assert.deepEqual(firstMonth(4, '12'), { partial: true, amount: 160, lessons: 4 });
});

test('the first month is the trainings left only when that is cheaper than a subscription', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 710);
  // Mondays and Wednesdays from 18 November 2026: 18, 23, 25, 30.
  const id = await addStudent(app, { coachId, startDate: '2026-11-18', schedule: [{ weekday: 0, time: '18:00' }, { weekday: 2, time: '18:00' }] });
  const student = await studentRow(app, id);
  // Four trainings at 40 Br would cost more than the 150 Br subscription, so it is the whole subscription.
  const invoice = await ensureInvoice(app.db, student, '2026-11-18');
  assert.equal(invoice.is_partial, 0);
  assert.equal(invoice.lessons_count, 8);
  assert.equal(invoice.amount, 150);
  assert.equal(invoice.due_date, '2026-11-18');

  // The next month is a full subscription with the normal deadline.
  const december = await ensureInvoice(app.db, student, '2026-12-01');
  assert.equal(december.is_partial, 0);
  assert.equal(december.lessons_count, 8);
  assert.equal(december.due_date, '2026-12-10');

  // Few trainings left cost less than a subscription; a changed start date recalculates an unpaid invoice.
  await app.db.prepare('DELETE FROM payments').run();
  await app.db.prepare(`UPDATE students SET training_start_date = '2026-11-25' WHERE id = ?`).bind(id).run();
  const moved = await studentRow(app, id);
  const later = await ensureInvoice(app.db, moved, '2026-11-25');
  assert.equal(later.lessons_count, 2);
  assert.equal(later.amount, 80);
  await app.db.prepare(`UPDATE students SET training_start_date = '2026-11-30' WHERE id = ?`).bind(id).run();
  await refreshFirstInvoice(app.db, await studentRow(app, id), '2026-11-25');
  assert.equal((await ensureInvoice(app.db, await studentRow(app, id), '2026-11-25')).amount, 40);
});

test('a first invoice follows the schedule entered later and never falls due before the start', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 712);
  const id = await addStudent(app, { coachId, startDate: '2026-11-18' });
  const invoice = await ensureInvoice(app.db, await studentRow(app, id), '2026-11-15');
  assert.equal(invoice.is_partial, 0);
  assert.equal(invoice.due_date, '2026-11-18');
  await app.db.prepare(`INSERT INTO schedules (student_id, weekday, time) VALUES (?, 0, '18:00')`).bind(id).run();
  await refreshFirstInvoice(app.db, await studentRow(app, id), '2026-11-15');
  const refreshed = await ensureInvoice(app.db, await studentRow(app, id), '2026-11-15');
  assert.equal(refreshed.is_partial, 1);
  assert.equal(refreshed.lessons_count, 2);
  assert.equal(refreshed.amount, 80);
  assert.equal(refreshed.due_date, '2026-11-23');
});

test('a student starting next month has a working card and no invoice yet', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 713);
  const parentId = await addParent(app, 714);
  const next = addDays(monthBounds(today()).end, 1);
  const id = await addStudent(app, { coachId, parentId, startDate: next, schedule: [{ weekday: 0, time: '18:00' }] });
  const detail = await app.post(`/api/staff/students/${id}`, 713);
  assert.equal(detail.status, 200);
  assert.equal(detail.data.student.invoice, null);
  const [child] = (await app.post('/api/parent/context', 714)).data.students;
  assert.equal(child.invoice, null);
  assert.equal(child.starts_on, next);
  assert.ok(child.next_training.date >= next);
  const early = await app.post('/api/parent/training-response', 714, { student_id: id, date: addDays(today(), 1), time: '18:00', response: 'absent' });
  assert.ok(early.status >= 400);
  assert.equal((await app.post('/api/parent/tariff', 714, { student_id: id, tariff_code: '12' })).data.error, 'no_invoice');
  assert.equal((await app.post('/api/staff/students', 713)).status, 200);
  assert.equal((await app.post('/api/admin/money', SUPER_ADMIN)).status, 200);
});

test('an approved schedule change reprices an unpaid first month', async () => {
  const app = createApp();
  await addCoach(app, 716);
  const parentId = await addParent(app, 717);
  const coachId = (await app.db.prepare('SELECT id FROM coaches WHERE telegram_id = 716').first()).id;
  const { end } = monthBounds(today());
  // Starting on the last day of the month: one training on that weekday, none on the one after it.
  const lastWeekday = weekdayOf(end);
  const id = await addStudent(app, { coachId, parentId, startDate: end, schedule: [{ weekday: lastWeekday, time: '18:00' }] });
  if (end > monthBounds(today()).start) {
    const before = (await app.post('/api/parent/context', 717)).data.students[0].invoice;
    assert.equal(before.amount, 40);
    await app.post('/api/parent/schedule-request', 717, {
      student_id: id,
      proposed_schedule: [{ weekday: lastWeekday, time: '18:00' }, { weekday: lastWeekday, time: '20:00' }],
    });
    const request = (await app.post('/api/staff/requests', 716)).data.schedule_changes[0];
    assert.equal((await app.post(`/api/staff/schedule-requests/${request.id}/review`, 716, { decision: 'approve' })).status, 200);
    const after = (await app.post('/api/parent/context', 717)).data.students[0].invoice;
    assert.equal(after.amount, 80);
  }
});

test('a single lesson does not earn makeups for other missed trainings', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 718);
  const parentId = await addParent(app, 719);
  const id = await addStudent(app, { coachId, parentId });
  const student = await studentRow(app, id);
  await mark(app, student, today(), 'absent');
  const single = await app.post('/api/parent/single', 719, { student_id: id });
  await app.post(`/api/admin/payments/${single.data.id}/cash`, SUPER_ADMIN);
  assert.equal(await rowCount(app, 'SELECT COUNT(*) AS count FROM makeups'), 0);
});

test('an abandoned receipt on a closed month does not block new payments', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 721);
  const id = await addStudent(app, { coachId });
  const invoice = await ensureInvoice(app.db, await studentRow(app, id), '2026-09-01');
  await app.db.prepare(`UPDATE payments SET status = 'awaiting_receipt' WHERE id = ?`).bind(invoice.id).run();
  await closeUnusedMonths(app.db, '2026-10-01');
  assert.equal((await app.db.prepare('SELECT status FROM payments WHERE id = ?').bind(invoice.id).first()).status, 'skipped');
  assert.equal((await app.post(`/api/admin/payments/${invoice.id}/cash`, SUPER_ADMIN)).data.error, 'month_closed');
});

test('offers nobody paid get no reminders, and a failed daily job runs again', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 722);
  const parentId = await addParent(app, 723);
  const id = await addStudent(app, { coachId, parentId });
  // 3 November is 7 days before the subscription deadline and an unpaid single lesson would come first by id otherwise.
  await app.db.prepare(
    `INSERT INTO payments (student_id, coach_id, kind, amount, lessons_count, tariff_code, period_start, period_end, due_date, created_at)
     VALUES (?, ?, 'single', 40, 1, 'single', '2026-11-01', '2026-11-30', '2026-11-10', '2026-11-01')`,
  ).bind(id, coachId).run();
  const realPrepare = app.db.prepare;
  let failed = false;
  app.db.prepare = (sql) => {
    if (!failed && sql.includes(`SET status = 'skipped'`)) {
      failed = true;
      throw new Error('temporary failure');
    }
    return realPrepare(sql);
  };
  const morning = new Date('2026-11-03T07:30:00Z');
  await assert.rejects(runTick(app.env, morning));
  assert.equal(await rowCount(app, `SELECT COUNT(*) AS count FROM job_runs WHERE key = 'daily:2026-11-03'`), 0);
  await runTick(app.env, new Date('2026-11-03T07:35:00Z'));
  assert.equal(await rowCount(app, `SELECT COUNT(*) AS count FROM job_runs WHERE key = 'daily:2026-11-03'`), 1);
  const reminders = app.telegram.filter((call) => call.payload?.text?.includes('Оплата абонемента'));
  assert.equal(reminders.length, 1);
  assert.ok(reminders[0].payload.text.includes('150 Br'));
});

test('no invoice before the start month or without trainings left', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 715);
  const future = await addStudent(app, { coachId, startDate: '2026-12-01', schedule: [{ weekday: 0, time: '18:00' }] });
  assert.equal(await ensureInvoice(app.db, await studentRow(app, future), '2026-11-20'), null);
  // No Sunday remains after 30 November 2026, a Monday.
  const none = await addStudent(app, { coachId, startDate: '2026-11-30', schedule: [{ weekday: 6, time: '18:00' }] });
  assert.equal(await ensureInvoice(app.db, await studentRow(app, none), '2026-11-30'), null);
});

test('an unpaid month without training is not a debt and closes by itself', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 720);
  const id = await addStudent(app, { coachId });
  const student = await studentRow(app, id);
  const stored = (paymentId) => app.db.prepare('SELECT * FROM payments WHERE id = ?').bind(paymentId).first();

  const away = await ensureInvoice(app.db, student, '2026-09-01');
  await mark(app, student, '2026-09-07', 'absent');
  assert.equal((await stored(away.id)).attended, 0);
  assert.equal(await app.db.prepare('SELECT COUNT(*) AS count FROM makeups').first().then((row) => row.count), 0);
  await closeUnusedMonths(app.db, '2026-10-01');
  assert.equal((await stored(away.id)).status, 'skipped');

  // A training in a month makes the unpaid invoice a debt that stays open, even if it was already closed.
  await mark(app, student, '2026-09-14');
  assert.equal((await stored(away.id)).status, 'pending');
  assert.equal((await stored(away.id)).attended, 1);
  await closeUnusedMonths(app.db, '2026-10-01');
  assert.equal((await stored(away.id)).status, 'pending');
});

test('makeups need a paid month and appear when it gets paid', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 725);
  const id = await addStudent(app, { coachId });
  const student = await studentRow(app, id);
  const invoice = await ensureInvoice(app.db, student, '2026-11-01');
  const credits = () => rowCount(app, `SELECT COUNT(*) AS count FROM makeups WHERE status = 'available'`);
  await mark(app, student, '2026-11-02', 'absent');
  assert.equal(await credits(), 0);
  await markPaid(app.db, invoice, student, coachId);
  assert.equal(await credits(), 1);
  await mark(app, student, '2026-11-04', 'absent');
  assert.equal(await credits(), 2);
});

test('parents pay the difference to move up, and buy single lessons separately', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 730);
  const parentId = await addParent(app, 731);
  const id = await addStudent(app, { coachId, parentId, schedule: ALL_WEEK('23:59') });
  const context = async () => (await app.post('/api/parent/context', 731)).data.students[0];
  const pay = (paymentId) => app.post(`/api/admin/payments/${paymentId}/cash`, SUPER_ADMIN);

  assert.equal((await app.post('/api/parent/upgrade', 731, { student_id: id })).status, 409);
  await pay((await context()).invoice.id);
  const stayed = await context();
  assert.equal(stayed.lessons_count, 16);

  const payments = await app.db.prepare(`SELECT * FROM payments WHERE student_id = ? AND kind = 'subscription'`).bind(id).first();
  await app.db.prepare(`UPDATE payments SET tariff_code = '8', amount = 150, lessons_count = 8 WHERE id = ?`).bind(payments.id).run();
  const upgrade = await app.post('/api/parent/upgrade', 731, { student_id: id });
  assert.equal(upgrade.status, 200);
  assert.equal((await app.post('/api/parent/upgrade', 731, { student_id: id })).data.id, upgrade.data.id);
  const row = await app.db.prepare('SELECT * FROM payments WHERE id = ?').bind(upgrade.data.id).first();
  assert.equal(row.amount, 30);
  assert.equal(row.lessons_count, 4);
  assert.equal((await context()).lessons_count, 8);
  await pay(upgrade.data.id);
  assert.equal((await context()).lessons_count, 12);

  const nextUp = await app.post('/api/parent/upgrade', 731, { student_id: id });
  assert.equal((await app.db.prepare('SELECT amount FROM payments WHERE id = ?').bind(nextUp.data.id).first()).amount, 20);
  await pay(nextUp.data.id);
  assert.equal((await context()).lessons_count, 16);
  assert.equal((await app.post('/api/parent/upgrade', 731, { student_id: id })).status, 409);

  // Single lessons add up, and more than one fits in a month.
  for (const expected of [17, 18]) {
    const single = await app.post('/api/parent/single', 731, { student_id: id });
    assert.equal(single.status, 200);
    await pay(single.data.id);
    assert.equal((await context()).lessons_count, expected);
  }
  // The next month repeats the subscription, not the extras.
  const student = await studentRow(app, id);
  const next = addDays(monthBounds(today()).end, 1);
  assert.equal((await ensureInvoice(app.db, student, next)).tariff_code, '8');
  assert.equal((await balanceOf(app.db, id, next)).total, 8);
});

test('a single lesson covers a visit without creating a debt', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 735);
  const parentId = await addParent(app, 736);
  const id = await addStudent(app, { coachId, parentId });
  const student = await studentRow(app, id);
  const date = today();
  const single = await app.post('/api/parent/single', 736, { student_id: id });
  await app.post(`/api/admin/payments/${single.data.id}/cash`, SUPER_ADMIN);
  await mark(app, student, date);
  const invoice = await ensureInvoice(app.db, student, date);
  assert.equal(invoice.attended, 0);
});

test('administrators correct the balance only with a reason', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 740);
  const id = await addStudent(app, { coachId });
  const url = `/api/staff/students/${id}/adjust`;
  assert.equal((await app.post(url, 740, { delta: 1, reason: 'x' })).status, 403);
  assert.equal((await app.post(url, SUPER_ADMIN, { delta: 1, reason: ' ' })).data.error, 'reason_required');
  assert.equal((await app.post(url, SUPER_ADMIN, { delta: 0, reason: 'x' })).data.error, 'invalid_delta');
  assert.equal((await app.post(url, SUPER_ADMIN, { delta: 2, reason: 'Подарок' })).status, 200);
  const { student } = (await app.post(`/api/staff/students/${id}`, 740)).data;
  assert.equal(student.lessons_remaining, 10);
  assert.equal(await rowCount(app, `SELECT COUNT(*) AS count FROM audit_log WHERE action = 'lessons_adjusted'`), 1);
});

test('the parent is told once when lessons run past the subscription', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 745);
  const parentId = await addParent(app, 746);
  const date = today();
  const id = await addStudent(app, { coachId, parentId, schedule: [{ weekday: weekdayOf(date), time: '00:01' }] });
  const student = await studentRow(app, id);
  const invoice = await ensureInvoice(app.db, student, date);
  await app.db.prepare('UPDATE payments SET lessons_count = 0 WHERE id = ?').bind(invoice.id).run();
  const { data } = await app.post('/api/staff/day', 745, { date });
  const save = () => app.post('/api/staff/attendance', 745, { date, key: data.sessions[0].key, marks: [{ student_id: id, status: 'present' }] });
  await save();
  await save();
  const notes = await rowCount(app, `SELECT COUNT(*) AS count FROM outbox WHERE text LIKE '%больше, чем оплачено%'`);
  assert.equal(notes, 1);
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

test('an admin sets the month: subscription, price, payment and lessons left', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 730);
  // Started yesterday-like: a short first month counted from the start date.
  const id = await addStudent(app, { coachId, startDate: today(), schedule: [{ weekday: 0, time: '18:00' }] });
  const edit = (body, user = SUPER_ADMIN) => app.post(`/api/admin/students/${id}/month`, user, body);

  assert.equal((await edit({ tariff_code: '12', amount: 180, paid: true, lessons_left: 9 }, 730)).status, 403);
  assert.equal((await edit({ tariff_code: '9', amount: 180 })).data.error, 'invalid_tariff');
  assert.equal((await edit({ tariff_code: '12', amount: -5 })).data.error, 'invalid_amount');

  assert.equal((await edit({ tariff_code: '12', amount: 180, paid: true, lessons_left: 9 })).status, 200);
  const invoice = await ensureInvoice(app.db, await studentRow(app, id), today());
  assert.equal(invoice.is_partial, 0);
  assert.equal(invoice.lessons_count, 12);
  assert.equal(invoice.amount, 180);
  assert.equal(invoice.status, 'paid');
  assert.equal((await balanceOf(app.db, id, today())).left, 9);
  // Lessons used before the bot count as used: the subscription still shows all 12.
  assert.equal((await balanceOf(app.db, id, today())).total, 12);
  assert.equal((await studentRow(app, id)).tariff_code, '12');

  // A partial month with its own lesson count, and taking a payment back.
  assert.equal((await edit({ tariff_code: 'partial', lessons: 3, amount: 120, paid: false, lessons_left: 3 })).status, 200);
  const partial = await ensureInvoice(app.db, await studentRow(app, id), today());
  assert.equal(partial.is_partial, 1);
  assert.equal(partial.lessons_count, 3);
  assert.equal(partial.status, 'pending');
  assert.equal((await balanceOf(app.db, id, today())).left, 3);
  // The chosen full subscription stays for the next months.
  assert.equal((await studentRow(app, id)).tariff_code, '12');
});

test('parents choose a subscription in the form and it becomes the child subscription', async () => {
  const app = createApp();
  await addCoach(app, 731);
  const parent = { full_name: 'Анна Иванова', phone: '+375291112233' };
  const child = { name: 'Лев Иванов', nickname: 'Лёва', birthday: '2018-03-04', schedule: [{ weekday: 1, time: '18:00' }] };
  assert.equal((await app.post('/api/parent/register', 732, { parent, children: [{ ...child, tariff: '9' }] })).data.error, 'invalid_tariff');
  assert.equal((await app.post('/api/parent/register', 732, { parent, children: [{ ...child, tariff: '16' }] })).status, 200);
  const { data } = await app.post('/api/staff/requests', 731);
  assert.equal(data.registrations[0].preferred_tariff, '16');
  const approved = await app.post(`/api/staff/registrations/${data.registrations[0].id}/review`, 731, {
    decision: 'approve',
    start_date: monthBounds(today()).start,
  });
  const student = await studentRow(app, approved.data.student_id);
  assert.equal(student.tariff_code, '16');
  const invoice = await ensureInvoice(app.db, student, today());
  assert.equal(invoice.tariff_code, '16');
  assert.equal(invoice.lessons_count, 16);
  assert.equal(invoice.amount, 200);
});

test('the migration turns first months that cost a whole subscription into that subscription', async () => {
  const app = createApp();
  const coachId = await addCoach(app, 733);
  const full = await addStudent(app, { coachId });
  const short = await addStudent(app, { coachId });
  const insertPayment = (studentId, amount, lessons, code) => app.db.prepare(
    `INSERT INTO payments (student_id, coach_id, kind, amount, lessons_count, tariff_code, is_partial, status, period_start, period_end, due_date, created_at)
     VALUES (?, ?, 'subscription', ?, ?, ?, 1, 'paid', '2026-11-01', '2026-11-30', '2026-11-05', '2026-11-03')`,
  ).bind(studentId, coachId, amount, lessons, code).run();
  await insertPayment(full, 180, 11, '12');
  await insertPayment(short, 80, 2, '8');
  const sql = readFileSync(new URL('../migrations/0005_tariff_choice.sql', import.meta.url), 'utf8').replace(/^--.*$/gm, '');
  for (const statement of sql.split(';').map((part) => part.trim()).filter((part) => part.startsWith('UPDATE'))) {
    await app.db.prepare(statement).run();
  }
  const rows = await app.db.prepare('SELECT student_id, is_partial, lessons_count, amount, status FROM payments ORDER BY student_id').all();
  assert.deepEqual(rows.results.map((row) => [row.is_partial, row.lessons_count, row.amount, row.status]), [[0, 12, 180, 'paid'], [1, 2, 80, 'paid']]);
  assert.equal((await studentRow(app, full)).tariff_code, '12');
  assert.equal((await studentRow(app, short)).tariff_code, null);
});
