import { createHmac } from 'node:crypto';
import { readdirSync, readFileSync } from 'node:fs';
import { DatabaseSync } from 'node:sqlite';

import worker from '../src/index.js';

export const BOT_TOKEN = '123456:test-token';
export const WEBHOOK_SECRET = 'test-webhook-secret';
export const SUPER_ADMIN = 1001;

// D1-compatible wrapper over SQLite so tests run the real SQL.
class Statement {
  constructor(database, sql, args = []) {
    this.database = database;
    this.sql = sql;
    this.args = args;
  }

  bind(...args) {
    return new Statement(this.database, this.sql, args);
  }

  async first() {
    return this.database.prepare(this.sql).get(...this.args) ?? null;
  }

  async all() {
    return { results: this.database.prepare(this.sql).all(...this.args) };
  }

  async run() {
    const result = this.database.prepare(this.sql).run(...this.args);
    return { meta: { last_row_id: Number(result.lastInsertRowid), changes: Number(result.changes) } };
  }
}

function createD1() {
  const database = new DatabaseSync(':memory:');
  database.exec('PRAGMA foreign_keys = ON');
  for (const file of readdirSync('migrations').sort()) {
    database.exec(readFileSync(`migrations/${file}`, 'utf8'));
  }
  return {
    prepare: (sql) => new Statement(database, sql),
    async batch(statements) {
      database.exec('BEGIN');
      try {
        const results = [];
        for (const statement of statements) results.push(await statement.run());
        database.exec('COMMIT');
        return results;
      } catch (error) {
        database.exec('ROLLBACK');
        throw error;
      }
    },
  };
}

export function signInitData(userId, { authDate = Math.floor(Date.now() / 1000), token = BOT_TOKEN, firstName = 'Тест' } = {}) {
  const fields = { auth_date: String(authDate), user: JSON.stringify({ id: userId, first_name: firstName }) };
  const check = Object.keys(fields).sort().map((key) => `${key}=${fields[key]}`).join('\n');
  const secret = createHmac('sha256', 'WebAppData').update(token).digest();
  const hash = createHmac('sha256', secret).update(check).digest('hex');
  return new URLSearchParams({ ...fields, hash }).toString();
}

export function createApp() {
  const db = createD1();
  const telegram = [];
  const failures = {};
  const env = {
    DB: db,
    BOT_TOKEN,
    ADMIN_IDS: String(SUPER_ADMIN),
    ADMIN_SECRET: 'coach-code',
    TELEGRAM_WEBHOOK_SECRET: WEBHOOK_SECRET,
    WEBAPP_URL: 'https://crm.example.dev',
    PAYMENT_DETAILS: 'реквизиты',
    ASSETS: { fetch: async () => new Response('asset') },
  };
  globalThis.fetch = async (url, options = {}) => {
    const method = String(url).split('/').pop();
    let payload = options.body;
    if (typeof payload === 'string') payload = JSON.parse(payload);
    telegram.push({ method, payload });
    if (failures[method]) {
      return new Response(JSON.stringify({ ok: false, description: 'failed' }), { status: failures[method] });
    }
    const result = { message_id: 1, document: { file_id: 'telegram-file-id' }, username: 'crm_test_bot' };
    return new Response(JSON.stringify({ ok: true, result }));
  };
  const waits = [];
  const executionContext = { waitUntil: (promise) => waits.push(promise) };

  async function settle() {
    while (waits.length) await waits.shift();
  }

  async function post(path, userId, body = {}) {
    const initData = typeof userId === 'number' ? signInitData(userId) : userId;
    const response = await worker.fetch(
      new Request(`https://crm.example.dev${path}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, ...body }),
      }),
      env,
      executionContext,
    );
    await settle();
    const data = await response.clone().json().catch(() => null);
    return { status: response.status, data, response };
  }

  async function upload(path, userId, file) {
    const form = new FormData();
    form.append('initData', signInitData(userId));
    if (file) form.append('file', file);
    const response = await worker.fetch(
      new Request(`https://crm.example.dev${path}`, { method: 'POST', body: form }),
      env,
      executionContext,
    );
    await settle();
    return { status: response.status, data: await response.json() };
  }

  async function webhook(update, secret = WEBHOOK_SECRET) {
    const response = await worker.fetch(
      new Request('https://crm.example.dev/telegram/webhook', {
        method: 'POST',
        headers: { 'x-telegram-bot-api-secret-token': secret },
        body: JSON.stringify(update),
      }),
      env,
      executionContext,
    );
    await settle();
    return response;
  }

  return { env, db, telegram, failures, post, upload, webhook, settle };
}

export async function addCoach(app, telegramId, name = 'Тренер') {
  return (
    await app.db.prepare(
      `INSERT INTO coaches (telegram_id, first_name, created_at) VALUES (?, ?, '2026-01-01') RETURNING id`,
    ).bind(telegramId, name).first()
  ).id;
}

export async function addParent(app, telegramId, name = 'Родитель') {
  return (
    await app.db.prepare(
      `INSERT INTO parents (telegram_id, full_name, phone, created_at) VALUES (?, ?, '+375290000000', '2026-01-01') RETURNING id`,
    ).bind(telegramId, name).first()
  ).id;
}

export async function addStudent(app, { coachId, parentId = null, name = 'Ученик', startDate = null, schedule = [] }) {
  const { id } = await app.db.prepare(
    `INSERT INTO students (coach_id, parent_id, name, training_start_date, created_at)
     VALUES (?, ?, ?, ?, '2026-01-01') RETURNING id`,
  ).bind(coachId, parentId, name, startDate).first();
  for (const row of schedule) {
    await app.db.prepare('INSERT INTO schedules (student_id, weekday, time, location_id, group_id) VALUES (?, ?, ?, ?, ?)')
      .bind(id, row.weekday, row.time, row.location_id ?? null, row.group_id ?? null).run();
  }
  return id;
}
