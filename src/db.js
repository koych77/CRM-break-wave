import { isoNow } from './rules.js';

const clean = (args) => args.map((value) => (value === undefined ? null : value));

export async function all(db, sql, ...args) {
  return (await db.prepare(sql).bind(...clean(args)).all()).results;
}

export function first(db, sql, ...args) {
  return db.prepare(sql).bind(...clean(args)).first();
}

export function run(db, sql, ...args) {
  return db.prepare(sql).bind(...clean(args)).run();
}

export async function insert(db, sql, ...args) {
  return (await run(db, sql, ...args)).meta.last_row_id;
}

export function audit(db, actorTelegramId, action, entity, entityId, details = null) {
  return run(
    db,
    'INSERT INTO audit_log (actor_telegram_id, action, entity, entity_id, details, created_at) VALUES (?, ?, ?, ?, ?, ?)',
    actorTelegramId,
    action,
    entity,
    entityId,
    details ? JSON.stringify(details) : null,
    isoNow(),
  );
}

export function placeholders(values) {
  return values.map(() => '?').join(',');
}
