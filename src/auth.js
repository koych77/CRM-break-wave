import { first, run } from './db.js';
import { ApiError } from './http.js';
import { isoNow } from './rules.js';

const MAX_AGE_SECONDS = 24 * 60 * 60;
const encoder = new TextEncoder();

async function hmac(keyBytes, message) {
  const key = await crypto.subtle.importKey('raw', keyBytes, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  return new Uint8Array(await crypto.subtle.sign('HMAC', key, encoder.encode(message)));
}

function toHex(bytes) {
  return [...bytes].map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

export function safeEqual(left, right) {
  if (left.length !== right.length) return false;
  let difference = 0;
  for (let index = 0; index < left.length; index += 1) {
    difference |= left.charCodeAt(index) ^ right.charCodeAt(index);
  }
  return difference === 0;
}

export async function verifyInitData(initData, botToken, now = Date.now()) {
  if (!initData || !botToken) return null;
  const params = new URLSearchParams(initData);
  const supplied = params.get('hash');
  if (!supplied) return null;
  params.delete('hash');
  const check = [...params.entries()]
    .map(([key, value]) => `${key}=${value}`)
    .sort()
    .join('\n');
  const secret = await hmac(encoder.encode('WebAppData'), botToken);
  const computed = toHex(await hmac(secret, check));
  if (!safeEqual(computed, supplied)) return null;
  const age = now / 1000 - Number(params.get('auth_date') || 0);
  if (!(age < MAX_AGE_SECONDS)) return null;
  try {
    const user = JSON.parse(params.get('user'));
    return user && user.id ? user : null;
  } catch {
    return null;
  }
}

export function superAdminIds(env) {
  return String(env.ADMIN_IDS || '')
    .split(/[,;]/)
    .map((value) => Number(value.trim()))
    .filter((value) => Number.isInteger(value) && value > 0);
}

function isListedUsername(env, username) {
  if (!username) return false;
  return String(env.ADMIN_USERNAMES || '')
    .split(',')
    .some((value) => value.trim().toLowerCase() === username.toLowerCase());
}

// An account in ADMIN_IDS or ADMIN_USERNAMES becomes an administrator and a coach on first contact.
// After that the row is bound to the Telegram ID, so the role can be changed in the team screen.
export async function loadCoach(env, telegramUser) {
  const { id, first_name: firstName, username } = telegramUser;
  const isSuper = superAdminIds(env).includes(id);
  let coach = await first(env.DB, 'SELECT * FROM coaches WHERE telegram_id = ?', id);
  if (!coach && (isSuper || isListedUsername(env, username))) {
    await run(
      env.DB,
      'INSERT OR IGNORE INTO coaches (telegram_id, first_name, username, is_admin, is_manager, created_at) VALUES (?, ?, ?, 1, 1, ?)',
      id,
      firstName,
      username,
      isoNow(),
    );
    coach = await first(env.DB, 'SELECT * FROM coaches WHERE telegram_id = ?', id);
  }
  if (!coach || (!coach.is_active && !isSuper)) return null;
  return { ...coach, is_super: isSuper, can_admin: Boolean(isSuper || coach.is_admin || coach.is_manager) };
}

export async function loadActor(env, initData) {
  const user = await verifyInitData(initData, env.BOT_TOKEN);
  if (!user) throw new ApiError(403, 'unauthorized', 'Откройте приложение из Telegram');
  const coach = await loadCoach(env, user);
  const parent = coach ? null : await first(env.DB, 'SELECT * FROM parents WHERE telegram_id = ?', user.id);
  return { user, coach, parent };
}

export function requireCoach(actor) {
  if (!actor.coach) throw new ApiError(403, 'forbidden', 'Нет доступа');
  return actor.coach;
}

export function requireAdmin(actor) {
  if (!actor.coach?.can_admin) throw new ApiError(403, 'forbidden', 'Нужны права администратора');
  return actor.coach;
}

export function requireParent(actor) {
  if (!actor.parent) throw new ApiError(403, 'unauthorized', 'Сначала заполните анкету');
  return actor.parent;
}
