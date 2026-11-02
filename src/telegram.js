import { all, first, run } from './db.js';
import { isoNow } from './rules.js';
import { superAdminIds } from './auth.js';

const OUTBOX_BATCH = 40;

export class TelegramError extends Error {
  constructor(method, status, description) {
    super(`${method}: ${description}`);
    this.status = status;
  }
}

export async function callTelegram(env, method, payload) {
  const options = payload instanceof FormData
    ? { method: 'POST', body: payload }
    : { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload ?? {}) };
  const response = await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/${method}`, options);
  const data = await response.json().catch(() => ({}));
  if (!data.ok) throw new TelegramError(method, response.status, data.description || 'unknown error');
  return data.result;
}

// Every button in the bot opens the app through this one address.
export function openButton(env, label = 'Открыть CRM') {
  return { text: label, web_app: { url: `${env.WEBAPP_URL}/` } };
}

export function openMarkup(env, label) {
  return { inline_keyboard: [[openButton(env, label)]] };
}

export async function enqueue(env, chatId, text, { key = null, markup = null, buttonLabel = null } = {}) {
  if (!chatId) return;
  const replyMarkup = markup ?? (buttonLabel ? openMarkup(env, buttonLabel) : null);
  await run(
    env.DB,
    'INSERT OR IGNORE INTO outbox (dedupe_key, chat_id, text, reply_markup, created_at) VALUES (?, ?, ?, ?, ?)',
    key,
    chatId,
    text,
    replyMarkup ? JSON.stringify(replyMarkup) : null,
    isoNow(),
  );
}

export async function adminChatIds(env) {
  const rows = await all(
    env.DB,
    'SELECT telegram_id FROM coaches WHERE is_active = 1 AND (is_admin = 1 OR is_manager = 1)',
  );
  return [...new Set([...rows.map((row) => row.telegram_id), ...superAdminIds(env)])];
}

export async function notifyAdmins(env, text, buttonLabel = 'Открыть заявки') {
  for (const chatId of await adminChatIds(env)) {
    await enqueue(env, chatId, text, { buttonLabel });
  }
}

export async function notifyParentOfStudent(env, student, text, key = null) {
  if (!student.parent_id) return;
  const parent = await first(env.DB, 'SELECT telegram_id FROM parents WHERE id = ?', student.parent_id);
  if (parent) await enqueue(env, parent.telegram_id, text, { key, buttonLabel: 'Открыть кабинет' });
}

export async function flushOutbox(env, limit = OUTBOX_BATCH) {
  const pending = await all(env.DB, 'SELECT * FROM outbox WHERE sent_at IS NULL ORDER BY id LIMIT ?', limit);
  for (const message of pending) {
    let error = null;
    try {
      await callTelegram(env, 'sendMessage', {
        chat_id: message.chat_id,
        text: message.text,
        ...(message.reply_markup ? { reply_markup: JSON.parse(message.reply_markup) } : {}),
      });
    } catch (failure) {
      error = String(failure.message).slice(0, 500);
      if (failure.status === 403) {
        await run(env.DB, 'UPDATE parents SET bot_blocked_at = ? WHERE telegram_id = ?', isoNow(), message.chat_id);
      }
    }
    await run(env.DB, 'UPDATE outbox SET sent_at = ?, error = ? WHERE id = ?', isoNow(), error, message.id);
  }
  return pending.length;
}

// Upload through the parent's own chat: Telegram then keeps the file and gives a reusable id.
export async function storeReceipt(env, parentChatId, file, filename, caption) {
  const form = new FormData();
  form.append('chat_id', String(parentChatId));
  form.append('caption', 'Чек получен и отправлен на проверку.');
  form.append('document', file, filename);
  const sent = await callTelegram(env, 'sendDocument', form);
  const fileId = sent.document.file_id;
  for (const chatId of await adminChatIds(env)) {
    try {
      await callTelegram(env, 'sendDocument', { chat_id: chatId, document: fileId, caption });
    } catch {
      // The receipt stays available inside the app, so one failed copy is not fatal.
    }
  }
  return fileId;
}

export async function configureBot(env) {
  const secret = env.TELEGRAM_WEBHOOK_SECRET;
  await callTelegram(env, 'setWebhook', {
    url: `${env.WEBAPP_URL}/telegram/webhook`,
    secret_token: secret,
    allowed_updates: ['message', 'callback_query'],
    drop_pending_updates: true,
  });
  await callTelegram(env, 'setMyCommands', {
    commands: [
      { command: 'start', description: 'Открыть CRM' },
      { command: 'help', description: 'Помощь' },
    ],
  });
  await callTelegram(env, 'setChatMenuButton', {
    menu_button: { type: 'web_app', text: 'Открыть', web_app: { url: `${env.WEBAPP_URL}/` } },
  });
  return callTelegram(env, 'getWebhookInfo');
}
