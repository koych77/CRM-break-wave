import { loadCoach, safeEqual } from './auth.js';
import { first, run } from './db.js';
import { isoNow, validDate, validTime } from './rules.js';
import { saveTrainingResponse } from './parent.js';
import { adminChatIds, callTelegram, openMarkup } from './telegram.js';

function reply(env, chatId, text, label = 'Открыть CRM') {
  return callTelegram(env, 'sendMessage', { chat_id: chatId, text, reply_markup: openMarkup(env, label) });
}

async function registerCoach(env, message, code) {
  const chatId = message.chat.id;
  if (!env.ADMIN_SECRET) return reply(env, chatId, 'Регистрация тренеров сейчас отключена.');
  if (!code || !safeEqual(code, env.ADMIN_SECRET)) return reply(env, chatId, 'Неверный код.');
  const { id, first_name: firstName, username } = message.from;
  await run(
    env.DB,
    `INSERT INTO coaches (telegram_id, first_name, username, created_at) VALUES (?, ?, ?, ?)
     ON CONFLICT (telegram_id) DO UPDATE SET is_active = 1, first_name = excluded.first_name, username = excluded.username`,
    id,
    firstName,
    username,
    isoNow(),
  );
  return reply(env, chatId, 'Вы зарегистрированы как тренер.');
}

async function start(env, message) {
  const chatId = message.chat.id;
  const coach = await loadCoach(env, message.from);
  if (coach) return reply(env, chatId, 'Break Wave CRM готова к работе.');
  const parent = await first(env.DB, 'SELECT id FROM parents WHERE telegram_id = ?', message.from.id);
  if (parent) {
    await run(env.DB, 'UPDATE parents SET bot_blocked_at = NULL WHERE id = ?', parent.id);
    return reply(env, chatId, 'Семейный кабинет Break Wave: расписание, оплата и отработки.', 'Открыть кабинет');
  }
  return reply(env, chatId, 'Добро пожаловать в Break Wave. Заполните анкету, и тренер подтвердит занятия.', 'Заполнить анкету');
}

async function receiveReceipt(env, message) {
  const parent = await first(env.DB, 'SELECT * FROM parents WHERE telegram_id = ?', message.from.id);
  if (!parent) return;
  const payment = await first(
    env.DB,
    `SELECT p.*, s.name AS student_name FROM payments p JOIN students s ON s.id = p.student_id
     WHERE s.parent_id = ? AND p.status = 'awaiting_receipt' ORDER BY p.period_start, p.id LIMIT 1`,
    parent.id,
  );
  if (!payment) {
    await reply(env, message.chat.id, 'Сейчас нет оплаты, которая ждёт чек. Сначала выберите оплату в приложении.', 'Открыть кабинет');
    return;
  }
  const fileId = message.photo ? message.photo[message.photo.length - 1].file_id : message.document.file_id;
  const caption = `Чек на проверку\nРебёнок: ${payment.student_name}\nСумма: ${payment.amount} Br`;
  await run(
    env.DB,
    `UPDATE payments SET receipt_file_id = ?, status = 'reported', reported_at = ? WHERE id = ?`,
    fileId,
    isoNow(),
    payment.id,
  );
  for (const chatId of await adminChatIds(env)) {
    const method = message.photo ? 'sendPhoto' : 'sendDocument';
    const field = message.photo ? 'photo' : 'document';
    await callTelegram(env, method, { chat_id: chatId, [field]: fileId, caption }).catch(() => {});
  }
  await callTelegram(env, 'sendMessage', { chat_id: message.chat.id, text: 'Чек получен и отправлен на проверку.' });
}

async function trainingAnswer(env, callback) {
  const [, studentRaw, date, time, answer] = callback.data.split('|');
  const parent = await first(env.DB, 'SELECT id FROM parents WHERE telegram_id = ?', callback.from.id);
  const student = parent
    ? await first(env.DB, 'SELECT id FROM students WHERE id = ? AND parent_id = ? AND is_active = 1', Number(studentRaw), parent.id)
    : null;
  if (!student || !validDate(date) || !validTime(time)) {
    await callTelegram(env, 'answerCallbackQuery', { callback_query_id: callback.id, text: 'Не удалось сохранить ответ', show_alert: true });
    return;
  }
  await saveTrainingResponse(env.DB, student.id, date, time, answer === 'yes' ? 'attending' : 'absent');
  const label = answer === 'yes' ? 'Придёт' : 'Не придёт';
  await callTelegram(env, 'answerCallbackQuery', { callback_query_id: callback.id, text: `Ответ сохранён: ${label}` });
  if (callback.message) {
    await callTelegram(env, 'editMessageReplyMarkup', {
      chat_id: callback.message.chat.id,
      message_id: callback.message.message_id,
      reply_markup: {
        inline_keyboard: [
          [{ text: `Ответ: ${label}`, callback_data: callback.data }],
          [{ text: 'Открыть кабинет', web_app: { url: `${env.WEBAPP_URL}/` } }],
        ],
      },
    });
  }
}

export async function handleUpdate(env, update) {
  if (update.callback_query?.data?.startsWith('rsvp|')) {
    await trainingAnswer(env, update.callback_query);
    return;
  }
  const message = update.message;
  if (!message?.from || message.chat?.type !== 'private') return;
  if (message.photo || message.document) {
    await receiveReceipt(env, message);
    return;
  }
  const [command, ...rest] = String(message.text || '').trim().split(/\s+/);
  if (command === '/coach') {
    await registerCoach(env, message, rest.join(' '));
  } else if (command === '/help') {
    await reply(env, message.chat.id, 'Всё работает в приложении: откройте его кнопкой ниже или кнопкой меню слева от поля ввода.');
  } else {
    await start(env, message);
  }
}
