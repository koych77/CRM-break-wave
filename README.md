# CRM Break Wave

Единый Telegram Mini App и бот для руководителей, тренеров и родителей школы:
ученики, расписание, посещаемость, оплаты и отработки. Интерфейс определяется по
Telegram ID: новый пользователь видит анкету, родитель — семейный кабинет,
тренер и администратор — рабочие экраны.

Работает на Cloudflare Workers с базой D1. Все кнопки бота и кнопка меню открывают
адрес из `WEBAPP_URL`.

## Правила школы

- Разовое занятие — 40 Br; 8 занятий — 150 Br; 12 занятий — 180 Br; 16 занятий — 200 Br.
- Оплата до 10 числа. Пени нет, за пропуски деньги не возвращаются.
- До 5 числа родитель может сменить абонемент; иначе повторяется прошлый.
- Старый долг закрывается первым. Оплату подтверждает администратор.
- Запланированный пропуск списывает занятие и даёт отработку: не более 8 в месяц, срок 2 месяца.
- Отмена тренером не списывает занятие и даёт отработку сверх лимита.
- Пропущенная назначенная отработка сгорает.

Правила находятся в `src/rules.js` и `src/domain.js`.

## Структура

- `src/` — сервер: API, бот (webhook), задачи по расписанию.
- `public/` — Mini App.
- `migrations/` — схема D1.
- `test/` — тесты на настоящем SQL.

Регулярная задача запускается каждые 5 минут: напоминания, счета, сводки и отправка
очереди сообщений. Сообщения уходят через таблицу `outbox`, повторная отправка исключена.

## Разработка

```powershell
npm install
npm test
npm run migrate:local
npm run dev
```

Для локального запуска нужен файл `.dev.vars` с теми же секретами, что и в рабочей среде.

## Запуск на Cloudflare

```powershell
npx wrangler login
npx wrangler d1 create crm-break-wave-db
```

Подставьте полученный `database_id` в `wrangler.jsonc`, затем:

```powershell
npm run migrate:remote
npx wrangler secret put BOT_TOKEN
npx wrangler secret put ADMIN_IDS
npx wrangler secret put ADMIN_SECRET
npx wrangler secret put TELEGRAM_WEBHOOK_SECRET
npx wrangler secret put PAYMENT_DETAILS
npm run deploy
```

После первого запуска подключите бота: отправьте POST на `/telegram/setup` с заголовком
`X-Telegram-Bot-Api-Secret-Token`, равным `TELEGRAM_WEBHOOK_SECRET`. Этот запрос настраивает
webhook, команды и кнопку меню.

Тренеры регистрируются в боте командой `/coach <код>`, где код равен `ADMIN_SECRET`.
