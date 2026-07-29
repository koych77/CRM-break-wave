# CRM Break Wave

Telegram Mini App и бот для ежедневной работы тренера: ученики, расписание,
посещаемость и оплаты.

## Основные сценарии

- «Сегодня» показывает ключевые показатели, ближайшие группы и предупреждения.
- «Ученики» содержит единый поиск по имени, никнейму и телефонам.
- «Расписание» объединяет календарь и отметку посещаемости.
- «Отметка» позволяет быстро обработать всю группу.
- «Деньги» объединяет выручку, оплаты и единый список учеников, требующих внимания.

Просрочка оплаты не хранится как отдельное ручное состояние: она автоматически
определяется по `period_end`. Администраторы из `ADMIN_IDS` получают профиль
тренера при первом входе. Остальные тренеры регистрируются через `/coach <код>`.

## Локальный запуск

Требуется Python 3.11.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt -r requirements-dev.txt
Copy-Item .env.example .env
.\.venv\Scripts\python.exe main.py
```

Мини-приложение будет доступно на `http://localhost:8080`, проверка готовности —
на `http://localhost:8080/healthz`.

## Конфигурация

Обязательные переменные:

- `BOT_TOKEN` — токен Telegram-бота;
- `ADMIN_IDS` — Telegram ID администраторов через запятую;
- `ADMIN_SECRET` — секрет регистрации тренера;
- `WEBAPP_URL` — публичный HTTPS URL Railway;
- `DATABASE_URL` — строка подключения, по умолчанию SQLite.

Полный пример находится в `.env.example`.

## Проверки

```powershell
.\.venv\Scripts\python.exe -m compileall -q app main.py
node --check app/webapp/assets/app.js
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m pip_audit -r requirements.txt
```

GitHub Actions выполняет эти проверки для каждого pull request и push в
`master`.

## База данных и обновления

Единственный поддерживаемый механизм создания схемы и совместимых миграций —
`init_db()` и `run_migrations()` в `app/database.py`. Они запускаются из
`main.py` до API и Telegram polling.

Перед ручными операциями с production SQLite сохраните копию файла базы с
Railway volume. Не удаляйте legacy-поля расписания и абонемента вручную: они
пока служат слоем совместимости для существующих записей.

## Railway

`railway.json` задаёт команду запуска, перезапуск при сбое и проверку
`/healthz`. Production-деплой считается успешным только после состояния
`SUCCESS` и ответа healthcheck `200`.
