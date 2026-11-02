/* === Coach and administrator screens === */

const STAFF_ICONS = {
    today: 'M12 3a9 9 0 1 0 0 18 9 9 0 0 0 0-18zM8 12l3 3 5-6',
    students: 'M9 11a4 4 0 1 0 0-8 4 4 0 0 0 0 8zM2 21c0-4 3-6 7-6s7 2 7 6M17 4a4 4 0 0 1 0 7M19 15c2 1 3 3 3 6',
    requests: 'M4 4h16v12H8l-4 4V4zM8 9h8M8 12h5',
    money: 'M3 7h18v12H3V7zM3 7l3-3h12l3 3M16 13h2',
    team: 'M12 8a3 3 0 1 0 0-6 3 3 0 0 0 0 6zM5 22v-4a7 7 0 0 1 14 0v4',
};

const staff = {
    me: null,
    tab: 'today',
    screen: 'root',
    date: null,
    day: null,
    session: null,
    marks: {},
    confirmCancel: false,
    students: [],
    query: '',
    student: null,
    form: null,
    resources: {locations: [], groups: [], coaches: []},
    requests: null,
    rejecting: null,
    reason: '',
    receipts: {},
    money: null,
    team: null,
    reminded: {},
    copied: false,
    busy: false,
};

async function startStaff(me) {
    staff.me = me;
    await staffOpenTab('today');
}

function staffRoot() {
    return document.getElementById('parent-content');
}

async function staffLoad(loader) {
    try {
        await loader();
        staffPaint();
    } catch (error) {
        renderScreenState(staffRoot(), error.message, {retry: 'staffOpenTab(staff.tab)'});
    }
}

async function staffOpenTab(tab) {
    Object.assign(staff, {tab, screen: 'root', rejecting: null, reason: '', confirmCancel: false});
    const loaders = {
        today: staffLoadDay,
        students: staffLoadStudents,
        requests: staffLoadRequests,
        money: staffLoadMoney,
        team: staffLoadTeam,
    };
    await staffLoad(loaders[tab]);
    window.scrollTo(0, 0);
}

async function staffLoadDay() {
    staff.day = await apiPost('/api/staff/day', staff.date ? {date: staff.date} : {});
    staff.date = staff.day.date;
    if (staff.requests === null) await staffLoadRequests();
}

async function staffLoadStudents() {
    staff.students = (await apiPost('/api/staff/students')).students;
}

async function staffLoadRequests() {
    staff.requests = await apiPost('/api/staff/requests');
    staff.resources = {
        locations: staff.requests.locations,
        groups: staff.requests.groups,
        coaches: staff.requests.coaches,
    };
}

async function staffLoadMoney() {
    staff.money = await apiPost('/api/admin/money');
}

async function staffLoadTeam() {
    staff.team = await apiPost('/api/admin/team');
}

function staffRequestCount() {
    const data = staff.requests;
    if (!data) return 0;
    return data.registrations.length + data.makeups.length + data.schedule_changes.length + data.payments.length;
}

function staffNav() {
    const count = staffRequestCount();
    const items = [
        ['today', 'Сегодня'],
        ['students', 'Ученики'],
        ['requests', count ? `Заявки (${count})` : 'Заявки'],
    ];
    if (staff.me.is_admin) items.push(['money', 'Деньги'], ['team', 'Команда']);
    return `
        <nav class="st-nav" aria-label="Разделы">
            ${items.map(([id, label]) => `
                <button type="button" data-st="tab" data-value="${id}" ${staff.tab === id ? 'aria-current="page"' : ''}>
                    <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="${STAFF_ICONS[id]}"></path></svg>
                    <span>${label}</span>
                </button>`).join('')}
        </nav>`;
}

function staffPaint() {
    const screens = {
        attendance: staffAttendanceHtml,
        student: staffStudentHtml,
        form: staffFormHtml,
    };
    const tabs = {
        today: staffTodayHtml,
        students: staffStudentsHtml,
        requests: staffRequestsHtml,
        money: staffMoneyHtml,
        team: staffTeamHtml,
    };
    const scrollTop = window.scrollY;
    const body = staff.screen === 'root'
        ? `<div class="st-page">${tabs[staff.tab]()}</div>${staffNav()}`
        : screens[staff.screen]();
    staffRoot().innerHTML = `<div class="pa">${body}</div>`;
    window.scrollTo(0, scrollTop);
}

function staffBar(title, subtitle = '') {
    return `
        <div class="pa-bar">
            <button type="button" class="pa-back" data-st="back" aria-label="Назад">${PARENT_BACK_ICON}</button>
            <div>
                <h1>${escapeHtml(title)}</h1>
                ${subtitle ? `<div class="pa-muted pa-small">${escapeHtml(subtitle)}</div>` : ''}
            </div>
        </div>`;
}

/* --- Today --- */

function staffDayTitle() {
    const {date, today} = staff.day;
    if (date === today) return 'Сегодня';
    return PARENT_WEEKDAYS[parentDayIndex(parentDate(date))];
}

function staffPlace(session) {
    return [session.group ? `${session.group} группа` : null, session.location].filter(Boolean).join(', ') || 'Без группы и зала';
}

function staffTodayHtml() {
    const {sessions, date, today} = staff.day;
    const cards = sessions.map(session => {
        const absent = session.roster.filter(item => item.status === 'absent').length;
        let status = '<span class="pa-warn pa-small">Не отмечена</span>';
        if (session.cancelled) status = '<span class="pa-danger pa-small">Отменена</span>';
        else if (session.saved) status = `<span class="pa-ok pa-small">Есть ${session.roster.length - absent}, нет ${absent}</span>`;
        else if (date > today) status = '<span class="pa-muted pa-small">Впереди</span>';
        const label = session.saved ? 'Исправить отметку' : 'Отметить посещаемость';
        const action = session.cancelled || date > today
            ? ''
            : `<button type="button" class="pa-btn ${session.saved ? '' : 'is-primary'}" data-st="open-session" data-value="${escapeHtml(session.key)}">${label}</button>`;
        return `
            <section class="pa-card">
                <div class="pa-row">
                    <div class="pa-big">${escapeHtml(session.time)}</div>
                    ${status}
                </div>
                <div class="pa-muted">${escapeHtml(staffPlace(session))}, учеников: ${session.roster.length}</div>
                ${action}
                ${date > today && !session.cancelled ? `<button type="button" class="pa-btn" data-st="open-session" data-value="${escapeHtml(session.key)}">Состав и отмена</button>` : ''}
            </section>`;
    }).join('');
    return `
        <div class="st-title">
            <div>
                <h1>${escapeHtml(staffDayTitle())}</h1>
                <div class="pa-muted">${escapeHtml(parentLongDate(date))}</div>
            </div>
            <div class="st-arrows">
                <button type="button" class="pa-back" data-st="shift-day" data-value="-1" aria-label="Предыдущий день">${PARENT_BACK_ICON}</button>
                <button type="button" class="pa-back st-flip" data-st="shift-day" data-value="1" aria-label="Следующий день">${PARENT_BACK_ICON}</button>
            </div>
        </div>
        ${cards || '<div class="st-empty">В этот день тренировок нет</div>'}`;
}

function staffAttendanceHtml() {
    const session = staff.session;
    const future = staff.day.date > staff.day.today;
    const absentCount = session.roster.filter(item => staff.marks[item.student_id] === 'absent').length;
    const rows = session.roster.map(item => {
        const absent = staff.marks[item.student_id] === 'absent';
        return `
            <button type="button" class="pa-choice st-mark ${absent ? 'is-absent' : ''}" data-st="toggle-mark" data-value="${item.student_id}" aria-pressed="${absent}" ${future ? 'disabled' : ''}>
                <span class="pa-stack">
                    <span class="pa-strong">${escapeHtml(item.name)}</span>
                    ${item.tag ? `<span class="pa-muted pa-small">${escapeHtml(item.tag)}</span>` : ''}
                </span>
                ${future ? '' : `<span class="${absent ? 'pa-warn' : 'pa-ok'}">${absent ? 'Нет' : 'Есть'}</span>`}
            </button>`;
    }).join('');
    const cancel = staff.confirmCancel
        ? `<section class="pa-card">
                <div class="pa-mid">Отменить тренировку?</div>
                <div class="pa-muted">Занятие не спишется. Все ученики получат отработку, а родители — уведомление.</div>
                <div class="pa-two">
                    <button type="button" class="pa-btn st-danger" data-st="cancel-session" ${staff.busy ? 'disabled' : ''}>Отменить</button>
                    <button type="button" class="pa-btn" data-st="keep-session">Не отменять</button>
                </div>
           </section>`
        : '<button type="button" class="pa-link st-danger-link" data-st="ask-cancel">Отменить тренировку</button>';
    return `
        ${staffBar(`${session.time}, ${session.group || 'Тренировка'}`, future ? parentLongDate(staff.day.date) : 'Нажмите на тех, кого нет')}
        <div class="pa-body">
            ${rows}
            ${cancel}
        </div>
        ${future ? '' : `<div class="pa-foot">
            <button type="button" class="pa-btn is-primary" data-st="save-marks" ${staff.busy ? 'disabled' : ''}>${staff.busy ? 'Сохраняем…' : `Сохранить: есть ${session.roster.length - absentCount}, нет ${absentCount}`}</button>
        </div>`}`;
}

/* --- Students --- */

function staffStudentRows() {
    const query = staff.query.trim().toLowerCase();
    const rows = staff.students.filter(item => !query
        || [item.name, item.parent_name, item.phone, item.parent_phone].some(value => String(value || '').toLowerCase().includes(query)));
    if (!rows.length) return '<div class="st-empty">Никого не нашли</div>';
    const flags = {debt: ['Долг', 'pa-danger'], low: ['Мало занятий', 'pa-warn']};
    return rows.map(item => {
        const flag = flags[item.flag];
        return `
            <button type="button" class="pa-choice" data-st="open-student" data-value="${item.id}">
                <span class="pa-stack">
                    <span class="pa-strong">${escapeHtml(item.name)}</span>
                    <span class="pa-muted pa-small">${item.group ? `${escapeHtml(item.group)}, ` : ''}занятий: ${item.lessons_remaining}</span>
                </span>
                ${flag ? `<span class="${flag[1]} pa-small">${flag[0]}</span>` : ''}
            </button>`;
    }).join('');
}

function staffStudentsHtml() {
    return `
        <div class="st-title">
            <h1>Ученики</h1>
            <button type="button" class="pa-link" data-st="new-student">Добавить</button>
        </div>
        <label class="pa-stack">
            <span class="pa-label">Поиск по имени или телефону</span>
            <input class="pa-field" type="search" data-st-input="query" value="${escapeHtml(staff.query)}">
        </label>
        <div class="pa-group" id="st-student-list">${staffStudentRows()}</div>`;
}

function staffStudentHtml() {
    const item = staff.student;
    const invoiceLabels = {
        paid: ['Оплачено', 'pa-ok'],
        pending: [`До ${parentShortDate(item.invoice.due_date)}`, 'pa-warn'],
        overdue: ['Долг', 'pa-danger'],
        reported: ['На проверке', 'pa-warn'],
        awaiting_receipt: ['Ждём чек', 'pa-warn'],
        rejected: ['Отклонено', 'pa-danger'],
    };
    const invoice = invoiceLabels[item.invoice.status] || ['', ''];
    const historyLabels = {present: ['Был', 'pa-ok'], absent: ['Не был', 'pa-warn']};
    const history = item.history.map(row => {
        const [label, css] = row.source === 'coach_cancelled' ? ['Отмена', 'pa-muted'] : historyLabels[row.status];
        return `<div class="pa-row"><span>${escapeHtml(parentShortDate(row.date))}${row.source === 'makeup' ? ', отработка' : ''}</span><span class="${css}">${label}</span></div>`;
    }).join('');
    const schedule = item.schedule.map(row => `
        <div class="pa-row"><span>${PARENT_WEEKDAYS[row.weekday]}</span><span class="pa-muted">${escapeHtml(row.time)}</span></div>`).join('');
    return `
        ${staffBar(item.name)}
        <div class="pa-body">
            <section class="pa-card">
                <div class="pa-row"><span class="pa-muted">Осталось занятий</span><span class="pa-strong">${item.lessons_remaining} из ${item.lessons_count}</span></div>
                <div class="pa-row"><span class="pa-muted">Отработки</span><span>${item.makeups}</span></div>
                <div class="pa-row"><span class="pa-muted">Оплата за ${escapeHtml(PARENT_MONTHS_NOMINATIVE[parentDate(item.invoice.due_date).getMonth()].toLowerCase())}, ${item.invoice.amount} Br</span><span class="${invoice[1]}">${invoice[0]}</span></div>
            </section>
            <section class="pa-card">
                <div class="pa-label">Расписание</div>
                ${schedule || '<div>Не назначено</div>'}
            </section>
            <section class="pa-card">
                <div>
                    <div class="pa-label">Родитель</div>
                    <div class="pa-mid">${escapeHtml(item.parent_name || 'Не привязан')}</div>
                    ${item.parent_phone ? `<div class="pa-muted">${escapeHtml(item.parent_phone)}</div>` : ''}
                </div>
                ${item.parent_phone ? `<a class="pa-btn" href="tel:${escapeHtml(item.parent_phone.replace(/[^+\d]/g, ''))}">Позвонить</a>` : ''}
            </section>
            ${item.notes ? `<section class="pa-card"><div class="pa-label">Заметки</div><div>${escapeHtml(item.notes)}</div></section>` : ''}
            ${history ? `<section class="pa-card"><div class="pa-label">Последние занятия</div>${history}</section>` : ''}
            <button type="button" class="pa-btn" data-st="edit-student">Изменить</button>
            ${staff.me.is_admin ? '<button type="button" class="pa-link st-danger-link" data-st="delete-student">Удалить ученика</button>' : ''}
        </div>`;
}

function staffOptions(items, selected, labelKey = 'name', emptyLabel = 'Не выбрано') {
    return `<option value="">${emptyLabel}</option>` + items.map(item =>
        `<option value="${item.id}" ${Number(selected) === item.id ? 'selected' : ''}>${escapeHtml(item[labelKey])}</option>`
    ).join('');
}

function staffFormHtml() {
    const form = staff.form;
    const weekdays = selected => PARENT_WEEKDAYS.map((label, index) =>
        `<option value="${index}" ${index === Number(selected) ? 'selected' : ''}>${label}</option>`
    ).join('');
    const field = (label, name, type = 'text') => `
        <label class="pa-stack">
            <span class="pa-label">${label}</span>
            <input class="pa-field" type="${type}" ${type === 'number' ? 'min="0" inputmode="numeric"' : ''} data-st-form="${name}" value="${escapeHtml(form[name] ?? '')}">
        </label>`;
    const rows = form.schedule.map((row, index) => `
        <div class="st-schedule">
            <div class="pa-schedule-row">
                <select class="pa-field" aria-label="День" data-st-row="${index}" data-field="weekday">${weekdays(row.weekday)}</select>
                <input class="pa-field" type="time" aria-label="Время" data-st-row="${index}" data-field="time" value="${escapeHtml(row.time)}">
                <button type="button" class="pa-remove" data-st="remove-row" data-value="${index}" aria-label="Убрать">×</button>
            </div>
            <div class="pa-two">
                <select class="pa-field" aria-label="Группа" data-st-row="${index}" data-field="group_id">${staffOptions(staff.resources.groups, row.group_id, 'name', 'Группа')}</select>
                <select class="pa-field" aria-label="Зал" data-st-row="${index}" data-field="location_id">${staffOptions(staff.resources.locations, row.location_id, 'name', 'Зал')}</select>
            </div>
        </div>`).join('');
    return `
        ${staffBar(form.id ? 'Изменить ученика' : 'Новый ученик')}
        <div class="pa-body">
            <section class="pa-card">
                ${field('Имя и фамилия', 'name')}
                ${field('Дата рождения', 'birthday', 'date')}
                ${field('Телефон ученика', 'phone', 'tel')}
                ${form.parent_linked ? '' : field('Телефон родителя', 'parent_phone', 'tel')}
                ${staff.me.is_admin ? `
                    <label class="pa-stack">
                        <span class="pa-label">Тренер</span>
                        <select class="pa-field" data-st-form="coach_id">${staffOptions(staff.resources.coaches, form.coach_id, 'first_name', 'Я')}</select>
                    </label>` : ''}
            </section>
            <section class="pa-card">
                <div class="pa-two">
                    ${field('Занятий в абонементе', 'lessons_count', 'number')}
                    ${field('Осталось занятий', 'lessons_remaining', 'number')}
                </div>
                ${field('Дата начала занятий', 'training_start_date', 'date')}
            </section>
            <section class="pa-card">
                <div class="pa-label">Расписание</div>
                ${rows}
                <button type="button" class="pa-btn" data-st="add-row">Добавить день</button>
            </section>
            <section class="pa-card">
                <label class="pa-stack">
                    <span class="pa-label">Заметки</span>
                    <textarea class="pa-field st-textarea" data-st-form="notes">${escapeHtml(form.notes ?? '')}</textarea>
                </label>
            </section>
        </div>
        <div class="pa-foot">
            <button type="button" class="pa-btn is-primary" data-st="save-student" ${staff.busy ? 'disabled' : ''}>${staff.busy ? 'Сохраняем…' : 'Сохранить'}</button>
        </div>`;
}

/* --- Requests --- */

function staffScheduleText(rows) {
    return rows.map(row => `${PARENT_WEEKDAYS[row.weekday]} ${row.time}`).join(', ') || 'нет';
}

function staffDecision(kind, id) {
    const key = `${kind}:${id}`;
    if (staff.rejecting === key) {
        const empty = !staff.reason.trim();
        return `
            <div class="pa-group">
                <label class="pa-stack">
                    <span class="pa-label">Причина, её увидит родитель</span>
                    <input class="pa-field" data-st-input="reason" value="${escapeHtml(staff.reason)}">
                </label>
                <div class="pa-two">
                    <button type="button" class="pa-btn is-primary" data-st="confirm-reject" data-kind="${kind}" data-value="${id}" ${empty ? 'data-empty="1"' : ''}>Отправить отказ</button>
                    <button type="button" class="pa-btn" data-st="cancel-reject">Назад</button>
                </div>
            </div>`;
    }
    return `
        <div class="pa-two">
            <button type="button" class="pa-btn is-primary" data-st="approve" data-kind="${kind}" data-value="${id}">Подтвердить</button>
            <button type="button" class="pa-btn" data-st="reject" data-kind="${kind}" data-value="${id}">Отказать</button>
        </div>`;
}

function staffRequestsHtml() {
    const data = staff.requests;
    const {groups, locations, coaches} = staff.resources;
    const today = staff.day?.today || '';
    const registrations = data.registrations.map(item => `
        <section class="pa-card" data-reg-card="${item.id}">
            <div>
                <div class="pa-label">Новая семья</div>
                <div class="pa-mid">${escapeHtml(item.child_name)}</div>
                <div class="pa-muted">Родитель: ${escapeHtml(item.parent_name)}, ${escapeHtml(item.parent_phone)}</div>
                <div class="pa-muted">Удобно: ${escapeHtml(staffScheduleText(item.proposed_schedule))}</div>
            </div>
            ${data.linkable_students.length ? `
                <label class="pa-stack">
                    <span class="pa-label">Если ребёнок уже есть в списке</span>
                    <select class="pa-field" data-review="link_student_id">${staffOptions(data.linkable_students, null, 'name', 'Новый ученик')}</select>
                </label>` : ''}
            <div class="pa-two">
                <label class="pa-stack">
                    <span class="pa-label">Группа</span>
                    <select class="pa-field" data-review="group_id">${staffOptions(groups, null)}</select>
                </label>
                <label class="pa-stack">
                    <span class="pa-label">Зал</span>
                    <select class="pa-field" data-review="location_id">${staffOptions(locations, null)}</select>
                </label>
            </div>
            <label class="pa-stack">
                <span class="pa-label">Начало занятий</span>
                <input class="pa-field" type="date" data-review="start_date" value="${escapeHtml(today)}">
            </label>
            ${staff.me.is_admin ? `
                <label class="pa-stack">
                    <span class="pa-label">Тренер</span>
                    <select class="pa-field" data-review="coach_id">${staffOptions(coaches, null, 'first_name', 'Я')}</select>
                </label>` : ''}
            ${staffDecision('registration', item.id)}
        </section>`).join('');
    const makeups = data.makeups.map(item => `
        <section class="pa-card">
            <div>
                <div class="pa-label">Отработка</div>
                <div class="pa-mid">${escapeHtml(item.student_name)}</div>
                <div class="pa-muted">Хочет прийти: ${escapeHtml(parentLongDate(item.requested_date))}${item.requested_time ? `, ${escapeHtml(item.requested_time)}` : ''}</div>
            </div>
            ${staffDecision('makeup', item.id)}
        </section>`).join('');
    const schedules = data.schedule_changes.map(item => `
        <section class="pa-card">
            <div>
                <div class="pa-label">Смена расписания</div>
                <div class="pa-mid">${escapeHtml(item.student_name)}</div>
                <div class="pa-muted">Сейчас: ${escapeHtml(staffScheduleText(item.current_schedule))}</div>
                <div class="pa-muted">Просит: ${escapeHtml(staffScheduleText(item.proposed_schedule))}</div>
            </div>
            ${staffDecision('schedule', item.id)}
        </section>`).join('');
    const payments = data.payments.map(item => {
        const receipt = staff.receipts[item.id];
        let preview = '';
        if (item.has_receipt) {
            preview = receipt
                ? (receipt.type.startsWith('image/')
                    ? `<img class="st-receipt" src="${receipt.url}" alt="Чек">`
                    : `<a class="pa-btn" href="${receipt.url}" target="_blank" rel="noopener">Открыть файл чека</a>`)
                : `<button type="button" class="pa-btn" data-st="load-receipt" data-value="${item.id}">Показать чек</button>`;
        }
        return `
            <section class="pa-card" data-pay-card="${item.id}">
                <div>
                    <div class="pa-label">${item.method === 'cash' ? 'Наличные' : 'Чек на проверку'}</div>
                    <div class="pa-mid">${escapeHtml(item.student_name)}, ${item.amount} Br</div>
                    <div class="pa-muted">${escapeHtml(PARENT_MONTHS_NOMINATIVE[parentDate(item.period_start).getMonth()])}</div>
                </div>
                ${preview}
                ${item.method === 'cash' ? `
                    <label class="pa-stack">
                        <span class="pa-label">Кто принял деньги</span>
                        <select class="pa-field" data-review="received_by_coach_id">${staffOptions(coaches, null, 'first_name', 'Я')}</select>
                    </label>` : ''}
                ${staffDecision('payment', item.id)}
            </section>`;
    }).join('');
    const content = registrations + makeups + schedules + payments;
    return `
        <div class="st-title"><h1>Заявки</h1></div>
        ${content || '<div class="st-empty">Все заявки разобраны</div>'}`;
}

/* --- Money and team --- */

function staffMoneyHtml() {
    const data = staff.money;
    const rows = data.debtors.map(item => {
        const overdue = item.status === 'overdue';
        const month = PARENT_MONTHS_NOMINATIVE[parentDate(item.period_start).getMonth()];
        const note = overdue ? `${month}: просрочено` : `${month}: до ${parentShortDate(item.due_date)}`;
        const reminded = staff.reminded[item.id];
        return `
            <section class="pa-card">
                <div class="pa-row">
                    <div class="pa-mid">${escapeHtml(item.student_name)}</div>
                    <div class="pa-mid">${item.amount} Br</div>
                </div>
                <div class="${overdue ? 'pa-danger' : 'pa-warn'} pa-small">${escapeHtml(note)}</div>
                <div class="pa-two">
                    <button type="button" class="pa-btn" data-st="take-cash" data-value="${item.id}">Принять наличные</button>
                    <button type="button" class="pa-btn" data-st="remind" data-value="${item.id}" ${reminded || !item.can_remind ? 'disabled' : ''}>${reminded ? 'Отправлено' : item.can_remind ? 'Напомнить' : 'Нет родителя'}</button>
                </div>
            </section>`;
    }).join('');
    return `
        <div class="st-title">
            <div>
                <h1>Деньги</h1>
                <div class="pa-muted">${escapeHtml(PARENT_MONTHS_NOMINATIVE[Number(data.month.slice(5)) - 1])}</div>
            </div>
        </div>
        <div class="pa-two">
            <div class="st-tile"><div class="pa-label">Получено</div><div class="pa-big">${data.collected} Br</div></div>
            <div class="st-tile"><div class="pa-label">Ещё ждём</div><div class="pa-big">${data.waiting} Br</div></div>
        </div>
        ${data.in_review ? `<button type="button" class="pa-btn" data-st="tab" data-value="requests">На проверке: ${data.in_review}</button>` : ''}
        <h2 class="pa-mid">Не оплатили</h2>
        ${rows || '<div class="st-empty">Все оплатили</div>'}`;
}

function staffResourceList(title, items, kind, placeholder) {
    return `
        <section class="pa-card">
            <div class="pa-mid">${title}</div>
            ${items.map(item => `
                <div class="pa-row st-resource">
                    <span>${escapeHtml(item.name)}</span>
                    <button type="button" class="pa-remove" data-st="remove-resource" data-kind="${kind}" data-value="${item.id}" aria-label="Убрать ${escapeHtml(item.name)}">×</button>
                </div>`).join('') || '<div class="pa-muted">Пока пусто</div>'}
            <div class="st-add">
                <input class="pa-field" aria-label="${placeholder}" placeholder="${placeholder}" data-new-resource="${kind}">
                <button type="button" class="pa-btn" data-st="add-resource" data-kind="${kind}">Добавить</button>
            </div>
        </section>`;
}

function staffTeamHtml() {
    const data = staff.team;
    const role = (coach, key, label) => `
        <button type="button" class="st-role" data-st="toggle-role" data-value="${coach.id}" data-role="${key}" aria-pressed="${coach[key]}" ${coach.locked ? 'disabled' : ''}>${label}</button>`;
    return `
        <div class="st-title"><h1>Команда</h1></div>
        ${data.coaches.map(coach => `
            <section class="pa-card">
                <div class="pa-mid">${escapeHtml(coach.name)}</div>
                <div class="st-roles">
                    <span class="st-role is-fixed">Тренер</span>
                    ${role(coach, 'is_admin', 'Администратор')}
                    ${role(coach, 'is_manager', 'Руководитель')}
                </div>
            </section>`).join('')}
        <section class="pa-card">
            <div>
                <div class="pa-mid">Регистрация семей</div>
                <div class="pa-muted">Отправьте родителям ссылку, и анкета придёт в раздел «Заявки».</div>
            </div>
            <button type="button" class="pa-btn is-primary" data-st="copy-link">${staff.copied ? 'Ссылка скопирована' : 'Скопировать ссылку'}</button>
        </section>
        ${staffResourceList('Залы', data.locations, 'locations', 'Новый зал')}
        ${staffResourceList('Группы', data.groups, 'groups', 'Новая группа')}`;
}

/* --- Actions --- */

async function staffRun(action, {reload = null} = {}) {
    staff.busy = true;
    try {
        await action();
        if (reload) await reload();
    } catch (error) {
        showNotification(error.message, 'error');
    }
    staff.busy = false;
    staffPaint();
}

function staffOpenSession(key) {
    const session = staff.day.sessions.find(item => item.key === key);
    staff.session = session;
    staff.marks = Object.fromEntries(session.roster.map(item => [item.student_id, item.status]));
    staff.confirmCancel = false;
    staff.screen = 'attendance';
    staffPaint();
    window.scrollTo(0, 0);
}

async function staffOpenStudent(id) {
    await staffRun(async () => {
        if (!staff.resources.locations.length && !staff.requests) await staffLoadRequests();
        staff.student = (await apiPost(`/api/staff/students/${id}`)).student;
        staff.screen = 'student';
    });
    window.scrollTo(0, 0);
}

function staffStartForm(student) {
    staff.form = student
        ? {
            id: student.id,
            name: student.name,
            birthday: student.birthday,
            phone: student.phone,
            parent_phone: student.parent_phone,
            parent_linked: Boolean(student.parent_name),
            coach_id: student.coach_id,
            lessons_count: student.lessons_count,
            lessons_remaining: student.lessons_remaining,
            training_start_date: student.training_start_date,
            notes: student.notes,
            schedule: student.schedule.map(row => ({...row})),
        }
        : {
            id: null,
            name: '',
            birthday: '',
            phone: '',
            parent_phone: '',
            coach_id: null,
            lessons_count: 8,
            lessons_remaining: 8,
            training_start_date: '',
            notes: '',
            schedule: [{weekday: 0, time: '18:00', group_id: null, location_id: null}],
        };
    staff.screen = 'form';
    staffPaint();
    window.scrollTo(0, 0);
}

function staffCollectReview(button) {
    const card = button.closest('[data-reg-card], [data-pay-card]');
    const values = {};
    card?.querySelectorAll('[data-review]').forEach(input => {
        if (input.value) values[input.dataset.review] = input.value;
    });
    return values;
}

const STAFF_REVIEW_PATHS = {
    registration: id => `/api/staff/registrations/${id}/review`,
    makeup: id => `/api/staff/makeups/${id}/review`,
    schedule: id => `/api/staff/schedule-requests/${id}/review`,
    payment: id => `/api/admin/payments/${id}/review`,
};

const STAFF_ACTIONS = {
    tab: target => staffOpenTab(target.dataset.value),
    back: () => {
        if (staff.screen === 'form' && staff.form.id) {
            staff.screen = 'student';
            staffPaint();
            return;
        }
        staffOpenTab(staff.tab);
    },
    'shift-day': target => {
        const next = parentDate(staff.date);
        next.setDate(next.getDate() + Number(target.dataset.value));
        staff.date = `${next.getFullYear()}-${String(next.getMonth() + 1).padStart(2, '0')}-${String(next.getDate()).padStart(2, '0')}`;
        staffLoad(staffLoadDay);
    },
    'open-session': target => staffOpenSession(target.dataset.value),
    'toggle-mark': target => {
        const id = target.dataset.value;
        staff.marks[id] = staff.marks[id] === 'absent' ? 'present' : 'absent';
        staffPaint();
    },
    'ask-cancel': () => {
        staff.confirmCancel = true;
        staffPaint();
    },
    'keep-session': () => {
        staff.confirmCancel = false;
        staffPaint();
    },
    'cancel-session': () => staffRun(async () => {
        await apiPost('/api/staff/cancel-session', {date: staff.day.date, key: staff.session.key});
        staff.screen = 'root';
        showNotification('Тренировка отменена');
    }, {reload: staffLoadDay}),
    'save-marks': () => staffRun(async () => {
        await apiPost('/api/staff/attendance', {
            date: staff.day.date,
            key: staff.session.key,
            marks: Object.entries(staff.marks).map(([student_id, status]) => ({student_id: Number(student_id), status})),
        });
        staff.screen = 'root';
        showNotification('Посещаемость сохранена');
    }, {reload: staffLoadDay}),
    'open-student': target => staffOpenStudent(target.dataset.value),
    'new-student': () => staffRun(async () => {
        if (!staff.requests) await staffLoadRequests();
        staffStartForm(null);
    }),
    'edit-student': () => staffStartForm(staff.student),
    'add-row': () => {
        staff.form.schedule.push({weekday: 0, time: '18:00', group_id: staff.form.schedule[0]?.group_id ?? null, location_id: staff.form.schedule[0]?.location_id ?? null});
        staffPaint();
    },
    'remove-row': target => {
        staff.form.schedule.splice(Number(target.dataset.value), 1);
        staffPaint();
    },
    'save-student': () => staffRun(async () => {
        const {id, schedule, parent_linked: _linked, ...fields} = staff.form;
        const path = id ? `/api/staff/students/${id}/update` : '/api/staff/students/create';
        const saved = await apiPost(path, {student: fields, schedule});
        await staffLoadStudents();
        staff.student = (await apiPost(`/api/staff/students/${id || saved.id}`)).student;
        staff.tab = 'students';
        staff.screen = 'student';
        showNotification('Сохранено');
    }),
    'delete-student': () => {
        tgConfirm(`Удалить ${staff.student.name}? Личные данные, расписание и отработки будут стёрты, это нельзя отменить.`, () => staffRun(async () => {
            await apiPost(`/api/staff/students/${staff.student.id}/delete`);
            staff.screen = 'root';
            await staffLoadStudents();
            showNotification('Ученик удалён');
        }));
    },
    approve: target => {
        const {kind, value: id} = target.dataset;
        staffRun(async () => {
            await apiPost(STAFF_REVIEW_PATHS[kind](id), {decision: 'approve', ...staffCollectReview(target)});
            showNotification('Подтверждено');
        }, {reload: staffLoadRequests});
    },
    reject: target => {
        staff.rejecting = `${target.dataset.kind}:${target.dataset.value}`;
        staff.reason = '';
        staffPaint();
    },
    'cancel-reject': () => {
        staff.rejecting = null;
        staffPaint();
    },
    'confirm-reject': target => {
        if (!staff.reason.trim()) {
            showNotification('Укажите причину', 'error');
            return;
        }
        const {kind, value: id} = target.dataset;
        staffRun(async () => {
            await apiPost(STAFF_REVIEW_PATHS[kind](id), {decision: 'reject', reason: staff.reason});
            staff.rejecting = null;
            showNotification('Отказ отправлен');
        }, {reload: staffLoadRequests});
    },
    'load-receipt': target => {
        const id = target.dataset.value;
        staffRun(async () => {
            const response = await fetch(`${API}/api/admin/payments/${id}/receipt`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({initData}),
            });
            if (!response.ok) throw new Error('Не удалось загрузить чек');
            const blob = await response.blob();
            staff.receipts[id] = {url: URL.createObjectURL(blob), type: blob.type};
        });
    },
    'take-cash': target => staffRun(async () => {
        await apiPost(`/api/admin/payments/${target.dataset.value}/cash`);
        showNotification('Оплата принята');
    }, {reload: staffLoadMoney}),
    remind: target => staffRun(async () => {
        await apiPost(`/api/admin/payments/${target.dataset.value}/remind`);
        staff.reminded[target.dataset.value] = true;
    }),
    'toggle-role': target => {
        const coach = staff.team.coaches.find(item => item.id === Number(target.dataset.value));
        const roles = {is_admin: coach.is_admin, is_manager: coach.is_manager};
        roles[target.dataset.role] = !roles[target.dataset.role];
        staffRun(() => apiPost(`/api/admin/coaches/${coach.id}/roles`, roles), {reload: staffLoadTeam});
    },
    'copy-link': async () => {
        try {
            await navigator.clipboard.writeText(staff.team.registration_link);
            staff.copied = true;
            staffPaint();
        } catch {
            showNotification(staff.team.registration_link);
        }
    },
    'add-resource': target => {
        const {kind} = target.dataset;
        const input = document.querySelector(`[data-new-resource="${kind}"]`);
        if (!input.value.trim()) return;
        staffRun(() => apiPost(`/api/admin/${kind}/create`, {name: input.value}), {reload: staffAfterResourceChange});
    },
    'remove-resource': target => staffRun(
        () => apiPost(`/api/admin/${target.dataset.kind}/${target.dataset.value}/delete`),
        {reload: staffAfterResourceChange},
    ),
};

async function staffAfterResourceChange() {
    await staffLoadTeam();
    await staffLoadRequests();
}

function tgConfirm(message, onYes) {
    if (tg?.showConfirm && tg.isVersionAtLeast?.('6.2')) {
        tg.showConfirm(message, accepted => {
            if (accepted) onYes();
        });
    } else if (window.confirm(message)) {
        onYes();
    }
}

document.addEventListener('click', event => {
    const target = event.target.closest('[data-st]');
    if (!target || target.disabled) return;
    STAFF_ACTIONS[target.dataset.st]?.(target);
});

document.addEventListener('input', event => {
    const target = event.target;
    if (target.dataset.stInput === 'query') {
        staff.query = target.value;
        document.getElementById('st-student-list').innerHTML = staffStudentRows();
    } else if (target.dataset.stInput === 'reason') {
        staff.reason = target.value;
    } else if (target.dataset.stForm) {
        staff.form[target.dataset.stForm] = target.value;
    } else if (target.dataset.stRow !== undefined) {
        const row = staff.form.schedule[Number(target.dataset.stRow)];
        row[target.dataset.field] = target.dataset.field === 'time' ? target.value : (target.value === '' ? null : Number(target.value));
    }
});
