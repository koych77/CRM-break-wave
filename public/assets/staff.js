/* === Coach and administrator screens === */

const staff = {
    me: null,
    tab: 'today',
    screen: 'root',
    date: null,
    day: null,
    session: null,
    marks: {},
    confirmCancel: false,
    students: null,
    studentCache: {},
    query: '',
    studentFilter: 'all',
    student: null,
    form: null,
    month: null,
    formDirty: false,
    resources: {locations: [], groups: [], coaches: []},
    requests: null,
    requestFilter: 'all',
    request: null,
    review: {},
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
    // The tab opens at once, with a spinner on the first visit, and its data arrives behind the scenes.
    staffPaint();
    window.scrollTo(0, 0);
    try {
        await loaders[tab]();
        if (staff.tab === tab && staff.screen === 'root' && !document.activeElement?.matches('input, select, textarea')) staffPaint();
    } catch (error) {
        if (staff.tab !== tab || staff.screen !== 'root') return;
        if (staffTabData(tab)) showNotification(error.message, 'error');
        else renderScreenState(staffRoot(), error.message, {retry: 'staffOpenTab(staff.tab)'});
    }
}

function staffTabData(tab) {
    return {today: staff.day, students: staff.students, requests: staff.requests, money: staff.money, team: staff.team}[tab];
}

async function staffLoadDay() {
    const [day] = await Promise.all([
        apiPost('/api/staff/day', staff.date ? {date: staff.date} : {}),
        staff.requests === null ? staffLoadRequests() : null,
    ]);
    staff.day = day;
    staff.date = day.date;
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
        ['today', 'calendar', 'Сегодня'],
        ['students', 'users', 'Ученики'],
        ['requests', 'clipboard', 'Заявки'],
    ];
    if (staff.me.is_admin) items.push(['money', 'wallet', 'Деньги'], ['team', 'settings', 'Команда']);
    return `
        <nav class="pa-nav" aria-label="Разделы">
            ${items.map(([id, icon, label]) => `
                <button type="button" data-st="tab" data-value="${id}" ${staff.tab === id ? 'aria-current="page"' : ''}>
                    ${bwIcon(icon)}
                    ${id === 'requests' && count ? `<span class="st-badge" aria-label="Новых: ${count}">${count}</span>` : ''}
                    <span>${label}</span>
                </button>`).join('')}
        </nav>`;
}

function staffPaint() {
    const screens = {
        attendance: staffAttendanceHtml,
        student: staffStudentHtml,
        form: staffFormHtml,
        month: staffMonthHtml,
        request: staffRequestHtml,
    };
    const tabs = {
        today: staffTodayHtml,
        students: staffStudentsHtml,
        requests: staffRequestsHtml,
        money: staffMoneyHtml,
        team: staffTeamHtml,
    };
    const scrollTop = window.scrollY;
    const page = staffTabData(staff.tab) ? tabs[staff.tab]() : `<div class="st-loading">${IOS_SPINNER}</div>`;
    const body = staff.screen === 'root'
        ? `<div class="st-page">${page}</div>${staffNav()}`
        : screens[staff.screen]();
    const depths = {root: 0, student: 1, attendance: 1, request: 1, form: 2, month: 2};
    const key = staff.screen === 'root' ? `tab:${staff.tab}` : staff.screen;
    paintScreen(staffRoot(), `<div class="pa">${body}</div>`, key, depths[staff.screen]);
    if (window.scrollY !== scrollTop) window.scrollTo(0, scrollTop);
}

function staffBar(title, subtitle = '', extra = '') {
    return `
        <div class="pa-bar">
            <button type="button" class="pa-back" data-st="back" data-back aria-label="Назад">${bwIcon('chevron-left', 26)}</button>
            <div>
                <h1>${escapeHtml(title)}</h1>
                ${subtitle ? `<div class="pa-muted pa-small">${escapeHtml(subtitle)}</div>` : ''}
            </div>
            <span class="pa-bar-end">${extra}</span>
        </div>`;
}

function staffHeading(title, subtitle = '', extra = '') {
    return `
        <div class="st-title">
            <div>
                <div class="pa-brand">Break Wave</div>
                <h1 data-large-title="${escapeHtml(title)}">${escapeHtml(title)}</h1>
                ${subtitle ? `<div class="pa-muted">${escapeHtml(subtitle)}</div>` : ''}
            </div>
            ${extra}
        </div>`;
}

function staffBadge(text, tone) {
    return `<span class="pa-badge ${tone}">${escapeHtml(text)}</span>`;
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

function staffSessionBadge(session, isFuture) {
    const absent = session.roster.filter(item => item.status === 'absent').length;
    if (session.cancelled) return staffBadge('Отменена', 'is-danger');
    if (session.saved) return staffBadge(`Есть ${session.roster.length - absent}, нет ${absent}`, 'is-ok');
    if (isFuture) return staffBadge('Впереди', 'is-neutral');
    return staffBadge('Отметить', 'is-warn');
}

function staffTodayHtml() {
    const {sessions, date, today} = staff.day;
    const isFuture = date > today;
    const active = sessions.filter(session => !session.cancelled);
    const unmarked = active.filter(session => !session.saved).length;
    const pupils = active.reduce((sum, session) => sum + session.roster.length, 0);
    const rows = sessions.map(session => {
        const inner = `
            <span class="st-session-time">${escapeHtml(session.time)}</span>
            <span class="pa-stack">
                <span class="pa-strong">${escapeHtml(session.group || 'Тренировка')}</span>
                <span class="pa-muted pa-small">${escapeHtml(session.location || 'Без зала')} · учеников: ${session.roster.length}</span>
            </span>
            <span class="pa-list-value">${staffSessionBadge(session, isFuture)}</span>`;
        return session.cancelled
            ? `<div class="pa-list-row">${inner}</div>`
            : `<button type="button" class="pa-list-row" data-st="open-session" data-value="${escapeHtml(session.key)}">${inner}${bwChevron()}</button>`;
    }).join('');
    const arrows = `
        <div class="st-arrows">
            <button type="button" class="st-icon-btn" data-st="shift-day" data-value="-1" aria-label="Предыдущий день">${bwIcon('chevron-left', 24)}</button>
            <button type="button" class="st-icon-btn" data-st="shift-day" data-value="1" aria-label="Следующий день">${bwIcon('chevron-right', 24)}</button>
        </div>`;
    return `
        ${staffHeading(staffDayTitle(), parentLongDate(date), arrows)}
        ${sessions.length ? `
            <div class="st-tiles">
                <div class="st-tile"><div class="pa-big">${active.length}</div><div class="pa-label">Тренировки</div></div>
                <div class="st-tile"><div class="pa-big">${pupils}</div><div class="pa-label">Учеников</div></div>
                <div class="st-tile"><div class="pa-big ${unmarked && !isFuture ? 'pa-warn' : ''}">${isFuture ? '—' : unmarked}</div><div class="pa-label">Не отмечено</div></div>
            </div>
            <div class="pa-list">${rows}</div>`
            : `<div class="st-empty">${bwIcon('calendar', 32)}<span>На этот день тренировок нет</span>
                ${date === today ? '' : '<button type="button" class="pa-btn" data-st="go-today">К сегодняшнему дню</button>'}</div>`}`;
}

function staffAttendanceHtml() {
    const session = staff.session;
    const future = staff.day.date > staff.day.today;
    const absentCount = session.roster.filter(item => staff.marks[item.student_id] === 'absent').length;
    const rows = session.roster.map(item => {
        const absent = staff.marks[item.student_id] === 'absent';
        return `
            <button type="button" class="pa-list-row st-mark ${absent ? 'is-absent' : ''}" data-st="toggle-mark" data-value="${item.student_id}" aria-pressed="${absent}" ${future ? 'disabled' : ''}>
                <span class="st-avatar">${escapeHtml(bwInitials(item.name))}</span>
                <span class="pa-stack">
                    <span class="pa-strong">${escapeHtml(item.name)}</span>
                    ${item.nickname ? `<span class="pa-muted pa-small">${escapeHtml(item.nickname)}</span>` : ''}
                    ${item.tag ? `<span class="pa-muted pa-small">${escapeHtml(item.tag)}</span>` : ''}
                </span>
                ${future ? '' : `<span class="pa-list-value st-mark-state ${absent ? 'pa-danger' : 'pa-ok'}">${bwIcon(absent ? 'x' : 'check', 20)}${absent ? 'Нет' : 'Есть'}</span>`}
            </button>`;
    }).join('');
    const cancel = staff.confirmCancel
        ? `<section class="pa-panel is-danger">
                <div class="pa-panel-title">Отменить тренировку?</div>
                <div>Занятие не спишется. Все ученики получат отработку, а родители — уведомление.</div>
                <div class="pa-two">
                    <button type="button" class="pa-btn st-danger" data-st="cancel-session" ${staff.busy ? 'disabled' : ''}>Отменить</button>
                    <button type="button" class="pa-btn" data-st="keep-session">Не отменять</button>
                </div>
           </section>`
        : '<button type="button" class="pa-link st-danger-link" data-st="ask-cancel">Отменить тренировку</button>';
    return `
        ${staffBar(`${session.time}, ${session.group || 'Тренировка'}`, parentLongDate(staff.day.date))}
        <div class="pa-body">
            ${future ? '' : `
                <div class="pa-row">
                    <span class="pa-muted">Нажмите на ученика, чтобы отметить отсутствие</span>
                    <span class="pa-strong">Нет: ${absentCount} из ${session.roster.length}</span>
                </div>`}
            ${rows ? `<div class="pa-list">${rows}</div>` : '<div class="st-empty">В этой тренировке нет учеников</div>'}
            ${cancel}
        </div>
        ${future ? '' : `<div class="pa-foot">
            <button type="button" class="pa-btn is-primary" data-st="save-marks" ${staff.busy ? 'disabled' : ''}>${staff.busy ? 'Сохраняем…' : `Сохранить: есть ${session.roster.length - absentCount}, нет ${absentCount}`}</button>
        </div>`}`;
}

/* --- Students --- */

function staffStudentRows() {
    const query = staff.query.trim().toLowerCase();
    const rows = staff.students.filter(item => (staff.studentFilter === 'all' || item.flag === staff.studentFilter)
        && (!query || [item.name, item.nickname, item.parent_name, item.phone, item.parent_phone].some(value => String(value || '').toLowerCase().includes(query))));
    if (!staff.students.length) return `<div class="st-empty">${bwIcon('users', 32)}<span>Пока нет учеников</span></div>`;
    if (!rows.length) return `<div class="st-empty">${query ? 'По запросу ничего не найдено' : 'Учеников с такой меткой нет'}</div>`;
    const flags = {debt: staffBadge('Долг', 'is-danger'), low: staffBadge('Мало занятий', 'is-warn')};
    return `<div class="pa-list">${rows.map(item => `
        <button type="button" class="pa-list-row" data-st="open-student" data-value="${item.id}">
            <span class="st-avatar">${escapeHtml(bwInitials(item.name))}</span>
            <span class="pa-stack">
                <span class="pa-strong">${escapeHtml(item.name)}</span>
                <span class="pa-muted pa-small">${item.nickname ? `${escapeHtml(item.nickname)} · ` : ''}${item.group ? `${escapeHtml(item.group)}, ` : ''}занятий: ${item.lessons_remaining}</span>
            </span>
            <span class="pa-list-value">${flags[item.flag] || ''}</span>
        </button>`).join('')}</div>`;
}

function staffStudentsHtml() {
    const filters = [['all', 'Все'], ['debt', 'Долг'], ['low', 'Мало занятий']];
    return `
        ${staffHeading('Ученики', '', `<button type="button" class="st-icon-btn is-accent" data-st="new-student" aria-label="Добавить ученика">${bwIcon('plus', 24)}</button>`)}
        <label class="pa-search">
            ${bwIcon('search', 20)}
            <input class="pa-field" type="search" aria-label="Поиск по имени, никнейму или телефону" placeholder="Имя, никнейм или телефон" data-st-input="query" value="${escapeHtml(staff.query)}">
        </label>
        <div class="pa-pills">
            ${filters.map(([id, label]) => `<button type="button" data-st="student-filter" data-value="${id}" aria-pressed="${staff.studentFilter === id}">${label}</button>`).join('')}
        </div>
        <div class="pa-group" id="st-student-list">${staffStudentRows()}</div>`;
}

function staffStudentHtml() {
    const item = staff.student;
    if (item.loading) return `${staffBar(item.name)}<div class="st-loading">${IOS_SPINNER}</div>`;
    const invoiceLabels = item.invoice ? {
        paid: ['Оплачено', 'is-ok'],
        pending: [`До ${parentShortDate(item.invoice.due_date)}`, 'is-warn'],
        overdue: ['Долг', 'is-danger'],
        reported: ['На проверке', 'is-warn'],
        awaiting_receipt: ['Ждём чек', 'is-warn'],
        rejected: ['Отклонено', 'is-danger'],
        skipped: ['Не занимался', 'is-neutral'],
    } : {};
    const invoice = invoiceLabels[item.invoice?.status] || ['', 'is-neutral'];
    const isPaid = item.invoice?.status === 'paid';
    const historyLabels = {present: ['Был', 'is-ok'], absent: ['Не был', 'is-warn']};
    const history = item.history.map(row => {
        const [label, tone] = row.source === 'coach_cancelled' ? ['Отмена', 'is-neutral'] : historyLabels[row.status];
        return `<div class="pa-list-row"><span>${escapeHtml(parentShortDate(row.date))}${row.source === 'makeup' ? ', отработка' : ''}</span><span class="pa-list-value">${staffBadge(label, tone)}</span></div>`;
    }).join('');
    const schedule = item.schedule.map(row => `
        <div class="pa-list-row"><span>${PARENT_WEEKDAYS[row.weekday]}</span><span class="pa-list-value pa-strong">${escapeHtml(row.time)}</span></div>`).join('');
    const edit = `<button type="button" class="st-icon-btn" data-st="edit-student" aria-label="Изменить">${bwIcon('edit', 22)}</button>`;
    const facts = [
        item.nickname ? ['Никнейм', item.nickname] : null,
        item.birthday ? ['Дата рождения', parentBirthday(item.birthday)] : null,
    ].filter(Boolean);
    return `
        ${staffBar(item.name, item.nickname || '', edit)}
        <div class="pa-body">
            <section class="pa-hero">
                ${item.invoice ? `
                    <div class="pa-row">
                        <span class="pa-eyebrow">${escapeHtml(staffMonthName(item.invoice))} · ${escapeHtml(staffTariffLabel(item.invoice))} · ${item.invoice.amount} Br</span>
                        ${staffBadge(invoice[0], invoice[1])}
                    </div>
                    <div class="pa-hero-count">
                        <span class="pa-hero-time">${item.lessons_remaining}</span>
                        <span class="pa-muted">из ${item.lessons_count} ${isPaid ? 'занятий осталось' : 'занятий по учёту'}</span>
                    </div>
                    ${isPaid ? bwSegments(item.lessons_remaining, item.lessons_count) : ''}`
                    : '<div class="pa-muted">В этом месяце занятий нет</div>'}
                <div class="pa-row"><span class="pa-muted">Отработки</span><span class="pa-strong">${item.makeups}</span></div>
                ${item.lessons_over ? `<div class="pa-warn">Сверх абонемента: ${item.lessons_over}</div>` : ''}
            </section>
            ${staff.me.is_admin && item.invoice ? `
                <button type="button" class="pa-list-row pa-list-alone" data-st="open-month">
                    ${bwTile('credit-card')}
                    <span class="pa-stack">
                        <span class="pa-strong">Абонемент и оплата</span>
                        <span class="pa-muted pa-small">Изменить абонемент, сумму, оплату и остаток</span>
                    </span>
                    ${bwChevron()}
                </button>` : ''}
            <h2 class="pa-section">Родитель</h2>
            <div class="pa-list">
                <div class="pa-list-row">
                    ${bwTile('user')}
                    <span class="pa-stack">
                        <span class="pa-strong">${escapeHtml(item.parent_name || 'Не привязан')}</span>
                        ${item.parent_phone ? `<span class="pa-muted pa-small">${escapeHtml(item.parent_phone)}</span>` : ''}
                    </span>
                    ${item.parent_phone ? `<a class="st-icon-btn is-accent" href="tel:${escapeHtml(item.parent_phone.replace(/[^+\d]/g, ''))}" aria-label="Позвонить">${bwIcon('phone', 22)}</a>` : ''}
                </div>
            </div>
            <h2 class="pa-section">Расписание</h2>
            <div class="pa-list">${schedule || '<div class="pa-list-row pa-muted">Не назначено</div>'}</div>
            ${facts.length ? `
                <h2 class="pa-section">Данные</h2>
                <div class="pa-list">${facts.map(([label, value]) => `
                    <div class="pa-list-row"><span class="pa-muted">${label}</span><span class="pa-list-value">${escapeHtml(value)}</span></div>`).join('')}
                </div>` : ''}
            ${item.notes ? `
                <h2 class="pa-section">Заметки</h2>
                <div class="pa-note">${escapeHtml(item.notes)}</div>` : ''}
            <h2 class="pa-section">Последние занятия</h2>
            <div class="pa-list">${history || '<div class="pa-list-row pa-muted">Занятий пока нет</div>'}</div>
            ${staff.me.is_admin ? `
                <details class="pa-more">
                    <summary>${bwTile('sliders')}Дополнительно</summary>
                    <button type="button" class="pa-btn st-danger-soft" data-st="delete-student">${bwIcon('trash', 20)}Удалить ученика</button>
                </details>` : ''}
        </div>`;
}

function staffMonthName(invoice) {
    return PARENT_MONTHS_NOMINATIVE[parentDate(invoice.period_start || invoice.due_date).getMonth()];
}

function staffTariffLabel(invoice) {
    return invoice.is_partial ? `неполный, ${bwLessonsWord(invoice.lessons_count)}` : bwTariffs[invoice.tariff_code]?.label || bwLessonsWord(invoice.lessons_count);
}

function staffTariffOptions(selected, emptyLabel = '') {
    return (emptyLabel ? `<option value="" ${selected ? '' : 'selected'}>${emptyLabel}</option>` : '')
        + Object.entries(bwTariffs).map(([code, item]) =>
            `<option value="${code}" ${String(selected) === code ? 'selected' : ''}>${escapeHtml(item.label)} · ${item.price} Br</option>`).join('');
}

/* --- Subscription and payment of the month (admins) --- */

function staffMonthHtml() {
    const item = staff.student;
    const month = staff.month;
    const options = [
        ...Object.entries(bwTariffs).map(([code, tariff]) => [code, tariff.label, `${tariff.price} Br`]),
        ['partial', 'Неполный месяц', 'по 40 Br'],
    ];
    const number = (label, field, hint = '') => `
        <label class="pa-stack">
            <span class="pa-label">${label}</span>
            <input class="pa-field" type="number" inputmode="numeric" min="0" data-month="${field}" value="${escapeHtml(month[field])}">
            ${hint ? `<span class="pa-label">${hint}</span>` : ''}
        </label>`;
    return `
        ${staffBar('Абонемент и оплата', `${item.name}, ${staffMonthName(item.invoice).toLowerCase()}`)}
        <div class="pa-body">
            <div class="pa-group">
                <h2 class="pa-section">Абонемент</h2>
                ${options.map(([code, label, price]) => `
                    <button type="button" class="pa-choice" data-st="month-tariff" data-value="${code}" aria-pressed="${month.tariff === code}">
                        <span>${label}</span>
                        <span class="pa-strong">${price}</span>
                    </button>`).join('')}
                ${month.tariff === 'partial' ? number('Занятий в этом месяце', 'lessons') : ''}
            </div>
            <div class="pa-group">
                <h2 class="pa-section">Оплата</h2>
                ${number('Сумма, Br', 'amount')}
                <div class="pa-segment">
                    <button type="button" data-st="month-paid" data-value="no" aria-pressed="${!month.paid}">Не оплачен</button>
                    <button type="button" data-st="month-paid" data-value="yes" aria-pressed="${month.paid}">Оплачен</button>
                </div>
            </div>
            <div class="pa-group">
                <h2 class="pa-section">Занятия</h2>
                ${number('Осталось занятий', 'left', `Занятия считаются с 1 числа. По расписанию с 1 числа уже прошло тренировок: ${item.trainings_passed}, в боте отмечено: ${item.lessons_used}.`)}
                ${item.trainings_passed > item.lessons_used ? `
                    <button type="button" class="pa-btn" data-st="month-write-off">
                        Списать прошедшие с 1 числа (${item.trainings_passed}): останется ${Math.max(0, Number(month.lessons) - item.trainings_passed)}
                    </button>` : ''}
            </div>
        </div>
        <div class="pa-foot">
            <button type="button" class="pa-btn is-primary" data-st="save-month" ${staff.busy ? 'disabled' : ''}>${staff.busy ? 'Сохраняем…' : 'Сохранить'}</button>
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
                <button type="button" class="pa-remove" data-st="remove-row" data-value="${index}" aria-label="Убрать">${bwIcon('x', 20)}</button>
            </div>
            <div class="pa-two">
                <select class="pa-field" aria-label="Группа" data-st-row="${index}" data-field="group_id">${staffOptions(staff.resources.groups, row.group_id, 'name', 'Группа')}</select>
                <select class="pa-field" aria-label="Зал" data-st-row="${index}" data-field="location_id">${staffOptions(staff.resources.locations, row.location_id, 'name', 'Зал')}</select>
            </div>
        </div>`).join('');
    return `
        ${staffBar(form.id ? 'Изменить ученика' : 'Новый ученик')}
        <div class="pa-body">
            <h2 class="pa-section">Личные данные</h2>
            ${field('Имя и фамилия', 'name')}
            ${field('Никнейм', 'nickname')}
            <label class="pa-stack">
                <span class="pa-label">Дата рождения</span>
                ${bwDateField('data-st-form="birthday"', form.birthday, form.birthdayText)}
            </label>
            ${field('Телефон ученика', 'phone', 'tel')}
            ${form.parent_linked ? '' : field('Телефон родителя', 'parent_phone', 'tel')}
            <h2 class="pa-section">Занятия</h2>
            ${staff.me.is_admin && form.id && staff.student?.invoice ? `
                <button type="button" class="pa-list-row pa-list-alone" data-st="open-month">
                    ${bwTile('credit-card')}
                    <span class="pa-stack">
                        <span class="pa-strong">Остаток и оплата</span>
                        <span class="pa-muted pa-small">Осталось ${staff.student.lessons_remaining} из ${staff.student.lessons_count}. Изменить остаток, абонемент и оплату</span>
                    </span>
                    ${bwChevron()}
                </button>` : ''}
            ${staff.me.is_admin ? `
                <label class="pa-stack">
                    <span class="pa-label">Тренер</span>
                    <select class="pa-field" data-st-form="coach_id">${staffOptions(staff.resources.coaches, form.coach_id, 'first_name', 'Я')}</select>
                </label>` : ''}
            <label class="pa-stack">
                <span class="pa-label">Абонемент</span>
                <select class="pa-field" data-st-form="tariff_code">${staffTariffOptions(form.tariff_code, 'По числу дней в расписании')}</select>
                ${form.tariff_locked ? '<span class="pa-label">Этот месяц уже оплачен или неполный: новый абонемент начнётся со следующего месяца.</span>' : ''}
            </label>
            ${field('Дата начала занятий', 'training_start_date', 'date')}
            <h2 class="pa-section">Расписание</h2>
            ${rows}
            <button type="button" class="pa-btn" data-st="add-row">${bwIcon('plus', 20)}Добавить день</button>
            <h2 class="pa-section">Заметки</h2>
            <textarea class="pa-field st-textarea" aria-label="Заметки" data-st-form="notes">${escapeHtml(form.notes ?? '')}</textarea>
        </div>
        <div class="pa-foot">
            <button type="button" class="pa-btn is-primary" data-st="save-student" ${staff.busy ? 'disabled' : ''}>${staff.busy ? 'Сохраняем…' : 'Сохранить'}</button>
        </div>`;
}

/* --- Requests --- */

function staffScheduleText(rows) {
    return rows.map(row => `${PARENT_WEEKDAYS[row.weekday]} ${row.time}`).join(', ') || 'нет';
}

function staffDecision(kind, id, approveLabel = 'Подтвердить') {
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
        <div class="pa-group">
            <button type="button" class="pa-btn is-primary" data-st="approve" data-kind="${kind}" data-value="${id}">${bwIcon('check', 20)}${approveLabel}</button>
            <button type="button" class="pa-link st-reject" data-st="reject" data-kind="${kind}" data-value="${id}">Отказать</button>
        </div>`;
}

function staffRequestRow(kind, item, title, subtitle, label) {
    return `
        <button type="button" class="pa-list-row" data-st="open-request" data-kind="${kind}" data-value="${item.id}">
            <span class="pa-stack">
                <span class="pa-eyebrow">${escapeHtml(label)}</span>
                <span class="pa-strong">${escapeHtml(title)}</span>
                <span class="pa-muted pa-small">${escapeHtml(subtitle)}</span>
            </span>
            <span class="pa-list-value"></span>
            ${bwChevron()}
        </button>`;
}

function staffPaymentLabel(item) {
    return `${item.method === 'cash' ? 'Наличные' : 'Чек на проверку'}${{upgrade: ', доплата', single: ', разовое'}[item.kind] || ''}`;
}

function staffRequestsHtml() {
    const data = staff.requests;
    const types = [
        ['registrations', 'Анкеты', data.registrations.length],
        ['makeups', 'Отработки', data.makeups.length],
        ['schedule_changes', 'Расписание', data.schedule_changes.length],
        ['payments', 'Оплаты', data.payments.length],
    ].filter(([, , count]) => count);
    const filter = types.some(([id]) => id === staff.requestFilter) ? staff.requestFilter : 'all';
    const shows = id => filter === 'all' || filter === id;
    const registrations = shows('registrations') && data.registrations.length ? `<div class="pa-list">${data.registrations.map(item =>
        staffRequestRow('registration', item, item.child_name, `Родитель: ${item.parent_name}`, 'Новая семья')).join('')}</div>` : '';
    const payments = shows('payments') && data.payments.length ? `<div class="pa-list">${data.payments.map(item =>
        staffRequestRow('payment', item, `${item.student_name}, ${item.amount} Br`, PARENT_MONTHS_NOMINATIVE[parentDate(item.period_start).getMonth()], staffPaymentLabel(item))).join('')}</div>` : '';
    const makeups = shows('makeups') ? data.makeups.map(item => `
        <section class="pa-panel">
            <div class="pa-eyebrow">Отработка</div>
            <div class="pa-panel-title">${escapeHtml(item.student_name)}</div>
            <div class="pa-muted">Хочет прийти: ${escapeHtml(parentLongDate(item.requested_date))}${item.requested_time ? `, ${escapeHtml(item.requested_time)}` : ''}</div>
            ${staffDecision('makeup', item.id)}
        </section>`).join('') : '';
    const schedules = shows('schedule_changes') ? data.schedule_changes.map(item => `
        <section class="pa-panel">
            <div class="pa-eyebrow">Смена расписания</div>
            <div class="pa-panel-title">${escapeHtml(item.student_name)}</div>
            <div class="pa-muted">Сейчас: ${escapeHtml(staffScheduleText(item.current_schedule))}</div>
            <div>Просит: ${escapeHtml(staffScheduleText(item.proposed_schedule))}</div>
            ${staffDecision('schedule', item.id)}
        </section>`).join('') : '';
    const content = registrations + payments + makeups + schedules;
    return `
        ${staffHeading('Заявки', staffRequestCount() ? `Ждут решения: ${staffRequestCount()}` : '')}
        ${types.length > 1 ? `<div class="pa-pills">
            <button type="button" data-st="request-filter" data-value="all" aria-pressed="${filter === 'all'}">Все</button>
            ${types.map(([id, label, count]) => `<button type="button" data-st="request-filter" data-value="${id}" aria-pressed="${filter === id}">${label} ${count}</button>`).join('')}
        </div>` : ''}
        ${content || `<div class="st-empty">${bwIcon('check', 32)}<span>Все заявки разобраны</span></div>`}`;
}

function staffRegistrationHtml(item) {
    const data = staff.requests;
    const {groups, locations, coaches} = staff.resources;
    const review = staff.review;
    const facts = [
        item.child_nickname ? ['Никнейм', item.child_nickname] : null,
        ['Дата рождения', parentBirthday(item.child_birthday)],
        item.preferred_coach_name ? ['Выбранный тренер', item.preferred_coach_name] : null,
        ['Родитель', item.parent_name],
        ['Телефон', item.parent_phone],
        ['Удобно', staffScheduleText(item.proposed_schedule)],
    ].filter(Boolean);
    return `
        ${staffBar(item.child_name, 'Новая семья')}
        <div class="pa-body" data-reg-card="${item.id}">
            <div class="pa-list">${facts.map(([label, value]) => `
                <div class="pa-list-row"><span class="pa-muted">${label}</span><span class="pa-list-value">${escapeHtml(value)}</span></div>`).join('')}
            </div>
            <h2 class="pa-section">Куда записать</h2>
            ${data.linkable_students.length ? `
                <label class="pa-stack">
                    <span class="pa-label">Если ребёнок уже есть в списке</span>
                    <select class="pa-field" data-review="link_student_id">${staffOptions(data.linkable_students, review.link_student_id, 'name', 'Новый ученик')}</select>
                </label>` : ''}
            <div class="pa-two">
                <label class="pa-stack">
                    <span class="pa-label">Группа</span>
                    <select class="pa-field" data-review="group_id">${staffOptions(groups, review.group_id)}</select>
                </label>
                <label class="pa-stack">
                    <span class="pa-label">Зал</span>
                    <select class="pa-field" data-review="location_id">${staffOptions(locations, review.location_id)}</select>
                </label>
            </div>
            <label class="pa-stack">
                <span class="pa-label">Абонемент${item.preferred_tariff ? ', выбрал родитель' : ''}</span>
                <select class="pa-field" data-review="tariff_code">${staffTariffOptions(review.tariff_code ?? item.preferred_tariff ?? bwSuggestTariff(item.proposed_days))}</select>
            </label>
            <label class="pa-stack">
                <span class="pa-label">Начало занятий</span>
                <input class="pa-field" type="date" data-review="start_date" value="${escapeHtml(review.start_date ?? staff.day?.today ?? '')}">
            </label>
            ${staff.me.is_admin ? `
                <label class="pa-stack">
                    <span class="pa-label">Тренер</span>
                    <select class="pa-field" data-review="coach_id">${staffOptions(coaches, review.coach_id ?? item.preferred_coach_id, 'first_name', 'Я')}</select>
                </label>` : ''}
            ${staffDecision('registration', item.id, 'Принять')}
        </div>`;
}

function staffPaymentHtml(item) {
    const receipt = staff.receipts[item.id];
    let preview = '';
    if (item.has_receipt) {
        preview = receipt
            ? (receipt.type.startsWith('image/')
                ? `<img class="st-receipt" src="${receipt.url}" alt="Чек">`
                : `<a class="pa-btn" href="${receipt.url}" target="_blank" rel="noopener">Открыть файл чека</a>`)
            : `<button type="button" class="pa-btn" data-st="load-receipt" data-value="${item.id}">${bwIcon('receipt', 20)}Показать чек</button>`;
    }
    return `
        ${staffBar(item.student_name, staffPaymentLabel(item))}
        <div class="pa-body" data-pay-card="${item.id}">
            <section class="pa-hero">
                <div class="pa-eyebrow">${escapeHtml(PARENT_MONTHS_NOMINATIVE[parentDate(item.period_start).getMonth()])}</div>
                <div class="pa-hero-time">${item.amount}&nbsp;Br</div>
            </section>
            ${preview}
            ${item.method === 'cash' ? `
                <label class="pa-stack">
                    <span class="pa-label">Кто принял деньги</span>
                    <select class="pa-field" data-review="received_by_coach_id">${staffOptions(staff.resources.coaches, staff.review.received_by_coach_id, 'first_name', 'Я')}</select>
                </label>` : ''}
            ${staffDecision('payment', item.id, item.method === 'cash' ? 'Подтвердить получение' : 'Подтвердить оплату')}
        </div>`;
}

function staffRequestHtml() {
    const {kind, id} = staff.request;
    const list = kind === 'registration' ? staff.requests.registrations : staff.requests.payments;
    const item = list.find(row => row.id === id);
    if (!item) return `${staffBar('Заявка')}<div class="st-empty">Заявка уже разобрана</div>`;
    return kind === 'registration' ? staffRegistrationHtml(item) : staffPaymentHtml(item);
}

/* --- Money and team --- */

function staffMoneyHtml() {
    const data = staff.money;
    const rows = data.debtors.map(item => {
        const overdue = item.status === 'overdue';
        const month = PARENT_MONTHS_NOMINATIVE[parentDate(item.period_start).getMonth()];
        let note = overdue ? `${month}: просрочено` : `${month}: до ${parentShortDate(item.due_date)}`;
        if (item.not_training) note = `${month}: пока не занимается`;
        else if (item.kind === 'upgrade') note = `${month}: доплата за абонемент`;
        else if (item.kind === 'single') note = `${month}: разовое занятие`;
        const reminded = staff.reminded[item.id];
        return `
            <section class="pa-panel">
                <div class="pa-row">
                    <div class="pa-panel-title">${escapeHtml(item.student_name)}</div>
                    <div class="pa-panel-title">${item.amount} Br</div>
                </div>
                <div class="${overdue ? 'pa-danger' : 'pa-warn'} pa-small">${escapeHtml(note)}</div>
                <div class="pa-two">
                    <button type="button" class="pa-btn" data-st="take-cash" data-value="${item.id}">${bwIcon('wallet', 20)}Наличные</button>
                    <button type="button" class="pa-btn" data-st="remind" data-value="${item.id}" ${reminded || !item.can_remind ? 'disabled' : ''}>${item.can_remind && !reminded ? bwIcon('bell', 20) : ''}${reminded ? 'Отправлено' : item.can_remind ? 'Напомнить' : 'Нет родителя'}</button>
                </div>
            </section>`;
    }).join('');
    return `
        ${staffHeading('Деньги', PARENT_MONTHS_NOMINATIVE[Number(data.month.slice(5)) - 1])}
        <section class="pa-hero">
            <div class="pa-eyebrow">Получено</div>
            <div class="pa-hero-time">${data.collected}&nbsp;Br</div>
            <div class="pa-row"><span class="pa-muted">Ещё ждём</span><span class="pa-strong">${data.waiting} Br</span></div>
        </section>
        ${data.in_review ? `<button type="button" class="pa-list-row pa-list-alone" data-st="tab" data-value="requests">${bwTile('clock')}<span class="pa-strong">На проверке: ${data.in_review}</span><span class="pa-list-value"></span>${bwChevron()}</button>` : ''}
        <h2 class="pa-section">Не оплатили</h2>
        ${rows || '<div class="st-empty">Все оплатили</div>'}`;
}

function staffResourceList(title, items, kind, placeholder) {
    return `
        <h2 class="pa-section">${title}</h2>
        <div class="pa-list">
            ${items.map(item => `
                <div class="pa-list-row">
                    ${bwTile(kind === 'locations' ? 'building' : 'users')}
                    <span>${escapeHtml(item.name)}</span>
                    <span class="pa-list-value"></span>
                    <button type="button" class="pa-remove" data-st="remove-resource" data-kind="${kind}" data-value="${item.id}" aria-label="Убрать ${escapeHtml(item.name)}">${bwIcon('x', 20)}</button>
                </div>`).join('') || '<div class="pa-list-row pa-muted">Пока пусто</div>'}
        </div>
        <div class="st-add">
            <input class="pa-field" aria-label="${placeholder}" placeholder="${placeholder}" data-new-resource="${kind}">
            <button type="button" class="pa-btn" data-st="add-resource" data-kind="${kind}">Добавить</button>
        </div>`;
}

function staffTeamHtml() {
    const data = staff.team;
    const role = (coach, key, label) => `
        <button type="button" class="st-role" data-st="toggle-role" data-value="${coach.id}" data-role="${key}" aria-pressed="${coach[key]}" ${coach.locked ? 'disabled' : ''}>${label}</button>`;
    return `
        ${staffHeading('Команда')}
        <h2 class="pa-section">Сотрудники</h2>
        ${data.coaches.map(coach => `
            <section class="pa-panel">
                <div class="pa-panel-title">${escapeHtml(coach.name)}</div>
                <div class="st-roles">
                    <span class="st-role is-fixed">Тренер</span>
                    ${role(coach, 'is_admin', 'Администратор')}
                    ${role(coach, 'is_manager', 'Руководитель')}
                </div>
            </section>`).join('')}
        <h2 class="pa-section">Регистрация семей</h2>
        <section class="pa-panel">
            <div class="pa-muted">Отправьте родителям ссылку, и анкета придёт в раздел «Заявки».</div>
            <button type="button" class="pa-btn is-primary" data-st="copy-link">${bwIcon(staff.copied ? 'check' : 'link', 20)}${staff.copied ? 'Ссылка скопирована' : 'Скопировать ссылку'}</button>
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

// The screen slides in at once: from the last known card, or with a spinner on the first visit.
async function staffOpenStudent(rawId) {
    const id = Number(rawId);
    const listed = staff.students?.find(item => item.id === id);
    staff.student = staff.studentCache[id] || {id, name: listed?.name || '', loading: true};
    staff.screen = 'student';
    staffPaint();
    window.scrollTo(0, 0);
    try {
        const [detail] = await Promise.all([
            apiPost(`/api/staff/students/${id}`),
            staff.requests ? null : staffLoadRequests(),
        ]);
        staff.studentCache[id] = detail.student;
        if (staff.screen === 'student' && staff.student.id === id) {
            staff.student = detail.student;
            staffPaint();
        }
    } catch (error) {
        showNotification(error.message, 'error');
        if (staff.screen === 'student' && staff.student.id === id && staff.student.loading) {
            staff.screen = 'root';
            staffPaint();
        }
    }
}

function staffStartForm(student) {
    staff.form = student
        ? {
            id: student.id,
            name: student.name,
            nickname: student.nickname,
            birthday: student.birthday,
            phone: student.phone,
            parent_phone: student.parent_phone,
            parent_linked: Boolean(student.parent_name),
            coach_id: student.coach_id,
            tariff_code: student.tariff_code ?? student.invoice?.tariff_code ?? '',
            tariff_locked: Boolean(student.invoice?.is_partial) || !['pending', 'rejected', 'overdue'].includes(student.invoice?.status),
            training_start_date: student.training_start_date,
            notes: student.notes,
            schedule: student.schedule.map(row => ({...row})),
        }
        : {
            id: null,
            name: '',
            nickname: '',
            birthday: '',
            phone: '',
            parent_phone: '',
            coach_id: null,
            tariff_code: '',
            tariff_locked: false,
            training_start_date: '',
            notes: '',
            schedule: [{weekday: 0, time: '18:00', group_id: null, location_id: null}],
        };
    staff.screen = 'form';
    staff.formDirty = false;
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
    tab: target => {
        staffOpenTab(target.dataset.value);
    },
    back: () => {
        const leave = () => {
            staff.formDirty = false;
            if (staff.screen === 'form' && staff.form.id) {
                staff.screen = 'student';
                staffPaint();
                return;
            }
            staffOpenTab(staff.tab);
        };
        if (staff.screen === 'month') {
            staff.screen = 'student';
            staffPaint();
            return;
        }
        if (staff.screen === 'form' && staff.formDirty) tgConfirm('Выйти без сохранения? Изменения пропадут.', leave);
        else leave();
    },
    'open-month': () => {
        if (staff.screen === 'form' && staff.formDirty) {
            showNotification('Сначала сохраните изменения ученика', 'error');
            return;
        }
        const {invoice} = staff.student;
        staff.month = {
            tariff: invoice.is_partial ? 'partial' : String(invoice.tariff_code),
            lessons: invoice.lessons_count,
            amount: invoice.amount,
            paid: invoice.stored_status === 'paid',
            left: staff.student.lessons_remaining,
        };
        staff.screen = 'month';
        staffPaint();
        window.scrollTo(0, 0);
    },
    'month-tariff': target => {
        const code = target.dataset.value;
        const month = staff.month;
        const lessons = code === 'partial' ? Number(month.lessons) || 1 : bwTariffs[code].lessons;
        // A new subscription brings its price and a fresh balance; both stay editable.
        Object.assign(month, {
            tariff: code,
            lessons,
            amount: code === 'partial' ? lessons * 40 : bwTariffs[code].price,
            left: Math.max(0, lessons - staff.student.lessons_used),
        });
        staffPaint();
    },
    // Takes every training that has passed by the schedule as used, marked in the bot or not.
    'month-write-off': () => {
        staff.month.left = Math.max(0, Number(staff.month.lessons) - staff.student.trainings_passed);
        staffPaint();
    },
    'month-paid': target => {
        staff.month.paid = target.dataset.value === 'yes';
        staffPaint();
    },
    'save-month': () => {
        const month = staff.month;
        const id = staff.student.id;
        return staffRun(async () => {
            await apiPost(`/api/admin/students/${id}/month`, {
                tariff_code: month.tariff,
                lessons: Number(month.lessons),
                amount: Number(month.amount),
                paid: month.paid,
                lessons_left: month.left === '' ? null : Number(month.left),
            });
            staff.student = (await apiPost(`/api/staff/students/${id}`)).student;
            staff.studentCache[id] = staff.student;
            staff.students = null;
            staff.screen = 'student';
            showNotification('Абонемент сохранён');
        });
    },
    'go-today': () => {
        staff.date = null;
        return staffLoad(staffLoadDay);
    },
    'student-filter': target => {
        staff.studentFilter = target.dataset.value;
        staffPaint();
    },
    'request-filter': target => {
        staff.requestFilter = target.dataset.value;
        staffPaint();
    },
    'open-request': target => {
        Object.assign(staff, {
            screen: 'request',
            request: {kind: target.dataset.kind, id: Number(target.dataset.value)},
            review: {},
            rejecting: null,
            reason: '',
        });
        staffPaint();
        window.scrollTo(0, 0);
    },
    'shift-day': target => {
        const next = parentDate(staff.date);
        next.setDate(next.getDate() + Number(target.dataset.value));
        staff.date = `${next.getFullYear()}-${String(next.getMonth() + 1).padStart(2, '0')}-${String(next.getDate()).padStart(2, '0')}`;
        return staffLoad(staffLoadDay);
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
    'open-student': target => {
        staffOpenStudent(target.dataset.value);
    },
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
        const {id, schedule, parent_linked: _linked, tariff_locked: _locked, birthdayText, ...fields} = staff.form;
        if (birthdayText && !fields.birthday) throw new Error('Проверьте дату рождения: ДД.ММ.ГГГГ');
        const path = id ? `/api/staff/students/${id}/update` : '/api/staff/students/create';
        const saved = await apiPost(path, {student: fields, schedule});
        staff.formDirty = false;
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
        return staffRun(async () => {
            await apiPost(STAFF_REVIEW_PATHS[kind](id), {decision: 'approve', ...staffCollectReview(target)});
            staff.screen = 'root';
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
        return staffRun(async () => {
            await apiPost(STAFF_REVIEW_PATHS[kind](id), {decision: 'reject', reason: staff.reason});
            staff.rejecting = null;
            staff.screen = 'root';
            showNotification('Отказ отправлен');
        }, {reload: staffLoadRequests});
    },
    'adjust-lessons': target => {
        const delta = document.querySelector('[data-adjust="delta"]').value;
        const reason = document.querySelector('[data-adjust="reason"]').value;
        const id = target.dataset.value;
        return staffRun(async () => {
            await apiPost(`/api/staff/students/${id}/adjust`, {delta: Number(delta), reason});
            staff.student = (await apiPost(`/api/staff/students/${id}`)).student;
            staff.studentCache[id] = staff.student;
            showNotification('Остаток исправлен');
        });
    },
    'load-receipt': target => {
        const id = target.dataset.value;
        return staffRun(async () => {
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
    'take-cash': target => {
        const item = staff.money.debtors.find(row => row.id === Number(target.dataset.value));
        const month = PARENT_MONTHS_NOMINATIVE[parentDate(item.period_start).getMonth()].toLowerCase();
        tgConfirm(`Принять ${item.amount} Br наличными: ${item.student_name}, ${month}?`, () => staffRun(async () => {
            await apiPost(`/api/admin/payments/${item.id}/cash`);
            showNotification('Оплата принята');
        }, {reload: staffLoadMoney}));
    },
    remind: target => staffRun(async () => {
        await apiPost(`/api/admin/payments/${target.dataset.value}/remind`);
        staff.reminded[target.dataset.value] = true;
    }),
    'toggle-role': target => {
        const coach = staff.team.coaches.find(item => item.id === Number(target.dataset.value));
        const roles = {is_admin: coach.is_admin, is_manager: coach.is_manager};
        roles[target.dataset.role] = !roles[target.dataset.role];
        return staffRun(() => apiPost(`/api/admin/coaches/${coach.id}/roles`, roles), {reload: staffLoadTeam});
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
        if (!input.value.trim()) return undefined;
        return staffRun(() => apiPost(`/api/admin/${kind}/create`, {name: input.value}), {reload: staffAfterResourceChange});
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
    const action = STAFF_ACTIONS[target.dataset.st];
    if (action) runControl(target, action);
});

document.addEventListener('input', event => {
    const target = event.target;
    if (target.dataset.stInput === 'query') {
        staff.query = target.value;
        document.getElementById('st-student-list').innerHTML = staffStudentRows();
    } else if (target.dataset.stInput === 'reason') {
        staff.reason = target.value;
    } else if (target.dataset.review) {
        staff.review[target.dataset.review] = target.value;
    } else if (target.dataset.month) {
        staff.month[target.dataset.month] = target.value;
    } else if (target.dataset.stForm) {
        if (target.dataset.dateMask !== undefined) {
            staff.form.birthdayText = target.value;
            staff.form[target.dataset.stForm] = bwParseDate(target.value);
        } else {
            staff.form[target.dataset.stForm] = target.value;
        }
        staff.formDirty = true;
    } else if (target.dataset.stRow !== undefined) {
        staff.formDirty = true;
        const row = staff.form.schedule[Number(target.dataset.stRow)];
        row[target.dataset.field] = target.dataset.field === 'time' ? target.value : (target.value === '' ? null : Number(target.value));
    }
});
