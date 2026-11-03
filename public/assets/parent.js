/* === Parent cabinet === */

const PARENT_MONTHS = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'];
const PARENT_MONTHS_NOMINATIVE = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь', 'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'];
const PARENT_WEEKDAYS = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье'];
const PARENT_WEEKDAYS_SHORT = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'];
// Below this number of lessons the parent is warned; nothing else depends on it.
const LOW_LESSONS = 2;

const parentUi = {
    studentId: null,
    tab: 'home',
    lessonsTab: 'schedule',
    view: 'home',
    payFrom: 'home',
    tariff: null,
    method: 'online',
    slot: null,
    extraId: null,
    scheduleRows: [],
    busy: false,
};

function parentDate(value) {
    const [year, month, day] = String(value).slice(0, 10).split('-').map(Number);
    return new Date(year, month - 1, day);
}

function parentDayIndex(date) {
    return (date.getDay() + 6) % 7;
}

function parentShortDate(value) {
    const date = parentDate(value);
    return `${date.getDate()} ${PARENT_MONTHS[date.getMonth()]}`;
}

function parentLongDate(value) {
    return `${PARENT_WEEKDAYS[parentDayIndex(parentDate(value))]}, ${parentShortDate(value)}`;
}

function parentBirthday(value) {
    const born = parentDate(value);
    const today = new Date();
    let age = today.getFullYear() - born.getFullYear();
    if (today.getMonth() < born.getMonth() || (today.getMonth() === born.getMonth() && today.getDate() < born.getDate())) age -= 1;
    const tens = age % 100;
    const ones = age % 10;
    let word = 'лет';
    if (tens < 11 || tens > 14) {
      if (ones === 1) word = 'год';
      else if (ones >= 2 && ones <= 4) word = 'года';
    }
    return `${parentShortDate(value)} ${born.getFullYear()}, ${age} ${word}`;
}

function parentNearDate(value) {
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const days = Math.round((parentDate(value) - today) / 86400000);
    if (days === 0) return 'Сегодня';
    if (days === 1) return 'Завтра';
    return parentLongDate(value);
}

function parentMonth(invoice) {
    return PARENT_MONTHS_NOMINATIVE[parentDate(invoice.period_start).getMonth()];
}

function parentPlace(item) {
    return [item.group ? `${item.group} группа` : null, item.location].filter(Boolean).join(', ');
}

function parentLessonsWord(count) {
    const tens = count % 100;
    const ones = count % 10;
    if (tens >= 11 && tens <= 14) return `${count} занятий`;
    if (ones === 1) return `${count} занятие`;
    if (ones >= 2 && ones <= 4) return `${count} занятия`;
    return `${count} занятий`;
}

function parentStudent() {
    const list = parentData?.students || [];
    return list.find(item => item.id === parentUi.studentId) || list[0] || null;
}

function parentPayableInvoice(student) {
    const wanted = parentUi.extraId || student.oldest_open_invoice_id;
    return (student.invoices || []).find(item => item.id === wanted) || null;
}

function parentIsExtra() {
    return parentUi.extraId !== null;
}

async function loadParentContext() {
    const container = document.getElementById('parent-content');
    if (!container) return;
    if (!parentData) renderScreenState(container, 'Загружаем кабинет…', {loading: true});
    try {
        parentData = await apiPost('/api/parent/context');
        renderParentContext();
    } catch (error) {
        renderScreenState(container, error.message || 'Не удалось загрузить данные', {
            retry: 'loadParentContext()'
        });
    }
}

function renderParentContext() {
    const container = document.getElementById('parent-content');
    if (!container || !parentData) return;
    const student = parentStudent();
    if (student) parentUi.studentId = student.id;
    const screens = {
        subscription: [renderParentSubscription, 1],
        pay: [renderParentPay, 2],
        payDone: [renderParentPayDone, 1],
        makeup: [renderParentMakeup, 1],
        schedule: [renderParentSchedule, 1],
    };
    const tabs = {home: renderParentHome, lessons: renderParentLessons, profile: renderParentProfile};
    const [screen, depth] = screens[parentUi.view] || [tabs[parentUi.tab], 0];
    const atRoot = depth === 0;
    const key = atRoot ? `tab:${parentUi.tab}:${parentUi.studentId}` : parentUi.view;
    const html = `<div class="pa">${student ? screen(student) : renderParentEmpty()}${student && atRoot ? parentNav() : ''}</div>`;
    const stays = currentScreen?.key === key;
    const scrollTop = window.scrollY;
    paintScreen(container, html, key, depth);
    const nextScroll = stays ? scrollTop : 0;
    if (window.scrollY !== nextScroll) window.scrollTo(0, nextScroll);
}

function parentNav() {
    const items = [['home', 'home', 'Главная'], ['lessons', 'calendar', 'Занятия'], ['profile', 'user', 'Профиль']];
    return `
        <nav class="pa-nav" aria-label="Разделы">
            ${items.map(([id, icon, label]) => `
                <button type="button" data-pa="tab" data-value="${id}" ${parentUi.tab === id ? 'aria-current="page"' : ''}>
                    ${bwIcon(icon)}
                    <span>${label}</span>
                </button>`).join('')}
        </nav>`;
}

function parentHeading(title) {
    return `
        <div class="pa-head">
            <div class="pa-brand">Break Wave</div>
            <h1 data-large-title="${escapeHtml(title)}">${escapeHtml(title)}</h1>
        </div>`;
}

function parentRegistrationCards() {
    return (parentData.registration_requests || [])
        .filter(item => item.status !== 'approved')
        .map(item => `
            <section class="pa-panel ${item.status === 'pending' ? 'is-calm' : 'is-danger'}">
                <div class="pa-panel-title">${escapeHtml(item.child_name)}</div>
                <div>${item.status === 'pending' ? 'Анкета ждёт подтверждения тренера' : 'Анкета отклонена'}</div>
                ${item.rejection_reason ? `<div>Причина: ${escapeHtml(item.rejection_reason)}</div>` : ''}
            </section>
        `).join('');
}

function renderParentEmpty() {
    const cards = parentRegistrationCards();
    return `
        ${parentHeading('Семья')}
        <div class="pa-body">
            ${cards || `<div class="st-empty">${bwIcon('users', 32)}<span>Детей пока нет</span></div>`}
            <button type="button" class="pa-btn ${cards ? '' : 'is-primary'}" data-pa="add-child">${bwIcon('plus', 20)}Добавить ребёнка</button>
        </div>`;
}

/* --- Home --- */

function parentTrainingCard(student) {
    const training = student.next_training;
    if (!training) {
        return `
            <section class="pa-hero">
                <div class="pa-eyebrow">Следующая тренировка</div>
                <div class="pa-hero-title">Расписание назначит тренер</div>
            </section>`;
    }
    const answers = [['attending', 'Придёт', 'check'], ['absent', 'Не придёт', 'x']];
    const notes = {
        attending: 'Ждём на тренировке. Нажмите другой вариант, чтобы изменить ответ.',
        absent: 'Тренер предупреждён. Занятие спишется. В оплаченном месяце вместо него появится отработка на 2 месяца.',
    };
    return `
        <section class="pa-hero">
            <div class="pa-eyebrow">${escapeHtml(parentNearDate(training.date))}</div>
            <div>
                <div class="pa-hero-time is-huge">${escapeHtml(training.time)}</div>
                <div class="pa-muted">${escapeHtml(parentPlace(training))}</div>
            </div>
            <div class="pa-two">
                ${answers.map(([value, label, icon]) => `
                    <button type="button" class="pa-btn pa-answer" data-pa="rsvp" data-value="${training.response === value ? '' : value}" aria-pressed="${training.response === value}">${bwIcon(icon, 20)}${label}</button>`).join('')}
            </div>
            <div class="pa-muted pa-small">${notes[training.response] || 'Вы ещё не ответили'}</div>
        </section>`;
}

// What the current month looks like. Lessons count as remaining only once the month is paid.
function parentMonthStatus(student) {
    const invoice = student.invoice;
    const total = student.lessons_count || 0;
    const left = student.lessons_remaining || 0;
    if (['paid', 'written_off'].includes(invoice.status)) {
        if (left <= 0) return {paid: true, text: 'Занятия закончились', css: 'pa-danger'};
        return {
            paid: true,
            text: `Осталось ${left} из ${total}`,
            note: left <= LOW_LESSONS ? 'Занятия заканчиваются' : '',
            css: left <= LOW_LESSONS ? 'pa-warn' : 'pa-muted',
        };
    }
    const labels = {
        reported: ['Оплата проверяется', 'pa-warn'],
        awaiting_receipt: ['Нужен чек', 'pa-warn'],
        rejected: ['Оплата отклонена', 'pa-danger'],
        overdue: ['Есть задолженность', 'pa-danger'],
        skipped: ['Занятий в этом месяце не было', 'pa-muted'],
    };
    const [text, css] = labels[invoice.status] || ['Не оплачен', 'pa-warn'];
    return {paid: false, text, css, note: left < total ? `По учёту: ${left} из ${total}` : ''};
}

function parentProgress(student) {
    return bwSegments(student.lessons_remaining || 0, student.lessons_count || 0);
}

function parentSubscriptionRow(student) {
    if (!student.invoice) {
        return `
            <section class="pa-panel is-calm">
                <div>${student.starts_on
                    ? `Занятия начнутся ${escapeHtml(parentShortDate(student.starts_on))}, тогда же появится счёт.`
                    : 'В этом месяце тренировок нет.'}</div>
            </section>`;
    }
    const status = parentMonthStatus(student);
    if (status.paid) {
        const note = student.lessons_remaining > 0 ? status.note : status.text;
        return `
            <button type="button" class="pa-rowlink pa-subscription" data-pa="go-subscription">
                <span class="pa-rowlink-main">
                    <span class="pa-rowlink-title">Осталось занятий</span>
                    <span class="pa-rowlink-count">${Math.max(student.lessons_remaining, 0)} из ${student.lessons_count}</span>
                    ${bwChevron()}
                </span>
                ${parentProgress(student)}
                ${note ? `<span class="${status.css} pa-small">${note}</span>` : ''}
            </button>`;
    }
    return `
        <button type="button" class="pa-rowlink pa-subscription" data-pa="go-subscription">
            <span class="pa-rowlink-main">
                ${bwTile('credit-card')}
                <span class="pa-stack">
                    <span class="pa-strong">Абонемент · ${parentLessonsWord(student.lessons_count)}</span>
                    <span class="${status.css} pa-small">${status.text}${status.note ? ` · ${status.note}` : ''}</span>
                </span>
                ${bwChevron()}
            </span>
        </button>`;
}

// One urgent payment at most: the oldest month that is still open.
function parentPaymentPanel(student) {
    const invoice = (student.invoices || []).find(item => item.id === student.oldest_open_invoice_id);
    if (!invoice) return '';
    const month = escapeHtml(parentMonth(invoice));
    if (invoice.stored_status === 'reported') {
        return `<section class="pa-panel is-calm"><div>${month}: ${invoice.amount} Br на проверке</div></section>`;
    }
    const panel = (tone, title, buttonLabel, note = '') => `
        <section class="pa-panel pa-pay ${tone}">
            <div class="pa-stack">
                <div class="pa-small">${title}</div>
                <div class="pa-pay-sum">${invoice.amount} Br</div>
            </div>
            <button type="button" class="pa-btn is-primary" data-pa="go-pay">${buttonLabel}</button>
            ${note ? `<div class="pa-pay-note">${note}</div>` : ''}
        </section>`;
    if (invoice.stored_status === 'awaiting_receipt') {
        return panel('is-warn', `Прикрепите чек за ${month.toLowerCase()}`, `${bwIcon('upload', 20)}Чек`);
    }
    const isLate = parentDate(invoice.due_date) < parentDate(parentToday());
    if (invoice.stored_status === 'rejected') {
        return panel('is-danger', `Оплата за ${month.toLowerCase()} отклонена`, 'Оплатить', escapeHtml(invoice.rejection_reason || ''));
    }
    if (invoice.status === 'overdue') return panel('is-danger', `Долг за ${month.toLowerCase()}`, 'Оплатить');
    if (isLate) return panel('is-action', month, 'Оплатить', 'Если ребёнка в этом месяце не будет, можно не оплачивать.');
    return panel('is-action', `Оплатить до ${escapeHtml(parentShortDate(invoice.due_date))}`, 'Оплатить');
}

function parentToday() {
    const now = new Date();
    return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
}

function parentScheduleDays(student) {
    const rows = [];
    student.schedules.forEach(schedule => {
        String(schedule.days || '').split(',').filter(Boolean).forEach(day => {
            const index = Number(day);
            rows.push({index, time: schedule.times[String(index)] || '', place: parentPlace(schedule)});
        });
    });
    return rows.sort((left, right) => left.index - right.index);
}

function parentHomeRows(student) {
    const days = parentScheduleDays(student);
    const available = student.makeups.filter(item => item.status === 'available').length;
    const row = (icon, title, value, tab) => `
        <button type="button" class="pa-list-row" data-pa="open-lessons" data-value="${tab}">
            ${bwTile(icon)}
            <span class="pa-strong">${title}</span>
            <span class="pa-list-value">${escapeHtml(value)}</span>
            ${bwChevron()}
        </button>`;
    return `
        <div class="pa-list">
            ${row('calendar', 'Расписание', days.map(item => PARENT_WEEKDAYS_SHORT[item.index]).join(' · ') || 'Не назначено', 'schedule')}
            ${row('clock', 'Отработки', available ? `Доступно: ${available}` : 'Нет', 'makeups')}
        </div>`;
}

function renderParentHome(student) {
    return `
        <div class="pa-head is-row">
            <div class="pa-logo" data-large-title="Break Wave">Break Wave</div>
            <div class="pa-segment is-capsule">
                ${parentData.students.map(item => `
                    <button type="button" data-pa="pick-student" data-id="${item.id}" aria-pressed="${item.id === student.id}">${escapeHtml(item.name.split(' ')[0])}</button>
                `).join('')}
            </div>
        </div>
        <div class="pa-body">
            ${parentRegistrationCards()}
            ${parentTrainingCard(student)}
            ${parentSubscriptionRow(student)}
            ${parentPaymentPanel(student)}
            ${parentHomeRows(student)}
        </div>`;
}

/* --- Subscription --- */

function parentExtrasBlock(student) {
    const open = student.open_extras.map(item => `
        <button type="button" class="pa-btn" data-pa="open-extra" data-id="${item.id}" ${item.stored_status === 'reported' ? 'disabled' : ''}>${escapeHtml(item.label)}: ${item.amount} Br${item.stored_status === 'reported' ? ', на проверке' : ''}</button>`).join('');
    const offer = student.upgrade_offer;
    const hasOpenUpgrade = student.open_extras.some(item => item.kind === 'upgrade');
    return `
        ${student.lessons_over ? `<section class="pa-panel is-warn"><div>Занятий в этом месяце больше, чем в абонементе: на ${student.lessons_over}</div></section>` : ''}
        ${offer && !hasOpenUpgrade ? `
            <section class="pa-panel is-action">
                <div class="pa-panel-title">Занятий может не хватить</div>
                <div>До конца месяца тренировок: ${student.trainings_left}, занятий осталось: ${student.lessons_remaining}.</div>
                <button type="button" class="pa-btn is-primary" data-pa="buy-upgrade">Перейти на ${offer.lessons} занятий, доплата ${offer.amount} Br</button>
            </section>` : ''}
        ${open}
        ${student.open_extras.some(item => item.kind === 'single') ? '' : `<button type="button" class="pa-btn" data-pa="buy-single">${bwIcon('plus', 20)}Разовое занятие за 40 Br</button>`}`;
}

function renderParentSubscription(student) {
    if (!student.invoice) return renderParentHome(student);
    const status = parentMonthStatus(student);
    const month = parentMonth(student.invoice);
    return `
        ${parentBar('Абонемент', 'go-home', student.name)}
        <div class="pa-body">
            <section class="pa-hero">
                <div class="pa-eyebrow">${escapeHtml(month)}</div>
                ${status.paid ? `
                    <div class="pa-hero-count">
                        <span class="pa-hero-time">${student.lessons_remaining}</span>
                        <span class="pa-muted">из ${student.lessons_count} занятий осталось</span>
                    </div>
                    ${parentProgress(student)}` : `
                    <div class="pa-hero-title">${parentLessonsWord(student.lessons_count)}</div>`}
                <div class="${status.css}">${status.paid ? (student.lessons_remaining > 0 ? status.note || student.invoice.label : status.text) : status.text}${!status.paid && status.note ? ` · ${status.note}` : ''}</div>
            </section>
            ${parentPaymentPanel(student)}
            ${parentExtrasBlock(student)}
            <button type="button" class="pa-list-row pa-list-alone" data-pa="open-lessons" data-value="payments">
                ${bwTile('receipt')}
                <span class="pa-strong">История оплат</span>
                <span class="pa-list-value"></span>
                ${bwChevron()}
            </button>
        </div>`;
}

/* --- Lessons tab --- */

function parentLessonsSchedule(student) {
    const rows = parentScheduleDays(student);
    const request = (parentData.schedule_requests || []).find(item => item.student_id === student.id);
    return `
        ${rows.length ? `<div class="pa-list">
            ${rows.map(row => `
                <div class="pa-list-row">
                    <span class="pa-stack">
                        <span class="pa-strong">${PARENT_WEEKDAYS[row.index]}</span>
                        ${row.place ? `<span class="pa-muted pa-small">${escapeHtml(row.place)}</span>` : ''}
                    </span>
                    <span class="pa-list-value pa-strong">${escapeHtml(row.time)}</span>
                </div>`).join('')}
        </div>` : '<div class="st-empty">Расписание назначит тренер</div>'}
        ${request?.status === 'pending' ? '<section class="pa-panel is-calm"><div>Новое расписание ждёт ответа тренера. Пока действует прежнее.</div></section>' : ''}
        ${request?.status === 'rejected' ? `<section class="pa-panel is-danger"><div>Изменение отклонено${request.rejection_reason ? `: ${escapeHtml(request.rejection_reason)}` : ''}</div></section>` : ''}
        <button type="button" class="pa-btn" data-pa="go-schedule">${bwIcon('edit', 20)}Изменить расписание</button>`;
}

function parentLessonsMakeups(student) {
    const available = student.makeups.filter(item => item.status === 'available');
    const requested = student.makeups.filter(item => item.status === 'requested');
    const scheduled = student.makeups.filter(item => item.status === 'scheduled');
    const rejected = available.find(item => item.rejection_reason);
    if (!student.makeups.length) {
        return `<div class="st-empty">${bwIcon('clock', 32)}<span>Сейчас отработок нет</span></div>`;
    }
    return `
        <section class="pa-hero">
            <div class="pa-eyebrow">Доступно</div>
            <div class="pa-hero-time is-huge">${available.length}</div>
            ${available.length ? `<div class="pa-muted">Первая действует до ${escapeHtml(parentShortDate(available[0].expires_at))}</div>` : ''}
            ${available.length ? '<button type="button" class="pa-btn is-primary" data-pa="go-makeup">Записаться на отработку</button>' : ''}
        </section>
        ${scheduled.map(item => `
            <section class="pa-panel is-ok"><div>Записаны: ${escapeHtml(parentLongDate(item.scheduled_date))}${item.scheduled_time ? `, ${escapeHtml(item.scheduled_time)}` : ''}</div></section>
        `).join('')}
        ${requested.map(item => `
            <section class="pa-panel is-calm"><div>Заявка на ${escapeHtml(parentShortDate(item.requested_date))} ждёт ответа тренера</div></section>
        `).join('')}
        ${rejected ? `<section class="pa-panel is-danger"><div>Заявка отклонена: ${escapeHtml(rejected.rejection_reason)}</div></section>` : ''}`;
}

function parentLessonsPayments(student) {
    const paid = (student.invoices || []).filter(item => item.stored_status === 'paid');
    if (!paid.length) return `<div class="st-empty">${bwIcon('receipt', 32)}<span>Оплат пока нет</span></div>`;
    return `
        <div class="pa-list">
            ${paid.map(item => `
                <div class="pa-list-row">
                    <span class="pa-stack">
                        <span class="pa-strong">${escapeHtml(parentMonth(item))} ${parentDate(item.period_start).getFullYear()}</span>
                        <span class="pa-muted pa-small">${escapeHtml(item.label)}</span>
                    </span>
                    <span class="pa-list-value pa-strong">${item.amount} Br</span>
                </div>`).join('')}
        </div>`;
}

function renderParentLessons(student) {
    const sections = [['schedule', 'Расписание', parentLessonsSchedule], ['makeups', 'Отработки', parentLessonsMakeups], ['payments', 'Платежи', parentLessonsPayments]];
    const current = sections.find(([id]) => id === parentUi.lessonsTab);
    const several = parentData.students.length > 1;
    return `
        ${parentHeading('Занятия')}
        <div class="pa-body">
            ${several ? `<div class="pa-pills">
                ${parentData.students.map(item => `
                    <button type="button" data-pa="pick-student" data-id="${item.id}" aria-pressed="${item.id === student.id}">${escapeHtml(item.name.split(' ')[0])}</button>`).join('')}
            </div>` : ''}
            <div class="pa-segment">
                ${sections.map(([id, label]) => `
                    <button type="button" data-pa="pick-lessons" data-value="${id}" aria-pressed="${id === parentUi.lessonsTab}">${label}</button>`).join('')}
            </div>
            ${current[2](student)}
        </div>`;
}

/* --- Profile tab --- */

function renderParentProfile() {
    const parent = parentData.parent;
    return `
        ${parentHeading('Профиль')}
        <div class="pa-body">
            <h2 class="pa-section">Родитель</h2>
            <div class="pa-list">
                <div class="pa-list-row">
                    ${bwTile('user')}
                    <span class="pa-stack">
                        <span class="pa-strong">${escapeHtml(parent?.full_name)}</span>
                        <span class="pa-muted pa-small">${escapeHtml(parent?.phone)}</span>
                    </span>
                </div>
            </div>
            <h2 class="pa-section">Дети</h2>
            ${parentRegistrationCards()}
            <div class="pa-list">
                ${parentData.students.map(item => `
                    <button type="button" class="pa-list-row" data-pa="pick-student" data-id="${item.id}" data-tab="home">
                        <span class="st-avatar">${escapeHtml(bwInitials(item.name))}</span>
                        <span class="pa-strong">${escapeHtml(item.name)}</span>
                        <span class="pa-list-value"></span>
                        ${bwChevron()}
                    </button>`).join('')}
                <button type="button" class="pa-list-row pa-list-action" data-pa="add-child">
                    ${bwTile('plus')}
                    <span class="pa-strong">Добавить ребёнка</span>
                </button>
            </div>
            <h2 class="pa-section">Напоминать накануне в 18:00</h2>
            <div class="pa-list">
                ${parentData.students.map(item => `
                    <button type="button" class="pa-switch" data-pa="toggle-reminder" data-id="${item.id}" aria-pressed="${item.training_reminders_enabled ? 'true' : 'false'}">
                        ${bwTile('bell')}
                        <span>${escapeHtml(item.name.split(' ')[0])}</span>
                        <span class="pa-switch-track"></span>
                    </button>`).join('')}
            </div>
        </div>`;
}

/* --- Nested screens --- */

function parentBar(title, action = 'go-home', subtitle = '') {
    return `
        <div class="pa-bar">
            <button type="button" class="pa-back" data-pa="${action}" data-back aria-label="Назад">${bwIcon('chevron-left', 26)}</button>
            <div>
                <h1>${escapeHtml(title)}</h1>
                ${subtitle ? `<div class="pa-muted pa-small">${escapeHtml(subtitle)}</div>` : ''}
            </div>
            <span class="pa-bar-end"></span>
        </div>`;
}

function renderParentPay(student) {
    const invoice = parentPayableInvoice(student);
    if (!invoice) return renderParentHome(student);
    const isExtra = parentIsExtra();
    const canChooseTariff = !isExtra && invoice.id === student.invoice?.id && student.tariff_change_allowed;
    const tariffCode = parentUi.tariff || invoice.tariff_code;
    const amount = canChooseTariff ? parentData.tariffs[tariffCode].price : invoice.amount;
    const partialNote = invoice.is_partial
        ? `<div class="pa-label">${parentLessonsWord(invoice.lessons_count)} × 40 Br${invoice.lessons_count * 40 > invoice.amount ? `, но не дороже абонемента: ${invoice.amount} Br` : ` = ${invoice.amount} Br`}</div>`
        : '';
    const tariffBlock = canChooseTariff
        ? `
            ${Object.entries(parentData.tariffs).filter(([code]) => code !== 'single').map(([code, tariff]) => {
                const blocked = student.tariff_upgrade_only && tariff.lessons < invoice.lessons_count;
                return `
                    <button type="button" class="pa-choice" data-pa="pick-tariff" data-value="${escapeHtml(code)}" aria-pressed="${code === tariffCode}" ${blocked ? 'disabled' : ''}>
                        <span>${escapeHtml(tariff.label)}</span>
                        <span class="pa-strong">${tariff.price} Br</span>
                    </button>`;
            }).join('')}
            <div class="pa-label">${student.tariff_upgrade_only ? 'Уменьшить абонемент можно до 5 числа, увеличить можно всегда' : 'Уменьшить абонемент можно до 5 числа'}</div>`
        : `<div class="pa-note">${escapeHtml(invoice.label)}: ${amount} Br</div>${partialNote}`;
    const online = parentUi.method === 'online';
    const details = parentData.payment_details
        ? `Переведите ${amount} Br по реквизитам:\n${parentData.payment_details}\n\nЗатем прикрепите чек.`
        : `Переведите ${amount} Br. Реквизиты уточните у тренера, затем прикрепите чек.`;
    const action = online
        ? `
            <input id="pa-receipt" class="pa-file" type="file" accept="image/*,application/pdf" data-pa-file ${parentUi.busy ? 'disabled' : ''}>
            <label for="pa-receipt" class="pa-btn ${parentUi.busy ? 'is-busy' : 'is-primary'}">${parentUi.busy ? 'Отправляем…' : `${bwIcon('upload', 20)}Прикрепить чек · ${amount} Br`}</label>`
        : `<button type="button" class="pa-btn is-primary" data-pa="submit-cash" ${parentUi.busy ? 'disabled' : ''}>${parentUi.busy ? 'Отправляем…' : `Я передам наличные · ${amount} Br`}</button>`;
    const title = isExtra ? (invoice.kind === 'single' ? 'Разовое занятие' : 'Доплата') : `Оплата за ${parentMonth(invoice).toLowerCase()}`;
    return `
        ${parentBar(title, 'pay-back', student.name)}
        <div class="pa-body">
            <div class="pa-group">
                <h2 class="pa-section">Абонемент</h2>
                ${tariffBlock}
            </div>
            <div class="pa-group">
                <h2 class="pa-section">Как оплатить</h2>
                <div class="pa-segment">
                    <button type="button" data-pa="pick-method" data-value="online" aria-pressed="${online}">Онлайн</button>
                    <button type="button" data-pa="pick-method" data-value="cash" aria-pressed="${!online}">Наличными</button>
                </div>
                <div class="pa-note">${online ? escapeHtml(details) : `Передайте ${amount} Br тренеру или администратору на тренировке.`}</div>
            </div>
        </div>
        <div class="pa-foot">${action}</div>`;
}

function renderParentPayDone() {
    return `
        <div class="pa-stack pa-center">
            <div class="pa-result">${bwIcon('clock', 40)}</div>
            <h1>Платёж отправлен на проверку</h1>
            <p class="pa-muted">Администратор проверит его, и бот сообщит результат в Telegram.</p>
            <button type="button" class="pa-btn is-primary" data-pa="go-home" data-back>На главную</button>
        </div>`;
}

function renderParentMakeup(student) {
    const slots = student.makeup_slots || [];
    return `
        ${parentBar('Отработка', 'go-home', student.name)}
        <div class="pa-body">
            <div class="pa-group">
                <h2 class="pa-section">Выберите тренировку</h2>
                ${slots.length ? slots.map((slot, index) => `
                    <button type="button" class="pa-choice pa-choice-column" data-pa="pick-slot" data-value="${index}" aria-pressed="${parentUi.slot === index}">
                        <span class="pa-strong">${escapeHtml(parentLongDate(slot.date))}, ${escapeHtml(slot.time)}</span>
                        <span class="pa-muted pa-small">${escapeHtml(parentPlace(slot))}</span>
                    </button>`).join('') : '<div class="pa-note">Подходящих тренировок пока нет. Договоритесь об отработке с тренером.</div>'}
            </div>
        </div>
        <div class="pa-foot">
            <button type="button" class="pa-btn is-primary" data-pa="submit-makeup" ${parentUi.slot === null || parentUi.busy ? 'disabled' : ''}>${parentUi.busy ? 'Отправляем…' : 'Отправить заявку'}</button>
        </div>`;
}

function renderParentSchedule(student) {
    const options = selected => PARENT_WEEKDAYS.map((label, index) =>
        `<option value="${index}" ${index === Number(selected) ? 'selected' : ''}>${label}</option>`
    ).join('');
    return `
        ${parentBar('Изменить расписание', 'go-home', student.name)}
        <div class="pa-body">
            <div class="pa-group">
                <h2 class="pa-section">Какие дни и время удобны</h2>
                ${parentUi.scheduleRows.map((row, index) => `
                    <div class="pa-schedule-row">
                        <select class="pa-field" aria-label="День" data-pa-row="${index}" data-field="day">${options(row.day)}</select>
                        <input class="pa-field" type="time" aria-label="Время" value="${escapeHtml(row.time)}" data-pa-row="${index}" data-field="time">
                        <button type="button" class="pa-remove" data-pa="remove-row" data-value="${index}" aria-label="Убрать">${bwIcon('x', 20)}</button>
                    </div>`).join('')}
                <button type="button" class="pa-btn" data-pa="add-row">${bwIcon('plus', 20)}Добавить день</button>
                <div class="pa-label">Тренер подтвердит новое расписание или предложит другое.</div>
            </div>
        </div>
        <div class="pa-foot">
            <button type="button" class="pa-btn is-primary" data-pa="submit-schedule" ${!parentUi.scheduleRows.length || parentUi.busy ? 'disabled' : ''}>${parentUi.busy ? 'Отправляем…' : 'Отправить тренеру'}</button>
        </div>`;
}

function parentGo(view, changes = {}) {
    Object.assign(parentUi, {view, busy: false}, changes);
    renderParentContext();
}

async function parentRun(action) {
    parentUi.busy = true;
    renderParentContext();
    try {
        await action();
    } catch (error) {
        showNotification(error.message || 'Не удалось выполнить действие', 'error');
    }
    parentUi.busy = false;
    await loadParentContext();
}

function parentScheduleRowsFrom(student) {
    const rows = [];
    student.schedules.forEach(schedule => {
        String(schedule.days || '').split(',').filter(Boolean).forEach(day => {
            rows.push({day: Number(day), time: schedule.times[String(Number(day))] || '18:00'});
        });
    });
    return rows.length ? rows : [{day: 0, time: '18:00'}];
}

async function parentSaveTariff(student, invoice) {
    const canChoose = !parentIsExtra() && invoice.id === student.invoice?.id && student.tariff_change_allowed;
    if (canChoose && parentUi.tariff && parentUi.tariff !== invoice.tariff_code) {
        await apiPost('/api/parent/tariff', {student_id: student.id, tariff_code: parentUi.tariff});
    }
}

async function parentUploadReceipt(paymentId, file) {
    const form = new FormData();
    form.append('initData', initData);
    form.append('file', file);
    const response = await fetch(`${API}/api/parent/payments/${paymentId}/receipt`, {method: 'POST', body: form});
    const data = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(data.message || 'Не удалось отправить чек');
}

async function parentSubmitReceipt(file) {
    const student = parentStudent();
    const invoice = parentPayableInvoice(student);
    await parentRun(async () => {
        await parentSaveTariff(student, invoice);
        if (invoice.stored_status !== 'awaiting_receipt') {
            await apiPost(`/api/parent/payments/${invoice.id}/report`, {payment_method: 'online'});
        }
        await parentUploadReceipt(invoice.id, file);
        Object.assign(parentUi, {view: 'payDone', tariff: null, extraId: null});
    });
}

function parentOpenPay(changes) {
    parentGo('pay', {payFrom: parentUi.view, tariff: null, extraId: null, ...changes});
}

const PARENT_ACTIONS = {
    tab: target => {
        parentGo('home', {tab: target.dataset.value});
        // The tab opens at once; fresh data arrives behind the scenes.
        loadParentContext();
    },
    'add-child': () => renderRegistration({existingParent: parentData.parent}),
    'pick-student': target => parentGo('home', {
        studentId: Number(target.dataset.id),
        tab: target.dataset.tab || parentUi.tab,
        tariff: null,
    }),
    'pick-lessons': target => parentGo('home', {lessonsTab: target.dataset.value}),
    'open-lessons': target => parentGo('home', {tab: 'lessons', lessonsTab: target.dataset.value}),
    'go-home': () => parentGo('home', {tariff: null, slot: null, extraId: null}),
    'go-subscription': () => parentGo('subscription', {extraId: null, tariff: null}),
    'go-pay': () => parentOpenPay({}),
    'pay-back': () => parentGo(parentUi.payFrom === 'subscription' ? 'subscription' : 'home', {tariff: null, extraId: null}),
    'open-extra': target => parentOpenPay({extraId: Number(target.dataset.id)}),
    'buy-upgrade': () => parentRun(async () => {
        const {id} = await apiPost('/api/parent/upgrade', {student_id: parentStudent().id});
        Object.assign(parentUi, {view: 'pay', payFrom: 'subscription', tariff: null, extraId: id});
    }),
    'buy-single': () => parentRun(async () => {
        const {id} = await apiPost('/api/parent/single', {student_id: parentStudent().id});
        Object.assign(parentUi, {view: 'pay', payFrom: 'subscription', tariff: null, extraId: id});
    }),
    'go-makeup': () => parentGo('makeup', {slot: null}),
    'go-schedule': () => parentGo('schedule', {scheduleRows: parentScheduleRowsFrom(parentStudent())}),
    'pick-tariff': target => parentGo('pay', {tariff: target.dataset.value}),
    'pick-method': target => parentGo('pay', {method: target.dataset.value}),
    'pick-slot': target => parentGo('makeup', {slot: Number(target.dataset.value)}),
    'add-row': () => {
        parentUi.scheduleRows.push({day: 0, time: '18:00'});
        renderParentContext();
    },
    'remove-row': target => {
        parentUi.scheduleRows.splice(Number(target.dataset.value), 1);
        renderParentContext();
    },
    rsvp: target => {
        const student = parentStudent();
        const training = student.next_training;
        return parentRun(() => apiPost('/api/parent/training-response', {
            student_id: student.id,
            date: training.date,
            time: training.time,
            location_id: training.location_id,
            response: target.dataset.value || null,
        }));
    },
    'toggle-reminder': target => {
        const student = parentData.students.find(item => item.id === Number(target.dataset.id));
        // The switch moves at once; the reload after the request restores it if saving fails.
        student.training_reminders_enabled = !student.training_reminders_enabled;
        return parentRun(() => apiPost('/api/parent/reminders', {
            student_id: student.id,
            enabled: student.training_reminders_enabled,
        }));
    },
    'submit-cash': () => {
        const student = parentStudent();
        const invoice = parentPayableInvoice(student);
        return parentRun(async () => {
            await parentSaveTariff(student, invoice);
            await apiPost(`/api/parent/payments/${invoice.id}/report`, {payment_method: 'cash'});
            Object.assign(parentUi, {view: 'payDone', tariff: null, extraId: null});
        });
    },
    'submit-makeup': () => {
        const student = parentStudent();
        const slot = student.makeup_slots[parentUi.slot];
        const credit = student.makeups.find(item => item.status === 'available');
        return parentRun(async () => {
            await apiPost(`/api/parent/makeups/${credit.id}/request`, {
                requested_date: slot.date,
                requested_time: slot.time,
                requested_location_id: slot.location_id,
                requested_group_id: slot.group_id,
            });
            Object.assign(parentUi, {view: 'home', slot: null});
            showNotification('Заявка отправлена тренеру');
        });
    },
    'submit-schedule': () => {
        const student = parentStudent();
        return parentRun(async () => {
            await apiPost('/api/parent/schedule-request', {
                student_id: student.id,
                proposed_schedule: parentUi.scheduleRows.map(row => ({weekday: row.day, time: row.time})),
            });
            parentUi.view = 'home';
            showNotification('Заявка отправлена тренеру');
        });
    },
};

document.addEventListener('click', event => {
    const target = event.target.closest('[data-pa]');
    if (!target || target.disabled) return;
    const action = PARENT_ACTIONS[target.dataset.pa];
    if (action) runControl(target, action);
});

document.addEventListener('change', event => {
    const target = event.target;
    if (target.matches('[data-pa-file]') && target.files[0]) {
        parentSubmitReceipt(target.files[0]);
    } else if (target.matches('[data-pa-row]')) {
        const row = parentUi.scheduleRows[Number(target.dataset.paRow)];
        if (target.dataset.field === 'day') row.day = Number(target.value);
        else row.time = target.value;
    }
});
