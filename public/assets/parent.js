/* === Parent cabinet === */

const PARENT_MONTHS = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'];
const PARENT_MONTHS_NOMINATIVE = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь', 'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'];
const PARENT_WEEKDAYS = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье'];
const PARENT_BACK_ICON = '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M15 4.5L7.5 12l7.5 7.5"></path></svg>';
const REMOVE_ICON = '<svg width="22" height="22" viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="10" fill="currentColor"></circle><path d="M8.5 8.5l7 7M15.5 8.5l-7 7" fill="none" stroke="#07111f" stroke-width="2" stroke-linecap="round"></path></svg>';

const parentUi = {
    studentId: null,
    view: 'home',
    tariff: null,
    method: 'online',
    slot: null,
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

function parentStudent() {
    const list = parentData?.students || [];
    return list.find(item => item.id === parentUi.studentId) || list[0] || null;
}

function parentPayableInvoice(student) {
    const invoices = student.invoices || [student.invoice];
    return invoices.find(item => item.id === student.oldest_open_invoice_id) || null;
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
    const views = {
        home: renderParentHome,
        pay: renderParentPay,
        payDone: renderParentPayDone,
        makeup: renderParentMakeup,
        schedule: renderParentSchedule,
    };
    const scrollTop = window.scrollY;
    const html = `<div class="pa">${(student ? views[parentUi.view] : renderParentEmpty)(student)}</div>`;
    const atHome = parentUi.view === 'home';
    paintScreen(container, html, atHome ? `home:${parentUi.studentId}` : parentUi.view, atHome ? 0 : 1);
    const nextScroll = atHome ? scrollTop : 0;
    if (window.scrollY !== nextScroll) window.scrollTo(0, nextScroll);
}

function parentRegistrationCards() {
    return (parentData.registration_requests || [])
        .filter(item => item.status !== 'approved')
        .map(item => `
            <section class="pa-card">
                <div>
                    <div class="pa-mid">${escapeHtml(item.child_name)}</div>
                    ${item.status === 'pending'
                        ? '<div class="pa-warn">Анкета ждёт подтверждения тренера</div>'
                        : '<div class="pa-danger">Анкета отклонена</div>'}
                    ${item.rejection_reason ? `<div class="pa-muted">Причина: ${escapeHtml(item.rejection_reason)}</div>` : ''}
                </div>
            </section>
        `).join('');
}

function renderParentEmpty() {
    const cards = parentRegistrationCards();
    return `
        <div class="pa-head"><h1>Break Wave</h1></div>
        <div class="pa-body">
            ${cards || `
                <section class="pa-card">
                    <div class="pa-mid">Детей пока нет</div>
                </section>`}
            <button type="button" class="pa-btn" data-pa="add-child">Добавить ребёнка</button>
        </div>`;
}

function parentTrainingCard(student) {
    const training = student.next_training;
    if (!training) {
        return `
            <section class="pa-card">
                <div class="pa-label">Следующая тренировка</div>
                <div>Расписание назначит тренер</div>
            </section>`;
    }
    let answer = `
        <div class="pa-two">
            <button type="button" class="pa-btn is-primary" data-pa="rsvp" data-value="attending">Придёт</button>
            <button type="button" class="pa-btn" data-pa="rsvp" data-value="absent">Не придёт</button>
        </div>`;
    if (training.response === 'attending') {
        answer = `
            <div class="pa-row">
                <div class="pa-ok">Ждём на тренировке</div>
                <button type="button" class="pa-link" data-pa="rsvp" data-value="">Изменить</button>
            </div>`;
    } else if (training.response === 'absent') {
        answer = `
            <div class="pa-stack">
                <div class="pa-warn">Тренер предупреждён</div>
                <div class="pa-muted pa-small">Занятие спишется, вместо него появится отработка на 2 месяца.</div>
                <button type="button" class="pa-link" data-pa="rsvp" data-value="">Изменить</button>
            </div>`;
    }
    return `
        <section class="pa-card">
            <div>
                <div class="pa-label">Следующая тренировка</div>
                <div class="pa-big">${escapeHtml(parentNearDate(training.date))}, ${escapeHtml(training.time)}</div>
                <div class="pa-muted">${escapeHtml(parentPlace(training))}</div>
            </div>
            ${answer}
        </section>`;
}

function parentPaymentBlock(student) {
    const invoice = parentPayableInvoice(student);
    if (!invoice) {
        return `<div class="pa-ok">${escapeHtml(parentMonth(student.invoice))} оплачен</div>`;
    }
    const month = escapeHtml(parentMonth(invoice));
    if (invoice.stored_status === 'reported') {
        return `<div class="pa-warn">${month}: ${invoice.amount} Br на проверке</div>`;
    }
    if (invoice.stored_status === 'awaiting_receipt') {
        return `
            <div class="pa-warn">${month}: ${invoice.amount} Br, нужен чек</div>
            <button type="button" class="pa-btn is-primary" data-pa="go-pay">Прикрепить чек</button>`;
    }
    const rejected = invoice.stored_status === 'rejected'
        ? `<div class="pa-danger">Оплата отклонена${invoice.rejection_reason ? `: ${escapeHtml(invoice.rejection_reason)}` : ''}</div>`
        : '';
    const summary = invoice.status === 'overdue'
        ? `<div class="pa-danger">Долг за ${month.toLowerCase()}: ${invoice.amount} Br</div>`
        : `<div><span class="pa-strong">${month}: ${invoice.amount} Br.</span> Оплатить до ${escapeHtml(parentShortDate(invoice.due_date))}.</div>`;
    return `
        ${rejected}
        ${summary}
        <button type="button" class="pa-btn is-primary" data-pa="go-pay">Оплатить</button>`;
}

function parentBalanceCard(student) {
    const total = student.lessons_count || 0;
    const left = student.lessons_remaining || 0;
    const percent = total ? Math.min(100, Math.round((left / total) * 100)) : 0;
    return `
        <section class="pa-card">
            <div class="pa-row">
                <div class="pa-label">Осталось занятий</div>
                <div class="pa-big">${left} из ${total}</div>
            </div>
            <div class="pa-progress"><span style="width: ${percent}%"></span></div>
            ${parentPaymentBlock(student)}
            ${parentHistory(student)}
        </section>`;
}

function parentHistory(student) {
    const paid = (student.invoices || []).filter(item => item.stored_status === 'paid');
    if (!paid.length) return '';
    return `
        <details>
            <summary>История оплат</summary>
            <div class="pa-stack">
                ${paid.map(item => `
                    <div class="pa-row">
                        <span>${escapeHtml(parentMonth(item))} ${parentDate(item.period_start).getFullYear()}</span>
                        <span class="pa-muted">${item.amount} Br</span>
                    </div>`).join('')}
            </div>
        </details>`;
}

function parentMakeupsCard(student) {
    const available = student.makeups.filter(item => item.status === 'available');
    const requested = student.makeups.filter(item => item.status === 'requested');
    const scheduled = student.makeups.filter(item => item.status === 'scheduled');
    if (!student.makeups.length) {
        return `
            <section class="pa-card">
                <div>
                    <div class="pa-label">Отработки</div>
                    <div>Сейчас отработок нет</div>
                </div>
            </section>`;
    }
    const rejected = available.find(item => item.rejection_reason);
    return `
        <section class="pa-card">
            <div>
                <div class="pa-label">Отработки</div>
                <div class="pa-mid">Доступно: ${available.length}</div>
                ${available.length ? `<div class="pa-muted">Первая действует до ${escapeHtml(parentShortDate(available[0].expires_at))}</div>` : ''}
            </div>
            ${scheduled.map(item => `
                <div class="pa-ok">Записаны: ${escapeHtml(parentLongDate(item.scheduled_date))}${item.scheduled_time ? `, ${escapeHtml(item.scheduled_time)}` : ''}</div>
            `).join('')}
            ${requested.map(item => `
                <div class="pa-warn">Заявка на ${escapeHtml(parentShortDate(item.requested_date))} ждёт ответа тренера</div>
            `).join('')}
            ${rejected ? `<div class="pa-danger">Заявка отклонена: ${escapeHtml(rejected.rejection_reason)}</div>` : ''}
            ${available.length ? '<button type="button" class="pa-btn" data-pa="go-makeup">Записаться на отработку</button>' : ''}
        </section>`;
}

function parentScheduleCard(student) {
    const rows = [];
    student.schedules.forEach(schedule => {
        String(schedule.days || '').split(',').filter(Boolean).forEach(day => {
            const index = Number(day);
            rows.push({index, time: schedule.times[String(index)] || '', place: parentPlace(schedule)});
        });
    });
    rows.sort((left, right) => left.index - right.index);
    const request = (parentData.schedule_requests || []).find(item => item.student_id === student.id);
    return `
        <section class="pa-card">
            <div class="pa-stack">
                <div class="pa-label">Расписание</div>
                ${rows.length ? rows.map(row => `
                    <div class="pa-row">
                        <span class="pa-strong">${PARENT_WEEKDAYS[row.index]}</span>
                        <span class="pa-muted">${escapeHtml(row.time)}</span>
                    </div>`).join('') : '<div>Расписание назначит тренер</div>'}
            </div>
            ${request?.status === 'pending' ? '<div class="pa-warn">Новое расписание ждёт ответа тренера</div>' : ''}
            ${request?.status === 'rejected' ? `<div class="pa-danger">Изменение отклонено${request.rejection_reason ? `: ${escapeHtml(request.rejection_reason)}` : ''}</div>` : ''}
            <button type="button" class="pa-btn" data-pa="go-schedule">Изменить расписание</button>
            <button type="button" class="pa-switch" data-pa="toggle-reminder" aria-pressed="${student.training_reminders_enabled ? 'true' : 'false'}">
                <span>Напоминать накануне в 18:00</span>
                <span class="pa-switch-track"></span>
            </button>
        </section>`;
}

function renderParentHome(student) {
    const switcher = parentData.students.length > 1
        ? `<div class="pa-segment">
                ${parentData.students.map(item => `
                    <button type="button" data-pa="pick-student" data-id="${item.id}" aria-pressed="${item.id === student.id}">${escapeHtml(item.name.split(' ')[0])}</button>
                `).join('')}
           </div>`
        : `<div class="pa-muted">${escapeHtml(student.name)}</div>`;
    return `
        <div class="pa-head">
            <h1>Break Wave</h1>
            ${switcher}
        </div>
        <div class="pa-body">
            ${parentRegistrationCards()}
            ${parentTrainingCard(student)}
            ${parentBalanceCard(student)}
            ${parentMakeupsCard(student)}
            ${parentScheduleCard(student)}
            <button type="button" class="pa-btn" data-pa="add-child">Добавить ребёнка</button>
        </div>`;
}

function parentBar(title) {
    return `
        <div class="pa-bar">
            <button type="button" class="pa-back" data-pa="go-home" aria-label="Назад">${PARENT_BACK_ICON}</button>
            <h1>${escapeHtml(title)}</h1>
        </div>`;
}

function renderParentPay(student) {
    const invoice = parentPayableInvoice(student);
    if (!invoice) return renderParentHome(student);
    const firstName = student.name.split(' ')[0];
    const canChooseTariff = invoice.id === student.invoice.id && student.tariff_change_allowed;
    const tariffCode = parentUi.tariff || invoice.tariff_code;
    const amount = canChooseTariff ? parentData.tariffs[tariffCode].price : invoice.amount;
    const tariffBlock = canChooseTariff
        ? `
            ${Object.entries(parentData.tariffs).map(([code, tariff]) => `
                <button type="button" class="pa-choice" data-pa="pick-tariff" data-value="${escapeHtml(code)}" aria-pressed="${code === tariffCode}">
                    <span>${escapeHtml(tariff.label)}</span>
                    <span class="pa-strong">${tariff.price} Br</span>
                </button>`).join('')}
            <div class="pa-label">Изменить абонемент можно до 5 числа</div>`
        : `<div class="pa-note">${escapeHtml(invoice.tariff?.label || 'Абонемент')}: ${amount} Br</div>`;
    const online = parentUi.method === 'online';
    const details = parentData.payment_details
        ? `Переведите ${amount} Br по реквизитам:\n${parentData.payment_details}\n\nЗатем прикрепите чек.`
        : `Переведите ${amount} Br. Реквизиты уточните у тренера, затем прикрепите чек.`;
    const action = online
        ? `
            <input id="pa-receipt" class="pa-file" type="file" accept="image/*,application/pdf" data-pa-file ${parentUi.busy ? 'disabled' : ''}>
            <label for="pa-receipt" class="pa-btn ${parentUi.busy ? 'is-busy' : 'is-primary'}">${parentUi.busy ? 'Отправляем…' : 'Прикрепить чек и отправить'}</label>`
        : `<button type="button" class="pa-btn is-primary" data-pa="submit-cash" ${parentUi.busy ? 'disabled' : ''}>${parentUi.busy ? 'Отправляем…' : 'Я передам наличные'}</button>`;
    return `
        ${parentBar(`Оплата за ${parentMonth(invoice).toLowerCase()}, ${firstName}`)}
        <div class="pa-body">
            <div class="pa-group">
                <h2>Абонемент</h2>
                ${tariffBlock}
            </div>
            <div class="pa-group">
                <h2>Как оплатить</h2>
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
        <div class="pa-stack pa-center" style="gap: 12px">
            <h1>Оплата на проверке</h1>
            <p class="pa-muted">Администратор проверит её, и бот сообщит результат в Telegram.</p>
            <button type="button" class="pa-btn is-primary" data-pa="go-home">На главную</button>
        </div>`;
}

function renderParentMakeup(student) {
    const slots = student.makeup_slots || [];
    return `
        ${parentBar(`Отработка, ${student.name.split(' ')[0]}`)}
        <div class="pa-body">
            <div class="pa-group">
                <h2>Выберите тренировку</h2>
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
        ${parentBar(`Расписание, ${student.name.split(' ')[0]}`)}
        <div class="pa-body">
            <div class="pa-group">
                <h2>Какие дни и время удобны</h2>
                ${parentUi.scheduleRows.map((row, index) => `
                    <div class="pa-schedule-row">
                        <select class="pa-field" aria-label="День" data-pa-row="${index}" data-field="day">${options(row.day)}</select>
                        <input class="pa-field" type="time" aria-label="Время" value="${escapeHtml(row.time)}" data-pa-row="${index}" data-field="time">
                        <button type="button" class="pa-remove" data-pa="remove-row" data-value="${index}" aria-label="Убрать">${REMOVE_ICON}</button>
                    </div>`).join('')}
                <button type="button" class="pa-btn" data-pa="add-row">Добавить день</button>
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
    const canChoose = invoice.id === student.invoice.id && student.tariff_change_allowed;
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
        Object.assign(parentUi, {view: 'payDone', tariff: null});
    });
}

const PARENT_ACTIONS = {
    'add-child': () => renderRegistration({existingParent: parentData.parent}),
    'pick-student': target => parentGo('home', {studentId: Number(target.dataset.id), tariff: null}),
    'go-home': () => parentGo('home', {tariff: null, slot: null}),
    'go-pay': () => parentGo('pay', {tariff: null}),
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
    'toggle-reminder': () => {
        const student = parentStudent();
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
            Object.assign(parentUi, {view: 'payDone', tariff: null});
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
