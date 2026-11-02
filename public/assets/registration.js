/* === Family registration form === */

const registration = {
    parentName: '',
    parentPhone: '',
    children: [],
    busy: false,
    canGoBack: false,
};

function newRegistrationChild() {
    return {name: '', nickname: '', birthday: '', schedule: [{weekday: 1, time: '18:00'}]};
}

function renderRegistration({existingParent = null} = {}) {
    if (existingParent) {
        registration.parentName = existingParent.full_name;
        registration.parentPhone = existingParent.phone;
        registration.canGoBack = true;
    }
    if (!registration.children.length) registration.children = [newRegistrationChild()];
    paintRegistration();
}

function paintRegistration() {
    const weekdays = selected => PARENT_WEEKDAYS.map((label, index) =>
        `<option value="${index}" ${index === Number(selected) ? 'selected' : ''}>${label}</option>`
    ).join('');
    const now = new Date();
    const today = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
    const children = registration.children.map((child, childIndex) => `
        <section class="pa-card">
            <div class="pa-row">
                <h2 class="pa-mid">Ребёнок${registration.children.length > 1 ? ` ${childIndex + 1}` : ''}</h2>
                ${registration.children.length > 1 ? `<button type="button" class="pa-link" data-reg="remove-child" data-child="${childIndex}">Убрать</button>` : ''}
            </div>
            <label class="pa-stack">
                <span class="pa-label">Имя и фамилия</span>
                <input class="pa-field" autocomplete="off" data-reg-child="${childIndex}" data-field="name" value="${escapeHtml(child.name)}">
            </label>
            <label class="pa-stack">
                <span class="pa-label">Никнейм</span>
                <input class="pa-field" autocomplete="off" maxlength="50" data-reg-child="${childIndex}" data-field="nickname" value="${escapeHtml(child.nickname)}">
            </label>
            <label class="pa-stack">
                <span class="pa-label">Дата рождения</span>
                <input class="pa-field ${child.birthday ? '' : 'is-empty'}" type="date" max="${today}" data-reg-child="${childIndex}" data-field="birthday" value="${escapeHtml(child.birthday)}">
            </label>
            <div class="pa-group">
                <span class="pa-label">Удобные дни и время</span>
                ${child.schedule.map((row, rowIndex) => `
                    <div class="pa-schedule-row">
                        <select class="pa-field" aria-label="День" data-reg-row="${rowIndex}" data-child="${childIndex}" data-field="weekday">${weekdays(row.weekday)}</select>
                        <input class="pa-field" type="time" aria-label="Время" data-reg-row="${rowIndex}" data-child="${childIndex}" data-field="time" value="${escapeHtml(row.time)}">
                        <button type="button" class="pa-remove" data-reg="remove-row" data-child="${childIndex}" data-row="${rowIndex}" aria-label="Убрать">${REMOVE_ICON}</button>
                    </div>`).join('')}
                <button type="button" class="pa-btn" data-reg="add-row" data-child="${childIndex}">Добавить день</button>
            </div>
        </section>`).join('');
    paintScreen(document.getElementById('parent-content'), `
        <div class="pa">
            ${registration.canGoBack
                ? parentBar('Новый ребёнок')
                : `<div class="pa-head">
                        <h1>Анкета Break Wave</h1>
                        <div class="pa-muted">Заполните её один раз. Тренер проверит расписание и подтвердит занятия.</div>
                   </div>`}
            <div class="pa-body">
                <section class="pa-card">
                    <h2 class="pa-mid">Родитель</h2>
                    <label class="pa-stack">
                        <span class="pa-label">ФИО</span>
                        <input class="pa-field" autocomplete="name" data-reg-parent="parentName" value="${escapeHtml(registration.parentName)}">
                    </label>
                    <label class="pa-stack">
                        <span class="pa-label">Телефон</span>
                        <input class="pa-field" type="tel" autocomplete="tel" placeholder="+375 29 000-00-00" data-reg-parent="parentPhone" value="${escapeHtml(registration.parentPhone)}">
                    </label>
                </section>
                ${children}
                <button type="button" class="pa-btn" data-reg="add-child">Добавить ещё ребёнка</button>
            </div>
            <div class="pa-foot">
                <button type="button" class="pa-btn is-primary" data-reg="submit" ${registration.busy ? 'disabled' : ''}>${registration.busy ? 'Отправляем…' : 'Отправить анкету'}</button>
            </div>
        </div>`, 'registration', registration.canGoBack ? 1 : 0);
}

const REGISTRATION_ACTIONS = {
    'add-child': () => registration.children.push(newRegistrationChild()),
    'remove-child': target => registration.children.splice(Number(target.dataset.child), 1),
    'add-row': target => registration.children[Number(target.dataset.child)].schedule.push({weekday: 1, time: '18:00'}),
    'remove-row': target => {
        const rows = registration.children[Number(target.dataset.child)].schedule;
        rows.splice(Number(target.dataset.row), 1);
    },
    submit: () => submitRegistration(),
};

async function submitRegistration() {
    registration.busy = true;
    paintRegistration();
    try {
        await apiPost('/api/parent/register', {
            parent: {full_name: registration.parentName, phone: registration.parentPhone},
            children: registration.children,
        });
        registration.children = [];
        registration.canGoBack = false;
        registration.busy = false;
        showNotification('Анкета отправлена тренеру');
        parentGo('home');
        await loadParentContext();
    } catch (error) {
        registration.busy = false;
        paintRegistration();
        showNotification(error.message, 'error');
    }
}

document.addEventListener('click', event => {
    const target = event.target.closest('[data-reg]');
    if (!target || target.disabled) return;
    if (target.dataset.reg === 'submit') {
        runControl(target, REGISTRATION_ACTIONS.submit);
        return;
    }
    REGISTRATION_ACTIONS[target.dataset.reg]?.(target);
    paintRegistration();
});

document.addEventListener('input', event => {
    const target = event.target;
    if (target.dataset.regParent) {
        registration[target.dataset.regParent] = target.value;
    } else if (target.dataset.regChild !== undefined) {
        registration.children[Number(target.dataset.regChild)][target.dataset.field] = target.value;
        if (target.type === 'date') target.classList.toggle('is-empty', !target.value);
    } else if (target.dataset.regRow !== undefined) {
        const row = registration.children[Number(target.dataset.child)].schedule[Number(target.dataset.regRow)];
        row[target.dataset.field] = target.dataset.field === 'weekday' ? Number(target.value) : target.value;
    }
});
