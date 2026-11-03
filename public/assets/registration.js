/* === Family registration form === */

const registration = {
    parentName: '',
    parentPhone: '',
    children: [],
    current: 0,
    step: 0,
    errors: {},
    busy: false,
    canGoBack: false,
};

function newRegistrationChild() {
    return {name: '', nickname: '', coach: '', birthday: '', schedule: [{weekday: 1, time: '18:00'}]};
}

function renderRegistration({existingParent = null} = {}) {
    if (existingParent) {
        registration.parentName = existingParent.full_name;
        registration.parentPhone = existingParent.phone;
        registration.canGoBack = true;
    }
    if (!registration.children.length) registration.children = [newRegistrationChild()];
    registration.current = 0;
    registration.step = 0;
    registration.errors = {};
    paintRegistration();
}

// A parent already known to the school skips the first step.
function registrationSteps() {
    return registration.canGoBack ? ['child', 'plan'] : ['parent', 'child', 'plan'];
}

function registrationToday() {
    const now = new Date();
    return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
}

function registrationField(label, name, control) {
    const error = registration.errors[name];
    return `
        <label class="pa-stack">
            <span class="pa-label">${label}</span>
            ${control}
            ${error ? `<span class="pa-error">${escapeHtml(error)}</span>` : ''}
        </label>`;
}

function registrationParentStep() {
    return `
        <h2 class="pa-step-title">Кто заполняет анкету</h2>
        ${registrationField('ФИО', 'parentName', `<input class="pa-field" autocomplete="name" data-reg-parent="parentName" value="${escapeHtml(registration.parentName)}">`)}
        ${registrationField('Телефон', 'parentPhone', `<input class="pa-field" type="tel" autocomplete="tel" placeholder="+375 29 000-00-00" data-reg-parent="parentPhone" value="${escapeHtml(registration.parentPhone)}">`)}`;
}

function registrationChildStep() {
    const index = registration.current;
    const child = registration.children[index];
    return `
        <h2 class="pa-step-title">Расскажите о ребёнке</h2>
        ${registrationField('Имя и фамилия', 'name', `<input class="pa-field" autocomplete="off" data-reg-child="${index}" data-field="name" value="${escapeHtml(child.name)}">`)}
        ${registrationField('Никнейм', 'nickname', `<input class="pa-field" autocomplete="off" maxlength="50" data-reg-child="${index}" data-field="nickname" value="${escapeHtml(child.nickname)}">`)}
        ${registrationField('Дата рождения', 'birthday', `<input class="pa-field ${child.birthday ? '' : 'is-empty'}" type="date" max="${registrationToday()}" data-reg-child="${index}" data-field="birthday" value="${escapeHtml(child.birthday)}">`)}`;
}

function registrationPlanStep() {
    const index = registration.current;
    const child = registration.children[index];
    const weekdays = selected => PARENT_WEEKDAYS.map((label, day) =>
        `<option value="${day}" ${day === Number(selected) ? 'selected' : ''}>${label}</option>`
    ).join('');
    const coach = coachOptions.length
        ? registrationField('Тренер', 'coach', `
            <select class="pa-field" data-reg-child="${index}" data-field="coach">
                <option value="">Выберите тренера</option>
                ${coachOptions.map(option => `<option value="${escapeHtml(option.username)}" ${option.username === child.coach ? 'selected' : ''}>${escapeHtml(option.name)}</option>`).join('')}
            </select>`)
        : '';
    const added = registration.children.length > 1
        ? `
            <h2 class="pa-section">Дети в анкете</h2>
            <div class="pa-list">
                ${registration.children.map((item, childIndex) => `
                    <div class="pa-list-row">
                        <button type="button" class="pa-list-text" data-reg="edit-child" data-child="${childIndex}">
                            <span class="pa-strong">${escapeHtml(item.name || 'Без имени')}</span>
                            ${item.nickname ? `<span class="pa-muted pa-small">${escapeHtml(item.nickname)}</span>` : ''}
                        </button>
                        <button type="button" class="pa-remove" data-reg="remove-child" data-child="${childIndex}" aria-label="Убрать ${escapeHtml(item.name)}">${bwIcon('x', 20)}</button>
                    </div>`).join('')}
            </div>`
        : '';
    return `
        <h2 class="pa-step-title">${escapeHtml(child.name || 'Тренер и расписание')}</h2>
        ${coach}
        <div class="pa-group">
            <span class="pa-label">Удобные дни и время</span>
            ${child.schedule.map((row, rowIndex) => `
                <div class="pa-schedule-row">
                    <select class="pa-field" aria-label="День" data-reg-row="${rowIndex}" data-child="${index}" data-field="weekday">${weekdays(row.weekday)}</select>
                    <input class="pa-field" type="time" aria-label="Время" data-reg-row="${rowIndex}" data-child="${index}" data-field="time" value="${escapeHtml(row.time)}">
                    <button type="button" class="pa-remove" data-reg="remove-row" data-child="${index}" data-row="${rowIndex}" aria-label="Убрать">${bwIcon('x', 20)}</button>
                </div>`).join('')}
            <button type="button" class="pa-btn" data-reg="add-row" data-child="${index}">${bwIcon('plus', 20)}Добавить день</button>
            ${registration.errors.schedule ? `<span class="pa-error">${escapeHtml(registration.errors.schedule)}</span>` : ''}
        </div>
        ${added}
        <button type="button" class="pa-btn" data-reg="add-child">${bwIcon('users', 20)}Добавить ещё ребёнка</button>`;
}

function paintRegistration() {
    const steps = registrationSteps();
    const kind = steps[registration.step];
    const content = {parent: registrationParentStep, child: registrationChildStep, plan: registrationPlanStep}[kind]();
    const last = kind === 'plan';
    const hasBack = registration.step > 0 || registration.canGoBack;
    const scrollKey = `registration:${kind}:${registration.current}`;
    const moved = currentScreen?.key !== scrollKey;
    paintScreen(document.getElementById('parent-content'), `
        <div class="pa">
            <div class="pa-bar">
                ${hasBack ? `<button type="button" class="pa-back" data-reg="back" data-back aria-label="Назад">${bwIcon('chevron-left', 26)}</button>` : '<span class="pa-back"></span>'}
                <div>
                    <h1>${registration.canGoBack ? 'Новый ребёнок' : 'Анкета Break Wave'}</h1>
                    <div class="pa-muted pa-small">Шаг ${registration.step + 1} из ${steps.length}</div>
                </div>
                <span class="pa-bar-end"></span>
            </div>
            <div class="pa-steps" aria-hidden="true">
                ${steps.map((_, index) => `<span class="${index <= registration.step ? 'is-done' : ''}"></span>`).join('')}
            </div>
            <div class="pa-body">
                ${content}
            </div>
            <div class="pa-foot">
                <button type="button" class="pa-btn is-primary" data-reg="${last ? 'submit' : 'next'}" ${registration.busy ? 'disabled' : ''}>${registration.busy ? 'Отправляем…' : last ? 'Отправить анкету' : 'Продолжить'}</button>
            </div>
        </div>`, scrollKey, registration.step + (registration.canGoBack ? 1 : 0));
    if (moved) window.scrollTo(0, 0);
}

// The server checks all of this again when the form is sent.
function registrationStepErrors(kind) {
    const errors = {};
    if (kind === 'parent') {
        if (registration.parentName.trim().length < 2) errors.parentName = 'Укажите ФИО';
        if (registration.parentPhone.trim().length < 5) errors.parentPhone = 'Укажите телефон';
    }
    const child = registration.children[registration.current];
    if (kind === 'child') {
        if (child.name.trim().length < 2) errors.name = 'Укажите имя ребёнка';
        if (!child.nickname.trim()) errors.nickname = 'Укажите никнейм ребёнка';
        if (!child.birthday || child.birthday > registrationToday()) errors.birthday = 'Укажите дату рождения';
    }
    if (kind === 'plan') {
        if (coachOptions.length && !child.coach) errors.coach = 'Выберите тренера';
        if (!child.schedule.length) errors.schedule = 'Укажите хотя бы один день';
    }
    return errors;
}

function registrationStepIsValid() {
    registration.errors = registrationStepErrors(registrationSteps()[registration.step]);
    return !Object.keys(registration.errors).length;
}

const REGISTRATION_ACTIONS = {
    next: () => {
        if (registrationStepIsValid()) registration.step += 1;
    },
    back: () => {
        registration.errors = {};
        const steps = registrationSteps();
        const child = registration.children[registration.current];
        if (steps[registration.step] === 'child' && registration.current > 0) {
            // Leaving a child that was only just added and is still blank discards it.
            if (!child.name && !child.nickname && !child.birthday) registration.children.splice(registration.current, 1);
            registration.current = registration.children.length - 1;
            registration.step = steps.indexOf('plan');
        } else if (registration.step > 0) {
            registration.step -= 1;
        } else {
            registration.children = [];
            parentGo('home');
            return false;
        }
        return true;
    },
    'add-child': () => {
        if (!registrationStepIsValid()) return;
        registration.children.push(newRegistrationChild());
        registration.current = registration.children.length - 1;
        registration.step = registrationSteps().indexOf('child');
    },
    'edit-child': target => {
        registration.errors = {};
        registration.current = Number(target.dataset.child);
        registration.step = registrationSteps().indexOf('child');
    },
    'remove-child': target => {
        registration.children.splice(Number(target.dataset.child), 1);
        registration.current = registration.children.length - 1;
    },
    'add-row': target => registration.children[Number(target.dataset.child)].schedule.push({weekday: 1, time: '18:00'}),
    'remove-row': target => {
        const rows = registration.children[Number(target.dataset.child)].schedule;
        rows.splice(Number(target.dataset.row), 1);
    },
    submit: () => submitRegistration(),
};

async function submitRegistration() {
    if (!registrationStepIsValid()) {
        paintRegistration();
        return;
    }
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
        parentData = null;
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
    if (REGISTRATION_ACTIONS[target.dataset.reg]?.(target) !== false) paintRegistration();
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
