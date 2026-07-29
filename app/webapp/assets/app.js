/* === CRM Break Wave Mini App === */

const API = window.location.origin;
const tg = window.Telegram?.WebApp;

let currentScreen = 'loading';
let screenHistory = [];
let initData = '';
let currentCoach = null;
let currentRole = null;
let currentParent = null;
let parentData = null;
let guestInvitation = null;
let registrationScheduleRows = [];
let coaches = [];
let students = [];
let payments = [];
let calendarData = {};
let currentCalendarDate = new Date();
let selectedCalendarDay = null;
let editingStudentId = null;
let editingPaymentId = null;
let currentStudentDetailId = null;
let currentStudentDetailName = '';
let currentPaymentsFilter = 'all';
let accessibilityControlId = 0;
const ROOT_SCREENS = new Set(['dashboard', 'students', 'calendar', 'quick-lesson', 'finance']);

function callTelegram(method, ...args) {
    try {
        const result = tg?.[method]?.(...args);
        result?.catch?.(() => {});
    } catch (error) {
        console.debug(`Telegram WebApp method ${method} is unavailable`, error);
    }
}

function telegramVersionAtLeast(version) {
    try {
        return typeof tg?.isVersionAtLeast !== 'function' || tg.isVersionAtLeast(version);
    } catch {
        return false;
    }
}

// === Init ===
document.addEventListener('DOMContentLoaded', async () => {
    enhanceAccessibility(document);
    initializeDialogAccessibility();

    if (tg) {
        callTelegram('ready');
        callTelegram('expand');
        if (telegramVersionAtLeast('8.0')) {
            callTelegram('requestFullscreen');
        }
        if (telegramVersionAtLeast('6.2')) {
            callTelegram('enableClosingConfirmation');
        }
        if (telegramVersionAtLeast('6.1')) {
            callTelegram('setBackgroundColor', '#07111F');
            callTelegram('setHeaderColor', '#0B1829');
        }
        initData = tg.initData || '';
        
        // Store current user info from Telegram
        if (tg.initDataUnsafe?.user) {
            const user = tg.initDataUnsafe.user;
            localStorage.setItem('crm_current_user', JSON.stringify({
                id: user.id,
                first_name: user.first_name,
                last_name: user.last_name,
                username: user.username
            }));
        }
    }
    
    // Initialize date inputs with today
    const today = new Date().toISOString().split('T')[0];
    document.getElementById('ql-date')?.setAttribute('value', today);
    
    // Setup forms
    setupForms();
    
    // Authenticate
    await authenticate();
});

function enhanceAccessibility(root = document) {
    root.querySelectorAll?.('button:not([type])').forEach((button) => {
        button.setAttribute('type', 'button');
    });

    root.querySelectorAll?.('.back-btn').forEach((button) => {
        if (!button.getAttribute('aria-label')) button.setAttribute('aria-label', 'Назад');
    });

    root.querySelectorAll?.('.close-btn').forEach((button) => {
        if (!button.getAttribute('aria-label')) button.setAttribute('aria-label', 'Закрыть');
    });

    root.querySelectorAll?.('.btn-icon').forEach((button) => {
        if (!button.getAttribute('aria-label') && button.textContent.trim() === '×') {
            button.setAttribute('aria-label', 'Удалить');
        }
    });

    root.querySelectorAll?.('.form-group').forEach((group) => {
        const label = group.querySelector(':scope > label');
        const control = group.querySelector('input, select, textarea');
        if (!label || !control || label.contains(control)) return;
        if (!control.id) {
            accessibilityControlId += 1;
            control.id = `accessible-control-${accessibilityControlId}`;
        }
        if (!label.htmlFor) label.htmlFor = control.id;
    });

    const previousMonth = root.querySelector?.('.calendar-nav button[onclick="changeMonth(-1)"]');
    const nextMonth = root.querySelector?.('.calendar-nav button[onclick="changeMonth(1)"]');
    previousMonth?.setAttribute('aria-label', 'Предыдущий месяц');
    nextMonth?.setAttribute('aria-label', 'Следующий месяц');
}

function initializeDialogAccessibility() {
    let activeDialog = null;
    let focusBeforeDialog = null;

    const syncDialogs = () => {
        const modal = document.querySelector('.modal');
        const appRoot = document.getElementById('app');
        document.body.classList.toggle('modal-open', Boolean(modal));
        if (appRoot) appRoot.inert = Boolean(modal);
        if (!modal) {
            if (activeDialog) {
                const restoreTarget = focusBeforeDialog;
                activeDialog = null;
                focusBeforeDialog = null;
                requestAnimationFrame(() => restoreTarget?.focus?.());
            }
            return;
        }

        modal.setAttribute('role', 'dialog');
        modal.setAttribute('aria-modal', 'true');
        modal.setAttribute('tabindex', '-1');
        const heading = modal.querySelector('h2, h3');
        if (heading) {
            if (!heading.id) heading.id = `dialog-title-${Date.now()}`;
            modal.setAttribute('aria-labelledby', heading.id);
        }
        enhanceAccessibility(modal);
        if (modal !== activeDialog) {
            focusBeforeDialog = document.activeElement;
            activeDialog = modal;
            requestAnimationFrame(() => {
                const firstControl = modal.querySelector(
                    'input:not([type="hidden"]), select, textarea, button'
                );
                (firstControl || modal).focus();
            });
        }
    };

    const observer = new MutationObserver(() => {
        enhanceAccessibility(document);
        syncDialogs();
    });
    observer.observe(document.body, {childList: true, subtree: true});
    syncDialogs();

    document.addEventListener('keydown', (event) => {
        const modal = document.querySelector('.modal');
        if (!modal) return;

        if (event.key === 'Escape') {
            const closeButton = modal.querySelector(
                '[data-close-modal], .close-btn, button[onclick*=".remove()"]'
            );
            closeButton?.click();
            return;
        }

        if (event.key !== 'Tab') return;
        const focusable = [...modal.querySelectorAll(
            'button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'
        )].filter((element) => element.getClientRects().length > 0);
        if (focusable.length === 0) return;

        const first = focusable[0];
        const last = focusable[focusable.length - 1];
        if (event.shiftKey && document.activeElement === first) {
            event.preventDefault();
            last.focus();
        } else if (!event.shiftKey && document.activeElement === last) {
            event.preventDefault();
            first.focus();
        }
    });
}

function setupForms() {
    document.getElementById('parent-registration-form')?.addEventListener('submit', async (e) => {
        e.preventDefault();
        await submitParentRegistration();
    });

    // Student form
    document.getElementById('student-form')?.addEventListener('submit', async (e) => {
        e.preventDefault();
        await saveStudent();
    });
    
    // Payment form
    document.getElementById('payment-form')?.addEventListener('submit', async (e) => {
        e.preventDefault();
        await savePayment();
    });
}

// === Auth ===

async function authenticate() {
    try {
        if (!initData) {
            showApplicationError('Откройте CRM из меню Telegram-бота Break Wave.');
            return;
        }

        const urlInvitation = new URLSearchParams(window.location.search).get('invite');
        const startParam = urlInvitation || tg?.initDataUnsafe?.start_param || '';
        const res = await fetch(`${API}/api/auth`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData, startParam})
        });
        
        if (!res.ok) {
            if (res.status === 401 || res.status === 403) {
                showApplicationError('Сессия Telegram устарела. Закройте CRM и откройте её снова из меню бота.');
                return;
            }
            throw new Error(`Authentication failed with ${res.status}`);
        }
        const data = await res.json();

        currentRole = data.role;
        const subtitle = document.querySelector('.app-subtitle');
        const context = document.querySelector('.brand-context');

        if (data.role === 'guest') {
            guestInvitation = data;
            if (subtitle) subtitle.textContent = 'Регистрация';
            if (context) context.textContent = 'Семья';
            document.getElementById('reg-child-name').value = data.preliminary_child_name || '';
            document.getElementById('reg-parent-name').value = data.existing_parent?.full_name || '';
            document.getElementById('reg-parent-phone').value = data.existing_parent?.phone || '';
            initializeRegistrationSchedule();
            showScreen('registration');
            return;
        }

        if (data.role === 'parent') {
            currentParent = data;
            if (subtitle) subtitle.textContent = 'Семейный кабинет';
            if (context) context.textContent = 'Семья';
            showScreen('parent');
            return;
        }

        currentCoach = data;
        if (subtitle) subtitle.textContent = data.is_admin ? 'CRM руководителя' : 'CRM тренера';
        if (context) context.textContent = data.is_admin ? 'Админ' : 'CRM';
        
        // Store coach info for auto-fill forms
        localStorage.setItem('crm_coach_info', JSON.stringify({
            id: data.coach_id,
            first_name: data.first_name,
            username: data.username,
            is_admin: data.is_admin
        }));
        
        const requestedScreen = new URLSearchParams(window.location.search).get('screen');
        const allowedEntryScreens = new Set(['dashboard', 'students', 'calendar', 'quick-lesson', 'finance', 'requests']);
        showScreen(allowedEntryScreens.has(requestedScreen) ? requestedScreen : 'dashboard');
    } catch (e) {
        console.error('Auth error:', e);
        showApplicationError('Не удалось связаться с сервером. Проверьте интернет и повторите попытку.');
    }
}

function showApplicationError(message) {
    const messageElement = document.getElementById('error-message');
    if (messageElement) messageElement.textContent = message;
    showScreen('error');
}

async function retryApplication() {
    showScreen('loading');
    await authenticate();
}

// === Navigation ===

function navigate(screen) {
    if (currentScreen !== screen && currentScreen !== 'loading') {
        screenHistory.push(currentScreen);
    }
    showScreen(screen);
}

function navigateRoot(screen) {
    screenHistory = [];
    showScreen(screen);
}

function goBack() {
    if (screenHistory.length > 0) {
        const prev = screenHistory.pop();
        showScreen(prev);
    } else {
        showScreen(currentRole === 'parent' ? 'parent' : 'dashboard');
    }
}

function showScreen(screen) {
    currentScreen = screen;
    
    document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
    const el = document.getElementById(`screen-${screen}`);
    if (el) {
        el.classList.add('active');
    }
    
    // The document is the real scroll container in the current layout.
    window.scrollTo(0, 0);
    const content = document.getElementById('content');
    if (content) content.scrollTop = 0;

    const bottomNav = document.getElementById('bottom-nav');
    const navigationVisible = ['admin', 'coach'].includes(currentRole) && ROOT_SCREENS.has(screen);
    if (bottomNav) {
        bottomNav.hidden = !navigationVisible;
        bottomNav.querySelectorAll('button').forEach((button) => {
            const isActive = button.dataset.screen === screen;
            button.classList.toggle('active', isActive);
            if (isActive) button.setAttribute('aria-current', 'page');
            else button.removeAttribute('aria-current');
        });
    }
    
    // Load data
    switch (screen) {
        case 'dashboard':
            loadDashboard();
            break;
        case 'students':
            loadStudents();
            break;
        case 'calendar':
            loadCalendar();
            break;
        case 'payments':
            loadPayments();
            break;
        case 'quick-lesson':
            loadQuickLesson();
            break;
        case 'finance':
            loadFinance();
            break;
        case 'parent':
            loadParentContext();
            break;
        case 'requests':
            loadAdminRequests();
            break;
    }
}

function renderScreenState(container, message, options = {}) {
    if (!container) return;
    const {retry, icon = '⚠️'} = options;
    container.innerHTML = `
        <div class="screen-state" role="status">
            <div class="screen-state-icon" aria-hidden="true">${icon}</div>
            <p>${escapeHtml(message)}</p>
            ${retry ? `<button type="button" class="btn-secondary" onclick="${retry}">Повторить</button>` : ''}
        </div>
    `;
}

// === Coaches ===

async function loadCoaches() {
    try {
        const res = await fetch(`${API}/api/coaches`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData})
        });
        coaches = await res.json();
        
        // Update filter button labels with coach names
        updateCoachFilterLabels();
        
        return coaches;
    } catch (e) {
        console.error('Coaches load error:', e);
        return [];
    }
}

function updateCoachFilterLabels() {
    const operationalFilter = document.querySelector('#coach-filter-tabs [data-filter="all"]');
    if (operationalFilter) operationalFilter.textContent = 'Мои ученики';
}

function renderCoachSelect() {
    const select = document.getElementById('st-coach');
    const display = document.getElementById('coach-display');
    
    if (!select || !display) return;
    
    // Get coach info from auth data
    const coachInfoData = localStorage.getItem('crm_coach_info');
    const coachInfo = coachInfoData ? JSON.parse(coachInfoData) : null;
    
    // Get current coach from server data
    const currentCoach = coaches.find(c => c.is_current);
    
    if (!coachInfo && !currentCoach) {
        select.innerHTML = '<option value="">Нет тренеров</option>';
        return;
    }
    
    // Use coach info from auth (most reliable)
    const coachName = coachInfo?.first_name || currentCoach?.first_name || 'Тренер';
    const coachUsername = coachInfo?.username || currentCoach?.username;
    const coachId = coachInfo?.id || currentCoach?.id || coaches[0]?.id;
    
    // Show current coach info (auto-filled from Telegram)
    select.style.display = 'none';
    display.style.display = 'block';
    display.innerHTML = `
        <span class="coach-name">${escapeHtml(coachName)}</span>
        ${coachUsername ? `<span class="coach-username">@${escapeHtml(coachUsername)}</span>` : ''}
    `;
    select.value = coachId;
    
    // Operational screens intentionally stay in the signed-in coach context.
}

// === Dashboard ===

async function loadDashboard() {
    try {
        const res = await fetch(`${API}/api/dashboard`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData})
        });
        
        const data = await res.json();
        
        renderDashboard(data);
    } catch (e) {
        console.error('Dashboard load error:', e);
        ['stat-students', 'stat-lessons', 'stat-attendance', 'stat-revenue'].forEach((id) => {
            const value = document.getElementById(id);
            if (value) value.textContent = '—';
        });
        const alertsSection = document.getElementById('alerts-section');
        if (alertsSection) alertsSection.style.display = 'block';
        renderScreenState(
            document.getElementById('alerts-list'),
            'Не удалось загрузить сводку.',
            {retry: 'loadDashboard()'}
        );
    }
}

async function renderDashboard(data) {
    const greeting = document.getElementById('dashboard-greeting');
    const coachName = currentCoach?.first_name?.trim();
    if (greeting) {
        greeting.textContent = coachName ? `Добрый день, ${coachName}` : 'Рабочий обзор';
    }

    // Update stats
    document.getElementById('stat-students').textContent = data.students_count;
    document.getElementById('stat-lessons').textContent = data.lessons_this_month;
    document.getElementById('stat-attendance').textContent = data.attendance_rate + '%';
    document.getElementById('stat-revenue').textContent = data.monthly_revenue.toLocaleString() + ' Br';
    
    // Current date
    const dateOptions = { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' };
    document.getElementById('current-date').textContent = new Date().toLocaleDateString('ru-RU', dateOptions);
    
    // Load daily summary for detailed alerts
    const summary = await loadDailySummary();
    renderTodayFocus(summary);
    
    // Alerts
    const alertsContainer = document.getElementById('alerts-list');
    const alertsSection = document.getElementById('alerts-section');
    
    let alerts = [];
    
    // Subscription alerts
    if (data.overdue_count > 0) {
        alerts.push({
            icon: '❌',
            title: `Просроченных абонементов: ${data.overdue_count}`,
            subtitle: 'Требуется продление',
            type: 'danger'
        });
    }
    
    if (data.ending_soon_count > 0) {
        alerts.push({
            icon: '⏳',
            title: `Заканчивается скоро: ${data.ending_soon_count}`,
            subtitle: 'Осталось менее 3 дней',
            type: 'warning'
        });
    }
    
    // Lessons remaining alerts from summary
    if (summary && summary.alerts) {
        if (summary.alerts.depleted && summary.alerts.depleted.length > 0) {
            alerts.push({
                icon: '🚫',
                title: `Закончились занятия: ${summary.alerts.depleted.length}`,
                subtitle: 'Требуется оплата',
                type: 'danger'
            });
        }
        
        if (summary.alerts.low_lessons && summary.alerts.low_lessons.length > 0) {
            alerts.push({
                icon: '⚠️',
                title: `Мало занятий: ${summary.alerts.low_lessons.length}`,
                subtitle: 'Осталось 1-2 занятия',
                type: 'warning'
            });
        }
    }
    
    if (alerts.length === 0) {
        alertsSection.style.display = 'none';
    } else {
        alertsSection.style.display = 'block';
        alertsContainer.innerHTML = alerts.map(a => `
            <button type="button" class="alert-item" onclick="navigate('students')">
                <div class="alert-icon">${a.icon}</div>
                <div class="alert-content">
                    <div class="alert-title">${a.title}</div>
                    <div class="alert-subtitle">${a.subtitle}</div>
                </div>
            </button>
        `).join('');
    }
}

function renderTodayFocus(summary) {
    const container = document.getElementById('today-focus-list');
    if (!container) return;

    const schedule = summary?.today_schedule || {};
    const timeSlots = Object.entries(schedule).sort(([left], [right]) => left.localeCompare(right));

    if (timeSlots.length === 0) {
        renderScreenState(container, 'На сегодня занятий по расписанию нет.', {icon: '✓'});
        return;
    }

    container.innerHTML = timeSlots.slice(0, 4).map(([time, group]) => {
        const studentsAtTime = Array.isArray(group) ? group : [];
        const names = studentsAtTime.slice(0, 2).map((student) => student.name).join(', ');
        const extraCount = Math.max(studentsAtTime.length - 2, 0);
        const locations = [...new Set(studentsAtTime.map((student) => student.location).filter(Boolean))];
        const meta = `${names}${extraCount ? ` +${extraCount}` : ''}${locations.length ? ` · ${locations.join(', ')}` : ''}`;

        return `
            <button type="button" class="today-focus-card" onclick="openQuickLesson()">
                <span class="today-focus-time">${escapeHtml(time)}</span>
                <span class="today-focus-copy">
                    <span class="today-focus-title">${escapeHtml(meta || 'Групповое занятие')}</span>
                    <span class="today-focus-meta">Открыть быструю отметку</span>
                </span>
                <span class="today-focus-count">${studentsAtTime.length}</span>
            </button>
        `;
    }).join('');
}

// === Students ===

async function loadStudents() {
    // Load coaches first (for displaying coach info)
    await loadCoaches();
    
    const requestBody = {
        initData,
        coach_id: currentCoach?.coach_id || null,
    };
    
    try {
        const res = await fetch(`${API}/api/students`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(requestBody)
        });
        
        students = await res.json();
        
        renderStudentsList(students);
    } catch (e) {
        console.error('Students load error:', e);
        renderScreenState(
            document.getElementById('students-list'),
            'Не удалось загрузить учеников.',
            {retry: 'loadStudents()'}
        );
    }
}

function renderStudentsList(list) {
    const container = document.getElementById('students-list');
    
    if (list.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">👥</div>
                <p>Учеников пока нет</p>
            </div>
        `;
        return;
    }
    
    container.innerHTML = list.map(s => {
        const days = s.lesson_days ? s.lesson_days.split(',').map(d => {
            const daysMap = {0:'Пн',1:'Вт',2:'Ср',3:'Чт',4:'Пт',5:'Сб',6:'Вс'};
            return daysMap[d];
        }).join(', ') : '—';
        
        // Check subscription status
        let statusBadge = '';
        if (s.subscription_end) {
            const end = new Date(s.subscription_end);
            const today = new Date();
            const daysLeft = Math.ceil((end - today) / (1000 * 60 * 60 * 24));
            
            if (daysLeft < 0) {
                statusBadge = '<span class="list-item-badge danger">Просрочен</span>';
            } else if (daysLeft <= 3) {
                statusBadge = '<span class="list-item-badge warning">' + daysLeft + ' дн.</span>';
            }
        }
        
        // Check lessons remaining
        let lessonsBadge = '';
        const remaining = getStudentRemainingLessons(s);
        if (!s.is_unlimited && remaining <= 0) {
            lessonsBadge = '<span class="list-item-badge danger">Нет занятий</span>';
        } else if (!s.is_unlimited && remaining <= 2) {
            lessonsBadge = `<span class="list-item-badge warning">${formatLessonCount(remaining)}</span>`;
        }
        
        // Show locations info
        let locationsInfo = '';
        if (s.schedules && s.schedules.length > 1) {
            const locationCount = s.schedules.length;
            const primaryLoc = s.schedules.find(sch => sch.is_primary);
            locationsInfo = `<span>📍 ${escapeHtml(primaryLoc?.location_name || 'Зал')} +${locationCount - 1}</span>`;
        } else {
            locationsInfo = `<span>📍 ${escapeHtml(s.location || 'Зал Break Wave')}</span>`;
        }
        
        // Lessons indicator
        const indicatorClass = s.is_unlimited ? '' : (remaining <= 0 ? 'none' : remaining <= 2 ? 'low' : '');
        const lessonsIndicator = `<span class="lessons-indicator ${indicatorClass}">${getStudentLessonsDisplay(s)}</span>`;
        
        return `
            <button type="button" class="list-item" onclick="openStudentDetail(${s.id})">
                <div class="list-item-header">
                    <span class="list-item-title">${escapeHtml(s.name)} ${lessonsIndicator}</span>
                    <div class="list-item-badges">
                        ${lessonsBadge || statusBadge}
                    </div>
                </div>
                <div class="list-item-subtitle">${escapeHtml(s.nickname || '')}</div>
                <div class="list-item-meta">
                    ${locationsInfo}
                    <span>🕐 ${days}</span>
                </div>
            </button>
        `;
    }).join('');
}

function filterStudents(query) {
    const normalizedQuery = query.trim().toLocaleLowerCase('ru-RU');
    const digitsQuery = query.replace(/\D/g, '');
    const filtered = students.filter((student) => {
        const searchableText = [
            student.name,
            student.nickname,
            student.phone,
            student.parent_phone,
        ].filter(Boolean).join(' ').toLocaleLowerCase('ru-RU');
        const searchablePhone = [student.phone, student.parent_phone]
            .filter(Boolean)
            .join(' ')
            .replace(/\D/g, '');

        return !normalizedQuery
            || searchableText.includes(normalizedQuery)
            || (digitsQuery.length >= 2 && searchablePhone.includes(digitsQuery));
    });
    renderStudentsList(filtered);
}

async function openStudentDetail(id, options = {}) {
    const { navigateToScreen = true } = options;
    currentStudentDetailId = id;
    try {
        const res = await fetch(`${API}/api/students/${id}`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData})
        });
        
        const student = await res.json();
        if (student.error) {
            showNotification('Ученик не найден', 'error');
            return;
        }
        currentStudentDetailName = student.name || '';
        
        const days = student.lesson_days ? student.lesson_days.split(',').map(d => {
            const daysMap = {0:'Пн',1:'Вт',2:'Ср',3:'Чт',4:'Пт',5:'Сб',6:'Вс'};
            return daysMap[d];
        }).join(', ') : '—';
        
        // Subscription status
        let subStatus = 'Нет абонемента';
        let subAlert = '';
        if (student.subscription_end) {
            const end = new Date(student.subscription_end);
            const today = new Date();
            const daysLeft = Math.ceil((end - today) / (1000 * 60 * 60 * 24));
            
            if (daysLeft < 0) {
                subStatus = `❌ Просрочен (${formatDate(student.subscription_end)})`;
                subAlert = '<div class="alert-danger">Абонемент просрочен! Требуется оплата.</div>';
            } else if (daysLeft <= 3) {
                subStatus = `⏳ До ${formatDate(student.subscription_end)} (${daysLeft} дн.)`;
                subAlert = `<div class="alert-warning">Абонемент заканчивается через ${daysLeft} дн.</div>`;
            } else {
                subStatus = `✅ До ${formatDate(student.subscription_end)} (${daysLeft} дн.)`;
            }
        }
        
        // Lessons remaining
        const remaining = getStudentRemainingLessons(student);
        const total = student.lessons_count || 0;
        const used = Math.max(0, total - (Number.isFinite(remaining) ? remaining : 0));
        let lessonsAlert = '';
        
        if (!student.is_unlimited && remaining <= 0) {
            lessonsAlert = '<div class="alert-danger">Занятия закончились! Требуется оплата.</div>';
        } else if (!student.is_unlimited && remaining <= 2) {
            lessonsAlert = `<div class="alert-warning">Осталось ${formatLessonCount(remaining)}. Пора оплачивать!</div>`;
        }
        
        // Attendance history summary
        let attendanceSummary = '';
        if (student.attendance && student.attendance.length > 0) {
            const present = student.attendance.filter(a => a.status === 'present').length;
            const rate = Math.round((present / student.attendance.length) * 100);
            attendanceSummary = `
                <div class="attendance-summary">
                    <div class="attendance-stat">
                        <span class="stat-number">${student.attendance.length}</span>
                        <span class="stat-label">Всего</span>
                    </div>
                    <div class="attendance-stat">
                        <span class="stat-number success">${present}</span>
                        <span class="stat-label">Посещено</span>
                    </div>
                    <div class="attendance-stat">
                        <span class="stat-number">${rate}%</span>
                        <span class="stat-label">Посещаемость</span>
                    </div>
                </div>
            `;
        }
        
        const attendanceStats = student.attendance_summary || null;
        if (attendanceStats) {
            const attendanceCards = [
                {
                    label: 'Посетил',
                    value: attendanceStats.scheduled_present || 0,
                    hint: 'Регулярные занятия',
                    numberClass: 'success'
                },
                {
                    label: 'Болел',
                    value: attendanceStats.scheduled_sick || 0,
                    hint: 'По болезни',
                    numberClass: ''
                },
                {
                    label: 'Пропустил',
                    value: attendanceStats.scheduled_absent || 0,
                    hint: 'Без посещения',
                    numberClass: ''
                },
                {
                    label: 'Отработал',
                    value: attendanceStats.extra_present || 0,
                    hint: 'Внеплановые занятия',
                    numberClass: 'success'
                },
                {
                    label: 'К отработке',
                    value: attendanceStats.makeup_needed || 0,
                    hint: 'Осталось закрыть',
                    numberClass: attendanceStats.makeup_needed > 0 ? 'text-warning' : 'success'
                },
                {
                    label: 'Посещаемость',
                    value: `${attendanceStats.attendance_rate || 0}%`,
                    hint: 'По регулярным занятиям',
                    numberClass: ''
                }
            ];

            attendanceSummary = `
                <div class="attendance-summary">
                    <div class="attendance-summary-header">
                        <div>
                            <div class="attendance-summary-title">Кратко по занятиям</div>
                            <div class="attendance-summary-subtitle">Отработать = Болел + Пропустил - Отработал</div>
                        </div>
                    </div>
                    <div class="attendance-summary-grid">
                        ${attendanceCards.map((card) => `
                            <div class="attendance-stat">
                                <span class="stat-label">${card.label}</span>
                                <span class="stat-number ${card.numberClass}">${card.value}</span>
                                <span class="stat-hint">${card.hint}</span>
                            </div>
                        `).join('')}
                    </div>
                </div>
            `;
        }

        const content = document.getElementById('student-detail-content');
        content.innerHTML = `
            <div class="student-header">
                <div class="student-avatar">${escapeHtml(student.name.charAt(0))}</div>
                <div class="student-name">${escapeHtml(student.name)}</div>
                ${student.nickname ? `<div class="student-nickname">${escapeHtml(student.nickname)}</div>` : ''}
                <span class="student-status ${student.is_active ? 'active' : 'inactive'}">
                    ${student.is_active ? 'Активен' : 'Неактивен'}
                </span>
            </div>
            
            ${lessonsAlert}
            ${subAlert}
            
            <div class="info-section">
                <h3>Контакты</h3>
                <div class="info-row">
                    <span class="info-label">Телефон</span>
                    <span class="info-value">${escapeHtml(student.phone || '—')}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Тел. родителя</span>
                    <span class="info-value">${escapeHtml(student.parent_phone || '—')}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Возраст</span>
                    <span class="info-value">${student.age ? student.age + ' лет' : '—'}</span>
                </div>
            </div>
            
            <div class="info-section">
                <h3>Залы и расписание</h3>
                ${renderStudentDetailLocations(student)}
            </div>
            
            <div class="info-section">
                <h3>Абонемент</h3>
                ${student.is_unlimited ? `
                <div style="background: var(--bg-secondary); border-radius: 8px; padding: 12px; margin-bottom: 12px; border-left: 3px solid var(--accent);">
                    <div style="font-size: 13px; color: var(--text-secondary); margin-bottom: 4px;">Тип абонемента</div>
                    <div style="font-weight: 600; color: var(--accent);">♾️ Безлимитный (по месяцам)</div>
                    <div style="font-size: 12px; color: var(--text-muted); margin-top: 4px;">
                        Занятия не считаются. Оплата по окончанию срока.
                    </div>
                </div>
                ` : (total > 0 ? `
                <div class="lessons-progress">
                    <div class="progress-bar">
                        <div class="progress-fill ${remaining <= 2 ? 'low' : remaining <= 0 ? 'empty' : ''}" 
                             style="width: ${total > 0 ? (used / total) * 100 : 0}%"></div>
                    </div>
                    <div class="progress-text">
                        <span>Использовано: <b>${used}</b></span>
                        <span class="${remaining <= 2 ? 'text-warning' : ''}">Осталось: <b>${remaining}</b></span>
                    </div>
                </div>
                <div style="font-size: 12px; color: var(--text-muted); margin-top: 8px; padding: 8px; background: var(--bg-secondary); border-radius: 8px;">
                    💡 При отметке "Присутствовал" — занятие списывается автоматически
                </div>
                ` : `
                <div style="font-size: 14px; color: var(--text-muted); padding: 12px; background: var(--bg-secondary); border-radius: 8px;">
                    Абонемент не оформлен. Нажмите "💰 Оплата" чтобы добавить.
                </div>
                `)}
                <div class="info-row" style="margin-top: 12px;">
                    <span class="info-label">Статус</span>
                    <span class="info-value">${subStatus}</span>
                </div>
                ${student.subscription_start ? `
                <div class="info-row">
                    <span class="info-label">Начало</span>
                    <span class="info-value">${formatDate(student.subscription_start)}</span>
                </div>
                ` : ''}
            </div>
            
            ${student.payments && student.payments.length > 0 ? `
            <div class="info-section">
                <h3>История оплат</h3>
                ${student.payments.map(p => {
                    const statusText = {paid: 'Оплачено', pending: 'Ожидает', overdue: 'Просрочено'}[p.status];
                    const lessonsText = p.is_unlimited ? '♾️ Безлимит' : formatLessonCount(p.lessons_count || 0);
                    return `
                    <div class="list-item" style="margin-bottom: 8px;">
                        <div class="list-item-header">
                            <span class="list-item-title">${p.amount.toLocaleString()} Br</span>
                            <span class="payment-status ${p.status}">${statusText}</span>
                        </div>
                        <div class="list-item-subtitle">${lessonsText}${p.period_start && p.period_end ? ' • ' + formatDate(p.period_start) + ' — ' + formatDate(p.period_end) : ''}</div>
                        <div style="display: flex; gap: 8px; margin-top: 8px;">
                            <button class="btn-secondary" style="flex: 1; padding: 6px; font-size: 13px;" onclick="openEditPayment(${p.id})">✏️ Редактировать оплату</button>
                            <button class="btn-danger" style="flex: 1; padding: 6px; font-size: 13px;" onclick="deletePayment(${p.id})">🗑 Удалить</button>
                        </div>
                    </div>
                    `;
                }).join('')}
            </div>
            ` : ''}
            
            ${attendanceSummary ? `
            <div class="info-section">
                <h3>Посещаемость</h3>
                ${attendanceSummary}
            </div>
            ` : ''}
            
            ${student.notes ? `
            <div class="info-section">
                <h3>Заметки</h3>
                <p style="color: var(--text-secondary); font-size: 14px;">${escapeHtml(student.notes)}</p>
            </div>
            ` : ''}
            
            <div class="action-buttons-grid">
                <button class="btn-primary" onclick="openEditStudent(${student.id})">Редактировать</button>
                <button class="btn-secondary" onclick="markExtraAttendance(${student.id})">Внеплановое занятие</button>
                <button class="btn-secondary" onclick="viewAttendanceHistory(${student.id})">История посещений</button>
            </div>
            <section class="danger-zone">
                <h3>Опасная зона</h3>
                <p>Деактивацию можно отменить без потери истории. Полное удаление необратимо.</p>
                <div class="action-buttons-grid">
                    <button class="btn-secondary btn-danger" onclick="deactivateStudent(${student.id})">Деактивировать</button>
                    <button class="btn-danger" onclick="destroyStudent(${student.id})">Удалить навсегда</button>
                </div>
            </section>
        `;
        
        if (navigateToScreen) {
            navigate('student-detail');
        }
    } catch (e) {
        console.error('Student detail error:', e);
        showNotification('Ошибка загрузки', 'error');
    }
}

async function refreshVisibleData(studentId = null) {
    const refreshTasks = [loadDashboard()];

    if (currentScreen === 'payments') {
        refreshTasks.push(loadPayments(currentPaymentsFilter));
    }

    if (currentScreen === 'students') {
        refreshTasks.push(loadStudents());
    }

    if (currentScreen === 'quick-lesson') {
        refreshTasks.push(loadQuickLesson());
    }

    if (studentId && currentScreen === 'student-detail' && currentStudentDetailId === studentId) {
        refreshTasks.push(openStudentDetail(studentId, { navigateToScreen: false }));
    }

    await Promise.all(refreshTasks);
}

// === Calendar ===

async function loadCalendar() {
    const year = currentCalendarDate.getFullYear();
    const month = currentCalendarDate.getMonth() + 1;
    
    // Update header
    const monthNames = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь', 
                        'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'];
    document.getElementById('calendar-month').textContent = `${monthNames[month-1]} ${year}`;
    
    try {
        const res = await fetch(`${API}/api/calendar`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData, year, month})
        });
        
        calendarData = await res.json();
        renderCalendar(year, month, calendarData.days);
    } catch (e) {
        console.error('Calendar load error:', e);
        renderScreenState(
            document.getElementById('calendar-day-details'),
            'Не удалось загрузить календарь.',
            {retry: 'loadCalendar()'}
        );
    }
}

function renderCalendar(year, month, daysWithLessons) {
    const grid = document.getElementById('calendar-grid');
    
    const firstDay = new Date(year, month - 1, 1);
    const lastDay = new Date(year, month, 0);
    const startPadding = (firstDay.getDay() + 6) % 7; // Monday start
    
    let html = '';
    
    // Padding days
    for (let i = 0; i < startPadding; i++) {
        html += '<span class="calendar-day other-month" aria-hidden="true"></span>';
    }
    
    // Days
    const today = new Date();
    for (let day = 1; day <= lastDay.getDate(); day++) {
        const isToday = today.getDate() === day && 
                       today.getMonth() + 1 === month && 
                       today.getFullYear() === year;
        
        const hasLessons = daysWithLessons[day]?.length > 0;
        const dot = hasLessons ? '<span class="day-dot" aria-hidden="true"></span>' : '';
        const spokenDate = new Date(year, month - 1, day).toLocaleDateString(
            'ru-RU',
            {day: 'numeric', month: 'long'}
        );
        
        html += `
            <button type="button" class="calendar-day ${isToday ? 'today' : ''}"
                 data-day="${day}"
                 aria-label="${spokenDate}${hasLessons ? ', есть занятия' : ', занятий нет'}"
                 onclick="selectCalendarDay(${day}, this)">
                ${day}
                ${dot}
            </button>
        `;
    }
    
    grid.innerHTML = html;

    const isCurrentMonth = today.getFullYear() === year && today.getMonth() + 1 === month;
    const defaultDay = selectedCalendarDay
        || (isCurrentMonth
            ? today.getDate()
            : (Object.keys(daysWithLessons).map(Number).sort((a, b) => a - b)[0] || 1));
    const defaultButton = grid.querySelector(`[data-day="${defaultDay}"]`);
    selectCalendarDay(defaultDay, defaultButton);
}

function changeMonth(delta) {
    currentCalendarDate.setMonth(currentCalendarDate.getMonth() + delta);
    selectedCalendarDay = null;
    loadCalendar();
}

function selectCalendarDay(day, element) {
    selectedCalendarDay = day;
    const lessons = calendarData.days[day] || [];
    const container = document.getElementById('calendar-day-details');
    
    // Get day of week name
    const monthNames = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь', 
                        'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'];
    const dayNames = ['Воскресенье', 'Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота'];
    const monthText = document.getElementById('calendar-month').textContent;
    const [monthName, year] = monthText.split(' ');
    const monthIndex = monthNames.indexOf(monthName);
    const dateObj = new Date(parseInt(year), monthIndex, day);
    const dayOfWeek = dayNames[dateObj.getDay()];
    
    if (lessons.length === 0) {
        container.innerHTML = `
            <div class="calendar-day-summary is-empty">
                <div class="calendar-date-number">${day}</div>
                <div class="calendar-date-weekday">${dayOfWeek}</div>
                <p>На этот день занятий нет</p>
            </div>
        `;
    } else {
        // Group by time
        const byTime = {};
        lessons.forEach((l, lessonIndex) => {
            const time = l.time || '—';
            if (!byTime[time]) byTime[time] = [];
            byTime[time].push({...l, calendar_index: lessonIndex});
        });
        
        // Calculate totals
        const totalStudents = lessons.length;
        const markedStudents = lessons.filter(s => s.is_marked).length;
        const presentStudents = lessons.filter(s => s.status === 'present').length;
        
        let html = `
            <div class="calendar-day-summary">
                <div class="calendar-date-number">${day}</div>
                <div class="calendar-date-weekday">${dayOfWeek}</div>
                <div class="calendar-day-metrics">
                    <span><b>${totalStudents}</b> всего</span>
                    <span><b class="success">${markedStudents}</b> отмечено</span>
                    <span><b class="accent">${presentStudents}</b> были</span>
                </div>
            </div>
        `;
        
        // Show lessons grouped by time
        Object.keys(byTime).sort().forEach((time) => {
            const students = byTime[time];
            const markedCount = students.filter(s => s.is_marked).length;
            
            html += `
                <section class="calendar-session-card">
                    <div class="calendar-session-header">
                        <div class="calendar-session-time">
                            <span class="calendar-session-icon" aria-hidden="true">🕐</span>
                            <span>${escapeHtml(time)}</span>
                        </div>
                        <div class="calendar-session-count ${markedCount > 0 ? 'is-marked' : ''}">
                            ${markedCount > 0 ? `✓ ${markedCount}/${students.length}` : `${students.length} уч.`}
                        </div>
                    </div>
                    <div class="calendar-session-students">
                        ${students.map(s => {
                            let statusIcon = '⏳';
                            let statusColor = 'var(--text-muted)';
                            let statusText = 'Не отмечен';
                            if (s.status === 'present') {
                                statusIcon = '✅';
                                statusColor = 'var(--success)';
                                statusText = 'Присутствовал';
                            } else if (s.status === 'absent') {
                                statusIcon = '❌';
                                statusColor = 'var(--danger)';
                                statusText = 'Отсутствовал';
                            } else if (s.status === 'sick') {
                                statusIcon = '🤒';
                                statusColor = 'var(--warning)';
                                statusText = 'Болел';
                            }
                            
                            return `
                                <button type="button" class="calendar-student-row"
                                     aria-label="${escapeHtml(s.student_name)}: ${statusText}"
                                     onclick="openLessonDetailFromCalendar(${day}, ${s.calendar_index})">
                                    <div class="calendar-student-copy">
                                        <div class="calendar-student-name">${escapeHtml(s.student_name)}</div>
                                        <div class="calendar-student-meta">
                                            ${escapeHtml(s.location || 'Зал')} · <span style="color: ${statusColor};">${statusText}</span>
                                        </div>
                                    </div>
                                    <div class="calendar-student-status" aria-hidden="true">${statusIcon}</div>
                                </button>
                            `;
                        }).join('')}
                    </div>
                </section>
            `;
        });
        
        container.innerHTML = html;
    }
    
    // Highlight selected day
    document.querySelectorAll('.calendar-day').forEach((el) => {
        el.classList.remove('selected');
    });
    if (element) {
        element.classList.add('selected');
    }
}

function getCalendarIsoDate(day) {
    const year = currentCalendarDate.getFullYear();
    const month = String(currentCalendarDate.getMonth() + 1).padStart(2, '0');
    const dayString = String(day).padStart(2, '0');
    return `${year}-${month}-${dayString}`;
}

function getLessonStatusLabel(status) {
    if (status === 'present') return 'Присутствовал';
    if (status === 'absent') return 'Отсутствовал';
    if (status === 'sick') return 'Болел';
    return 'Не отмечен';
}

function renderLessonDetail(lesson, lessonDate, day, lessonIndex) {
    const container = document.getElementById('lesson-detail-content');
    if (!container || !lesson) return;

    const currentStatus = lesson.status || '';
    const statusButtons = [
        { value: 'present', label: '✅ Был' },
        { value: 'absent', label: '❌ Не был' },
        { value: 'sick', label: '🤒 Болел' },
    ];

    container.innerHTML = `
        <div class="screen-header">
            <button type="button" class="back-btn" aria-label="Назад" onclick="goBack()">
                <svg width="24" height="24" viewBox="0 0 24 24" fill="none"><path d="M15 18L9 12L15 6" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>
            </button>
            <span class="screen-title">Занятие</span>
            <div style="width: 40px;"></div>
        </div>
        <div class="lesson-detail-shell">
            <section class="lesson-hero">
                <span class="lesson-hero-kicker">Ученик</span>
                <h2>${escapeHtml(lesson.student_name)}</h2>
                <span class="lesson-status-chip ${currentStatus || 'unmarked'}">${escapeHtml(getLessonStatusLabel(currentStatus))}</span>
            </section>

            <section class="lesson-info-card" aria-label="Детали занятия">
                <div><span>📅 Дата</span><strong>${formatDate(lessonDate)}</strong></div>
                <div><span>🕐 Время</span><strong>${escapeHtml(lesson.time || '—')}</strong></div>
                <div><span>📍 Зал</span><strong>${escapeHtml(lesson.location || 'Зал')}</strong></div>
            </section>

            <section class="lesson-attendance-card">
                <h3>Посещаемость</h3>
                <p>Выберите актуальный статус ученика.</p>
                <div class="lesson-status-actions">
                    ${statusButtons.map((button) => `
                        <button type="button"
                                class="${currentStatus === button.value ? 'btn-primary' : 'btn-secondary'}"
                                aria-pressed="${currentStatus === button.value}"
                                onclick="saveLessonAttendanceFromCalendar(${day}, ${lessonIndex}, '${button.value}')">
                            ${button.label}
                        </button>
                    `).join('')}
                </div>
            </section>

            <div class="form-actions">
                <button type="button" class="btn-secondary" onclick="openStudentDetail(${lesson.student_id})">Карточка ученика</button>
                <button type="button" class="btn-secondary" onclick="goBack()">Назад</button>
            </div>
        </div>
    `;
}

function openLessonDetailFromCalendar(day, lessonIndex) {
    const lesson = calendarData.days?.[day]?.[lessonIndex];
    if (!lesson) {
        return;
    }

    renderLessonDetail(lesson, getCalendarIsoDate(day), day, lessonIndex);
    navigate('lesson-detail');
}

async function saveLessonAttendanceFromCalendar(day, lessonIndex, status) {
    const lesson = calendarData.days?.[day]?.[lessonIndex];
    if (!lesson) {
        showNotification('Урок не найден', 'error');
        return;
    }

    const lessonDate = getCalendarIsoDate(day);
    const endpoint = lesson.lesson_id
        ? `${API}/api/lessons/${lesson.lesson_id}/attendance`
        : `${API}/api/lessons/create`;
    const payload = lesson.lesson_id
        ? { initData, status }
        : {
            initData,
            lesson: {
                student_id: lesson.student_id,
                date: lessonDate,
                time: lesson.time,
                location: lesson.location,
                status,
            }
        };

    try {
        const res = await fetch(endpoint, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload)
        });
        const result = await res.json();

        if (!result.success) {
            showNotification('Ошибка сохранения', 'error');
            return;
        }

        lesson.status = status;
        lesson.is_marked = true;
        lesson.lesson_id = result.id || lesson.lesson_id;
        renderLessonDetail(lesson, lessonDate, day, lessonIndex);
        showNotification('Посещаемость обновлена', 'success');
        loadCalendar();
    } catch (e) {
        console.error('Lesson attendance save error:', e);
        showNotification('Ошибка сети', 'error');
    }
}

// === Payments ===

async function loadPayments(status = currentPaymentsFilter) {
    currentPaymentsFilter = status || 'all';

    try {
        const body = {initData};
        if (currentPaymentsFilter !== 'all') {
            body.status = currentPaymentsFilter;
        }
        
        const res = await fetch(`${API}/api/payments`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body)
        });
        
        payments = await res.json();
        renderPayments(payments);
    } catch (e) {
        console.error('Payments load error:', e);
    }
}

function renderPayments(list) {
    const container = document.getElementById('payments-list');
    
    if (list.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">💰</div>
                <p>Платежей нет</p>
            </div>
        `;
        return;
    }
    
    container.innerHTML = list.map(p => {
        const statusClass = p.status;
        const statusText = {paid: 'Оплачено', pending: 'Ожидает', overdue: 'Просрочено'}[p.status];
        const lessonsText = p.is_unlimited ? '♾️ Безлимит' : formatLessonCount(p.lessons_count || 0);
        
        return `
            <article class="list-item payment-card">
                <div class="list-item-header">
                    <span class="list-item-title">${escapeHtml(p.student_name)}</span>
                    <span class="payment-status ${statusClass}">${statusText}</span>
                </div>
                <div class="payment-amount">${p.amount.toLocaleString()} Br</div>
                <div class="list-item-subtitle">${lessonsText}</div>
                <div class="list-item-meta">
                    ${p.period_start && p.period_end ? 
                        `<span>📅 ${formatDate(p.period_start)} — ${formatDate(p.period_end)}</span>` : ''}
                </div>
                <div class="payment-actions">
                    ${p.status !== 'paid' ? `
                        <button class="payment-action payment-action-primary" onclick="markPaymentPaid(${p.id})">
                            <span aria-hidden="true">✓</span> Отметить оплату
                        </button>
                    ` : ''}
                    <button class="payment-action payment-action-secondary" onclick="openEditPayment(${p.id})">
                        Редактировать
                    </button>
                    <button class="payment-action payment-action-danger" onclick="deletePayment(${p.id})">
                        Удалить
                    </button>
                </div>
            </article>
        `;
    }).join('');
}

function switchPaymentTab(status, btn) {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    btn.classList.add('active');
    currentPaymentsFilter = status;
    loadPayments(status);
}

async function resetPaymentForm() {
    editingPaymentId = null;
    document.getElementById('pay-form-title').textContent = 'Новая оплата';
    document.getElementById('payment-form').reset();
    document.getElementById('pay-unlimited').checked = false;
    togglePayUnlimited(false);
    document.getElementById('pay-student').disabled = false;
    
    // Set default dates
    const today = new Date();
    const nextMonth = new Date(today.getFullYear(), today.getMonth() + 1, today.getDate());
    document.getElementById('pay-start').value = today.toISOString().split('T')[0];
    document.getElementById('pay-end').value = nextMonth.toISOString().split('T')[0];
}

async function openAddPayment() {
    await resetPaymentForm();
    
    // Load students for select
    const res = await fetch(`${API}/api/students`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({initData, coach_id: currentCoach?.coach_id || null})
    });
    
    const studentsList = await res.json();
    const select = document.getElementById('pay-student');
    select.innerHTML = '<option value="">Выберите ученика</option>' + 
        studentsList.map(s => `<option value="${s.id}">${escapeHtml(s.name)}</option>`).join('');
    
    navigate('payment-form');
}

async function openEditPayment(paymentId) {
    editingPaymentId = paymentId;
    document.getElementById('pay-form-title').textContent = 'Редактировать оплату';
    
    // Load payment details from current list or fetch fresh
    const paymentsRes = await fetch(`${API}/api/payments`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({initData, coach_id: currentCoach?.coach_id || null})
    });
    const payments = await paymentsRes.json();
    const payment = payments.find(p => p.id === paymentId);
    
    if (!payment) {
        showNotification('Платёж не найден', 'error');
        return;
    }
    
    // Load students for select
    const res = await fetch(`${API}/api/students`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({initData})
    });
    const studentsList = await res.json();
    const select = document.getElementById('pay-student');
    select.innerHTML = '<option value="">Выберите ученика</option>' + 
        studentsList.map(s => `<option value="${s.id}">${escapeHtml(s.name)}</option>`).join('');
    
    // Fill form
    select.value = payment.student_id;
    select.disabled = true; // Don't allow changing student on edit
    document.getElementById('pay-amount').value = payment.amount;
    document.getElementById('pay-unlimited').checked = payment.is_unlimited || false;
    togglePayUnlimited(payment.is_unlimited || false);
    document.getElementById('pay-count').value = payment.lessons_count || 8;
    document.getElementById('pay-start').value = payment.period_start || '';
    document.getElementById('pay-end').value = payment.period_end || '';
    document.getElementById('pay-status').value = payment.status || 'pending';
    document.getElementById('pay-notes').value = payment.notes || '';
    
    navigate('payment-form');
}

async function deletePayment(paymentId) {
    if (!confirm('Удалить платёж? Это пересчитает остаток занятий ученика.')) {
        return;
    }
    
    try {
        const res = await fetch(`${API}/api/payments/${paymentId}/delete`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData})
        });
        
        const result = await res.json();
        
        if (result.success) {
            showNotification('Платёж удалён', 'success');
            await refreshVisibleData(result.student_id || null);
        } else {
            showNotification('Ошибка удаления', 'error');
        }
    } catch (e) {
        console.error('Delete payment error:', e);
        showNotification('Ошибка удаления', 'error');
    }
}

function addPaymentForStudent(studentId) {
    openAddPayment().then(() => {
        document.getElementById('pay-student').value = studentId;
    });
}

let paymentSubmitting = false;

async function savePayment() {
    if (paymentSubmitting) {
        return;
    }
    paymentSubmitting = true;
    
    try {
        const isUnlimited = document.getElementById('pay-unlimited').checked;
        const lessonsCount = parseInt(document.getElementById('pay-count').value, 10) || 0;
        const data = {
            student_id: parseInt(document.getElementById('pay-student').value),
            amount: parseInt(document.getElementById('pay-amount').value),
            lessons_count: isUnlimited ? 0 : lessonsCount,
            period_start: document.getElementById('pay-start').value,
            period_end: document.getElementById('pay-end').value,
            status: document.getElementById('pay-status').value,
            is_unlimited: isUnlimited,
            notes: document.getElementById('pay-notes').value,
        };
        
        if (!data.student_id || !data.amount) {
            showNotification('Заполните обязательные поля', 'error');
            return;
        }

        if (!isUnlimited && data.lessons_count <= 0) {
            showNotification('Укажите количество занятий', 'error');
            return;
        }

        if (!data.period_start || !data.period_end) {
            showNotification('Укажите период действия абонемента', 'error');
            return;
        }

        if (data.period_end < data.period_start) {
            showNotification('Дата окончания не может быть раньше даты начала', 'error');
            return;
        }
        
        const url = editingPaymentId 
            ? `${API}/api/payments/${editingPaymentId}/update`
            : `${API}/api/payments/create`;
        
        const res = await fetch(url, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData, payment: data})
        });
        const result = await res.json();
        
        if (result.success) {
            showNotification(editingPaymentId ? 'Оплата обновлена' : 'Оплата добавлена', 'success');
            const affectedStudentId = result.student_id || data.student_id;
            editingPaymentId = null;
            goBack();
            await refreshVisibleData(affectedStudentId);
        } else {
            showNotification('Ошибка сохранения', 'error');
        }
    } catch (e) {
        console.error('Save payment error:', e);
        showNotification('Ошибка сохранения', 'error');
    } finally {
        paymentSubmitting = false;
    }
}

async function markPaymentPaid(id) {
    try {
        const res = await fetch(`${API}/api/payments/${id}/mark-paid`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData})
        });
        
        const result = await res.json();
        
        if (result.success) {
            showNotification('Оплачено!', 'success');
            await refreshVisibleData(result.student_id || null);
        }
    } catch (e) {
        console.error('Mark paid error:', e);
    }
}

// === Quick Lesson ===

async function openQuickLesson() {
    navigate('quick-lesson');
}

// Quick attendance data
let quickAttendanceData = {};

function buildQuickAttendanceKey(studentId, scheduleTime, locationId) {
    const normalizedTime = (scheduleTime || '00:00').replace(':', '-');
    return `${studentId}__${normalizedTime}__${locationId || 0}`;
}

function getQuickStatusIcon(status) {
    if (status === 'present') return '✅';
    if (status === 'absent') return '❌';
    if (status === 'sick') return '🤒';
    return '⏳';
}

function getQuickStatusLabel(status) {
    if (status === 'present') return 'присутствует';
    if (status === 'absent') return 'отсутствует';
    if (status === 'sick') return 'болеет';
    return 'не отмечен';
}

function applyQuickStatusUI(item, statusEl, status) {
    if (!item || !statusEl) return;

    item.classList.remove('selected-present', 'selected-absent', 'selected-sick');
    if (status) {
        item.classList.add(`selected-${status}`);
    }
    statusEl.textContent = getQuickStatusIcon(status);
}

async function loadQuickLesson() {
    const dateInput = document.getElementById('ql-date');
    const locationSelect = document.getElementById('ql-location');
    
    // Set default date if not set
    if (!dateInput.value) {
        dateInput.value = new Date().toISOString().split('T')[0];
    }
    
    // Load locations for filter
    if (locationSelect.options.length <= 1) {
        try {
            const locRes = await fetch(`${API}/api/locations`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({initData})
            });
            const locations = await locRes.json();
            locationSelect.innerHTML = '<option value="">Все залы</option>' +
                locations.map(l => `<option value="${l.id}">${escapeHtml(l.name)}</option>`).join('');
        } catch (e) {
            console.error('Load locations error:', e);
        }
    }
    
    const date = dateInput.value;
    const locationId = locationSelect.value;
    
    try {
        const [studentsRes, dayStatusRes] = await Promise.all([
            fetch(`${API}/api/students`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({initData, coach_id: currentCoach?.coach_id || null})
            }),
            fetch(`${API}/api/attendance/day-status`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({initData, date, location_id: locationId || null})
            })
        ]);
        
        const studentsList = await studentsRes.json();
        const dayStatus = await dayStatusRes.json();
        const savedStatusMap = new Map();

        (dayStatus.attendance || []).forEach(entry => {
            const entryKey = buildQuickAttendanceKey(entry.student_id, entry.time, entry.location_id);
            savedStatusMap.set(entryKey, entry.status || null);

            if (!entry.location_id) {
                savedStatusMap.set(
                    buildQuickAttendanceKey(entry.student_id, entry.time, 0),
                    entry.status || null
                );
            }
        });
        
        // Convert JS day (0=Sun, 1=Mon) to Python weekday (0=Mon, 6=Sun)
        const jsDay = new Date(date).getDay();
        const pythonDay = jsDay === 0 ? 6 : jsDay - 1;
        
        // Filter students who have lesson on this day with their schedule info
        // IMPORTANT: Use ONLY schedules table, ignore legacy lesson_days to avoid format confusion
        let studentsWithSchedules = [];
        
        studentsList.forEach(s => {
            // Use only new schedules table
            if (s.schedules && s.schedules.length > 0) {
                s.schedules.forEach(sch => {
                    if (sch.days) {
                        const days = sch.days.split(',').map(d => d.trim());
                        if (days.includes(String(pythonDay))) {
                            // Check location filter
                            if (!locationId || sch.location_id == locationId) {
                                // Parse times JSON to get time for this day
                                let time = '18:00';
                                try {
                                    const times = JSON.parse(sch.times || '{}');
                                    // Try to get time for specific day, fallback to first available
                                    time = times[String(pythonDay)] || times[Object.keys(times)[0]] || '18:00';
                                } catch (e) {
                                    time = '18:00';
                                }
                                
                                studentsWithSchedules.push({
                                    ...s,
                                    schedule_time: time,
                                    schedule_location: sch.location_name || 'Зал',
                                    quick_key: buildQuickAttendanceKey(s.id, time, sch.location_id),
                                    schedule_location_id: sch.location_id
                                });
                            }
                        }
                    }
                });
            }
            // Note: NO fallback to lesson_days - it can be in inconsistent format
            // If student has no schedules, they won't appear (need to edit and save to create schedules)
        });
        
        // Group by time
        const byTime = {};
        studentsWithSchedules.forEach(s => {
            const time = s.schedule_time || '18:00';
            if (!byTime[time]) byTime[time] = [];
            byTime[time].push(s);
        });
        
        // Sort times
        const sortedTimes = Object.keys(byTime).sort();
        
        // Initialize attendance data
        quickAttendanceData = {};
        studentsWithSchedules.forEach(s => {
            const savedStatus =
                savedStatusMap.get(s.quick_key) ??
                savedStatusMap.get(buildQuickAttendanceKey(s.id, s.schedule_time, 0)) ??
                null;

            quickAttendanceData[s.quick_key] = {
                key: s.quick_key,
                student_id: s.id,
                status: savedStatus,
                student: s
            };
        });
        
        renderQuickLessonListGrouped(byTime, sortedTimes);
        updateQuickStats(studentsWithSchedules.length);
        
    } catch (e) {
        console.error('Quick lesson load error:', e);
        renderScreenState(
            document.getElementById('quick-lesson-students'),
            'Не удалось загрузить расписание.',
            {retry: 'loadQuickLesson()'}
        );
    }
}

function renderQuickLessonListGrouped(byTime, sortedTimes) {
    const container = document.getElementById('quick-lesson-students');
    
    if (sortedTimes.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">📅</div>
                <p>Нет учеников на этот день</p>
            </div>
        `;
        document.getElementById('ql-title').textContent = 'Отметка · 0';
        return;
    }
    
    let html = '';
    const totalStudents = Object.values(byTime).reduce((sum, group) => sum + group.length, 0);
    
    sortedTimes.forEach(time => {
        const students = byTime[time];
        html += `
            <div class="time-group">
                <div class="time-group-header">
                    <span class="time-badge">🕐 ${escapeHtml(time)}</span>
                    <span class="count-badge">${students.length} уч.</span>
                </div>
                <div class="time-group-students">
                    ${students.map((s, index) => {
                        const remaining = getStudentRemainingLessons(s);
                        let dotClass = 'ok';
                        if (!s.is_unlimited && remaining <= 0) dotClass = 'none';
                        else if (!s.is_unlimited && remaining <= 2) dotClass = 'low';
                        
                        const currentStatus = quickAttendanceData[s.quick_key]?.status || null;
                        const selectedClass = currentStatus ? `selected-${currentStatus}` : '';
                        
                        const locationName = s.schedule_location || s.location || 'Зал';
                        
                        return `
                            <button type="button"
                                    class="quick-student-item ${selectedClass}"
                                    data-attendance-key="${s.quick_key}"
                                    aria-label="${escapeHtml(s.name)}: ${getQuickStatusLabel(currentStatus)}. Нажмите, чтобы изменить статус"
                                    onclick="toggleQuickStatus('${s.quick_key}')">
                                <div class="quick-student-avatar">${escapeHtml(s.name.charAt(0))}</div>
                                <div class="quick-student-info">
                                    <div class="quick-student-name">
                                        ${index + 1}. ${escapeHtml(s.name)}
                                        <span class="lessons-dot ${dotClass}"></span>
                                    </div>
                                    <div class="quick-student-meta">
                                        ${getStudentLessonsMeta(s)} • 📍 ${escapeHtml(locationName)}
                                    </div>
                                </div>
                                <div class="quick-student-status" id="status-${s.quick_key}">${getQuickStatusIcon(currentStatus)}</div>
                            </button>
                        `;
                    }).join('')}
                </div>
            </div>
        `;
    });
    
    container.innerHTML = html;
    document.getElementById('ql-title').textContent = `Отметка · ${totalStudents}`;
}

function toggleQuickStatus(attendanceKey) {
    let resolvedKey = attendanceKey;
    let data = quickAttendanceData[resolvedKey];
    if (!data) {
        const fallbackEntry = Object.entries(quickAttendanceData).find(([, value]) => value.student_id == attendanceKey);
        if (fallbackEntry) {
            [resolvedKey, data] = fallbackEntry;
        }
    }
    if (!data) {
        return;
    }

    const item = document.querySelector(`[data-attendance-key="${resolvedKey}"]`);
    const statusEl = document.getElementById(`status-${resolvedKey}`);
    
    // Cycle: null -> present -> absent -> sick -> null
    const cycle = [null, 'present', 'absent', 'sick'];
    const currentIndex = cycle.indexOf(data.status);
    const nextStatus = cycle[(currentIndex + 1) % cycle.length];
    
    data.status = nextStatus;
    applyQuickStatusUI(item, statusEl, nextStatus);
    item?.setAttribute(
        'aria-label',
        `${data.student?.name || 'Ученик'}: ${getQuickStatusLabel(nextStatus)}. Нажмите, чтобы изменить статус`
    );
    announce(`${data.student?.name || 'Ученик'}: ${getQuickStatusLabel(nextStatus)}`);
    updateQuickStats();
}

function selectAllQuick(status) {
    Object.keys(quickAttendanceData).forEach(key => {
        quickAttendanceData[key].status = status;
        const item = document.querySelector(`[data-attendance-key="${key}"]`);
        const statusEl = document.getElementById(`status-${key}`);
        applyQuickStatusUI(item, statusEl, status);
    });
    
    updateQuickStats();
}

function updateQuickStats(totalOverride) {
    const total = totalOverride || Object.keys(quickAttendanceData).length;
    const marked = Object.values(quickAttendanceData).filter(d => d.status !== null).length;
    const present = Object.values(quickAttendanceData).filter(d => d.status === 'present').length;
    
    document.getElementById('quick-lesson-stats').innerHTML = `
        <div class="quick-stat">
            <span class="quick-stat-value">${marked}/${total}</span>
            <span class="quick-stat-label">Отмечено</span>
        </div>
        <div class="quick-stat">
            <span class="quick-stat-value" style="color: var(--success)">${present}</span>
            <span class="quick-stat-label">Присутствуют</span>
        </div>
        <div class="quick-stat">
            <span class="quick-stat-value">${total - marked}</span>
            <span class="quick-stat-label">Осталось</span>
        </div>
    `;
}

async function saveQuickAttendance() {
    const date = document.getElementById('ql-date').value;
    const attendances = Object.values(quickAttendanceData)
        .filter(d => d.status !== null)
        .map(d => ({
            student_id: d.student_id,
            status: d.status,
            time: d.student?.schedule_time || null
        }));
    
    if (attendances.length === 0) {
        showNotification('Отметьте хотя бы одного ученика', 'error');
        return;
    }
    
    try {
        const res = await fetch(`${API}/api/bulk-attendance`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                initData,
                date: date,
                attendance: attendances
            })
        });
        
        const result = await res.json();
        
        if (result.success) {
            showNotification(`Сохранено: ${result.marked} учеников`, 'success');
            
            // Show alert about low lessons if any
            if (result.low_lessons_alert && result.low_lessons_alert.length > 0) {
                const names = result.low_lessons_alert.map(s => s.name).join(', ');
                setTimeout(() => {
                    showNotification(`⚠️ Мало занятий: ${names}`, 'warning', 5000);
                }, 1000);
            }
            
            goBack();
            await refreshVisibleData();
        } else {
            showNotification('Ошибка сохранения', 'error');
        }
    } catch (e) {
        console.error('Save quick attendance error:', e);
        showNotification('Ошибка сети', 'error');
    }
}

// === Helpers ===

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function announce(message) {
    const liveRegion = document.getElementById('live-region');
    if (!liveRegion) return;
    liveRegion.textContent = '';
    requestAnimationFrame(() => {
        liveRegion.textContent = message;
    });
}

function formatLessonCount(count) {
    const absolute = Math.abs(Number(count) || 0);
    const mod100 = absolute % 100;
    const mod10 = absolute % 10;
    const word = mod100 >= 11 && mod100 <= 14
        ? 'занятий'
        : mod10 === 1
            ? 'занятие'
            : mod10 >= 2 && mod10 <= 4
                ? 'занятия'
                : 'занятий';
    return `${count} ${word}`;
}

function formatMonthLabel(value) {
    const isoMatch = String(value || '').match(/^(\d{4})-(\d{2})$/);
    if (isoMatch) {
        const labels = ['Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн', 'Июл', 'Авг', 'Сен', 'Окт', 'Ноя', 'Дек'];
        return labels[Number(isoMatch[2]) - 1] || value;
    }

    const monthMap = {
        Jan: 'Янв', Feb: 'Фев', Mar: 'Мар', Apr: 'Апр',
        May: 'Май', Jun: 'Июн', Jul: 'Июл', Aug: 'Авг',
        Sep: 'Сен', Oct: 'Окт', Nov: 'Ноя', Dec: 'Дек'
    };
    const [month, year] = String(value || '').split(' ');
    return `${monthMap[month] || month}${year ? ` ${year}` : ''}`.trim();
}

function getPeriodCaption(period) {
    if (period === 'all') return 'За всё время';
    const now = new Date();
    const start = period === 'year'
        ? new Date(now.getFullYear(), 0, 1)
        : period === 'week'
            ? new Date(now.getFullYear(), now.getMonth(), now.getDate() - ((now.getDay() + 6) % 7))
            : new Date(now.getFullYear(), now.getMonth(), 1);
    const formatter = new Intl.DateTimeFormat('ru-RU', {day: 'numeric', month: 'short', year: 'numeric'});
    return `${formatter.format(start)} — ${formatter.format(now)}`;
}

function formatDate(dateStr) {
    if (!dateStr) return '—';
    const date = new Date(dateStr);
    return date.toLocaleDateString('ru-RU', {day: '2-digit', month: '2-digit', year: 'numeric'});
}

function showNotification(message, type = 'info') {
    // Remove existing notifications
    document.querySelectorAll('.notification').forEach(n => n.remove());
    
    const notif = document.createElement('div');
    notif.className = `notification ${type}`;
    notif.setAttribute('role', type === 'error' ? 'alert' : 'status');
    notif.textContent = message;
    document.body.appendChild(notif);
    announce(message);
    
    setTimeout(() => {
        notif.style.opacity = '0';
        setTimeout(() => notif.remove(), 300);
    }, 3000);
}

// Handle date change in quick lesson
document.getElementById('ql-date')?.addEventListener('change', () => {
    if (currentScreen === 'quick-lesson') {
        loadQuickLesson();
    }
});

// === Extra Attendance & Attendance History ===

async function openExtraAttendanceModal(studentId = null) {
    const now = new Date();
    const quickDateInput = document.getElementById('ql-date');
    const quickLocationSelect = document.getElementById('ql-location');
    const defaultDate = currentScreen === 'quick-lesson' && quickDateInput?.value
        ? quickDateInput.value
        : now.toISOString().split('T')[0];
    const defaultTime = now.toTimeString().slice(0, 5);
    const defaultLocationId = currentScreen === 'quick-lesson' && quickLocationSelect?.value
        ? quickLocationSelect.value
        : '';

    const [studentsRes, locationsRes] = await Promise.all([
        fetch(`${API}/api/students`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData, coach_id: currentCoach?.coach_id || null})
        }),
        fetch(`${API}/api/locations`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData})
        })
    ]);

    const studentsList = await studentsRes.json();
    const locationsList = await locationsRes.json();
    const selectedStudent = studentId ? studentsList.find(s => s.id === studentId) : null;
    const selectedStudentLocationId = selectedStudent?.location_id
        || selectedStudent?.schedules?.find(schedule => schedule.is_primary)?.location_id
        || '';
    const resolvedLocationId = defaultLocationId || selectedStudentLocationId;

    const studentFieldHtml = studentId && selectedStudent
        ? `
            <div class="form-group">
                <label>Ученик</label>
                <div style="padding: 12px; border-radius: 10px; background: var(--bg-secondary); font-weight: 600;">
                    ${escapeHtml(selectedStudent.name)}
                </div>
                <input type="hidden" id="extra-student-id" value="${studentId}">
            </div>
        `
        : `
            <div class="form-group">
                <label>Ученик</label>
                <select id="extra-student-id">
                    <option value="">Выберите ученика</option>
                    ${studentsList.map(s => `<option value="${s.id}">${escapeHtml(s.name)}</option>`).join('')}
                </select>
            </div>
        `;

    const modal = document.createElement('div');
    modal.className = 'modal';
    modal.innerHTML = `
        <div class="modal-content">
            <h3>⭐ Внеплановое занятие</h3>
            <p style="margin-bottom: 16px; color: var(--text-secondary);">
                Отметьте отработку или дополнительное занятие в любой день. При необходимости урок сразу спишется из абонемента.
            </p>
            ${studentFieldHtml}
            <div class="form-group">
                <label>Дата</label>
                <input type="date" id="extra-date" value="${defaultDate}">
            </div>
            <div class="form-group">
                <label>Время</label>
                <input type="time" id="extra-time" value="${defaultTime}">
            </div>
            <div class="form-group">
                <label>Зал</label>
                <select id="extra-location-id">
                    <option value="">Без зала</option>
                    ${locationsList.map(loc => `<option value="${loc.id}" ${String(loc.id) === String(resolvedLocationId) ? 'selected' : ''}>${escapeHtml(loc.name)}</option>`).join('')}
                </select>
            </div>
            <div class="form-group">
                <label>Статус</label>
                <select id="extra-status">
                    <option value="present">✅ Присутствовал</option>
                    <option value="absent">❌ Отсутствовал</option>
                    <option value="sick">🤒 Болел</option>
                </select>
            </div>
            <div class="form-group">
                <label>Заметки</label>
                <input type="text" id="extra-notes" placeholder="Например: отработка за пропуск 21.04">
            </div>
            <div class="form-group">
                <label>
                    <input type="checkbox" id="extra-deduct" checked>
                    Списать занятие с абонемента (снимите только для подтверждённой отработки)
                </label>
            </div>
            <div class="modal-actions">
                <button class="btn-secondary" onclick="this.closest('.modal').remove()">Отмена</button>
                <button class="btn-primary" onclick="saveExtraAttendance()">Сохранить</button>
            </div>
        </div>
    `;

    document.body.appendChild(modal);
}

async function submitExtraAttendance() {
    const studentId = parseInt(document.getElementById('extra-student-id')?.value, 10);
    const date = document.getElementById('extra-date').value;
    const time = document.getElementById('extra-time').value;
    const locationId = document.getElementById('extra-location-id')?.value || null;
    const status = document.getElementById('extra-status').value;
    const notes = document.getElementById('extra-notes').value;
    const deduct = document.getElementById('extra-deduct').checked;

    if (!studentId) {
        showNotification('Выберите ученика', 'error');
        return;
    }

    const res = await fetch(`${API}/api/extra-attendance`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            initData,
            student_id: studentId,
            date,
            time,
            location_id: locationId,
            status,
            notes,
            deduct_lesson: deduct
        })
    });

    const result = await res.json();

    if (result.success) {
        showNotification(result.message, 'success');
        document.querySelector('.modal')?.remove();
        await refreshVisibleData(result.student_id || studentId);
        return;
    }

    if (result.error === 'already_marked') {
        showNotification('На это время посещаемость уже отмечена', 'warning');
        return;
    }

    if (result.error === 'no_lessons_remaining') {
        showNotification('У ученика не осталось занятий в абонементе', 'error');
        return;
    }

    showNotification('Ошибка сохранения', 'error');
}

async function markExtraAttendance(studentId) {
    try {
        return await openExtraAttendanceModal(studentId);
    } catch (e) {
        console.error('Open extra attendance modal error:', e);
        showNotification('Ошибка загрузки формы', 'error');
        return;
    }
    // Show confirmation dialog with options
    const now = new Date();
    const today = now.toISOString().split('T')[0];
    const currentTime = now.toTimeString().slice(0, 5);
    
    // Create modal
    const modal = document.createElement('div');
    modal.className = 'modal';
    modal.innerHTML = `
        <div class="modal-content">
            <h3>⭐ Внеплановое посещение</h3>
            <p style="margin-bottom: 16px; color: var(--text-secondary);">Отметить ученика вне расписания (отработка/дополнительное занятие)</p>
            
            <div class="form-group">
                <label>Дата</label>
                <input type="date" id="extra-date" value="${today}">
            </div>
            
            <div class="form-group">
                <label>Время</label>
                <input type="time" id="extra-time" value="${currentTime}">
            </div>
            
            <div class="form-group">
                <label>Статус</label>
                <select id="extra-status">
                    <option value="present">✅ Присутствовал</option>
                    <option value="absent">❌ Отсутствовал</option>
                    <option value="sick">🤒 Болеет</option>
                </select>
            </div>
            
            <div class="form-group">
                <label>Заметки</label>
                <input type="text" id="extra-notes" placeholder="Например: Отработка за 15.03">
            </div>
            
            <div class="form-group">
                <label>
                    <input type="checkbox" id="extra-deduct" checked>
                    Списать занятие с абонемента
                </label>
            </div>
            
            <div class="modal-actions">
                <button class="btn-secondary" onclick="this.closest('.modal').remove()">Отмена</button>
                <button class="btn-primary" onclick="saveExtraAttendance(${studentId})">Сохранить</button>
            </div>
        </div>
    `;
    
    document.body.appendChild(modal);
}

async function saveExtraAttendance(studentId) {
    try {
        return await submitExtraAttendance();
    } catch (e) {
        console.error('Extra attendance error:', e);
        showNotification('Ошибка сохранения', 'error');
        return;
    }
    const date = document.getElementById('extra-date').value;
    const time = document.getElementById('extra-time').value;
    const status = document.getElementById('extra-status').value;
    const notes = document.getElementById('extra-notes').value;
    const deduct = document.getElementById('extra-deduct').checked;
    
    try {
        const res = await fetch(`${API}/api/extra-attendance`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                initData,
                student_id: studentId,
                date,
                time,
                status,
                notes,
                deduct_lesson: deduct
            })
        });
        
        const result = await res.json();
        
        if (result.success) {
            showNotification(result.message, 'success');
            document.querySelector('.modal')?.remove();
            // Refresh student detail
            openStudentDetail(studentId);
        } else if (result.error === 'already_marked') {
            showNotification('На это время посещаемость уже отмечена', 'warning');
        } else if (result.error === 'no_lessons_remaining') {
            showNotification('У ученика не осталось занятий в абонементе', 'error');
        } else {
            showNotification('Ошибка сохранения', 'error');
        }
    } catch (e) {
        console.error('Extra attendance error:', e);
        showNotification('Ошибка сохранения', 'error');
    }
}

async function deactivateStudent(studentId) {
    if (!confirm('Деактивировать ученика? Он будет скрыт из списков, но данные сохранятся.')) {
        return;
    }
    
    try {
        const res = await fetch(`${API}/api/students/${studentId}/delete`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData})
        });
        
        const result = await res.json();
        
        if (result.success) {
            showNotification('Ученик деактивирован', 'success');
            goBack();
            loadStudents();
        } else {
            showNotification('Ошибка деактивации', 'error');
        }
    } catch (e) {
        console.error('Deactivate error:', e);
        showNotification('Ошибка сети', 'error');
    }
}

async function destroyStudent(studentId) {
    const studentName = currentStudentDetailId === studentId ? currentStudentDetailName : '';
    if (!studentName) {
        showNotification('Не удалось подтвердить имя ученика', 'error');
        return;
    }
    const confirmText = prompt(`ВНИМАНИЕ! Это действие НЕОБРАТИМО!\\n\\nДля подтверждения удаления ученика "${studentName}" введите его имя:`);
    
    if (confirmText !== studentName) {
        showNotification('Удаление отменено - имя не совпадает', 'error');
        return;
    }
    
    if (!confirm(`УДАЛИТЬ УЧЕНИКА НАВСЕГДА?\\n\\nВсе занятия, оплаты и история будут безвозвратно удалены!`)) {
        return;
    }
    
    try {
        const res = await fetch(`${API}/api/students/${studentId}/destroy`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData, confirm_destroy: true})
        });
        
        const result = await res.json();
        
        if (result.success) {
            showNotification(`Ученик ${studentName} удален навсегда`, 'success');
            goBack();
            loadStudents();
        } else if (result.error === 'confirmation_required') {
            showNotification('Требуется подтверждение', 'error');
        } else {
            showNotification('Ошибка удаления', 'error');
        }
    } catch (e) {
        console.error('Destroy error:', e);
        showNotification('Ошибка сети', 'error');
    }
}

async function viewAttendanceHistory(studentId) {
    try {
        const res = await fetch(`${API}/api/students/${studentId}/attendance-history`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData})
        });
        
        const data = await res.json();
        
        if (data.error) {
            showNotification('Ошибка загрузки истории', 'error');
            return;
        }
        
        const student = data.student;
        const attendance = data.attendance;
        const stats = data.stats;
        
        // Create modal
        const modal = document.createElement('div');
        modal.className = 'modal';
        modal.onclick = (e) => {
            if (e.target === modal) modal.remove();
        };
        
        let attendanceHtml = '';
        if (attendance.length === 0) {
            attendanceHtml = '<p style="text-align: center; color: var(--text-secondary); padding: 20px;">Нет записей о посещениях</p>';
        } else {
            attendanceHtml = attendance.map(a => {
                const statusEmoji = {
                    'present': '✅',
                    'absent': '❌',
                    'sick': '🤒',
                    'excused': '📝'
                }[a.status] || '❓';
                
                const statusText = {
                    'present': 'Присутствовал',
                    'absent': 'Отсутствовал',
                    'sick': 'Болел',
                    'excused': 'Отменено'
                }[a.status] || a.status;
                
                const extraBadge = a.is_extra ? '<span class="badge-extra">⭐ Внеплановое</span>' : '';
                
                return `
                    <div class="attendance-history-item">
                        <div class="attendance-date">
                            <span class="date-day">${formatDate(a.date)}</span>
                            <span class="date-time">${a.time || a.scheduled_time || ''}</span>
                        </div>
                        <div class="attendance-status">
                            <span class="status-emoji">${statusEmoji}</span>
                            <span class="status-text">${statusText}</span>
                            ${extraBadge}
                        </div>
                        ${a.notes ? `<div class="attendance-notes">${escapeHtml(a.notes)}</div>` : ''}
                    </div>
                `;
            }).join('');
        }
        
        modal.innerHTML = `
            <div class="modal-content" style="max-height: 85vh; overflow-y: auto; display: flex; flex-direction: column;">
                <div class="modal-header">
                <h3>История посещений</h3>
                    <button class="close-btn" onclick="this.closest('.modal').remove()">✕</button>
                </div>
                
                <div class="student-summary">
                    <div class="summary-row">
                        <span class="summary-name">${escapeHtml(student.name)}</span>
                        <span class="summary-lessons ${!student.is_unlimited && student.lessons_remaining <= 2 ? 'warning' : ''}">
                            ${student.is_unlimited ? '♾️ Безлимитный абонемент' : `${student.lessons_remaining}/${student.lessons_count} занятий`}
                        </span>
                    </div>
                </div>
                
                <div class="attendance-stats-bar">
                    <div class="stat-pill">
                        <span class="stat-value">${stats.total_scheduled}</span>
                        <span class="stat-label">По расписанию</span>
                    </div>
                    <div class="stat-pill">
                        <span class="stat-value">${stats.extra_lessons}</span>
                        <span class="stat-label">Внеплановые</span>
                    </div>
                    <div class="stat-pill success">
                        <span class="stat-value">${stats.attendance_rate}%</span>
                        <span class="stat-label">Посещаемость</span>
                    </div>
                </div>
                
                <div class="attendance-history-list" style="flex: 1; overflow-y: auto; max-height: 50vh;">
                    ${attendanceHtml}
                </div>
                
                <div class="modal-actions" style="margin-top: 16px; padding-top: 16px; border-top: 1px solid var(--border);">
                    <button class="btn-secondary" onclick="this.closest('.modal').remove()">Закрыть</button>
                </div>
            </div>
        `;
        
        document.body.appendChild(modal);
    } catch (e) {
        console.error('Attendance history error:', e);
        showNotification('Ошибка загрузки истории', 'error');
    }
}

// === Daily Summary Button ===

async function loadDailySummary() {
    try {
        const res = await fetch(`${API}/api/coach/daily-summary`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData})
        });
        
        const data = await res.json();
        
        // Store for dashboard use
        window.dailySummaryData = data;
        
        return data;
    } catch (e) {
        console.error('Daily summary error:', e);
        return null;
    }
}


// === Multiple Locations Management ===

let currentLocationSchedules = [];
let availableLocations = [];
let availableGroups = [];

// Initialize with one default location
function initLocationSchedules(schedules = null) {
    if (schedules && schedules.length > 0) {
        currentLocationSchedules = schedules.map(s => ({
            id: s.id,
            location_id: s.location_id,
            group_id: s.group_id || null,
            days: s.days ? s.days.split(',').map(d => parseInt(d.trim())) : [],
            times: typeof s.times === 'string' ? JSON.parse(s.times) : s.times,
            duration: s.duration || 90,
            is_primary: s.is_primary
        }));
    } else {
        // Default schedule
        currentLocationSchedules = [{
            id: null,
            location_id: null,
            group_id: null,
            days: [1, 3], // Tue, Thu
            times: {"1": "18:00", "3": "18:00"},
            duration: 90,
            is_primary: true
        }];
    }
    renderLocationSchedules();
}

function addLocationSchedule() {
    currentLocationSchedules.push({
        id: null,
        location_id: null,
        group_id: null,
        days: [],
        times: {},
        duration: 90,
        is_primary: false
    });
    renderLocationSchedules();
}

function removeLocationSchedule(index) {
    if (currentLocationSchedules.length <= 1) {
        showNotification('Нужен хотя бы один зал', 'error');
        return;
    }
    currentLocationSchedules.splice(index, 1);
    // Ensure at least one is primary
    if (!currentLocationSchedules.some(s => s.is_primary)) {
        currentLocationSchedules[0].is_primary = true;
    }
    renderLocationSchedules();
}

function setPrimaryLocation(index) {
    currentLocationSchedules.forEach((s, i) => {
        s.is_primary = (i === index);
    });
    renderLocationSchedules();
}

function toggleLocationDay(locationIndex, day) {
    const schedule = currentLocationSchedules[locationIndex];
    const dayIndex = schedule.days.indexOf(day);
    
    if (dayIndex > -1) {
        schedule.days.splice(dayIndex, 1);
        delete schedule.times[day];
    } else {
        schedule.days.push(day);
        schedule.days.sort();
        schedule.times[day] = '18:00';
    }
    renderLocationSchedules();
}

function updateLocationTime(locationIndex, day, time) {
    currentLocationSchedules[locationIndex].times[day] = time;
}

function updateLocationField(locationIndex, field, value) {
    currentLocationSchedules[locationIndex][field] = value;
}

function renderLocationSchedules() {
    const container = document.getElementById('student-locations-container');
    if (!container) return;
    
    container.innerHTML = currentLocationSchedules.map((schedule, index) => {
        const locationOptions = availableLocations.map(loc => 
            `<option value="${loc.id}" ${schedule.location_id == loc.id ? 'selected' : ''}>${escapeHtml(loc.name)}</option>`
        ).join('');
        const groupOptions = availableGroups.map(group =>
            `<option value="${group.id}" ${schedule.group_id == group.id ? 'selected' : ''}>${escapeHtml(group.name)}</option>`
        ).join('');
        
        const dayButtons = [0, 1, 2, 3, 4, 5, 6].map(day => {
            const dayNames = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'];
            const isActive = schedule.days.includes(day);
            return `<button type="button" class="${isActive ? 'active' : ''}" onclick="toggleLocationDay(${index}, ${day})">${dayNames[day]}</button>`;
        }).join('');
        
        const timeInputs = schedule.days.map(day => {
            const dayNames = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'];
            const time = schedule.times[day] || '18:00';
            return `
                <div class="time-input-row">
                    <span class="day-label">${dayNames[day]}</span>
                    <input type="time" class="day-time" value="${time}" 
                           onchange="updateLocationTime(${index}, ${day}, this.value)">
                </div>
            `;
        }).join('');
        
        return `
            <div class="location-schedule-card ${schedule.is_primary ? 'primary' : ''}">
                <div class="location-header">
                    <span class="location-number">${schedule.is_primary ? '⭐ Основной зал' : `Доп. зал #${index + 1}`}</span>
                    <div class="location-actions">
                        ${!schedule.is_primary ? `<button type="button" class="btn-set-primary" onclick="setPrimaryLocation(${index})">Сделать основным</button>` : ''}
                        <button type="button" class="btn-icon" onclick="removeLocationSchedule(${index})">×</button>
                    </div>
                </div>
                
                <div class="location-select-wrapper">
                    <select id="loc-select-${index}" onchange="handleLocationSelect(${index}, this.value)">
                        <option value="">-- Выберите зал --</option>
                        ${locationOptions}
                        <option value="__new__" style="color: var(--accent); font-weight: 600;">+ Создать новый зал</option>
                    </select>
                    
                    <!-- New location input (hidden by default) -->
                    <div id="new-loc-${index}" class="new-location-input" style="display: none; margin-top: 8px;">
                        <input type="text" id="new-loc-name-${index}" placeholder="Название зала (например: Зал на Ленина)" 
                               style="width: 100%; padding: 10px; background: var(--bg-secondary); border: 1px solid var(--accent); border-radius: 8px; color: var(--text-primary);">
                        <div style="display: flex; gap: 8px; margin-top: 8px;">
                            <button type="button" class="btn-primary" onclick="createNewLocation(${index})" style="flex: 1; padding: 8px;">Создать</button>
                            <button type="button" class="btn-secondary" onclick="cancelNewLocation(${index})" style="padding: 8px 12px;">Отмена</button>
                        </div>
                    </div>
                </div>

                <div class="location-select-wrapper">
                    <select aria-label="Группа" onchange="updateLocationField(${index}, 'group_id', this.value ? Number(this.value) : null)">
                        <option value="">-- Без группы --</option>
                        ${groupOptions}
                    </select>
                </div>
                
                <div class="form-group">
                    <label>Дни недели</label>
                    <div class="weekdays-selector">
                        ${dayButtons}
                    </div>
                </div>
                
                ${schedule.days.length > 0 ? `
                    <div class="lesson-times-grid">
                        <span class="section-label">Время занятий</span>
                        <div class="times-grid">
                            ${timeInputs}
                        </div>
                    </div>
                ` : '<p style="color: var(--text-muted); font-size: 13px; margin: 12px 0;">Выберите дни недели</p>'}
            </div>
        `;
    }).join('');
    
    // Set select values after render
    setTimeout(() => {
        currentLocationSchedules.forEach((schedule, index) => {
            const select = document.getElementById(`loc-select-${index}`);
            if (select && schedule.location_id) {
                select.value = schedule.location_id;
            }
        });
    }, 10);
}

function collectLocationSchedules() {
    return currentLocationSchedules.map(s => ({
        id: s.id,
        location_id: s.location_id,
        group_id: s.group_id,
        days: s.days.join(','),
        times: JSON.stringify(s.times),
        duration: s.duration,
        is_primary: s.is_primary
    }));
}

// Handle location select change
function handleLocationSelect(index, value) {
    if (value === '__new__') {
        // Show new location input
        document.getElementById(`new-loc-${index}`).style.display = 'block';
        document.getElementById(`loc-select-${index}`).value = '';
        setTimeout(() => document.getElementById(`new-loc-name-${index}`).focus(), 100);
    } else {
        updateLocationField(index, 'location_id', value ? parseInt(value) : null);
    }
}

// Create new location
async function createNewLocation(index) {
    const nameInput = document.getElementById(`new-loc-name-${index}`);
    const name = nameInput.value.trim();
    
    if (!name) {
        showNotification('Введите название зала', 'error');
        return;
    }
    
    try {
        const res = await fetch(`${API}/api/locations/create`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                initData,
                location: {name: name}
            })
        });
        
        const result = await res.json();
        
        if (result.success) {
            // Add to available locations
            availableLocations.push({id: result.id, name: name});
            
            // Update schedule with new location
            currentLocationSchedules[index].location_id = result.id;
            
            // Re-render
            renderLocationSchedules();
            
            showNotification('Зал создан!', 'success');
        } else {
            showNotification('Ошибка создания зала', 'error');
        }
    } catch (e) {
        console.error('Create location error:', e);
        showNotification('Ошибка сети', 'error');
    }
}

// Cancel new location creation
function cancelNewLocation(index) {
    document.getElementById(`new-loc-${index}`).style.display = 'none';
    document.getElementById(`new-loc-name-${index}`).value = '';
    document.getElementById(`loc-select-${index}`).value = '';
}

// Load locations for select
async function loadLocationsForSelect() {
    try {
        const [locationsResponse, groupsResponse] = await Promise.all([
            fetch(`${API}/api/locations`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({initData})
            }),
            fetch(`${API}/api/training-groups`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({initData})
            })
        ]);
        availableLocations = await locationsResponse.json();
        availableGroups = groupsResponse.ok ? await groupsResponse.json() : [];
        renderLocationSchedules();
    } catch (e) {
        console.error('Load locations error:', e);
    }
}

async function openAddStudent() {
    editingStudentId = null;
    document.getElementById('student-form-title').textContent = 'Новый ученик';
    document.getElementById('student-form').reset();
    
    // Load locations first
    await loadLocationsForSelect();
    
    // Init with default schedule
    initLocationSchedules();
    
    // Load coaches for admin
    await loadCoaches();
    renderCoachSelect();
    
    navigate('student-form');
}

// Toggle unlimited lessons
function togglePayUnlimited(checked) {
    const countGroup = document.getElementById('pay-count-group');
    const countInput = document.getElementById('pay-count');
    const hint = document.getElementById('pay-unlimited-hint');
    if (checked) {
        countGroup.style.display = 'none';
        countInput.value = '';
        countInput.disabled = true;
        countInput.required = false;
        if (hint) hint.style.display = 'block';
    } else {
        countGroup.style.display = 'block';
        countInput.disabled = false;
        countInput.required = true;
        countInput.value = countInput.value || '8';
        if (hint) hint.style.display = 'none';
    }
}

async function openEditStudent(studentId) {
    editingStudentId = studentId;
    document.getElementById('student-form-title').textContent = 'Редактирование';
    
    // Load locations first
    await loadLocationsForSelect();
    
    try {
        const res = await fetch(`${API}/api/students/${studentId}`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData})
        });
        
        const student = await res.json();
        
        // Fill basic fields
        document.getElementById('st-name').value = student.name || '';
        document.getElementById('st-nickname').value = student.nickname || '';
        document.getElementById('st-phone').value = student.phone || '';
        document.getElementById('st-parent-phone').value = student.parent_phone || '';
        document.getElementById('st-age').value = student.age || '';
        document.getElementById('st-notes').value = student.notes || '';
        
        // Load coaches for admin
        await loadCoaches();
        renderCoachSelect();
        
        // Set coach if admin
        const coachSelect = document.getElementById('st-coach');
        if (coachSelect && student.coach_id) {
            coachSelect.value = student.coach_id;
        }
        
        // Init schedules
        if (student.schedules && student.schedules.length > 0) {
            initLocationSchedules(student.schedules);
        } else {
            // Fallback to legacy data
            const legacySchedule = {
                id: null,
                location_id: student.location_id,
                days: student.lesson_days ? student.lesson_days.split(',').map(d => parseInt(d.trim())) : [1, 3],
                times: student.lesson_times ? JSON.parse(student.lesson_times) : {"1": "18:00", "3": "18:00"},
                duration: 90,
                is_primary: true
            };
            initLocationSchedules([legacySchedule]);
        }
        
        navigate('student-form');
    } catch (e) {
        console.error('Edit student error:', e);
        showNotification('Ошибка загрузки', 'error');
    }
}

async function saveStudent() {
    const schedules = collectLocationSchedules().filter(s => s.location_id && s.days);
    if (schedules.length === 0) {
        showNotification('Добавьте хотя бы одно расписание с залом и днями', 'error');
        return;
    }

    const studentName = document.getElementById('st-name').value.trim();
    if (!studentName) {
        showNotification('Введите имя ученика', 'error');
        return;
    }

    const studentData = {
        name: studentName,
        nickname: document.getElementById('st-nickname').value || null,
        phone: document.getElementById('st-phone').value || null,
        parent_phone: document.getElementById('st-parent-phone').value || null,
        age: document.getElementById('st-age').value ? parseInt(document.getElementById('st-age').value) : null,
        notes: document.getElementById('st-notes').value || null,
        schedules: schedules
    };
    
    // Add coach_id for admin
    const coachSelect = document.getElementById('st-coach');
    if (coachSelect && coachSelect.style.display !== 'none') {
        studentData.coach_id = parseInt(coachSelect.value);
    }
    
    try {
        const url = editingStudentId 
            ? `${API}/api/students/${editingStudentId}/update`
            : `${API}/api/students/create`;
        
        const res = await fetch(url, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData, student: studentData})
        });
        
        const result = await res.json();
        
        if (result.success) {
            showNotification(editingStudentId ? 'Сохранено!' : 'Ученик добавлен!', 'success');
            goBack();
            if (currentScreen === 'students') {
                loadStudents();
            }
        } else if (result.error === 'coach_not_found') {
            showNotification('Тренер не найден', 'error');
        } else {
            showNotification('Ошибка сохранения', 'error');
        }
    } catch (e) {
        console.error('Save student error:', e);
        showNotification('Ошибка сети', 'error');
    }
}

// Update renderStudentDetail to show multiple locations
function renderStudentDetailLocations(student) {
    if (!student.schedules || student.schedules.length === 0) {
        // Fallback to legacy display
        return `
            <div class="detail-locations">
                <div class="detail-location-item primary">
                    <div class="detail-location-icon">📍</div>
                    <div class="detail-location-info">
                        <div class="detail-location-name">${escapeHtml(student.location || 'Зал Break Wave')}</div>
                        <div class="detail-location-schedule">${formatDays(student.lesson_days)} ${formatTimes(student.lesson_times)}</div>
                    </div>
                </div>
            </div>
        `;
    }
    
    return `
        <div class="detail-locations">
            ${student.schedules.map(schedule => `
                <div class="detail-location-item ${schedule.is_primary ? 'primary' : ''}">
                    <div class="detail-location-icon">📍</div>
                    <div class="detail-location-info">
                        <div class="detail-location-name">${escapeHtml(schedule.location_name || 'Зал')}</div>
                        <div class="detail-location-schedule">${formatDays(schedule.days)} ${formatTimes(schedule.times)}${schedule.group_name ? ` · ${escapeHtml(schedule.group_name)}` : ''}</div>
                    </div>
                    ${schedule.is_primary ? '<span class="detail-location-primary-badge">ОСНОВНОЙ</span>' : ''}
                </div>
            `).join('')}
        </div>
    `;
}

// Helper functions
function formatDays(daysStr) {
    if (!daysStr) return '';
    const dayNames = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'];
    return daysStr.split(',').map(d => dayNames[parseInt(d.trim())]).join(', ');
}

function formatTimes(timesStr) {
    if (!timesStr) return '';
    try {
        const times = typeof timesStr === 'string' ? JSON.parse(timesStr) : timesStr;
        const uniqueTimes = [...new Set(Object.values(times))];
        return escapeHtml(uniqueTimes.join(', '));
    } catch {
        return '';
    }
}

function getStudentRemainingLessons(student) {
    if (student.is_unlimited) {
        return Infinity;
    }
    if (student.lessons_remaining !== undefined && student.lessons_remaining !== null) {
        return student.lessons_remaining;
    }
    return student.lessons_count || 0;
}

function getStudentLessonsDisplay(student) {
    if (student.is_unlimited) {
        return '♾️ Безлимит';
    }

    const remaining = getStudentRemainingLessons(student);
    const total = student.lessons_count || 0;
    return total > 0 ? `${remaining}/${total}` : `${remaining}`;
}

function getStudentLessonsMeta(student) {
    if (student.is_unlimited) {
        return '♾️ Безлимитный абонемент';
    }

    const remaining = getStudentRemainingLessons(student);
    if (remaining <= 0) return 'Нет занятий';
    return formatLessonCount(remaining);
}

window.editStudent = openEditStudent;


// === Finance ===

let currentFinancePeriod = 'month';

async function loadFinance() {
    const container = document.getElementById('finance-content');
    container.innerHTML = `
        <div class="loading-container">
            <div class="spinner"></div>
            <p>Загрузка...</p>
        </div>
    `;
    
    try {
        // Load summary
        const summaryRes = await fetch(`${API}/api/finance/summary`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData, period: currentFinancePeriod})
        });
        const summary = await summaryRes.json();
        
        // Load debtors
        const debtorsRes = await fetch(`${API}/api/finance/debtors`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData})
        });
        const debtors = await debtorsRes.json();
        
        renderFinance(summary, debtors);
    } catch (e) {
        console.error('Finance load error:', e);
        renderScreenState(container, 'Не удалось загрузить финансовые данные.', {retry: 'loadFinance()'});
    }
}

function switchFinancePeriod(period, btn) {
    currentFinancePeriod = period;
    
    // Update tabs
    btn.parentElement.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    btn.classList.add('active');
    
    loadFinance();
}

function renderFinance(summary, debtors) {
    const container = document.getElementById('finance-content');
    
    // Summary cards
    const periodLabel = currentFinancePeriod === 'month' ? 'за месяц' : 
                        currentFinancePeriod === 'year' ? 'за год' : 'всего';
    
    // By coach chart
    let byCoachHtml = '';
    if (summary.by_coach && summary.by_coach.length > 1) {
        byCoachHtml = summary.by_coach.map(c => `
            <div class="finance-row">
                <span class="label">${escapeHtml(c.coach_name)}</span>
                <span class="value positive">${c.revenue.toLocaleString()} Br</span>
            </div>
        `).join('');
    }
    
    // By location chart
    let byLocationHtml = '';
    if (summary.by_location && summary.by_location.length > 0) {
        byLocationHtml = summary.by_location.map(l => `
            <div class="finance-row">
                <span class="label">${escapeHtml(l.location_name)}</span>
                <span class="value positive">${l.revenue.toLocaleString()} Br</span>
            </div>
        `).join('');
    }
    
    // Monthly trend
    let trendHtml = '';
    if (summary.monthly_trend) {
        const maxRevenue = Math.max(...summary.monthly_trend.map(m => m.revenue), 1);
        trendHtml = summary.monthly_trend.map(m => `
            <div class="trend-item">
                <span class="trend-month">${formatMonthLabel(m.month)}</span>
                <div class="trend-bar-wrapper">
                    <div class="trend-bar" style="height: ${Math.max(10, (m.revenue / maxRevenue) * 100)}px"></div>
                </div>
                <span class="trend-count">${m.revenue >= 1000 ? (m.revenue / 1000).toFixed(1) + 'k' : m.revenue}</span>
            </div>
        `).join('');
    }
    
    const attentionItems = Array.isArray(debtors.items) ? debtors.items : [];
    const debtorsHtml = attentionItems.length ? `
        <div class="finance-section">
            <h3>Нужно внимание (${attentionItems.length})</h3>
            ${attentionItems.map((item) => `
                <button type="button" class="debtor-item ${item.severity}" onclick="openStudentDetail(${item.id})">
                    <div class="debtor-info">
                        <div class="debtor-name">${escapeHtml(item.name)}</div>
                        <div class="debtor-meta">
                            ${escapeHtml(item.detail)}
                            ${item.reasons?.length > 1 ? ` · ещё ${item.reasons.length - 1}` : ''}
                        </div>
                    </div>
                    <span class="debtor-badge ${item.severity}">${escapeHtml(item.label)}</span>
                </button>
            `).join('')}
        </div>
    ` : '';
    
    container.innerHTML = `
        <p class="period-caption">
            ${getPeriodCaption(currentFinancePeriod)}
            ${currentCoach?.is_admin ? ' · Сводка школы; должники — ваши ученики' : ''}
        </p>
        <div class="finance-summary-cards">
            <div class="finance-card revenue">
                <span class="finance-value">${summary.summary.total_revenue.toLocaleString()} Br</span>
                <span class="finance-label">Доход ${periodLabel}</span>
            </div>
            <div class="finance-card pending">
                <span class="finance-value">${summary.summary.pending_amount.toLocaleString()} Br</span>
                <span class="finance-label">Ожидается</span>
            </div>
            <div class="finance-card overdue">
                <span class="finance-value">${summary.summary.overdue_total.toLocaleString()} Br</span>
                <span class="finance-label">Просрочено</span>
            </div>
            <div class="finance-card">
                <span class="finance-value">${debtors.counts.total}</span>
                <span class="finance-label">Должников</span>
            </div>
        </div>
        
        ${byCoachHtml ? `
            <div class="finance-section">
                <h3>По тренерам</h3>
                ${byCoachHtml}
            </div>
        ` : ''}
        
        ${byLocationHtml ? `
            <div class="finance-section">
                <h3>По залам</h3>
                ${byLocationHtml}
            </div>
        ` : ''}
        
        <div class="finance-section">
            <h3>Динамика доходов за 6 месяцев</h3>
            <div class="trend-chart">${trendHtml}</div>
        </div>
        
        ${debtorsHtml}
    `;
}

// === Family cabinet and shared request queue ===

async function apiPost(path, payload = {}) {
    const response = await fetch(`${API}${path}`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({initData, ...payload})
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
        const error = new Error(data.message || data.error || `Ошибка ${response.status}`);
        error.payload = data;
        throw error;
    }
    return data;
}

function initializeRegistrationSchedule() {
    if (!registrationScheduleRows.length) {
        registrationScheduleRows = [
            {day: '0', time: '18:00'},
            {day: '2', time: '18:00'}
        ];
    }
    renderRegistrationSchedule();
}

function addRegistrationScheduleRow() {
    registrationScheduleRows.push({day: '0', time: '18:00'});
    renderRegistrationSchedule();
}

function updateRegistrationScheduleRow(index, field, value) {
    if (registrationScheduleRows[index]) registrationScheduleRows[index][field] = value;
}

function removeRegistrationScheduleRow(index) {
    if (registrationScheduleRows.length <= 1) {
        showNotification('Нужен хотя бы один день тренировки', 'error');
        return;
    }
    registrationScheduleRows.splice(index, 1);
    renderRegistrationSchedule();
}

function renderRegistrationSchedule() {
    const container = document.getElementById('registration-schedule-list');
    if (!container) return;
    const weekdays = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье'];
    container.innerHTML = registrationScheduleRows.map((row, index) => `
        <div class="schedule-editor-row">
            <select aria-label="День тренировки" onchange="updateRegistrationScheduleRow(${index}, 'day', this.value)">
                ${weekdays.map((name, day) => `<option value="${day}" ${String(day) === row.day ? 'selected' : ''}>${name}</option>`).join('')}
            </select>
            <input type="time" aria-label="Время тренировки" value="${escapeHtml(row.time)}"
                onchange="updateRegistrationScheduleRow(${index}, 'time', this.value)">
            <button type="button" class="btn-icon" aria-label="Удалить день" onclick="removeRegistrationScheduleRow(${index})">×</button>
        </div>
    `).join('');
}

async function submitParentRegistration() {
    if (!guestInvitation) return;
    const proposedSchedule = registrationScheduleRows.map((row, index) => ({
        days: row.day,
        times: {[row.day]: row.time},
        duration: 90,
        is_primary: index === 0
    }));
    const submitButton = document.querySelector('#parent-registration-form button[type="submit"]');
    if (submitButton) submitButton.disabled = true;
    try {
        await apiPost('/api/parent/register', {
            invite_token: guestInvitation.invite_token,
            parent: {
                full_name: document.getElementById('reg-parent-name').value.trim(),
                phone: document.getElementById('reg-parent-phone').value.trim()
            },
            child: {
                name: document.getElementById('reg-child-name').value.trim(),
                birthday: document.getElementById('reg-child-birthday').value,
                phone: document.getElementById('reg-child-phone').value.trim()
            },
            proposed_schedule: proposedSchedule
        });
        currentRole = 'parent';
        guestInvitation = null;
        showNotification('Анкета отправлена тренерам');
        showScreen('parent');
    } catch (error) {
        showNotification(error.message || 'Не удалось отправить анкету', 'error');
    } finally {
        if (submitButton) submitButton.disabled = false;
    }
}

async function loadParentContext() {
    const container = document.getElementById('parent-content');
    if (!container) return;
    renderScreenState(container, 'Загружаем семейный кабинет…', {icon: 'BW'});
    try {
        parentData = await apiPost('/api/parent/context');
        document.getElementById('parent-greeting').textContent = `Здравствуйте, ${parentData.parent.full_name}`;
        renderParentContext();
    } catch (error) {
        renderScreenState(container, error.message || 'Не удалось загрузить данные', {
            retry: 'loadParentContext()'
        });
    }
}

function parentPaymentStatus(invoice) {
    const labels = {
        paid: 'Оплачено',
        pending: 'Ожидает оплаты',
        overdue: 'Есть задолженность',
        reported: 'На проверке',
        awaiting_receipt: 'Нужен чек',
        rejected: 'Отклонено',
        written_off: 'Списано'
    };
    return labels[invoice?.status] || 'Ожидает';
}

function renderParentContext() {
    const container = document.getElementById('parent-content');
    if (!container || !parentData) return;
    const studentsHtml = parentData.students.map(student => {
        const invoice = student.invoice;
        const schedules = student.schedules.length
            ? student.schedules.map(item => `
                <div class="family-row">
                    <span>${escapeHtml(formatDays(item.days))} · ${escapeHtml(formatTimes(JSON.stringify(item.times)))}</span>
                    <small>${escapeHtml(item.location)}${item.group ? ` · ${escapeHtml(item.group)}` : ''}</small>
                </div>
            `).join('')
            : '<p class="muted-copy">Расписание назначит тренер.</p>';
        const makeups = student.makeups.length
            ? student.makeups.map(item => `
                <div class="makeup-item">
                    <div>
                        <strong>До ${formatDate(item.expires_at)}</strong>
                        <small>${item.status === 'scheduled'
                            ? `Назначено: ${formatDate(item.scheduled_date)} ${escapeHtml(item.scheduled_time || '')}`
                            : item.status === 'requested' ? 'Ожидает подтверждения' : 'Можно запросить день'}</small>
                        ${item.rejection_reason ? `<small class="danger-copy">${escapeHtml(item.rejection_reason)}</small>` : ''}
                    </div>
                    ${item.status === 'available' ? `<button type="button" class="btn-secondary compact-btn" onclick="requestMakeupDate(${item.id})">Выбрать день</button>` : ''}
                </div>
            `).join('')
            : '<p class="muted-copy">Активных отработок нет.</p>';
        const paymentActions = invoice.stored_status === 'paid'
            ? ''
            : invoice.stored_status === 'reported'
                ? '<p class="status-note success-copy">Оплата отправлена на проверку.</p>'
                : invoice.stored_status === 'awaiting_receipt'
                    ? '<p class="status-note warning-copy">Пришлите чек фотографией в чат с ботом.</p>'
                    : `
                        <div class="family-actions">
                            <button type="button" class="btn-primary" onclick="reportParentPayment(${invoice.id}, 'online')">Оплатил онлайн</button>
                            <button type="button" class="btn-secondary" onclick="reportParentPayment(${invoice.id}, 'cash')">Оплатил наличными</button>
                        </div>
                    `;
        return `
            <article class="family-student-card">
                <div class="family-card-head">
                    <div>
                        <span class="page-eyebrow">${escapeHtml(student.coach_name || 'Тренер назначается')}</span>
                        <h3>${escapeHtml(student.name)}</h3>
                    </div>
                    <span class="balance-pill">${student.lessons_remaining} / ${student.lessons_count}</span>
                </div>

                <section class="family-panel">
                    <div class="section-heading-row">
                        <h4>Расписание</h4>
                        <button type="button" class="section-link" onclick="requestParentSchedule(${student.id})">Изменить</button>
                    </div>
                    ${schedules}
                    <label class="family-toggle">
                        <span><strong>Напоминание за сутки</strong><small>Ежедневно в 18:00, только перед тренировкой</small></span>
                        <input type="checkbox" ${student.training_reminders_enabled ? 'checked' : ''}
                            onchange="toggleParentReminders(${student.id}, this.checked)">
                    </label>
                </section>

                <section class="family-panel invoice-panel ${invoice.status === 'overdue' ? 'is-overdue' : ''}">
                    <div class="family-card-head">
                        <div>
                            <span class="page-eyebrow">Абонемент ${formatMonthLabel(String(invoice.period_start || '').slice(0, 7))}</span>
                            <h4>${escapeHtml(invoice.tariff?.label || 'Выберите тариф')}</h4>
                        </div>
                        <span class="status-chip status-${escapeHtml(invoice.status)}">${parentPaymentStatus(invoice)}</span>
                    </div>
                    <div class="money-breakdown">
                        <span>Стоимость <strong>${invoice.base_amount} Br</strong></span>
                        <span>Доплата <strong>${invoice.late_fee_amount} Br</strong></span>
                        <span class="money-total">Итого <strong>${invoice.amount} Br</strong></span>
                    </div>
                    ${invoice.rejection_reason ? `<p class="danger-copy">Причина: ${escapeHtml(invoice.rejection_reason)}</p>` : ''}
                    ${!['reported', 'awaiting_receipt', 'paid'].includes(invoice.stored_status) ? `
                        <label class="inline-select">
                            <span>Тариф до 5 числа</span>
                            <select onchange="chooseParentTariff(${student.id}, this.value)">
                                ${Object.entries(parentData.tariffs).map(([code, tariff]) =>
                                    `<option value="${code}" ${code === invoice.tariff_code ? 'selected' : ''}>${escapeHtml(tariff.label)} · ${tariff.price} Br</option>`
                                ).join('')}
                            </select>
                        </label>
                    ` : ''}
                    ${paymentActions}
                </section>

                <section class="family-panel">
                    <div class="section-heading-row">
                        <h4>Отработки</h4>
                        <span class="count-badge">${student.makeups.length}</span>
                    </div>
                    ${makeups}
                </section>
            </article>
        `;
    }).join('');

    const pendingRegistrations = parentData.registration_requests.filter(item => item.status !== 'approved');
    container.innerHTML = `
        ${pendingRegistrations.map(item => `
            <div class="family-status-card ${item.status === 'rejected' ? 'is-rejected' : ''}">
                <strong>${escapeHtml(item.child_name)}</strong>
                <span>${item.status === 'pending' ? 'Анкета ожидает подтверждения тренера' : 'Анкета отклонена'}</span>
                ${item.rejection_reason ? `<small>Причина: ${escapeHtml(item.rejection_reason)}</small>` : ''}
            </div>
        `).join('')}
        ${studentsHtml || (pendingRegistrations.length ? '' : `
            <div class="family-status-card">
                <strong>Пока нет детей в кабинете</strong>
                <span>Откройте персональную ссылку-приглашение от тренера.</span>
            </div>
        `)}
        <p class="privacy-note">Обязательные сообщения об оплате, отменах и решениях тренера остаются включёнными.</p>
    `;
}

async function toggleParentReminders(studentId, enabled) {
    try {
        await apiPost('/api/parent/reminders', {student_id: studentId, enabled});
        showNotification(enabled ? 'Напоминания включены' : 'Напоминания отключены');
    } catch (error) {
        showNotification(error.message, 'error');
        await loadParentContext();
    }
}

async function chooseParentTariff(studentId, tariffCode) {
    try {
        await apiPost('/api/parent/tariff', {student_id: studentId, tariff_code: tariffCode});
        showNotification('Тариф сохранён');
        await loadParentContext();
    } catch (error) {
        showNotification(error.message, 'error');
        await loadParentContext();
    }
}

async function reportParentPayment(paymentId, method) {
    try {
        const result = await apiPost(`/api/parent/payments/${paymentId}/report`, {
            payment_method: method
        });
        showNotification(result.message);
        await loadParentContext();
    } catch (error) {
        showNotification(error.message, 'error');
    }
}

function parseSimpleSchedule(value) {
    const weekdayMap = {
        'пн': '0', 'понедельник': '0',
        'вт': '1', 'вторник': '1',
        'ср': '2', 'среда': '2',
        'чт': '3', 'четверг': '3',
        'пт': '4', 'пятница': '4',
        'сб': '5', 'суббота': '5',
        'вс': '6', 'воскресенье': '6'
    };
    return String(value || '').split(',').map(part => {
        const match = part.trim().toLowerCase().match(/^([а-яё]+)\s+(\d{1,2}:\d{2})$/i);
        if (!match || weekdayMap[match[1]] === undefined) return null;
        const day = weekdayMap[match[1]];
        return {days: day, times: {[day]: match[2]}, duration: 90};
    }).filter(Boolean);
}

async function requestParentSchedule(studentId) {
    const value = window.prompt('Новое расписание, например: Пн 18:00, Ср 18:00');
    if (!value) return;
    const proposedSchedule = parseSimpleSchedule(value);
    if (!proposedSchedule.length) {
        showNotification('Формат: Пн 18:00, Ср 18:00', 'error');
        return;
    }
    try {
        await apiPost('/api/parent/schedule-request', {
            student_id: studentId,
            proposed_schedule: proposedSchedule
        });
        showNotification('Запрос отправлен тренерам');
        await loadParentContext();
    } catch (error) {
        showNotification(error.message, 'error');
    }
}

async function requestMakeupDate(makeupId) {
    const requestedDate = window.prompt('Желаемая дата отработки в формате ГГГГ-ММ-ДД');
    if (!requestedDate) return;
    try {
        await apiPost(`/api/parent/makeups/${makeupId}/request`, {
            requested_date: requestedDate
        });
        showNotification('Запрос на отработку отправлен');
        await loadParentContext();
    } catch (error) {
        showNotification(error.message, 'error');
    }
}

async function createParentInvitation() {
    const childName = window.prompt('Предварительное ФИО ребёнка');
    if (!childName?.trim()) return;
    try {
        const result = await apiPost('/api/admin/invitations/create', {child_name: childName.trim()});
        try {
            await navigator.clipboard.writeText(result.invite_url);
            showNotification('Ссылка приглашения скопирована');
        } catch {
            window.prompt('Скопируйте персональную ссылку', result.invite_url);
        }
        if (currentScreen === 'requests') await loadAdminRequests();
    } catch (error) {
        showNotification(error.message || 'Не удалось создать приглашение', 'error');
    }
}

async function loadAdminRequests() {
    const container = document.getElementById('requests-content');
    if (!container) return;
    renderScreenState(container, 'Собираем запросы…', {icon: '◎'});
    try {
        const [requests, invitations] = await Promise.all([
            apiPost('/api/admin/requests'),
            apiPost('/api/admin/invitations')
        ]);
        renderAdminRequests(requests, invitations);
    } catch (error) {
        renderScreenState(container, error.message || 'Не удалось загрузить запросы', {
            retry: 'loadAdminRequests()'
        });
    }
}

function describeProposedSchedule(items) {
    return (items || []).map(item =>
        `${formatDays(String(item.days || ''))} · ${formatTimes(typeof item.times === 'string' ? item.times : JSON.stringify(item.times || {}))}`
    ).join('; ') || 'Не указано';
}

function syncSchoolResourceLocations() {
    const coachId = document.getElementById('school-resource-coach')?.value;
    const select = document.getElementById('school-resource-location');
    if (!select) return;
    select.value = '';
    [...select.options].forEach(option => {
        if (!option.dataset.coachId) return;
        const matches = option.dataset.coachId === coachId;
        option.disabled = !matches;
        option.hidden = !matches;
    });
}

async function updateCoachRoles(coachId) {
    const isAdmin = document.getElementById(`coach-admin-${coachId}`)?.checked || false;
    const isManager = document.getElementById(`coach-manager-${coachId}`)?.checked || false;
    try {
        await apiPost(`/api/admin/coaches/${coachId}/roles`, {
            is_admin: isAdmin,
            is_manager: isManager
        });
        showNotification('Права тренера обновлены');
        await loadAdminRequests();
    } catch (error) {
        showNotification(error.message, 'error');
    }
}

async function createSchoolLocation() {
    const name = window.prompt('Название нового зала');
    if (!name?.trim()) return;
    const address = window.prompt('Адрес зала (необязательно)') || '';
    const coachId = Number(document.getElementById('school-resource-coach')?.value || currentCoach.coach_id);
    try {
        await apiPost('/api/admin/locations/create', {
            name: name.trim(),
            address: address.trim(),
            coach_id: coachId
        });
        showNotification('Зал добавлен');
        await loadAdminRequests();
    } catch (error) {
        showNotification(error.message, 'error');
    }
}

async function createSchoolGroup() {
    const name = window.prompt('Название новой группы');
    if (!name?.trim()) return;
    const coachId = Number(document.getElementById('school-resource-coach')?.value || currentCoach.coach_id);
    const locationId = Number(document.getElementById('school-resource-location')?.value) || null;
    try {
        await apiPost('/api/training-groups/create', {
            name: name.trim(),
            coach_id: coachId,
            location_id: locationId
        });
        showNotification('Группа добавлена');
        await loadAdminRequests();
    } catch (error) {
        showNotification(error.message, 'error');
    }
}

async function cancelSchoolTraining() {
    const coachId = Number(document.getElementById('school-resource-coach')?.value || currentCoach.coach_id);
    const trainingDate = window.prompt('Дата отмены ГГГГ-ММ-ДД', new Date().toISOString().slice(0, 10));
    if (!trainingDate) return;
    const trainingTime = window.prompt('Время группы, например 18:00. Оставьте пустым, чтобы отменить все тренировки дня.', '') || '';
    const reason = window.prompt('Причина отмены, которую увидят родители')?.trim();
    if (!reason) return;
    if (!window.confirm('Отменить тренировку? Занятия не спишутся, каждому ребёнку добавится отработка.')) return;
    try {
        const result = await apiPost('/api/skip-lesson', {
            coach_id: coachId,
            date: trainingDate,
            time: trainingTime,
            reason
        });
        showNotification(`Отменено слотов: ${result.skipped}`);
    } catch (error) {
        showNotification(error.message, 'error');
    }
}

function renderAdminRequests(requests, invitations) {
    const container = document.getElementById('requests-content');
    if (!container) return;
    const total = requests.registrations.length + requests.schedules.length + requests.makeups.length + requests.payments.length;
    const resources = requests.resources || {coaches: [], locations: [], groups: []};
    const coachOptions = resources.coaches.map(coach =>
        `<option value="${coach.id}" ${coach.id === currentCoach?.coach_id ? 'selected' : ''}>${escapeHtml(coach.name || 'Тренер')}</option>`
    ).join('');
    const locationOptions = resources.locations.map(item =>
        `<option value="${item.id}" data-coach-id="${item.coach_id}" ${item.coach_id !== currentCoach?.coach_id ? 'disabled hidden' : ''}>${escapeHtml(item.name)}</option>`
    ).join('');
    const groupOptions = resources.groups.map(item =>
        `<option value="${item.id}" data-coach-id="${item.coach_id}" ${item.coach_id !== currentCoach?.coach_id ? 'disabled hidden' : ''}>${escapeHtml(item.name)}</option>`
    ).join('');
    const sections = [];

    sections.push(`
        <section class="request-section">
            <div class="section-heading-row"><h3>Структура школы</h3><span class="count-badge">${resources.locations.length} / ${resources.groups.length}</span></div>
            <p class="muted-copy">Залы и группы можно менять без создания новых разделов или ботов.</p>
            <div class="role-list">
                ${resources.coaches.map(coach => `
                    <div class="role-row">
                        <strong>${escapeHtml(coach.name || 'Тренер')}</strong>
                        <label><input id="coach-admin-${coach.id}" type="checkbox" ${coach.is_admin ? 'checked' : ''} ${coach.is_configured_owner ? 'disabled' : ''}> Админ</label>
                        <label><input id="coach-manager-${coach.id}" type="checkbox" ${coach.is_manager ? 'checked' : ''} ${coach.is_configured_owner ? 'disabled' : ''}> Руководитель</label>
                        ${coach.is_configured_owner ? '<small>Главный</small>' : `<button class="btn-secondary compact-btn" onclick="updateCoachRoles(${coach.id})">Сохранить</button>`}
                    </div>
                `).join('')}
            </div>
            <label class="inline-select"><span>Тренер</span><select id="school-resource-coach" onchange="syncSchoolResourceLocations()">${coachOptions}</select></label>
            <label class="inline-select"><span>Зал для новой группы</span><select id="school-resource-location"><option value="">Без зала</option>${locationOptions}</select></label>
            <div class="family-actions">
                <button class="btn-secondary" onclick="createSchoolLocation()">+ Новый зал</button>
                <button class="btn-primary" onclick="createSchoolGroup()">+ Новая группа</button>
                <button class="btn-secondary danger-action" onclick="cancelSchoolTraining()">Отменить тренировку</button>
            </div>
        </section>
    `);

    sections.push(`
        <section class="request-section">
            <div class="section-heading-row"><h3>Новые регистрации</h3><span class="count-badge">${requests.registrations.length}</span></div>
            ${requests.registrations.map(item => `
                <article class="request-card">
                    <span class="page-eyebrow">Родитель: ${escapeHtml(item.parent_name)} · ${escapeHtml(item.parent_phone)}</span>
                    <h4>${escapeHtml(item.child_name)}</h4>
                    <p>${formatDate(item.birthday)}${item.child_phone ? ` · ${escapeHtml(item.child_phone)}` : ''}</p>
                    <small>${escapeHtml(describeProposedSchedule(item.proposed_schedule))}</small>
                    <label class="inline-select"><span>Назначить тренера</span><select id="registration-coach-${item.id}" onchange="syncRegistrationResources(${item.id})">${coachOptions}</select></label>
                    <label class="inline-select"><span>Зал</span><select id="registration-location-${item.id}"><option value="">Назначить позже</option>${locationOptions}</select></label>
                    <label class="inline-select"><span>Группа</span><select id="registration-group-${item.id}"><option value="">Без группы</option>${groupOptions}</select></label>
                    <label class="inline-select"><span>Начало занятий</span><input id="registration-start-${item.id}" type="date" value="${new Date().toISOString().slice(0, 10)}"></label>
                    <div class="family-actions">
                        <button class="btn-primary" onclick="reviewRegistration(${item.id}, 'approve')">Подтвердить</button>
                        <button class="btn-secondary danger-action" onclick="reviewRegistration(${item.id}, 'reject')">Отклонить</button>
                    </div>
                </article>
            `).join('') || '<p class="muted-copy">Новых анкет нет.</p>'}
        </section>
    `);
    sections.push(`
        <section class="request-section">
            <div class="section-heading-row"><h3>Расписание</h3><span class="count-badge">${requests.schedules.length}</span></div>
            ${requests.schedules.map(item => `
                <article class="request-card">
                    <h4>${escapeHtml(item.student_name)}</h4>
                    <p>${escapeHtml(describeProposedSchedule(item.proposed_schedule))}</p>
                    <div class="family-actions">
                        <button class="btn-primary" onclick="reviewScheduleRequest(${item.id}, 'approve')">Подтвердить</button>
                        <button class="btn-secondary danger-action" onclick="reviewScheduleRequest(${item.id}, 'reject')">Отклонить</button>
                    </div>
                </article>
            `).join('') || '<p class="muted-copy">Запросов на изменение нет.</p>'}
        </section>
    `);
    sections.push(`
        <section class="request-section">
            <div class="section-heading-row"><h3>Отработки</h3><span class="count-badge">${requests.makeups.length}</span></div>
            ${requests.makeups.map(item => `
                <article class="request-card">
                    <h4>${escapeHtml(item.student_name)}</h4>
                    <p>Желаемая дата: ${formatDate(item.requested_date)} · право до ${formatDate(item.expires_at)}</p>
                    <label class="inline-select"><span>Зал</span><select id="makeup-location-${item.id}"><option value="">Без зала</option>${resources.locations.filter(location => location.coach_id === item.coach_id).map(location => `<option value="${location.id}">${escapeHtml(location.name)}</option>`).join('')}</select></label>
                    <label class="inline-select"><span>Группа</span><select id="makeup-group-${item.id}"><option value="">Без группы</option>${resources.groups.filter(group => group.coach_id === item.coach_id).map(group => `<option value="${group.id}">${escapeHtml(group.name)}</option>`).join('')}</select></label>
                    <div class="family-actions">
                        <button class="btn-primary" onclick="reviewMakeupRequest(${item.id}, 'approve', '${escapeHtml(item.requested_date || '')}')">Назначить</button>
                        <button class="btn-secondary danger-action" onclick="reviewMakeupRequest(${item.id}, 'reject')">Отклонить</button>
                    </div>
                </article>
            `).join('') || '<p class="muted-copy">Запросов на отработку нет.</p>'}
        </section>
    `);
    sections.push(`
        <section class="request-section">
            <div class="section-heading-row"><h3>Оплаты и чеки</h3><span class="count-badge">${requests.payments.length}</span></div>
            ${requests.payments.map(item => `
                <article class="request-card">
                    <span class="page-eyebrow">${item.method === 'cash' ? 'Наличные' : 'Онлайн'}${item.receipt_attached ? ' · чек прикреплён в чате' : ''}</span>
                    <h4>${escapeHtml(item.student_name)}</h4>
                    <p>${item.amount} Br</p>
                    ${item.method === 'cash' ? `<label class="inline-select"><span>Кто принял наличные</span><select id="payment-receiver-${item.id}">${coachOptions}</select></label>` : ''}
                    <div class="family-actions">
                        <button class="btn-primary" onclick="reviewParentPayment(${item.id}, 'approve')">Подтвердить</button>
                        <button class="btn-secondary danger-action" onclick="reviewParentPayment(${item.id}, 'reject')">Отклонить</button>
                    </div>
                </article>
            `).join('') || '<p class="muted-copy">Оплат на проверке нет.</p>'}
        </section>
    `);
    sections.push(`
        <section class="request-section invite-history">
            <div class="section-heading-row"><h3>Приглашения</h3><span class="count-badge">${invitations.length}</span></div>
            ${invitations.slice(0, 10).map(item => `
                <article class="invite-row">
                    <div><strong>${escapeHtml(item.child_name)}</strong><small>${item.status === 'active' ? `до ${formatDate(item.expires_at)}` : item.status}</small></div>
                    ${item.status === 'active' ? `<button class="btn-secondary compact-btn" onclick="copyInvitation('${escapeHtml(item.invite_url)}')">Копировать</button>` : ''}
                </article>
            `).join('') || '<p class="muted-copy">Приглашений пока нет.</p>'}
        </section>
    `);
    container.innerHTML = `
        <div class="request-summary"><strong>${total}</strong><span>запросов требуют решения</span></div>
        ${sections.join('')}
    `;
}

async function copyInvitation(url) {
    try {
        await navigator.clipboard.writeText(url);
        showNotification('Ссылка скопирована');
    } catch {
        window.prompt('Скопируйте ссылку', url);
    }
}

function rejectionReason() {
    return window.prompt('Укажите причину — родитель увидит её в кабинете')?.trim() || '';
}

function syncRegistrationResources(requestId) {
    const coachId = document.getElementById(`registration-coach-${requestId}`)?.value;
    ['location', 'group'].forEach(type => {
        const select = document.getElementById(`registration-${type}-${requestId}`);
        if (!select) return;
        select.value = '';
        [...select.options].forEach(option => {
            if (!option.dataset.coachId) return;
            const matches = option.dataset.coachId === coachId;
            option.disabled = !matches;
            option.hidden = !matches;
        });
    });
}

async function reviewRegistration(requestId, decision) {
    const payload = {decision};
    if (decision === 'approve') {
        payload.coach_id = Number(document.getElementById(`registration-coach-${requestId}`)?.value || currentCoach.coach_id);
        payload.location_id = Number(document.getElementById(`registration-location-${requestId}`)?.value) || null;
        payload.group_id = Number(document.getElementById(`registration-group-${requestId}`)?.value) || null;
        payload.training_start_date = document.getElementById(`registration-start-${requestId}`)?.value;
    } else {
        payload.reason = rejectionReason();
        if (!payload.reason) return;
    }
    try {
        await apiPost(`/api/admin/registrations/${requestId}/review`, payload);
        showNotification(decision === 'approve' ? 'Ребёнок зарегистрирован' : 'Отказ отправлен родителю');
        await loadAdminRequests();
    } catch (error) {
        showNotification(error.message, 'error');
    }
}

async function reviewScheduleRequest(requestId, decision) {
    const payload = {decision};
    if (decision === 'reject') {
        payload.reason = rejectionReason();
        if (!payload.reason) return;
    }
    try {
        await apiPost(`/api/admin/schedule-requests/${requestId}/review`, payload);
        showNotification('Решение сохранено');
        await loadAdminRequests();
    } catch (error) {
        showNotification(error.message, 'error');
    }
}

async function reviewMakeupRequest(requestId, decision, proposedDate = '') {
    const payload = {decision};
    if (decision === 'approve') {
        payload.scheduled_date = window.prompt('Дата отработки ГГГГ-ММ-ДД', proposedDate) || '';
        payload.scheduled_time = window.prompt('Время', '18:00') || '';
        payload.location_id = Number(document.getElementById(`makeup-location-${requestId}`)?.value) || null;
        payload.group_id = Number(document.getElementById(`makeup-group-${requestId}`)?.value) || null;
        if (!payload.scheduled_date || !payload.scheduled_time) return;
    } else {
        payload.reason = rejectionReason();
        if (!payload.reason) return;
    }
    try {
        await apiPost(`/api/admin/makeups/${requestId}/review`, payload);
        showNotification('Решение по отработке отправлено');
        await loadAdminRequests();
    } catch (error) {
        showNotification(error.message, 'error');
    }
}

async function reviewParentPayment(paymentId, decision) {
    const payload = {decision};
    const receiverSelect = document.getElementById(`payment-receiver-${paymentId}`);
    if (receiverSelect) payload.received_by_coach_id = Number(receiverSelect.value);
    if (decision === 'reject') {
        payload.reason = rejectionReason();
        if (!payload.reason) return;
    }
    try {
        await apiPost(`/api/admin/payments/${paymentId}/review`, payload);
        showNotification(decision === 'approve' ? 'Оплата подтверждена' : 'Отказ отправлен родителю');
        await loadAdminRequests();
    } catch (error) {
        showNotification(error.message, 'error');
    }
}

