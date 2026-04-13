/* === CRM Break Wave Mini App === */

const API = window.location.origin;
const tg = window.Telegram?.WebApp;

// Cache busting - force reload if version changed
const APP_VERSION_KEY = 'crm_bw_version';
const CURRENT_VERSION = '16'; // Version 16: Fixed SQLAlchemy eager loading for student schedules

// Check version on load
const savedVersion = localStorage.getItem(APP_VERSION_KEY);
if (savedVersion && savedVersion !== CURRENT_VERSION) {
    // Version changed - clear cache and reload
    localStorage.removeItem('crm_cached_students');
    localStorage.removeItem('crm_cached_payments');
    localStorage.removeItem('crm_cached_dashboard');
    localStorage.setItem(APP_VERSION_KEY, CURRENT_VERSION);
    window.location.reload(true);
} else {
    localStorage.setItem(APP_VERSION_KEY, CURRENT_VERSION);
}

// Data cache helpers
const DataCache = {
    STUDENTS_KEY: 'crm_cached_students',
    PAYMENTS_KEY: 'crm_cached_payments',
    DASHBOARD_KEY: 'crm_cached_dashboard',
    LAST_SYNC_KEY: 'crm_last_sync',
    
    save(key, data) {
        try {
            localStorage.setItem(key, JSON.stringify({
                data,
                timestamp: Date.now()
            }));
        } catch (e) {
            console.warn('Cache save failed:', e);
        }
    },
    
    load(key) {
        try {
            const item = localStorage.getItem(key);
            if (!item) return null;
            const parsed = JSON.parse(item);
            // Cache valid for 1 hour
            if (Date.now() - parsed.timestamp > 3600000) {
                localStorage.removeItem(key);
                return null;
            }
            return parsed.data;
        } catch (e) {
            return null;
        }
    },
    
    clear() {
        localStorage.removeItem(this.STUDENTS_KEY);
        localStorage.removeItem(this.PAYMENTS_KEY);
        localStorage.removeItem(this.DASHBOARD_KEY);
    }
};

let currentScreen = 'loading';
let screenHistory = [];
let initData = '';
let currentCoach = null;
let coaches = [];
let students = [];
let payments = [];
let calendarData = {};
let currentCalendarDate = new Date();
let editingStudentId = null;
let selectedDays = new Set([1, 3]); // Default Mon, Wed

// === Init ===
document.addEventListener('DOMContentLoaded', async () => {
    if (tg) {
        tg.ready();
        tg.expand();
        tg.requestFullscreen?.();
        tg.enableClosingConfirmation?.();
        tg.setBackgroundColor?.('#0A1628');
        tg.setHeaderColor?.('#0F2035');
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
    document.getElementById('st-sub-start')?.setAttribute('value', today);
    
    // Setup weekday selector
    setupWeekdaySelector();
    
    // Setup forms
    setupForms();
    
    // Authenticate
    await authenticate();
});

function setupWeekdaySelector() {
    const container = document.getElementById('weekdays-selector');
    if (!container) return;
    
    container.querySelectorAll('button').forEach(btn => {
        const day = parseInt(btn.dataset.day);
        if (selectedDays.has(day)) {
            btn.classList.add('active');
        }
        
        btn.addEventListener('click', () => {
            btn.classList.toggle('active');
            if (btn.classList.contains('active')) {
                selectedDays.add(day);
            } else {
                selectedDays.delete(day);
            }
        });
    });
}

function setupForms() {
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
        const res = await fetch(`${API}/api/auth`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData})
        });
        
        const data = await res.json();
        
        if (data.error === 'not_registered') {
            showScreen('auth');
            return;
        }
        
        currentCoach = data;
        
        // Store coach info for auto-fill forms
        localStorage.setItem('crm_coach_info', JSON.stringify({
            id: data.coach_id,
            first_name: data.first_name,
            username: data.username,
            is_admin: data.is_admin
        }));
        
        await loadDashboard();
        showScreen('dashboard');
    } catch (e) {
        console.error('Auth error:', e);
        showNotification('Ошибка авторизации', 'error');
    }
}

// === Navigation ===

function navigate(screen) {
    if (currentScreen !== screen && currentScreen !== 'loading') {
        screenHistory.push(currentScreen);
    }
    showScreen(screen);
}

function goBack() {
    if (screenHistory.length > 0) {
        const prev = screenHistory.pop();
        showScreen(prev);
    } else {
        showScreen('dashboard');
    }
}

function showScreen(screen) {
    currentScreen = screen;
    
    document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
    const el = document.getElementById(`screen-${screen}`);
    if (el) {
        el.classList.add('active');
    }
    
    // Scroll to top
    document.getElementById('content')?.scrollTo(0, 0);
    
    // Load data
    switch (screen) {
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
        case 'statistics':
            loadStatistics();
            break;
        case 'finance':
            loadFinance();
            break;
    }
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
    if (coaches.length < 2) return;
    
    const myBtn = document.getElementById('filter-my');
    const otherBtn = document.getElementById('filter-other');
    
    if (!myBtn || !otherBtn) return;
    
    // Find current coach and other coach
    const myCoach = coaches.find(c => c.is_current);
    const otherCoach = coaches.find(c => !c.is_current);
    
    if (myCoach) {
        myBtn.textContent = myCoach.first_name || 'Мои';
    }
    
    if (otherCoach) {
        otherBtn.textContent = otherCoach.first_name || 'Брат';
    }
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
    const isAdmin = coachInfo?.is_admin || currentCoach?.is_admin;
    
    // Show current coach info (auto-filled from Telegram)
    select.style.display = 'none';
    display.style.display = 'block';
    display.innerHTML = `
        <span class="coach-name">${escapeHtml(coachName)}</span>
        ${coachUsername ? `<span class="coach-username">@${escapeHtml(coachUsername)}</span>` : ''}
    `;
    select.value = coachId;
    
    // For admin with multiple coaches - show dropdown instead
    if (coaches.length > 1 && isAdmin) {
        select.style.display = 'block';
        display.style.display = 'none';
        select.innerHTML = coaches.map(c => {
            const isCurrent = c.is_current ? ' (вы)' : '';
            const username = c.username ? ` @${c.username}` : '';
            return `<option value="${c.id}">${escapeHtml(c.first_name || 'Без имени')}${username}${isCurrent}</option>`;
        }).join('');
        
        if (coachId) {
            select.value = coachId;
        }
    }
}

// === Dashboard ===

async function loadDashboard() {
    // First, show cached data if available
    const cached = DataCache.load(DataCache.DASHBOARD_KEY);
    if (cached) {
        renderDashboard(cached);
    }
    
    try {
        const res = await fetch(`${API}/api/dashboard`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData})
        });
        
        const data = await res.json();
        
        // Save to cache
        DataCache.save(DataCache.DASHBOARD_KEY, data);
        
        // Render fresh data
        renderDashboard(data);
    } catch (e) {
        console.error('Dashboard load error:', e);
        if (!cached) {
            showNotification('Ошибка загрузки данных', 'error');
        }
    }
}

async function renderDashboard(data) {
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
            <div class="alert-item" onclick="navigate('students')">
                <div class="alert-icon">${a.icon}</div>
                <div class="alert-content">
                    <div class="alert-title">${a.title}</div>
                    <div class="alert-subtitle">${a.subtitle}</div>
                </div>
            </div>
        `).join('');
    }
}

// === Students ===

let currentStudentsFilter = 'all'; // 'all', 'my', 'other'

async function loadStudents() {
    // Load coaches first (for displaying coach info)
    await loadCoaches();
    
    // Build request body based on filter
    const requestBody = {initData};
    if (currentStudentsFilter === 'my') {
        requestBody.view_mode = 'my';
    } else if (currentStudentsFilter === 'other' && coaches.length > 1) {
        // Find other coach (brother)
        const otherCoach = coaches.find(c => !c.is_current);
        if (otherCoach) {
            requestBody.coach_id = otherCoach.id;
        }
    }
    
    // Show cached data first
    const cached = DataCache.load(DataCache.STUDENTS_KEY);
    if (cached) {
        students = cached;
        renderStudentsList(students);
    }
    
    try {
        const res = await fetch(`${API}/api/students`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(requestBody)
        });
        
        students = await res.json();
        
        // Save to cache
        DataCache.save(DataCache.STUDENTS_KEY, students);
        
        // Render fresh data
        renderStudentsList(students);
    } catch (e) {
        console.error('Students load error:', e);
        if (!cached) {
            showNotification('Ошибка загрузки учеников', 'error');
        }
    }
}

function setStudentsFilter(filter) {
    currentStudentsFilter = filter;
    
    // Update UI
    document.querySelectorAll('.filter-tab').forEach(tab => {
        tab.classList.toggle('active', tab.dataset.filter === filter);
    });
    
    // Reload students with filter
    loadStudents();
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
        const remaining = s.lessons_remaining !== undefined ? s.lessons_remaining : s.lessons_count;
        if (remaining <= 0) {
            lessonsBadge = '<span class="list-item-badge danger">Нет занятий</span>';
        } else if (remaining <= 2) {
            lessonsBadge = `<span class="list-item-badge warning">${remaining} занятия</span>`;
        }
        
        // Get coach badge (my vs other)
        let coachBadge = '';
        if (coaches.length > 1) {
            const isMyStudent = s.is_my_student !== undefined ? s.is_my_student : s.coach_id === currentCoach?.coach_id;
            const badgeClass = isMyStudent ? 'my' : 'other';
            const badgeText = isMyStudent ? 'Мой' : 'Брат';
            coachBadge = `<span class="coach-badge ${badgeClass}">${badgeText}</span>`;
        }
        
        // Show locations info
        let locationsInfo = '';
        if (s.schedules && s.schedules.length > 1) {
            const locationCount = s.schedules.length;
            const primaryLoc = s.schedules.find(sch => sch.is_primary);
            locationsInfo = `<span>📍 ${primaryLoc?.location_name || 'Зал'} +${locationCount - 1}</span>`;
        } else {
            locationsInfo = `<span>📍 ${escapeHtml(s.location || 'Зал Break Wave')}</span>`;
        }
        
        // Lessons indicator
        const lessonsIndicator = `<span class="lessons-indicator ${remaining <= 2 ? 'low' : remaining <= 0 ? 'none' : ''}">${remaining}/${s.lessons_count || 8}</span>`;
        
        return `
            <div class="list-item" onclick="openStudentDetail(${s.id})">
                <div class="list-item-header">
                    <span class="list-item-title">${escapeHtml(s.name)} ${lessonsIndicator}</span>
                    <div style="display: flex; gap: 4px;">
                        ${coachBadge}
                        ${lessonsBadge || statusBadge}
                    </div>
                </div>
                <div class="list-item-subtitle">${escapeHtml(s.nickname || '')}</div>
                <div class="list-item-meta">
                    ${locationsInfo}
                    <span>🕐 ${days}</span>
                </div>
            </div>
        `;
    }).join('');
}

function filterStudents(query) {
    const filtered = students.filter(s => 
        s.name.toLowerCase().includes(query.toLowerCase()) ||
        (s.nickname && s.nickname.toLowerCase().includes(query.toLowerCase()))
    );
    renderStudentsList(filtered);
}

async function openAddStudent() {
    editingStudentId = null;
    document.getElementById('student-form-title').textContent = 'Новый ученик';
    document.getElementById('student-form').reset();
    selectedDays = new Set([1, 3]);
    
    // Reset weekday buttons
    document.querySelectorAll('#weekdays-selector button').forEach(btn => {
        const day = parseInt(btn.dataset.day);
        btn.classList.toggle('active', selectedDays.has(day));
    });
    
    // Set default values
    document.getElementById('st-location').value = 'Зал Break Wave';
    document.getElementById('st-price').value = '150';
    document.getElementById('st-count').value = '8';
    
    const today = new Date().toISOString().split('T')[0];
    document.getElementById('st-sub-start').value = today;
    
    // Load locations
    await loadLocations();
    
    // Load coaches list
    await loadCoaches();
    renderCoachSelect();
    
    // Generate time inputs
    generateLessonTimeInputs();
    
    navigate('student-form');
}

async function openStudentDetail(id) {
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
        const remaining = student.lessons_remaining !== undefined ? student.lessons_remaining : student.lessons_count;
        const total = student.lessons_count || 8;
        const used = total - remaining;
        let lessonsAlert = '';
        
        if (remaining <= 0) {
            lessonsAlert = '<div class="alert-danger">Занятия закончились! Требуется оплата.</div>';
        } else if (remaining <= 2) {
            lessonsAlert = `<div class="alert-warning">Осталось ${remaining} занятия. Пора оплачивать!</div>`;
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
        
        const content = document.getElementById('student-detail-content');
        content.innerHTML = `
            <div class="student-header">
                <div class="student-avatar">${student.name.charAt(0)}</div>
                <div class="student-name">${escapeHtml(student.name)}</div>
                ${student.nickname ? `<div class="student-nickname">${escapeHtml(student.nickname)}</div>` : ''}
                <span class="student-status ${student.is_active ? 'active' : 'inactive'}">
                    ${student.is_active ? 'Активен' : 'Неактивен'}
                </span>
            </div>
            
            ${lessonsAlert}
            ${subAlert}
            
            <div class="info-section">
                <h3>📞 Контакты</h3>
                <div class="info-row">
                    <span class="info-label">Телефон</span>
                    <span class="info-value">${student.phone || '—'}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Тел. родителя</span>
                    <span class="info-value">${student.parent_phone || '—'}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Возраст</span>
                    <span class="info-value">${student.age ? student.age + ' лет' : '—'}</span>
                </div>
            </div>
            
            <div class="info-section">
                <h3>📍 Залы и расписание</h3>
                ${renderStudentDetailLocations(student)}
                <div class="info-row" style="margin-top: 12px;">
                    <span class="info-label">Стоимость</span>
                    <span class="info-value">${student.lesson_price?.toLocaleString() || 0} Br / ${total} занятий</span>
                </div>
            </div>
            
            <div class="info-section">
                <h3>📅 Абонемент</h3>
                ${student.is_unlimited ? `
                <div style="background: var(--bg-secondary); border-radius: 8px; padding: 12px; margin-bottom: 12px; border-left: 3px solid var(--accent);">
                    <div style="font-size: 13px; color: var(--text-secondary); margin-bottom: 4px;">Тип абонемента</div>
                    <div style="font-weight: 600; color: var(--accent);">♾️ Безлимитный (по месяцам)</div>
                    <div style="font-size: 12px; color: var(--text-muted); margin-top: 4px;">
                        Занятия не считаются. Оплата по окончанию срока.
                    </div>
                </div>
                ` : `
                <div class="lessons-progress">
                    <div class="progress-bar">
                        <div class="progress-fill ${remaining <= 2 ? 'low' : remaining <= 0 ? 'empty' : ''}" 
                             style="width: ${(used / total) * 100}%"></div>
                    </div>
                    <div class="progress-text">
                        <span>Использовано: <b>${used}</b></span>
                        <span class="${remaining <= 2 ? 'text-warning' : ''}">Осталось: <b>${remaining}</b></span>
                    </div>
                </div>
                <div style="font-size: 12px; color: var(--text-muted); margin-top: 8px; padding: 8px; background: var(--bg-secondary); border-radius: 8px;">
                    💡 При отметке "Присутствовал" — занятие списывается автоматически
                </div>
                `}
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
            
            ${attendanceSummary ? `
            <div class="info-section">
                <h3>📊 Посещаемость</h3>
                ${attendanceSummary}
            </div>
            ` : ''}
            
            ${student.notes ? `
            <div class="info-section">
                <h3>📝 Заметки</h3>
                <p style="color: var(--text-secondary); font-size: 14px;">${escapeHtml(student.notes)}</p>
            </div>
            ` : ''}
            
            <div class="action-buttons-grid">
                <button class="btn-primary" onclick="editStudent(${student.id})">✏️ Редактировать</button>
                <button class="btn-secondary" onclick="addPaymentForStudent(${student.id})">💰 Оплата</button>
                <button class="btn-secondary" onclick="markExtraAttendance(${student.id})">⭐ Внеплановое</button>
                <button class="btn-secondary" onclick="viewAttendanceHistory(${student.id})">📋 История</button>
            </div>
        `;
        
        navigate('student-detail');
    } catch (e) {
        console.error('Student detail error:', e);
        showNotification('Ошибка загрузки', 'error');
    }
}

async function editStudent(id) {
    editingStudentId = id;
    
    try {
        const res = await fetch(`${API}/api/students/${id}`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData})
        });
        
        const student = await res.json();
        
        // Basic info
        document.getElementById('student-form-title').textContent = 'Редактировать ученика';
        document.getElementById('st-name').value = student.name || '';
        document.getElementById('st-nickname').value = student.nickname || '';
        document.getElementById('st-phone').value = student.phone || '';
        document.getElementById('st-parent-phone').value = student.parent_phone || '';
        document.getElementById('st-age').value = student.age || '';
        document.getElementById('st-notes').value = student.notes || '';
        
        // Subscription
        document.getElementById('st-price').value = student.lesson_price || 150;
        document.getElementById('st-count').value = student.lessons_count || 8;
        
        // Handle unlimited subscription
        const isUnlimited = student.is_unlimited || false;
        document.getElementById('st-unlimited').checked = isUnlimited;
        toggleUnlimited(isUnlimited);
        
        if (student.subscription_start) {
            document.getElementById('st-sub-start').value = student.subscription_start;
        }
        if (student.subscription_end) {
            document.getElementById('st-sub-end').value = student.subscription_end;
        }
        
        // Location schedules - NEW SYSTEM
        if (student.schedules && student.schedules.length > 0) {
            currentLocationSchedules = student.schedules.map(s => ({
                id: s.id,
                location_id: s.location_id,
                days: s.days ? s.days.split(',').map(Number) : [1, 3],
                times: s.times ? JSON.parse(s.times) : {},
                duration: s.duration || 90,
                is_primary: s.is_primary
            }));
        } else {
            // Fallback to legacy data
            const legacyDays = student.lesson_days ? student.lesson_days.split(',').map(Number) : [1, 3];
            const legacyTimes = {};
            legacyDays.forEach(d => {
                legacyTimes[d] = student.lesson_time || '18:00';
            });
            
            currentLocationSchedules = [{
                id: null,
                location_id: null,
                days: legacyDays,
                times: legacyTimes,
                duration: 90,
                is_primary: true
            }];
        }
        
        // Ensure locations are loaded before rendering
        if (availableLocations.length === 0) {
            await loadLocationsForSelect();
        } else {
            renderLocationSchedules();
        }
        
        navigate('student-form');
    } catch (e) {
        console.error('Edit student error:', e);
        showNotification('Ошибка загрузки данных ученика', 'error');
    }
}

async function saveStudent() {
    // Get coach_id from select or use current coach
    const coachSelect = document.getElementById('st-coach');
    const coachId = coachSelect && coachSelect.value ? parseInt(coachSelect.value) : null;
    
    // Validate location schedules
    const schedules = collectLocationSchedules();
    if (schedules.length === 0 || !schedules.some(s => s.location_id)) {
        showNotification('Выберите хотя бы один зал', 'error');
        return;
    }
    
    const data = {
        name: document.getElementById('st-name').value,
        nickname: document.getElementById('st-nickname').value,
        phone: document.getElementById('st-phone').value,
        parent_phone: document.getElementById('st-parent-phone').value,
        age: document.getElementById('st-age').value,
        lesson_price: parseInt(document.getElementById('st-price').value) || 150,
        lessons_count: parseInt(document.getElementById('st-count').value) || 8,
        is_unlimited: document.getElementById('st-unlimited').checked,
        subscription_start: document.getElementById('st-sub-start').value,
        subscription_end: document.getElementById('st-sub-end').value,
        notes: document.getElementById('st-notes').value,
        coach_id: coachId,
        schedules: schedules
    };
    
    try {
        const url = editingStudentId 
            ? `${API}/api/students/${editingStudentId}/update`
            : `${API}/api/students/create`;
        
        const res = await fetch(url, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData, student: data})
        });
        
        const result = await res.json();
        
        if (result.success) {
            showNotification(editingStudentId ? 'Ученик обновлен' : 'Ученик добавлен', 'success');
            // Clear cache to force refresh
            DataCache.clear();
            goBack();
            if (currentScreen === 'students') {
                loadStudents();
            }
        } else {
            showNotification('Ошибка сохранения', 'error');
        }
    } catch (e) {
        console.error('Save student error:', e);
        showNotification('Ошибка сохранения', 'error');
    }
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
        html += '<div class="calendar-day other-month"></div>';
    }
    
    // Days
    const today = new Date();
    for (let day = 1; day <= lastDay.getDate(); day++) {
        const isToday = today.getDate() === day && 
                       today.getMonth() + 1 === month && 
                       today.getFullYear() === year;
        
        const hasLessons = daysWithLessons[day]?.length > 0;
        const dot = hasLessons ? '<div class="day-dot"></div>' : '';
        
        html += `
            <div class="calendar-day ${isToday ? 'today' : ''}" 
                 onclick="selectCalendarDay(${day}, this)">
                ${day}
                ${dot}
            </div>
        `;
    }
    
    grid.innerHTML = html;
}

function changeMonth(delta) {
    currentCalendarDate.setMonth(currentCalendarDate.getMonth() + delta);
    loadCalendar();
}

function selectCalendarDay(day, element) {
    const lessons = calendarData.days[day] || [];
    const container = document.getElementById('calendar-day-details');
    
    if (lessons.length === 0) {
        container.innerHTML = `
            <h4>${day} ${document.getElementById('calendar-month').textContent}</h4>
            <p style="color: var(--text-muted); margin-top: 8px;">Нет занятий</p>
        `;
    } else {
        // Group by time
        const byTime = {};
        lessons.forEach(l => {
            const time = l.time || '—';
            if (!byTime[time]) byTime[time] = [];
            byTime[time].push(l);
        });
        
        let html = `<h4>${day} ${document.getElementById('calendar-month').textContent}</h4>`;
        
        // Show lessons grouped by time
        Object.keys(byTime).sort().forEach(time => {
            const students = byTime[time];
            const markedCount = students.filter(s => s.is_marked).length;
            
            html += `
                <div style="margin-top: 16px; margin-bottom: 8px;">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                        <span style="font-weight: 600; color: var(--accent);">🕐 ${escapeHtml(time)}</span>
                        <span style="font-size: 12px; color: var(--text-muted);">
                            ${markedCount > 0 ? `✓ ${markedCount}/${students.length}` : `${students.length} уч.`}
                        </span>
                    </div>
                    <div style="display: flex; flex-direction: column; gap: 8px;">
                        ${students.map(s => {
                            let statusIcon = '⏳'; // Not marked
                            let statusColor = 'var(--text-muted)';
                            if (s.status === 'present') {
                                statusIcon = '✅';
                                statusColor = 'var(--success)';
                            } else if (s.status === 'absent') {
                                statusIcon = '❌';
                                statusColor = 'var(--danger)';
                            } else if (s.status === 'sick') {
                                statusIcon = '🤒';
                                statusColor = 'var(--warning)';
                            }
                            
                            return `
                                <div class="list-item" style="margin-bottom: 0; cursor: pointer;" 
                                     onclick="openStudentDetail(${s.student_id})">
                                    <div class="list-item-header">
                                        <span class="list-item-title">${escapeHtml(s.student_name)}</span>
                                        <span style="color: ${statusColor}; font-size: 16px;">${statusIcon}</span>
                                    </div>
                                    <div class="list-item-subtitle">
                                        ${s.location || 'Зал'} 
                                        ${s.status === 'present' ? '• Присутствовал' : 
                                          s.status === 'absent' ? '• Отсутствовал' : 
                                          s.status === 'sick' ? '• Болел' : '• Не отмечен'}
                                    </div>
                                </div>
                            `;
                        }).join('')}
                    </div>
                </div>
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

// === Payments ===

async function loadPayments(status = 'all') {
    try {
        const body = {initData};
        if (status !== 'all') {
            body.status = status;
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
        
        return `
            <div class="list-item">
                <div class="list-item-header">
                    <span class="list-item-title">${escapeHtml(p.student_name)}</span>
                    <span class="payment-status ${statusClass}">${statusText}</span>
                </div>
                <div class="list-item-subtitle">${p.amount.toLocaleString()} Br • ${p.lessons_count} занятий</div>
                <div class="list-item-meta">
                    ${p.period_start && p.period_end ? 
                        `<span>📅 ${formatDate(p.period_start)} — ${formatDate(p.period_end)}</span>` : ''}
                </div>
                ${p.status !== 'paid' ? `
                    <button class="btn-primary" style="margin-top: 12px; width: 100%;" 
                            onclick="markPaymentPaid(${p.id})">
                        ✅ Отметить оплаченным
                    </button>
                ` : ''}
            </div>
        `;
    }).join('');
}

function switchPaymentTab(status, btn) {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    btn.classList.add('active');
    loadPayments(status);
}

async function openAddPayment() {
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
    
    // Set default dates
    const today = new Date();
    const nextMonth = new Date(today.getFullYear(), today.getMonth() + 1, today.getDate());
    document.getElementById('pay-start').value = today.toISOString().split('T')[0];
    document.getElementById('pay-end').value = nextMonth.toISOString().split('T')[0];
    
    navigate('payment-form');
}

function addPaymentForStudent(studentId) {
    openAddPayment().then(() => {
        document.getElementById('pay-student').value = studentId;
    });
}

async function savePayment() {
    const data = {
        student_id: parseInt(document.getElementById('pay-student').value),
        amount: parseInt(document.getElementById('pay-amount').value),
        lessons_count: parseInt(document.getElementById('pay-count').value),
        period_start: document.getElementById('pay-start').value,
        period_end: document.getElementById('pay-end').value,
        status: document.getElementById('pay-status').value,
        notes: document.getElementById('pay-notes').value,
    };
    
    if (!data.student_id || !data.amount) {
        showNotification('Заполните обязательные поля', 'error');
        return;
    }
    
    try {
        const res = await fetch(`${API}/api/payments/create`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData, payment: data})
        });
        
        const result = await res.json();
        
        if (result.success) {
            showNotification('Оплата добавлена', 'success');
            // Clear cache to force refresh
            DataCache.clear();
            goBack();
        } else {
            showNotification('Ошибка сохранения', 'error');
        }
    } catch (e) {
        console.error('Save payment error:', e);
        showNotification('Ошибка сохранения', 'error');
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
            // Clear cache to force refresh
            DataCache.clear();
            loadPayments();
            loadDashboard();
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
        // Get students with their schedules for this date
        const res = await fetch(`${API}/api/students`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData, view_mode: 'my'})
        });
        
        const studentsList = await res.json();
        const dayOfWeek = new Date(date).getDay();
        
        // Filter students who have lesson on this day (and optionally in this location)
        let filteredStudents = studentsList.filter(s => {
            // Check if student has schedule for this day
            if (!s.schedules || s.schedules.length === 0) {
                // Legacy check
                if (!s.lesson_days) return false;
                return s.lesson_days.split(',').includes(String(dayOfWeek));
            }
            return s.schedules.some(sch => sch.days && sch.days.split(',').includes(String(dayOfWeek)));
        });
        
        // Filter by location if selected
        if (locationId) {
            filteredStudents = filteredStudents.filter(s => 
                s.schedules && s.schedules.some(sch => sch.location_id == locationId)
            );
        }
        
        // Sort by name
        filteredStudents.sort((a, b) => a.name.localeCompare(b.name));
        
        // Initialize attendance data
        quickAttendanceData = {};
        filteredStudents.forEach(s => {
            quickAttendanceData[s.id] = {
                student_id: s.id,
                status: null, // null, present, absent, sick
                student: s
            };
        });
        
        renderQuickLessonList(filteredStudents);
        updateQuickStats();
        
    } catch (e) {
        console.error('Quick lesson load error:', e);
        document.getElementById('quick-lesson-students').innerHTML = `
            <div class="empty-state">
                <p>Ошибка загрузки</p>
            </div>
        `;
    }
}

function renderQuickLessonList(students) {
    const container = document.getElementById('quick-lesson-students');
    
    if (students.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">📅</div>
                <p>Нет учеников на этот день</p>
            </div>
        `;
        return;
    }
    
    container.innerHTML = students.map((s, index) => {
        const remaining = s.lessons_remaining !== undefined ? s.lessons_remaining : s.lessons_count;
        let dotClass = 'ok';
        if (remaining <= 0) dotClass = 'none';
        else if (remaining <= 2) dotClass = 'low';
        
        return `
            <div class="quick-student-item" data-student-id="${s.id}" onclick="toggleQuickStatus(${s.id})">
                <div class="quick-student-avatar">${s.name.charAt(0)}</div>
                <div class="quick-student-info">
                    <div class="quick-student-name">
                        ${index + 1}. ${escapeHtml(s.name)}
                        <span class="lessons-dot ${dotClass}"></span>
                    </div>
                    <div class="quick-student-meta">${remaining} занятий</div>
                </div>
                <div class="quick-student-status" id="status-${s.id}">⏳</div>
            </div>
        `;
    }).join('');
    
    document.getElementById('ql-title').textContent = `✅ Отметка (${students.length})`;
}

function toggleQuickStatus(studentId) {
    const data = quickAttendanceData[studentId];
    const item = document.querySelector(`[data-student-id="${studentId}"]`);
    const statusEl = document.getElementById(`status-${studentId}`);
    
    // Cycle: null -> present -> absent -> sick -> null
    const cycle = [null, 'present', 'absent', 'sick'];
    const currentIndex = cycle.indexOf(data.status);
    const nextStatus = cycle[(currentIndex + 1) % cycle.length];
    
    data.status = nextStatus;
    
    // Update UI
    item.classList.remove('selected-present', 'selected-absent', 'selected-sick');
    
    if (nextStatus === 'present') {
        item.classList.add('selected-present');
        statusEl.textContent = '✅';
    } else if (nextStatus === 'absent') {
        item.classList.add('selected-absent');
        statusEl.textContent = '❌';
    } else if (nextStatus === 'sick') {
        item.classList.add('selected-sick');
        statusEl.textContent = '🤒';
    } else {
        statusEl.textContent = '⏳';
    }
    
    updateQuickStats();
}

function selectAllQuick(status) {
    Object.keys(quickAttendanceData).forEach(id => {
        quickAttendanceData[id].status = status;
        const item = document.querySelector(`[data-student-id="${id}"]`);
        const statusEl = document.getElementById(`status-${id}`);
        
        item.classList.remove('selected-present', 'selected-absent', 'selected-sick');
        item.classList.add(`selected-${status}`);
        
        if (status === 'present') statusEl.textContent = '✅';
        else if (status === 'absent') statusEl.textContent = '❌';
        else if (status === 'sick') statusEl.textContent = '🤒';
    });
    
    updateQuickStats();
}

function updateQuickStats() {
    const total = Object.keys(quickAttendanceData).length;
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
            status: d.status
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
            
            DataCache.clear();
            goBack();
            loadDashboard();
        } else {
            showNotification('Ошибка сохранения', 'error');
        }
    } catch (e) {
        console.error('Save quick attendance error:', e);
        showNotification('Ошибка сети', 'error');
    }
}

// Legacy function for compatibility
function setAttendance(studentId, status) {
    toggleQuickStatus(studentId);
}

async function saveQuickLesson() {
    await saveQuickAttendance();
}

// === Helpers ===

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
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
    notif.textContent = message;
    document.body.appendChild(notif);
    
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

// Add save button to quick lesson screen
const quickLessonContent = document.getElementById('quick-lesson-content');
if (quickLessonContent) {
    const saveBtn = document.createElement('button');
    saveBtn.className = 'btn-primary';
    saveBtn.style.cssText = 'width: 100%; margin-top: 20px;';
    saveBtn.textContent = '💾 Сохранить занятия';
    saveBtn.onclick = saveQuickLesson;
    quickLessonContent.appendChild(saveBtn);
}


// === Extra Attendance & Attendance History ===

async function markExtraAttendance(studentId) {
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
                    <h3>📋 История посещений</h3>
                    <button class="close-btn" onclick="this.closest('.modal').remove()">✕</button>
                </div>
                
                <div class="student-summary">
                    <div class="summary-row">
                        <span class="summary-name">${escapeHtml(student.name)}</span>
                        <span class="summary-lessons ${student.lessons_remaining <= 2 ? 'warning' : ''}">
                            ${student.lessons_remaining}/${student.lessons_count} занятий
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


// === Locations ===

let locations = [];

async function loadLocations() {
    try {
        const res = await fetch(`${API}/api/locations`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData})
        });
        locations = await res.json();
        renderLocationSelect();
    } catch (e) {
        console.error('Locations load error:', e);
    }
}

function renderLocationSelect() {
    const select = document.getElementById('st-location-id');
    if (!select) return;
    
    let html = '<option value="">Основной зал</option>';
    locations.forEach(loc => {
        html += `<option value="${loc.id}">${escapeHtml(loc.name)}</option>`;
    });
    select.innerHTML = html;
}

// === Lesson Times (per day) ===

let lessonTimes = {}; // day -> time

function generateLessonTimeInputs() {
    const container = document.getElementById('lesson-times-container');
    if (!container) return;
    
    const daysMap = {0:'Пн',1:'Вт',2:'Ср',3:'Чт',4:'Пт',5:'Сб',6:'Вс'};
    
    let html = '<div class="lesson-times-grid">';
    html += '<label class="section-label">Время по дням:</label>';
    html += '<div class="times-grid">';
    
    selectedDays.forEach(day => {
        const time = lessonTimes[day] || '18:00';
        html += `
            <div class="time-input-row" data-day="${day}">
                <span class="day-label">${daysMap[day]}</span>
                <input type="time" class="day-time" value="${time}" data-day="${day}">
            </div>
        `;
    });
    
    html += '</div></div>';
    container.innerHTML = html;
    
    // Add change listeners
    container.querySelectorAll('.day-time').forEach(input => {
        input.addEventListener('change', (e) => {
            lessonTimes[e.target.dataset.day] = e.target.value;
        });
    });
}

// Override setupWeekdaySelector to regenerate time inputs
const originalSetupWeekdaySelector = setupWeekdaySelector;
setupWeekdaySelector = function() {
    const container = document.getElementById('weekdays-selector');
    if (!container) return;
    
    container.querySelectorAll('button').forEach(btn => {
        const day = parseInt(btn.dataset.day);
        if (selectedDays.has(day)) {
            btn.classList.add('active');
        }
        
        btn.addEventListener('click', () => {
            btn.classList.toggle('active');
            if (btn.classList.contains('active')) {
                selectedDays.add(day);
                if (!lessonTimes[day]) lessonTimes[day] = '18:00';
            } else {
                selectedDays.delete(day);
                delete lessonTimes[day];
            }
            generateLessonTimeInputs();
        });
    });
    
    generateLessonTimeInputs();
};

// Override saveStudent to include lesson_times
const originalSaveStudent = saveStudent;
saveStudent = async function() {
    // Collect lesson times
    const times = {};
    selectedDays.forEach(day => {
        const input = document.querySelector(`.day-time[data-day="${day}"]`);
        times[day] = input ? input.value : '18:00';
    });
    
    const coachSelect = document.getElementById('st-coach');
    const coachId = coachSelect && coachSelect.value ? parseInt(coachSelect.value) : null;
    const locationSelect = document.getElementById('st-location-id');
    const locationId = locationSelect ? parseInt(locationSelect.value) || null : null;
    
    const data = {
        name: document.getElementById('st-name').value,
        nickname: document.getElementById('st-nickname').value,
        phone: document.getElementById('st-phone').value,
        parent_phone: document.getElementById('st-parent-phone').value,
        age: document.getElementById('st-age').value,
        location: document.getElementById('st-location').value,
        location_id: locationId,
        lesson_days: Array.from(selectedDays).join(','),
        lesson_times: times,
        lesson_price: parseInt(document.getElementById('st-price').value) || 150,
        lessons_count: parseInt(document.getElementById('st-count').value) || 8,
        subscription_start: document.getElementById('st-sub-start').value,
        subscription_end: document.getElementById('st-sub-end').value,
        notes: document.getElementById('st-notes').value,
        coach_id: coachId,
    };
    
    try {
        const url = editingStudentId 
            ? `${API}/api/students/${editingStudentId}/update`
            : `${API}/api/students/create`;
        
        const res = await fetch(url, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData, student: data})
        });
        
        const result = await res.json();
        
        if (result.success) {
            showNotification(editingStudentId ? 'Ученик обновлен' : 'Ученик добавлен', 'success');
            DataCache.clear();
            goBack();
            if (currentScreen === 'students') {
                loadStudents();
            }
        } else {
            showNotification('Ошибка сохранения', 'error');
        }
    } catch (e) {
        console.error('Save student error:', e);
        showNotification('Ошибка сохранения', 'error');
    }
};

// Edit student function is defined above and works with new schedule system

// === Statistics ===

let currentStatsPeriod = 'month';

function switchStatsPeriod(period, btn) {
    document.querySelectorAll('#screen-statistics .tab').forEach(t => t.classList.remove('active'));
    btn.classList.add('active');
    currentStatsPeriod = period;
    loadStatistics();
}

async function loadStatistics() {
    const container = document.getElementById('statistics-content');
    container.innerHTML = '<div class="loading"><div class="spinner"></div>Загрузка...</div>';
    
    try {
        const res = await fetch(`${API}/api/statistics`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData, period: currentStatsPeriod})
        });
        
        const data = await res.json();
        renderStatistics(data);
    } catch (e) {
        console.error('Statistics error:', e);
        container.innerHTML = '<div class="empty-state">Ошибка загрузки статистики</div>';
    }
}

function renderStatistics(data) {
    const container = document.getElementById('statistics-content');
    
    const daysMap = {0:'Пн',1:'Вт',2:'Ср',3:'Чт',4:'Пт',5:'Сб',6:'Вс'};
    
    let byDayHtml = '';
    for (let day = 0; day < 7; day++) {
        const dayData = data.by_day_of_week[day];
        if (dayData && dayData.total > 0) {
            byDayHtml += `
                <div class="stat-bar-item">
                    <span class="bar-label">${daysMap[day]}</span>
                    <div class="bar-wrapper">
                        <div class="bar-fill" style="width: ${dayData.rate}%"></div>
                    </div>
                    <span class="bar-value">${dayData.rate}%</span>
                </div>
            `;
        }
    }
    
    let byLocationHtml = '';
    for (const [loc, locData] of Object.entries(data.by_location)) {
        if (locData.total > 0) {
            byLocationHtml += `
                <div class="stat-bar-item">
                    <span class="bar-label">${escapeHtml(loc)}</span>
                    <div class="bar-wrapper">
                        <div class="bar-fill" style="width: ${locData.rate}%"></div>
                    </div>
                    <span class="bar-value">${locData.rate}%</span>
                </div>
            `;
        }
    }
    
    let ageGroupsHtml = '';
    for (const [age, count] of Object.entries(data.age_groups)) {
        if (count > 0) {
            ageGroupsHtml += `
                <div class="stat-pill">
                    <span class="stat-value">${count}</span>
                    <span class="stat-label">${age}</span>
                </div>
            `;
        }
    }
    
    let trendHtml = '';
    data.monthly_trend.forEach(m => {
        trendHtml += `
            <div class="trend-item">
                <span class="trend-month">${m.month}</span>
                <div class="trend-bar-wrapper">
                    <div class="trend-bar" style="height: ${Math.max(10, m.count * 2)}px"></div>
                </div>
                <span class="trend-count">${m.count}</span>
            </div>
        `;
    });
    
    container.innerHTML = `
        <div class="statistics-content">
            <div class="stats-summary-cards">
                <div class="summary-card">
                    <span class="summary-value">${data.summary.total_students}</span>
                    <span class="summary-label">Учеников</span>
                </div>
                <div class="summary-card">
                    <span class="summary-value">${data.summary.total_lessons}</span>
                    <span class="summary-label">Занятий</span>
                </div>
                <div class="summary-card highlight">
                    <span class="summary-value">${data.summary.attendance_rate}%</span>
                    <span class="summary-label">Посещаемость</span>
                </div>
            </div>
            
            <div class="stat-section">
                <h3>📅 По дням недели</h3>
                <div class="stat-bars">${byDayHtml || '<p>Нет данных</p>'}</div>
            </div>
            
            <div class="stat-section">
                <h3>📍 По залам</h3>
                <div class="stat-bars">${byLocationHtml || '<p>Нет данных</p>'}</div>
            </div>
            
            <div class="stat-section">
                <h3>👥 Возрастные группы</h3>
                <div class="age-groups">${ageGroupsHtml || '<p>Нет данных</p>'}</div>
            </div>
            
            <div class="stat-section">
                <h3>📈 Динамика (6 мес)</h3>
                <div class="trend-chart">${trendHtml || '<p>Нет данных</p>'}</div>
            </div>
        </div>
    `;
}

// === Search ===

let searchTimeout = null;

function performSearch(query) {
    clearTimeout(searchTimeout);
    
    if (!query || query.length < 2) {
        document.getElementById('search-results').innerHTML = '';
        return;
    }
    
    searchTimeout = setTimeout(async () => {
        try {
            const res = await fetch(`${API}/api/search`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({initData, query})
            });
            
            const data = await res.json();
            renderSearchResults(data.results);
        } catch (e) {
            console.error('Search error:', e);
        }
    }, 300);
}

function renderSearchResults(results) {
    const container = document.getElementById('search-results');
    
    if (results.length === 0) {
        container.innerHTML = '<div class="empty-state">Ничего не найдено</div>';
        return;
    }
    
    container.innerHTML = results.map(r => `
        <div class="list-item" onclick="openStudentDetail(${r.id}); goBack();">
            <div class="list-item-header">
                <span class="list-item-title">${escapeHtml(r.name)}</span>
                <span class="lessons-indicator ${r.lessons_remaining <= 2 ? 'low' : ''}">${r.lessons_remaining}</span>
            </div>
            <div class="list-item-subtitle">${escapeHtml(r.nickname || '')}</div>
            <div class="list-item-meta">
                ${r.phone ? `<span>📞 ${r.phone}</span>` : ''}
                ${r.age ? `<span>🎂 ${r.age} лет</span>` : ''}
                <span>📍 ${escapeHtml(r.location || 'Зал Break Wave')}</span>
            </div>
        </div>
    `).join('');
}


// === Multiple Locations Management ===

let currentLocationSchedules = [];
let availableLocations = [];

// Initialize with one default location
function initLocationSchedules(schedules = null) {
    if (schedules && schedules.length > 0) {
        currentLocationSchedules = schedules.map(s => ({
            id: s.id,
            location_id: s.location_id,
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
        const res = await fetch(`${API}/api/locations`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData})
        });
        availableLocations = await res.json();
        renderLocationSchedules();
    } catch (e) {
        console.error('Load locations error:', e);
    }
}

// Override openAddStudent to init locations
openAddStudent = async function() {
    editingStudentId = null;
    document.getElementById('student-form-title').textContent = 'Новый ученик';
    document.getElementById('student-form').reset();
    
    // Reset unlimited checkbox
    document.getElementById('st-unlimited').checked = false;
    toggleUnlimited(false);
    
    // Load locations first
    await loadLocationsForSelect();
    
    // Init with default schedule
    initLocationSchedules();
    
    // Load coaches for admin
    await loadCoaches();
    renderCoachSelect();
    
    showScreen('student-form');
};

// Toggle unlimited lessons
function toggleUnlimited(checked) {
    const lessonsGroup = document.getElementById('lessons-count-group');
    if (checked) {
        lessonsGroup.classList.add('lessons-count-hidden');
        document.getElementById('st-count').value = 999; // Set high number for unlimited
    } else {
        lessonsGroup.classList.remove('lessons-count-hidden');
        document.getElementById('st-count').value = 8; // Default value
    }
}

// Override openEditStudent
openEditStudent = async function(studentId) {
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
        document.getElementById('st-price').value = student.lesson_price || 150;
        document.getElementById('st-notes').value = student.notes || '';
        document.getElementById('st-sub-start').value = student.subscription_start || '';
        document.getElementById('st-sub-end').value = student.subscription_end || '';
        
        // Handle unlimited subscription
        const isUnlimited = student.is_unlimited || false;
        document.getElementById('st-unlimited').checked = isUnlimited;
        toggleUnlimited(isUnlimited);
        
        // Set lessons count (only if not unlimited)
        if (!isUnlimited) {
            document.getElementById('st-count').value = student.lessons_count || 8;
        }
        
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
        
        showScreen('student-form');
    } catch (e) {
        console.error('Edit student error:', e);
        showNotification('Ошибка загрузки', 'error');
    }
};

// Override saveStudent to include schedules
saveStudent = async function() {
    const isUnlimited = document.getElementById('st-unlimited').checked;
    
    const studentData = {
        name: document.getElementById('st-name').value,
        nickname: document.getElementById('st-nickname').value || null,
        phone: document.getElementById('st-phone').value || null,
        parent_phone: document.getElementById('st-parent-phone').value || null,
        age: document.getElementById('st-age').value ? parseInt(document.getElementById('st-age').value) : null,
        lesson_price: parseInt(document.getElementById('st-price').value) || 150,
        lessons_count: isUnlimited ? 999 : (parseInt(document.getElementById('st-count').value) || 8),
        is_unlimited: isUnlimited,
        notes: document.getElementById('st-notes').value || null,
        subscription_start: document.getElementById('st-sub-start').value || null,
        subscription_end: document.getElementById('st-sub-end').value || null,
        schedules: collectLocationSchedules()
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
            DataCache.clear();
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
};

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
                        <div class="detail-location-schedule">${formatDays(schedule.days)} ${formatTimes(schedule.times)}</div>
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
        return uniqueTimes.join(', ');
    } catch {
        return '';
    }
}


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
        document.getElementById('finance-content').innerHTML = `
            <div class="empty-state">
                <p>Ошибка загрузки данных</p>
            </div>
        `;
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
                <span class="trend-month">${m.month}</span>
                <div class="trend-bar-wrapper">
                    <div class="trend-bar" style="height: ${Math.max(10, (m.revenue / maxRevenue) * 100)}px"></div>
                </div>
                <span class="trend-count">${m.revenue >= 1000 ? (m.revenue / 1000).toFixed(1) + 'k' : m.revenue}</span>
            </div>
        `).join('');
    }
    
    // Debtors sections
    let debtorsHtml = '';
    
    // Expired subscriptions
    if (debtors.debtors.expired_subscription.length > 0) {
        debtorsHtml += `
            <div class="finance-section">
                <h3>🚨 Просроченные абонементы (${debtors.debtors.expired_subscription.length})</h3>
                ${debtors.debtors.expired_subscription.map(d => `
                    <div class="debtor-item critical" onclick="openStudentDetail(${d.id})">
                        <div class="debtor-info">
                            <div class="debtor-name">${escapeHtml(d.name)}</div>
                            <div class="debtor-meta">Просрочено ${d.days_overdue} дн.</div>
                        </div>
                        <span class="debtor-badge critical">Просрочен</span>
                    </div>
                `).join('')}
            </div>
        `;
    }
    
    // Ending soon
    if (debtors.debtors.ending_soon.length > 0) {
        debtorsHtml += `
            <div class="finance-section">
                <h3>⏰ Заканчивается скоро (${debtors.debtors.ending_soon.length})</h3>
                ${debtors.debtors.ending_soon.map(d => `
                    <div class="debtor-item warning" onclick="openStudentDetail(${d.id})">
                        <div class="debtor-info">
                            <div class="debtor-name">${escapeHtml(d.name)}</div>
                            <div class="debtor-meta">Осталось ${d.days_left} дн.</div>
                        </div>
                        <span class="debtor-badge warning">${d.days_left} дн.</span>
                    </div>
                `).join('')}
            </div>
        `;
    }
    
    // No lessons
    if (debtors.debtors.no_lessons.length > 0) {
        debtorsHtml += `
            <div class="finance-section">
                <h3>❌ Закончились занятия (${debtors.debtors.no_lessons.length})</h3>
                ${debtors.debtors.no_lessons.map(d => `
                    <div class="debtor-item critical" onclick="openStudentDetail(${d.id})">
                        <div class="debtor-info">
                            <div class="debtor-name">${escapeHtml(d.name)}</div>
                            <div class="debtor-meta">Нет доступных занятий</div>
                        </div>
                        <span class="debtor-badge critical">0 занятий</span>
                    </div>
                `).join('')}
            </div>
        `;
    }
    
    // Low lessons
    if (debtors.debtors.low_lessons.length > 0) {
        debtorsHtml += `
            <div class="finance-section">
                <h3>⚠️ Мало занятий (${debtors.debtors.low_lessons.length})</h3>
                ${debtors.debtors.low_lessons.map(d => `
                    <div class="debtor-item warning" onclick="openStudentDetail(${d.id})">
                        <div class="debtor-info">
                            <div class="debtor-name">${escapeHtml(d.name)}</div>
                            <div class="debtor-meta">Осталось ${d.remaining} занятия</div>
                        </div>
                        <span class="debtor-badge warning">${d.remaining} занятия</span>
                    </div>
                `).join('')}
            </div>
        `;
    }
    
    container.innerHTML = `
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
                <h3>👥 По тренерам</h3>
                ${byCoachHtml}
            </div>
        ` : ''}
        
        ${byLocationHtml ? `
            <div class="finance-section">
                <h3>📍 По залам</h3>
                ${byLocationHtml}
            </div>
        ` : ''}
        
        <div class="finance-section">
            <h3>📈 Динамика доходов (6 мес)</h3>
            <div class="trend-chart">${trendHtml}</div>
        </div>
        
        ${debtorsHtml}
    `;
}

// Add finance to navigation
const originalNavigate = navigate;
navigate = function(screen) {
    if (screen === 'finance') {
        loadFinance();
    }
    return originalNavigate(screen);
};
