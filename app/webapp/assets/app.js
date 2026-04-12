/* === CRM Break Wave Mini App === */

const API = window.location.origin;
const tg = window.Telegram?.WebApp;

// Cache busting - force reload if version changed
const APP_VERSION_KEY = 'crm_bw_version';
const CURRENT_VERSION = '2'; // Change this when deploying major updates

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
        return coaches;
    } catch (e) {
        console.error('Coaches load error:', e);
        return [];
    }
}

function renderCoachSelect() {
    const select = document.getElementById('st-coach');
    const display = document.getElementById('coach-display');
    const container = document.getElementById('coach-select-container');
    
    if (!select || !display) return;
    
    if (coaches.length === 0) {
        select.innerHTML = '<option value="">Нет тренеров</option>';
        return;
    }
    
    if (coaches.length === 1) {
        // Only one coach - show as read-only info
        const coach = coaches[0];
        select.style.display = 'none';
        display.style.display = 'block';
        display.innerHTML = `
            <span class="coach-name">${escapeHtml(coach.first_name || 'Без имени')}</span>
            ${coach.username ? `<span class="coach-username">@${escapeHtml(coach.username)}</span>` : ''}
        `;
        select.value = coach.id;
    } else {
        // Multiple coaches - show select
        select.style.display = 'block';
        display.style.display = 'none';
        select.innerHTML = coaches.map(c => {
            const isCurrent = c.is_current ? ' (вы)' : '';
            const username = c.username ? ` @${c.username}` : '';
            return `<option value="${c.id}">${escapeHtml(c.first_name || 'Без имени')}${username}${isCurrent}</option>`;
        }).join('');
        
        // Select current coach by default
        const currentCoachId = coaches.find(c => c.is_current)?.id;
        if (currentCoachId) {
            select.value = currentCoachId;
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

function renderDashboard(data) {
    // Update stats
    document.getElementById('stat-students').textContent = data.students_count;
    document.getElementById('stat-lessons').textContent = data.lessons_this_month;
    document.getElementById('stat-attendance').textContent = data.attendance_rate + '%';
    document.getElementById('stat-revenue').textContent = data.monthly_revenue.toLocaleString() + '₽';
    
    // Current date
    const dateOptions = { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' };
    document.getElementById('current-date').textContent = new Date().toLocaleDateString('ru-RU', dateOptions);
    
    // Alerts
    const alertsContainer = document.getElementById('alerts-list');
    const alertsSection = document.getElementById('alerts-section');
    
    let alerts = [];
    
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

async function loadStudents() {
    // Load coaches first (for displaying coach info)
    await loadCoaches();
    
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
            body: JSON.stringify({initData})
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
        
        // Get coach info if available
        let coachInfo = '';
        if (s.coach_id && coaches.length > 1) {
            const coach = coaches.find(c => c.id === s.coach_id);
            if (coach) {
                const coachName = escapeHtml(coach.first_name || 'Без имени');
                const coachUsername = coach.username ? `@${escapeHtml(coach.username)}` : '';
                coachInfo = `<div class="list-item-coach">👤 ${coachName} ${coachUsername}</div>`;
            }
        }
        
        return `
            <div class="list-item" onclick="openStudentDetail(${s.id})">
                <div class="list-item-header">
                    <span class="list-item-title">${escapeHtml(s.name)}</span>
                    ${statusBadge}
                </div>
                <div class="list-item-subtitle">${escapeHtml(s.nickname || '')}</div>
                ${coachInfo}
                <div class="list-item-meta">
                    <span>📍 ${escapeHtml(s.location || 'Зал Break Wave')}</span>
                    <span>🕐 ${days} ${s.lesson_time || ''}</span>
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
    document.getElementById('st-time').value = '18:00';
    document.getElementById('st-price').value = '5000';
    document.getElementById('st-count').value = '8';
    
    const today = new Date().toISOString().split('T')[0];
    document.getElementById('st-sub-start').value = today;
    
    // Load coaches list
    await loadCoaches();
    renderCoachSelect();
    
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
        
        let subStatus = 'Нет абонемента';
        if (student.subscription_end) {
            const end = new Date(student.subscription_end);
            const today = new Date();
            const daysLeft = Math.ceil((end - today) / (1000 * 60 * 60 * 24));
            
            if (daysLeft < 0) {
                subStatus = `❌ Просрочен (${formatDate(student.subscription_end)})`;
            } else {
                subStatus = `✅ До ${formatDate(student.subscription_end)} (${daysLeft} дн.)`;
            }
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
                <h3>📍 Занятия</h3>
                <div class="info-row">
                    <span class="info-label">Место</span>
                    <span class="info-value">${escapeHtml(student.location || 'Зал Break Wave')}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Дни</span>
                    <span class="info-value">${days}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Время</span>
                    <span class="info-value">${student.lesson_time || '—'}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Стоимость</span>
                    <span class="info-value">${student.lesson_price?.toLocaleString() || 0}₽ / ${student.lessons_count || 8} занятий</span>
                </div>
            </div>
            
            <div class="info-section">
                <h3>📅 Абонемент</h3>
                <div class="info-row">
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
            
            ${student.notes ? `
            <div class="info-section">
                <h3>📝 Заметки</h3>
                <p style="color: var(--text-secondary); font-size: 14px;">${escapeHtml(student.notes)}</p>
            </div>
            ` : ''}
            
            <div style="display: flex; gap: 12px; margin-top: 24px;">
                <button class="btn-primary" style="flex: 1;" onclick="editStudent(${student.id})">✏️ Редактировать</button>
                <button class="btn-secondary" style="flex: 1;" onclick="addPaymentForStudent(${student.id})">💰 Оплата</button>
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
        
        document.getElementById('student-form-title').textContent = 'Редактировать ученика';
        document.getElementById('st-name').value = student.name || '';
        document.getElementById('st-nickname').value = student.nickname || '';
        document.getElementById('st-phone').value = student.phone || '';
        document.getElementById('st-parent-phone').value = student.parent_phone || '';
        document.getElementById('st-age').value = student.age || '';
        document.getElementById('st-location').value = student.location || 'Зал Break Wave';
        document.getElementById('st-time').value = student.lesson_time || '18:00';
        document.getElementById('st-price').value = student.lesson_price || 5000;
        document.getElementById('st-count').value = student.lessons_count || 8;
        document.getElementById('st-notes').value = student.notes || '';
        
        if (student.subscription_start) {
            document.getElementById('st-sub-start').value = student.subscription_start;
        }
        if (student.subscription_end) {
            document.getElementById('st-sub-end').value = student.subscription_end;
        }
        
        // Set weekdays
        selectedDays = new Set((student.lesson_days || '1,3').split(',').map(Number));
        document.querySelectorAll('#weekdays-selector button').forEach(btn => {
            const day = parseInt(btn.dataset.day);
            btn.classList.toggle('active', selectedDays.has(day));
        });
        
        navigate('student-form');
    } catch (e) {
        console.error('Edit student error:', e);
    }
}

async function saveStudent() {
    // Get coach_id from select or use current coach
    const coachSelect = document.getElementById('st-coach');
    const coachId = coachSelect && coachSelect.value ? parseInt(coachSelect.value) : null;
    
    const data = {
        name: document.getElementById('st-name').value,
        nickname: document.getElementById('st-nickname').value,
        phone: document.getElementById('st-phone').value,
        parent_phone: document.getElementById('st-parent-phone').value,
        age: document.getElementById('st-age').value,
        location: document.getElementById('st-location').value,
        lesson_days: Array.from(selectedDays).join(','),
        lesson_time: document.getElementById('st-time').value,
        lesson_price: parseInt(document.getElementById('st-price').value) || 5000,
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
                 onclick="selectCalendarDay(${day})">
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

function selectCalendarDay(day) {
    const lessons = calendarData.days[day] || [];
    const container = document.getElementById('calendar-day-details');
    
    if (lessons.length === 0) {
        container.innerHTML = `
            <h4>${day} ${document.getElementById('calendar-month').textContent}</h4>
            <p style="color: var(--text-muted); margin-top: 8px;">Нет занятий</p>
        `;
    } else {
        container.innerHTML = `
            <h4>${day} ${document.getElementById('calendar-month').textContent}</h4>
            <div style="margin-top: 12px;">
                ${lessons.map(l => `
                    <div class="list-item" style="margin-bottom: 8px;" onclick="openLessonDetail(${l.id})">
                        <div class="list-item-header">
                            <span class="list-item-title">${escapeHtml(l.time || '—')}</span>
                        </div>
                        <div class="list-item-subtitle">${escapeHtml(l.student_name)}</div>
                    </div>
                `).join('')}
            </div>
        `;
    }
    
    // Highlight selected day
    document.querySelectorAll('.calendar-day').forEach((el, i) => {
        el.classList.remove('selected');
    });
    event.currentTarget.classList.add('selected');
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
                <div class="list-item-subtitle">${p.amount.toLocaleString()}₽ • ${p.lessons_count} занятий</div>
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

async function loadQuickLesson() {
    const date = document.getElementById('ql-date').value;
    
    try {
        const res = await fetch(`${API}/api/students`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({initData})
        });
        
        const studentsList = await res.json();
        const container = document.getElementById('quick-lesson-students');
        
        container.innerHTML = studentsList.map(s => `
            <div class="quick-student-item" data-student-id="${s.id}">
                <span class="quick-student-name">${escapeHtml(s.name)}</span>
                <div class="attendance-buttons">
                    <button class="att-btn present" onclick="setAttendance(${s.id}, 'present')" title="Был">✓</button>
                    <button class="att-btn absent" onclick="setAttendance(${s.id}, 'absent')" title="Не был">✗</button>
                    <button class="att-btn sick" onclick="setAttendance(${s.id}, 'sick')" title="Болеет">🤒</button>
                </div>
            </div>
        `).join('');
    } catch (e) {
        console.error('Quick lesson load error:', e);
    }
}

function setAttendance(studentId, status) {
    const row = document.querySelector(`[data-student-id="${studentId}"]`);
    row.querySelectorAll('.att-btn').forEach(btn => btn.classList.remove('selected'));
    row.querySelector(`.att-btn.${status}`).classList.add('selected');
    row.dataset.status = status;
}

async function saveQuickLesson() {
    const date = document.getElementById('ql-date').value;
    const rows = document.querySelectorAll('#quick-lesson-students > div');
    
    const attendances = [];
    rows.forEach(row => {
        if (row.dataset.status) {
            attendances.push({
                student_id: parseInt(row.dataset.studentId),
                status: row.dataset.status
            });
        }
    });
    
    if (attendances.length === 0) {
        showNotification('Отметьте хотя бы одного ученика', 'error');
        return;
    }
    
    // Create lessons with attendance
    try {
        for (const att of attendances) {
            await fetch(`${API}/api/lessons/create`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    initData,
                    lesson: {
                        student_id: att.student_id,
                        date: date,
                        status: att.status
                    }
                })
            });
        }
        
        showNotification('Занятия сохранены!', 'success');
        // Clear cache to force refresh
        DataCache.clear();
        goBack();
        loadDashboard();
    } catch (e) {
        console.error('Save quick lesson error:', e);
        showNotification('Ошибка сохранения', 'error');
    }
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
