/* === Shared app shell === */

const API = window.location.origin;
const tg = window.Telegram?.WebApp;
let initData = tg?.initData || '';
let parentData = null;

function escapeHtml(value) {
    const map = {'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'};
    return String(value ?? '').replace(/[&<>"']/g, char => map[char]);
}

async function apiPost(path, payload = {}) {
    const response = await fetch(`${API}${path}`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({initData, ...payload}),
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
        const error = new Error(data.message || 'Не удалось выполнить действие');
        error.payload = data;
        throw error;
    }
    return data;
}

function showNotification(message, type = 'info') {
    document.querySelectorAll('.notification').forEach(item => item.remove());
    const toast = document.createElement('div');
    toast.className = `notification ${type}`;
    toast.setAttribute('role', type === 'error' ? 'alert' : 'status');
    toast.textContent = message;
    document.body.appendChild(toast);
    setTimeout(() => toast.remove(), 4000);
}

function renderScreenState(container, message, {retry} = {}) {
    container.innerHTML = `
        <div class="pa">
            <div class="pa-stack pa-center" style="gap: 12px">
                <p class="pa-muted">${escapeHtml(message)}</p>
                ${retry ? `<button type="button" class="pa-btn" onclick="${retry}">Повторить</button>` : ''}
            </div>
        </div>`;
}

function applySafeArea() {
    const device = tg?.safeAreaInset?.top || 0;
    const content = tg?.contentSafeAreaInset?.top || 0;
    document.documentElement.style.setProperty('--app-top-inset', `${device + content}px`);
}

async function boot() {
    const container = document.getElementById('parent-content');
    try {
        tg?.ready();
        tg?.expand();
        tg?.setHeaderColor?.('#07111f');
        tg?.setBackgroundColor?.('#07111f');
        tg?.onEvent?.('safeAreaChanged', applySafeArea);
        tg?.onEvent?.('contentSafeAreaChanged', applySafeArea);
        applySafeArea();
    } catch {
        // An older Telegram client may not support every call; the app still works without them.
    }
    if (!initData) {
        renderScreenState(container, 'Откройте приложение из Telegram: бот CRM Break Wave, кнопка меню.');
        return;
    }
    renderScreenState(container, 'Загружаем…');
    try {
        const me = await apiPost('/api/auth');
        if (me.role === 'parent') await loadParentContext();
        else if (me.role === 'guest') renderRegistration();
        else await startStaff(me);
    } catch (error) {
        renderScreenState(container, error.message, {retry: 'boot()'});
    }
}
