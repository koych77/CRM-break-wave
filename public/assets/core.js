/* === Shared app shell === */

const API = window.location.origin;
const tg = window.Telegram?.WebApp;
let initData = tg?.initData || '';
let parentData = null;
let coachOptions = [];

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

/* --- Icons: 24 px grid, 1.8 stroke, colored by the text around them --- */

const BW_ICONS = {
    home: 'M3 10 12 3l9 7v10a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1z M9 21v-8h6v8',
    calendar: 'M7 3v4 M17 3v4 M3 10h18 M5 5h14a2 2 0 0 1 2 2v12a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V7a2 2 0 0 1 2-2z',
    users: 'M12 20v-2a5 5 0 0 0-5-5H6a5 5 0 0 0-5 5v2 M8.5 10a4 4 0 1 0 0-8a4 4 0 0 0 0 8z M16 4a4 4 0 0 1 0 8 M17 13a5 5 0 0 1 5 5v2',
    user: 'M12 12a4 4 0 1 0 0-8a4 4 0 0 0 0 8z M4 21v-2a8 8 0 0 1 16 0v2',
    clipboard: 'M9 4H6a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V6a2 2 0 0 0-2-2h-3 M9 2h6v4H9z M8 12h8 M8 16h8',
    wallet: 'M3 7V5a2 2 0 0 1 2-2h14 M3 7h16a2 2 0 0 1 2 2v10a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z M16 14h5',
    settings: 'M12 3v2 M12 19v2 M3 12h2 M19 12h2 M5.6 5.6 7 7 M17 17l1.4 1.4 M18.4 5.6 17 7 M7 17l-1.4 1.4 M12 16a4 4 0 1 0 0-8a4 4 0 0 0 0 8z',
    check: 'M5 12l5 5L20 7',
    x: 'M18 6 6 18 M6 6l12 12',
    'chevron-left': 'M15 18l-6-6 6-6',
    'chevron-right': 'M9 18l6-6-6-6',
    plus: 'M12 5v14 M5 12h14',
    search: 'M11 18a7 7 0 1 0 0-14a7 7 0 0 0 0 14z M16 16l5 5',
    'credit-card': 'M4 5h16a2 2 0 0 1 2 2v10a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V7a2 2 0 0 1 2-2z M2 10h20 M6 15h4',
    upload: 'M12 16V3 M7 8l5-5 5 5 M4 16v4a1 1 0 0 0 1 1h14a1 1 0 0 0 1-1v-4',
    clock: 'M12 21a9 9 0 1 0 0-18a9 9 0 0 0 0 18z M12 7v5l4 2',
    bell: 'M18 8a6 6 0 0 0-12 0c0 7-3 7-3 9h18c0-2-3-2-3-9 M10 21h4',
    phone: 'M6 3h4l2 5-3 2a14 14 0 0 0 5 5l2-3 5 2v4a3 3 0 0 1-3 3A16 16 0 0 1 3 6a3 3 0 0 1 3-3z',
    edit: 'M12 5H5a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-7 M16 4l4 4-9 9-4 1 1-4z',
    trash: 'M3 6h18 M8 6V4h8v2 M6 6l1 15h10l1-15 M10 10v7 M14 10v7',
    building: 'M4 3h16v18H4z M9 21v-5h6v5 M8 8h2 M14 8h2 M8 12h2 M14 12h2',
    link: 'M10 13a5 5 0 0 0 7 .3l3-3a5 5 0 0 0-7-7l-2 2 M14 11a5 5 0 0 0-7-.3l-3 3a5 5 0 0 0 7 7l2-2',
    receipt: 'M5 3h14v18l-3-2-4 2-4-2-3 2z M8 8h8 M8 12h8 M8 16h4',
    sliders: 'M4 7h6 M14 7h6 M4 17h10 M18 17h2 M12 9a2 2 0 1 0 0-4a2 2 0 0 0 0 4z M16 19a2 2 0 1 0 0-4a2 2 0 0 0 0 4z',
};

function bwIcon(name, size = 22) {
    return `<svg class="bw-icon" width="${size}" height="${size}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="${BW_ICONS[name]}"></path></svg>`;
}

function bwInitials(name) {
    return String(name || '').trim().split(/\s+/).slice(0, 2).map(word => word[0] || '').join('').toUpperCase();
}

/* --- Telegram back button --- */

function telegramBackSupported() {
    return Boolean(tg?.BackButton && tg.isVersionAtLeast?.('6.1'));
}

// Telegram's own button replaces the one on the page, which stays only as the action to run.
function syncTelegramBack() {
    if (!telegramBackSupported()) return;
    if (document.querySelector('#parent-content [data-back]')) tg.BackButton.show();
    else tg.BackButton.hide();
}

/* --- iOS-style motion --- */

const IOS_EASE = 'cubic-bezier(0.32, 0.72, 0, 1)';
const IOS_PUSH_MS = 320;
const IOS_SPINNER = `<svg class="pa-spinner" width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" aria-hidden="true">${
    [...Array(8).keys()].map(step => `<path d="M12 3v4.2" opacity="${(step + 1) / 8}" transform="rotate(${step * 45} 12 12)"></path>`).join('')
}</svg>`;
let currentScreen = null;
let currentHtml = null;
let pendingKey = null;

function motionAllowed() {
    return !window.matchMedia('(prefers-reduced-motion: reduce)').matches;
}

function afterAnimation(animation, done) {
    animation.finished.catch(() => {}).then(done);
}

function readControls(container) {
    return {
        thumbs: [...container.querySelectorAll('.pa-segment')].map(segment => {
            const pressed = segment.querySelector('[aria-pressed="true"]');
            return pressed ? {left: pressed.offsetLeft, width: pressed.offsetWidth} : null;
        }),
        switches: [...container.querySelectorAll('.pa-switch')].map(item => item.getAttribute('aria-pressed')),
        choices: [...container.querySelectorAll('.pa-choice[aria-pressed], .st-role[aria-pressed]')].map(item => item.getAttribute('aria-pressed')),
    };
}

function slideSegmentThumb(segment, from) {
    const pressed = segment.querySelector('[aria-pressed="true"]');
    if (!pressed || !from || from.left === pressed.offsetLeft) return;
    const thumb = document.createElement('span');
    thumb.className = 'pa-segment-thumb';
    thumb.style.left = `${pressed.offsetLeft}px`;
    thumb.style.width = `${pressed.offsetWidth}px`;
    segment.classList.add('is-sliding');
    segment.prepend(thumb);
    const animation = thumb.animate(
        [{transform: `translateX(${from.left - pressed.offsetLeft}px)`}, {transform: 'none'}],
        {duration: 280, easing: IOS_EASE},
    );
    afterAnimation(animation, () => {
        thumb.remove();
        segment.classList.remove('is-sliding');
    });
}

function animateControls(container, before) {
    const after = readControls(container);
    container.querySelectorAll('.pa-segment').forEach((segment, index) => slideSegmentThumb(segment, before.thumbs[index]));
    container.querySelectorAll('.pa-switch').forEach((item, index) => {
        if (before.switches[index] !== after.switches[index]) {
            item.classList.add(after.switches[index] === 'true' ? 'is-turning-on' : 'is-turning-off');
        }
    });
    if (before.choices.length !== after.choices.length) return;
    container.querySelectorAll('.pa-choice[aria-pressed], .st-role[aria-pressed]').forEach((item, index) => {
        if (before.choices[index] !== after.choices[index]) item.classList.add('is-toggled');
    });
}

// The previous screen stays on top of or under the new one until the slide ends.
function slideScreens(container, oldScreen, scrollTop, isPush) {
    const ghost = document.createElement('div');
    ghost.className = `pa-ghost ${isPush ? 'is-under' : 'is-over'}`;
    ghost.inert = true;
    ghost.append(oldScreen);
    document.body.append(ghost);
    ghost.scrollTop = scrollTop;
    const options = {duration: IOS_PUSH_MS, easing: IOS_EASE};
    const behind = [{transform: 'translateX(0)', opacity: 1}, {transform: 'translateX(-30%)', opacity: 0.5}];
    const front = [{transform: 'translateX(100%)'}, {transform: 'translateX(0)'}];
    const moves = isPush
        ? [container.animate(front, options), ghost.animate(behind, options)]
        : [container.animate([...behind].reverse(), options), ghost.animate([...front].reverse(), options)];
    afterAnimation(moves[1], () => ghost.remove());
}

// Paints a screen. Depth sets the direction: deeper slides in from the right, shallower goes back.
function paintScreen(container, html, key, depth) {
    const previous = currentScreen;
    // Unchanged content keeps its DOM, so a background refresh never interrupts a tap or a scroll.
    if (previous?.key === key && currentHtml === html) return;
    currentHtml = html;
    const oldScreen = container.firstElementChild;
    const scrollTop = window.scrollY;
    const before = readControls(container);
    container.innerHTML = html;
    currentScreen = {key, depth};
    markPending(container);
    syncTelegramBack();
    if (!motionAllowed()) return;
    if (previous?.key === key) {
        animateControls(container, before);
    } else if (!previous || previous.depth === depth) {
        const part = previous ? container.querySelector('.st-page, .pa-body') : container;
        part?.animate([{opacity: 0}, {opacity: 1}], {duration: 180, easing: 'ease-out'});
        if (previous) animateControls(container, before);
    } else if (oldScreen) {
        slideScreens(container, oldScreen, scrollTop, depth > previous.depth);
    }
}

function controlKey(control) {
    const {st, pa, reg, value, kind, id, child, row} = control.dataset;
    return [st, pa, reg, value, kind, id, child, row].join('|');
}

function markPending(root) {
    if (!pendingKey) return;
    root.querySelectorAll('[data-st], [data-pa], [data-reg]').forEach(control => {
        if (controlKey(control) === pendingKey) control.classList.add('is-pending');
    });
}

function clearPending() {
    pendingKey = null;
    document.querySelectorAll('.is-pending').forEach(control => control.classList.remove('is-pending'));
}

// Shows a tapped control as busy until its request ends and ignores repeated taps on it.
function runControl(control, action) {
    if (pendingKey === controlKey(control)) return;
    pendingKey = controlKey(control);
    let result;
    try {
        result = action(control);
    } finally {
        if (!result?.then) clearPending();
    }
    if (result?.then) {
        markPending(document);
        result.catch(() => {}).then(clearPending);
    }
}

function haptic(type, style) {
    const feedback = tg?.HapticFeedback;
    if (!feedback || !tg.isVersionAtLeast?.('6.1')) return;
    if (type === 'selection') feedback.selectionChanged();
    else if (type === 'notification') feedback.notificationOccurred(style);
    else feedback.impactOccurred(style);
}

function showNotification(message, type = 'info') {
    document.querySelectorAll('.notification').forEach(item => item.remove());
    const toast = document.createElement('div');
    toast.className = `notification ${type}`;
    toast.setAttribute('role', type === 'error' ? 'alert' : 'status');
    if (document.querySelector('.pa-nav, .pa-foot')) toast.classList.add('is-raised');
    toast.textContent = message;
    document.body.appendChild(toast);
    haptic('notification', type === 'error' ? 'error' : 'success');
    setTimeout(() => {
        if (!motionAllowed()) {
            toast.remove();
            return;
        }
        const fade = toast.animate([{opacity: 1}, {opacity: 0, transform: 'translateY(8px) scale(0.98)'}], {duration: 200, easing: 'ease-in', fill: 'forwards'});
        afterAnimation(fade, () => toast.remove());
    }, 3000);
}

document.addEventListener('click', event => {
    const target = event.target.closest('[data-st], [data-pa], [data-reg]');
    if (!target || target.disabled) return;
    const isSelection = target.hasAttribute('aria-pressed') || target.closest('.pa-nav');
    if (isSelection) haptic('selection');
    else haptic('impact', 'light');
});

// iOS applies :active on tap only when the page listens for touches.
document.addEventListener('touchstart', () => {}, {passive: true});

function renderScreenState(container, message, {retry, loading} = {}) {
    currentScreen = null;
    currentHtml = null;
    tg?.BackButton?.hide?.();
    container.innerHTML = `
        <div class="pa">
            <div class="pa-stack pa-center pa-state">
                ${loading ? IOS_SPINNER : ''}
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
        tg?.setHeaderColor?.('#000000');
        tg?.setBackgroundColor?.('#000000');
        if (tg?.isVersionAtLeast?.('7.7')) tg.disableVerticalSwipes?.();
        if (telegramBackSupported()) {
            document.documentElement.classList.add('tg-back');
            tg.BackButton.onClick(() => document.querySelector('#parent-content [data-back]')?.click());
        }
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
    renderScreenState(container, 'Загружаем…', {loading: true});
    try {
        const me = await apiPost('/api/auth');
        coachOptions = me.coach_options || [];
        if (me.role === 'parent') await loadParentContext();
        else if (me.role === 'guest') renderRegistration();
        else await startStaff(me);
    } catch (error) {
        renderScreenState(container, error.message, {retry: 'boot()'});
    }
}
