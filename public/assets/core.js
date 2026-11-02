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
    const result = action(control);
    if (result?.then) {
        markPending(document);
        result.finally(clearPending);
    } else {
        clearPending();
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
    }, 4000);
}

document.addEventListener('click', event => {
    const target = event.target.closest('[data-st], [data-pa], [data-reg]');
    if (!target || target.disabled) return;
    const isSelection = target.hasAttribute('aria-pressed') || target.closest('.st-nav');
    if (isSelection) haptic('selection');
    else haptic('impact', 'light');
});

// iOS applies :active on tap only when the page listens for touches.
document.addEventListener('touchstart', () => {}, {passive: true});

function renderScreenState(container, message, {retry, loading} = {}) {
    currentScreen = null;
    currentHtml = null;
    container.innerHTML = `
        <div class="pa">
            <div class="pa-stack pa-center pa-state" style="gap: 12px">
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
    renderScreenState(container, 'Загружаем…', {loading: true});
    try {
        const me = await apiPost('/api/auth');
        if (me.role === 'parent') await loadParentContext();
        else if (me.role === 'guest') renderRegistration();
        else await startStaff(me);
    } catch (error) {
        renderScreenState(container, error.message, {retry: 'boot()'});
    }
}
