import { coachOptions, loadActor, safeEqual } from './auth.js';
import { handleUpdate } from './bot.js';
import { runTick } from './cron.js';
import { first } from './db.js';
import { ApiError, json } from './http.js';
import * as parent from './parent.js';
import * as staff from './staff.js';
import { configureBot, flushOutbox } from './telegram.js';

const routes = [
  ['/api/parent/register', parent.register],
  ['/api/parent/context', parent.context],
  ['/api/parent/training-response', parent.trainingResponse],
  ['/api/parent/reminders', parent.reminders],
  ['/api/parent/tariff', parent.chooseTariff],
  ['/api/parent/schedule-request', parent.requestSchedule],
  ['/api/parent/payments/:id/report', parent.reportPayment],
  ['/api/parent/payments/:id/receipt', parent.uploadReceipt],
  ['/api/parent/makeups/:id/request', parent.requestMakeup],
  ['/api/staff/resources', staff.resources],
  ['/api/staff/day', staff.day],
  ['/api/staff/attendance', staff.saveMarks],
  ['/api/staff/cancel-session', staff.cancelSession],
  ['/api/staff/students', staff.students],
  ['/api/staff/students/create', staff.createStudent],
  ['/api/staff/students/:id', staff.studentDetail],
  ['/api/staff/students/:id/update', staff.updateStudent],
  ['/api/staff/students/:id/delete', staff.deleteStudent],
  ['/api/staff/requests', staff.requests],
  ['/api/staff/registrations/:id/review', staff.reviewRegistration],
  ['/api/staff/makeups/:id/review', staff.reviewMakeup],
  ['/api/staff/schedule-requests/:id/review', staff.reviewSchedule],
  ['/api/admin/payments/:id/review', staff.reviewPayment],
  ['/api/admin/payments/:id/receipt', staff.viewReceipt],
  ['/api/admin/payments/:id/cash', staff.takeCash],
  ['/api/admin/payments/:id/remind', staff.remindPayment],
  ['/api/admin/money', staff.money],
  ['/api/admin/team', staff.team],
  ['/api/admin/coaches/:id/roles', staff.setRoles],
  ['/api/admin/locations/create', staff.locationHandlers.create],
  ['/api/admin/locations/:id/delete', staff.locationHandlers.remove],
  ['/api/admin/groups/create', staff.groupHandlers.create],
  ['/api/admin/groups/:id/delete', staff.groupHandlers.remove],
].map(([path, handler]) => [new RegExp(`^${path.replace(':id', '(?<id>\\d+)')}$`), handler]);

async function authenticate(env, body) {
  const actor = await loadActor(env, body.initData);
  const coaches = coachOptions(env).map((item) => ({ name: item.name, username: item.username }));
  if (actor.coach) {
    const { coach } = actor;
    return {
      role: coach.can_admin ? 'admin' : 'coach',
      coach_id: coach.id,
      first_name: coach.first_name,
      is_admin: coach.can_admin,
    };
  }
  if (actor.parent) {
    return { role: 'parent', parent_id: actor.parent.id, first_name: actor.parent.full_name, coach_options: coaches };
  }
  // Anyone new opens the family form, whichever button brought them here.
  return { role: 'guest', first_name: actor.user.first_name || '', coach_options: coaches };
}

async function handleApi(request, env, ctx, pathname) {
  if (request.method !== 'POST') return json({ error: 'method_not_allowed' }, 405);
  const multipart = (request.headers.get('content-type') || '').startsWith('multipart/form-data');
  const form = multipart ? await request.formData() : null;
  const body = multipart ? {} : await request.json().catch(() => ({}));
  const initData = multipart ? form.get('initData') : body.initData;

  if (pathname === '/api/auth') return json(await authenticate(env, { initData }));

  for (const [pattern, handler] of routes) {
    const match = pathname.match(pattern);
    if (!match) continue;
    const actor = await loadActor(env, initData);
    const result = await handler({
      env,
      db: env.DB,
      actor,
      body,
      form,
      params: match.groups || {},
      request,
    });
    ctx.waitUntil(flushOutbox(env, 10).catch((error) => console.error('outbox', error)));
    return result instanceof Response ? result : json(result);
  }
  return json({ error: 'not_found' }, 404);
}

export default {
  async fetch(request, env, ctx) {
    const { pathname } = new URL(request.url);
    try {
      if (pathname === '/healthz') {
        await first(env.DB, 'SELECT 1 AS ok');
        return json({ status: 'ok' });
      }
      if (pathname.startsWith('/telegram/')) {
        const secret = request.headers.get('x-telegram-bot-api-secret-token') || '';
        if (!env.TELEGRAM_WEBHOOK_SECRET || !safeEqual(secret, env.TELEGRAM_WEBHOOK_SECRET)) {
          return json({ error: 'forbidden' }, 403);
        }
        if (pathname === '/telegram/setup') return json(await configureBot(env));
        if (pathname === '/telegram/webhook') {
          const update = await request.json();
          // Telegram retries on errors, so a failing update is logged and acknowledged.
          ctx.waitUntil(handleUpdate(env, update).catch((error) => console.error('update', error)));
          return json({ ok: true });
        }
        return json({ error: 'not_found' }, 404);
      }
      if (pathname.startsWith('/api/')) return await handleApi(request, env, ctx, pathname);
      return env.ASSETS.fetch(request);
    } catch (error) {
      if (error instanceof ApiError) {
        return json({ error: error.code, message: error.message }, error.status);
      }
      console.error(pathname, error);
      return json({ error: 'server_error', message: 'Ошибка сервера. Повторите попытку.' }, 500);
    }
  },

  scheduled(event, env, ctx) {
    ctx.waitUntil(runTick(env, new Date(event.scheduledTime)));
  },
};
