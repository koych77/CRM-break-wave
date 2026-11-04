// School business rules. Dates are ISO strings (YYYY-MM-DD) in Minsk time.

export const TARIFFS = {
  single: { label: 'Разовое занятие', lessons: 1, price: 40 },
  8: { label: '8 занятий', lessons: 8, price: 150 },
  12: { label: '12 занятий', lessons: 12, price: 180 },
  16: { label: '16 занятий', lessons: 16, price: 200 },
};
const MONTHLY_TARIFFS = ['8', '12', '16'];

export const PAYMENT_DUE_DAY = 10;
export const TARIFF_DEADLINE_DAY = 5;
export const REMINDER_STEP_DAYS = 7;
export const MAKEUP_VALID_MONTHS = 2;
export const MONTHLY_MAKEUP_LIMIT = 8;
export const LOW_LESSONS = 2;

const MINSK_OFFSET_MS = 3 * 60 * 60 * 1000;
const DAY_MS = 86400000;

// Minsk has no daylight saving, so a fixed offset is exact.
export function minskNow(now = new Date()) {
  const shifted = new Date(now.getTime() + MINSK_OFFSET_MS).toISOString();
  return { date: shifted.slice(0, 10), time: shifted.slice(11, 16), hour: Number(shifted.slice(11, 13)) };
}

export function isoNow() {
  return new Date().toISOString();
}

function toUtc(date) {
  const [year, month, day] = date.split('-').map(Number);
  return Date.UTC(year, month - 1, day);
}

export function addDays(date, days) {
  return new Date(toUtc(date) + days * DAY_MS).toISOString().slice(0, 10);
}

export function daysBetween(from, to) {
  return Math.round((toUtc(to) - toUtc(from)) / DAY_MS);
}

export function weekdayOf(date) {
  return (new Date(toUtc(date)).getUTCDay() + 6) % 7;
}

export function monthBounds(date) {
  const [year, month] = date.split('-').map(Number);
  const start = `${date.slice(0, 7)}-01`;
  const last = new Date(Date.UTC(year, month, 0)).getUTCDate();
  return { start, end: `${date.slice(0, 7)}-${String(last).padStart(2, '0')}` };
}

export function dueDate(periodStart) {
  return `${periodStart.slice(0, 7)}-${String(PAYMENT_DUE_DAY).padStart(2, '0')}`;
}

export function canSelectTariff(date) {
  return Number(date.slice(8, 10)) <= TARIFF_DEADLINE_DAY;
}

// Same day two months later, clamped to the end of a shorter month.
export function makeupExpiry(sourceDate) {
  const [year, month, day] = sourceDate.split('-').map(Number);
  const target = new Date(Date.UTC(year, month - 1 + MAKEUP_VALID_MONTHS, 1));
  const last = new Date(Date.UTC(target.getUTCFullYear(), target.getUTCMonth() + 1, 0)).getUTCDate();
  target.setUTCDate(Math.min(day, last));
  return target.toISOString().slice(0, 10);
}

export function isPaymentReminderDay(today, due) {
  const left = daysBetween(today, due);
  if (left === 7 || left === 3 || left === 0) return true;
  return left < 0 && (-left - 1) % REMINDER_STEP_DAYS === 0;
}

// Two trainings a week fit 8 lessons, three fit 12, four and more fit 16.
export function tariffForSchedule(trainingsPerWeek) {
  if (trainingsPerWeek >= 4) return '16';
  return trainingsPerWeek === 3 ? '12' : '8';
}

export function nextTariff(code) {
  return MONTHLY_TARIFFS[MONTHLY_TARIFFS.indexOf(String(code)) + 1] ?? null;
}

export function isMonthlyTariff(code) {
  return MONTHLY_TARIFFS.includes(String(code));
}

export function countTrainings(schedule, from, to) {
  let count = 0;
  for (let date = from; date <= to; date = addDays(date, 1)) {
    const weekday = weekdayOf(date);
    count += schedule.filter((row) => row.weekday === weekday).length;
  }
  return count;
}

export function firstTrainingDate(schedule, from, to) {
  for (let date = from; date <= to; date = addDays(date, 1)) {
    const weekday = weekdayOf(date);
    if (schedule.some((row) => row.weekday === weekday)) return date;
  }
  return null;
}

// A child who starts mid-month pays 40 Br for each training left. When that is not cheaper than the
// chosen subscription, the month is simply that subscription, with all its lessons counted from the 1st.
export function firstMonth(lessons, code) {
  const tariff = TARIFFS[code];
  const amount = lessons * TARIFFS.single.price;
  if (amount < tariff.price) return { partial: true, amount, lessons };
  return { partial: false, amount: tariff.price, lessons: tariff.lessons };
}

// The smallest subscription that holds this many lessons, used to label a partial month.
export function coveringTariff(lessons) {
  return MONTHLY_TARIFFS.find((item) => TARIFFS[item].lessons >= lessons) ?? MONTHLY_TARIFFS[MONTHLY_TARIFFS.length - 1];
}

// A month without training is not a debt: the invoice simply stays unpaid.
export function invoiceStatus(payment, today) {
  const isDebt = payment.kind !== 'subscription' || payment.attended;
  if (payment.status === 'pending' && payment.due_date < today && isDebt) return 'overdue';
  return payment.status;
}

export function validTime(value) {
  return /^([01]\d|2[0-3]):[0-5]\d$/.test(String(value || ''));
}

export function validDate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || '')) && !Number.isNaN(Date.parse(value));
}

export function formatDate(date) {
  const months = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'];
  return `${Number(date.slice(8, 10))} ${months[Number(date.slice(5, 7)) - 1]}`;
}
