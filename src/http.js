export class ApiError extends Error {
  constructor(status, code, message) {
    super(message || code);
    this.status = status;
    this.code = code;
  }
}

export function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' },
  });
}

export function positiveInt(value, code = 'invalid_id') {
  const number = Number(value);
  if (!Number.isInteger(number) || number <= 0) throw new ApiError(400, code, 'Неверные данные');
  return number;
}

export function optionalInt(value) {
  return value === null || value === undefined || value === '' ? null : positiveInt(value);
}

export function text(value, maxLength = 200) {
  return String(value ?? '').trim().slice(0, maxLength);
}
