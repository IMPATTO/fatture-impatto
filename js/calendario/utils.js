export function formatMonthLabel(date) {
  return date.toLocaleDateString('it-IT', { month: 'long', year: 'numeric' }).replace(/^\w/, (c) => c.toUpperCase());
}

export function formatRangeLabel(date) {
  const { start, end } = getMonthBounds(date);
  const from = formatDayMonthYear(parseIsoDate(start));
  const to = formatDayMonthYear(parseIsoDate(end));
  return `Dal ${from} al ${to}`;
}

export function formatWeekday(date) {
  return date.toLocaleDateString('it-IT', { weekday: 'short' }).replace('.', '').toUpperCase();
}

export function formatDayMonthYear(date) {
  return date.toLocaleDateString('it-IT', { day: 'numeric', month: 'long', year: 'numeric' });
}

export function formatDate(value) {
  if (!value) return '—';
  return parseIsoDate(value).toLocaleDateString('it-IT', { day: '2-digit', month: '2-digit', year: 'numeric' });
}

export function formatDateTime(value) {
  if (!value) return '—';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString('it-IT', { day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit' });
}

export function formatPrice(value) {
  return new Intl.NumberFormat('it-IT', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(Number(value || 0));
}

export function minutesAgoLabel(value) {
  const then = new Date(value);
  const diff = Math.max(0, Math.round((Date.now() - then.getTime()) / 60000));
  if (diff < 1) return 'meno di un minuto fa';
  if (diff === 1) return '1 minuto fa';
  if (diff < 60) return `${diff} minuti fa`;
  const hours = Math.round(diff / 60);
  return hours === 1 ? '1 ora fa' : `${hours} ore fa`;
}

export function nightsBetween(checkIn, checkOut) {
  return diffDays(parseIsoDate(checkIn), parseIsoDate(checkOut));
}

export function getMonthBounds(date) {
  const startDate = startOfMonth(date);
  const endDate = new Date(startDate.getFullYear(), startDate.getMonth() + 1, 0);
  return {
    start: isoDateLocal(startDate),
    end: isoDateLocal(endDate),
  };
}

export function getMonthDays(date) {
  const first = startOfMonth(date);
  const days = [];
  const cursor = new Date(first);
  while (cursor.getMonth() === first.getMonth()) {
    days.push(new Date(cursor));
    cursor.setDate(cursor.getDate() + 1);
  }
  return days;
}

export function getDayWidth() {
  const value = getComputedStyle(document.documentElement)
    .getPropertyValue('--day-width').trim();
  return parseInt(value, 10) || 48;
}

export function startOfMonth(date) {
  return new Date(date.getFullYear(), date.getMonth(), 1);
}

export function parseIsoDate(value) {
  const [year, month, day] = String(value).split('-').map(Number);
  return new Date(year, month - 1, day);
}

export function isoDateLocal(date) {
  const year = date.getFullYear();
  const month = `${date.getMonth() + 1}`.padStart(2, '0');
  const day = `${date.getDate()}`.padStart(2, '0');
  return `${year}-${month}-${day}`;
}

export function toMonthInputValue(date) {
  return `${date.getFullYear()}-${`${date.getMonth() + 1}`.padStart(2, '0')}`;
}

export function addDays(date, amount) {
  const next = new Date(date);
  next.setDate(next.getDate() + amount);
  return next;
}

export function diffDays(a, b) {
  const ms = parseIsoDate(isoDateLocal(b)).getTime() - parseIsoDate(isoDateLocal(a)).getTime();
  return Math.round(ms / 86400000);
}

export function isWeekend(date) {
  const day = date.getDay();
  return day === 0 || day === 6;
}

export function isSameDate(left, right) {
  return isoDateLocal(left) === isoDateLocal(right);
}

export function normalizeText(value) {
  const text = String(value ?? '').trim();
  return text || '';
}

export function titleCase(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

export function sum(values) {
  return values.reduce((total, value) => total + Number(value || 0), 0);
}

export function esc(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

export function groupBy(items, keyFn, sortFn = null) {
  const map = new Map();
  for (const item of items) {
    const key = keyFn(item);
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(item);
  }
  if (sortFn) {
    for (const entry of map.values()) {
      entry.sort(sortFn);
    }
  }
  return map;
}

export function unique(values) {
  return [...new Set(values)];
}

export function channelLabel(value, channelConfig) {
  if (!value) return 'Altro';
  return channelConfig?.[value]?.label || titleCase(String(value).replace(/_/g, ' '));
}

export function compareBookingsForRender(a, b) {
  const inCompare = String(a.check_in).localeCompare(String(b.check_in));
  if (inCompare !== 0) return inCompare;
  return String(a.beds24_booking_id).localeCompare(String(b.beds24_booking_id), 'it');
}

export function compareOrphans(a, b) {
  const inCompare = String(a.check_in).localeCompare(String(b.check_in));
  if (inCompare !== 0) return inCompare;
  return String(a.beds24_booking_id).localeCompare(String(b.beds24_booking_id), 'it');
}

export function bookingSpanWithinMonth(booking, monthStart, monthEndExclusive) {
  const bookingStart = parseIsoDate(booking.check_in);
  const bookingEnd = parseIsoDate(booking.check_out);
  const start = bookingStart > monthStart ? bookingStart : monthStart;
  const end = bookingEnd < monthEndExclusive ? bookingEnd : monthEndExclusive;
  const days = diffDays(start, end);
  if (days <= 0) return null;
  return {
    startIndex: diffDays(monthStart, start),
    days,
    continuesBefore: bookingStart < monthStart,
    continuesAfter: bookingEnd > monthEndExclusive,
  };
}
