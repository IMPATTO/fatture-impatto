const { createClient } = require('@supabase/supabase-js');
const {
  getInternalSupabaseUserFromHeaders,
  normalizeSupabaseUrl,
} = require('./_lib/shared-auth');

const ROME_TZ = 'Europe/Rome';
const SCHEDULE = '0 6 * * *';
const ACTOR_EMAIL = 'system@daily-checkin-checkout-notify';
const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Content-Type': 'application/json; charset=utf-8',
};
const RESEND_ENDPOINT = 'https://api.resend.com/emails';

exports.config = {
  schedule: SCHEDULE,
};

exports.handler = async (event = {}) => {
  if (event.httpMethod === 'OPTIONS') {
    return jsonResponse(204, { ok: true });
  }

  const isHttp = Boolean(event.httpMethod);
  if (isHttp && !['GET', 'POST'].includes(event.httpMethod)) {
    return jsonResponse(405, { error: 'Method not allowed' });
  }

  let requestBody = {};
  if (isHttp && event.httpMethod === 'POST' && event.body) {
    try {
      requestBody = JSON.parse(event.body);
    } catch (_error) {
      return jsonResponse(400, { error: 'Invalid JSON body' });
    }
  }

  const query = event.queryStringParameters || {};
  const dryRun = readBooleanParam(query.dryRun, requestBody.dryRun, false);
  const targetDate = normalizeIsoDate(query.date || requestBody.date) || getRomeIsoDate(new Date());
  let actor = ACTOR_EMAIL;

  if (isHttp) {
    const auth = await getInternalSupabaseUserFromHeaders(event.headers || {});
    if (!auth?.email) {
      return jsonResponse(401, { error: 'Unauthorized' });
    }
    actor = auth.email;
  }

  const env = getRuntimeConfig();
  const recipients = parseRecipientList(env.notificationEmails);

  if (!env.supabaseUrl || !env.serviceRoleKey) {
    return jsonResponse(500, { error: 'Configurazione Supabase mancante' });
  }

  if (!dryRun) {
    if (!env.resendApiKey) {
      return jsonResponse(500, { error: 'RESEND_API_KEY mancante' });
    }
    if (!env.emailFrom) {
      return jsonResponse(500, { error: 'EMAIL_FROM mancante' });
    }
    if (!recipients.length) {
      return jsonResponse(500, { error: 'DAILY_CHECKIN_NOTIFICATION_EMAILS mancante' });
    }
  }

  const supabase = createClient(env.supabaseUrl, env.serviceRoleKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  });

  try {
    const columnSupport = await detectColumnSupport(supabase);
    const rows = await loadOperationalGuests(supabase, targetDate, columnSupport);
    const summary = buildDailySummary(targetDate, rows);
    const email = buildEmail(summary);

    if (dryRun) {
      return jsonResponse(200, {
        success: true,
        dry_run: true,
        actor,
        schedule_utc: SCHEDULE,
        recipients_preview: recipients,
        summary: buildSummaryPayload(summary),
        email_preview: email,
      });
    }

    const sendResult = await sendSummaryEmail({
      resendApiKey: env.resendApiKey,
      emailFrom: env.emailFrom,
      recipients,
      subject: email.subject,
      text: email.text,
      html: email.html,
    });

    return jsonResponse(200, {
      success: true,
      actor,
      schedule_utc: SCHEDULE,
      recipients,
      resend_email_id: sendResult.id || null,
      summary: buildSummaryPayload(summary),
    });
  } catch (error) {
    console.error('[daily-checkin-checkout-notify] fatal', error);
    return jsonResponse(500, {
      success: false,
      error: error.message || 'Errore invio notifica giornaliera',
      actor,
      date: targetDate,
    });
  }
};

function getRuntimeConfig() {
  return {
    supabaseUrl: normalizeSupabaseUrl(process.env.SUPABASE_RUNTIME_URL || process.env.SUPABASE_URL),
    serviceRoleKey: process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY || '',
    resendApiKey: String(process.env.RESEND_API_KEY || '').trim(),
    emailFrom: String(process.env.EMAIL_FROM || '').trim(),
    notificationEmails: String(process.env.DAILY_CHECKIN_NOTIFICATION_EMAILS || '').trim(),
  };
}

function jsonResponse(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: statusCode === 204 ? '' : JSON.stringify(payload),
  };
}

function readBooleanParam(...values) {
  for (const value of values) {
    if (value == null || value === '') continue;
    const normalized = String(value).trim().toLowerCase();
    if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
    if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  }
  return false;
}

function normalizeIsoDate(value) {
  const normalized = String(value || '').trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(normalized) ? normalized : '';
}

function parseRecipientList(rawValue) {
  return String(rawValue || '')
    .split(/[\n,;]+/)
    .map((value) => value.trim())
    .filter(Boolean);
}

function getRomeIsoDate(date) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: ROME_TZ,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date);
}

async function detectColumnSupport(supabase) {
  const names = ['capogruppo_id', 'tag_prenotazione', 'numero_persone'];
  const entries = await Promise.all(names.map(async (name) => [name, await hasColumn(supabase, 'ospiti_check_in', name)]));
  return Object.fromEntries(entries);
}

async function hasColumn(supabase, tableName, columnName) {
  const { error } = await supabase
    .from(tableName)
    .select(columnName)
    .limit(1);

  if (!error) return true;

  const message = String(error.message || '').toLowerCase();
  if (message.includes('column') && message.includes('does not exist')) {
    return false;
  }

  console.warn('[daily-checkin-checkout-notify] column probe fallback', {
    tableName,
    columnName,
    message: error.message,
  });
  return false;
}

function buildGuestSelect(columnSupport) {
  const baseColumns = [
    'id',
    'nome',
    'cognome',
    'data_checkin',
    'data_checkout',
    'stato',
    'alloggiati_stato',
    'apartments(nome_appartamento)',
  ];

  if (columnSupport.capogruppo_id) baseColumns.push('capogruppo_id');
  if (columnSupport.tag_prenotazione) baseColumns.push('tag_prenotazione');
  if (columnSupport.numero_persone) baseColumns.push('numero_persone');

  return baseColumns.join(',');
}

async function loadOperationalGuests(supabase, targetDate, columnSupport) {
  let query = supabase
    .from('ospiti_check_in')
    .select(buildGuestSelect(columnSupport))
    .or(`data_checkin.eq.${targetDate},data_checkout.eq.${targetDate}`)
    .neq('stato', 'SCARTATA')
    .order('data_checkin', { ascending: true })
    .order('cognome', { ascending: true });

  if (columnSupport.capogruppo_id) {
    query = query.is('capogruppo_id', null);
  }

  const { data, error } = await query;
  if (error) {
    throw new Error(`Errore lettura ospiti_check_in: ${error.message}`);
  }

  return Array.isArray(data) ? data : [];
}

function buildDailySummary(targetDate, rows) {
  const arrivals = [];
  const departures = [];
  const apartmentsTouched = new Set();

  for (const row of rows) {
    const apartmentName = normalizeApartmentName(row);
    const item = {
      apartment: apartmentName,
      guest: normalizeGuestName(row),
      peopleCount: normalizePeopleCount(row.numero_persone),
      bookingTag: normalizeText(row.tag_prenotazione),
      status: normalizeText(row.stato),
      alloggiatiStatus: normalizeText(row.alloggiati_stato),
    };

    if (row.data_checkin === targetDate) {
      arrivals.push(item);
      apartmentsTouched.add(apartmentName);
    }
    if (row.data_checkout === targetDate) {
      departures.push(item);
      apartmentsTouched.add(apartmentName);
    }
  }

  return {
    date: targetDate,
    arrivals,
    departures,
    arrivalsByApartment: groupByApartment(arrivals),
    departuresByApartment: groupByApartment(departures),
    apartmentsTouched: apartmentsTouched.size,
  };
}

function normalizeApartmentName(row) {
  const apartment = Array.isArray(row?.apartments) ? row.apartments[0] : row?.apartments;
  return normalizeText(apartment?.nome_appartamento) || 'Appartamento non assegnato';
}

function normalizeGuestName(row) {
  const firstName = normalizeText(row?.nome);
  const lastName = normalizeText(row?.cognome);
  return [firstName, lastName].filter(Boolean).join(' ') || 'Ospite senza nome';
}

function normalizePeopleCount(value) {
  const count = Number.parseInt(String(value || '1'), 10);
  return Number.isFinite(count) && count > 0 ? count : 1;
}

function normalizeText(value) {
  return String(value || '').trim();
}

function groupByApartment(items) {
  const map = new Map();

  for (const item of items) {
    const key = item.apartment;
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(item);
  }

  return Array.from(map.entries())
    .sort(([left], [right]) => left.localeCompare(right, 'it'))
    .map(([apartment, entries]) => ({
      apartment,
      entries: entries.slice().sort((left, right) => left.guest.localeCompare(right.guest, 'it')),
    }));
}

function buildSummaryPayload(summary) {
  return {
    date: summary.date,
    arrivals_count: summary.arrivals.length,
    departures_count: summary.departures.length,
    apartments_touched: summary.apartmentsTouched,
    arrivals_by_apartment: summary.arrivalsByApartment,
    departures_by_apartment: summary.departuresByApartment,
  };
}

function buildEmail(summary) {
  const subject = [
    `Operativita ${formatDateIt(summary.date)}`,
    `${summary.arrivals.length} check-in`,
    `${summary.departures.length} check-out`,
  ].join(' | ');

  const introLine = summary.arrivals.length || summary.departures.length
    ? `Riepilogo del ${formatDateIt(summary.date)} per ${summary.apartmentsTouched} appartamenti.`
    : `Nessun check-in e nessun check-out registrato per il ${formatDateIt(summary.date)}.`;

  const textParts = [
    introLine,
    '',
    `Check-in: ${summary.arrivals.length}`,
    `Check-out: ${summary.departures.length}`,
    '',
    renderTextSection('Check-in', summary.arrivalsByApartment, 'Nessun check-in previsto oggi.'),
    '',
    renderTextSection('Check-out', summary.departuresByApartment, 'Nessun check-out previsto oggi.'),
  ];

  const htmlParts = [
    `<h2>Riepilogo operativita ${escapeHtml(formatDateIt(summary.date))}</h2>`,
    `<p>${escapeHtml(introLine)}</p>`,
    `<p><strong>Check-in:</strong> ${summary.arrivals.length}<br/><strong>Check-out:</strong> ${summary.departures.length}</p>`,
    renderHtmlSection('Check-in', summary.arrivalsByApartment, 'Nessun check-in previsto oggi.'),
    renderHtmlSection('Check-out', summary.departuresByApartment, 'Nessun check-out previsto oggi.'),
  ];

  return {
    subject,
    text: textParts.join('\n'),
    html: htmlParts.join('\n'),
  };
}

function renderTextSection(title, groups, emptyMessage) {
  if (!groups.length) {
    return `${title}\n${emptyMessage}`;
  }

  const lines = [title];
  for (const group of groups) {
      lines.push(`- ${group.apartment}`);
    for (const entry of group.entries) {
      lines.push(`  - ${formatGuestEntry(entry)}`);
    }
  }
  return lines.join('\n');
}

function renderHtmlSection(title, groups, emptyMessage) {
  if (!groups.length) {
    return `<h3>${escapeHtml(title)}</h3><p>${escapeHtml(emptyMessage)}</p>`;
  }

  const blocks = [`<h3>${escapeHtml(title)}</h3>`];
  for (const group of groups) {
    blocks.push(`<p><strong>${escapeHtml(group.apartment)}</strong></p>`);
    blocks.push('<ul>');
    for (const entry of group.entries) {
      blocks.push(`<li>${escapeHtml(formatGuestEntry(entry))}</li>`);
    }
    blocks.push('</ul>');
  }
  return blocks.join('\n');
}

function formatGuestEntry(entry) {
  const bits = [entry.guest];
  if (entry.peopleCount > 1) {
    bits.push(`${entry.peopleCount} ospiti`);
  }
  if (entry.bookingTag) {
    bits.push(`prenotazione ${entry.bookingTag}`);
  }
  return bits.join(' | ');
}

function formatDateIt(isoDate) {
  const [year, month, day] = String(isoDate || '').split('-');
  if (!year || !month || !day) return String(isoDate || '');
  return `${day}/${month}/${year}`;
}

function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

async function sendSummaryEmail({
  resendApiKey,
  emailFrom,
  recipients,
  subject,
  text,
  html,
}) {
  const response = await fetch(RESEND_ENDPOINT, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${resendApiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      from: emailFrom,
      to: recipients,
      subject,
      text,
      html,
    }),
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Invio email notifica fallito: HTTP ${response.status} ${body.slice(0, 400)}`);
  }

  try {
    return await response.json();
  } catch (_error) {
    return {};
  }
}
