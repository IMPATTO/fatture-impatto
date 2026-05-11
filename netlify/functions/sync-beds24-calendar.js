const BEDS24_URL = 'https://api.beds24.com/v2';
const {
  getBearerToken,
  getSupabaseUserFromToken,
  normalizeSupabaseUrl,
} = require('./_lib/shared-auth');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json; charset=utf-8',
};

let beds24TokenCache = null;

exports.handler = async (event) => {
  const corsHeaders = CORS;
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: corsHeaders, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return respond(405, { error: 'Method not allowed' }, corsHeaders);
  }

  const token = getBearerToken(event.headers);
  if (!token) {
    return respond(401, { error: 'Autenticazione mancante' }, corsHeaders);
  }

  const user = await getSupabaseUserFromToken(token);
  if (!user?.id) {
    return respond(401, { error: 'Autenticazione non valida' }, corsHeaders);
  }

  const env = {
    SUPABASE_URL: normalizeSupabaseUrl(process.env.SUPABASE_RUNTIME_URL || process.env.SUPABASE_URL),
    SUPABASE_SERVICE_ROLE_KEY: process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY,
    BEDS24_REFRESH_TOKEN: String(process.env.BEDS24_REFRESH_TOKEN || process.env.BEDS24_API_KEY || '').trim(),
  };

  if (!env.SUPABASE_URL || !env.SUPABASE_SERVICE_ROLE_KEY) {
    return respond(500, { error: 'Configurazione Supabase mancante' }, corsHeaders);
  }
  if (!env.BEDS24_REFRESH_TOKEN) {
    return respond(500, { error: 'BEDS24_REFRESH_TOKEN mancante' }, corsHeaders);
  }

  const startTime = Date.now();
  try {
    const accessToken = await getBeds24AccessToken(env);
    const units = await getApartmentUnits(env);
    if (!units.length) {
      return respond(200, {
        success: true,
        rows_upserted: 0,
        units_synced: 0,
        date_from: null,
        date_to: null,
        duration_ms: Date.now() - startTime,
      }, corsHeaders);
    }

    const today = new Date();
    const dateFrom = today.toISOString().slice(0, 10);
    const endDate = new Date(today);
    endDate.setDate(endDate.getDate() + 90);
    const dateTo = endDate.toISOString().slice(0, 10);

    const roomIds = unique(units.map((unit) => String(unit.beds24_room_id || '').trim()).filter(Boolean));
    const calendarMap = await loadCalendarOverrides(accessToken, roomIds, dateFrom, dateTo);
    const availabilityMap = await loadAvailability(accessToken, roomIds, dateFrom, dateTo);
    let offerMap = new Map();
    try {
      offerMap = await loadOfferPrices(accessToken, roomIds, dateFrom, dateTo);
    } catch (error) {
      console.warn('sync-beds24-calendar offers fallback', error.message);
    }

    const rows = [];
    const sourceUpdatedAt = new Date().toISOString();
    for (const unit of units) {
      const roomId = String(unit.beds24_room_id || '').trim();
      if (!roomId) continue;
      for (const date of eachDate(dateFrom, dateTo)) {
        const calendarEntry = calendarMap.get(`${roomId}:${date}`) || null;
        const offerEntry = offerMap.get(`${roomId}:${date}`) || null;
        const available = availabilityMap.has(`${roomId}:${date}`)
          ? availabilityMap.get(`${roomId}:${date}`)
          : null;
        const closed = calendarEntry?.closed != null
          ? Boolean(calendarEntry.closed)
          : (available == null ? false : !available);
        rows.push({
          apartment_unit_id: unit.id,
          date,
          price: normalizeNumber(offerEntry?.price ?? calendarEntry?.price),
          min_stay: normalizeInteger(calendarEntry?.minStay),
          available: available == null ? null : Boolean(available),
          closed,
          source_updated_at: sourceUpdatedAt,
          updated_at: sourceUpdatedAt,
          raw_payload: {
            room_id: roomId,
            offer: offerEntry,
            calendar: calendarEntry,
            available,
          },
        });
      }
    }

    await upsertCalendarDays(env, rows);

    return respond(200, {
      success: true,
      rows_upserted: rows.length,
      units_synced: units.length,
      date_from: dateFrom,
      date_to: dateTo,
      duration_ms: Date.now() - startTime,
    }, corsHeaders);
  } catch (error) {
    console.error('sync-beds24-calendar error', error);
    return respond(500, { error: error.message || 'Errore sync-beds24-calendar' }, corsHeaders);
  }
};

async function getBeds24AccessToken(env) {
  if (beds24TokenCache && beds24TokenCache.expiresAt > Date.now() + 15 * 1000) {
    return beds24TokenCache.token;
  }

  const response = await fetch(`${BEDS24_URL}/authentication/token`, {
    method: 'GET',
    headers: {
      Accept: 'application/json',
      refreshToken: env.BEDS24_REFRESH_TOKEN,
    },
  });

  if (!response.ok) {
    const detail = await safeReadText(response);
    throw new Error(`Beds24 auth failed: ${response.status}${detail ? ` ${detail}` : ''}`);
  }

  const data = await response.json();
  const token = String(data?.token || data?.data?.token || '').trim();
  if (!token) {
    throw new Error('Beds24 auth failed: token mancante');
  }

  const expiresInSeconds = Number(data?.expiresIn || data?.data?.expiresIn || 3600);
  beds24TokenCache = {
    token,
    expiresAt: Date.now() + Math.max(60, expiresInSeconds - 60) * 1000,
  };
  return token;
}

async function getApartmentUnits(env) {
  const url = new URL(`${env.SUPABASE_URL}/rest/v1/apartment_units`);
  url.searchParams.set('select', 'id,beds24_room_id');
  url.searchParams.set('active', 'eq.true');
  url.searchParams.set('beds24_room_id', 'not.is.null');

  const response = await fetch(url, {
    headers: {
      apikey: env.SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
    },
  });

  if (!response.ok) {
    const detail = await safeReadText(response);
    throw new Error(`Get units failed: ${response.status}${detail ? ` ${detail}` : ''}`);
  }

  const rows = await response.json();
  return Array.isArray(rows) ? rows : [];
}

async function loadCalendarOverrides(accessToken, roomIds, from, to) {
  if (!roomIds.length) return new Map();
  const params = new URLSearchParams();
  roomIds.forEach((id) => params.append('roomId', id));
  params.set('from', from);
  params.set('to', to);

  const response = await fetch(`${BEDS24_URL}/inventory/rooms/calendar?${params.toString()}`, {
    headers: {
      accept: 'application/json',
      token: accessToken,
    },
  });

  if (!response.ok) {
    const detail = await safeReadText(response);
    throw new Error(`Beds24 calendar failed: ${response.status}${detail ? ` ${detail}` : ''}`);
  }

  const payload = await response.json();
  const rows = payload?.data || payload || [];
  const map = new Map();

  rows.forEach((row) => {
    const roomKey = String(row.roomId || '');
    normalizeCalendarEntries(row.calendar || []).forEach((entry) => {
      map.set(`${roomKey}:${entry.date}`, entry);
    });
  });

  return map;
}

async function loadAvailability(accessToken, roomIds, from, to) {
  if (!roomIds.length) return new Map();
  const params = new URLSearchParams();
  roomIds.forEach((id) => params.append('roomId', id));
  params.set('dateFrom', from);
  params.set('dateTo', to);

  const response = await fetch(`${BEDS24_URL}/inventory/rooms/availability?${params.toString()}`, {
    headers: {
      accept: 'application/json',
      token: accessToken,
    },
  });

  if (!response.ok) {
    const detail = await safeReadText(response);
    throw new Error(`Beds24 availability failed: ${response.status}${detail ? ` ${detail}` : ''}`);
  }

  const payload = await response.json();
  const rows = payload?.data || payload || [];
  const map = new Map();

  rows.forEach((row) => {
    const roomKey = String(row.roomId || '');
    Object.entries(row.availability || {}).forEach(([date, available]) => {
      if (date >= from && date <= to) {
        map.set(`${roomKey}:${date}`, Boolean(available));
      }
    });
  });

  return map;
}

async function loadOfferPrices(accessToken, roomIds, from, to) {
  if (!roomIds.length) return new Map();
  const map = new Map();
  const today = new Date().toISOString().slice(0, 10);
  const eligibleDates = eachDate(from, to).filter((date) => date >= today);

  for (const date of eligibleDates) {
    const departure = addDaysUtc(parseIsoDateUtc(date), 1);
    const params = new URLSearchParams();
    roomIds.forEach((id) => params.append('roomId', id));
    params.set('arrival', date);
    params.set('departure', formatIsoDateUtc(departure));
    params.set('numAdults', '2');

    const response = await fetch(`${BEDS24_URL}/inventory/offers?${params.toString()}`, {
      headers: {
        accept: 'application/json',
        token: accessToken,
      },
    });

    if (!response.ok) {
      const detail = await safeReadText(response);
      throw new Error(`Beds24 offers failed: ${response.status}${detail ? ` ${detail}` : ''}`);
    }

    const payload = await response.json();
    const rows = payload?.data || payload || [];
    rows.forEach((row) => {
      const roomKey = String(row.roomId || '');
      const price = normalizeNumber(firstDefined(row.price, row.totalPrice, row.roomPrice, row.amount));
      if (roomKey && price !== null) {
        map.set(`${roomKey}:${date}`, {
          price,
          unitsAvailable: normalizeInteger(row.unitsAvailable),
          minStay: normalizeInteger(firstDefined(row.minStay, row.minimumStay)),
        });
      }
    });
  }

  return map;
}

async function upsertCalendarDays(env, rows) {
  const batches = chunk(rows, 500);
  for (const batch of batches) {
    const response = await fetch(
      `${env.SUPABASE_URL}/rest/v1/calendar_days?on_conflict=apartment_unit_id,date`,
      {
        method: 'POST',
        headers: {
          apikey: env.SUPABASE_SERVICE_ROLE_KEY,
          Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
          'Content-Type': 'application/json',
          Prefer: 'resolution=merge-duplicates',
        },
        body: JSON.stringify(batch),
      }
    );

    if (!response.ok) {
      const detail = await safeReadText(response);
      throw new Error(`calendar_days upsert failed: ${response.status}${detail ? ` ${detail}` : ''}`);
    }
  }
}

function normalizeCalendarEntries(entries) {
  const normalized = [];
  entries.forEach((entry) => {
    if (entry?.date) {
      normalized.push({
        date: entry.date,
        price: firstDefined(entry.price1, entry.price, entry.roomPrice),
        minStay: firstDefined(entry.minStay, entry.minimumStay),
        closed: firstDefined(entry.closed, entry.bookable === false ? true : undefined),
      });
      return;
    }

    if (entry?.from && entry?.to) {
      eachDate(entry.from, entry.to).forEach((date) => {
        normalized.push({
          date,
          price: firstDefined(entry.price1, entry.price, entry.roomPrice),
          minStay: firstDefined(entry.minStay, entry.minimumStay),
          closed: firstDefined(entry.closed, entry.bookable === false ? true : undefined),
        });
      });
    }
  });
  return normalized;
}

function eachDate(from, to) {
  const dates = [];
  let cursor = parseIsoDateUtc(from);
  const end = parseIsoDateUtc(to);
  while (cursor.getTime() <= end.getTime()) {
    dates.push(formatIsoDateUtc(cursor));
    cursor = addDaysUtc(cursor, 1);
  }
  return dates;
}

function parseIsoDateUtc(value) {
  const [year, month, day] = String(value).split('-').map((part) => Number(part));
  return new Date(Date.UTC(year, month - 1, day));
}

function formatIsoDateUtc(date) {
  return [
    date.getUTCFullYear(),
    String(date.getUTCMonth() + 1).padStart(2, '0'),
    String(date.getUTCDate()).padStart(2, '0'),
  ].join('-');
}

function addDaysUtc(date, days) {
  const copy = new Date(date.getTime());
  copy.setUTCDate(copy.getUTCDate() + days);
  return copy;
}

function firstDefined(...values) {
  return values.find((value) => value !== undefined && value !== null);
}

function normalizeNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeInteger(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.round(parsed) : null;
}

function chunk(items, size) {
  const result = [];
  for (let index = 0; index < items.length; index += size) {
    result.push(items.slice(index, index + size));
  }
  return result;
}

function unique(items) {
  return [...new Set(items)];
}

async function safeReadText(response) {
  try {
    return await response.text();
  } catch (_error) {
    return '';
  }
}

function respond(statusCode, payload, headers = CORS) {
  return {
    statusCode,
    headers,
    body: JSON.stringify(payload),
  };
}
