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

const BASE_URL = BEDS24_URL;

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
    console.warn('[AUTH-FAIL]', 'Autenticazione mancante');
    return respond(401, { error: 'Autenticazione mancante' }, corsHeaders);
  }

  const user = await getSupabaseUserFromToken(token);
  if (!user?.id) {
    console.warn('[AUTH-FAIL]', 'Autenticazione non valida');
    return respond(401, { error: 'Autenticazione non valida' }, corsHeaders);
  }
  console.log('[AUTH-OK] user:', user.email || user.id);

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

  const requestUrl = new URL(event.rawUrl || `https://local.invalid${event.path || '/.netlify/functions/sync-beds24-calendar-background'}`);
  const daysParam = requestUrl.searchParams.get('days');
  const parsedDays = Number.parseInt(String(daysParam || '60'), 10);
  const daysToSync = Number.isFinite(parsedDays)
    ? Math.max(1, Math.min(90, parsedDays))
    : 60;

  const startTime = Date.now();

  try {
    const accessToken = await getBeds24AccessToken(env);
    const units = await getApartmentUnits(env);
    if (!units.length) {
      return respond(200, {
        success: true,
        days_synced: daysToSync,
        units_synced: 0,
        tasks_processed: 0,
        rows_upserted: 0,
        errors: 0,
        duration_ms: Date.now() - startTime,
      }, corsHeaders);
    }

    const { rows, processedUnits, errorsCount } = await syncCalendarPrices(accessToken, units, daysToSync);
    const upsertedCount = await upsertCalendarDays(env, rows);

    return respond(200, {
      success: true,
      days_synced: daysToSync,
      units_synced: units.length,
      tasks_processed: processedUnits,
      rows_upserted: upsertedCount,
      errors: errorsCount,
      duration_ms: Date.now() - startTime,
    }, corsHeaders);
  } catch (error) {
    console.error('sync-beds24-calendar-background error', error);
    return respond(500, { error: error.message || 'Errore sync-beds24-calendar-background' }, corsHeaders);
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

async function getCalendarForRoom(accessToken, roomId, startDate, endDate) {
  const url = `${BASE_URL}/inventory/rooms/calendar?roomId=${encodeURIComponent(roomId)}&from=${encodeURIComponent(startDate)}&to=${encodeURIComponent(endDate)}&includePrices=true&includeMinStay=true`;

  let response = await fetch(url, {
    method: 'GET',
    headers: {
      token: accessToken,
      Accept: 'application/json',
    },
  });

  if (response.status === 429) {
    await delay(5000);
    response = await fetch(url, {
      method: 'GET',
      headers: {
        token: accessToken,
        Accept: 'application/json',
      },
    });
    if (!response.ok) {
      return { ranges: [], error: '429-after-retry' };
    }
    const retryData = await response.json();
    return parseCalendarResponse(retryData);
  }

  if (!response.ok) {
    console.warn(`[CALENDAR-FAIL] room=${roomId} status=${response.status}`);
    return { ranges: [], error: String(response.status) };
  }

  const data = await response.json();
  return parseCalendarResponse(data);
}

async function getAvailabilityForRoom(accessToken, roomId, startDate, endDate) {
  const url = `${BASE_URL}/inventory/rooms/availability?roomId=${encodeURIComponent(roomId)}&dateFrom=${encodeURIComponent(startDate)}&dateTo=${encodeURIComponent(endDate)}`;

  let response = await fetch(url, {
    method: 'GET',
    headers: {
      token: accessToken,
      Accept: 'application/json',
    },
  });

  if (response.status === 429) {
    await delay(5000);
    response = await fetch(url, {
      method: 'GET',
      headers: {
        token: accessToken,
        Accept: 'application/json',
      },
    });
    if (!response.ok) {
      return { availability: new Map(), error: '429-after-retry' };
    }
    const retryData = await response.json();
    return parseAvailabilityResponse(retryData);
  }

  if (!response.ok) {
    console.warn(`[AVAILABILITY-FAIL] room=${roomId} status=${response.status}`);
    return { availability: new Map(), error: String(response.status) };
  }

  const data = await response.json();
  return parseAvailabilityResponse(data);
}

function parseCalendarResponse(parsed) {
  if (!parsed?.success || !Array.isArray(parsed.data)) {
    return { ranges: [], error: 'no-data' };
  }

  const roomData = parsed.data[0];
  if (!roomData?.calendar || !Array.isArray(roomData.calendar)) {
    return { ranges: [] };
  }

  const ranges = roomData.calendar.map((r) => ({
    from: r.from,
    to: r.to,
    price1: typeof r.price1 === 'number' ? r.price1 : null,
    minStay: typeof r.minStay === 'number' ? r.minStay : null,
    closed: normalizeCalendarClosed(r),
    available: normalizeCalendarAvailable(r),
    raw: r,
  }));
  return { ranges };
}

function parseAvailabilityResponse(parsed) {
  if (!parsed?.success || !Array.isArray(parsed.data)) {
    return { availability: new Map(), error: 'no-data' };
  }

  const roomData = parsed.data[0];
  const availability = new Map();
  Object.entries(roomData?.availability || {}).forEach(([date, value]) => {
    availability.set(date, value === null ? null : Boolean(value));
  });

  return { availability };
}

function expandRangeToDays(range) {
  const days = [];
  const start = parseIsoDateUtc(range.from);
  const end = parseIsoDateUtc(range.to);

  for (let d = new Date(start); d <= end; d = addDaysUtc(d, 1)) {
    days.push({
      date: formatIsoDateUtc(d),
      price: range.price1,
      minStay: range.minStay,
      closed: range.closed,
      available: range.available,
      raw_range: range.raw,
    });
  }

  return days;
}

function expandAllRangesToDays(ranges) {
  const dayMap = new Map();
  for (const range of ranges) {
    const days = expandRangeToDays(range);
    for (const day of days) {
      dayMap.set(day.date, day);
    }
  }
  return dayMap;
}

async function syncCalendarPrices(accessToken, units, daysToSync) {
  const today = new Date();
  const startDateStr = formatIsoDateUtc(dateToUtc(today));
  const endDate = addDaysUtc(dateToUtc(today), daysToSync);
  const endDateStr = formatIsoDateUtc(endDate);
  const rows = [];
  let processedUnits = 0;
  let errorsCount = 0;
  const startTime = Date.now();
  console.log(`[SYNC] start: ${units.length} units, range ${startDateStr} -> ${endDateStr}`);

  const BATCH_SIZE = 4;
  const BATCH_DELAY_MS = 200;

  for (let i = 0; i < units.length; i += BATCH_SIZE) {
    const batch = units.slice(i, i + BATCH_SIZE);
    const batchResults = await Promise.all(batch.map(async (unit) => {
      try {
        const result = await getCalendarForRoom(
          accessToken,
          unit.beds24_room_id,
          startDateStr,
          endDateStr
        );
        const availabilityResult = await getAvailabilityForRoom(
          accessToken,
          unit.beds24_room_id,
          startDateStr,
          endDateStr
        );

        if (result.error) {
          console.warn(`[UNIT-ERR] unit=${unit.id} room=${unit.beds24_room_id} err=${result.error}`);
          errorsCount += 1;
          return [];
        }
        if (availabilityResult.error) {
          console.warn(`[UNIT-AVAIL-ERR] unit=${unit.id} room=${unit.beds24_room_id} err=${availabilityResult.error}`);
          errorsCount += 1;
          return [];
        }

        const dayMap = expandAllRangesToDays(result.ranges);
        const unitRows = [];
        const sourceUpdatedAt = new Date().toISOString();

        for (let d = dateToUtc(today); d < endDate; d = addDaysUtc(d, 1)) {
          const dateStr = formatIsoDateUtc(d);
          const dayData = dayMap.get(dateStr);
          const available = availabilityResult.availability.has(dateStr)
            ? availabilityResult.availability.get(dateStr)
            : null;
          unitRows.push({
            apartment_unit_id: unit.id,
            date: dateStr,
            price: dayData?.price ?? null,
            min_stay: dayData?.minStay ?? null,
            available,
            closed: dayData?.closed === true || available === false,
            source_updated_at: sourceUpdatedAt,
            updated_at: sourceUpdatedAt,
            raw_payload: dayData?.raw_range ? { range: dayData.raw_range } : null,
          });
        }

        console.log(`[UNIT-OK] unit=${unit.id} room=${unit.beds24_room_id} ranges=${result.ranges.length} days=${unitRows.length} priced=${unitRows.filter((r) => r.price != null).length}`);
        return unitRows;
      } catch (error) {
        console.error(`[UNIT-EXC] unit=${unit.id}`, error.message);
        errorsCount += 1;
        return [];
      }
    }));

    batchResults.forEach((unitRows) => {
      rows.push(...unitRows);
      if (unitRows.length > 0) processedUnits += 1;
    });

    if (i + BATCH_SIZE < units.length) {
      await delay(BATCH_DELAY_MS);
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`[SYNC] complete: ${processedUnits}/${units.length} units in ${elapsed}s, ${rows.length} rows, ${errorsCount} errors`);
  return { rows, processedUnits, errorsCount };
}

async function upsertCalendarDays(env, rows) {
  const batches = chunk(rows, 500);
  let upsertedCount = 0;

  for (let i = 0; i < batches.length; i += 1) {
    const batch = batches[i];
    console.log(`[UPSERT] batch ${i}: ${batch.length} rows, first row:`, JSON.stringify(batch[0]));
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

    upsertedCount += batch.length;
  }

  return upsertedCount;
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

function dateToUtc(date) {
  return new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
}

function chunk(items, size) {
  const result = [];
  for (let index = 0; index < items.length; index += size) {
    result.push(items.slice(index, index + size));
  }
  return result;
}

function normalizeCalendarClosed(entry) {
  if (!entry || typeof entry !== 'object') return undefined;
  if (entry.closed !== undefined && entry.closed !== null) return Boolean(entry.closed);
  if (entry.bookable !== undefined && entry.bookable !== null) return !Boolean(entry.bookable);

  const inventoryValue = normalizeCalendarNumber(
    firstDefined(entry.numAvail, entry.num_available, entry.inventory, entry.quantity)
  );
  if (inventoryValue !== null) {
    return inventoryValue <= 0;
  }

  return undefined;
}

function normalizeCalendarAvailable(entry) {
  const closed = normalizeCalendarClosed(entry);
  if (closed !== undefined) return !closed;
  return undefined;
}

function normalizeCalendarNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function firstDefined(...values) {
  return values.find((value) => value !== undefined && value !== null);
}

async function delay(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
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
