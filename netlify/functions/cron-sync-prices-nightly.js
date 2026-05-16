const { normalizeSupabaseUrl } = require('./_lib/shared-auth');

const BEDS24_URL = 'https://api.beds24.com/v2';
const DAYS_TO_SYNC = 90;
const CRON_EXPRESSION = '0 2 * * *';

let beds24TokenCache = null;

async function runNightlySync() {
  console.log('[CRON-SYNC] Started at', new Date().toISOString());

  const env = {
    SUPABASE_URL: normalizeSupabaseUrl(process.env.SUPABASE_RUNTIME_URL || process.env.SUPABASE_URL),
    SUPABASE_SERVICE_ROLE_KEY: process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY,
    BEDS24_REFRESH_TOKEN: String(process.env.BEDS24_REFRESH_TOKEN || process.env.BEDS24_API_KEY || '').trim(),
  };

  if (!env.SUPABASE_URL || !env.SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error('Configurazione Supabase mancante');
  }
  if (!env.BEDS24_REFRESH_TOKEN) {
    throw new Error('BEDS24_REFRESH_TOKEN mancante');
  }

  const startedAt = Date.now();
  const accessToken = await getBeds24AccessToken(env);
  const units = await getApartmentUnits(env);
  if (!units.length) {
    const result = {
      success: true,
      days_synced: DAYS_TO_SYNC,
      units_synced: 0,
      tasks_processed: 0,
      rows_upserted: 0,
      errors: 0,
      duration_ms: Date.now() - startedAt,
      cron_expression_utc: CRON_EXPRESSION,
    };
    console.log('[CRON-SYNC] No units to sync', JSON.stringify(result));
    return result;
  }

  const { rows, processedUnits, errorsCount } = await syncCalendarPrices(accessToken, units, DAYS_TO_SYNC);
  const upsertedCount = await upsertCalendarDays(env, rows);

  const result = {
    success: true,
    days_synced: DAYS_TO_SYNC,
    units_synced: units.length,
    tasks_processed: processedUnits,
    rows_upserted: upsertedCount,
    errors: errorsCount,
    duration_ms: Date.now() - startedAt,
    cron_expression_utc: CRON_EXPRESSION,
  };

  console.log('[CRON-SYNC] Completed successfully', JSON.stringify(result));
  return result;
}

async function nightlySyncHandler() {
  try {
    const payload = await runNightlySync();
    return {
      statusCode: 200,
      headers: { 'Content-Type': 'application/json; charset=utf-8' },
      body: JSON.stringify(payload),
    };
  } catch (error) {
    console.error('[CRON-SYNC] FAILED', error);
    return {
      statusCode: 500,
      headers: { 'Content-Type': 'application/json; charset=utf-8' },
      body: JSON.stringify({
        success: false,
        error: error.message || 'Nightly cron sync failed',
        cron_expression_utc: CRON_EXPRESSION,
      }),
    };
  }
}

exports.config = {
  schedule: CRON_EXPRESSION,
};

exports.handler = nightlySyncHandler;

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
  const url = `${BEDS24_URL}/inventory/rooms/calendar?roomId=${encodeURIComponent(roomId)}&startDate=${encodeURIComponent(startDate)}&endDate=${encodeURIComponent(endDate)}&includePrices=true&includeMinStay=true`;

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
    console.warn(`[CRON-SYNC][CALENDAR-FAIL] room=${roomId} status=${response.status}`);
    return { ranges: [], error: String(response.status) };
  }

  const data = await response.json();
  return parseCalendarResponse(data);
}

function parseCalendarResponse(parsed) {
  if (!parsed?.success || !Array.isArray(parsed.data)) {
    return { ranges: [], error: 'no-data' };
  }

  const roomData = parsed.data[0];
  if (!roomData?.calendar || !Array.isArray(roomData.calendar)) {
    return { ranges: [] };
  }

  const ranges = roomData.calendar.map((range) => ({
    from: range.from,
    to: range.to,
    price1: typeof range.price1 === 'number' ? range.price1 : null,
    minStay: typeof range.minStay === 'number' ? range.minStay : null,
    raw: range,
  }));

  return { ranges };
}

function expandRangeToDays(range) {
  const days = [];
  const start = parseIsoDateUtc(range.from);
  const end = parseIsoDateUtc(range.to);

  for (let date = new Date(start); date <= end; date = addDaysUtc(date, 1)) {
    days.push({
      date: formatIsoDateUtc(date),
      price: range.price1,
      minStay: range.minStay,
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
  const startDate = dateToUtc(today);
  const startDateStr = formatIsoDateUtc(startDate);
  const endDate = addDaysUtc(startDate, daysToSync);
  const endDateStr = formatIsoDateUtc(endDate);
  const rows = [];
  let processedUnits = 0;
  let errorsCount = 0;
  const startedAt = Date.now();

  console.log(`[CRON-SYNC] start: ${units.length} units, range ${startDateStr} -> ${endDateStr}`);

  const BATCH_SIZE = 4;
  const BATCH_DELAY_MS = 200;

  for (let index = 0; index < units.length; index += BATCH_SIZE) {
    const batch = units.slice(index, index + BATCH_SIZE);
    const batchResults = await Promise.all(batch.map(async (unit) => {
      try {
        const result = await getCalendarForRoom(
          accessToken,
          unit.beds24_room_id,
          startDateStr,
          endDateStr
        );

        if (result.error) {
          console.warn(`[CRON-SYNC][UNIT-ERR] unit=${unit.id} room=${unit.beds24_room_id} err=${result.error}`);
          errorsCount += 1;
          return [];
        }

        const dayMap = expandAllRangesToDays(result.ranges);
        const unitRows = [];
        const sourceUpdatedAt = new Date().toISOString();

        for (let date = new Date(startDate); date < endDate; date = addDaysUtc(date, 1)) {
          const dateStr = formatIsoDateUtc(date);
          const dayData = dayMap.get(dateStr);
          unitRows.push({
            apartment_unit_id: unit.id,
            date: dateStr,
            price: dayData?.price ?? null,
            min_stay: dayData?.minStay ?? null,
            available: dayData ? true : null,
            closed: false,
            source_updated_at: sourceUpdatedAt,
            updated_at: sourceUpdatedAt,
            raw_payload: dayData?.raw_range ? { range: dayData.raw_range } : null,
          });
        }

        console.log(`[CRON-SYNC][UNIT-OK] unit=${unit.id} room=${unit.beds24_room_id} ranges=${result.ranges.length} days=${unitRows.length} priced=${unitRows.filter((row) => row.price != null).length}`);
        return unitRows;
      } catch (error) {
        console.error(`[CRON-SYNC][UNIT-EXC] unit=${unit.id}`, error.message);
        errorsCount += 1;
        return [];
      }
    }));

    batchResults.forEach((unitRows) => {
      rows.push(...unitRows);
      if (unitRows.length > 0) processedUnits += 1;
    });

    if (index + BATCH_SIZE < units.length) {
      await delay(BATCH_DELAY_MS);
    }
  }

  const elapsed = ((Date.now() - startedAt) / 1000).toFixed(1);
  console.log(`[CRON-SYNC] complete: ${processedUnits}/${units.length} units in ${elapsed}s, ${rows.length} rows, ${errorsCount} errors`);
  return { rows, processedUnits, errorsCount };
}

async function upsertCalendarDays(env, rows) {
  const batches = chunk(rows, 500);
  let upsertedCount = 0;

  for (let index = 0; index < batches.length; index += 1) {
    const batch = batches[index];
    console.log(`[CRON-SYNC][UPSERT] batch ${index}: ${batch.length} rows`);
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

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function safeReadText(response) {
  try {
    return await response.text();
  } catch (_error) {
    return '';
  }
}
