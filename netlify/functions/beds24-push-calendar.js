const { createClient } = require('@supabase/supabase-js');
const {
  getBearerToken,
  getSupabaseUserFromToken,
  normalizeSupabaseUrl,
} = require('./_lib/shared-auth');
const { resolvePmsCalendarScope, canEditApartment } = require('./_lib/pms-calendar-access');

const BEDS24_URL = 'https://api.beds24.com/v2';

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json; charset=utf-8',
};

let beds24TokenCache = null;

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: CORS, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return respond(405, { error: 'Method not allowed' });
  }

  const token = getBearerToken(event.headers || {});
  if (!token) {
    return respond(401, { error: 'Unauthorized' });
  }

  const user = await getSupabaseUserFromToken(token);
  if (!user?.id) {
    return respond(401, { error: 'Unauthorized' });
  }
  const userEmail = String(user.email || '').trim().toLowerCase();

  let body;
  try {
    body = JSON.parse(event.body || '{}');
  } catch (_error) {
    return respond(400, { error: 'Invalid JSON' });
  }

  if (!Array.isArray(body.changes) || body.changes.length === 0) {
    return respond(400, { error: 'changes array required' });
  }

  const validationErrors = [];
  for (const [index, change] of body.changes.entries()) {
    if (!change || typeof change !== 'object') {
      validationErrors.push(`change[${index}]: invalid change object`);
      continue;
    }
    if (!change.apartment_unit_id) validationErrors.push(`change[${index}]: missing apartment_unit_id`);
    if (!change.date_from) validationErrors.push(`change[${index}]: missing date_from`);
    if (!change.date_to) validationErrors.push(`change[${index}]: missing date_to`);
    if (change.price != null && (!Number.isFinite(change.price) || change.price < 0)) {
      validationErrors.push(`change[${index}]: invalid price (must be >= 0)`);
    }
    if (change.min_stay != null && (!Number.isFinite(change.min_stay) || change.min_stay < 1)) {
      validationErrors.push(`change[${index}]: invalid min_stay (must be >= 1)`);
    }
    if (change.closed != null && typeof change.closed !== 'boolean') {
      validationErrors.push(`change[${index}]: invalid closed flag`);
    }
    if (!isIsoDate(change.date_from)) validationErrors.push(`change[${index}]: invalid date_from`);
    if (!isIsoDate(change.date_to)) validationErrors.push(`change[${index}]: invalid date_to`);
    if (change.date_from && change.date_to && change.date_from > change.date_to) {
      validationErrors.push(`change[${index}]: date_from > date_to`);
    }
  }

  if (validationErrors.length) {
    return respond(400, { error: 'Validation failed', details: validationErrors });
  }

  const env = {
    SUPABASE_URL: normalizeSupabaseUrl(process.env.SUPABASE_RUNTIME_URL || process.env.SUPABASE_URL),
    SUPABASE_SERVICE_ROLE_KEY: process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY,
    BEDS24_REFRESH_TOKEN: String(process.env.BEDS24_REFRESH_TOKEN || process.env.BEDS24_API_KEY || '').trim(),
  };

  if (!env.SUPABASE_URL || !env.SUPABASE_SERVICE_ROLE_KEY || !env.BEDS24_REFRESH_TOKEN) {
    return respond(500, { error: 'Missing env vars' });
  }

  const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);
  const scope = await resolvePmsCalendarScope(supabase, userEmail);
  if (!scope.canEditAny) {
    return respond(403, { error: 'Forbidden: no editable calendar access configured' });
  }

  let beds24Token;
  try {
    beds24Token = await getBeds24AccessToken(env.BEDS24_REFRESH_TOKEN);
  } catch (error) {
    console.error('[BEDS24-AUTH-FAIL]', error.message);
    return respond(502, { error: 'Beds24 auth failed', detail: error.message });
  }

  const unitIds = [...new Set(body.changes.map((change) => change.apartment_unit_id))];
  const { data: units, error: unitsError } = await supabase
    .from('apartment_units')
    .select('id, apartment_id, beds24_room_id')
    .in('id', unitIds);

  if (unitsError) {
    return respond(500, { error: 'Failed to fetch units', detail: unitsError.message });
  }

  const unitMap = new Map((units || []).map((unit) => [unit.id, unit]));
  const unauthorizedUnit = (units || []).find((unit) => !canEditApartment(scope, unit.apartment_id));
  if (unauthorizedUnit) {
    return respond(403, { error: 'Forbidden: one or more units are outside your editable scope' });
  }
  const beds24PayloadByRoom = new Map();

  for (const change of body.changes) {
    const unit = unitMap.get(change.apartment_unit_id);
    if (!unit?.beds24_room_id) {
      console.warn('[SKIP] unit without beds24_room_id', change.apartment_unit_id);
      continue;
    }

    const roomId = Number(unit.beds24_room_id);
    const calendarEntry = {
      from: change.date_from,
      to: change.date_to,
    };

    if (change.price !== undefined) calendarEntry.price1 = change.price;
    if (change.min_stay !== undefined) calendarEntry.minStay = change.min_stay;
    if (change.closed === true) calendarEntry.numAvail = 0;
    else if (change.closed === false) calendarEntry.numAvail = 1;

    if (!beds24PayloadByRoom.has(roomId)) {
      beds24PayloadByRoom.set(roomId, { roomId, calendar: [] });
    }
    beds24PayloadByRoom.get(roomId).calendar.push(calendarEntry);
  }

  const beds24Body = [...beds24PayloadByRoom.values()];
  if (beds24Body.length === 0) {
    return respond(400, { error: 'No valid changes after unit mapping' });
  }

  console.log('[BEDS24-PUSH] body', JSON.stringify(beds24Body).slice(0, 500));

  const beds24Response = await fetch(`${BEDS24_URL}/inventory/rooms/calendar`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      token: beds24Token,
      Accept: 'application/json',
    },
    body: JSON.stringify(beds24Body),
  });

  const beds24Result = await beds24Response.json().catch(() => null);
  if (!beds24Response.ok || beds24Response.status !== 201) {
    console.error('[BEDS24-FAIL]', beds24Response.status, beds24Result);
    return respond(502, {
      error: 'Beds24 push failed',
      status: beds24Response.status,
      detail: beds24Result,
    });
  }

  const allSucceeded = Array.isArray(beds24Result)
    ? beds24Result.every((row) => row && row.success === true)
    : Boolean(beds24Result?.success);

  if (!allSucceeded) {
    return respond(502, {
      error: 'Beds24 partial failure',
      detail: beds24Result,
    });
  }

  const localRows = [];
  for (const change of body.changes) {
    const unit = unitMap.get(change.apartment_unit_id);
    if (!unit) continue;

    const startDate = parseIsoDateUtc(change.date_from);
    const endDate = parseIsoDateUtc(change.date_to);
    for (let day = new Date(startDate); day <= endDate; day = addDaysUtc(day, 1)) {
      const row = {
        apartment_unit_id: change.apartment_unit_id,
        date: formatIsoDateUtc(day),
        source_updated_at: new Date().toISOString(),
      };
      if (change.price !== undefined) row.price = change.price;
      if (change.min_stay !== undefined) row.min_stay = change.min_stay;
      if (change.closed !== undefined) row.closed = change.closed === true;
      localRows.push(row);
    }
  }

  const { error: upsertError } = await supabase
    .from('calendar_days')
    .upsert(localRows, { onConflict: 'apartment_unit_id,date' });

  if (upsertError) {
    console.error('[LOCAL-UPSERT-FAIL]', upsertError);
    return respond(200, {
      success: true,
      beds24_updated: true,
      local_cache_updated: false,
      warning: 'Beds24 updated successfully but local cache update failed. Refresh page to fetch from Beds24.',
      changes_applied: body.changes.length,
    });
  }

  return respond(200, {
    success: true,
    beds24_updated: true,
    local_cache_updated: true,
    changes_applied: body.changes.length,
    rows_upserted: localRows.length,
  });
};

async function getBeds24AccessToken(refreshToken) {
  if (beds24TokenCache && beds24TokenCache.expiresAt > Date.now() + 15 * 1000) {
    return beds24TokenCache.token;
  }

  const response = await fetch(`${BEDS24_URL}/authentication/token`, {
    method: 'GET',
    headers: {
      refreshToken,
      Accept: 'application/json',
    },
  });

  if (!response.ok) {
    const detail = await safeReadText(response);
    throw new Error(`Autenticazione Beds24 fallita (${response.status}): ${detail}`);
  }

  const data = await response.json();
  const token = String(data?.token || data?.data?.token || '').trim();
  if (!token) {
    throw new Error('Token Beds24 non presente nella risposta di autenticazione');
  }

  const expiresInSeconds = Number(data?.expiresIn || data?.data?.expiresIn || 3600);
  beds24TokenCache = {
    token,
    expiresAt: Date.now() + Math.max(60, expiresInSeconds - 60) * 1000,
  };
  return beds24TokenCache.token;
}

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}

function isIsoDate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || ''));
}

function parseIsoDateUtc(value) {
  return new Date(`${value}T00:00:00.000Z`);
}

function addDaysUtc(date, days) {
  const copy = new Date(date);
  copy.setUTCDate(copy.getUTCDate() + days);
  return copy;
}

function formatIsoDateUtc(date) {
  return date.toISOString().slice(0, 10);
}

async function safeReadText(response) {
  try {
    return await response.text();
  } catch (_error) {
    return '';
  }
}
