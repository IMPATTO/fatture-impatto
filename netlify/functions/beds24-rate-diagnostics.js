const { createClient } = require('@supabase/supabase-js');

const BEDS24_URL = 'https://api.beds24.com/v2';

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

let beds24TokenCache = null;

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers: CORS, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return respond(405, { error: 'Method not allowed' });
  }

  const env = {
    SUPABASE_URL: process.env.SUPABASE_URL || 'https://tysxeikqbgebpfyblgeb.supabase.co',
    SUPABASE_SERVICE_ROLE_KEY: process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY,
    BEDS24_API_KEY: process.env.BEDS24_API_KEY,
  };

  if (!env.SUPABASE_URL || !env.SUPABASE_SERVICE_ROLE_KEY) {
    return respond(500, { error: 'Configurazione Supabase mancante' });
  }
  if (!env.BEDS24_API_KEY) {
    return respond(500, { error: 'BEDS24_API_KEY mancante' });
  }

  const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);
  const authHeader = event.headers.authorization || event.headers.Authorization;
  if (!authHeader?.startsWith('Bearer ')) {
    return respond(401, { error: 'Unauthorized' });
  }

  const token = authHeader.replace('Bearer ', '').trim();
  const { data: authData, error: authError } = await supabase.auth.getUser(token);
  if (authError || !authData?.user) {
    return respond(401, { error: 'Unauthorized' });
  }

  let body;
  try {
    body = JSON.parse(event.body || '{}');
  } catch (_error) {
    return respond(400, { error: 'JSON non valido' });
  }

  const apartmentId = String(body.apartmentId || '').trim();
  const propertyId = String(body.propertyId || '').trim();
  const requestedRoomId = String(body.roomId || '').trim();
  const dateFrom = String(body.dateFrom || '').trim();
  const dateTo = String(body.dateTo || '').trim();
  const occupancy = Number.isFinite(Number(body.occupancy)) ? Number(body.occupancy) : 2;

  if (!propertyId || !dateFrom || !dateTo) {
    return respond(400, { error: 'propertyId, dateFrom e dateTo sono obbligatori' });
  }
  if (!isIsoDate(dateFrom) || !isIsoDate(dateTo) || dateFrom > dateTo) {
    return respond(400, { error: 'Intervallo date non valido' });
  }

  try {
    let apartment = null;
    if (apartmentId) {
      const { data: apartmentData, error: apartmentError } = await supabase
        .from('apartments')
        .select('id,nome_appartamento,beds24_room_id')
        .eq('id', apartmentId)
        .limit(1)
        .maybeSingle();

      if (apartmentError) {
        return respond(500, { error: `Errore lettura appartamento: ${apartmentError.message}` });
      }
      if (!apartmentData?.id) {
        return respond(404, { error: 'Appartamento non trovato' });
      }
      if (String(apartmentData.beds24_room_id || '') !== propertyId) {
        return respond(400, { error: 'propertyId non coerente con il mapping Beds24 dell’appartamento' });
      }
      apartment = apartmentData;
    }

    const propertyRes = await beds24Fetch(
      `/properties?id=${encodeURIComponent(propertyId)}&includeAllRooms=true&includePriceRules=true&includeOffers=true`,
      env
    );
    const propertyDiag = await probeResponse(
      'GET /properties?includeAllRooms=true&includePriceRules=true&includeOffers=true',
      propertyRes
    );

    if (!propertyRes.ok) {
      return respond(propertyRes.status, {
        error: 'Errore lettura property Beds24',
        propertyDiagnostics: propertyDiag,
      });
    }

    const propertyPayload = propertyDiag.raw;
    const property = (propertyPayload?.data || propertyPayload || [])[0];
    const selectedRoom = property?.roomTypes?.[0] || null;
    if (!selectedRoom?.id) {
      return respond(400, {
        error: 'Nessuna room type Beds24 disponibile per questa property',
        propertyDiagnostics: stripRaw(propertyDiag),
      });
    }

    const roomId = requestedRoomId || String(selectedRoom.id);
    const offerDeparture = addDaysIso(dateTo, 1);
    const probes = [];

    probes.push(await probeBeds24Endpoint(
      env,
      'GET /inventory/rooms/calendar',
      `/inventory/rooms/calendar?roomId=${encodeURIComponent(roomId)}&from=${encodeURIComponent(dateFrom)}&to=${encodeURIComponent(dateTo)}`
    ));
    probes.push(await probeBeds24Endpoint(
      env,
      'GET /inventory/calendar',
      `/inventory/calendar?roomId=${encodeURIComponent(roomId)}&from=${encodeURIComponent(dateFrom)}&to=${encodeURIComponent(dateTo)}`
    ));
    probes.push(await probeBeds24Endpoint(
      env,
      'GET /inventory/rooms/availability',
      `/inventory/rooms/availability?roomId=${encodeURIComponent(roomId)}&dateFrom=${encodeURIComponent(dateFrom)}&dateTo=${encodeURIComponent(dateTo)}`
    ));
    probes.push(await probeBeds24Endpoint(
      env,
      'GET /inventory/rooms/offers (numAdult/numChild)',
      `/inventory/rooms/offers?roomId=${encodeURIComponent(roomId)}&arrival=${encodeURIComponent(dateFrom)}&departure=${encodeURIComponent(offerDeparture)}&numAdult=${occupancy}&numChild=0`
    ));
    probes.push(await probeBeds24Endpoint(
      env,
      'GET /inventory/offers (numAdult/numChild)',
      `/inventory/offers?roomId=${encodeURIComponent(roomId)}&arrival=${encodeURIComponent(dateFrom)}&departure=${encodeURIComponent(offerDeparture)}&numAdult=${occupancy}&numChild=0`
    ));
    const offerProbeSpecs = buildOfferProbeSpecs({ roomId, propertyId, dateFrom, dateTo, offerDeparture });
    for (const spec of offerProbeSpecs) {
      probes.push(await probeBeds24Endpoint(env, spec.label, spec.path, spec.params));
    }

    const normalized = buildNormalizedCandidates(probes, roomId, dateFrom, dateTo);

    return respond(200, {
      success: true,
      apartmentId: apartment?.id || null,
      propertyId,
      roomId,
      dateFrom,
      dateTo,
      occupancy,
      findings: {
        propertyDefaults: {
          roomMinStay: normalizeInteger(selectedRoom.minStay),
          rackRate: normalizeNumber(selectedRoom.rackRate),
          priceRules: sanitizeDiagnosticValue(selectedRoom.priceRules || []),
          offers: sanitizeDiagnosticValue(selectedRoom.offers || []),
        },
        normalized,
      },
      diagnostics: {
        property: stripRaw(propertyDiag),
        probes: probes.map(stripRaw),
      },
    });
  } catch (error) {
    return respond(500, { error: error.message });
  }
};

async function probeBeds24Endpoint(env, label, path, params = null) {
  const res = await beds24Fetch(path, env);
  return await probeResponse(label, res, path, params);
}

async function probeResponse(label, response, path = null, params = null) {
  const text = await response.text();
  let raw = null;
  try {
    raw = text ? JSON.parse(text) : null;
  } catch (_error) {
    raw = text;
  }

  const diagnostics = {
    endpoint: label,
    path,
    params,
    status: response.status,
    ok: response.ok,
    topLevelKeys: raw && typeof raw === 'object' && !Array.isArray(raw) ? Object.keys(raw) : [],
    relevantArrays: findRelevantArrays(raw),
    sample: sanitizeDiagnosticValue(Array.isArray(raw?.data) ? raw.data[0] : (Array.isArray(raw) ? raw[0] : raw)),
    error: extractErrorMessage(raw),
    normalized: inferNormalizedFromProbe(raw),
    raw,
  };

  console.info('[beds24-rate-diagnostics] endpoint probe', {
    endpoint: label,
    path,
    params,
    status: response.status,
    topLevelKeys: diagnostics.topLevelKeys,
    relevantArrays: diagnostics.relevantArrays.map((item) => ({
      path: item.path,
      length: item.length,
    })),
  });

  return diagnostics;
}

function buildNormalizedCandidates(probes, roomId, dateFrom, dateTo) {
  const candidates = [];

  probes.forEach((probe) => {
    if (!probe.ok) return;
    const path = probe.endpoint.toLowerCase();
    if (path.includes('/calendar')) {
      extractCalendarCandidates(probe.raw, roomId, dateFrom, dateTo).forEach((item) => candidates.push({
        source: probe.endpoint,
        ...item,
      }));
    }
    if (path.includes('/offers')) {
      extractOfferCandidates(probe.raw).forEach((item) => candidates.push({
        source: probe.endpoint,
        ...item,
      }));
    }
  });

  return candidates;
}

function buildOfferProbeSpecs({ roomId, propertyId, dateFrom, dateTo, offerDeparture }) {
  const endpoints = [
    '/inventory/rooms/offers',
    '/inventory/offers',
  ];
  const variants = [
    { label: 'arrival/departure + adults=1', params: { roomId, arrival: dateFrom, departure: offerDeparture, adults: 1 } },
    { label: 'arrival/departure + adults=2', params: { roomId, arrival: dateFrom, departure: offerDeparture, adults: 2 } },
    { label: 'arrival/departure + numAdults=2', params: { roomId, arrival: dateFrom, departure: offerDeparture, numAdults: 2 } },
    { label: 'arrival/departure + guests=2', params: { roomId, arrival: dateFrom, departure: offerDeparture, guests: 2 } },
    { label: 'arrival/departure + occupancy=2', params: { roomId, arrival: dateFrom, departure: offerDeparture, occupancy: 2 } },
    { label: 'from/to + adults=2', params: { roomId, from: dateFrom, to: dateTo, adults: 2 } },
    { label: 'from/to + occupancy=2', params: { roomId, from: dateFrom, to: dateTo, occupancy: 2 } },
    { label: 'checkIn/checkOut + adults=2', params: { roomId, checkIn: dateFrom, checkOut: offerDeparture, adults: 2 } },
    { label: 'checkIn/checkOut + guests=2', params: { roomId, checkIn: dateFrom, checkOut: offerDeparture, guests: 2 } },
    { label: 'arrival/departure + propertyId + adults=2', params: { roomId, propertyId, arrival: dateFrom, departure: offerDeparture, adults: 2 } },
  ];

  const specs = [];
  endpoints.forEach((endpoint) => {
    variants.forEach((variant) => {
      const search = new URLSearchParams();
      Object.entries(variant.params).forEach(([key, value]) => {
        if (value !== undefined && value !== null && value !== '') search.append(key, String(value));
      });
      specs.push({
        label: `GET ${endpoint} (${variant.label})`,
        path: `${endpoint}?${search.toString()}`,
        params: variant.params,
      });
    });
  });
  return specs;
}

function extractCalendarCandidates(raw, roomId, dateFrom, dateTo) {
  const rows = Array.isArray(raw?.data) ? raw.data : (Array.isArray(raw) ? raw : []);
  const normalized = [];
  rows
    .filter((row) => !roomId || String(row?.roomId || '') === String(roomId))
    .forEach((row) => {
      (row.calendar || []).forEach((entry) => {
        if (entry?.date && entry.date >= dateFrom && entry.date <= dateTo) {
          normalized.push(normalizeDailyEntry(entry.date, entry));
          return;
        }
        if (entry?.from && entry?.to) {
          eachDate(entry.from, entry.to)
            .filter((date) => date >= dateFrom && date <= dateTo)
            .forEach((date) => normalized.push(normalizeDailyEntry(date, entry)));
        }
      });
    });
  return normalized;
}

function extractOfferCandidates(raw) {
  const rows = Array.isArray(raw?.data) ? raw.data : (Array.isArray(raw) ? raw : []);
  return rows.slice(0, 10).map((entry) => ({
    date: entry.arrival || entry.date || null,
    price: normalizeNumber(firstDefined(entry.price, entry.totalPrice, entry.roomPrice, entry.amount)),
    minStay: normalizeInteger(firstDefined(entry.minStay, entry.minimumStay)),
    closed: normalizeClosed(entry),
    offerId: entry.offerId || null,
  }));
}

function normalizeDailyEntry(date, entry) {
  return {
    date,
    price: normalizeNumber(firstDefined(entry.price1, entry.price, entry.roomPrice)),
    minStay: normalizeInteger(firstDefined(entry.minStay, entry.minimumStay)),
    closed: normalizeClosed(entry),
  };
}

function normalizeClosed(entry) {
  if (entry?.closed !== undefined && entry?.closed !== null) return Boolean(entry.closed);
  if (entry?.bookable !== undefined && entry?.bookable !== null) return !Boolean(entry.bookable);
  return null;
}

function inferNormalizedFromProbe(raw) {
  const rows = Array.isArray(raw?.data) ? raw.data : (Array.isArray(raw) ? raw : []);
  const first = rows[0] || raw;
  return {
    price: normalizeNumber(firstDefined(first?.price, first?.totalPrice, first?.roomPrice, first?.amount, first?.rackRate)),
    minStay: normalizeInteger(firstDefined(first?.minStay, first?.minimumStay, first?.minimumStayDays)),
    closed: normalizeClosed(first),
    available: normalizeAvailable(first),
  };
}

function normalizeAvailable(entry) {
  if (entry?.available !== undefined && entry?.available !== null) return Boolean(entry.available);
  if (entry?.bookable !== undefined && entry?.bookable !== null) return Boolean(entry.bookable);
  return null;
}

function extractErrorMessage(raw) {
  if (!raw) return null;
  if (typeof raw === 'string') return raw;
  return raw.error || raw.message || null;
}

function findRelevantArrays(raw) {
  const found = [];
  walkArrays(raw, 'root', found, 0);
  return found.slice(0, 12).map((item) => ({
    path: item.path,
    length: item.length,
    sample: item.sample,
  }));
}

function walkArrays(value, path, found, depth) {
  if (depth > 3 || value == null) return;
  if (Array.isArray(value)) {
    found.push({
      path,
      length: value.length,
      sample: value.slice(0, 2).map((item) => sanitizeDiagnosticValue(item)),
    });
    value.slice(0, 2).forEach((item, index) => walkArrays(item, `${path}[${index}]`, found, depth + 1));
    return;
  }
  if (typeof value !== 'object') return;
  Object.entries(value).forEach(([key, child]) => {
    walkArrays(child, path === 'root' ? key : `${path}.${key}`, found, depth + 1);
  });
}

function sanitizeDiagnosticValue(value, depth = 0) {
  if (value == null || depth > 2) return value;
  if (typeof value === 'string') {
    return value.length > 140 ? `${value.slice(0, 140)}...` : value;
  }
  if (typeof value === 'number' || typeof value === 'boolean') return value;
  if (Array.isArray(value)) {
    return value.slice(0, 2).map((item) => sanitizeDiagnosticValue(item, depth + 1));
  }
  if (typeof value === 'object') {
    const out = {};
    Object.entries(value).slice(0, 16).forEach(([key, child]) => {
      if (['message', 'comments', 'notes', 'apiMessage', 'invoiceItems', 'description'].includes(key)) return;
      out[key] = sanitizeDiagnosticValue(child, depth + 1);
    });
    return out;
  }
  return String(value);
}

function stripRaw(item) {
  const { raw, ...rest } = item;
  return rest;
}

function isIsoDate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(value);
}

function addDaysIso(date, days) {
  const copy = parseIsoDateUtc(date);
  copy.setUTCDate(copy.getUTCDate() + days);
  return formatIsoDateUtc(copy);
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
  const [year, month, day] = value.split('-').map((part) => Number(part));
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

async function beds24Fetch(path, env, options = {}) {
  const request = async (token) => fetch(`${BEDS24_URL}${path}`, {
    ...options,
    headers: {
      accept: 'application/json',
      ...(options.headers || {}),
      token,
    },
  });

  let response = await request(await getBeds24Token(env.BEDS24_API_KEY, false));
  if (response.status !== 401) return response;

  const refreshedToken = await getBeds24Token(env.BEDS24_API_KEY, true);
  if (refreshedToken && refreshedToken !== env.BEDS24_API_KEY) {
    response = await request(refreshedToken);
  }
  return response;
}

async function getBeds24Token(rawKey, forceRefresh) {
  if (!rawKey) return null;
  if (!forceRefresh) {
    if (beds24TokenCache && beds24TokenCache.sourceKey === rawKey && beds24TokenCache.expiresAt > Date.now()) {
      return beds24TokenCache.token;
    }
    return rawKey;
  }

  const res = await fetch(`${BEDS24_URL}/authentication/token`, {
    method: 'GET',
    headers: {
      accept: 'application/json',
      refreshToken: rawKey,
    },
  });

  if (!res.ok) return rawKey;
  const data = await res.json();
  if (!data?.token) return rawKey;

  beds24TokenCache = {
    sourceKey: rawKey,
    token: data.token,
    expiresAt: Date.now() + Math.max(Number(data.expiresIn || 3600) - 60, 60) * 1000,
  };
  return data.token;
}

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}
