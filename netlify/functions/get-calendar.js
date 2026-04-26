const BEDS24_URL = 'https://api.beds24.com/v2';
const CACHE = new Map();
const CACHE_TTL = {
  apartments: 10 * 60 * 1000,
  properties: 6 * 60 * 60 * 1000,
  bookings: 10 * 60 * 1000,
  inventory: 15 * 60 * 1000,
  offers: 60 * 60 * 1000,
};
const CACHE_STALE_TTL = {
  apartments: 60 * 60 * 1000,
  properties: 24 * 60 * 60 * 1000,
  bookings: 60 * 60 * 1000,
  inventory: 2 * 60 * 60 * 1000,
  offers: 6 * 60 * 60 * 1000,
};

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return respond(204, '');
  }

  if (event.httpMethod !== 'GET') {
    return respond(405, { error: 'Method not allowed' });
  }

  const query = event.queryStringParameters || {};
  const from = query.dateFrom || new Date().toISOString().slice(0, 10);
  const to = query.dateTo || new Date(Date.now() + 60 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  const roomId = String(query.roomId || '').trim();
  const diagnostics = createDiagnostics();

  const env = {
    SUPABASE_URL: process.env.SUPABASE_URL || 'https://tysxeikqbgebpfyblgeb.supabase.co',
    SUPABASE_SERVICE_KEY: process.env.SUPABASE_SERVICE_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY,
    BEDS24_API_KEY: process.env.BEDS24_API_KEY,
  };

  if (!env.BEDS24_API_KEY) return respond(500, { error: 'BEDS24_API_KEY mancante' });
  if (!env.SUPABASE_SERVICE_KEY) return respond(500, { error: 'SUPABASE_SERVICE_KEY mancante' });

  try {
    const warnings = [];
    const allApartments = await loadApartments(env, diagnostics);
    const apartments = roomId
      ? allApartments.filter((apartment) => String(apartment.beds24_room_id) === roomId)
      : allApartments;
    let bookings = [];
    try {
      bookings = await loadBookings(env, from, to, roomId, diagnostics);
    } catch (error) {
      console.error('[get-calendar] bookings fatal', {
        dateFrom: from,
        dateTo: to,
        roomId,
        message: error.message,
      });
      warnings.push(`Beds24 prenotazioni non disponibili: ${error.message}`);
    }
    const roomMap = {};
    apartments.forEach((apartment) => {
      roomMap[String(apartment.beds24_room_id)] = apartment.nome_appartamento;
    });

    const normalizedBookings = bookings.map((booking) => {
      const propertyKey = String(booking.propertyId || booking.roomId || '');
      const guestName = [booking.firstName, booking.lastName].filter(Boolean).join(' ')
        || [booking.guestFirstName, booking.guestName].filter(Boolean).join(' ')
        || booking.firstName
        || booking.guestFirstName
        || booking.lastName
        || booking.guestName
        || 'Ospite';

      return {
        id: String(booking.bookId || booking.id || ''),
        roomId: propertyKey,
        propertyId: propertyKey,
        unitId: String(booking.roomId || ''),
        roomName: roomMap[propertyKey] || `Property ${propertyKey || booking.roomId || 'N/D'}`,
        guestName,
        checkIn: booking.arrival || booking.checkIn || '',
        checkOut: booking.departure || booking.checkOut || '',
        nights: booking.numNights || booking.nights || null,
        channel: booking.referer || booking.channel || '',
        status: booking.status || '',
        price: booking.price || booking.totalPrice || null,
      };
    });

    const inventory = await loadInventoryDays(env, apartments, normalizedBookings, from, to, roomId, diagnostics);

    return respond(200, {
      bookings: normalizedBookings,
      apartments: apartments.map((apartment) => ({
        id: apartment.id,
        name: apartment.nome_appartamento,
        beds24_room_id: apartment.beds24_room_id,
      })),
      inventoryDays: inventory.inventoryDays,
      warnings: [...warnings, ...inventory.warnings],
      diagnostics,
    });
  } catch (error) {
    return respond(500, { error: error.message });
  }
};

async function loadApartments(env, diagnostics) {
  return withCache({
    key: `apartments:${env.SUPABASE_URL}`,
    ttlMs: CACHE_TTL.apartments,
    staleTtlMs: CACHE_STALE_TTL.apartments,
    diagnostics,
    loader: async () => {
      const res = await fetch(
        `${env.SUPABASE_URL}/rest/v1/apartments?select=id,nome_appartamento,beds24_room_id&beds24_room_id=not.is.null&order=nome_appartamento`,
        {
          headers: {
            apikey: env.SUPABASE_SERVICE_KEY,
            Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
          },
        }
      );

      if (!res.ok) throw new Error(`Errore lettura apartments: ${res.status}`);
      return await res.json();
    },
  });
}

async function loadBookings(env, from, to, roomId, diagnostics) {
  const params = new URLSearchParams({
    arrivalFrom: from,
    arrivalTo: to,
    includeInvoice: 'false',
  });
  if (roomId) params.append('propertyId', roomId);
  const path = `/bookings?${params.toString()}`;

  return withCache({
    key: `bookings:${from}:${to}:${roomId || 'all'}`,
    ttlMs: CACHE_TTL.bookings,
    staleTtlMs: CACHE_STALE_TTL.bookings,
    diagnostics,
    loader: async () => {
      const res = await beds24Fetch(path, env, {}, diagnostics);
      if (!res.ok) {
        const text = await res.text();
        throw createBeds24Error(res.status, `Beds24 error ${res.status}: ${text}`);
      }
      const data = await res.json();
      return data?.data || data || [];
    },
  });
}

async function loadInventoryDays(env, apartments, bookings, from, to, selectedRoomId, diagnostics) {
  const warnings = [];
  if (!apartments.length) {
    return { inventoryDays: [], warnings };
  }

  const propertyIds = [...new Set(apartments.map((apartment) => String(apartment.beds24_room_id)).filter(Boolean))];
  let propertyRooms = [];
  try {
    propertyRooms = await loadPropertyRooms(env, propertyIds, diagnostics);
  } catch (error) {
    console.error('[get-calendar] property rooms fatal', {
      propertyIds,
      dateFrom: from,
      dateTo: to,
      message: error.message,
    });
    warnings.push(`Beds24 property rooms non disponibili: ${error.message}`);
    return { inventoryDays: [], warnings };
  }
  const roomIds = propertyRooms.map((item) => item.roomId);
  console.info('[get-calendar] inventory setup', {
    propertyIds,
    roomIds,
    dateFrom: from,
    dateTo: to,
  });

  if (!roomIds.length) {
    warnings.push('Inventory Beds24 non disponibile: room type non risolte per le property richieste.');
    return { inventoryDays: [], warnings };
  }

  propertyRooms
    .filter((item) => item.roomCount > 1)
    .forEach((item) => warnings.push(`Property ${item.propertyId}: presenti ${item.roomCount} room type. Uso la prima room type disponibile per la vista calendario.`));

  const calendarMap = new Map();
  const availabilityMap = new Map();
  const offerPriceMap = new Map();
  const roomByProperty = new Map(propertyRooms.map((item) => [item.propertyId, item]));
  await loadCalendarOverrides(env, roomIds, from, to, diagnostics, warnings, calendarMap, propertyIds);
  await loadAvailability(env, roomIds, from, to, diagnostics, warnings, availabilityMap, propertyIds);

  if (selectedRoomId) {
    try {
      const selectedRoomInfo = roomByProperty.get(String(selectedRoomId));
      const offerRoomIds = selectedRoomInfo?.roomId ? [selectedRoomInfo.roomId] : [];
      const offerPrices = await loadOfferPrices(env, offerRoomIds, from, to, diagnostics);
      offerPrices.forEach((value, key) => offerPriceMap.set(key, value));
      if (offerPriceMap.size) {
        warnings.push('Prezzi calcolati da Beds24 offers per 2 adulti');
      }
    } catch (error) {
      console.error('[get-calendar] inventory offers fatal', {
        roomId: selectedRoomId,
        dateFrom: from,
        dateTo: to,
        message: error.message,
      });
      warnings.push(`Beds24 offers fallita: ${error.message}`);
    }
  }

  const bookingsByPropertyAndDate = new Map();
  bookings.forEach((booking) => {
    if (!booking.checkIn || !booking.checkOut || !booking.propertyId) return;
    const start = new Date(`${booking.checkIn}T00:00:00`);
    const end = new Date(`${booking.checkOut}T00:00:00`);
    for (let date = new Date(start); date < end; date.setDate(date.getDate() + 1)) {
      const dateStr = date.toISOString().slice(0, 10);
      const key = `${booking.propertyId}:${dateStr}`;
      if (!bookingsByPropertyAndDate.has(key)) bookingsByPropertyAndDate.set(key, booking);
    }
  });

  const inventoryDays = [];
  apartments.forEach((apartment) => {
    const propertyId = String(apartment.beds24_room_id);
    const roomInfo = roomByProperty.get(propertyId);
      const roomKey = roomInfo?.roomId || null;
      for (const date of eachDate(from, to)) {
        const calendarEntry = roomKey ? calendarMap.get(`${roomKey}:${date}`) : null;
        const availability = roomKey ? availabilityMap.get(`${roomKey}:${date}`) : null;
        const offerEntry = roomKey ? offerPriceMap.get(`${roomKey}:${date}`) : null;
        const booking = bookingsByPropertyAndDate.get(`${propertyId}:${date}`) || null;
        const derivedClosed = calendarEntry?.closed != null
          ? Boolean(calendarEntry.closed)
          : (availability == null ? null : !availability);

        inventoryDays.push({
          date,
          propertyId,
          apartmentId: apartment.id,
          price: normalizeNumber(offerEntry?.price ?? calendarEntry?.price),
          priceSource: offerEntry?.price != null ? 'offers' : (calendarEntry?.price != null ? 'calendar' : null),
          minStay: normalizeInteger(calendarEntry?.minStay),
          closed: derivedClosed === null ? false : derivedClosed,
          available: availability == null ? null : Boolean(availability),
          hasBooking: Boolean(booking),
          booking: booking ? {
            id: booking.id,
          guestName: booking.guestName,
          checkIn: booking.checkIn,
          checkOut: booking.checkOut,
        } : null,
      });
    }
  });

  return { inventoryDays, warnings };
}

async function loadOfferPrices(env, roomIds, from, to, diagnostics) {
  if (!roomIds.length) return new Map();
  const map = new Map();
  const today = new Date().toISOString().slice(0, 10);
  const eligibleDates = eachDate(from, to).filter((date) => date >= today);

  for (const date of eligibleDates) {
    const cachedRows = await withCache({
      key: `offers:${roomIds.slice().sort().join(',')}:${date}:2`,
      ttlMs: CACHE_TTL.offers,
      staleTtlMs: CACHE_STALE_TTL.offers,
      diagnostics,
      loader: async () => {
        const departure = addDaysUtc(parseIsoDateUtc(date), 1);
        const departureStr = formatIsoDateUtc(departure);
        const params = new URLSearchParams();
        roomIds.forEach((id) => params.append('roomId', id));
        params.set('arrival', date);
        params.set('departure', departureStr);
        params.set('numAdults', '2');

        const res = await beds24Fetch(`/inventory/offers?${params.toString()}`, env, {}, diagnostics);
        if (!res.ok) {
          const detail = await safeReadErrorBody(res);
          throw createBeds24Error(res.status, `${res.status} ${detail}`.trim());
        }

        const payload = await res.json();
        return payload?.data || payload || [];
      },
    });

    cachedRows.forEach((row) => {
      const roomKey = String(row.roomId || '');
      const price = normalizeNumber(firstDefined(row.price, row.totalPrice, row.roomPrice, row.amount));
      if (roomKey && price !== null) {
        map.set(`${roomKey}:${date}`, {
          price,
          unitsAvailable: normalizeInteger(row.unitsAvailable),
        });
      }
    });
  }

  console.info('[get-calendar] inventory offers response', {
    roomCount: roomIds.length,
    dateFrom: from,
    dateTo: to,
    queriedDays: eligibleDates.length,
    pricedDays: map.size,
  });

  return map;
}

async function loadPropertyRooms(env, propertyIds, diagnostics) {
  if (!propertyIds.length) return [];
  return withCache({
    key: `properties:${propertyIds.slice().sort().join(',')}`,
    ttlMs: CACHE_TTL.properties,
    staleTtlMs: CACHE_STALE_TTL.properties,
    diagnostics,
    loader: async () => {
      const params = new URLSearchParams();
      propertyIds.forEach((id) => params.append('id', id));
      params.set('includeAllRooms', 'true');

      const res = await beds24Fetch(`/properties?${params.toString()}`, env, {}, diagnostics);
      if (!res.ok) {
        const text = await res.text();
        throw createBeds24Error(res.status, `Errore lettura property Beds24: ${res.status} ${text}`);
      }

      const payload = await res.json();
      const properties = payload?.data || payload || [];
      return properties
        .map((property) => {
          const rooms = Array.isArray(property.roomTypes) ? property.roomTypes : [];
          if (!rooms.length) return null;
          return {
            propertyId: String(property.id),
            roomId: String(rooms[0].id),
            roomCount: rooms.length,
          };
        })
        .filter(Boolean);
    },
  });
}

async function safeReadErrorBody(response) {
  try {
    const text = await response.text();
    if (!text) return '';
    try {
      const parsed = JSON.parse(text);
      return parsed?.error || parsed?.message || text;
    } catch (_error) {
      return text;
    }
  } catch (_error) {
    return '';
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
      for (const date of eachDate(entry.from, entry.to)) {
        normalized.push({
          date,
          price: firstDefined(entry.price1, entry.price, entry.roomPrice),
          minStay: firstDefined(entry.minStay, entry.minimumStay),
          closed: firstDefined(entry.closed, entry.bookable === false ? true : undefined),
        });
      }
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

let beds24TokenCache = null;

async function beds24Fetch(path, env, options = {}, diagnostics) {
  const request = async (token) => fetch(`${BEDS24_URL}${path}`, {
    ...options,
    headers: {
      accept: 'application/json',
      ...(options.headers || {}),
      token,
    },
  });

  diagnostics.beds24Calls += 1;
  let response = await request(await getBeds24Token(env.BEDS24_API_KEY, false));
  if (response.status === 429) {
    diagnostics.rateLimited = true;
    await sleep(250);
    diagnostics.beds24Calls += 1;
    response = await request(await getBeds24Token(env.BEDS24_API_KEY, false));
  }
  if (response.status !== 401) return response;

  const refreshedToken = await getBeds24Token(env.BEDS24_API_KEY, true);
  if (refreshedToken && refreshedToken !== env.BEDS24_API_KEY) {
    diagnostics.beds24Calls += 1;
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
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
    },
    body: typeof payload === 'string' ? payload : JSON.stringify(payload),
  };
}

async function loadCalendarOverrides(env, roomIds, from, to, diagnostics, warnings, calendarMap, propertyIds) {
  const params = new URLSearchParams();
  roomIds.forEach((id) => params.append('roomId', id));
  params.set('from', from);
  params.set('to', to);
  const path = `/inventory/rooms/calendar?${params.toString()}`;

  try {
    const rows = await withCache({
      key: `calendar:${roomIds.slice().sort().join(',')}:${from}:${to}`,
      ttlMs: CACHE_TTL.inventory,
      staleTtlMs: CACHE_STALE_TTL.inventory,
      diagnostics,
      loader: async () => {
        const calendarRes = await beds24Fetch(path, env, {}, diagnostics);
        if (!calendarRes.ok) {
          const detail = await safeReadErrorBody(calendarRes);
          throw createBeds24Error(calendarRes.status, `Inventory Beds24 calendar non disponibile (${calendarRes.status}): ${detail}`);
        }
        const payload = await calendarRes.json();
        return payload?.data || payload || [];
      },
    });

    rows.forEach((row) => {
      const roomKey = String(row.roomId || '');
      normalizeCalendarEntries(row.calendar || []).forEach((entry) => {
        calendarMap.set(`${roomKey}:${entry.date}`, entry);
      });
    });
  } catch (error) {
    console.error('[get-calendar] inventory calendar fatal', {
      propertyIds,
      roomIds,
      dateFrom: from,
      dateTo: to,
      message: error.message,
    });
    warnings.push(error.status === 429
      ? 'Beds24 rate limit: mostrati dati cache/fallback'
      : `Inventory Beds24 calendar fallita: ${error.message}`);
  }
}

async function loadAvailability(env, roomIds, from, to, diagnostics, warnings, availabilityMap, propertyIds) {
  const params = new URLSearchParams();
  roomIds.forEach((id) => params.append('roomId', id));
  params.set('dateFrom', from);
  params.set('dateTo', to);
  const path = `/inventory/rooms/availability?${params.toString()}`;

  try {
    const rows = await withCache({
      key: `availability:${roomIds.slice().sort().join(',')}:${from}:${to}`,
      ttlMs: CACHE_TTL.inventory,
      staleTtlMs: CACHE_STALE_TTL.inventory,
      diagnostics,
      loader: async () => {
        const availabilityRes = await beds24Fetch(path, env, {}, diagnostics);
        if (!availabilityRes.ok) {
          const detail = await safeReadErrorBody(availabilityRes);
          throw createBeds24Error(availabilityRes.status, `Beds24 availability non disponibile (${availabilityRes.status}): ${detail}`);
        }
        const payload = await availabilityRes.json();
        return payload?.data || payload || [];
      },
    });

    rows.forEach((row) => {
      const roomKey = String(row.roomId || '');
      Object.entries(row.availability || {}).forEach(([date, available]) => {
        if (date >= from && date <= to) {
          availabilityMap.set(`${roomKey}:${date}`, Boolean(available));
        }
      });
    });
  } catch (error) {
    console.error('[get-calendar] inventory availability fatal', {
      propertyIds,
      roomIds,
      dateFrom: from,
      dateTo: to,
      message: error.message,
    });
    warnings.push(error.status === 429
      ? 'Beds24 rate limit: mostrati dati cache/fallback'
      : `Beds24 availability fallita: ${error.message}`);
  }
}

function withCache({ key, ttlMs, staleTtlMs, diagnostics, loader }) {
  const now = Date.now();
  const cached = CACHE.get(key);
  if (cached && cached.expiresAt > now) {
    diagnostics.cacheHits += 1;
    return Promise.resolve(cached.value);
  }

  diagnostics.cacheMisses += 1;
  return Promise.resolve()
    .then(loader)
    .then((value) => {
      CACHE.set(key, {
        value,
        expiresAt: now + ttlMs,
        staleUntil: now + staleTtlMs,
      });
      return value;
    })
    .catch((error) => {
      if (cached && cached.staleUntil > now && isRateLimitError(error)) {
        diagnostics.cacheHits += 1;
        diagnostics.usedStaleCache = true;
        return cached.value;
      }
      throw error;
    });
}

function createDiagnostics() {
  return {
    beds24Calls: 0,
    cacheHits: 0,
    cacheMisses: 0,
    rateLimited: false,
    usedStaleCache: false,
  };
}

function createBeds24Error(status, message) {
  const error = new Error(message);
  error.status = status;
  return error;
}

function isRateLimitError(error) {
  return Number(error?.status) === 429 || String(error?.message || '').includes('429');
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
