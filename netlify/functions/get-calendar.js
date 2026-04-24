const BEDS24_URL = 'https://api.beds24.com/v2';

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

  const env = {
    SUPABASE_URL: process.env.SUPABASE_URL || 'https://tysxeikqbgebpfyblgeb.supabase.co',
    SUPABASE_SERVICE_KEY: process.env.SUPABASE_SERVICE_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY,
    BEDS24_API_KEY: process.env.BEDS24_API_KEY,
  };

  if (!env.BEDS24_API_KEY) return respond(500, { error: 'BEDS24_API_KEY mancante' });
  if (!env.SUPABASE_SERVICE_KEY) return respond(500, { error: 'SUPABASE_SERVICE_KEY mancante' });

  try {
    const apartments = await loadApartments(env);
    const bookings = await loadBookings(env, from, to, roomId);

    const roomMap = {};
    apartments.forEach((apartment) => {
      roomMap[String(apartment.beds24_room_id)] = apartment.nome_appartamento;
    });

    const normalized = bookings.map((booking) => {
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

    return respond(200, {
      bookings: normalized,
      apartments: apartments.map((apartment) => ({
        id: apartment.id,
        name: apartment.nome_appartamento,
        beds24_room_id: apartment.beds24_room_id,
      })),
    });
  } catch (error) {
    return respond(500, { error: error.message });
  }
};

async function loadApartments(env) {
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
}

async function loadBookings(env, from, to, roomId) {
  const params = new URLSearchParams({
    arrivalFrom: from,
    arrivalTo: to,
    includeInvoice: 'false',
  });
  if (roomId) params.append('propertyId', roomId);

  const res = await beds24Fetch(`/bookings?${params.toString()}`, env);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Beds24 error ${res.status}: ${text}`);
  }
  const data = await res.json();
  return data?.data || data || [];
}

let beds24TokenCache = null;

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
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
    },
    body: typeof payload === 'string' ? payload : JSON.stringify(payload),
  };
}
