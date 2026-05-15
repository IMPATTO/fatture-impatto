const {
  getAuthContextFromHeaders,
} = require('./_lib/shared-auth');

const BEDS24_URL = 'https://api.beds24.com/v2';
const PMS_EDITORS = new Set([
  'fatturazione@illupoaffitta.com',
  'contabilita@illupoaffitta.com',
  'info@marcovenzon.com',
  'veronica.dieta@gmail.com',
  'jessica.appartamenticaldari@gmail.com',
  'cerulliserena@gmail.com',
]);

const TEST_ROOM_ID = 674231;
const TEST_ARRIVAL = '2027-12-20';
const TEST_DEPARTURE = '2027-12-22';
const TEST_UPDATED_ARRIVAL = '2027-12-21';
const TEST_UPDATED_DEPARTURE = '2027-12-23';

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Content-Type': 'application/json; charset=utf-8',
};

let beds24TokenCache = null;

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: CORS, body: '' };
  }

  if (!['GET', 'POST'].includes(event.httpMethod)) {
    return respond(405, { error: 'Method not allowed' });
  }

  const auth = await getAuthContextFromHeaders(event.headers || {}, {
    allowInternalSupabase: true,
  });
  if (!auth) {
    return respond(401, { error: 'Unauthorized' });
  }

  const userEmail = String(
    auth.user?.email ||
    auth.email ||
    auth.actor ||
    '',
  ).trim().toLowerCase();
  if (!PMS_EDITORS.has(userEmail)) {
    return respond(403, { error: 'Forbidden: not a PMS editor' });
  }

  const refreshToken = String(process.env.BEDS24_REFRESH_TOKEN || process.env.BEDS24_API_KEY || '').trim();
  if (!refreshToken) {
    return respond(500, { error: 'BEDS24_REFRESH_TOKEN mancante' });
  }

  let token;
  try {
    token = await getBeds24AccessToken(refreshToken);
  } catch (error) {
    return respond(502, { error: 'Beds24 auth failed', detail: error.message });
  }

  const tests = [];
  let createdBookingId = null;

  const createBody = [
    {
      roomId: TEST_ROOM_ID,
      status: 'confirmed',
      arrival: TEST_ARRIVAL,
      departure: TEST_DEPARTURE,
      numAdult: 2,
      numChild: 0,
      firstName: 'Test',
      lastName: 'Marco',
      email: 'test@example.com',
      phone: '+39123456789',
      price: 200,
      notes: 'Booking di diagnostica - cancellare',
    },
  ];

  const createResult = await runTest({
    testNumber: 1,
    method: 'POST',
    path: '/bookings',
    token,
    requestBody: createBody,
  });
  createdBookingId = extractBookingId(createResult.responseBody);
  if (createdBookingId) {
    createResult.booking_id_created = createdBookingId;
  }
  tests.push(createResult);

  let updatePriceResult = null;
  if (createdBookingId) {
    updatePriceResult = await runTest({
      testNumber: 2,
      method: 'PATCH',
      path: '/bookings',
      token,
      requestBody: [
        {
          id: createdBookingId,
          price: 250,
          notes: 'Booking aggiornata',
        },
      ],
    });
  } else {
    updatePriceResult = skippedTest(2, 'PATCH', `${BEDS24_URL}/bookings`, 'bookingId non disponibile dal Test 1');
  }
  tests.push(updatePriceResult);

  let updateDatesResult = null;
  if (createdBookingId) {
    updateDatesResult = await runTest({
      testNumber: 3,
      method: 'PATCH',
      path: '/bookings',
      token,
      requestBody: [
        {
          id: createdBookingId,
          arrival: TEST_UPDATED_ARRIVAL,
          departure: TEST_UPDATED_DEPARTURE,
        },
      ],
    });
  } else {
    updateDatesResult = skippedTest(3, 'PATCH', `${BEDS24_URL}/bookings`, 'bookingId non disponibile dal Test 1');
  }
  tests.push(updateDatesResult);

  const conflictResult = await runTest({
    testNumber: 5,
    method: 'POST',
    path: '/bookings',
    token,
    requestBody: [
      {
        roomId: TEST_ROOM_ID,
        status: 'confirmed',
        arrival: TEST_UPDATED_ARRIVAL,
        departure: TEST_UPDATED_DEPARTURE,
        numAdult: 1,
        numChild: 0,
        firstName: 'Conflict',
        lastName: 'Marco',
        email: 'conflict@example.com',
        phone: '+39111111111',
        price: 180,
        notes: 'Conflict test diagnostico - cancellare',
      },
    ],
  });
  const conflictBookingId = extractBookingId(conflictResult.responseBody);
  if (conflictBookingId) {
    conflictResult.booking_id_created = conflictBookingId;
  }
  tests.push(conflictResult);

  let deleteResult = null;
  if (createdBookingId) {
    deleteResult = await runTest({
      testNumber: 4,
      method: 'DELETE',
      path: `/bookings/${encodeURIComponent(createdBookingId)}`,
      token,
    });
  } else {
    deleteResult = skippedTest(4, 'DELETE', `${BEDS24_URL}/bookings/<bookingId>`, 'bookingId non disponibile dal Test 1');
  }
  tests.push(deleteResult);

  if (conflictBookingId) {
    const cleanupResult = await runTest({
      testNumber: 6,
      method: 'DELETE',
      path: `/bookings/${encodeURIComponent(conflictBookingId)}`,
      token,
    });
    cleanupResult.note = 'Cleanup booking creato dal Test 5';
    tests.push(cleanupResult);
  }

  return respond(200, {
    tests,
  });
};

async function runTest({ testNumber, method, path, token, requestBody }) {
  const url = `${BEDS24_URL}${path}`;
  const headers = {
    Accept: 'application/json',
    token,
  };
  if (requestBody !== undefined) {
    headers['Content-Type'] = 'application/json';
  }

  let response;
  let responseBody;
  let responseText = '';
  try {
    response = await fetch(url, {
      method,
      headers,
      body: requestBody !== undefined ? JSON.stringify(requestBody) : undefined,
    });
    responseText = await response.text();
    responseBody = parseResponseBody(responseText);
  } catch (error) {
    responseBody = { error: error.message };
    response = { status: 0 };
  }

  console.log(`[BEDS24-BOOKING-DIAG][${testNumber}] ${method} ${url}`);
  if (requestBody !== undefined) {
    console.log(`[BEDS24-BOOKING-DIAG][${testNumber}] request`, JSON.stringify(requestBody));
  }
  console.log(`[BEDS24-BOOKING-DIAG][${testNumber}] status`, response.status);
  console.log(`[BEDS24-BOOKING-DIAG][${testNumber}] response`, truncateForLog(responseText || JSON.stringify(responseBody)));

  return {
    test_number: testNumber,
    method,
    url,
    request_body: requestBody === undefined ? null : requestBody,
    status: Number(response.status || 0),
    response_body: responseBody,
  };
}

function skippedTest(testNumber, method, url, reason) {
  return {
    test_number: testNumber,
    method,
    url,
    request_body: null,
    status: 0,
    response_body: { skipped: true, reason },
  };
}

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
  return token;
}

function extractBookingId(payload) {
  if (!payload) return null;
  const candidates = Array.isArray(payload) ? payload : [payload];
  for (const item of candidates) {
    const direct = item?.id ?? item?.bookingId ?? item?.booking_id;
    if (direct != null && String(direct).trim()) return String(direct).trim();
    const created = item?.new?.id ?? item?.new?.bookingId ?? item?.new?.booking_id;
    if (created != null && String(created).trim()) return String(created).trim();
    const nested = item?.data?.id ?? item?.data?.bookingId ?? item?.data?.booking_id;
    if (nested != null && String(nested).trim()) return String(nested).trim();
    if (Array.isArray(item?.info)) {
      for (const infoRow of item.info) {
        const infoId = infoRow?.id ?? infoRow?.bookingId ?? infoRow?.booking_id;
        if (infoId != null && String(infoId).trim()) return String(infoId).trim();
      }
    }
  }
  return null;
}

function parseResponseBody(text) {
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch (_error) {
    return { raw: text };
  }
}

async function safeReadText(response) {
  try {
    return await response.text();
  } catch (_error) {
    return '';
  }
}

function truncateForLog(value) {
  const text = String(value || '');
  return text.length > 2000 ? `${text.slice(0, 2000)}…` : text;
}

function respond(statusCode, body) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(body),
  };
}
