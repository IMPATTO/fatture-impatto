const { getAuthContextFromHeaders } = require('./_lib/shared-auth');

const BEDS24_URL = 'https://api.beds24.com/v2';
const ROOM_ID = 674231;
const DATE_FROM = '2026-05-15';
const DATE_TO = '2026-05-22';
const PMS_EDITORS = new Set([
  'fatturazione@illupoaffitta.com',
  'contabilita@illupoaffitta.com',
  'info@marcovenzon.com',
  'veronica.dieta@gmail.com',
  'jessica.appartamenticaldari@gmail.com',
  'cerulliserena@gmail.com',
]);

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Content-Type': 'application/json; charset=utf-8',
};

let beds24TokenCache = null;

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return respond(200, '');
  }

  if (event.httpMethod !== 'GET') {
    return respond(405, { error: 'Method not allowed' });
  }

  const auth = await getAuthContextFromHeaders(event.headers || {}, {
    allowInternalSupabase: true,
  });
  if (!auth) {
    return respond(401, { error: 'Unauthorized' });
  }

  const userEmail = String(auth.user?.email || auth.email || '').trim().toLowerCase();
  if (!PMS_EDITORS.has(userEmail)) {
    return respond(403, { error: 'Forbidden: not a PMS editor' });
  }

  const refreshToken = String(process.env.BEDS24_REFRESH_TOKEN || process.env.BEDS24_API_KEY || '').trim();
  if (!refreshToken) {
    return respond(500, { error: 'Missing BEDS24 refresh token' });
  }

  let accessToken;
  try {
    accessToken = await getBeds24AccessToken(refreshToken);
  } catch (error) {
    console.error('[BEDS24-AUTH-FAIL]', error.message);
    return respond(502, { error: 'Beds24 auth failed', detail: error.message });
  }

  const tests = [
    {
      test_number: 1,
      url: `${BEDS24_URL}/inventory/rooms/calendar?roomId=${ROOM_ID}&startDate=${DATE_FROM}&endDate=${DATE_TO}&include=prices,minStay`,
    },
    {
      test_number: 2,
      url: `${BEDS24_URL}/inventory/rooms/calendar?roomId=${ROOM_ID}&startDate=${DATE_FROM}&endDate=${DATE_TO}&includePrices=true&includeMinStay=true`,
    },
    {
      test_number: 3,
      url: `${BEDS24_URL}/inventory/rooms/calendar?roomId=${ROOM_ID}&startDate=${DATE_FROM}&endDate=${DATE_TO}&includePrices=true&includeRestrictions=true`,
    },
    {
      test_number: 4,
      url: `${BEDS24_URL}/inventory/rooms/calendar?roomId=${ROOM_ID}&startDate=${DATE_FROM}&endDate=${DATE_TO}&includePrices=true&includeNumAvail=true&includeMinStay=true`,
    },
    {
      test_number: 5,
      url: `${BEDS24_URL}/inventory/rooms/calendar?roomId=${ROOM_ID}&startDate=${DATE_FROM}&endDate=${DATE_TO}&includePrices=true&includeMinStay=true&includeMaxStay=true&includeNumAvail=true`,
    },
    {
      test_number: 6,
      url: `${BEDS24_URL}/inventory/rooms/restrictions?roomId=${ROOM_ID}&startDate=${DATE_FROM}&endDate=${DATE_TO}`,
    },
  ];

  const results = [];
  for (const test of tests) {
    results.push(await runTest(test, accessToken));
  }

  return respond(200, {
    room_id: ROOM_ID,
    tests: results,
  });
};

async function runTest(test, accessToken) {
  try {
    const response = await fetch(test.url, {
      method: 'GET',
      headers: {
        token: accessToken,
        Accept: 'application/json',
      },
    });
    const rawText = await response.text();
    const parsed = tryParseJson(rawText);
    const minStayValues = findMinStayValues(parsed ?? rawText);
    const bodyExcerpt = rawText.slice(0, 2000);

    console.log(`[MINSTAY-DIAG ${test.test_number}] ${test.url}`);
    console.log(`[MINSTAY-DIAG ${test.test_number}] status=${response.status}`);
    console.log(`[MINSTAY-DIAG ${test.test_number}] body=${bodyExcerpt}`);

    return {
      test_number: test.test_number,
      url: test.url,
      status: response.status,
      has_minstay_anywhere: minStayValues.length > 0,
      minstay_values: minStayValues,
      body_excerpt: bodyExcerpt,
    };
  } catch (error) {
    console.error(`[MINSTAY-DIAG ${test.test_number}] exception`, error.message);
    return {
      test_number: test.test_number,
      url: test.url,
      status: 0,
      has_minstay_anywhere: false,
      minstay_values: [],
      body_excerpt: String(error.message || error).slice(0, 2000),
    };
  }
}

function findMinStayValues(root) {
  const found = [];

  function walk(value) {
    if (Array.isArray(value)) {
      value.forEach(walk);
      return;
    }
    if (!value || typeof value !== 'object') {
      return;
    }

    for (const [key, nested] of Object.entries(value)) {
      if (/^(minStay|minimumStay|minNights)$/i.test(key)) {
        found.push({ key, value: nested });
      }
      walk(nested);
    }
  }

  walk(root);
  return found;
}

function tryParseJson(rawText) {
  try {
    return JSON.parse(rawText);
  } catch (_error) {
    return null;
  }
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
  return beds24TokenCache.token;
}

async function safeReadText(response) {
  try {
    return await response.text();
  } catch (_error) {
    return '';
  }
}

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: typeof payload === 'string' ? payload : JSON.stringify(payload),
  };
}
