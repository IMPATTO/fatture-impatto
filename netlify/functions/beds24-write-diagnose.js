const {
  getAuthContextFromHeaders,
} = require('./_lib/shared-auth');

const BASE_URL = 'https://api.beds24.com/v2';

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Content-Type': 'application/json; charset=utf-8',
};

const ROOM_ID = 674231;
const PROPERTY_ID = 324764;
const TEST_PRICE = 999;
const TEST_MIN_STAY = 7;

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers: CORS, body: '' };
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

  if (!process.env.BEDS24_REFRESH_TOKEN) {
    return respond(500, { error: 'BEDS24_REFRESH_TOKEN mancante' });
  }

  try {
    const accessToken = await getBeds24AccessToken();
    const tests = [];

    tests.push(await runCalendarWriteTest({
      testNumber: 1,
      accessToken,
      requestBody: [
        {
          roomId: ROOM_ID,
          calendar: [
            {
              from: '2027-12-01',
              to: '2027-12-01',
              price1: TEST_PRICE,
            },
          ],
        },
      ],
    }));

    tests.push(await runCalendarWriteTest({
      testNumber: 2,
      accessToken,
      requestBody: [
        {
          roomId: ROOM_ID,
          calendar: [
            {
              from: '2027-12-02',
              to: '2027-12-02',
              price1: 888,
              minStay: TEST_MIN_STAY,
            },
          ],
        },
      ],
    }));

    tests.push(await runCalendarWriteTest({
      testNumber: 3,
      accessToken,
      requestBody: [
        {
          roomId: ROOM_ID,
          calendar: [
            {
              from: '2027-12-03',
              to: '2027-12-07',
              price1: 777,
              minStay: 3,
            },
          ],
        },
      ],
    }));

    tests.push(await runCalendarWriteTest({
      testNumber: 4,
      accessToken,
      requestBody: [
        {
          roomId: ROOM_ID,
          calendar: [
            {
              from: '2027-12-08',
              to: '2027-12-08',
              numAvail: 0,
            },
          ],
        },
      ],
    }));

    tests.push(await runCalendarWriteTest({
      testNumber: 5,
      accessToken,
      requestBody: [
        {
          roomId: ROOM_ID,
          calendar: [
            {
              from: '2027-12-09',
              to: '2027-12-09',
              price1: -100,
            },
          ],
        },
      ],
    }));

    return respond(200, {
      room_id: ROOM_ID,
      property_id: PROPERTY_ID,
      tests,
    });
  } catch (error) {
    console.error('[beds24-write-diagnose] fatal', error);
    return respond(500, { error: error.message || 'Diagnostica non riuscita' });
  }
};

async function getBeds24AccessToken() {
  const response = await fetch(`${BASE_URL}/authentication/token`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      refreshToken: String(process.env.BEDS24_REFRESH_TOKEN || '').trim(),
    },
  });

  const text = await response.text();
  let parsed = null;
  try {
    parsed = text ? JSON.parse(text) : null;
  } catch (_error) {
    parsed = text;
  }

  console.log('[AUTH] status=', response.status);
  console.log('[AUTH] body=', typeof parsed === 'string' ? parsed : JSON.stringify(parsed, null, 2));

  if (!response.ok || !parsed?.token) {
    throw new Error(`Beds24 auth failed: ${response.status}`);
  }

  return parsed.token;
}

async function runCalendarWriteTest({ testNumber, accessToken, requestBody }) {
  const url = `${BASE_URL}/inventory/rooms/calendar`;
  console.log(`[TEST ${testNumber}] POST ${url}`);
  console.log(`[TEST ${testNumber}] request_body=`, JSON.stringify(requestBody, null, 2));

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      token: accessToken,
      Accept: 'application/json',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(requestBody),
  });

  const text = await response.text();
  let parsed = null;
  try {
    parsed = text ? JSON.parse(text) : null;
  } catch (_error) {
    parsed = text;
  }

  console.log(`[TEST ${testNumber}] status=`, response.status);
  console.log(
    `[TEST ${testNumber}] response_body=`,
    typeof parsed === 'string' ? parsed : JSON.stringify(parsed, null, 2),
  );

  return {
    test_number: testNumber,
    url,
    request_body: requestBody,
    status: response.status,
    response_body: parsed,
    actual_change_observed: 'manuale da Marco nel Beds24 UI',
  };
}

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}
