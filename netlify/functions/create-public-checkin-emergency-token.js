const {
  derivePublicCheckinEmergencySecret,
  issuePublicCheckinEmergencyToken,
} = require('./_public-checkin-emergency-token');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: CORS, body: '' };
  }
  if (event.httpMethod !== 'POST') {
    return respond(405, { error: 'Method not allowed' });
  }

  let body;
  try {
    body = JSON.parse(event.body || '{}');
  } catch (_error) {
    return respond(400, { error: 'Invalid JSON body' });
  }

  const secret = derivePublicCheckinEmergencySecret(
    process.env.PUBLIC_CHECKIN_EMERGENCY_TOKEN_SECRET,
    process.env.SUPABASE_SERVICE_ROLE_KEY
  );
  if (!secret) {
    return respond(500, { error: 'Missing emergency token secret' });
  }

  const issued = issuePublicCheckinEmergencyToken(body, secret);
  if (!issued.ok) {
    return respond(400, {
      error: 'Validation failed',
      fields: (issued.missingFields || []).map((field) => ({
        field,
        message: 'Dato obbligatorio per attivare il percorso di emergenza',
      })),
    });
  }

  return respond(200, {
    ok: true,
    token: issued.token,
    expires_at: issued.expiresAt,
  });
};

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}
