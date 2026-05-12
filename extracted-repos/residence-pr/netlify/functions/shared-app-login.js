const {
  parsePasswordList,
  signSharedSession,
} = require('./_lib/shared-auth');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};
const LOGIN_ATTEMPTS = new Map();
const LOGIN_WINDOW_MS = 10 * 60 * 1000;
const LOGIN_MAX_ATTEMPTS = 8;

const APP_CONFIG = {
  rm_kekko: {
    role: 'residence_partner',
    envName: 'RESIDENCE_KEKKO_PASSWORDS',
    subject: 'residence-kekko',
  },
  rm_admin: {
    role: 'residence_admin',
    envName: 'RESIDENCE_ADMIN_PASSWORDS',
    subject: 'residence-admin',
  },
};

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers: CORS, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return respond(405, { error: 'Method not allowed' });
  }

  const clientIp = getClientIp(event.headers);
  if (isRateLimited(clientIp)) {
    return respond(429, { error: 'Troppi tentativi di login. Riprova tra qualche minuto.' });
  }

  let body;
  try {
    body = JSON.parse(event.body || '{}');
  } catch (_error) {
    return respond(400, { error: 'JSON non valido' });
  }

  const app = String(body.app || '').trim();
  const password = String(body.password || '');
  const config = APP_CONFIG[app];
  if (!config) {
    return respond(400, { error: 'App non valida' });
  }
  if (!password) {
    return respond(400, { error: 'Password obbligatoria' });
  }

  const configuredPasswords = parsePasswordList(process.env[config.envName]);
  if (!configuredPasswords.length) {
    return respond(503, {
      error: `Configurazione mancante: imposta ${config.envName} nelle env Netlify`,
    });
  }

  if (!configuredPasswords.includes(password)) {
    recordFailedAttempt(clientIp);
    return respond(401, { error: 'Password errata' });
  }

  clearFailedAttempts(clientIp);

  const token = signSharedSession({
    app,
    role: config.role,
    sub: config.subject,
  });
  const payload = decodeSharedSession(token);

  return respond(200, {
    ok: true,
    token,
    role: config.role,
    app,
    expires_at: payload?.exp ? new Date(payload.exp * 1000).toISOString() : null,
  });
};

function decodeSharedSession(token) {
  const encoded = String(token || '').split('.')[0] || '';
  if (!encoded) return null;
  try {
    const normalized = encoded.replace(/-/g, '+').replace(/_/g, '/');
    const padding = normalized.length % 4;
    const padded = normalized + (padding ? '='.repeat(4 - padding) : '');
    return JSON.parse(Buffer.from(padded, 'base64').toString('utf8'));
  } catch (_error) {
    return null;
  }
}

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}

function getClientIp(headers = {}) {
  const forwarded = String(headers['x-forwarded-for'] || headers['X-Forwarded-For'] || '').trim();
  if (forwarded) return forwarded.split(',')[0].trim();
  return String(headers['client-ip'] || headers['Client-Ip'] || 'unknown').trim() || 'unknown';
}

function isRateLimited(clientIp) {
  pruneAttempts();
  const attempts = LOGIN_ATTEMPTS.get(clientIp) || [];
  return attempts.length >= LOGIN_MAX_ATTEMPTS;
}

function recordFailedAttempt(clientIp) {
  pruneAttempts();
  const attempts = LOGIN_ATTEMPTS.get(clientIp) || [];
  attempts.push(Date.now());
  LOGIN_ATTEMPTS.set(clientIp, attempts);
}

function clearFailedAttempts(clientIp) {
  LOGIN_ATTEMPTS.delete(clientIp);
}

function pruneAttempts() {
  const cutoff = Date.now() - LOGIN_WINDOW_MS;
  for (const [clientIp, attempts] of LOGIN_ATTEMPTS.entries()) {
    const filtered = attempts.filter((timestamp) => timestamp >= cutoff);
    if (filtered.length) LOGIN_ATTEMPTS.set(clientIp, filtered);
    else LOGIN_ATTEMPTS.delete(clientIp);
  }
}
