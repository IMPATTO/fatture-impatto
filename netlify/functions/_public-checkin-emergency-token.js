const crypto = require('crypto');

const EMERGENCY_TOKEN_TTL_MS = 30 * 60 * 1000;

function normalizeText(value) {
  return String(value || '').trim().replace(/\s+/g, ' ');
}

function normalizeLower(value) {
  return normalizeText(value).toLowerCase();
}

function normalizeUpper(value) {
  return normalizeText(value).toUpperCase();
}

function normalizePhone(value) {
  return String(value || '').replace(/[^\d+]/g, '').trim();
}

function normalizeDate(value) {
  const raw = normalizeText(value);
  return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : '';
}

function normalizeEmergencyBinding(payload = {}) {
  return {
    apartment_ref: normalizeLower(payload.apartment_ref),
    email: normalizeLower(payload.email),
    telefono: normalizePhone(payload.telefono || payload.phone),
    cognome: normalizeUpper(payload.cognome),
    data_checkin: normalizeDate(payload.data_checkin),
    data_checkout: normalizeDate(payload.data_checkout),
  };
}

function validateEmergencyBinding(binding) {
  const missingFields = [];
  if (!binding.apartment_ref) missingFields.push('apartment_ref');
  if (!binding.email) missingFields.push('email');
  if (!binding.telefono) missingFields.push('telefono');
  if (!binding.cognome) missingFields.push('cognome');
  if (!binding.data_checkin) missingFields.push('data_checkin');
  if (!binding.data_checkout) missingFields.push('data_checkout');
  return missingFields;
}

function base64UrlEncode(value) {
  return Buffer.from(value)
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '');
}

function base64UrlDecode(value) {
  const normalized = String(value || '').replace(/-/g, '+').replace(/_/g, '/');
  const padding = normalized.length % 4 === 0 ? '' : '='.repeat(4 - (normalized.length % 4));
  return Buffer.from(normalized + padding, 'base64').toString('utf8');
}

function createSignature(encodedPayload, secret) {
  return base64UrlEncode(
    crypto.createHmac('sha256', String(secret || ''))
      .update(String(encodedPayload || ''))
      .digest()
  );
}

function derivePublicCheckinEmergencySecret(rawSecret, fallbackSeed) {
  const explicitSecret = String(rawSecret || '').trim();
  if (explicitSecret) return explicitSecret;
  const seed = String(fallbackSeed || '').trim();
  if (!seed) return '';
  return crypto
    .createHash('sha256')
    .update(`public-checkin-emergency::${seed}`)
    .digest('hex');
}

function issuePublicCheckinEmergencyToken(payload, secret, options = {}) {
  const binding = normalizeEmergencyBinding(payload);
  const missingFields = validateEmergencyBinding(binding);
  if (missingFields.length) {
    return { ok: false, reason: 'missing_fields', missingFields };
  }
  const safeSecret = String(secret || '').trim();
  if (!safeSecret) {
    return { ok: false, reason: 'missing_secret', missingFields: [] };
  }

  const nowMs = Number.isFinite(options.nowMs) ? options.nowMs : Date.now();
  const ttlMs = Number.isFinite(options.ttlMs) && options.ttlMs > 0 ? options.ttlMs : EMERGENCY_TOKEN_TTL_MS;
  const claims = {
    v: 1,
    ...binding,
    nonce: crypto.randomUUID(),
    iat: Math.floor(nowMs / 1000),
    exp: Math.floor((nowMs + ttlMs) / 1000),
  };
  const encodedPayload = base64UrlEncode(JSON.stringify(claims));
  const signature = createSignature(encodedPayload, safeSecret);
  return {
    ok: true,
    token: `${encodedPayload}.${signature}`,
    claims,
    expiresAt: new Date((claims.exp || 0) * 1000).toISOString(),
  };
}

function verifyPublicCheckinEmergencyToken(token, payload, secret, options = {}) {
  const safeSecret = String(secret || '').trim();
  if (!safeSecret) {
    return { ok: false, reason: 'missing_secret' };
  }

  const rawToken = String(token || '').trim();
  if (!rawToken) {
    return { ok: false, reason: 'missing_token' };
  }

  const parts = rawToken.split('.');
  if (parts.length !== 2 || !parts[0] || !parts[1]) {
    return { ok: false, reason: 'invalid_format' };
  }

  const [encodedPayload, providedSignature] = parts;
  const expectedSignature = createSignature(encodedPayload, safeSecret);
  const providedBuffer = Buffer.from(providedSignature);
  const expectedBuffer = Buffer.from(expectedSignature);
  if (providedBuffer.length !== expectedBuffer.length || !crypto.timingSafeEqual(providedBuffer, expectedBuffer)) {
    return { ok: false, reason: 'invalid_signature' };
  }

  let claims;
  try {
    claims = JSON.parse(base64UrlDecode(encodedPayload));
  } catch (_error) {
    return { ok: false, reason: 'invalid_payload' };
  }

  const nowSeconds = Math.floor((Number.isFinite(options.nowMs) ? options.nowMs : Date.now()) / 1000);
  if (!Number.isFinite(claims?.exp) || claims.exp < nowSeconds) {
    return { ok: false, reason: 'expired', claims };
  }

  const expectedBinding = normalizeEmergencyBinding(payload);
  const missingFields = validateEmergencyBinding(expectedBinding);
  if (missingFields.length) {
    return { ok: false, reason: 'missing_fields', missingFields, claims };
  }

  const keys = ['apartment_ref', 'email', 'telefono', 'cognome', 'data_checkin', 'data_checkout'];
  const mismatchField = keys.find((key) => String(claims?.[key] || '') !== String(expectedBinding[key] || ''));
  if (mismatchField) {
    return { ok: false, reason: 'binding_mismatch', mismatchField, claims };
  }

  return { ok: true, claims };
}

module.exports = {
  EMERGENCY_TOKEN_TTL_MS,
  derivePublicCheckinEmergencySecret,
  issuePublicCheckinEmergencyToken,
  normalizeEmergencyBinding,
  validateEmergencyBinding,
  verifyPublicCheckinEmergencyToken,
};
