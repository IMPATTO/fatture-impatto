const crypto = require('crypto');
const {
  normalizeEmergencyBinding,
  validateEmergencyBinding,
} = require('./_public-checkin-emergency-token');

const UPLOAD_BATCH_TOKEN_TTL_MS = 2 * 60 * 60 * 1000;

function normalizeText(value) {
  return String(value || '').trim().replace(/\s+/g, ' ');
}

function normalizeLower(value) {
  return normalizeText(value).toLowerCase();
}

function normalizeUploadEntries(entries = []) {
  return (Array.isArray(entries) ? entries : [])
    .map((entry, index) => ({
      client_id: normalizeText(entry?.client_id) || `doc-${index + 1}`,
      guest_scope: String(entry?.guest_scope || '').trim() === 'additional' ? 'additional' : 'main',
      guest_index: normalizeNonNegativeInt(entry?.guest_index),
      display_order: normalizeNonNegativeInt(entry?.display_order),
      file_name: normalizeText(entry?.file_name).slice(0, 160),
      mime_type: normalizeLower(entry?.mime_type),
      size_bytes: normalizePositiveInt(entry?.size_bytes),
      storage_bucket: normalizeText(entry?.storage_bucket),
      storage_path: normalizeText(entry?.storage_path),
    }))
    .sort((left, right) => (
      String(left.storage_path || left.client_id).localeCompare(String(right.storage_path || right.client_id))
    ));
}

function normalizePositiveInt(value) {
  const parsed = parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
}

function normalizeNonNegativeInt(value) {
  const parsed = parseInt(value, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : 0;
}

function validateUploadEntries(entries) {
  if (!Array.isArray(entries) || !entries.length) {
    return [{ field: 'guest_documents', message: 'Carica almeno un documento valido.' }];
  }
  const errors = [];
  entries.forEach((entry, index) => {
    const label = `Documento ${index + 1}`;
    if (!entry.file_name) errors.push({ field: 'guest_documents', message: `${label}: nome file mancante.` });
    if (!entry.mime_type) errors.push({ field: 'guest_documents', message: `${label}: tipo file mancante.` });
    if (!entry.size_bytes) errors.push({ field: 'guest_documents', message: `${label}: dimensione file non valida.` });
    if (!entry.storage_bucket) errors.push({ field: 'guest_documents', message: `${label}: bucket mancante.` });
    if (!entry.storage_path) errors.push({ field: 'guest_documents', message: `${label}: percorso storage mancante.` });
  });
  return errors;
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

function derivePublicCheckinUploadBatchSecret(rawSecret, fallbackSeed) {
  const explicitSecret = String(rawSecret || '').trim();
  if (explicitSecret) return explicitSecret;
  const seed = String(fallbackSeed || '').trim();
  if (!seed) return '';
  return crypto
    .createHash('sha256')
    .update(`public-checkin-upload-batch::${seed}`)
    .digest('hex');
}

function issuePublicCheckinUploadBatchToken(payload, entries, secret, options = {}) {
  const binding = normalizeEmergencyBinding(payload);
  const missingFields = validateEmergencyBinding(binding);
  if (missingFields.length) {
    return { ok: false, reason: 'missing_fields', missingFields };
  }
  const documents = normalizeUploadEntries(entries);
  const documentErrors = validateUploadEntries(documents);
  if (documentErrors.length) {
    return { ok: false, reason: 'invalid_documents', documentErrors };
  }
  const safeSecret = String(secret || '').trim();
  if (!safeSecret) {
    return { ok: false, reason: 'missing_secret' };
  }

  const nowMs = Number.isFinite(options.nowMs) ? options.nowMs : Date.now();
  const ttlMs = Number.isFinite(options.ttlMs) && options.ttlMs > 0 ? options.ttlMs : UPLOAD_BATCH_TOKEN_TTL_MS;
  const claims = {
    v: 1,
    ...binding,
    documents,
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

function verifyPublicCheckinUploadBatchToken(token, payload, entries, secret, options = {}) {
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

  const bindingKeys = ['apartment_ref', 'email', 'telefono', 'cognome', 'data_checkin', 'data_checkout'];
  const mismatchField = bindingKeys.find((key) => String(claims?.[key] || '') !== String(expectedBinding[key] || ''));
  if (mismatchField) {
    return { ok: false, reason: 'binding_mismatch', mismatchField, claims };
  }

  const providedEntries = normalizeUploadEntries(entries);
  const claimedEntries = normalizeUploadEntries(claims?.documents || []);
  if (providedEntries.length !== claimedEntries.length) {
    return { ok: false, reason: 'documents_length_mismatch', claims };
  }
  for (let index = 0; index < claimedEntries.length; index += 1) {
    const left = claimedEntries[index];
    const right = providedEntries[index];
    const keys = ['client_id', 'guest_scope', 'guest_index', 'display_order', 'file_name', 'mime_type', 'size_bytes', 'storage_bucket', 'storage_path'];
    const mismatch = keys.find((key) => String(left?.[key] ?? '') !== String(right?.[key] ?? ''));
    if (mismatch) {
      return { ok: false, reason: 'documents_mismatch', mismatchField: mismatch, claims };
    }
  }

  return { ok: true, claims };
}

module.exports = {
  UPLOAD_BATCH_TOKEN_TTL_MS,
  derivePublicCheckinUploadBatchSecret,
  issuePublicCheckinUploadBatchToken,
  normalizeUploadEntries,
  validateUploadEntries,
  verifyPublicCheckinUploadBatchToken,
};
