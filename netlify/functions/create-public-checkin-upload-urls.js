const crypto = require('crypto');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');
const {
  derivePublicCheckinUploadBatchSecret,
  issuePublicCheckinUploadBatchToken,
} = require('./_public-checkin-upload-batch-token');
const { resolvePublicCheckinKeyAlias } = require('./_public-checkin-key-rotation');

const DEFAULT_SUPABASE_URL = 'https://tysxeikqbgebpfyblgeb.supabase.co';
const SUPABASE_URL = resolveSupabaseUrl(process.env.SUPABASE_URL);
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const CHECKIN_DOCUMENTS_BUCKET = process.env.PUBLIC_CHECKIN_DOCUMENTS_BUCKET || 'checkin-documents';
const PUBLIC_CHECKIN_UPLOAD_BATCH_SECRET = derivePublicCheckinUploadBatchSecret(
  process.env.PUBLIC_CHECKIN_UPLOAD_BATCH_SECRET,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);
const MAX_CHECKIN_DOCUMENT_SIZE_BYTES = 20 * 1024 * 1024;
const CHECKIN_ALLOWED_UPLOAD_MIME_TYPES = new Set([
  'application/pdf',
  'image/gif',
  'image/heic',
  'image/heif',
  'image/jpeg',
  'image/jpg',
  'image/png',
  'image/webp',
]);
const CHECKIN_ALLOWED_UPLOAD_EXTENSIONS = new Set([
  '.gif',
  '.heic',
  '.heif',
  '.jpeg',
  '.jpg',
  '.pdf',
  '.png',
  '.webp',
]);
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

let checkinDocumentsBucketPromise = null;

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: CORS, body: '' };
  }
  if (event.httpMethod !== 'POST') {
    return respond(405, { error: 'Method not allowed' });
  }
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    return respond(500, { error: 'Missing Supabase server configuration' });
  }

  let body;
  try {
    body = JSON.parse(event.body || '{}');
  } catch (_error) {
    return respond(400, { error: 'Invalid JSON body' });
  }

  const payload = normalizePayload(body);
  const validationErrors = validatePayload(payload);
  if (validationErrors.length) {
    return respond(400, { error: 'Validation failed', fields: validationErrors });
  }

  try {
    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
      auth: { autoRefreshToken: false, persistSession: false },
    });
    const { apartment, error: apartmentError } = await resolveApartmentReference(supabase, payload.apartment_ref);
    if (apartmentError) {
      console.error('[create-public-checkin-upload-urls] apartment lookup error:', apartmentError);
      return respond(500, { error: 'Apartment lookup failed' });
    }
    if (!apartment) {
      return respond(400, {
        error: 'Validation failed',
        fields: [{ field: 'apartment_id', message: 'Link appartamento non valido o disattivato' }],
      });
    }

    await ensureCheckinDocumentsBucket(supabase);
    const uploads = await createSignedUploads(supabase, payload);
    const issued = issuePublicCheckinUploadBatchToken(payload, uploads, PUBLIC_CHECKIN_UPLOAD_BATCH_SECRET);
    if (!issued.ok) {
      return respond(400, {
        error: 'Validation failed',
        fields: issued.documentErrors || (issued.missingFields || []).map((field) => ({
          field,
          message: 'Dato obbligatorio per preparare l upload documenti',
        })),
      });
    }

    return respond(200, {
      ok: true,
      upload_batch_token: issued.token,
      uploads,
      expires_at: issued.expiresAt,
    });
  } catch (error) {
    console.error('[create-public-checkin-upload-urls] error:', error);
    return respond(500, { error: 'Preparazione upload documenti fallita', detail: error.message });
  }
};

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}

function resolveSupabaseUrl(rawValue) {
  const fallback = DEFAULT_SUPABASE_URL;
  const candidate = String(rawValue || '').trim();
  if (!candidate) return fallback;
  try {
    const url = new URL(candidate);
    if (!/^https?:$/i.test(url.protocol)) return fallback;
    return url.toString().replace(/\/+$/, '');
  } catch (_error) {
    return fallback;
  }
}

function normalizePayload(body) {
  return {
    apartment_ref: String(body.apartment_ref || body.apt || body.apartment_id || '').trim(),
    email: String(body.email || '').trim().toLowerCase(),
    telefono: String(body.telefono || '').trim(),
    cognome: String(body.cognome || '').trim(),
    data_checkin: String(body.data_checkin || '').trim(),
    data_checkout: String(body.data_checkout || '').trim(),
    document_uploads: normalizeDocumentUploads(body.document_uploads),
  };
}

function validatePayload(payload) {
  const errors = [];
  if (!payload.apartment_ref) errors.push({ field: 'apartment_id', message: 'Link appartamento mancante' });
  if (!payload.email) errors.push({ field: 'email', message: 'Email obbligatoria' });
  if (!payload.telefono) errors.push({ field: 'telefono', message: 'Telefono obbligatorio' });
  if (!payload.cognome) errors.push({ field: 'cognome', message: 'Cognome obbligatorio' });
  if (!payload.data_checkin) errors.push({ field: 'checkin', message: 'Check-in obbligatorio' });
  if (!payload.data_checkout) errors.push({ field: 'checkout', message: 'Check-out obbligatorio' });
  if (!payload.document_uploads.length) errors.push({ field: 'guest_documents', message: 'Carica almeno un documento valido.' });
  payload.document_uploads.forEach((entry, index) => {
    const label = entry.file_name || `Documento ${index + 1}`;
    if (!entry.file_name) errors.push({ field: 'guest_documents', message: `${label}: nome file mancante.` });
    if (!entry.mime_type) errors.push({ field: 'guest_documents', message: `${label}: tipo file mancante.` });
    if (!isAllowedUploadDescriptor(entry)) {
      errors.push({ field: 'guest_documents', message: `${label}: formato non supportato. Usa PDF, JPG, PNG, WEBP, GIF o HEIC.` });
    }
    if (!entry.size_bytes || entry.size_bytes > MAX_CHECKIN_DOCUMENT_SIZE_BYTES) {
      errors.push({ field: 'guest_documents', message: `${label}: supera il limite di 20 MB.` });
    }
  });
  return errors;
}

function normalizeDocumentUploads(value) {
  const raw = Array.isArray(value) ? value : [];
  return raw.map((entry, index) => ({
    client_id: String(entry?.client_id || '').trim() || `doc-${index + 1}`,
    guest_scope: String(entry?.guest_scope || '').trim() === 'additional' ? 'additional' : 'main',
    guest_index: normalizeNonNegativeInt(entry?.guest_index),
    display_order: normalizeNonNegativeInt(entry?.display_order),
    file_name: String(entry?.file_name || '').trim(),
    mime_type: String(entry?.mime_type || '').trim().toLowerCase(),
    size_bytes: normalizePositiveInt(entry?.size_bytes),
  }));
}

function normalizePositiveInt(value) {
  const parsed = parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
}

function normalizeNonNegativeInt(value) {
  const parsed = parseInt(value, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : 0;
}

function isAllowedUploadDescriptor(entry) {
  const mimeType = String(entry?.mime_type || '').trim().toLowerCase();
  if (CHECKIN_ALLOWED_UPLOAD_MIME_TYPES.has(mimeType)) return true;
  const ext = path.extname(String(entry?.file_name || '').trim()).toLowerCase();
  return CHECKIN_ALLOWED_UPLOAD_EXTENSIONS.has(ext);
}

async function resolveApartmentReference(supabase, rawReference) {
  const reference = resolvePublicCheckinKeyAlias(rawReference);
  if (!reference) return { apartment: null, error: null };

  let query = supabase
    .from('apartments')
    .select('id, attivo, public_checkin_key')
    .eq('attivo', true);

  if (UUID_RE.test(reference)) {
    query = query.eq('id', reference);
  } else {
    query = query.eq('public_checkin_key', reference);
  }

  const { data, error } = await query.maybeSingle();
  if (error) return { apartment: null, error };
  return { apartment: data || null, error: null };
}

async function createSignedUploads(supabase, payload) {
  const datePrefix = new Date().toISOString().slice(0, 10);
  const requestKey = sanitizeStorageKeyPart([
    payload.apartment_ref || 'apartment',
    payload.cognome || 'guest',
    Date.now(),
    crypto.randomUUID().slice(0, 8),
  ].filter(Boolean).join('-'));

  const uploads = [];
  for (let index = 0; index < payload.document_uploads.length; index += 1) {
    const entry = payload.document_uploads[index];
    const safeName = sanitizeFilename(entry.file_name || `documento-${index + 1}`);
    const storagePath = `public-checkin/${datePrefix}/${requestKey}/${String(index + 1).padStart(2, '0')}_${safeName}`;
    const { data, error } = await supabase.storage
      .from(CHECKIN_DOCUMENTS_BUCKET)
      .createSignedUploadUrl(storagePath, { upsert: true });
    if (error) throw error;
    uploads.push({
      client_id: entry.client_id,
      guest_scope: entry.guest_scope,
      guest_index: entry.guest_index,
      display_order: entry.display_order,
      file_name: entry.file_name,
      mime_type: entry.mime_type,
      size_bytes: entry.size_bytes,
      storage_bucket: CHECKIN_DOCUMENTS_BUCKET,
      storage_path: storagePath,
      signed_url: data?.signedUrl || '',
    });
  }
  return uploads;
}

async function ensureCheckinDocumentsBucket(supabase) {
  if (!checkinDocumentsBucketPromise) {
    checkinDocumentsBucketPromise = ensureCheckinDocumentsBucketOnce(supabase);
  }
  try {
    await checkinDocumentsBucketPromise;
  } catch (error) {
    checkinDocumentsBucketPromise = null;
    throw error;
  }
}

async function ensureCheckinDocumentsBucketOnce(supabase) {
  const { data: buckets, error: listError } = await supabase.storage.listBuckets();
  if (listError) throw listError;
  if (Array.isArray(buckets) && buckets.some((bucket) => bucket.name === CHECKIN_DOCUMENTS_BUCKET)) return;

  const { error: createError } = await supabase.storage.createBucket(CHECKIN_DOCUMENTS_BUCKET, {
    public: false,
    fileSizeLimit: '20MB',
    allowedMimeTypes: [...CHECKIN_ALLOWED_UPLOAD_MIME_TYPES],
  });
  if (createError && !String(createError.message || '').toLowerCase().includes('already exists')) {
    throw createError;
  }
}

function sanitizeStorageKeyPart(value) {
  return String(value || 'item')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-zA-Z0-9._-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
    .slice(0, 80) || 'item';
}

function sanitizeFilename(filename) {
  return String(filename || 'documento')
    .replace(/[^a-zA-Z0-9._-]/g, '_')
    .replace(/_+/g, '_')
    .slice(0, 120);
}
