const Busboy = require('busboy');
const crypto = require('crypto');
const { createClient } = require('@supabase/supabase-js');
const { normalizeSupabaseUrl } = require('./_lib/shared-auth');

const SUPABASE_URL = normalizeSupabaseUrl(process.env.SUPABASE_RUNTIME_URL || process.env.SUPABASE_URL);
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
const STORAGE_BUCKET = process.env.CONTABILITA_PRIVATE_STORAGE_BUCKET || 'contabilita-private-media';
const MAX_FILES = 6;
const MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024;
const MAX_TOTAL_SIZE_BYTES = 25 * 1024 * 1024;
const ALLOWED_MIME_TYPES = new Set([
  'application/pdf',
  'image/jpeg',
  'image/png',
  'image/webp',
  'image/gif',
]);
const UPLOAD_ATTEMPTS = new Map();
const RATE_LIMIT_WINDOW_MS = 15 * 60 * 1000;
const RATE_LIMIT_MAX_REQUESTS = 8;

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

  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    return respond(500, { error: 'Configurazione Supabase mancante' });
  }

  try {
    const clientIp = getClientIp(event.headers);
    if (isRateLimited(clientIp)) {
      return respond(429, { error: 'Troppi invii ravvicinati. Riprova più tardi.' });
    }

    const parsed = await parseMultipart(event);
    const form = normalizeFields(parsed.fields || {});
    const validationErrors = validateForm(form, parsed.files || []);
    if (validationErrors.length) {
      return respond(400, { error: 'Validation failed', fields: validationErrors });
    }
    recordAttempt(clientIp);

    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
    const storedFiles = await persistAttachments(supabase, parsed.files || [], form);
    const guestResult = await createGuestLeadRecord(supabase, form);
    const accountingRecord = await createAccountingDocument(supabase, form, storedFiles, guestResult);

    return respond(200, {
      ok: true,
      guest_saved: guestResult.saved,
      guest_id: guestResult.id || null,
      guest_warning: guestResult.warning || null,
      document_id: accountingRecord.id,
      attachments_saved: storedFiles.length,
    });
  } catch (error) {
    console.error('[submit-public-invoice-documents] error:', error);
    return respond(500, { error: 'Invio richiesta fattura fallito', detail: error.message });
  }
};

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}

function parseMultipart(event) {
  return new Promise((resolve, reject) => {
    const contentType = event.headers['content-type'] || event.headers['Content-Type'] || '';
    if (!String(contentType).toLowerCase().includes('multipart/form-data')) {
      reject(new Error('Content-Type multipart/form-data richiesto'));
      return;
    }

    const fields = {};
    const files = [];
    const busboy = Busboy({ headers: { 'content-type': contentType } });
    const raw = event.isBase64Encoded
      ? Buffer.from(event.body || '', 'base64')
      : Buffer.from(event.body || '');

    busboy.on('field', (name, value) => {
      if (fields[name] !== undefined) {
        if (!Array.isArray(fields[name])) fields[name] = [fields[name]];
        fields[name].push(value);
      } else {
        fields[name] = value;
      }
    });

    busboy.on('file', (name, stream, info) => {
      const chunks = [];
      stream.on('data', (chunk) => chunks.push(chunk));
      stream.on('end', () => {
        files.push({
          field_name: name,
          filename: String(info.filename || '').trim(),
          mime_type: String(info.mimeType || '').trim() || 'application/octet-stream',
          buffer: Buffer.concat(chunks),
          size_bytes: chunks.reduce((sum, chunk) => sum + chunk.length, 0),
        });
      });
    });

    busboy.on('finish', () => resolve({ fields, files }));
    busboy.on('error', reject);
    busboy.end(raw);
  });
}

function normalizeFields(fields) {
  const type = String(fields.tipo_cliente || 'azienda').trim().toLowerCase();
  return {
    tipo_cliente: ['azienda', 'professionista'].includes(type) ? type : 'azienda',
    ragione_sociale: clean(fields.ragione_sociale),
    referente_nome: clean(fields.referente_nome),
    referente_cognome: clean(fields.referente_cognome),
    email: clean(fields.email).toLowerCase(),
    telefono: clean(fields.telefono),
    piva_cliente: clean(fields.piva_cliente).toUpperCase(),
    codice_fiscale: clean(fields.codice_fiscale).toUpperCase(),
    indirizzo_fatturazione: clean(fields.indirizzo_fatturazione),
    sdi: clean(fields.sdi).toUpperCase(),
    pec: clean(fields.pec).toLowerCase(),
    note: clean(fields.note),
    privacy_accepted: String(fields.privacy_accepted || '').trim().toLowerCase() === 'true',
  };
}

function validateForm(form, files) {
  const errors = [];
  if (!form.ragione_sociale) errors.push(fieldError('ragione_sociale', 'Ragione sociale obbligatoria'));
  if (!form.referente_nome) errors.push(fieldError('referente_nome', 'Nome referente obbligatorio'));
  if (!form.referente_cognome) errors.push(fieldError('referente_cognome', 'Cognome referente obbligatorio'));
  if (!form.email) errors.push(fieldError('email', 'Email obbligatoria'));
  else if (!isValidEmail(form.email)) errors.push(fieldError('email', 'Email non valida'));
  if (!form.telefono) errors.push(fieldError('telefono', 'Telefono obbligatorio'));
  if (!form.piva_cliente) errors.push(fieldError('piva_cliente', 'Partita IVA obbligatoria'));
  else if (!looksLikeVat(form.piva_cliente)) errors.push(fieldError('piva_cliente', 'Partita IVA non valida'));
  if (!form.indirizzo_fatturazione) errors.push(fieldError('indirizzo_fatturazione', 'Sede legale obbligatoria'));
  if (form.pec && !isValidEmail(form.pec)) errors.push(fieldError('pec', 'PEC non valida'));
  if (!Array.isArray(files) || !files.length) errors.push(fieldError('files', 'Carica almeno un documento'));
  if (Array.isArray(files) && files.some((file) => !file.filename || !file.buffer?.length)) {
    errors.push(fieldError('files', 'Uno o più file non sono validi'));
  }
  if (Array.isArray(files) && files.length > MAX_FILES) {
    errors.push(fieldError('files', `Puoi caricare massimo ${MAX_FILES} file`));
  }
  const totalSize = Array.isArray(files)
    ? files.reduce((sum, file) => sum + Number(file.size_bytes || file.buffer?.length || 0), 0)
    : 0;
  if (totalSize > MAX_TOTAL_SIZE_BYTES) {
    errors.push(fieldError('files', 'Dimensione totale allegati troppo alta'));
  }
  if (Array.isArray(files) && files.some((file) => Number(file.size_bytes || file.buffer?.length || 0) > MAX_FILE_SIZE_BYTES)) {
    errors.push(fieldError('files', 'Uno o più file superano la dimensione massima consentita'));
  }
  if (Array.isArray(files) && files.some((file) => !ALLOWED_MIME_TYPES.has(String(file.mime_type || '').toLowerCase()))) {
    errors.push(fieldError('files', 'Sono consentiti solo PDF o immagini JPG/PNG/WebP/GIF'));
  }
  if (!form.privacy_accepted) errors.push(fieldError('privacy', 'Accetta l informativa privacy per continuare'));
  return errors;
}

function fieldError(field, message) {
  return { field, message };
}

function clean(value) {
  return String(value || '').trim();
}

function isValidEmail(value) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(value || '').trim());
}

function looksLikeVat(value) {
  const normalized = String(value || '').replace(/\s+/g, '').toUpperCase();
  if (/^\d{11}$/.test(normalized)) return true;
  return /^[A-Z]{2}[A-Z0-9]{8,14}$/.test(normalized);
}

async function persistAttachments(supabase, files, form) {
  await ensureStorageBucket(supabase);
  const datePrefix = new Date().toISOString().slice(0, 10);
  const requestKey = sanitizeStorageKeyPart(`${form.ragione_sociale || 'azienda'}-${Date.now()}`);
  const attachments = [];

  for (let index = 0; index < files.length; index += 1) {
    const file = files[index];
    const safeName = sanitizeFilename(file.filename || `documento-${index + 1}`);
    const storagePath = `richieste-fattura-pubbliche/${datePrefix}/${requestKey}_${index}_${safeName}`;
    const { error } = await supabase.storage.from(STORAGE_BUCKET).upload(storagePath, file.buffer, {
      contentType: file.mime_type || 'application/octet-stream',
      upsert: true,
    });
    if (error) throw error;
    attachments.push({
      filename: file.filename,
      storage_bucket: STORAGE_BUCKET,
      storage_path: storagePath,
      url: '',
      preview_url: '',
      content_type: file.mime_type || 'application/octet-stream',
      size_bytes: Number(file.size_bytes || file.buffer.length || 0),
    });
  }

  return attachments;
}

async function ensureStorageBucket(supabase) {
  const { data: buckets, error: listError } = await supabase.storage.listBuckets();
  if (listError) throw listError;
  const existingBucket = Array.isArray(buckets)
    ? buckets.find((bucket) => bucket.name === STORAGE_BUCKET)
    : null;
  if (existingBucket) {
    if (existingBucket.public) {
      throw new Error(`Il bucket ${STORAGE_BUCKET} esiste ma è pubblico: impostalo privato prima di usarlo`);
    }
    return;
  }

  const { error: createError } = await supabase.storage.createBucket(STORAGE_BUCKET, {
    public: false,
    fileSizeLimit: '25MB',
    allowedMimeTypes: [...ALLOWED_MIME_TYPES],
  });

  if (createError && !String(createError.message || '').toLowerCase().includes('already exists')) {
    throw createError;
  }
}

async function createGuestLeadRecord(supabase, form) {
  const columnSupport = {
    tipo_cliente: await hasColumn(supabase, 'ospiti_check_in', 'tipo_cliente'),
    ragione_sociale: await hasColumn(supabase, 'ospiti_check_in', 'ragione_sociale'),
    indirizzo_fatturazione: await hasColumn(supabase, 'ospiti_check_in', 'indirizzo_fatturazione'),
    piva_cliente: await hasColumn(supabase, 'ospiti_check_in', 'piva_cliente'),
    segnalazione_jessica: await hasColumn(supabase, 'ospiti_check_in', 'segnalazione_jessica'),
    lingua: await hasColumn(supabase, 'ospiti_check_in', 'lingua'),
    dati_completi: await hasColumn(supabase, 'ospiti_check_in', 'dati_completi'),
    codice_fiscale: await hasColumn(supabase, 'ospiti_check_in', 'codice_fiscale'),
    codice_fiscale_verificato: await hasColumn(supabase, 'ospiti_check_in', 'codice_fiscale_verificato'),
    stato: await hasColumn(supabase, 'ospiti_check_in', 'stato'),
    data_checkin: await hasColumn(supabase, 'ospiti_check_in', 'data_checkin'),
    data_checkout: await hasColumn(supabase, 'ospiti_check_in', 'data_checkout'),
    apartment_id: await hasColumn(supabase, 'ospiti_check_in', 'apartment_id'),
  };

  const today = new Date().toISOString().slice(0, 10);
  const noteParts = [
    'RICHIESTA_FATTURA_PUBBLICA',
    form.sdi ? `SDI:${form.sdi}` : '',
    form.pec ? `PEC:${form.pec}` : '',
  ].filter(Boolean);

  const payload = {
    nome: form.referente_nome,
    cognome: form.referente_cognome,
    email: form.email,
    telefono: form.telefono,
  };

  if (columnSupport.tipo_cliente) payload.tipo_cliente = form.tipo_cliente;
  if (columnSupport.ragione_sociale) payload.ragione_sociale = form.ragione_sociale;
  if (columnSupport.indirizzo_fatturazione) payload.indirizzo_fatturazione = form.indirizzo_fatturazione;
  if (columnSupport.piva_cliente) payload.piva_cliente = form.piva_cliente;
  if (columnSupport.segnalazione_jessica) payload.segnalazione_jessica = noteParts.join('|') || null;
  if (columnSupport.lingua) payload.lingua = 'IT';
  if (columnSupport.dati_completi) payload.dati_completi = false;
  if (columnSupport.codice_fiscale) payload.codice_fiscale = form.codice_fiscale || null;
  if (columnSupport.codice_fiscale_verificato) payload.codice_fiscale_verificato = false;
  if (columnSupport.stato) payload.stato = 'DA_VERIFICARE';
  if (columnSupport.data_checkin) payload.data_checkin = null;
  if (columnSupport.data_checkout) payload.data_checkout = null;
  if (columnSupport.apartment_id) payload.apartment_id = null;

  const attempts = [
    payload,
    { ...payload, data_checkin: today, data_checkout: today },
    stripNullableKey({ ...payload, data_checkin: today, data_checkout: today }, 'apartment_id'),
  ];

  for (const candidate of attempts) {
    const { data, error } = await supabase
      .from('ospiti_check_in')
      .insert(candidate)
      .select('id')
      .single();

    if (!error && data?.id) {
      return { saved: true, id: data.id, warning: null };
    }
  }

  return {
    saved: false,
    id: null,
    warning: 'Record cliente non salvato in ospiti_check_in, allegati salvati comunque in contabilità',
  };
}

function stripNullableKey(obj, key) {
  const next = { ...obj };
  delete next[key];
  return next;
}

async function createAccountingDocument(supabase, form, attachments, guestResult) {
  const columnSupport = {
    source: await hasColumn(supabase, 'contabilita_documenti', 'source'),
    source_ref: await hasColumn(supabase, 'contabilita_documenti', 'source_ref'),
    file_name: await hasColumn(supabase, 'contabilita_documenti', 'file_name'),
    file_url: await hasColumn(supabase, 'contabilita_documenti', 'file_url'),
    mime_type: await hasColumn(supabase, 'contabilita_documenti', 'mime_type'),
    document_kind: await hasColumn(supabase, 'contabilita_documenti', 'document_kind'),
    counterparty: await hasColumn(supabase, 'contabilita_documenti', 'counterparty'),
    description: await hasColumn(supabase, 'contabilita_documenti', 'description'),
    fiscal_status: await hasColumn(supabase, 'contabilita_documenti', 'fiscal_status'),
    approval_status: await hasColumn(supabase, 'contabilita_documenti', 'approval_status'),
    notes: await hasColumn(supabase, 'contabilita_documenti', 'notes'),
    extracted_json: await hasColumn(supabase, 'contabilita_documenti', 'extracted_json'),
    reviewed_json: await hasColumn(supabase, 'contabilita_documenti', 'reviewed_json'),
    tags: await hasColumn(supabase, 'contabilita_documenti', 'tags'),
    category: await hasColumn(supabase, 'contabilita_documenti', 'category'),
    subcategory: await hasColumn(supabase, 'contabilita_documenti', 'subcategory'),
  };

  const messageId = `public-invoice-${crypto.randomUUID()}`;
  const firstAttachment = attachments[0] || null;
  const title = `Richiesta fattura pubblica - ${form.ragione_sociale}`;
  const structuredPayload = {
    tipo_cliente: form.tipo_cliente,
    ragione_sociale: form.ragione_sociale,
    referente_nome: form.referente_nome,
    referente_cognome: form.referente_cognome,
    email: form.email,
    telefono: form.telefono,
    piva_cliente: form.piva_cliente,
    codice_fiscale: form.codice_fiscale || null,
    indirizzo_fatturazione: form.indirizzo_fatturazione,
    sdi: form.sdi || null,
    pec: form.pec || null,
    note: form.note || null,
    guest_saved: guestResult.saved,
    guest_id: guestResult.id || null,
    attachments,
  };

  const payload = {
    source_channel: 'portal',
    source_email: form.email,
    source_subject: title,
    source_message_id: messageId,
    supplier_name: form.ragione_sociale,
    document_type: 'altro',
    stato: 'DA_APPROVARE',
    attachment_count: attachments.length,
    attachments,
    raw_payload: structuredPayload,
    extracted_text: form.note || '',
    ocr_payload: {},
    received_at: new Date().toISOString(),
  };

  if (columnSupport.source) payload.source = 'public_invoice_upload';
  if (columnSupport.source_ref) payload.source_ref = messageId;
  if (columnSupport.file_name) payload.file_name = firstAttachment?.filename || null;
  if (columnSupport.file_url) payload.file_url = firstAttachment?.storage_path || null;
  if (columnSupport.mime_type) payload.mime_type = firstAttachment?.content_type || null;
  if (columnSupport.document_kind) payload.document_kind = 'altro';
  if (columnSupport.counterparty) payload.counterparty = form.ragione_sociale;
  if (columnSupport.description) payload.description = form.note || 'Richiesta fattura pubblica da portale';
  if (columnSupport.fiscal_status) payload.fiscal_status = 'da_fatturare';
  if (columnSupport.approval_status) payload.approval_status = 'da_revisionare';
  if (columnSupport.notes) {
    payload.notes = [
      `Referente: ${form.referente_nome} ${form.referente_cognome}`,
      `Telefono: ${form.telefono}`,
      form.sdi ? `SDI: ${form.sdi}` : '',
      form.pec ? `PEC: ${form.pec}` : '',
      guestResult.saved ? `ospiti_check_in: ${guestResult.id}` : guestResult.warning || '',
    ].filter(Boolean).join(' | ');
  }
  if (columnSupport.extracted_json) payload.extracted_json = structuredPayload;
  if (columnSupport.reviewed_json) payload.reviewed_json = {};
  if (columnSupport.tags) payload.tags = ['richiesta-fattura-pubblica', form.tipo_cliente];
  if (columnSupport.category) payload.category = 'fatturazione';
  if (columnSupport.subcategory) payload.subcategory = 'richiesta_fattura_pubblica';

  const { data, error } = await supabase
    .from('contabilita_documenti')
    .insert(payload)
    .select('id')
    .single();

  if (error) throw error;
  return data;
}

async function hasColumn(supabase, table, column) {
  const { error } = await supabase.from(table).select(column).limit(0);
  return !error;
}

function sanitizeFilename(filename) {
  return String(filename || 'allegato')
    .replace(/[^a-zA-Z0-9._-]/g, '_')
    .replace(/_+/g, '_')
    .slice(0, 120);
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

function getClientIp(headers = {}) {
  const forwarded = String(headers['x-forwarded-for'] || headers['X-Forwarded-For'] || '').trim();
  if (forwarded) return forwarded.split(',')[0].trim();
  return String(headers['client-ip'] || headers['Client-Ip'] || 'unknown').trim() || 'unknown';
}

function isRateLimited(clientIp) {
  pruneAttempts();
  return (UPLOAD_ATTEMPTS.get(clientIp) || []).length >= RATE_LIMIT_MAX_REQUESTS;
}

function recordAttempt(clientIp) {
  pruneAttempts();
  const attempts = UPLOAD_ATTEMPTS.get(clientIp) || [];
  attempts.push(Date.now());
  UPLOAD_ATTEMPTS.set(clientIp, attempts);
}

function pruneAttempts() {
  const cutoff = Date.now() - RATE_LIMIT_WINDOW_MS;
  for (const [clientIp, attempts] of UPLOAD_ATTEMPTS.entries()) {
    const filtered = attempts.filter((timestamp) => timestamp >= cutoff);
    if (filtered.length) UPLOAD_ATTEMPTS.set(clientIp, filtered);
    else UPLOAD_ATTEMPTS.delete(clientIp);
  }
}
