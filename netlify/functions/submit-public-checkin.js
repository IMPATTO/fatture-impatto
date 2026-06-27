const Busboy = require('busboy');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');
const {
  preferCurrentComuneRows,
} = require('./_current-comuni-index');
const {
  derivePublicCheckinEmergencySecret,
  verifyPublicCheckinEmergencyToken,
} = require('./_public-checkin-emergency-token');
const {
  derivePublicCheckinUploadBatchSecret,
  verifyPublicCheckinUploadBatchToken,
} = require('./_public-checkin-upload-batch-token');
const { resolvePublicCheckinKeyAlias } = require('./_public-checkin-key-rotation');
const { ensureLocalInvoiceDraftForOspiteId } = require('./_lib/fatture-fic');
const {
  DEFAULT_CURRENCY: DEFAULT_STRIPE_TOURIST_TAX_CURRENCY,
  STRIPE_CHECKOUT_METHOD,
  evaluateStripeCheckoutSession,
  retrieveStripeCheckoutSession,
} = require('./_lib/public-tourist-tax-stripe');

const DEFAULT_SUPABASE_URL = 'https://tysxeikqbgebpfyblgeb.supabase.co';
const SUPABASE_URL = resolveSupabaseUrl(process.env.SUPABASE_URL);
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const PUBLIC_CHECKIN_EMERGENCY_TOKEN_SECRET = derivePublicCheckinEmergencySecret(
  process.env.PUBLIC_CHECKIN_EMERGENCY_TOKEN_SECRET,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);
const PUBLIC_CHECKIN_UPLOAD_BATCH_SECRET = derivePublicCheckinUploadBatchSecret(
  process.env.PUBLIC_CHECKIN_UPLOAD_BATCH_SECRET,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);
const PUBLIC_PORTAL_BASE_URL = process.env.PUBLIC_PORTAL_BASE_URL || '';
const CHECKIN_DOCUMENTS_BUCKET = process.env.PUBLIC_CHECKIN_DOCUMENTS_BUCKET || 'checkin-documents';
const MAX_CHECKIN_DOCUMENT_SIZE_BYTES = 20 * 1024 * 1024;
const BOOKING_CHANNEL_HINT = 'booking';
const TOURIST_TAX_DOCUMENT_SCOPE = 'tourist_tax';
const BANK_TRANSFER_TOURIST_TAX_METHOD = 'bank_transfer';
let siteLinksConfigPromise = null;
let checkinDocumentsBucketPromise = null;

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};
const ITALIA_CODE = '100000100';
const DOCUMENT_CODE_PREFERENCES = {
  "carta d'identita": ['IDENT', 'IDELE', 'CERID'],
  'carta di identita': ['IDENT', 'IDELE', 'CERID'],
  'passaporto': ['PASOR', 'PASSE', 'PASDI'],
  'patente': ['PATEN', 'PATNA'],
};
const STATE_ALIASES = {
  usa: "STATI UNITI D'AMERICA",
  'u.s.a.': "STATI UNITI D'AMERICA",
  us: "STATI UNITI D'AMERICA",
  'u.s.': "STATI UNITI D'AMERICA",
  'united states': "STATI UNITI D'AMERICA",
  'united states of america': "STATI UNITI D'AMERICA",
  america: "STATI UNITI D'AMERICA",
  americana: "STATI UNITI D'AMERICA",
  americano: "STATI UNITI D'AMERICA",
  americane: "STATI UNITI D'AMERICA",
  americani: "STATI UNITI D'AMERICA",
  uk: 'REGNO UNITO',
  'u.k.': 'REGNO UNITO',
  britain: 'REGNO UNITO',
  'great britain': 'REGNO UNITO',
  england: 'REGNO UNITO',
  britannica: 'REGNO UNITO',
  britannico: 'REGNO UNITO',
  britanniche: 'REGNO UNITO',
  britannici: 'REGNO UNITO',
  france: 'FRANCIA',
  french: 'FRANCIA',
  francese: 'FRANCIA',
  francesi: 'FRANCIA',
  germany: 'GERMANIA',
  german: 'GERMANIA',
  deutschland: 'GERMANIA',
  tedesca: 'GERMANIA',
  tedesco: 'GERMANIA',
  tedesche: 'GERMANIA',
  tedeschi: 'GERMANIA',
  spain: 'SPAGNA',
  spanish: 'SPAGNA',
  espana: 'SPAGNA',
  espagna: 'SPAGNA',
  spagnola: 'SPAGNA',
  spagnolo: 'SPAGNA',
  spagnole: 'SPAGNA',
  spagnoli: 'SPAGNA',
  netherlands: 'PAESI BASSI',
  dutch: 'PAESI BASSI',
  holland: 'PAESI BASSI',
  olanda: 'PAESI BASSI',
  belga: 'BELGIO',
  belghe: 'BELGIO',
  belgi: 'BELGIO',
  switzerland: 'SVIZZERA',
  swiss: 'SVIZZERA',
  svizzera: 'SVIZZERA',
  svizzero: 'SVIZZERA',
  svizzere: 'SVIZZERA',
  svizzeri: 'SVIZZERA',
  albanese: 'ALBANIA',
  albanesi: 'ALBANIA',
  moroccan: 'MAROCCO',
  marocchina: 'MAROCCO',
  marocchino: 'MAROCCO',
  marocchine: 'MAROCCO',
  marocchini: 'MAROCCO',
  romanian: 'ROMANIA',
  rumena: 'ROMANIA',
  rumeno: 'ROMANIA',
  rumene: 'ROMANIA',
  rumeni: 'ROMANIA',
  russian: 'FEDERAZIONE RUSSA',
  russa: 'FEDERAZIONE RUSSA',
  russo: 'FEDERAZIONE RUSSA',
  russe: 'FEDERAZIONE RUSSA',
  russi: 'FEDERAZIONE RUSSA',
  ukrainian: 'UCRAINA',
  ucraina: 'UCRAINA',
  ucraino: 'UCRAINA',
  ucraine: 'UCRAINA',
  ucraini: 'UCRAINA',
  chinese: 'CINA',
  cinese: 'CINA',
  cinesi: 'CINA',
};
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
let officialStateIndexPromise;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

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

  try {
    const parsedRequest = await parseIncomingRequest(event);
    const normalizedDocumentUploads = normalizeDocumentUploads(parsedRequest.documentUploads);
    const payload = normalizePayload(parsedRequest.body);
    payload.document_uploads = normalizedDocumentUploads;
    const validationErrors = validatePayload(payload);
    if (validationErrors.length) {
      return respond(400, { error: 'Validation failed', fields: validationErrors });
    }

    const documentErrors = validateUploadedDocuments(parsedRequest.files || []);
    if (documentErrors.length) {
      return respond(400, { error: 'Validation failed', fields: documentErrors });
    }
    if (payload.document_upload_issue) {
      const emergencyValidation = verifyPublicCheckinEmergencyToken(
        payload.document_emergency_token,
        payload,
        PUBLIC_CHECKIN_EMERGENCY_TOKEN_SECRET
      );
      if (!emergencyValidation.ok) {
        return respond(400, {
          error: 'Validation failed',
          fields: [{
            field: 'guest_documents',
            message: 'Percorso di emergenza scaduto o non valido. Riattiva WhatsApp e riprova il salvataggio.',
          }],
          detail: emergencyValidation.reason || 'invalid_emergency_token',
        });
      }
    }
    if (!parsedRequest.files.length && normalizedDocumentUploads.length) {
      const uploadBatchValidation = verifyPublicCheckinUploadBatchToken(
        payload.document_upload_batch_token,
        payload,
        normalizedDocumentUploads,
        PUBLIC_CHECKIN_UPLOAD_BATCH_SECRET
      );
      if (!uploadBatchValidation.ok) {
        return respond(400, {
          error: 'Validation failed',
          fields: [{
            field: 'guest_documents',
            message: 'Sessione upload documenti non valida o scaduta. Ricarica i documenti e riprova.',
          }],
          detail: uploadBatchValidation.reason || 'invalid_upload_batch',
        });
      }
    }
    if (!parsedRequest.files.length && !normalizedDocumentUploads.length && !payload.document_upload_issue) {
      return respond(400, {
        error: 'Validation failed',
        fields: [{ field: 'guest_documents', message: 'Carica i documenti di tutti gli ospiti oppure usa il percorso di emergenza WhatsApp.' }],
      });
    }

    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
    const { apartment, error: apartmentError } = await resolveApartmentReference(supabase, payload.apartment_ref);

    if (apartmentError) {
      console.error('submit-public-checkin apartment lookup error:', apartmentError);
      return respond(500, { error: 'Apartment lookup failed' });
    }

    if (!apartment) {
      return respond(400, {
        error: 'Validation failed',
        fields: [{ field: 'apartment_id', message: 'Link appartamento non valido o disattivato' }],
      });
    }

    payload.apartment_id = apartment.id;

    const touristTaxServerErrors = await validateTouristTaxPaymentServerSide(payload);
    if (touristTaxServerErrors.length) {
      return respond(400, { error: 'Validation failed', fields: touristTaxServerErrors });
    }

    const columnSupport = await detectOptionalColumns(supabase);
    if ((parsedRequest.files.length || normalizedDocumentUploads.length) && !columnSupport.documenti_caricati) {
      return respond(503, {
        error: 'Upload documenti temporaneamente non disponibile',
        fields: [{ field: 'files', message: 'Il supporto documenti non è ancora attivo sul database. Riprova tra poco.' }],
      });
    }

    if (!parsedRequest.files.length) {
      const storedUploadErrors = await validateStoredDocumentUploads(supabase, normalizedDocumentUploads);
      if (storedUploadErrors.length) {
        return respond(400, { error: 'Validation failed', fields: storedUploadErrors });
      }
    }

    const operationalErrors = await validateOperationalGuestCodes(supabase, payload);
    if (operationalErrors.length) {
      return respond(400, { error: 'Validation failed', fields: operationalErrors });
    }
    let storedDocuments = [];
    let insertedRecordId = '';

    try {
      storedDocuments = await persistUploadedDocuments(
        supabase,
        payload,
        parsedRequest.files,
        normalizedDocumentUploads
      );

      const insertPayload = await buildInsertPayload(supabase, payload, columnSupport, storedDocuments);
      const { data: inserted, error: insertError } = await insertCheckinRowsWithLegacyFallback(
        supabase,
        insertPayload,
        'id, portale_token, apartment_id, email, stato',
        { contextLabel: 'parent record', single: true }
      );

      if (insertError) {
        console.error('submit-public-checkin insert error:', insertError);
        throw new Error(`Insert failed: ${insertError.message}`);
      }

      if (!inserted) {
        throw new Error('Insert completed without returned row');
      }
      insertedRecordId = inserted.id;

      let childRecords = [];
      if (payload.additional_guests.length) {
        const childInsertPayloads = await buildAdditionalGuestInsertPayloads(
          supabase,
          payload,
          columnSupport,
          inserted.id
        );

        const { data: insertedChildren, error: childInsertError } = await insertCheckinRowsWithLegacyFallback(
          supabase,
          childInsertPayloads,
          'id, capogruppo_id, tipo_alloggiato, nome, cognome',
          { contextLabel: 'child records' }
        );

        if (childInsertError) {
          console.error('submit-public-checkin child insert error:', childInsertError);
          const childFailure = await diagnoseChildInsertFailure(supabase, childInsertPayloads);
          const { error: rollbackChildrenError } = await supabase
            .from('ospiti_check_in')
            .delete()
            .eq('capogruppo_id', inserted.id);
          const { error: rollbackParentError } = await supabase
            .from('ospiti_check_in')
            .delete()
            .eq('id', inserted.id);
          await cleanupStoredDocuments(supabase, storedDocuments);
          return respond(500, {
            error: 'Child insert failed',
            detail: childInsertError.message,
            supabase_error: {
              message: childInsertError.message || null,
              details: childInsertError.details || null,
              hint: childInsertError.hint || null,
              code: childInsertError.code || null,
            },
            failing_index: childFailure.index,
            failing_record: childFailure.record,
            failing_detail: childFailure.detail,
            child_summary: summarizeChildPayloads(childInsertPayloads),
            rollback_failed: !!(rollbackChildrenError || rollbackParentError),
            rollback_detail: [rollbackChildrenError?.message, rollbackParentError?.message].filter(Boolean).join(' | ') || null,
          });
        }

        childRecords = Array.isArray(insertedChildren) ? insertedChildren : [];
      }

      const portalUrl = buildPortalUrl(
        await resolvePortalBaseUrl(event),
        inserted.portale_token,
        apartment.public_checkin_key
      );

      if (payload.vuoi_fattura) {
        try {
          const draftResult = await ensureLocalInvoiceDraftForOspiteId(supabase, inserted.id, { columnSupport });
          if (!draftResult?.ok) {
            console.error('submit-public-checkin local draft sync error:', draftResult?.error || draftResult);
          }
        } catch (draftError) {
          console.error('submit-public-checkin local draft sync error:', draftError);
        }
      }

      return respond(200, {
        ok: true,
        record: inserted,
        child_records: childRecords,
        uploaded_documents: storedDocuments.length,
        portal_url: portalUrl,
      });
    } catch (error) {
      if (insertedRecordId) {
        await supabase.from('ospiti_check_in').delete().eq('capogruppo_id', insertedRecordId);
        await supabase.from('ospiti_check_in').delete().eq('id', insertedRecordId);
      }
      if (storedDocuments.length) {
        await cleanupStoredDocuments(supabase, storedDocuments);
      }
      console.error('submit-public-checkin error:', error);
      if (String(error.message || '').startsWith('Insert failed:')) {
        return respond(500, { error: 'Insert failed', detail: error.message.replace(/^Insert failed:\s*/, '') });
      }
      return respond(500, { error: 'Errore salvataggio check-in', detail: error.message });
    }
  } catch (error) {
    if (error?.statusCode) {
      return respond(error.statusCode, { error: error.publicMessage || error.message });
    }
    console.error('submit-public-checkin request parse error:', error);
    return respond(500, { error: 'Errore interno', detail: error.message });
  }
};

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}

async function parseIncomingRequest(event) {
  const contentType = String(event.headers['content-type'] || event.headers['Content-Type'] || '').toLowerCase();
  if (contentType.includes('multipart/form-data')) {
    const parsed = await parseMultipart(event);
    const body = parseMultipartPayload(parsed.fields || {});
    return {
      body,
      files: Array.isArray(parsed.files) ? parsed.files : [],
      documentUploads: parsed.fields?.document_uploads || parsed.fields?.documenti_caricati || '[]',
    };
  }

  let body;
  try {
    body = JSON.parse(event.body || '{}');
  } catch (_error) {
    throw createRequestError(400, 'Invalid JSON body');
  }

  return {
    body,
    files: [],
    documentUploads: body?.document_uploads || body?.documenti_caricati || [],
  };
}

function parseMultipart(event) {
  return new Promise((resolve, reject) => {
    const contentType = event.headers['content-type'] || event.headers['Content-Type'] || '';
    if (!String(contentType).toLowerCase().includes('multipart/form-data')) {
      reject(createRequestError(400, 'Content-Type multipart/form-data richiesto'));
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

function parseMultipartPayload(fields) {
  const rawPayload = fields.payload || fields.data || '{}';
  if (Array.isArray(rawPayload)) {
    throw createRequestError(400, 'Payload multipart non valido');
  }
  try {
    return JSON.parse(String(rawPayload || '{}'));
  } catch (_error) {
    throw createRequestError(400, 'Payload multipart non valido');
  }
}

function createRequestError(statusCode, message) {
  const error = new Error(message);
  error.statusCode = statusCode;
  error.publicMessage = message;
  return error;
}

function normalizePayload(body) {
  const rawTipoCliente = String(body.tipo_cliente || '').trim().toLowerCase();
  const normalizedTipoCliente = ['azienda', 'professionista', 'privato'].includes(rawTipoCliente)
    ? rawTipoCliente
    : 'privato';
  const hasBusinessInvoiceData = [
    body.ragione_sociale,
    body.piva_cliente,
    body.indirizzo_fatturazione,
  ].some((value) => String(value || '').trim());
  const wantsInvoice = body.vuoi_fattura === true
    || body.vuoi_fattura === 'true'
    || normalizedTipoCliente === 'azienda'
    || normalizedTipoCliente === 'professionista'
    || hasBusinessInvoiceData;
  const tipoCliente = normalizedTipoCliente === 'professionista'
    ? 'professionista'
    : hasBusinessInvoiceData || normalizedTipoCliente === 'azienda'
      ? 'azienda'
      : 'privato';
  const paese = String(body.paese_residenza || 'IT').trim().toUpperCase();

  return {
    apartment_ref: String(body.apartment_ref || body.apt || body.apartment_id || '').trim(),
    apartment_id: '',
    source_channel_hint: normalizeSourceChannelHint(body.source_channel_hint || body.channel || body.source || ''),
    tipo_cliente: tipoCliente,
    lingua: String(body.lingua || 'IT').trim().toUpperCase(),
    data_checkin: String(body.data_checkin || '').trim(),
    data_checkout: String(body.data_checkout || '').trim(),
    nome: String(body.nome || '').trim(),
    cognome: String(body.cognome || '').trim(),
    email: String(body.email || '').trim().toLowerCase(),
    telefono: String(body.telefono || '').trim(),
    data_nascita: String(body.data_nascita || '').trim(),
    luogo_nascita: String(body.luogo_nascita || '').trim(),
    luogo_nascita_codice: String(body.luogo_nascita_codice || '').trim(),
    stato_nascita: normalizeStateLikeValue(body.stato_nascita),
    nato_in_italia: body.nato_in_italia !== false && body.nato_in_italia !== '0' && body.nato_in_italia !== 0,
    sesso: normalizeSex(body.sesso),
    cittadinanza: normalizeCitizenshipValue(body.cittadinanza),
    tipo_documento: normalizeDocumentType(body.tipo_documento),
    numero_documento: String(body.numero_documento || '').trim(),
    luogo_rilascio_documento: String(body.luogo_rilascio_documento || '').trim(),
    luogo_rilascio_codice: String(body.luogo_rilascio_codice || '').trim(),
    tipo_alloggiato: normalizeTipoAlloggiato(body.tipo_alloggiato),
    numero_persone: normalizeNumeroPersone(body.numero_persone),
    additional_guests: normalizeAdditionalGuests(body.additional_guests),
    paese_residenza: paese,
    vuoi_fattura: wantsInvoice,
    ragione_sociale: String(body.ragione_sociale || '').trim(),
    indirizzo_fatturazione: String(body.indirizzo_fatturazione || '').trim(),
    codice_fiscale: String(body.codice_fiscale || '').trim().toUpperCase(),
    indirizzo_residenza: String(body.indirizzo_residenza || '').trim(),
    piva_cliente: String(body.piva_cliente || '').trim(),
    sdi: String(body.sdi || '').trim().toUpperCase(),
    pec: String(body.pec || '').trim().toLowerCase(),
    codice_fiscale_verificato: body.codice_fiscale_verificato !== false,
    document_upload_issue: body.document_upload_issue === true
      || body.document_upload_issue === 'true'
      || body.document_upload_issue === 1
      || body.document_upload_issue === '1',
    document_emergency_token: String(body.document_emergency_token || '').trim(),
    document_upload_batch_token: String(body.document_upload_batch_token || '').trim(),
    tourist_tax_payment: normalizeTouristTaxPayment(body.tourist_tax_payment),
  };
}

function normalizeSourceChannelHint(value) {
  const normalized = String(value || '').trim().toLowerCase();
  return normalized === BOOKING_CHANNEL_HINT ? BOOKING_CHANNEL_HINT : '';
}

function normalizeTouristTaxPayment(value) {
  const source = value && typeof value === 'object' && !Array.isArray(value) ? value : {};
  return {
    required: source.required === true || source.required === 'true' || normalizeSourceChannelHint(source.channel_hint) === BOOKING_CHANNEL_HINT,
    channel_hint: normalizeSourceChannelHint(source.channel_hint || source.channel),
    method: normalizeTouristTaxMethod(source.method),
    amount: normalizeNullableMoney(source.amount),
    currency: String(source.currency || DEFAULT_STRIPE_TOURIST_TAX_CURRENCY).trim().toUpperCase() || DEFAULT_STRIPE_TOURIST_TAX_CURRENCY,
    booking_amount_confirmed: source.booking_amount_confirmed === true
      || source.booking_amount_confirmed === 'true'
      || source.booking_amount_confirmed === 1
      || source.booking_amount_confirmed === '1',
    beneficiary: String(source.beneficiary || '').trim(),
    iban: normalizeIban(source.iban),
    bic: String(source.bic || '').trim().toUpperCase(),
    causal: String(source.causal || '').trim(),
    note: String(source.note || '').trim(),
    verification: normalizeTouristTaxVerification(source.verification),
  };
}

function normalizeTouristTaxVerification(value) {
  const source = value && typeof value === 'object' && !Array.isArray(value) ? value : {};
  return {
    matched: source.matched === true || source.matched === 'true',
    status: String(source.status || '').trim().toLowerCase(),
    message: String(source.message || '').trim(),
    expected_amount: normalizeNullableMoney(source.expected_amount),
    extracted_amount: normalizeNullableMoney(source.extracted_amount),
    difference: normalizeNullableMoney(source.difference),
    tolerance_eur: normalizeNullableMoney(source.tolerance_eur),
    checked_at: String(source.checked_at || '').trim(),
    provider: String(source.provider || '').trim().toLowerCase(),
    session_id: String(source.session_id || source.sessionId || '').trim(),
    payment_status: String(source.payment_status || '').trim().toLowerCase(),
    session_status: String(source.session_status || '').trim().toLowerCase(),
    amount_total: normalizeNullableMoney(source.amount_total),
    currency: String(source.currency || DEFAULT_STRIPE_TOURIST_TAX_CURRENCY).trim().toUpperCase() || DEFAULT_STRIPE_TOURIST_TAX_CURRENCY,
    livemode: source.livemode === true || source.livemode === 'true',
    customer_email: String(source.customer_email || '').trim().toLowerCase(),
  };
}

function normalizeTouristTaxMethod(value) {
  return String(value || '').trim().toLowerCase() === STRIPE_CHECKOUT_METHOD
    ? STRIPE_CHECKOUT_METHOD
    : BANK_TRANSFER_TOURIST_TAX_METHOD;
}

function normalizeNullableMoney(value) {
  if (value == null || value === '') return null;
  const normalized = Number(String(value).replace(',', '.').replace(/[^\d.-]/g, ''));
  if (!Number.isFinite(normalized)) return null;
  return Math.round(normalized * 100) / 100;
}

function normalizeIban(value) {
  return String(value || '').replace(/\s+/g, '').trim().toUpperCase();
}

function validatePayload(payload) {
  const errors = [];
  const isItalian = payload.paese_residenza === 'IT';
  const touristTaxRequired = payload.source_channel_hint === BOOKING_CHANNEL_HINT;

  if (!payload.apartment_ref) errors.push(errorField('apartment_id', 'Link appartamento mancante'));
  if (!payload.data_checkin) errors.push(errorField('checkin', 'Check-in obbligatorio'));
  if (!payload.data_checkout) errors.push(errorField('checkout', 'Check-out obbligatorio'));
  if (payload.data_checkin && payload.data_checkout && payload.data_checkout <= payload.data_checkin) {
    errors.push(errorField('checkout', 'Il check-out deve essere dopo il check-in'));
  }

  if (!isValidEmail(payload.email)) errors.push(errorField('email', 'Email non valida'));
  if (!isValidPhone(payload.telefono)) errors.push(errorField('telefono', 'Telefono non valido'));

  if (!payload.nome) errors.push(errorField('nome', 'Nome obbligatorio'));
  if (!payload.cognome) errors.push(errorField('cognome', 'Cognome obbligatorio'));
  if (!payload.data_nascita) errors.push(errorField('dataNascita', 'Data di nascita obbligatoria'));
  if (payload.nato_in_italia) {
    if (!payload.luogo_nascita) errors.push(errorField('luogoNascita', 'Comune di nascita obbligatorio'));
  } else if (!payload.stato_nascita) {
    errors.push(errorField('statoNascitaEstero', 'Stato di nascita obbligatorio'));
  }
  if (!['M', 'F'].includes(payload.sesso)) errors.push(errorField('sesso', 'Sesso obbligatorio'));
  if (!payload.cittadinanza) errors.push(errorField('cittadinanza', 'Cittadinanza obbligatoria'));
  if (!payload.tipo_documento) errors.push(errorField('tipoDocumento', 'Tipo documento obbligatorio'));
  if (!payload.numero_documento) errors.push(errorField('numeroDocumento', 'Numero documento obbligatorio'));
  if (!payload.luogo_rilascio_documento) errors.push(errorField('luogoRilascioDocumento', 'Luogo rilascio documento obbligatorio'));
  if (payload.numero_persone < 1) errors.push(errorField('numeroPersone', 'Numero persone non valido'));

  if (payload.numero_persone === 1) {
    payload.tipo_alloggiato = 16;
  } else {
    payload.tipo_alloggiato = [17, 18].includes(payload.tipo_alloggiato) ? payload.tipo_alloggiato : 18;
    const age = calculateAge(payload.data_nascita);
    if (age !== null && age < 18) {
      errors.push(errorField('dataNascita', payload.tipo_alloggiato === 17 ? 'Il capofamiglia deve essere maggiorenne' : 'Il capogruppo deve essere maggiorenne'));
    }
    if (payload.additional_guests.length !== payload.numero_persone - 1) {
      errors.push(errorField('numeroPersone', 'Dati ospiti aggiuntivi incompleti'));
    }
    payload.additional_guests.forEach((guest, index) => {
      const guestBornInItaly = isItalianBirthState(guest.stato_nascita);
      if (!guest.nome) errors.push(errorField(`additionalGuest_${index}_nome`, 'Nome obbligatorio'));
      if (!guest.cognome) errors.push(errorField(`additionalGuest_${index}_cognome`, 'Cognome obbligatorio'));
      if (!['M', 'F'].includes(guest.sesso)) errors.push(errorField(`additionalGuest_${index}_sesso`, 'Sesso obbligatorio'));
      if (!guest.data_nascita) errors.push(errorField(`additionalGuest_${index}_data_nascita`, 'Data di nascita obbligatoria'));
      if (guestBornInItaly) {
        if (!guest.luogo_nascita) errors.push(errorField(`additionalGuest_${index}_luogo_nascita`, 'Comune di nascita obbligatorio'));
        if (!guest.luogo_nascita_codice) errors.push(errorField(`additionalGuest_${index}_luogo_nascita_choice`, 'Comune di nascita obbligatorio'));
      } else if (!guest.stato_nascita) {
        errors.push(errorField(`additionalGuest_${index}_stato_nascita`, 'Stato di nascita obbligatorio'));
      }
      if (!guest.cittadinanza) errors.push(errorField(`additionalGuest_${index}_cittadinanza`, 'Cittadinanza obbligatoria'));
      guest.tipo_alloggiato = [19, 20].includes(guest.tipo_alloggiato)
        ? guest.tipo_alloggiato
        : (payload.tipo_alloggiato === 17 ? 19 : 20);
    });
  }

  if (isItalian) {
    if (!payload.codice_fiscale) errors.push(errorField('cf', 'Codice fiscale obbligatorio'));
    else if (!looksLikeItalianCF(payload.codice_fiscale)) errors.push(errorField('cf', 'Codice fiscale non valido'));

    if (!payload.indirizzo_residenza) errors.push(errorField('indirizzo', 'Indirizzo obbligatorio'));
    if (!extractItalianCap(payload.indirizzo_residenza)) errors.push(errorField('cap', 'CAP italiano non valido'));
  }

  if (payload.vuoi_fattura) {
    if (!payload.ragione_sociale) errors.push(errorField('ragioneSociale', 'Ragione sociale obbligatoria'));
    const vatCheck = classifyVatNumber(payload.piva_cliente);
    if (!vatCheck.valid) errors.push(errorField('piva', vatCheck.message));
    if (!payload.indirizzo_fatturazione) errors.push(errorField('indirizzoAz', 'Sede legale obbligatoria'));
    if (vatCheck.kind === 'it' && vatCheck.valid && !payload.sdi) {
      errors.push(errorField('sdi', 'Codice SDI obbligatorio per fattura aziendale italiana'));
    }
    if (payload.sdi && !isValidSdi(payload.sdi)) errors.push(errorField('sdi', 'Codice SDI non valido'));
    if (payload.pec && !isValidEmail(payload.pec)) errors.push(errorField('pec', 'PEC non valida'));
  }

  if (touristTaxRequired) {
    payload.tourist_tax_payment.required = true;
    payload.tourist_tax_payment.channel_hint = BOOKING_CHANNEL_HINT;
    if (!Number.isFinite(payload.tourist_tax_payment.amount) || payload.tourist_tax_payment.amount <= 0) {
      errors.push(errorField('tourist_tax_amount', 'Importo tassa di soggiorno obbligatorio'));
    }
    if (!payload.tourist_tax_payment.booking_amount_confirmed) {
      errors.push(errorField('tourist_tax_confirm', 'Conferma l importo della tassa di soggiorno indicato su Booking'));
    }
    if (payload.tourist_tax_payment.method === STRIPE_CHECKOUT_METHOD) {
      if (!payload.tourist_tax_payment.verification?.session_id) {
        errors.push(errorField('tourist_tax_stripe', 'Completa il pagamento Stripe prima di continuare'));
      }
    } else if (!hasTouristTaxProofUploads(payload.document_uploads)) {
      errors.push(errorField('tourist_tax_proof', 'Carica la ricevuta o lo screenshot del bonifico della tassa di soggiorno'));
    }
  }

  return errors;
}

function hasTouristTaxProofUploads(documentUploads) {
  return Array.isArray(documentUploads)
    && documentUploads.some((entry) => String(entry?.guest_scope || '').trim().toLowerCase() === TOURIST_TAX_DOCUMENT_SCOPE);
}

async function buildInsertPayload(supabase, payload, columnSupport, storedDocuments = []) {
  const isItalian = payload.paese_residenza === 'IT';
  const hasInvoice = payload.vuoi_fattura;
  const segnalazioneParts = [];
  if (hasInvoice) {
    if (payload.sdi) segnalazioneParts.push(`SDI:${payload.sdi}`);
    if (payload.pec) segnalazioneParts.push(`PEC:${payload.pec}`);
  } else if (isItalian && !payload.codice_fiscale_verificato) {
    segnalazioneParts.push('CF non verificato lato client - verificare');
  }
  if (payload.document_upload_issue) {
    segnalazioneParts.push(
      storedDocuments.length
        ? 'DOCUMENTI_PARZIALI_COMPLETARE_SU_WHATSAPP'
        : 'DOCUMENTI_VIA_WHATSAPP'
    );
  }
  const segnalazione = segnalazioneParts.join('|') || null;
  const resolvedCodes = await resolveGuestCodes(supabase, payload);

  const insertPayload = {
    apartment_id: payload.apartment_id,
    portale_token: crypto.randomUUID(),
    data_checkin: payload.data_checkin,
    data_checkout: payload.data_checkout,
    stato: 'CHECK_IN_COMPLETATO',
    lingua: payload.lingua,
    dati_completi: false,
    codice_fiscale_verificato: payload.codice_fiscale_verificato,
    data_nascita: payload.data_nascita || null,
    luogo_nascita: getStoredBirthText(payload),
    tipo_alloggiato: payload.tipo_alloggiato,
    nome: payload.nome,
    cognome: payload.cognome,
    email: payload.email,
    telefono: payload.telefono,
    paese_residenza: payload.paese_residenza,
    codice_fiscale: isItalian ? payload.codice_fiscale : null,
    indirizzo_residenza: payload.indirizzo_residenza || null,
    tipo_cliente: payload.tipo_cliente,
    iva_percentuale: hasInvoice ? 22 : 10,
    piva_cliente: hasInvoice ? payload.piva_cliente : null,
    segnalazione_jessica: segnalazione,
  };

  if (columnSupport.numero_persone) insertPayload.numero_persone = payload.numero_persone;
  if (columnSupport.additional_guests) insertPayload.additional_guests = payload.additional_guests;
  if (columnSupport.sesso) insertPayload.sesso = payload.sesso || null;
  if (columnSupport.cittadinanza) insertPayload.cittadinanza = payload.cittadinanza || null;
  if (columnSupport.cittadinanza_codice) insertPayload.cittadinanza_codice = resolvedCodes.cittadinanza_codice || null;
  if (columnSupport.tipo_documento) insertPayload.tipo_documento = payload.tipo_documento || null;
  if (columnSupport.tipo_documento_codice) insertPayload.tipo_documento_codice = resolvedCodes.tipo_documento_codice || null;
  if (columnSupport.numero_documento) insertPayload.numero_documento = payload.numero_documento || null;
  if (columnSupport.luogo_rilascio_documento) insertPayload.luogo_rilascio_documento = payload.luogo_rilascio_documento || null;
  if (columnSupport.luogo_rilascio_codice) insertPayload.luogo_rilascio_codice = resolvedCodes.luogo_rilascio_codice || null;
  if (columnSupport.luogo_nascita_codice) insertPayload.luogo_nascita_codice = resolvedCodes.luogo_nascita_codice || null;
  if (columnSupport.stato_nascita_codice) insertPayload.stato_nascita_codice = resolvedCodes.stato_nascita_codice || null;
  if (columnSupport.ragione_sociale) insertPayload.ragione_sociale = hasInvoice ? payload.ragione_sociale || null : null;
  if (columnSupport.indirizzo_fatturazione) insertPayload.indirizzo_fatturazione = hasInvoice ? payload.indirizzo_fatturazione || null : null;
  if (columnSupport.documenti_caricati) insertPayload.documenti_caricati = storedDocuments;
  if (columnSupport.tassa_soggiorno_pagamento) {
    insertPayload.tassa_soggiorno_pagamento = buildTouristTaxPaymentRecord(payload, storedDocuments);
  }
  insertPayload.dati_completi = computeGuestCompleteness(insertPayload);

  return insertPayload;
}

function buildTouristTaxPaymentRecord(payload, storedDocuments = []) {
  const touristTaxRequired = payload.source_channel_hint === BOOKING_CHANNEL_HINT;
  if (!touristTaxRequired && !payload.tourist_tax_payment?.required) {
    return {};
  }

  const proofDocuments = (Array.isArray(storedDocuments) ? storedDocuments : [])
    .filter((entry) => String(entry?.guest_scope || '').trim().toLowerCase() === TOURIST_TAX_DOCUMENT_SCOPE);
  const method = normalizeTouristTaxMethod(payload.tourist_tax_payment?.method);
  const verification = payload.tourist_tax_payment?.verification || null;

  return {
    required: true,
    status: verification?.matched
      ? (method === STRIPE_CHECKOUT_METHOD ? 'stripe_paid_verified' : 'proof_verified')
      : method === STRIPE_CHECKOUT_METHOD
        ? 'pending_stripe_payment'
        : proofDocuments.length
          ? 'proof_uploaded'
          : 'pending_proof',
    channel_hint: BOOKING_CHANNEL_HINT,
    method,
    amount: payload.tourist_tax_payment?.amount ?? null,
    currency: payload.tourist_tax_payment?.currency || DEFAULT_STRIPE_TOURIST_TAX_CURRENCY,
    booking_amount_confirmed: !!payload.tourist_tax_payment?.booking_amount_confirmed,
    beneficiary: payload.tourist_tax_payment?.beneficiary || null,
    iban: payload.tourist_tax_payment?.iban || null,
    bic: payload.tourist_tax_payment?.bic || null,
    causal: payload.tourist_tax_payment?.causal || null,
    note: payload.tourist_tax_payment?.note || null,
    verification,
    proof_documents: proofDocuments,
    submitted_at: new Date().toISOString(),
  };
}

async function validateTouristTaxPaymentServerSide(payload) {
  const touristTaxRequired = payload.source_channel_hint === BOOKING_CHANNEL_HINT;
  if (!touristTaxRequired) return [];
  if (normalizeTouristTaxMethod(payload.tourist_tax_payment?.method) !== STRIPE_CHECKOUT_METHOD) {
    return [];
  }

  const stripeSessionId = String(
    payload.tourist_tax_payment?.verification?.session_id
    || payload.tourist_tax_payment?.verification?.sessionId
    || ''
  ).trim();
  if (!stripeSessionId) {
    return [errorField('tourist_tax_stripe', 'Completa il pagamento Stripe prima di continuare')];
  }

  const secretKey = String(process.env.STRIPE_SECRET_KEY || '').trim();
  if (!secretKey) {
    return [errorField('tourist_tax_stripe', 'Pagamento Stripe non disponibile al momento. Usa il bonifico o contatta Serena su WhatsApp.')];
  }

  try {
    const session = await retrieveStripeCheckoutSession({
      secretKey,
      sessionId: stripeSessionId,
    });
    const verification = evaluateStripeCheckoutSession({
      session,
      expectedAmount: payload.tourist_tax_payment?.amount,
      expectedCurrency: payload.tourist_tax_payment?.currency || DEFAULT_STRIPE_TOURIST_TAX_CURRENCY,
      expectedApartmentRef: payload.apartment_ref,
      expectedChannelHint: payload.source_channel_hint,
    });

    if (!verification.matched) {
      return [errorField('tourist_tax_stripe', verification.message || 'Completa il pagamento Stripe prima di continuare')];
    }

    payload.tourist_tax_payment.method = STRIPE_CHECKOUT_METHOD;
    payload.tourist_tax_payment.amount = verification.amount ?? payload.tourist_tax_payment.amount;
    payload.tourist_tax_payment.currency = verification.currency || payload.tourist_tax_payment.currency || DEFAULT_STRIPE_TOURIST_TAX_CURRENCY;
    payload.tourist_tax_payment.verification = {
      matched: true,
      status: verification.status,
      message: verification.message,
      expected_amount: verification.expected_amount,
      extracted_amount: verification.amount,
      difference: verification.difference,
      tolerance_eur: 0,
      checked_at: verification.checked_at,
      provider: 'stripe',
      session_id: verification.session_id,
      payment_status: verification.payment_status,
      session_status: verification.session_status,
      amount_total: verification.amount_total,
      currency: verification.currency,
      livemode: verification.livemode,
      customer_email: verification.customer_email,
    };
    return [];
  } catch (error) {
    console.error('submit-public-checkin stripe verification error:', error);
    return [errorField('tourist_tax_stripe', 'Non riesco a verificare il pagamento Stripe. Usa il bonifico o contatta Serena su WhatsApp.')];
  }
}

async function buildAdditionalGuestInsertPayloads(supabase, payload, columnSupport, capogruppoId) {
  const records = [];

  for (const guest of payload.additional_guests) {
    const childTipoAlloggiato = [19, 20].includes(Number(guest.tipo_alloggiato))
      ? Number(guest.tipo_alloggiato)
      : (payload.tipo_alloggiato === 17 ? 19 : 20);
    const natoInItalia = isItalianBirthState(guest.stato_nascita);
    const guestPayload = {
      apartment_id: payload.apartment_id,
      tipo_cliente: 'privato',
      lingua: payload.lingua,
      data_checkin: payload.data_checkin,
      data_checkout: payload.data_checkout,
      nome: guest.nome,
      cognome: guest.cognome,
      email: '',
      telefono: '',
      data_nascita: guest.data_nascita,
      luogo_nascita: guest.luogo_nascita,
      luogo_nascita_codice: guest.luogo_nascita_codice || '',
      stato_nascita: natoInItalia ? '' : guest.stato_nascita,
      nato_in_italia: natoInItalia,
      sesso: guest.sesso,
      cittadinanza: guest.cittadinanza,
      tipo_documento: guest.tipo_documento || '',
      numero_documento: guest.numero_documento || '',
      luogo_rilascio_documento: guest.luogo_rilascio_documento || '',
      luogo_rilascio_codice: '',
      tipo_alloggiato: childTipoAlloggiato,
      numero_persone: 1,
      additional_guests: [],
      paese_residenza: '',
      codice_fiscale: '',
      indirizzo_residenza: '',
      piva_cliente: '',
      sdi: '',
      pec: '',
      codice_fiscale_verificato: false,
    };

    const resolvedCodes = await resolveGuestCodes(supabase, guestPayload);
    const childRecord = {
      apartment_id: payload.apartment_id,
      portale_token: crypto.randomUUID(),
      data_checkin: payload.data_checkin,
      data_checkout: payload.data_checkout,
      stato: 'CHECK_IN_COMPLETATO',
      lingua: payload.lingua,
      dati_completi: false,
      codice_fiscale_verificato: false,
      data_nascita: guest.data_nascita || null,
      luogo_nascita: natoInItalia ? guest.luogo_nascita || null : guest.stato_nascita || null,
      tipo_alloggiato: childTipoAlloggiato,
      nome: guest.nome,
      cognome: guest.cognome,
      email: '',
      telefono: '',
      paese_residenza: payload.paese_residenza === 'IT' ? '' : (payload.paese_residenza || ''),
      codice_fiscale: null,
      indirizzo_residenza: null,
      tipo_cliente: 'privato',
      iva_percentuale: 10,
      piva_cliente: null,
      segnalazione_jessica: null,
    };

    if (columnSupport.numero_persone) childRecord.numero_persone = 1;
    if (columnSupport.additional_guests) childRecord.additional_guests = [];
    if (columnSupport.sesso) childRecord.sesso = guest.sesso || null;
    if (columnSupport.cittadinanza) childRecord.cittadinanza = guest.cittadinanza || null;
    if (columnSupport.cittadinanza_codice) childRecord.cittadinanza_codice = resolvedCodes.cittadinanza_codice || null;
    if (columnSupport.tipo_documento) childRecord.tipo_documento = guest.tipo_documento || null;
    if (columnSupport.tipo_documento_codice) childRecord.tipo_documento_codice = resolvedCodes.tipo_documento_codice || null;
    if (columnSupport.numero_documento) childRecord.numero_documento = guest.numero_documento || null;
    if (columnSupport.luogo_rilascio_documento) childRecord.luogo_rilascio_documento = guest.luogo_rilascio_documento || null;
    if (columnSupport.luogo_rilascio_codice) childRecord.luogo_rilascio_codice = resolvedCodes.luogo_rilascio_codice || null;
    if (columnSupport.luogo_nascita_codice) childRecord.luogo_nascita_codice = resolvedCodes.luogo_nascita_codice || null;
    if (columnSupport.stato_nascita_codice) childRecord.stato_nascita_codice = resolvedCodes.stato_nascita_codice || null;
    if (columnSupport.capogruppo_id) childRecord.capogruppo_id = capogruppoId;
    if (columnSupport.tag_prenotazione) childRecord.tag_prenotazione = null;
    childRecord.dati_completi = computeGuestCompleteness(childRecord);

    records.push(childRecord);
  }

  return records;
}

async function resolvePortalBaseUrl(event) {
  if (PUBLIC_PORTAL_BASE_URL) return PUBLIC_PORTAL_BASE_URL;
  if (event.headers.origin) return event.headers.origin;
  if (event.headers.host) return `https://${event.headers.host}`;
  return getConfiguredPortalBaseUrl();
}

function buildPortalUrl(baseUrl, token, publicCheckinKey) {
  const url = new URL('/portale.html', `${baseUrl.replace(/\/+$/, '')}/`);
  if (token) url.searchParams.set('token', token);
  if (publicCheckinKey) url.searchParams.set('apt', publicCheckinKey);
  return url.toString();
}

function errorField(field, message) {
  return { field, message };
}

function resolveSupabaseUrl(rawValue) {
  const fallback = DEFAULT_SUPABASE_URL;
  const candidate = String(rawValue || '').trim();
  if (!candidate) return fallback;
  try {
    const url = new URL(candidate);
    if (!/^https?:$/i.test(url.protocol)) return fallback;
    return url.toString().replace(/\/+$/, '');
  } catch (error) {
    console.warn('submit-public-checkin invalid SUPABASE_URL, using fallback URL');
    return fallback;
  }
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

async function validateOperationalGuestCodes(supabase, payload) {
  const errors = [];
  const mainCodes = await resolveGuestCodes(supabase, payload);

  if (!mainCodes.cittadinanza_codice) {
    errors.push(errorField('cittadinanza', 'Cittadinanza non codificabile in modo affidabile'));
  }
  if (!mainCodes.stato_nascita_codice) {
    errors.push(errorField(payload.nato_in_italia ? 'luogoNascita' : 'statoNascitaEstero', 'Luogo di nascita non codificabile in modo affidabile'));
  }
  if (payload.nato_in_italia && !mainCodes.luogo_nascita_codice) {
    errors.push(errorField('luogoNascita', 'Comune di nascita non risolvibile in modo univoco'));
  }
  if ([16, 17, 18].includes(payload.tipo_alloggiato) && !mainCodes.luogo_rilascio_codice) {
    errors.push(errorField('luogoRilascioDocumento', 'Luogo rilascio documento non codificabile. Se il documento e italiano aggiungi comune o provincia; se e estero inserisci lo stato o paese di rilascio.'));
  }
  if ([16, 17, 18].includes(payload.tipo_alloggiato) && !mainCodes.tipo_documento_codice) {
    errors.push(errorField('tipoDocumento', 'Tipo documento non codificabile in modo affidabile'));
  }

  for (let index = 0; index < payload.additional_guests.length; index += 1) {
    const guest = payload.additional_guests[index];
    const childTipoAlloggiato = [19, 20].includes(Number(guest.tipo_alloggiato))
      ? Number(guest.tipo_alloggiato)
      : (payload.tipo_alloggiato === 17 ? 19 : 20);
    const natoInItalia = isItalianBirthState(guest.stato_nascita);
    const guestPayload = {
      apartment_id: payload.apartment_id,
      tipo_cliente: 'privato',
      lingua: payload.lingua,
      data_checkin: payload.data_checkin,
      data_checkout: payload.data_checkout,
      nome: guest.nome,
      cognome: guest.cognome,
      email: '',
      telefono: '',
      data_nascita: guest.data_nascita,
      luogo_nascita: guest.luogo_nascita,
      luogo_nascita_codice: guest.luogo_nascita_codice || '',
      stato_nascita: natoInItalia ? '' : guest.stato_nascita,
      nato_in_italia: natoInItalia,
      sesso: guest.sesso,
      cittadinanza: guest.cittadinanza,
      tipo_documento: '',
      numero_documento: '',
      luogo_rilascio_documento: '',
      luogo_rilascio_codice: '',
      tipo_alloggiato: childTipoAlloggiato,
      numero_persone: 1,
      additional_guests: [],
      paese_residenza: '',
      codice_fiscale: '',
      indirizzo_residenza: '',
      piva_cliente: '',
      sdi: '',
      pec: '',
      codice_fiscale_verificato: true,
    };
    const guestCodes = await resolveGuestCodes(supabase, guestPayload);

    if (!guestCodes.cittadinanza_codice) {
      errors.push(errorField(`additionalGuest_${index}_cittadinanza`, 'Cittadinanza non codificabile in modo affidabile'));
    }
    if (!guestCodes.stato_nascita_codice) {
      errors.push(errorField(`additionalGuest_${index}_stato_nascita`, 'Stato di nascita non codificabile in modo affidabile'));
    }
    if (natoInItalia && !guestCodes.luogo_nascita_codice) {
      errors.push(errorField(`additionalGuest_${index}_luogo_nascita`, 'Comune di nascita non risolvibile in modo univoco'));
    }
  }

  return errors;
}

function isValidEmail(value) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(value || '').trim());
}

function isValidPhone(value) {
  return String(value || '').replace(/\D/g, '').length >= 8;
}

function isValidVatNumber(value) {
  return classifyVatNumber(value).valid;
}

function classifyVatNumber(value) {
  const raw = String(value || '').trim().toUpperCase().replace(/\s+/g, '');
  if (!raw) return { kind: 'empty', valid: false, message: 'Partita IVA obbligatoria' };

  const italian = raw.startsWith('IT') ? raw.slice(2) : raw;
  if (/^\d{11}$/.test(italian)) {
    return validateItalianVatChecksum(italian)
      ? { kind: 'it', valid: true, message: null }
      : { kind: 'it', valid: false, message: 'Partita IVA italiana non valida' };
  }

  if (/^[A-Z]{2}[A-Z0-9]{2,14}$/.test(raw)) {
    return { kind: 'eu', valid: true, message: null };
  }

  if (/^[A-Z0-9][A-Z0-9./ -]{2,20}$/.test(String(value || '').trim().toUpperCase())) {
    return { kind: 'extra', valid: true, message: null };
  }

  return { kind: 'invalid', valid: false, message: 'Partita IVA non valida' };
}

function validateItalianVatChecksum(value) {
  if (!/^\d{11}$/.test(value)) return false;
  let sum = 0;
  for (let index = 0; index < 10; index += 1) {
    const digit = Number(value[index]);
    if (index % 2 === 0) sum += digit;
    else {
      const doubled = digit * 2;
      sum += doubled > 9 ? doubled - 9 : doubled;
    }
  }
  return ((10 - (sum % 10)) % 10) === Number(value[10]);
}

function isValidSdi(value) {
  return /^[A-Z0-9]{7}$/.test(String(value || '').trim().toUpperCase());
}

function looksLikeItalianCF(value) {
  return /^[A-Z]{6}[0-9LMNPQRSTUV]{2}[ABCDEHLMPRST][0-9LMNPQRSTUV]{2}[A-Z][0-9LMNPQRSTUV]{3}[A-Z]$/.test(String(value || '').trim().toUpperCase());
}

function extractItalianCap(value) {
  const match = String(value || '').match(/\b\d{5}\b/);
  return match ? match[0] : '';
}

async function detectOptionalColumns(supabase) {
  const numero_persone = await hasColumn(supabase, 'numero_persone');
  const additional_guests = await hasColumn(supabase, 'additional_guests');
  const sesso = await hasColumn(supabase, 'sesso');
  const cittadinanza = await hasColumn(supabase, 'cittadinanza');
  const cittadinanza_codice = await hasColumn(supabase, 'cittadinanza_codice');
  const tipo_documento = await hasColumn(supabase, 'tipo_documento');
  const tipo_documento_codice = await hasColumn(supabase, 'tipo_documento_codice');
  const numero_documento = await hasColumn(supabase, 'numero_documento');
  const luogo_rilascio_documento = await hasColumn(supabase, 'luogo_rilascio_documento');
  const luogo_rilascio_codice = await hasColumn(supabase, 'luogo_rilascio_codice');
  const stato_nascita_codice = await hasColumn(supabase, 'stato_nascita_codice');
  const luogo_nascita_codice = await hasColumn(supabase, 'luogo_nascita_codice');
  const capogruppo_id = await hasColumn(supabase, 'capogruppo_id');
  const tag_prenotazione = await hasColumn(supabase, 'tag_prenotazione');
  const ragione_sociale = await hasColumn(supabase, 'ragione_sociale');
  const indirizzo_fatturazione = await hasColumn(supabase, 'indirizzo_fatturazione');
  const documenti_caricati = await hasColumn(supabase, 'documenti_caricati');
  const tassa_soggiorno_pagamento = await hasColumn(supabase, 'tassa_soggiorno_pagamento');
  return {
    numero_persone,
    additional_guests,
    sesso,
    cittadinanza,
    cittadinanza_codice,
    tipo_documento,
    tipo_documento_codice,
    numero_documento,
    luogo_rilascio_documento,
    luogo_rilascio_codice,
    stato_nascita_codice,
    luogo_nascita_codice,
    capogruppo_id,
    tag_prenotazione,
    ragione_sociale,
    indirizzo_fatturazione,
    documenti_caricati,
    tassa_soggiorno_pagamento,
  };
}

async function hasColumn(supabase, column) {
  const { error } = await supabase.from('ospiti_check_in').select(column).limit(1);
  return !error;
}

async function insertCheckinRowsWithLegacyFallback(supabase, payload, selectColumns, options = {}) {
  const single = !!options.single;
  const contextLabel = options.contextLabel || 'ospiti_check_in insert';
  const attempts = buildLegacyCompatibleInsertAttempts(payload);
  let lastError = null;

  for (let index = 0; index < attempts.length; index += 1) {
    const candidate = attempts[index];
    let query = supabase
      .from('ospiti_check_in')
      .insert(candidate)
      .select(selectColumns);
    if (single) query = query.single();

    const { data, error } = await query;
    if (!error) {
      if (index > 0) {
        console.warn(`submit-public-checkin ${contextLabel}: legacy fallback used`);
      }
      return { data, error: null };
    }

    lastError = error;
    const canRetryWithLegacyFallback = index === 0
      && attempts.length > 1
      && isLegacyAdditionalGuestsTypeMismatch(error);
    if (!canRetryWithLegacyFallback) break;

    console.warn(`submit-public-checkin ${contextLabel}: retrying without additional_guests`, {
      message: error.message || null,
      details: error.details || null,
      hint: error.hint || null,
      code: error.code || null,
    });
  }

  return { data: null, error: lastError };
}

function buildLegacyCompatibleInsertAttempts(payload) {
  const attempts = [payload];
  const sanitized = stripLegacyOptionalFields(payload);
  if (sanitized !== payload) attempts.push(sanitized);
  return attempts;
}

function stripLegacyOptionalFields(payload) {
  if (Array.isArray(payload)) {
    let changed = false;
    const sanitized = payload.map((item) => {
      if (!item || typeof item !== 'object') {
        return item;
      }
      const next = stripLegacyOptionalFields(item);
      if (next !== item) changed = true;
      return next;
    });
    return changed ? sanitized : payload;
  }

  if (!payload || typeof payload !== 'object') {
    return payload;
  }

  let changed = false;
  const sanitized = { ...payload };
  if (Object.prototype.hasOwnProperty.call(sanitized, 'additional_guests')) {
    delete sanitized.additional_guests;
    changed = true;
  }
  if (Array.isArray(sanitized.documenti_caricati) && sanitized.documenti_caricati.length === 0) {
    delete sanitized.documenti_caricati;
    changed = true;
  }
  return changed ? sanitized : payload;
}

function isLegacyAdditionalGuestsTypeMismatch(error) {
  const raw = [
    error?.message || '',
    error?.details || '',
    error?.hint || '',
  ].join(' ').toLowerCase();
  return raw.includes('invalid input syntax for type boolean') && raw.includes('[]');
}

function validateUploadedDocuments(files) {
  if (!Array.isArray(files) || !files.length) return [];
  const errors = [];

  files.forEach((file, index) => {
    const safeIndex = index + 1;
    const filename = String(file?.filename || '').trim();
    const sizeBytes = Number(file?.size_bytes || file?.buffer?.length || 0);
    if (!filename || !file?.buffer?.length) {
      errors.push(errorField('files', `Documento ${safeIndex} non valido`));
      return;
    }
    if (!isAllowedUploadFile(file)) {
      errors.push(errorField('files', `${filename}: formato non supportato. Usa PDF, JPG, PNG, WEBP, GIF o HEIC.`));
    }
    if (sizeBytes <= 0) {
      errors.push(errorField('files', `${filename}: file vuoto`));
    } else if (sizeBytes > MAX_CHECKIN_DOCUMENT_SIZE_BYTES) {
      errors.push(errorField('files', `${filename}: supera il limite di 20 MB`));
    }
  });

  return errors;
}

function normalizeDocumentUploads(value) {
  const raw = Array.isArray(value) ? value : (() => {
    if (!value) return [];
    if (typeof value === 'string') {
      try {
        const parsed = JSON.parse(value);
        return Array.isArray(parsed) ? parsed : [];
      } catch (_error) {
        return [];
      }
    }
    return [];
  })();

  return raw.map((entry, index) => ({
    client_id: String(entry?.client_id || '').trim() || `doc-${index + 1}`,
    file_field: String(entry?.file_field || '').trim(),
    guest_scope: normalizeGuestDocumentScope(entry?.guest_scope),
    guest_index: normalizeGuestDocumentIndex(entry?.guest_index),
    display_order: normalizeGuestDocumentIndex(entry?.display_order),
    file_name: String(entry?.file_name || '').trim(),
    mime_type: String(entry?.mime_type || '').trim().toLowerCase(),
    size_bytes: normalizePositiveInt(entry?.size_bytes),
    storage_bucket: String(entry?.storage_bucket || '').trim(),
    storage_path: String(entry?.storage_path || '').trim(),
  }));
}

function normalizeGuestDocumentScope(value) {
  const normalized = String(value || '').trim().toLowerCase();
  if (normalized === 'additional') return 'additional';
  if (normalized === TOURIST_TAX_DOCUMENT_SCOPE) return TOURIST_TAX_DOCUMENT_SCOPE;
  return 'main';
}

async function persistUploadedDocuments(supabase, payload, files, documentUploads) {
  if (!Array.isArray(files) || !files.length) {
    return buildStoredDocumentsFromUploads(documentUploads);
  }

  await ensureCheckinDocumentsBucket(supabase);

  const datePrefix = new Date().toISOString().slice(0, 10);
  const requestKey = sanitizeStorageKeyPart([
    payload.apartment_ref || payload.apartment_id || 'apartment',
    payload.cognome || payload.nome || 'guest',
    Date.now(),
  ].filter(Boolean).join('-'));
  const metadataByField = new Map(
    documentUploads
      .filter((entry) => entry.file_field)
      .map((entry) => [entry.file_field, entry])
  );
  const storedDocuments = [];

  for (let index = 0; index < files.length; index += 1) {
    const file = files[index];
    const metadata = metadataByField.get(file.field_name) || null;
    const safeName = sanitizeFilename(file.filename || `documento-${index + 1}`);
    const storagePath = `public-checkin/${datePrefix}/${requestKey}/${String(index + 1).padStart(2, '0')}_${safeName}`;
    const mimeType = normalizeUploadMimeType(file);
    const sizeBytes = Number(file.size_bytes || file.buffer.length || 0);
    const { error } = await supabase.storage.from(CHECKIN_DOCUMENTS_BUCKET).upload(storagePath, file.buffer, {
      contentType: mimeType,
      upsert: true,
    });

    if (error) throw error;

    storedDocuments.push({
      client_id: metadata?.client_id || `doc-${index + 1}`,
      guest_scope: metadata?.guest_scope || 'main',
      guest_index: metadata?.guest_index ?? 0,
      display_order: metadata?.display_order ?? index,
      file_name: file.filename || safeName,
      mime_type: mimeType,
      size_bytes: sizeBytes,
      storage_bucket: CHECKIN_DOCUMENTS_BUCKET,
      storage_path: storagePath,
      uploaded_at: new Date().toISOString(),
    });
  }

  return storedDocuments;
}

function buildStoredDocumentsFromUploads(documentUploads) {
  return (Array.isArray(documentUploads) ? documentUploads : [])
    .filter((entry) => entry && entry.storage_bucket && entry.storage_path)
    .map((entry) => ({
      client_id: entry.client_id || '',
      guest_scope: entry.guest_scope || 'main',
      guest_index: entry.guest_index ?? 0,
      display_order: entry.display_order ?? 0,
      file_name: entry.file_name || '',
      mime_type: entry.mime_type || '',
      size_bytes: Number(entry.size_bytes || 0),
      storage_bucket: entry.storage_bucket || CHECKIN_DOCUMENTS_BUCKET,
      storage_path: entry.storage_path || '',
      uploaded_at: new Date().toISOString(),
    }));
}

async function validateStoredDocumentUploads(supabase, documentUploads) {
  if (!Array.isArray(documentUploads) || !documentUploads.length) return [];
  const errors = [];
  for (const entry of documentUploads) {
    const fileName = String(entry?.file_name || 'Documento').trim();
    const fieldName = String(entry?.guest_scope || '').trim().toLowerCase() === TOURIST_TAX_DOCUMENT_SCOPE
      ? 'tourist_tax_proof'
      : 'guest_documents';
    const storageBucket = String(entry?.storage_bucket || '').trim();
    const storagePath = String(entry?.storage_path || '').trim();
    if (storageBucket !== CHECKIN_DOCUMENTS_BUCKET) {
      errors.push(errorField(fieldName, `${fileName}: bucket documento non valido.`));
      continue;
    }
    if (!storagePath.startsWith('public-checkin/')) {
      errors.push(errorField(fieldName, `${fileName}: percorso documento non valido.`));
      continue;
    }
    const { data, error } = await supabase.storage.from(storageBucket).exists(storagePath);
    if (error || !data) {
      errors.push(errorField(fieldName, `${fileName}: upload documento mancante o incompleto. Ricarica il file e riprova.`));
    }
  }
  return errors;
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

async function cleanupStoredDocuments(supabase, storedDocuments) {
  if (!Array.isArray(storedDocuments) || !storedDocuments.length) return;
  const paths = storedDocuments
    .map((entry) => String(entry?.storage_path || '').trim())
    .filter(Boolean);
  if (!paths.length) return;
  const { error } = await supabase.storage.from(CHECKIN_DOCUMENTS_BUCKET).remove(paths);
  if (error) {
    console.warn('submit-public-checkin document cleanup failed:', error.message);
  }
}

function normalizeUploadMimeType(file) {
  const mimeType = String(file?.mime_type || '').trim().toLowerCase();
  if (CHECKIN_ALLOWED_UPLOAD_MIME_TYPES.has(mimeType)) {
    return mimeType === 'image/jpg' ? 'image/jpeg' : mimeType;
  }

  const ext = path.extname(String(file?.filename || '').trim()).toLowerCase();
  if (ext === '.pdf') return 'application/pdf';
  if (ext === '.png') return 'image/png';
  if (ext === '.webp') return 'image/webp';
  if (ext === '.gif') return 'image/gif';
  if (ext === '.heic') return 'image/heic';
  if (ext === '.heif') return 'image/heif';
  return 'image/jpeg';
}

function isAllowedUploadFile(file) {
  const mimeType = String(file?.mime_type || '').trim().toLowerCase();
  if (CHECKIN_ALLOWED_UPLOAD_MIME_TYPES.has(mimeType)) return true;
  const ext = path.extname(String(file?.filename || '').trim()).toLowerCase();
  return CHECKIN_ALLOWED_UPLOAD_EXTENSIONS.has(ext);
}

function normalizeGuestDocumentIndex(value) {
  const parsed = parseInt(value, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : 0;
}

function normalizePositiveInt(value) {
  const parsed = parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
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

async function diagnoseChildInsertFailure(supabase, records) {
  const insertedIds = [];
  for (let index = 0; index < records.length; index += 1) {
    const record = records[index];
    const { data, error } = await insertCheckinRowsWithLegacyFallback(
      supabase,
      record,
      'id',
      { contextLabel: `child diagnostic ${index}`, single: true }
    );
    if (error) {
      console.error('submit-public-checkin child insert diagnostic error:', {
        index,
        message: error.message,
        details: error.details,
        hint: error.hint,
        code: error.code,
        record,
      });
      if (insertedIds.length) {
        await supabase.from('ospiti_check_in').delete().in('id', insertedIds);
      }
      return {
        index,
        record,
        detail: {
          message: error.message || null,
          details: error.details || null,
          hint: error.hint || null,
          code: error.code || null,
        },
      };
    }
    if (data?.id) insertedIds.push(data.id);
  }
  if (insertedIds.length) {
    await supabase.from('ospiti_check_in').delete().in('id', insertedIds);
  }
  return { index: null, record: null, detail: null };
}

function summarizeChildPayloads(records) {
  const list = Array.isArray(records) ? records : [];
  return {
    expected_children: list.length,
    tipo_alloggiato: list.map((row) => row?.tipo_alloggiato ?? null),
    null_paese_residenza_indexes: list
      .map((row, index) => (row?.paese_residenza == null ? index : null))
      .filter((value) => value !== null),
    null_capogruppo_indexes: list
      .map((row, index) => (row?.capogruppo_id == null ? index : null))
      .filter((value) => value !== null),
  };
}

function normalizeTipoAlloggiato(value) {
  const parsed = parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : 16;
}

function normalizeNumeroPersone(value) {
  const parsed = parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 1;
}

function normalizeAdditionalGuests(value) {
  if (!Array.isArray(value)) return [];
  return value.map((guest) => ({
    nome: String(guest?.nome || '').trim(),
    cognome: String(guest?.cognome || '').trim(),
    sesso: String(guest?.sesso || '').trim().toUpperCase(),
    data_nascita: String(guest?.data_nascita || '').trim(),
    luogo_nascita: String(guest?.luogo_nascita || '').trim(),
    luogo_nascita_codice: String(guest?.luogo_nascita_codice || '').trim(),
    stato_nascita: normalizeStateLikeValue(guest?.stato_nascita),
    cittadinanza: normalizeCitizenshipValue(guest?.cittadinanza),
    tipo_documento: normalizeDocumentType(guest?.tipo_documento),
    numero_documento: String(guest?.numero_documento || '').trim(),
    luogo_rilascio_documento: String(guest?.luogo_rilascio_documento || '').trim(),
    tipo_alloggiato: normalizeTipoAlloggiato(guest?.tipo_alloggiato),
  }));
}

function isItalianBirthState(value) {
  const normalized = normalizeLookupValue(value);
  return !normalized || normalized === 'italia' || normalized === 'italy' || normalized === 'it';
}

function normalizeSex(value) {
  const normalized = String(value || '').trim().toUpperCase();
  return normalized === 'F' ? 'F' : normalized === 'M' ? 'M' : '';
}

function normalizeDocumentType(value) {
  const raw = String(value || '').trim();
  const normalized = raw.toLowerCase().replace(/[’]/g, "'");
  if (normalized === "carta d'identita" || normalized === 'carta di identita') return "Carta d'identita";
  if (normalized === 'passaporto') return 'Passaporto';
  if (normalized === 'patente') return 'Patente';
  return raw;
}

function normalizeStateLikeValue(value) {
  const raw = String(value || '').trim();
  const normalized = normalizeLookupValue(raw);
  if (!normalized) return '';
  if (['italia','italy','it','italiana','italiano','cittadinanza italiana','nazionalita italiana','nazionalita: italia'].includes(normalized)) return 'Italia';
  return raw;
}

function normalizeCitizenshipValue(value) {
  return normalizeStateLikeValue(value);
}

async function resolveGuestCodes(supabase, payload) {
  const cittadinanzaMatch = await findUniqueStateCode(supabase, payload.cittadinanza);
  const birthMatch = payload.nato_in_italia
    ? await findUniqueComuneCode(supabase, payload.luogo_nascita)
    : await findUniqueStateCode(supabase, payload.stato_nascita);
  const rilascioMatch = await findIssuePlaceMatch(supabase, payload);
  const documentMatch = await findDocumentCode(supabase, payload.tipo_documento);
  const birthCode = payload.nato_in_italia
    ? (payload.luogo_nascita_codice || birthMatch.code)
    : '';
  const rilascioCode = payload.luogo_rilascio_codice || rilascioMatch.code;

  logLookupResult('cittadinanza', payload.cittadinanza, cittadinanzaMatch);
  logLookupResult(payload.nato_in_italia ? 'luogo_nascita' : 'stato_nascita', payload.nato_in_italia ? payload.luogo_nascita : payload.stato_nascita, birthMatch);
  logLookupResult('luogo_rilascio_documento', payload.luogo_rilascio_documento, rilascioMatch);
  logLookupResult('tipo_documento', payload.tipo_documento, documentMatch);

  return {
    cittadinanza_codice: cittadinanzaMatch.code,
    tipo_documento_codice: documentMatch.code,
    luogo_rilascio_codice: rilascioCode,
    luogo_nascita_codice: birthCode,
    stato_nascita_codice: payload.nato_in_italia ? ITALIA_CODE : birthMatch.code,
  };
}

async function findIssuePlaceMatch(supabase, payload) {
  const normalizedIssuePlace = normalizeIssuePlaceValue(payload?.luogo_rilascio_documento);
  const comuneMatch = await findUniqueComuneCode(supabase, normalizedIssuePlace, {
    fallbackProvince: extractProvinceCodeFromAddress(payload?.indirizzo_residenza),
  });
  if (comuneMatch.status === 'matched') return comuneMatch;

  const stateMatch = await findUniqueStateCode(supabase, normalizedIssuePlace);
  if (stateMatch.status === 'matched') return stateMatch;

  return comuneMatch.status !== 'not_found' ? comuneMatch : stateMatch;
}

async function findUniqueStateCode(supabase, value) {
  const normalized = String(value || '').trim();
  if (!normalized) return emptyLookup();
  const normalizedKey = normalizeLookupValue(normalized);
  if (normalizedKey === 'italia' || normalized.toUpperCase() === 'IT') return { code: ITALIA_CODE, status: 'matched', matches: 1 };

  const officialState = await findOfficialStateByName(normalizedKey);
  if (officialState) {
    return { code: officialState.code, status: 'matched', matches: 1 };
  }

  let rows = await fetchStateMatches(supabase, 'nome_it', normalized);
  if (!rows.length) rows = await fetchStateMatches(supabase, 'nome', normalized);
  if (!rows.length) rows = await fetchStateLikeMatches(supabase, 'nome_it', normalized);
  if (!rows.length) rows = await fetchStateLikeMatches(supabase, 'nome', normalized);
  if (rows.length !== 1) return resolveLookupRows(rows);

  const mappedState = await mapStateRowToOfficialCode(rows[0]);
  if (!mappedState) return emptyLookup();
  return { code: mappedState.code, status: 'matched', matches: 1 };
}

async function fetchStateMatches(supabase, column, value) {
  const { data, error } = await supabase
    .from('codici_stati')
    .select('codice,nome,nome_it')
    .ilike(column, value)
    .limit(2);
  if (error) {
    console.warn(`submit-public-checkin state lookup error on ${column}:`, error.message);
    return [];
  }
  return Array.isArray(data) ? data : [];
}

async function fetchStateLikeMatches(supabase, column, value) {
  const escaped = escapeLikeValue(String(value || '').trim());
  if (!escaped) return [];
  const { data, error } = await supabase
    .from('codici_stati')
    .select('codice,nome,nome_it')
    .ilike(column, `%${escaped}%`)
    .limit(2);
  if (error) {
    console.warn(`submit-public-checkin state fuzzy lookup error on ${column}:`, error.message);
    return [];
  }
  return Array.isArray(data) ? data : [];
}

async function findOfficialStateByName(value) {
  const index = await getOfficialStateIndex();
  const normalized = normalizeLookupValue(value);
  if (!normalized) return null;
  if (index.has(normalized)) return index.get(normalized);

  const aliased = STATE_ALIASES[normalized];
  if (aliased) {
    const mapped = index.get(normalizeLookupValue(aliased));
    if (mapped) return mapped;
  }

  const compact = normalizeLookupValue(stripParentheticalText(normalized));
  if (compact && index.has(compact)) return index.get(compact);

  return findApproxOfficialStateByName(index, normalized);
}

async function mapStateRowToOfficialCode(row) {
  const candidates = [row?.nome_it, row?.nome]
    .map((value) => normalizeLookupValue(value))
    .filter(Boolean);
  for (const candidate of candidates) {
    const official = await findOfficialStateByName(candidate);
    if (official) return official;
  }
  const rawCode = String(row?.codice || '').trim();
  if (!rawCode) return null;
  return {
    code: rawCode,
    name: String(row?.nome_it || row?.nome || '').trim() || rawCode,
  };
}

async function getOfficialStateIndex() {
  if (!officialStateIndexPromise) {
    officialStateIndexPromise = loadOfficialStateIndex().catch((error) => {
      officialStateIndexPromise = null;
      throw error;
    });
  }
  return officialStateIndexPromise;
}

async function loadOfficialStateIndex() {
  const csv = await loadOfficialStatesCsv();
  if (!csv) return new Map();
  const rows = csv.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  if (rows.length < 2) return new Map();

  const header = rows[0].split(',').map((item) => item.trim().toLowerCase());
  const codiceIdx = header.indexOf('codice');
  const descrizioneIdx = header.indexOf('descrizione');
  const dataFineValIdx = header.indexOf('datafineval');
  const index = new Map();

  for (const line of rows.slice(1)) {
    const cols = line.split(',').map((item) => item.trim());
    const code = cols[codiceIdx] || '';
    const description = cols[descrizioneIdx] || '';
    const dataFineVal = dataFineValIdx !== -1 ? cols[dataFineValIdx] || '' : '';
    if (!code || !description || dataFineVal) continue;
    registerOfficialState(index, code, description);
  }

  return index;
}

async function loadOfficialStatesCsv() {
  const localCsv = loadOfficialStatesCsvFromFile();
  if (localCsv) return localCsv;

  const baseUrl = (await getConfiguredPortalBaseUrl()).replace(/\/+$/, '');
  try {
    const response = await fetch(`${baseUrl}/stati.csv`);
    if (!response.ok) {
      console.warn(`submit-public-checkin unable to load stati.csv: HTTP ${response.status}`);
      return '';
    }
    return response.text();
  } catch (error) {
    console.warn('submit-public-checkin unable to load stati.csv:', error.message);
    return '';
  }
}

async function getConfiguredPortalBaseUrl() {
  if (PUBLIC_PORTAL_BASE_URL) {
    return String(PUBLIC_PORTAL_BASE_URL).replace(/\/+$/, '');
  }

  try {
    const siteLinks = await loadSiteLinksConfig();
    const configured = String(siteLinks?.getCanonicalSiteUrl?.('portale') || '').trim();
    if (configured) {
      return configured.replace(/\/+$/, '');
    }
  } catch (_error) {
    // fall through to final static fallback
  }

  const envFallback = String(
    process.env.SITE_URL_PORTALE
      || process.env.URL
      || process.env.DEPLOY_PRIME_URL
      || 'http://localhost:8888',
  ).trim();
  return envFallback.replace(/\/+$/, '');
}

async function loadSiteLinksConfig() {
  if (!siteLinksConfigPromise) {
    siteLinksConfigPromise = import('../../config/site-links.mjs');
  }
  return siteLinksConfigPromise;
}

function loadOfficialStatesCsvFromFile() {
  const candidates = [
    path.resolve(__dirname, 'stati.csv'),
    path.resolve(process.cwd(), 'stati.csv'),
    path.resolve(__dirname, '../../stati.csv'),
    path.resolve(__dirname, '../stati.csv'),
  ];
  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      return fs.readFileSync(candidate, 'utf8');
    }
  }
  return '';
}

async function findUniqueComuneCode(supabase, value, options = {}) {
  const normalized = String(value || '').trim();
  if (!normalized) return emptyLookup();
  const normalizedComune = normalizeComuneLookupValue(normalized);
  if (!normalizedComune) return emptyLookup();

  const provinceHint = normalizeUpperProvinceCode(options.fallbackProvince) || extractProvinceCodeFromComuneText(normalized);
  const patterns = buildComuneLookupPatterns(normalized);
  const collectedRows = [];
  const seenCodes = new Set();

  for (const pattern of patterns) {
    const rows = await fetchComuneLookupRows(supabase, pattern);
    rows.forEach((row) => {
      const code = String(row?.codice || '').trim();
      if (!code || seenCodes.has(code)) return;
      seenCodes.add(code);
      collectedRows.push(row);
    });

    const resolved = await resolveComuneLookupRows(collectedRows, normalizedComune, provinceHint);
    if (resolved.status === 'matched' || resolved.status === 'ambiguous') {
      return resolved;
    }
  }

  return resolveComuneLookupRows(collectedRows, normalizedComune, provinceHint);
}

async function findDocumentCode(supabase, value) {
  const normalized = String(value || '').trim().toLowerCase().replace(/[’]/g, "'");
  const preferredCodes = DOCUMENT_CODE_PREFERENCES[normalized] || [];
  if (!preferredCodes.length) return emptyLookup();
  const { data, error } = await supabase
    .from('codici_documenti')
    .select('codice,descrizione')
    .in('codice', preferredCodes);
  if (error) {
    console.warn('submit-public-checkin document lookup error:', error.message);
    return emptyLookup('error');
  }
  const rows = Array.isArray(data) ? data : [];
  for (const code of preferredCodes) {
    if (rows.find((row) => row.codice === code)) {
      return { code, status: 'matched', matches: 1 };
    }
  }
  return emptyLookup();
}

function getStoredBirthText(payload) {
  return payload.nato_in_italia ? payload.luogo_nascita || null : payload.stato_nascita || null;
}

function computeGuestCompleteness(record) {
  if (!record) return false;

  const hasBaseFields = [
    record.nome,
    record.cognome,
    record.sesso,
    record.data_nascita,
    record.cittadinanza,
    record.cittadinanza_codice,
    record.stato_nascita_codice,
    record.data_checkin,
    record.data_checkout,
    record.apartment_id,
  ].every(Boolean);

  if (!hasBaseFields) return false;

  const natoInItalia = record.stato_nascita_codice === ITALIA_CODE;
  if (natoInItalia && !record.luogo_nascita_codice) return false;

  const tipo = Number(record.tipo_alloggiato || 16);
  if ([16, 17, 18].includes(tipo)) {
    return [
      record.tipo_documento,
      record.tipo_documento_codice,
      record.numero_documento,
      record.luogo_rilascio_documento,
      record.luogo_rilascio_codice,
    ].every(Boolean);
  }

  return true;
}

function resolveLookupRows(rows) {
  if (!rows.length) return emptyLookup();
  if (rows.length > 1) return { code: '', status: 'ambiguous', matches: rows.length };
  return { code: rows[0].codice || '', status: rows[0].codice ? 'matched' : 'not_found', matches: rows[0].codice ? 1 : 0 };
}

function emptyLookup(status) {
  return { code: '', status: status || 'not_found', matches: 0 };
}

function registerOfficialState(index, code, description) {
  const value = { code, description };
  const variants = new Set([
    normalizeLookupValue(description),
    normalizeLookupValue(stripParentheticalText(description)),
  ]);

  Object.entries(STATE_ALIASES).forEach(([alias, target]) => {
    if (normalizeLookupValue(target) === normalizeLookupValue(description)) {
      variants.add(normalizeLookupValue(alias));
    }
  });

  variants.forEach((variant) => {
    if (variant) index.set(variant, value);
  });
}

function stripParentheticalText(value) {
  return String(value || '').replace(/\s*\([^)]*\)\s*/g, ' ').replace(/\s+/g, ' ').trim();
}

function extractProvinceCodeFromComuneText(value) {
  const raw = String(value || '').replace(/[’`´]/g, "'").replace(/\s+/g, ' ').trim();
  if (!raw) return '';
  const parenMatch = raw.match(/\(([A-Za-z]{2})\)\s*$/);
  if (parenMatch) return normalizeUpperProvinceCode(parenMatch[1]);
  const separatorMatch = raw.match(/[-/]\s*([A-Za-z]{2})\s*$/);
  if (separatorMatch) return normalizeUpperProvinceCode(separatorMatch[1]);
  const commaParts = raw.split(',').map((part) => String(part || '').trim()).filter(Boolean);
  if (commaParts.length > 1) {
    const lastPart = normalizeUpperProvinceCode(commaParts[commaParts.length - 1]);
    if (lastPart) return lastPart;
  }
  const tokens = raw.split(/\s+/).filter(Boolean);
  if (tokens.length > 1) {
    const lastToken = normalizeUpperProvinceCode(tokens[tokens.length - 1]);
    if (lastToken) return lastToken;
  }
  return '';
}

function extractProvinceCodeFromAddress(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const parts = raw.split(',').map((part) => String(part || '').trim()).filter(Boolean);
  for (let index = parts.length - 1; index >= 0; index -= 1) {
    const province = normalizeUpperProvinceCode(parts[index]);
    if (province) return province;
  }
  const tokens = raw.split(/\s+/).filter(Boolean);
  for (let index = tokens.length - 1; index >= 0; index -= 1) {
    const province = normalizeUpperProvinceCode(tokens[index]);
    if (province) return province;
  }
  return '';
}

function normalizeIssuePlaceValue(value) {
  let raw = String(value || '').replace(/[’`´]/g, "'").replace(/\s+/g, ' ').trim();
  if (!raw) return '';
  raw = raw.replace(/^(rilasciato\s+(?:da|presso)\s+)/i, '');
  raw = raw.replace(/^(comune|questura|prefettura|motorizzazione|anagrafe)\s+di\s+/i, '');
  raw = raw.replace(/^(comune|questura|prefettura|motorizzazione|anagrafe)\s+/i, '');
  raw = raw.replace(/\s+/g, ' ').trim();
  return raw;
}

function normalizeUpperProvinceCode(value) {
  const normalized = String(value || '').trim().toUpperCase().replace(/[^A-Z]/g, '');
  return normalized.length === 2 ? normalized : '';
}

function sanitizeComuneLikeValue(value) {
  let raw = String(value || '').replace(/[’`´]/g, "'").replace(/\s+/g, ' ').trim();
  if (!raw) return '';
  raw = raw.replace(/\b\d{5}\b/g, ' ').replace(/\s+/g, ' ').trim();
  raw = raw.replace(/\(([A-Za-z]{2})\)\s*$/, ' ').trim();
  raw = raw.replace(/[-/]\s*[A-Za-z]{2}\s*$/, ' ').trim();
  const commaParts = raw.split(',').map((part) => String(part || '').trim()).filter(Boolean);
  if (commaParts.length > 1) {
    const lastPart = normalizeUpperProvinceCode(commaParts[commaParts.length - 1]);
    raw = lastPart && commaParts[commaParts.length - 2]
      ? commaParts[commaParts.length - 2]
      : commaParts[commaParts.length - 1];
  }
  const tokens = raw.split(/\s+/).filter(Boolean);
  if (tokens.length > 1) {
    const lastToken = normalizeUpperProvinceCode(tokens[tokens.length - 1]);
    if (lastToken) tokens.pop();
  }
  return tokens.join(' ').replace(/\s+/g, ' ').trim();
}

function normalizeComuneLookupValue(value) {
  return normalizeLookupValue(sanitizeComuneLikeValue(value))
    .replace(/['’`´-]/g, '')
    .replace(/\s+/g, '');
}

function buildComuneLookupPatterns(value) {
  const sanitized = sanitizeComuneLikeValue(value);
  if (!sanitized) return [];

  const patterns = [];
  const pushPattern = (pattern) => {
    if (!pattern || patterns.includes(pattern)) return;
    patterns.push(pattern);
  };

  pushPattern(sanitized);

  const prefix = sanitized.slice(0, Math.min(sanitized.length, 4)).trim();
  if (prefix.length >= 2) pushPattern(`${escapeLikeValue(prefix)}%`);

  const tokenCandidates = sanitized
    .split(/\s+/)
    .map((token) => token.replace(/[^A-Za-zÀ-ÿ']/g, '').trim())
    .filter(Boolean)
    .sort((left, right) => right.length - left.length);

  tokenCandidates.forEach((token) => {
    if (token.length >= 3) pushPattern(`%${escapeLikeValue(token)}%`);
  });

  return patterns;
}

async function fetchComuneLookupRows(supabase, pattern) {
  const { data, error } = await supabase
    .from('codici_comuni')
    .select('codice,nome,provincia')
    .ilike('nome', pattern)
    .order('provincia', { ascending: true })
    .limit(50);
  if (error) {
    console.warn('submit-public-checkin comune lookup error:', error.message);
    return [];
  }
  return Array.isArray(data) ? data : [];
}

async function resolveComuneLookupRows(rows, normalizedComune, provinceHint) {
  const exactRows = (Array.isArray(rows) ? rows : []).filter((row) => (
    normalizeComuneLookupValue(row?.nome) === normalizedComune
  ));

  if (provinceHint) {
    const narrowed = exactRows.filter((row) => (
      normalizeUpperProvinceCode(row?.provincia) === provinceHint
    ));
    if (narrowed.length) return resolveLookupRows(narrowed);
  }

  const preferredRows = await preferCurrentComuneRows(exactRows, normalizedComune, {
    normalize: normalizeComuneLookupValue,
  });
  if (preferredRows.length !== exactRows.length) {
    return resolveLookupRows(preferredRows);
  }

  return resolveLookupRows(exactRows);
}

function findApproxOfficialStateByName(index, value) {
  let best = null;
  let bestDistance = Number.POSITIVE_INFINITY;

  for (const [candidate, official] of index.entries()) {
    if (!candidate) continue;
    if (candidate.includes(value) || value.includes(candidate)) {
      if (best && best.code !== official.code) return null;
      best = official;
      bestDistance = 0;
      continue;
    }

    const distance = levenshteinDistance(value, candidate);
    const threshold = getApproximateStateThreshold(value, candidate);
    if (distance > threshold) continue;
    if (distance < bestDistance) {
      best = official;
      bestDistance = distance;
      continue;
    }
    if (distance === bestDistance && best && best.code !== official.code) {
      best = null;
    }
  }

  return best;
}

function getApproximateStateThreshold(a, b) {
  const maxLen = Math.max(String(a || '').length, String(b || '').length);
  if (maxLen <= 6) return 1;
  if (maxLen <= 9) return 2;
  if (maxLen <= 14) return 3;
  return 4;
}

function levenshteinDistance(a, b) {
  const left = String(a || '');
  const right = String(b || '');
  if (!left) return right.length;
  if (!right) return left.length;

  const prev = new Array(right.length + 1);
  const curr = new Array(right.length + 1);
  for (let j = 0; j <= right.length; j += 1) prev[j] = j;

  for (let i = 1; i <= left.length; i += 1) {
    curr[0] = i;
    for (let j = 1; j <= right.length; j += 1) {
      const cost = left[i - 1] === right[j - 1] ? 0 : 1;
      curr[j] = Math.min(
        prev[j] + 1,
        curr[j - 1] + 1,
        prev[j - 1] + cost
      );
    }
    for (let j = 0; j <= right.length; j += 1) prev[j] = curr[j];
  }

  return prev[right.length];
}

function escapeLikeValue(value) {
  return String(value || '').replace(/[%_]/g, '\\$&');
}

function normalizeLookupValue(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[’']/g, "'")
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function logLookupResult(field, rawValue, result) {
  if (result.status === 'matched') return;
  console.warn(`submit-public-checkin ${field} lookup ${result.status}:`, {
    value: rawValue || '',
    matches: result.matches || 0,
  });
}

function calculateAge(value) {
  const match = String(value || '').match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const today = new Date();
  let age = today.getUTCFullYear() - year;
  const monthDiff = (today.getUTCMonth() + 1) - month;
  if (monthDiff < 0 || (monthDiff === 0 && today.getUTCDate() < day)) age -= 1;
  return age;
}

exports.__test__ = {
  findUniqueComuneCode,
  findIssuePlaceMatch,
  normalizeComuneLookupValue,
  sanitizeComuneLikeValue,
  extractProvinceCodeFromComuneText,
  extractProvinceCodeFromAddress,
  normalizeIssuePlaceValue,
};
