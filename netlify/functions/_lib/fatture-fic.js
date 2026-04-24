const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const FIC_TOKEN = process.env.FATTURE_CLOUD_TOKEN;
const FIC_COMPANY_ID = process.env.FATTURE_CLOUD_COMPANY_ID;
const FIC_BASE = 'https://api-v2.fattureincloud.it';

const VAT_IDS = {
  22: 0,
  10: 3,
  4: 4,
  5: 54,
  0: 6
};

const BOLLO_SOGLIA = 77.47;
const BOLLO_IMPORTO = 2.0;
const SEZIONALI = new Set(['R', 'D', 'P']);
const SEZIONALE_LABELS = {
  R: 'Residence',
  P: 'Portali',
  D: 'Diretti'
};
const DEFAULT_SEZIONALE = 'D';
const BACKOFFICE_EMAILS = [
  'fatturazione@illupoaffitta.com',
  'contabilita@illupoaffitta.com'
];

function jsonResponse(statusCode, body) {
  return {
    statusCode,
    body: JSON.stringify(body)
  };
}

function createSupabaseAdmin() {
  return createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
}

async function authenticateRequest(event, supabase) {
  const authHeader = event.headers.authorization || event.headers.Authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return { errorResponse: jsonResponse(401, { error: 'Unauthorized' }) };
  }

  const token = authHeader.replace('Bearer ', '').trim();
  const {
    data: { user },
    error
  } = await supabase.auth.getUser(token);

  if (error || !user) {
    return { errorResponse: jsonResponse(401, { error: 'Unauthorized' }) };
  }

  const userEmail = normalizeString(user.email).toLowerCase();
  if (!BACKOFFICE_EMAILS.includes(userEmail)) {
    return { errorResponse: jsonResponse(403, { error: 'Forbidden' }) };
  }

  return { user, token };
}

function parseEventJson(event) {
  try {
    return { body: JSON.parse(event.body || '{}') };
  } catch (error) {
    return { errorResponse: jsonResponse(400, { error: 'Invalid JSON', detail: error.message }) };
  }
}

function toNumber(value) {
  const raw = String(value ?? '').trim();
  let normalized = raw;
  if (raw.includes(',') && raw.includes('.')) {
    normalized = raw.replace(/\./g, '').replace(',', '.');
  } else if (raw.includes(',')) {
    normalized = raw.replace(',', '.');
  }
  if (!normalized) return null;
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeString(value) {
  return String(value || '').trim();
}

function normalizeSearchToken(value) {
  return normalizeString(value)
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function computeInvoiceTotals(importoLordoInput, ivaPercentualeInput) {
  const importoLordo = toNumber(importoLordoInput);
  const ivaPercentuale = Number(ivaPercentualeInput ?? 22);

  if (!Number.isFinite(importoLordo) || importoLordo <= 0) {
    return {
      ok: false,
      error: 'importo_lordo non valido'
    };
  }

  const vatId = VAT_IDS[ivaPercentuale];
  if (vatId === undefined) {
    return {
      ok: false,
      error: `iva_percentuale ${ivaPercentuale} non supportata`,
      allowed: Object.keys(VAT_IDS)
    };
  }

  const imponibile = Number((importoLordo / (1 + ivaPercentuale / 100)).toFixed(2));
  const importoTotale = Number(importoLordo.toFixed(2));
  const bolloApplicato = ivaPercentuale === 0 && importoTotale > BOLLO_SOGLIA;
  const bollo = bolloApplicato ? BOLLO_IMPORTO : 0;
  const importoTotaleDocumento = Number((importoTotale + bollo).toFixed(2));
  const imponibileDocumento = Number((imponibile + bollo).toFixed(2));

  return {
    ok: true,
    ivaPercentuale,
    vatId,
    imponibile,
    importoTotale,
    bollo,
    bolloApplicato,
    importoTotaleDocumento,
    imponibileDocumento
  };
}

function resolvePaymentStatus(value) {
  return value === 'not_paid' ? 'not_paid' : 'paid';
}

function resolvePaymentAccountType(value) {
  return value === 'contanti' ? 'contanti' : 'bonifico';
}

function resolveEiPaymentMethodCode(paymentAccountType) {
  return paymentAccountType === 'contanti' ? 'MP01' : 'MP05';
}

function resolvePaymentAccountId(paymentAccountType) {
  const envName = paymentAccountType === 'contanti'
    ? 'FIC_PAYMENT_ACCOUNT_ID_CONTANTI'
    : 'FIC_PAYMENT_ACCOUNT_ID_BONIFICO';
  const label = paymentAccountType === 'contanti' ? 'Contanti' : 'Bonifico';
  const rawValue = String(process.env[envName] || '').trim();
  const accountId = Number(rawValue);

  if (!rawValue || !Number.isFinite(accountId)) {
    return {
      ok: false,
      error: `Payment account ${label} mancante o non valido`,
      detail: {
        payment_account_type: paymentAccountType,
        env_name: envName,
        raw_value: rawValue || null,
        reason: rawValue
          ? 'Il valore non e numerico e non puo essere convertito in id account FiC.'
          : 'La variabile ambiente non e valorizzata nel runtime della function.'
      }
    };
  }

  return {
    ok: true,
    paymentAccountId: accountId,
    paymentAccountEnvName: envName,
    paymentAccountLabel: label
  };
}

function resolveSezionale(value) {
  const requested = normalizeString(value).toUpperCase();
  const hasExplicitSezionale = requested.length > 0;
  const hasValidSezionale = SEZIONALI.has(requested);
  const sezionale = hasValidSezionale ? requested : DEFAULT_SEZIONALE;
  const source = hasValidSezionale ? 'input' : 'fallback_default';
  const warning = hasExplicitSezionale && !hasValidSezionale
    ? `Sezionale non valido "${requested}". Uso fallback ${sezionale} (${SEZIONALE_LABELS[sezionale]}).`
    : !hasExplicitSezionale
      ? `Sezionale mancante. Uso fallback temporaneo ${sezionale} (${SEZIONALE_LABELS[sezionale]}).`
      : null;

  return {
    requestedSezionale: requested || null,
    sezionale,
    sezionaleSource: source,
    sezionaleWarning: warning
  };
}

function buildClientEntity(source, options = {}) {
  const tipoCliente = normalizeString(options.tipoCliente || source.tipo_cliente).toLowerCase();
  const companyName = source.ragione_sociale || source.nome_visualizzato || source.nome || source.piva_cliente || source.piva || '';
  const personName = [source.nome, source.cognome].filter(Boolean).join(' ').trim();
  const billingAddress = source.indirizzo_fatturazione || source.indirizzo || source.indirizzo_residenza || null;
  const isCompany = tipoCliente === 'azienda';
  const client = compactObject({
    name: isCompany ? (companyName || personName || 'Cliente') : (personName || companyName || source.nome_visualizzato || 'Cliente'),
    type: isCompany ? 'company' : 'person',
    address_street: billingAddress || null
  });

  const vatNumber = source.piva_cliente || source.piva || null;
  const taxCode = source.codice_fiscale || null;

  if (vatNumber && (isCompany || tipoCliente === 'professionista')) {
    client.vat_number = vatNumber;
  }

  if (taxCode && (source.codice_fiscale_verificato !== false)) {
    client.tax_code = taxCode;
  }

  if (source.email) client.email = source.email;
  if (source.pec) client.certified_email = source.pec;
  if (source.codice_destinatario) client.ei_code = source.codice_destinatario;
  if (source.paese) client.country = source.paese;
  if (source.cap) client.address_postal_code = source.cap;
  if (source.citta) client.address_city = source.citta;
  if (source.provincia) client.address_province = source.provincia;

  return client;
}

function buildInvoiceItems({
  description,
  itemDescription,
  importoTotale,
  vatId,
  bolloApplicato
}) {
  const itemsList = [
    compactObject({
      name: description || 'Prestazione',
      description: itemDescription || null,
      qty: 1,
      gross_price: importoTotale,
      vat: { id: vatId },
      discount: 0,
      order: 1
    })
  ];

  if (bolloApplicato) {
    itemsList.push({
      name: 'Imposta di bollo',
      qty: 1,
      gross_price: BOLLO_IMPORTO,
      vat: { id: VAT_IDS[0] },
      order: 99
    });
  }

  return itemsList;
}

function compactObject(value) {
  return Object.fromEntries(
    Object.entries(value).filter(([, fieldValue]) => fieldValue !== null && fieldValue !== undefined && fieldValue !== '')
  );
}

function buildFicPayload({
  entity,
  tipoDocumento = 'invoice',
  date,
  itemsList,
  sezionale,
  paymentStatus,
  paymentAccountId,
  paymentAccountType,
  importoTotaleDocumento,
  imponibileDocumento
}) {
  return {
    data: {
      type: tipoDocumento,
      date,
      currency: { id: 'EUR' },
      language: { code: 'it', name: 'Italiano' },
      entity,
      use_gross_prices: true,
      items_list: itemsList,
      numeration: sezionale,
      payments_list: [
        {
          amount: importoTotaleDocumento,
          due_date: date,
          status: paymentStatus,
          payment_account: {
            id: paymentAccountId
          }
        }
      ],
      gross_worth: importoTotaleDocumento,
      net_worth: imponibileDocumento,
      is_marked: false,
      e_invoice: true,
      ei_data: {
        payment_method: resolveEiPaymentMethodCode(paymentAccountType)
      }
    }
  };
}

async function createDraftOnFic(ficPayload) {
  if (!FIC_COMPANY_ID || !FIC_TOKEN) {
    return {
      ok: false,
      statusCode: 500,
      error: 'Configurazione Fatture in Cloud mancante o non valida',
      detail: {
        company: FIC_COMPANY_ID ? 'OK' : 'MISSING',
        token: FIC_TOKEN ? 'OK' : 'MISSING'
      }
    };
  }

  let rawBody = '';
  try {
    const response = await fetch(`${FIC_BASE}/c/${FIC_COMPANY_ID}/issued_documents`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${FIC_TOKEN}`,
        'Content-Type': 'application/json',
        Accept: 'application/json'
      },
      body: JSON.stringify(ficPayload)
    });

    rawBody = await response.text();
    let parsedBody = null;
    try {
      parsedBody = rawBody ? JSON.parse(rawBody) : null;
    } catch (error) {
      return {
        ok: false,
        statusCode: 502,
        error: 'Risposta FiC non JSON',
        detail: error.message,
        raw_response: rawBody
      };
    }

    if (!response.ok) {
      return {
        ok: false,
        statusCode: response.status,
        error: 'Errore FiC API',
        detail: parsedBody
      };
    }

    const ficDoc = parsedBody?.data || null;
    const ficDocId = ficDoc?.id ?? null;
    const ficDocUrl = ficDoc?.url ?? null;
    const ficNumero = ficDoc?.number ?? null;
    const ficReturnedNumeration = ficDoc?.numeration ?? ficDoc?.numeration_current ?? ficDoc?.numeration_next ?? null;
    const resolvedDocumentUrl = ficDocUrl ?? (ficDocId ? `https://secure.fattureincloud.it/issued_documents/${ficDocId}` : null);

    return {
      ok: true,
      ficResponse: parsedBody,
      ficDoc,
      ficDocId,
      ficDocUrl: resolvedDocumentUrl,
      ficNumero,
      ficReturnedNumeration
    };
  } catch (error) {
    return {
      ok: false,
      statusCode: 500,
      error: 'Errore chiamata FiC',
      detail: error.message,
      raw_response: rawBody || null
    };
  }
}

async function saveFatturaStaging(supabase, payload) {
  if (payload.ospiti_check_in_id) {
    const { data, error } = await supabase
      .from('fatture_staging')
      .upsert(payload, {
        onConflict: 'ospiti_check_in_id',
        returning: 'representation'
      })
      .select()
      .single();
    return { data, error, mode: 'upsert' };
  }

  const { data, error } = await supabase
    .from('fatture_staging')
    .insert(payload)
    .select()
    .single();
  return { data, error, mode: 'insert' };
}

async function insertAuditLog(supabase, { userEmail, action, tableName, recordId }) {
  return supabase.from('audit_log').insert({
    user_email: userEmail || '',
    action,
    table_name: tableName,
    record_id: recordId,
    timestamp: new Date().toISOString()
  });
}

async function hasColumn(supabase, table, column) {
  const { error } = await supabase.from(table).select(column).limit(1);
  return !error;
}

async function detectOptionalColumns(supabase) {
  const [
    ragione_sociale,
    indirizzo_fatturazione,
    fatture_staging_payment_status,
    fatture_staging_fic_document_id,
    fatture_staging_sezionale,
    fatture_staging_cliente_id,
    fatture_staging_input_testuale_originale,
    fatture_staging_parsing_payload
  ] = await Promise.all([
    hasColumn(supabase, 'ospiti_check_in', 'ragione_sociale'),
    hasColumn(supabase, 'ospiti_check_in', 'indirizzo_fatturazione'),
    hasColumn(supabase, 'fatture_staging', 'payment_status'),
    hasColumn(supabase, 'fatture_staging', 'fic_document_id'),
    hasColumn(supabase, 'fatture_staging', 'sezionale'),
    hasColumn(supabase, 'fatture_staging', 'cliente_id'),
    hasColumn(supabase, 'fatture_staging', 'input_testuale_originale'),
    hasColumn(supabase, 'fatture_staging', 'parsing_payload')
  ]);

  return {
    ragione_sociale,
    indirizzo_fatturazione,
    fatture_staging_payment_status,
    fatture_staging_fic_document_id,
    fatture_staging_sezionale,
    fatture_staging_cliente_id,
    fatture_staging_input_testuale_originale,
    fatture_staging_parsing_payload
  };
}

module.exports = {
  BACKOFFICE_EMAILS,
  BOLLO_IMPORTO,
  BOLLO_SOGLIA,
  DEFAULT_SEZIONALE,
  SEZIONALI,
  SEZIONALE_LABELS,
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  VAT_IDS,
  authenticateRequest,
  buildClientEntity,
  buildFicPayload,
  buildInvoiceItems,
  computeInvoiceTotals,
  createDraftOnFic,
  createSupabaseAdmin,
  detectOptionalColumns,
  hasColumn,
  insertAuditLog,
  jsonResponse,
  normalizeSearchToken,
  normalizeString,
  parseEventJson,
  compactObject,
  resolvePaymentAccountId,
  resolvePaymentAccountType,
  resolveEiPaymentMethodCode,
  resolvePaymentStatus,
  resolveSezionale,
  saveFatturaStaging,
  toNumber
};
