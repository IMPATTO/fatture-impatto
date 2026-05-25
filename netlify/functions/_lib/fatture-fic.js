const { createClient } = require('@supabase/supabase-js');
const {
  INTERNAL_ALLOWED_EMAILS,
  getSupabaseRuntimeConfig,
  isInternalAllowedEmail,
} = require('./shared-auth');

const SUPABASE_URL = getSupabaseRuntimeConfig().url;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
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
const BACKOFFICE_EMAILS = Array.from(INTERNAL_ALLOWED_EMAILS);
const LOCAL_DRAFT_ELIGIBLE_STATI = ['CHECK_IN_COMPLETATO', 'DA_VERIFICARE', 'APPROVATA'];
const LOCAL_DRAFT_INCOMPLETE_STATO = 'DA_COMPLETARE';

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
  if (!isInternalAllowedEmail(userEmail)) {
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

async function ensureLocalInvoiceDraftForOspiteId(supabase, ospiteIdInput, options = {}) {
  const ospiteId = normalizeString(ospiteIdInput);
  if (!ospiteId) {
    return { ok: false, error: new Error('ospiti_check_in_id richiesto') };
  }

  const columnSupport = options.columnSupport || await detectOptionalColumns(supabase);
  const force = options.force === true;

  const selectFields = [
    'id',
    'nome',
    'cognome',
    'stato',
    'tipo_cliente',
    'piva_cliente',
    'iva_percentuale',
    'importo_lordo',
    'data_checkin',
    'data_checkout'
  ];
  if (columnSupport.ragione_sociale) selectFields.push('ragione_sociale');
  if (columnSupport.indirizzo_fatturazione) selectFields.push('indirizzo_fatturazione');

  const { data: ospite, error: ospiteError } = await supabase
    .from('ospiti_check_in')
    .select(selectFields.join(','))
    .eq('id', ospiteId)
    .maybeSingle();

  if (ospiteError) return { ok: false, error: ospiteError };
  if (!ospite) return { ok: false, error: new Error('Ospite non trovato') };

  const ospiteStato = String(ospite.stato || '');
  if (!LOCAL_DRAFT_ELIGIBLE_STATI.includes(ospiteStato)) {
    return {
      ok: true,
      ospite,
      data: null,
      skipped: true,
      reason: `stato_${ospiteStato || 'UNKNOWN'}`.toLowerCase()
    };
  }

  const tipoCliente = String(ospite.tipo_cliente || '').toLowerCase();
  const iva = Number(ospite.iva_percentuale ?? 0);
  const invoiceIntent = tipoCliente === 'azienda'
    || tipoCliente === 'professionista'
    || iva >= 22
    || !!String(ospite.ragione_sociale || '').trim()
    || !!String(ospite.piva_cliente || '').trim()
    || !!String(ospite.indirizzo_fatturazione || '').trim();

  if (!invoiceIntent) {
    return { ok: true, ospite, data: null, skipped: true, reason: 'no_invoice_intent' };
  }

  const { data: existing, error: existingError } = await supabase
    .from('fatture_staging')
    .select('id, stato, numero_fattura, link_fatture_cloud, importo_lordo, iva_percentuale, importo_totale_con_iva')
    .eq('ospiti_check_in_id', ospiteId)
    .maybeSingle();

  if (existingError) return { ok: false, error: existingError };

  const existingHasRemote = !!(
    existing?.link_fatture_cloud
    || existing?.numero_fattura
    || String(existing?.stato || '').toUpperCase() === 'BOZZA_CREATA'
  );

  if (existingHasRemote && !force) {
    // Esiste già una bozza remota o una bozza locale già collegata.
    // Se l'ospite e ancora in stato APPROVATA, riallinea a BOZZA_CREATA (arretrati).
    if (String(ospite.stato || '').toUpperCase() === 'APPROVATA') {
      const { error: ospiteUpdateError } = await supabase
        .from('ospiti_check_in')
        .update({ stato: 'BOZZA_CREATA', updated_at: new Date().toISOString() })
        .eq('id', ospiteId)
        .eq('stato', 'APPROVATA');
      if (ospiteUpdateError) return { ok: false, error: ospiteUpdateError };
      return { ok: true, ospite: { ...ospite, stato: 'BOZZA_CREATA' }, data: existing, updated: true, skipped: false, reason: 'ospite_status_synced' };
    }
    return { ok: true, ospite, data: existing, skipped: true, reason: 'already_exists' };
  }

  const totals = computeInvoiceTotals(ospite.importo_lordo, ospite.iva_percentuale ?? 22);
  const hasValidAmount = totals.ok === true;
  const existingStatoRaw = String(existing?.stato || '');
  const existingStato = existingStatoRaw.toUpperCase();
  const existingImportoLordo = Number(existing?.importo_lordo);
  const existingHasAmounts = Number.isFinite(existingImportoLordo) && existingImportoLordo > 0;
  const resultingHasAmount = hasValidAmount || existingHasAmounts;
  const existingIsAdvanced = existingStato.length > 0 && existingStato !== LOCAL_DRAFT_INCOMPLETE_STATO;

  let nextStato = LOCAL_DRAFT_INCOMPLETE_STATO;
  if (resultingHasAmount) {
    nextStato = existingStatoRaw || 'APPROVATA';
  } else if (existingIsAdvanced) {
    nextStato = existingStatoRaw;
  }

  const payload = {
    ospiti_check_in_id: ospite.id,
    nome_cliente: ospite.nome,
    cognome_cliente: ospite.cognome,
    stato: nextStato
  };

  if (hasValidAmount) {
    payload.importo_lordo = totals.importoTotale;
    payload.iva_percentuale = totals.ivaPercentuale;
    payload.importo_totale_con_iva = totals.importoTotaleDocumento;
  } else if (existingHasAmounts) {
    payload.importo_lordo = existing.importo_lordo;
    payload.iva_percentuale = existing.iva_percentuale ?? Number(ospite.iva_percentuale ?? 22);
    payload.importo_totale_con_iva = existing.importo_totale_con_iva ?? null;
  } else {
    payload.importo_lordo = null;
    payload.iva_percentuale = Number(ospite.iva_percentuale ?? 22);
    payload.importo_totale_con_iva = null;
  }

  const upsertResult = await saveFatturaStaging(supabase, payload);
  if (upsertResult.error) {
    return { ok: false, error: upsertResult.error };
  }

  const wasCreated = !existing;
  const wasUpdated = !!existing;

  return {
    ok: true,
    ospite,
    data: upsertResult.data || null,
    created: wasCreated,
    updated: wasUpdated,
    skipped: false,
    incomplete: !resultingHasAmount,
    reason: wasCreated
      ? (resultingHasAmount ? 'created' : 'created_incomplete')
      : (resultingHasAmount ? 'updated' : 'updated_incomplete')
  };
}

module.exports = {
  BACKOFFICE_EMAILS,
  BOLLO_IMPORTO,
  BOLLO_SOGLIA,
  DEFAULT_SEZIONALE,
  LOCAL_DRAFT_ELIGIBLE_STATI,
  LOCAL_DRAFT_INCOMPLETE_STATO,
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
  ensureLocalInvoiceDraftForOspiteId,
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
