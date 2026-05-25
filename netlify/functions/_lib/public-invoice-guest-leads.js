const crypto = require('crypto');

function clean(value) {
  return String(value || '').trim();
}

async function detectGuestLeadColumnSupport(supabase) {
  const names = [
    'portale_token',
    'tipo_cliente',
    'ragione_sociale',
    'indirizzo_fatturazione',
    'indirizzo_residenza',
    'paese_residenza',
    'piva_cliente',
    'segnalazione_jessica',
    'lingua',
    'dati_completi',
    'codice_fiscale',
    'codice_fiscale_verificato',
    'stato',
    'iva_percentuale',
    'importo_lordo',
    'numero_persone',
    'tipo_alloggiato',
    'data_checkin',
    'data_checkout',
    'apartment_id',
  ];

  const entries = await Promise.all(names.map(async (name) => [name, await hasColumn(supabase, 'ospiti_check_in', name)]));
  return Object.fromEntries(entries);
}

function buildGuestLeadPayload(form, columnSupport = {}, options = {}) {
  const today = new Date().toISOString().slice(0, 10);
  const countryCode = resolveCountryCode(form);
  const normalizedTaxCode = countryCode === 'IT' ? clean(form.codice_fiscale).toUpperCase() : '';
  const noteParts = [
    'RICHIESTA_FATTURA_PUBBLICA',
    form.sdi ? `SDI:${form.sdi}` : '',
    form.pec ? `PEC:${form.pec}` : '',
  ].filter(Boolean);

  const payload = {
    nome: clean(form.referente_nome),
    cognome: clean(form.referente_cognome),
    email: clean(form.email).toLowerCase(),
    telefono: clean(form.telefono),
  };

  if (columnSupport.portale_token) payload.portale_token = crypto.randomUUID();
  if (columnSupport.tipo_cliente) payload.tipo_cliente = clean(form.tipo_cliente || 'azienda').toLowerCase();
  if (columnSupport.ragione_sociale) payload.ragione_sociale = clean(form.ragione_sociale);
  if (columnSupport.indirizzo_fatturazione) payload.indirizzo_fatturazione = clean(form.indirizzo_fatturazione);
  if (columnSupport.indirizzo_residenza) payload.indirizzo_residenza = clean(form.indirizzo_fatturazione);
  if (columnSupport.paese_residenza) payload.paese_residenza = countryCode;
  if (columnSupport.piva_cliente) payload.piva_cliente = clean(form.piva_cliente).toUpperCase();
  if (columnSupport.segnalazione_jessica) payload.segnalazione_jessica = noteParts.join('|') || null;
  if (columnSupport.lingua) payload.lingua = 'IT';
  if (columnSupport.dati_completi) payload.dati_completi = false;
  if (columnSupport.codice_fiscale) payload.codice_fiscale = normalizedTaxCode || null;
  if (columnSupport.codice_fiscale_verificato) payload.codice_fiscale_verificato = false;
  if (columnSupport.stato) payload.stato = 'DA_VERIFICARE';
  if (columnSupport.iva_percentuale) payload.iva_percentuale = 22;
  if (columnSupport.importo_lordo) payload.importo_lordo = 0;
  if (columnSupport.numero_persone) payload.numero_persone = 1;
  if (columnSupport.tipo_alloggiato) payload.tipo_alloggiato = 16;
  if (columnSupport.data_checkin) payload.data_checkin = today;
  if (columnSupport.data_checkout) payload.data_checkout = today;
  if (columnSupport.apartment_id) payload.apartment_id = options.apartmentId || null;

  return payload;
}

async function createGuestLeadRecord(supabase, form, options = {}) {
  const columnSupport = options.columnSupport || await detectGuestLeadColumnSupport(supabase);
  const fallbackApartmentId = columnSupport.apartment_id
    ? await resolveFallbackApartmentId(supabase, options.apartmentId)
    : null;
  const payload = buildGuestLeadPayload(form, columnSupport, {
    ...options,
    apartmentId: fallbackApartmentId,
  });
  const today = new Date().toISOString().slice(0, 10);
  const attempts = [
    payload,
    { ...payload, data_checkin: today, data_checkout: today },
    stripNullableKey({ ...payload, data_checkin: today, data_checkout: today }, 'apartment_id'),
  ];
  const attemptErrors = [];

  for (const candidate of attempts) {
    const { data, error } = await supabase
      .from('ospiti_check_in')
      .insert(candidate)
      .select('id')
      .single();

    if (!error && data?.id) {
      return { saved: true, id: data.id, warning: null };
    }

    if (error) {
      attemptErrors.push({
        message: error.message || 'insert failed',
        code: error.code || null,
        details: error.details || null,
        hint: error.hint || null,
      });
    }
  }

  const firstError = attemptErrors[0] || null;
  return {
    saved: false,
    id: null,
    warning: firstError?.message
      ? `Documenti salvati, ma pratica fattura non creata in ospiti_check_in: ${firstError.message}`
      : 'Documenti salvati, ma pratica fattura non creata in ospiti_check_in',
    errors: attemptErrors,
  };
}

async function resolveFallbackApartmentId(supabase, explicitApartmentId) {
  const explicit = clean(explicitApartmentId);
  if (explicit) return explicit;

  const envApartmentId = clean(process.env.PUBLIC_INVOICE_FALLBACK_APARTMENT_ID);
  if (envApartmentId) return envApartmentId;

  const { data, error } = await supabase
    .from('apartments')
    .select('id, nome_appartamento')
    .eq('attivo', true)
    .ilike('nome_appartamento', '%Test Appartamento%')
    .limit(1)
    .maybeSingle();

  if (error) return null;
  return data?.id || null;
}

function extractFormFromAccountingDocument(record) {
  const raw = record?.raw_payload || record?.extracted_json || {};
  return {
    tipo_cliente: clean(raw.tipo_cliente || 'azienda').toLowerCase() || 'azienda',
    ragione_sociale: clean(raw.ragione_sociale || record?.supplier_name),
    referente_nome: clean(raw.referente_nome),
    referente_cognome: clean(raw.referente_cognome),
    email: clean(raw.email || record?.source_email).toLowerCase(),
    telefono: clean(raw.telefono),
    piva_cliente: clean(raw.piva_cliente).toUpperCase(),
    codice_fiscale: clean(raw.codice_fiscale).toUpperCase(),
    indirizzo_fatturazione: clean(raw.indirizzo_fatturazione),
    sdi: clean(raw.sdi).toUpperCase(),
    pec: clean(raw.pec).toLowerCase(),
    note: clean(raw.note || record?.extracted_text),
  };
}

function stripNullableKey(obj, key) {
  const next = { ...obj };
  delete next[key];
  return next;
}

function resolveCountryCode(form) {
  const vat = clean(form.piva_cliente).toUpperCase();
  if (/^\d{11}$/.test(vat) || vat.startsWith('IT')) return 'IT';
  const vatPrefix = vat.match(/^([A-Z]{2})/);
  if (vatPrefix) return vatPrefix[1];

  const address = clean(form.indirizzo_fatturazione).toLowerCase();
  if (address.includes('france') || address.includes('francia')) return 'FR';
  if (address.includes('germany') || address.includes('germania')) return 'DE';
  if (address.includes('spain') || address.includes('spagna')) return 'ES';
  if (address.includes('united kingdom') || address.includes('regno unito')) return 'GB';
  return 'IT';
}

async function hasColumn(supabase, table, column) {
  const { error } = await supabase.from(table).select(column).limit(0);
  return !error;
}

module.exports = {
  buildGuestLeadPayload,
  createGuestLeadRecord,
  detectGuestLeadColumnSupport,
  extractFormFromAccountingDocument,
  hasColumn,
  resolveFallbackApartmentId,
};
