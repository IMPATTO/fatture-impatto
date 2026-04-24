const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const PUBLIC_PORTAL_BASE_URL = process.env.PUBLIC_PORTAL_BASE_URL || '';

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

  let body;
  try {
    body = JSON.parse(event.body || '{}');
  } catch {
    return respond(400, { error: 'Invalid JSON body' });
  }

  const payload = normalizePayload(body);
  const validationErrors = validatePayload(payload);
  if (validationErrors.length) {
    return respond(400, { error: 'Validation failed', fields: validationErrors });
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

  const columnSupport = await detectOptionalColumns(supabase);
  const operationalErrors = await validateOperationalGuestCodes(supabase, payload);
  if (operationalErrors.length) {
    return respond(400, { error: 'Validation failed', fields: operationalErrors });
  }
  const insertPayload = await buildInsertPayload(supabase, payload, columnSupport);

  const { data: inserted, error: insertError } = await supabase
    .from('ospiti_check_in')
    .insert(insertPayload)
    .select('id, portale_token, apartment_id, email, stato')
    .single();

  if (insertError) {
    console.error('submit-public-checkin insert error:', insertError);
    return respond(500, { error: 'Insert failed', detail: insertError.message });
  }

  if (!inserted) {
    return respond(500, { error: 'Insert completed without returned row' });
  }

  let childRecords = [];
  if (payload.additional_guests.length) {
    const childInsertPayloads = await buildAdditionalGuestInsertPayloads(
      supabase,
      payload,
      columnSupport,
      inserted.id
    );

    const { data: insertedChildren, error: childInsertError } = await supabase
      .from('ospiti_check_in')
      .insert(childInsertPayloads)
      .select('id, capogruppo_id, tipo_alloggiato, nome, cognome');

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

  return respond(200, {
    ok: true,
    record: inserted,
    child_records: childRecords,
    portal_url: `${resolvePortalBaseUrl(event).replace(/\/+$/, '')}/portale.html?token=${inserted.portale_token}`,
  });
};

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
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
  };
}

function validatePayload(payload) {
  const errors = [];
  const isItalian = payload.paese_residenza === 'IT';

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
      if (!guest.luogo_nascita) errors.push(errorField(`additionalGuest_${index}_luogo_nascita`, 'Luogo di nascita obbligatorio'));
      if (guestBornInItaly) {
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

  return errors;
}

async function buildInsertPayload(supabase, payload, columnSupport) {
  const isItalian = payload.paese_residenza === 'IT';
  const hasInvoice = payload.vuoi_fattura;
  const segnalazione = hasInvoice
    ? [payload.sdi ? `SDI:${payload.sdi}` : '', payload.pec ? `PEC:${payload.pec}` : ''].filter(Boolean).join('|') || null
    : (isItalian && !payload.codice_fiscale_verificato ? 'CF non verificato lato client - verificare' : null);
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
  insertPayload.dati_completi = computeGuestCompleteness(insertPayload);

  return insertPayload;
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

function resolvePortalBaseUrl(event) {
  if (PUBLIC_PORTAL_BASE_URL) return PUBLIC_PORTAL_BASE_URL;
  if (event.headers.origin) return event.headers.origin;
  if (event.headers.host) return `https://${event.headers.host}`;
  return 'https://checkin.illupoaffitta.com';
}

function errorField(field, message) {
  return { field, message };
}

async function resolveApartmentReference(supabase, rawReference) {
  const reference = String(rawReference || '').trim();
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
    errors.push(errorField('luogoRilascioDocumento', 'Luogo rilascio documento non risolvibile in modo univoco'));
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
  };
}

async function hasColumn(supabase, column) {
  const { error } = await supabase.from('ospiti_check_in').select(column).limit(1);
  return !error;
}

async function diagnoseChildInsertFailure(supabase, records) {
  const insertedIds = [];
  for (let index = 0; index < records.length; index += 1) {
    const record = records[index];
    const { data, error } = await supabase
      .from('ospiti_check_in')
      .insert(record)
      .select('id')
      .single();
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
  const rilascioMatch = await findUniqueComuneCode(supabase, payload.luogo_rilascio_documento);
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

async function findOfficialStateByName(value) {
  const index = await getOfficialStateIndex();
  return index.get(value) || null;
}

async function mapStateRowToOfficialCode(row) {
  const candidates = [row?.nome_it, row?.nome]
    .map((value) => normalizeLookupValue(value))
    .filter(Boolean);
  for (const candidate of candidates) {
    const official = await findOfficialStateByName(candidate);
    if (official) return official;
  }
  return null;
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
    index.set(normalizeLookupValue(description), { code, description });
  }

  return index;
}

async function loadOfficialStatesCsv() {
  const localCsv = loadOfficialStatesCsvFromFile();
  if (localCsv) return localCsv;

  const baseUrl = (PUBLIC_PORTAL_BASE_URL || 'https://checkinillupoaffitta.netlify.app').replace(/\/+$/, '');
  const response = await fetch(`${baseUrl}/stati.csv`);
  if (!response.ok) throw new Error(`Unable to load stati.csv: HTTP ${response.status}`);
  return response.text();
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

async function findUniqueComuneCode(supabase, value) {
  const normalized = String(value || '').trim();
  if (!normalized) return emptyLookup();
  const { data, error } = await supabase
    .from('codici_comuni')
    .select('codice,nome,provincia')
    .ilike('nome', normalized)
    .limit(2);
  if (error) {
    console.warn('submit-public-checkin comune lookup error:', error.message);
    return emptyLookup('error');
  }
  return resolveLookupRows(Array.isArray(data) ? data : []);
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
