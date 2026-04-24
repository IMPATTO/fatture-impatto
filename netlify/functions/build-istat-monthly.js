const { createClient } = require('@supabase/supabase-js');

const VALID_STAY_STATUSES = new Set([
  'APPROVATA',
  'CREDENZIALI_INVIATE',
  'BOZZA_CREATA',
  'CHECK_IN_COMPLETATO',
]);

const REGION_RULES = {
  'emilia-romagna': {
    sistema: 'ross1000',
    portal_url: null,
    deadline_rule: 'entro il 5 del mese successivo',
    requires_open_close: false,
  },
  'marche': {
    sistema: 'istrice_ross1000',
    portal_url: null,
    deadline_rule: 'entro il 5 del mese successivo',
    requires_open_close: true,
  },
  'veneto': {
    sistema: 'ross1000',
    portal_url: null,
    deadline_rule: 'entro i primi 10 giorni lavorativi del mese successivo',
    requires_open_close: false,
  },
  'valle-daosta': {
    sistema: 'vit_albergatori',
    portal_url: null,
    deadline_rule: 'entro il 5 del mese successivo',
    requires_open_close: true,
  },
};

exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') {
    return jsonResponse(405, { error: 'Method not allowed' });
  }

  const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_ROLE_KEY
  );

  const authHeader = event.headers.authorization || event.headers.Authorization;
  if (!authHeader?.startsWith('Bearer ')) {
    return jsonResponse(401, { error: 'Unauthorized' });
  }

  const token = authHeader.replace('Bearer ', '');
  const { data: { user }, error: userErr } = await supabase.auth.getUser(token);
  if (userErr || !user) {
    return jsonResponse(401, { error: 'Unauthorized' });
  }

  let body;
  try {
    body = JSON.parse(event.body || '{}');
  } catch {
    return jsonResponse(400, { error: 'Invalid JSON' });
  }

  const validation = validateInput(body);
  if (validation.error) {
    return jsonResponse(400, { error: validation.error });
  }

  const { apartment_id: apartmentId, year, month, mode } = validation;

  try {
    const { monthStart, monthEndExclusive, monthKey, monthReferenceDate } = getMonthBounds(year, month);

    const { data: config, error: configErr } = await supabase
      .from('apartment_istat_config')
      .select('*')
      .eq('apartment_id', apartmentId)
      .eq('attivo', true)
      .maybeSingle();

    if (configErr) {
      return jsonResponse(500, { error: 'Errore caricamento configurazione ISTAT', detail: configErr.message });
    }

    if (!config) {
      return jsonResponse(400, { error: 'Configurazione ISTAT attiva non trovata per questo appartamento' });
    }

    const regionRule = REGION_RULES[config.regione];
    if (!regionRule) {
      return jsonResponse(400, { error: `Regione ISTAT non supportata: ${config.regione}` });
    }

    if (config.sistema !== regionRule.sistema) {
      return jsonResponse(400, {
        error: `Configurazione incoerente: ${config.regione} richiede sistema ${regionRule.sistema}`,
      });
    }

    const { data: apartment } = await supabase
      .from('apartments')
      .select('id,nome_appartamento')
      .eq('id', apartmentId)
      .maybeSingle();

    const effectiveRequiresOpenClose = Boolean(regionRule.requires_open_close || config.requires_open_close);

    const { data: rawRows, error: staysErr } = await supabase
      .from('ospiti_check_in')
      .select('*')
      .eq('apartment_id', apartmentId)
      .lt('data_checkin', monthEndExclusive.toISOString().slice(0, 10))
      .gt('data_checkout', monthStart.toISOString().slice(0, 10))
      .order('data_checkin', { ascending: true })
      .limit(5000);

    if (staysErr) {
      return jsonResponse(500, { error: 'Errore lettura soggiorni', detail: staysErr.message });
    }

    const normalizedData = buildMonthlyDataset({
      apartmentId,
      apartmentName: apartment?.nome_appartamento || null,
      config,
      effectiveRequiresOpenClose,
      records: rawRows || [],
      monthStart,
      monthEndExclusive,
      monthKey,
    });

    const preview = {
      apartment_id: apartmentId,
      apartment_name: apartment?.nome_appartamento || null,
      month_key: monthKey,
      regione: config.regione,
      sistema: config.sistema,
      codice_struttura: config.codice_struttura || null,
      deadline_rule: config.deadline_rule || regionRule.deadline_rule,
      summary: normalizedData.summary,
      rows: normalizedData.rows,
      daily_status: normalizedData.daily_status,
      warnings: normalizedData.warnings,
    };

    await insertAuditLog(supabase, {
      user_email: user.email,
      action: mode === 'preview' ? 'ISTAT_PREVIEW' : 'ISTAT_EXPORT',
      table_name: 'istat_invii',
      record_id: apartmentId,
    });

    if (mode === 'preview') {
      return jsonResponse(200, { success: true, mode, preview });
    }

    const exportPayload = buildRegionalExport(config, preview);
    const exportEsito = decideExportOutcome({ config, effectiveRequiresOpenClose, preview });

    const logResult = await logIstatRun(supabase, {
      apartment_id: apartmentId,
      config_id: config.id,
      mese_riferimento: monthReferenceDate,
      regione: config.regione,
      sistema: config.sistema,
      modalita: 'export',
      esito: exportEsito,
      payload_json: {
        preview,
        export: exportPayload,
      },
      file_generato_path: null,
      risposta_portale: null,
      errore_dettaglio: preview.warnings.length ? preview.warnings.join(' | ') : null,
      inviato_da: user.email,
    });

    return jsonResponse(200, {
      success: true,
      mode,
      preview,
      exportResult: {
        esito: exportEsito,
        file_generato_path: null,
        payload: exportPayload,
        log_id: logResult?.id || null,
      },
    });
  } catch (err) {
    console.error('[build-istat-monthly] error:', err);
    return jsonResponse(500, { error: 'Errore interno', detail: err.message });
  }
};

function validateInput(body) {
  const apartmentId = typeof body.apartment_id === 'string' ? body.apartment_id.trim() : '';
  const year = Number(body.year);
  const month = Number(body.month);
  const mode = body.mode === 'export' ? 'export' : body.mode === 'preview' ? 'preview' : '';

  if (!apartmentId) return { error: 'apartment_id richiesto' };
  if (!Number.isInteger(year) || year < 2020 || year > 2100) return { error: 'year non valido' };
  if (!Number.isInteger(month) || month < 1 || month > 12) return { error: 'month non valido' };
  if (!mode) return { error: 'mode deve essere preview oppure export' };

  return {
    apartment_id: apartmentId,
    year,
    month,
    mode,
  };
}

function getMonthBounds(year, month) {
  const monthStart = new Date(Date.UTC(year, month - 1, 1));
  const monthEndExclusive = new Date(Date.UTC(year, month, 1));
  return {
    monthStart,
    monthEndExclusive,
    monthKey: `${year}-${String(month).padStart(2, '0')}`,
    monthReferenceDate: `${year}-${String(month).padStart(2, '0')}-01`,
  };
}

function enumerateNights(checkin, checkout) {
  const nights = [];
  const current = new Date(checkin.getTime());
  while (current < checkout) {
    nights.push(current.toISOString().slice(0, 10));
    current.setUTCDate(current.getUTCDate() + 1);
  }
  return nights;
}

function splitStayAcrossMonth(checkin, checkout, monthStart, monthEndExclusive) {
  const effectiveStart = checkin > monthStart ? checkin : monthStart;
  const effectiveEnd = checkout < monthEndExclusive ? checkout : monthEndExclusive;
  if (effectiveStart >= effectiveEnd) {
    return {
      arrival_in_month: false,
      nights_in_month: 0,
      nights: [],
    };
  }

  return {
    arrival_in_month: isSameUtcDay(checkin, monthStart) || (checkin >= monthStart && checkin < monthEndExclusive),
    nights_in_month: enumerateNights(effectiveStart, effectiveEnd).length,
    nights: enumerateNights(effectiveStart, effectiveEnd),
  };
}

function normalizeCountry(paese_residenza, cittadinanza, regione) {
  const rawCountry = firstNonEmpty(paese_residenza, cittadinanza, 'Non specificato');
  const country = normalizeLabel(rawCountry);
  const regionLabel = normalizeLabel(regione || '');
  const italyAliases = new Set(['ITALIA', 'ITALY', 'IT']);
  const isItaly = italyAliases.has(country.toUpperCase());

  if (isItaly && regionLabel) {
    return {
      key: `ITALIA|${regionLabel.toUpperCase()}`,
      label: `Italia - ${regionLabel}`,
      country: 'Italia',
      region: regionLabel,
      is_italy: true,
    };
  }

  if (isItaly) {
    return {
      key: 'ITALIA',
      label: 'Italia',
      country: 'Italia',
      region: null,
      is_italy: true,
    };
  }

  return {
    key: country.toUpperCase(),
    label: country,
    country,
    region: null,
    is_italy: false,
  };
}

function buildDailyStatus({ monthStart, monthEndExclusive, dailyCounters, effectiveRequiresOpenClose, warnings }) {
  const days = [];
  const current = new Date(monthStart.getTime());
  while (current < monthEndExclusive) {
    const key = current.toISOString().slice(0, 10);
    const counter = dailyCounters[key] || { arrivals: 0, presences: 0, guests_present: 0 };
    const hasMovement = counter.arrivals > 0 || counter.presences > 0;
    let status = hasMovement ? 'OPEN' : 'ZERO_MOVEMENT';

    if (effectiveRequiresOpenClose && !hasMovement) {
      status = 'ZERO_MOVEMENT';
    }

    days.push({
      date: key,
      arrivals: counter.arrivals,
      presences: counter.presences,
      guests_present: counter.guests_present,
      zero_movement: !hasMovement,
      status,
    });

    current.setUTCDate(current.getUTCDate() + 1);
  }

  if (effectiveRequiresOpenClose && !days.some((day) => day.status === 'OPEN')) {
    warnings.push('Mese senza movimento: per la regione configurata potrebbe servire conferma manuale aperto/chiuso o chiusura mensile.');
  }

  return days;
}

function aggregateMonthlyRows(stayRows) {
  const bucket = new Map();

  for (const stay of stayRows) {
    const existing = bucket.get(stay.origin.key) || {
      provenienza_key: stay.origin.key,
      provenienza_label: stay.origin.label,
      arrivi: 0,
      presenze: 0,
      ospiti_coinvolti: 0,
    };

    if (stay.arrival_in_month) existing.arrivi += 1;
    existing.presenze += stay.nights_in_month;
    existing.ospiti_coinvolti += 1;
    bucket.set(stay.origin.key, existing);
  }

  return Array.from(bucket.values()).sort((a, b) => {
    if (b.presenze !== a.presenze) return b.presenze - a.presenze;
    return a.provenienza_label.localeCompare(b.provenienza_label, 'it');
  });
}

function buildRegionalExport(config, normalizedData) {
  switch (config.regione) {
    case 'emilia-romagna':
      return buildExportRoss1000EmiliaRomagna(normalizedData);
    case 'marche':
      return buildExportIstriceRoss1000Marche(normalizedData);
    case 'veneto':
      return buildExportRoss1000Veneto(normalizedData);
    case 'valle-daosta':
      return buildExportVitValleDAosta(normalizedData);
    default:
      throw new Error(`Adapter regionale non disponibile per ${config.regione}`);
  }
}

async function logIstatRun(supabase, payload) {
  const { data, error } = await supabase
    .from('istat_invii')
    .insert(payload)
    .select('id')
    .single();

  if (error) {
    console.error('[build-istat-monthly] insert istat_invii error', error);
    throw new Error(`Errore log istat_invii: ${error.message}`);
  }

  return data;
}

function buildMonthlyDataset({ apartmentId, apartmentName, config, effectiveRequiresOpenClose, records, monthStart, monthEndExclusive, monthKey }) {
  const warnings = [];
  const includedRows = [];
  const dailyCounters = {};

  for (const raw of records) {
    if (!isValidStayRecord(raw)) {
      continue;
    }

    const parsed = parseStayDates(raw);
    if (parsed.error) {
      warnings.push(parsed.error);
      continue;
    }

    const { checkin, checkout } = parsed;
    const split = splitStayAcrossMonth(checkin, checkout, monthStart, monthEndExclusive);
    if (!split.nights_in_month && !split.arrival_in_month) {
      continue;
    }

    const origin = normalizeCountry(raw.paese_residenza, raw.cittadinanza, raw.regione || raw.regione_residenza);

    const normalizedStay = {
      source_id: raw.id,
      apartment_id: apartmentId,
      apartment_name: apartmentName,
      guest_name: [raw.nome, raw.cognome].filter(Boolean).join(' ').trim() || 'Ospite senza nome',
      checkin: checkin.toISOString().slice(0, 10),
      checkout: checkout.toISOString().slice(0, 10),
      arrival_in_month: split.arrival_in_month,
      nights_in_month: split.nights_in_month,
      nights: split.nights,
      origin,
      capogruppo_id: raw.capogruppo_id || null,
      raw_status: raw.stato || null,
    };

    includedRows.push(normalizedStay);

    if (split.arrival_in_month) {
      ensureDailyCounter(dailyCounters, normalizedStay.checkin).arrivals += 1;
    }
    for (const night of split.nights) {
      const bucket = ensureDailyCounter(dailyCounters, night);
      bucket.presences += 1;
      bucket.guests_present += 1;
    }
  }

  if (!includedRows.length) {
    warnings.push('Nessun soggiorno valido trovato per il mese richiesto.');
  }

  const rows = aggregateMonthlyRows(includedRows);
  const daily_status = buildDailyStatus({ monthStart, monthEndExclusive, dailyCounters, effectiveRequiresOpenClose, warnings });

  return {
    apartment_id: apartmentId,
    apartment_name: apartmentName,
    month_key: monthKey,
    regione: config.regione,
    sistema: config.sistema,
    codice_struttura: config.codice_struttura || null,
    summary: {
      total_arrivi: includedRows.filter((row) => row.arrival_in_month).length,
      total_presenze: includedRows.reduce((sum, row) => sum + row.nights_in_month, 0),
      total_ospiti_coinvolti: includedRows.length,
    },
    rows,
    daily_status,
    warnings: uniqueList(warnings),
  };
}

function buildExportRoss1000EmiliaRomagna(data) {
  return {
    type: 'ross1000_preview',
    regione: data.regione,
    sistema: data.sistema,
    month_key: data.month_key,
    codice_struttura: data.codice_struttura,
    summary: data.summary,
    rows: data.rows,
    zero_movement: data.summary.total_presenze === 0,
    requires_open_close: false,
  };
}

function buildExportIstriceRoss1000Marche(data) {
  return {
    type: 'istrice_ross1000_preview',
    regione: data.regione,
    sistema: data.sistema,
    month_key: data.month_key,
    codice_struttura: data.codice_struttura,
    summary: data.summary,
    rows: data.rows,
    zero_movement: data.summary.total_presenze === 0,
    requires_open_close: true,
    open_close_required: true,
    daily_status: data.daily_status,
  };
}

function buildExportRoss1000Veneto(data) {
  return {
    type: 'ross1000_preview',
    regione: data.regione,
    sistema: data.sistema,
    month_key: data.month_key,
    codice_struttura: data.codice_struttura,
    summary: data.summary,
    rows: data.rows,
    zero_movement: data.summary.total_presenze === 0,
    requires_open_close: false,
    notes: 'Scadenza configurata separatamente: entro i primi 10 giorni lavorativi del mese successivo.',
  };
}

function buildExportVitValleDAosta(data) {
  return {
    type: 'vit_albergatori_preview',
    regione: data.regione,
    sistema: data.sistema,
    month_key: data.month_key,
    codice_struttura: data.codice_struttura,
    summary: data.summary,
    rows: data.rows,
    zero_movement: data.summary.total_presenze === 0,
    requires_open_close: true,
    monthly_closure_required: true,
    daily_status: data.daily_status,
  };
}

function parseStayDates(raw) {
  if (!raw.data_checkin || !raw.data_checkout) {
    return {
      error: `Record ${raw.id || 'senza-id'} escluso: check-in/check-out mancante.`,
    };
  }

  const checkin = parseIsoDate(raw.data_checkin);
  const checkout = parseIsoDate(raw.data_checkout);

  if (!checkin || !checkout) {
    return {
      error: `Record ${raw.id || 'senza-id'} escluso: date non valide.`,
    };
  }

  if (checkout <= checkin) {
    return {
      error: `Record ${raw.id || 'senza-id'} escluso: checkout non successivo al checkin.`,
    };
  }

  return { checkin, checkout };
}

function isValidStayRecord(raw) {
  if (!raw || !raw.apartment_id) return false;
  if (!raw.data_checkin) return false;
  if (raw.stato && !VALID_STAY_STATUSES.has(raw.stato)) return false;

  const discardFlags = [
    raw.scartato,
    raw.deleted,
    raw.annullato,
  ];
  if (discardFlags.some(Boolean)) return false;

  const skipStates = ['SCARTATA', 'ANNULLATA', 'RIFIUTATA', 'TEST', 'DUPLICATO'];
  if (skipStates.includes(String(raw.stato || '').toUpperCase())) return false;

  return true;
}

function parseIsoDate(value) {
  if (!value) return null;
  const str = String(value).slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(str)) return null;
  const date = new Date(`${str}T00:00:00.000Z`);
  return Number.isNaN(date.getTime()) ? null : date;
}

function isSameUtcDay(a, b) {
  return a.toISOString().slice(0, 10) === b.toISOString().slice(0, 10);
}

function ensureDailyCounter(map, dateKey) {
  if (!map[dateKey]) {
    map[dateKey] = { arrivals: 0, presences: 0, guests_present: 0 };
  }
  return map[dateKey];
}

function normalizeLabel(value) {
  if (!value) return '';
  const trimmed = String(value).trim();
  if (!trimmed) return '';
  return trimmed
    .toLowerCase()
    .split(/\s+/)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function firstNonEmpty() {
  for (const value of arguments) {
    if (value != null && String(value).trim()) return String(value).trim();
  }
  return '';
}

function uniqueList(list) {
  return Array.from(new Set(list.filter(Boolean)));
}

function decideExportOutcome({ config, effectiveRequiresOpenClose, preview }) {
  if (!config.codice_struttura) return 'DRAFT';
  if (effectiveRequiresOpenClose && preview.summary.total_presenze === 0) return 'DRAFT';
  return 'EXPORT_OK';
}

async function insertAuditLog(supabase, payload) {
  const { error } = await supabase.from('audit_log').insert({
    user_email: payload.user_email,
    action: payload.action,
    table_name: payload.table_name,
    record_id: payload.record_id,
    timestamp: new Date().toISOString(),
  });

  if (error) {
    console.error('[build-istat-monthly] audit_log error', error);
  }
}

function jsonResponse(statusCode, body) {
  return {
    statusCode,
    headers: { 'Content-Type': 'application/json; charset=utf-8' },
    body: JSON.stringify(body),
  };
}
