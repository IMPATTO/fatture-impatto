const { createClient } = require('@supabase/supabase-js');

const VALID_STAY_STATUSES = new Set([
  'APPROVATA',
  'CREDENZIALI_INVIATE',
  'BOZZA_CREATA',
  'CHECK_IN_COMPLETATO',
]);

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

  const validationError = validateInput(body);
  if (validationError) {
    return jsonResponse(400, { error: validationError });
  }

  const { apartment_id: apartmentId, date_from: dateFrom, date_to: dateTo } = body;

  try {
    const { data: config, error: configErr } = await supabase
      .from('apartment_istat_config')
      .select('id,regione,sistema,codice_struttura')
      .eq('apartment_id', apartmentId)
      .eq('attivo', true)
      .maybeSingle();

    if (configErr) {
      return jsonResponse(500, { error: 'Errore caricamento configurazione ISTAT', detail: configErr.message });
    }
    if (!config) {
      return jsonResponse(400, { error: 'Configurazione ISTAT attiva non trovata per questo appartamento' });
    }
    if (config.regione !== 'marche' || config.sistema !== 'istrice_ross1000') {
      return jsonResponse(400, { error: 'Export XML disponibile solo per Marche / Istrice-Ross1000' });
    }

    const { data: rows, error: rowsErr } = await supabase
      .from('ospiti_check_in')
      .select('id,nome,cognome,sesso,data_nascita,luogo_nascita,cittadinanza,tipo_documento,numero_documento,luogo_rilascio_documento,data_checkin,data_checkout,tipo_alloggiato,stato')
      .eq('apartment_id', apartmentId)
      .lte('data_checkin', dateTo)
      .gte('data_checkout', dateFrom)
      .order('data_checkin', { ascending: true });

    if (rowsErr) {
      return jsonResponse(500, { error: 'Errore lettura ospiti', detail: rowsErr.message });
    }

    const warnings = [];
    const warningFlags = new Set();
    const validRows = [];
    let skippedGuests = 0;

    for (const row of rows || []) {
      if (!isValidStayStatus(row.stato)) continue;

      const issues = validateGuestRow(row);
      if (issues.length) {
        warnings.push(`Ospite ${guestLabel(row)} escluso: ${issues.join(', ')}`);
        skippedGuests += 1;
        continue;
      }

      validRows.push(normalizeGuestRow(row, warningFlags, warnings));
    }

    const xml = buildMovimentiXml({
      codiceStruttura: config.codice_struttura || '',
      dateFrom,
      dateTo,
      guests: validRows,
      warningFlags,
      warnings,
    });

    const summary = {
      total: (rows || []).filter((row) => isValidStayStatus(row.stato)).length,
      valid: validRows.length,
      skipped: skippedGuests,
    };

    const esito = summary.skipped > 0 || summary.valid === 0 ? 'PARZIALE' : 'EXPORT_OK';

    const { error: logErr } = await supabase.from('istat_invii').insert({
      apartment_id: apartmentId,
      config_id: config.id,
      mese_riferimento: `${dateFrom.slice(0, 7)}-01`,
      regione: 'marche',
      sistema: 'istrice_ross1000',
      modalita: 'export',
      esito,
      payload_json: {
        summary,
        date_from: dateFrom,
        date_to: dateTo,
      },
      file_generato_path: null,
      risposta_portale: null,
      errore_dettaglio: warnings.length ? warnings.join(' | ') : null,
      inviato_da: user.email,
    });

    if (logErr) {
      console.error('[export-istat-marche-xml] istat_invii error', logErr);
    }

    return jsonResponse(200, {
      success: true,
      xml,
      summary,
      warnings,
    });
  } catch (err) {
    console.error('[export-istat-marche-xml] error:', err);
    return jsonResponse(500, { error: 'Errore interno', detail: err.message });
  }
};

function validateInput(body) {
  if (!body?.apartment_id || typeof body.apartment_id !== 'string') return 'apartment_id richiesto';
  if (!isIsoDate(body.date_from)) return 'date_from non valida';
  if (!isIsoDate(body.date_to)) return 'date_to non valida';
  if (body.date_to < body.date_from) return 'Intervallo date non valido';
  return null;
}

function isIsoDate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || ''));
}

function isValidStayStatus(status) {
  return VALID_STAY_STATUSES.has(String(status || ''));
}

function validateGuestRow(row) {
  const issues = [];
  if (!clean(row.nome)) issues.push('nome mancante');
  if (!clean(row.cognome)) issues.push('cognome mancante');
  if (!isIsoDate(row.data_nascita)) issues.push('data nascita mancante');
  if (!clean(row.numero_documento)) issues.push('documento mancante');
  return issues;
}

function normalizeGuestRow(row, warningFlags, warnings) {
  pushWarningOnce(warningFlags, warnings, 'tipo_documento', 'Tipo documento mappato per ipotesi verso valori XML gestionali.');
  pushWarningOnce(warningFlags, warnings, 'cittadinanza', 'Cittadinanza esportata come testo libero non codificato.');
  pushWarningOnce(warningFlags, warnings, 'comunerilascio', 'Comune rilascio esportato come testo libero non codificato.');

  return {
    cognome: clean(row.cognome),
    nome: clean(row.nome),
    sesso: normalizeSesso(row.sesso),
    datanascita: toCompactDate(row.data_nascita),
    cittadinanza: clean(row.cittadinanza),
    tipodocumento: normalizeDocumento(row.tipo_documento),
    numerodocumento: clean(row.numero_documento),
    comunerilascio: clean(row.luogo_rilascio_documento),
    dataarrivo: toCompactDate(row.data_checkin),
    datapartenza: toCompactDate(row.data_checkout),
    tipoalloggiato: normalizeTipoAlloggiato(row.tipo_alloggiato),
  };
}

function buildMovimentiXml({ codiceStruttura, dateFrom, dateTo, guests, warningFlags, warnings }) {
  const days = enumerateDays(dateFrom, dateTo);
  const arrivalsByDay = groupBy(guests, (guest) => guest.dataarrivo);
  const departuresByDay = groupBy(guests, (guest) => guest.datapartenza);

  pushWarningOnce(
    warningFlags,
    warnings,
    'camereoccupate',
    'camereoccupate non verificato sul tracciato Marche: esportato vuoto in attesa di test portale.'
  );
  pushWarningOnce(
    warningFlags,
    warnings,
    'root_codice',
    'Nodo root <codice> derivato da codice_struttura senza conferma del tracciato completo.'
  );
  pushWarningOnce(
    warningFlags,
    warnings,
    'root_prodotto',
    'Nodo root <prodotto> valorizzato come placeholder da confermare sul file reale completo.'
  );
  pushWarningOnce(
    warningFlags,
    warnings,
    'partenza_structure',
    'Struttura <partenza> allineata ad <arrivo> in attesa di conferma del tracciato ufficiale/XML reale con ospiti.'
  );

  const movimenti = days.map((day) => {
    const compactDay = day.replace(/-/g, '');
    const arrivals = arrivalsByDay[compactDay] || [];
    const departures = departuresByDay[compactDay] || [];

    return [
      '  <movimento>',
      `    <data>${compactDay}</data>`,
      '    <struttura>',
      '      <apertura>SI</apertura>',
      '      <camereoccupate></camereoccupate>',
      '      <cameredisponibili></cameredisponibili>',
      '      <lettidisponibili></lettidisponibili>',
      '    </struttura>',
      '    <arrivi>',
      arrivals.map((guest) => buildArrivoXml(guest)).join('\n'),
      '    </arrivi>',
      '    <partenze>',
      departures.map((guest) => buildPartenzaXml(guest)).join('\n'),
      '    </partenze>',
      '    <prenotazioni/>',
      '    <rettifiche/>',
      '  </movimento>',
    ].join('\n');
  });

  const metadata = [
    `  <codice>${escapeXml(codiceStruttura)}</codice>`,
    '  <prodotto>ISTRICE_ROSS1000</prodotto>'
  ].join('\n');

  return ['<?xml version="1.0" encoding="UTF-8"?>', '<movimenti>', metadata, movimenti.join('\n'), '</movimenti>'].join('\n');
}

function buildArrivoXml(guest) {
  return [
    '      <arrivo>',
    `        <cognome>${escapeXml(guest.cognome)}</cognome>`,
    `        <nome>${escapeXml(guest.nome)}</nome>`,
    `        <sesso>${escapeXml(guest.sesso)}</sesso>`,
    `        <datanascita>${escapeXml(guest.datanascita)}</datanascita>`,
    `        <cittadinanza>${escapeXml(guest.cittadinanza)}</cittadinanza>`,
    `        <tipodocumento>${escapeXml(guest.tipodocumento)}</tipodocumento>`,
    `        <numerodocumento>${escapeXml(guest.numerodocumento)}</numerodocumento>`,
    `        <comunerilascio>${escapeXml(guest.comunerilascio)}</comunerilascio>`,
    `        <dataarrivo>${escapeXml(guest.dataarrivo)}</dataarrivo>`,
    `        <datapartenza>${escapeXml(guest.datapartenza)}</datapartenza>`,
    `        <tipoalloggiato>${escapeXml(guest.tipoalloggiato)}</tipoalloggiato>`,
    '      </arrivo>',
  ].join('\n');
}

function buildPartenzaXml(guest) {
  // Struttura allineata ad arrivo in attesa di conferma dal tracciato import Marche.
  return [
    '      <partenza>',
    `        <cognome>${escapeXml(guest.cognome)}</cognome>`,
    `        <nome>${escapeXml(guest.nome)}</nome>`,
    `        <sesso>${escapeXml(guest.sesso)}</sesso>`,
    `        <datanascita>${escapeXml(guest.datanascita)}</datanascita>`,
    `        <cittadinanza>${escapeXml(guest.cittadinanza)}</cittadinanza>`,
    `        <tipodocumento>${escapeXml(guest.tipodocumento)}</tipodocumento>`,
    `        <numerodocumento>${escapeXml(guest.numerodocumento)}</numerodocumento>`,
    `        <comunerilascio>${escapeXml(guest.comunerilascio)}</comunerilascio>`,
    `        <dataarrivo>${escapeXml(guest.dataarrivo)}</dataarrivo>`,
    `        <datapartenza>${escapeXml(guest.datapartenza)}</datapartenza>`,
    `        <tipoalloggiato>${escapeXml(guest.tipoalloggiato)}</tipoalloggiato>`,
    '      </partenza>',
  ].join('\n');
}

function enumerateDays(dateFrom, dateTo) {
  const dates = [];
  const current = new Date(`${dateFrom}T00:00:00Z`);
  const end = new Date(`${dateTo}T00:00:00Z`);
  while (current <= end) {
    dates.push(current.toISOString().slice(0, 10));
    current.setUTCDate(current.getUTCDate() + 1);
  }
  return dates;
}

function groupBy(items, getKey) {
  return items.reduce((acc, item) => {
    const key = getKey(item);
    if (!acc[key]) acc[key] = [];
    acc[key].push(item);
    return acc;
  }, {});
}

function guestLabel(row) {
  return [row.nome, row.cognome].filter(Boolean).join(' ').trim() || row.id || 'senza-id';
}

function clean(value) {
  return value == null ? '' : String(value).trim();
}

function normalizeSesso(value) {
  return value === 'F' ? 'F' : 'M';
}

function normalizeDocumento(value) {
  const raw = clean(value).toUpperCase();
  if (!raw) return 'CARTA_IDENTITA';
  return raw
    .replaceAll("'", '')
    .replaceAll(' ', '_');
}

function normalizeTipoAlloggiato(value) {
  const code = Number(value);
  if (code === 17) return 'CAPOFAMIGLIA';
  if (code === 18) return 'CAPOGRUPPO';
  if (code === 19) return 'FAMILIARE';
  if (code === 20) return 'MEMBRO_GRUPPO';
  return 'OSPITE_SINGOLO';
}

function toCompactDate(value) {
  return String(value || '').slice(0, 10).replaceAll('-', '');
}

function escapeXml(value) {
  return String(value || '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&apos;');
}

function pushWarningOnce(flags, warnings, key, message) {
  if (flags.has(key)) return;
  flags.add(key);
  warnings.push(message);
}

function jsonResponse(statusCode, body) {
  return {
    statusCode,
    headers: { 'Content-Type': 'application/json; charset=utf-8' },
    body: JSON.stringify(body),
  };
}
