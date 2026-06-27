import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { createClient } from '@supabase/supabase-js';

const ENDPOINT = 'https://alloggiatiweb.poliziadistato.it/service/service.asmx';
const ITALIA_CODE = '100000100';
const DEFAULT_SUPABASE_URL = 'https://tysxeikqbgebpfyblgeb.supabase.co';
const ELIGIBLE_STATI = ['APPROVATA', 'CREDENZIALI_INVIATE', 'BOZZA_CREATA', 'CHECK_IN_COMPLETATO'];
const ELIGIBLE_ALLOGGIATI_STATI = ['DA_INVIARE', 'ERRORE'];

async function main() {
  const { date, execute } = parseArgs(process.argv.slice(2));
  const cutoffDate = shiftIsoDate(date, -2);
  const env = loadRuntimeEnv();
  const supabase = createClient(
    normalizeSupabaseUrl(env.SUPABASE_URL || env.SUPABASE_RUNTIME_URL),
    env.SUPABASE_SERVICE_KEY || env.SUPABASE_SERVICE_ROLE_KEY,
  );

  const { data, error } = await supabase
    .from('ospiti_check_in')
    .select([
      'id',
      'apartment_id',
      'capogruppo_id',
      'alloggiati_stato',
      'alloggiati_errore',
      'alloggiati_inviata_at',
      'data_checkin',
      'data_checkout',
      'stato',
      'created_at',
      'updated_at',
      'nome',
      'cognome',
      'sesso',
      'data_nascita',
      'cittadinanza_codice',
      'stato_nascita_codice',
      'luogo_nascita',
      'luogo_nascita_codice',
      'tipo_documento_codice',
      'numero_documento',
      'luogo_rilascio_codice',
      'tipo_alloggiato',
      'apartments(nome_appartamento)',
    ].join(','))
    .lte('data_checkin', date)
    .in('stato', ELIGIBLE_STATI)
    .in('alloggiati_stato', ELIGIBLE_ALLOGGIATI_STATI)
    .order('data_checkin', { ascending: true })
    .order('created_at', { ascending: true });

  if (error) throw new Error(`Load ospiti_check_in failed: ${error.message}`);

  const groups = buildGroups(data || []);
  const olderGroups = groups.filter((group) => group.dataCheckin < cutoffDate);
  const recentGroups = groups.filter((group) => group.dataCheckin >= cutoffDate && group.dataCheckin <= date);

  const summary = {
    date,
    cutoff_date: cutoffDate,
    mode: execute ? 'execute' : 'dry-run',
    pending_groups: groups.length,
    pending_guests: groups.reduce((sum, group) => sum + group.rows.length, 0),
    older_than_48h_groups: olderGroups.length,
    recent_groups: recentGroups.length,
    older_groups: olderGroups.map(describeGroup),
    recent_groups: recentGroups.map(describeGroup),
  };

  if (!execute) {
    printSummary(summary);
    return;
  }

  const results = {
    discarded_older_groups: [],
    sent_successfully: [],
    corrected_data_arrivo_errata: [],
    missing_configurations: [],
    blocked_groups: [],
  };

  for (const group of olderGroups) {
    const reason = `Scartato dal recupero automatico: check-in ${group.dataCheckin} antecedente alla soglia 48h ${cutoffDate}.`;
    await updateGuestsState(supabase, group.rows.map((row) => row.id), {
      alloggiati_stato: 'NON_NECESSARIA',
      alloggiati_errore: reason,
    });
    results.discarded_older_groups.push({
      ...describeGroup(group),
      reason,
    });
  }

  for (const group of recentGroups) {
    const result = await processGroup({ supabase, env, group, today: date });
    if (result.kind === 'sent') {
      results.sent_successfully.push(result.group);
      if (result.correctedDataArrivoErrata) {
        results.corrected_data_arrivo_errata.push(result.group);
      }
      continue;
    }
    if (result.kind === 'missing_config') {
      results.missing_configurations.push(result.group);
      continue;
    }
    results.blocked_groups.push(result.group);
  }

  printSummary({
    ...summary,
    results,
  });
}

function parseArgs(args) {
  let date = null;
  let execute = false;
  for (const arg of args) {
    if (arg.startsWith('--date=')) date = arg.slice('--date='.length);
    if (arg === '--execute') execute = true;
  }
  if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    throw new Error('Uso: node scripts/recover-alloggiati-48h.mjs --date=YYYY-MM-DD [--execute]');
  }
  return { date, execute };
}

function loadRuntimeEnv() {
  const runtimePath = path.resolve('.netlify/runtime-env-cache.json');
  const env = { ...process.env };
  if (fs.existsSync(runtimePath)) {
    const cache = JSON.parse(fs.readFileSync(runtimePath, 'utf8'));
    const entries = cache?.contexts?.production || [];
    for (const item of entries) {
      const value = item?.values?.[0]?.value;
      if (value && !env[item.key]) env[item.key] = value;
    }
  }
  if (!env.SUPABASE_SERVICE_KEY && !env.SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error('SUPABASE_SERVICE_KEY o SUPABASE_SERVICE_ROLE_KEY mancante');
  }
  if (!env.SUPABASE_URL && !env.SUPABASE_RUNTIME_URL) {
    throw new Error('SUPABASE_URL o SUPABASE_RUNTIME_URL mancante');
  }
  return env;
}

function normalizeSupabaseUrl(value) {
  const normalized = String(value || '').trim();
  if (/^https?:\/\//i.test(normalized)) return normalized.replace(/\/+$/, '');
  return DEFAULT_SUPABASE_URL;
}

function buildGroups(rows) {
  const grouped = new Map();
  for (const row of rows) {
    const key = `${row.capogruppo_id || row.id}:${row.apartment_id}:${row.data_checkin}`;
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(row);
  }
  return [...grouped.entries()]
    .map(([key, list]) => {
      const rows = sortGuestsForAlloggiati(list, list.map((item) => item.id));
      const head = rows.find((row) => row.capogruppo_id === null) || rows[0];
      return {
        key,
        rows,
        head,
        apartmentId: head.apartment_id,
        apartmentName: head.apartments?.nome_appartamento || null,
        dataCheckin: head.data_checkin,
      };
    })
    .sort((a, b) => a.dataCheckin.localeCompare(b.dataCheckin) || a.key.localeCompare(b.key));
}

function describeGroup(group) {
  return {
    key: group.key,
    apartment_id: group.apartmentId,
    apartment_name: group.apartmentName,
    data_checkin: group.dataCheckin,
    guest_count: group.rows.length,
    guest_names: group.rows.map((row) => `${row.nome} ${row.cognome}`.trim()),
  };
}

async function processGroup({ supabase, env, group, today }) {
  const ospitiIds = group.rows.map((row) => row.id);
  const { data: link, error: linkError } = await supabase
    .from('apartment_alloggiati')
    .select('*, alloggiati_accounts(*)')
    .eq('apartment_id', group.apartmentId)
    .maybeSingle();

  if (linkError) {
    const message = `Load apartment_alloggiati failed: ${linkError.message}`;
    await markGuestsAlloggiatiError(supabase, ospitiIds, message);
    return blockedGroup(group, message);
  }

  if (!link?.alloggiati_accounts) {
    const message = 'Appartamento non collegato a nessun account AlloggiatiWeb';
    await markGuestsAlloggiatiError(supabase, ospitiIds, message);
    return missingConfigGroup(group, message);
  }

  const account = link.alloggiati_accounts;
  if (!account.attivo) {
    const message = 'Account AlloggiatiWeb disattivato';
    await markGuestsAlloggiatiError(supabase, ospitiIds, message);
    return missingConfigGroup(group, message);
  }

  const tokenResult = await soapGenerateToken(account.username, decrypt(account.password_encrypted, env.ENCRYPTION_KEY), account.wskey);
  if (tokenResult.error) {
    const message = `Autenticazione AlloggiatiWeb fallita: ${tokenResult.error}`;
    await supabase.from('alloggiati_accounts').update({ ultimo_errore: tokenResult.error }).eq('id', account.id);
    await markGuestsAlloggiatiError(supabase, ospitiIds, message);
    return blockedGroup(group, message);
  }

  const validationErrors = validateGuests(group.rows);
  if (validationErrors.length > 0) {
    const message = validationErrors.join(' | ');
    await markGuestsAlloggiatiError(supabase, ospitiIds, message);
    return blockedGroup(group, message);
  }

  const initialResult = await sendGroup({
    env,
    account,
    link,
    rows: group.rows,
    token: tokenResult.token,
    mode: 'Send',
    overrideArrivalDate: null,
  });

  let finalResult = initialResult;
  let correctedDataArrivoErrata = false;

  if (matchesDataArrivoErrata(initialResult.error)) {
    const retryResult = await sendGroup({
      env,
      account,
      link,
      rows: group.rows,
      token: tokenResult.token,
      mode: 'Send',
      overrideArrivalDate: today,
    });
    correctedDataArrivoErrata = !retryResult.error && retryResult.schedineValide === group.rows.length;
    finalResult = correctedDataArrivoErrata ? retryResult : {
      ...retryResult,
      error: retryResult.error || initialResult.error,
      attemptedDataArrivoRetry: true,
    };
  }

  const fullSuccess = !finalResult.error && finalResult.schedineValide === group.rows.length;
  const partialSuccess = !finalResult.error && finalResult.schedineValide > 0 && finalResult.schedineValide < group.rows.length;
  const partialMessage = partialSuccess
    ? `Invio parziale rilevato (${finalResult.schedineValide}/${group.rows.length}). Verificare il portale Alloggiati prima di ripetere l'operazione.`
    : null;

  if (fullSuccess) {
    await updateGuestsState(supabase, ospitiIds, {
      alloggiati_stato: 'INVIATA',
      alloggiati_inviata_at: new Date().toISOString(),
      alloggiati_errore: null,
    });
    await supabase
      .from('alloggiati_accounts')
      .update({
        ultimo_invio_ok: new Date().toISOString(),
        ultimo_errore: null,
      })
      .eq('id', account.id);
  } else {
    await markGuestsAlloggiatiError(supabase, ospitiIds, finalResult.error || partialMessage || 'Errore sconosciuto');
  }

  await logSendAttempt(supabase, {
    apartmentId: group.apartmentId,
    accountId: account.id,
    ospitiIds,
    dataRiferimento: group.dataCheckin,
    numSchedine: group.rows.length,
    esito: fullSuccess ? 'SEND_OK' : (partialSuccess ? 'SEND_PARZIALE' : 'SEND_ERRORE'),
    errore: finalResult.error || partialMessage || null,
    payload: JSON.stringify({
      action: finalResult.action,
      apartment_portal_id: finalResult.apartmentPortalId || null,
      corrected_data_arrivo_errata: correctedDataArrivoErrata,
      num_schedine: group.rows.length,
    }),
    risposta: maskSensitiveData(finalResult.rawResponse?.slice(0, 2000) || ''),
  });

  if (fullSuccess) {
    return {
      kind: 'sent',
      correctedDataArrivoErrata,
      group: {
        ...describeGroup(group),
        corrected_data_arrivo_errata: correctedDataArrivoErrata,
      },
    };
  }

  return blockedGroup(
    group,
    finalResult.error || partialMessage || 'Errore sconosciuto',
    finalResult.attemptedDataArrivoRetry ? { attempted_data_arrivo_retry: true } : {},
  );
}

function missingConfigGroup(group, reason) {
  return {
    kind: 'missing_config',
    group: {
      ...describeGroup(group),
      reason,
    },
  };
}

function blockedGroup(group, reason, extra = {}) {
  return {
    kind: 'blocked',
    group: {
      ...describeGroup(group),
      reason,
      ...extra,
    },
  };
}

async function sendGroup({ account, link, rows, token, mode, overrideArrivalDate }) {
  const schedine = rows.map((row) => buildSchedina(row, overrideArrivalDate));
  const soapRequest = buildSoapRequestConfig({
    baseAction: mode,
    username: account.username,
    token,
    schedine,
    apartmentPortalId: link.id_appartamento_portale,
  });

  let result = await soapSendOrTest(soapRequest);
  let fallbackResult = null;
  if (isAlloggiatiMethodMismatch(result.error)) {
    const fallbackSoapRequest = flipSoapRequestMode(soapRequest);
    if (fallbackSoapRequest) {
      fallbackResult = await soapSendOrTest(fallbackSoapRequest);
      if (!fallbackResult.error || fallbackResult.schedineValide > 0) {
        result = fallbackResult;
        soapRequest.action = fallbackSoapRequest.action;
        soapRequest.apartmentPortalId = fallbackSoapRequest.apartmentPortalId;
      }
    }
  }

  result = resolveAlloggiatiMethodError({
    result,
    fallbackResult,
    apartmentPortalId: link.id_appartamento_portale,
  });

  return {
    ...result,
    action: soapRequest.action,
    apartmentPortalId: soapRequest.apartmentPortalId,
  };
}

function validateGuests(rows) {
  const validationErrors = [];
  for (const guest of rows) {
    if (!guest.sesso || !['M', 'F'].includes(guest.sesso)) {
      validationErrors.push(`${guest.nome} ${guest.cognome}: sesso non valido`);
    }
    if (!guest.data_nascita) validationErrors.push(`${guest.nome} ${guest.cognome}: data di nascita mancante`);
    if (!guest.cittadinanza_codice) validationErrors.push(`${guest.nome} ${guest.cognome}: codice cittadinanza mancante`);
    if (!guest.stato_nascita_codice) validationErrors.push(`${guest.nome} ${guest.cognome}: stato nascita mancante`);
    if (guest.stato_nascita_codice === ITALIA_CODE && !guest.luogo_nascita_codice) {
      validationErrors.push(`${guest.nome} ${guest.cognome}: comune nascita mancante`);
    }
    if (!guest.data_checkin) validationErrors.push(`${guest.nome} ${guest.cognome}: data check-in mancante`);
    const tipo = Number(guest.tipo_alloggiato || 16);
    if ([16, 17, 18].includes(tipo)) {
      if (!guest.tipo_documento_codice || !['IDENT', 'PASOR', 'PATEN'].includes(guest.tipo_documento_codice)) {
        validationErrors.push(`${guest.nome} ${guest.cognome}: tipo documento non valido`);
      }
      if (!guest.numero_documento) validationErrors.push(`${guest.nome} ${guest.cognome}: numero documento mancante`);
      if (!guest.luogo_rilascio_codice) validationErrors.push(`${guest.nome} ${guest.cognome}: luogo rilascio documento mancante`);
    }
  }
  return validationErrors;
}

async function updateGuestsState(supabase, ids, payload) {
  const { error } = await supabase
    .from('ospiti_check_in')
    .update(payload)
    .in('id', ids);
  if (error) throw new Error(`Update ospiti_check_in failed: ${error.message}`);
}

async function markGuestsAlloggiatiError(supabase, ids, message) {
  await updateGuestsState(supabase, ids, {
    alloggiati_stato: 'ERRORE',
    alloggiati_errore: message,
  });
}

async function logSendAttempt(supabase, {
  apartmentId,
  accountId,
  ospitiIds,
  dataRiferimento,
  numSchedine,
  esito,
  errore,
  payload,
  risposta,
}) {
  const { error } = await supabase.from('alloggiati_invii').insert({
    apartment_id: apartmentId,
    alloggiati_account_id: accountId,
    tipo: 'SOAP',
    data_riferimento: dataRiferimento,
    ospiti_ids: ospitiIds,
    num_schedine: numSchedine,
    esito,
    errore_dettaglio: errore,
    payload_inviato: payload,
    risposta_ricevuta: risposta,
    inviato_da: 'automation:recover-alloggiati-48h',
  });
  if (error) {
    console.error('[recover-alloggiati-48h] insert alloggiati_invii failed:', error.message);
  }
}

function sortGuestsForAlloggiati(ospiti, requestedIds) {
  const requestedOrder = new Map((requestedIds || []).map((id, index) => [id, index]));
  return [...ospiti].sort((a, b) => {
    const aTipo = Number(a.tipo_alloggiato || 16);
    const bTipo = Number(b.tipo_alloggiato || 16);
    const aMain = [16, 17, 18].includes(aTipo) ? 0 : 1;
    const bMain = [16, 17, 18].includes(bTipo) ? 0 : 1;
    if (aMain !== bMain) return aMain - bMain;

    const aForeign = a.stato_nascita_codice && a.stato_nascita_codice !== ITALIA_CODE ? 1 : 0;
    const bForeign = b.stato_nascita_codice && b.stato_nascita_codice !== ITALIA_CODE ? 1 : 0;
    if (aForeign !== bForeign) return aForeign - bForeign;

    const aRequested = requestedOrder.has(a.id) ? requestedOrder.get(a.id) : Number.MAX_SAFE_INTEGER;
    const bRequested = requestedOrder.has(b.id) ? requestedOrder.get(b.id) : Number.MAX_SAFE_INTEGER;
    if (aRequested !== bRequested) return aRequested - bRequested;

    return String(a.id).localeCompare(String(b.id));
  });
}

function decrypt(encryptedStr, encryptionKey) {
  if (!encryptedStr || !encryptedStr.includes(':')) return encryptedStr;
  try {
    const key = Buffer.from(String(encryptionKey || ''), 'hex');
    const [ivHex, authTagHex, ciphertext] = encryptedStr.split(':');
    const iv = Buffer.from(ivHex, 'hex');
    const authTag = Buffer.from(authTagHex, 'hex');
    const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv);
    decipher.setAuthTag(authTag);
    let decrypted = decipher.update(ciphertext, 'hex', 'utf8');
    decrypted += decipher.final('utf8');
    return decrypted;
  } catch {
    return encryptedStr;
  }
}

async function soapGenerateToken(utente, password, wskey) {
  const xml = `<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GenerateToken xmlns="AlloggiatiService">
      <Utente>${escXml(utente)}</Utente>
      <Password>${escXml(password)}</Password>
      <WsKey>${escXml(wskey)}</WsKey>
      <r><ErroreDettaglio></ErroreDettaglio></r>
    </GenerateToken>
  </soap:Body>
</soap:Envelope>`;

  const response = await fetch(ENDPOINT, {
    method: 'POST',
    headers: {
      'Content-Type': 'text/xml; charset=utf-8',
      SOAPAction: 'AlloggiatiService/GenerateToken',
    },
    body: xml,
  });
  const text = await response.text();
  const tokenMatch = text.match(/<token>(.*?)<\/token>/);
  const errorMatch = text.match(/<ErroreDettaglio>(.*?)<\/ErroreDettaglio>/);
  if (tokenMatch?.[1]) return { token: tokenMatch[1], error: null };
  return { token: null, error: errorMatch?.[1] || 'Token non ricevuto - risposta sconosciuta' };
}

function buildSoapRequestConfig({ baseAction, username, token, schedine, apartmentPortalId }) {
  const normalizedApartmentPortalId = String(apartmentPortalId || '').trim();
  const useApartmentManagement = normalizedApartmentPortalId.length > 0;
  return {
    action: useApartmentManagement ? `GestioneAppartamenti_${baseAction}` : baseAction,
    username,
    token,
    schedine,
    apartmentPortalId: useApartmentManagement ? normalizedApartmentPortalId : '',
  };
}

function isAlloggiatiMethodMismatch(errorMessage) {
  const message = String(errorMessage || '').toLowerCase();
  return message.includes('utilizzare i metodi appropriati')
    || message.includes('utente gestore di appartamenti');
}

function resolveAlloggiatiMethodError({ result, fallbackResult, apartmentPortalId }) {
  if (!isAlloggiatiMethodMismatch(result?.error) && !isAlloggiatiMethodMismatch(fallbackResult?.error)) {
    return result;
  }
  const normalizedApartmentPortalId = String(apartmentPortalId || '').trim();
  if (!normalizedApartmentPortalId) {
    return {
      ...result,
      error: 'Account Alloggiati di tipo Gestore Appartamenti: manca ID portale Alloggiati nel collegamento appartamento.',
    };
  }
  return {
    ...result,
    error: 'Account Alloggiati di tipo Gestore Appartamenti: ID portale Alloggiati mancante o non valido per questo appartamento.',
  };
}

function flipSoapRequestMode({ action, username, token, schedine, apartmentPortalId }) {
  const baseAction = String(action || '').includes('Test') ? 'Test' : 'Send';
  const usingApartmentManagement = String(action || '').startsWith('GestioneAppartamenti_');
  const normalizedApartmentPortalId = String(apartmentPortalId || '').trim();
  if (usingApartmentManagement) {
    return { action: baseAction, username, token, schedine, apartmentPortalId: '' };
  }
  if (!normalizedApartmentPortalId) return null;
  return {
    action: `GestioneAppartamenti_${baseAction}`,
    username,
    token,
    schedine,
    apartmentPortalId: normalizedApartmentPortalId,
  };
}

async function soapSendOrTest({ action, username, token, schedine, apartmentPortalId = '' }) {
  const schedineXml = schedine.map((item) => `<string>${escXml(item)}</string>`).join('\n        ');
  const apartmentPortalIdXml = apartmentPortalId
    ? `\n      <IdAppartamento>${escXml(apartmentPortalId)}</IdAppartamento>`
    : '';

  const xml = `<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <${action} xmlns="AlloggiatiService">
      <Utente>${escXml(username)}</Utente>
      <token>${escXml(token)}</token>
      <ElencoSchedine>
        ${schedineXml}
      </ElencoSchedine>
      ${apartmentPortalIdXml}
      <r>
        <SchedineValide>0</SchedineValide>
        <Dettaglio></Dettaglio>
      </r>
    </${action}>
  </soap:Body>
</soap:Envelope>`;

  const response = await fetch(ENDPOINT, {
    method: 'POST',
    headers: {
      'Content-Type': 'text/xml; charset=utf-8',
      SOAPAction: `AlloggiatiService/${action}`,
    },
    body: xml,
  });
  const text = await response.text();
  const valideMatch = text.match(/<SchedineValide>(\d+)<\/SchedineValide>/);
  const errorMatch = text.match(/<ErroreDettaglio>(.*?)<\/ErroreDettaglio>/g);
  const mainError = text.match(new RegExp(`<${action}Result>.*?<ErroreDettaglio>(.*?)<\\/ErroreDettaglio>.*?<\\/${action}Result>`, 's'));
  const dettaglio = [];
  for (const match of errorMatch || []) {
    const value = match.match(/<ErroreDettaglio>(.*?)<\/ErroreDettaglio>/)?.[1];
    if (value && value.trim()) dettaglio.push(value);
  }
  const schedineValide = valideMatch ? Number.parseInt(valideMatch[1], 10) : 0;
  const error = mainError?.[1]?.trim() || (schedineValide === 0 && dettaglio.length ? dettaglio[0] : null);
  return {
    schedineValide,
    error: error || null,
    dettaglio,
    rawResponse: text,
  };
}

function matchesDataArrivoErrata(message) {
  return String(message || '').toLowerCase().includes('data di arrivo errata');
}

function buildSchedina(ospite, overrideArrivalDate = null) {
  const tipo = String(ospite.tipo_alloggiato || 16).padStart(2, '0');
  const dataArr = formatDateIT(overrideArrivalDate || ospite.data_checkin);
  const perm = calcPermanenza(overrideArrivalDate || ospite.data_checkin, ospite.data_checkout);
  const cognome = pad(cleanStr(ospite.cognome || '').toUpperCase(), 50);
  const nome = pad(cleanStr(ospite.nome || '').toUpperCase(), 30);
  const sesso = ospite.sesso === 'M' ? '1' : ospite.sesso === 'F' ? '2' : '1';
  const dataNascita = formatDateIT(ospite.data_nascita);

  const codiceStatoNascita = ospite.stato_nascita_codice || ospite.cittadinanza_codice || ITALIA_CODE;
  const natoInItalia = codiceStatoNascita === ITALIA_CODE;
  const comuneNascita = natoInItalia && ospite.luogo_nascita_codice ? pad(ospite.luogo_nascita_codice, 9) : pad('', 9);
  const provNascita = natoInItalia && ospite.luogo_nascita ? pad(extractProv(ospite.luogo_nascita), 2) : pad('', 2);
  const statoNascita = pad(codiceStatoNascita, 9);
  const cittadinanza = pad(ospite.cittadinanza_codice || ITALIA_CODE, 9);

  let riga = tipo + dataArr + perm + cognome + nome + sesso + dataNascita
    + comuneNascita + provNascita + statoNascita + cittadinanza;

  const tipoNum = Number.parseInt(tipo, 10);
  if ([16, 17, 18].includes(tipoNum)) {
    const tipoDoc = pad(ospite.tipo_documento_codice || '', 5);
    const numDoc = pad(cleanStr(ospite.numero_documento || '').toUpperCase(), 20);
    const luogoRil = ospite.luogo_rilascio_codice ? pad(ospite.luogo_rilascio_codice, 9) : pad('', 9);
    riga += tipoDoc + numDoc + luogoRil;
  } else {
    riga += pad('', 34);
  }

  if (riga.length !== 168) {
    throw new Error(`Lunghezza riga errata: ${riga.length} invece di 168`);
  }
  return riga;
}

function pad(str, len) {
  const value = String(str || '');
  if (value.length >= len) return value.slice(0, len);
  return value + ' '.repeat(len - value.length);
}

function cleanStr(value) {
  return String(value || '').replace(/[^\w\s\-']/g, '').trim();
}

function formatDateIT(dateStr) {
  if (!dateStr) return '          ';
  const base = String(dateStr).trim().slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(base)) return '          ';
  const [yyyy, mm, dd] = base.split('-');
  return `${dd}/${mm}/${yyyy}`;
}

function calcPermanenza(checkin, checkout) {
  if (!checkin || !checkout) return '01';
  const c1 = String(checkin).trim().slice(0, 10);
  const c2 = String(checkout).trim().slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(c1) || !/^\d{4}-\d{2}-\d{2}$/.test(c2)) return '01';
  const d1 = new Date(`${c1}T12:00:00`);
  const d2 = new Date(`${c2}T12:00:00`);
  const diff = Math.max(1, Math.round((d2 - d1) / 86400000));
  return String(diff).padStart(2, '0');
}

function extractProv(luogo) {
  const match = String(luogo || '').match(/\(([A-Z]{2})\)/);
  if (match) return match[1];
  const trimmed = String(luogo || '').trim();
  if (/^[A-Z]{2}$/.test(trimmed)) return trimmed;
  return '';
}

function escXml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

function maskSensitiveData(value) {
  return String(value || '')
    .replace(/<Password>.*?<\/Password>/gi, '<Password>***</Password>')
    .replace(/<WsKey>.*?<\/WsKey>/gi, '<WsKey>***</WsKey>')
    .replace(/<token>.*?<\/token>/gi, '<token>***</token>');
}

function shiftIsoDate(isoDate, days) {
  const date = new Date(`${isoDate}T12:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function printSummary(summary) {
  console.log(`Verifica alloggiati ${summary.mode} per ${summary.date}`);
  console.log(`Soglia 48h: ${summary.cutoff_date}`);
  console.log(`Gruppi pendenti: ${summary.pending_groups}`);
  console.log(`Ospiti pendenti: ${summary.pending_guests}`);
  console.log(`Gruppi oltre 48h: ${summary.older_than_48h_groups}`);
  console.log(`Gruppi recenti: ${summary.recent_groups.length}`);
  console.log('');
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((error) => {
  const msg = String(error?.message || error || '');
  if (/fetch failed|ENOTFOUND|EAI_AGAIN|ECONNRESET|getaddrinfo/i.test(msg)) {
    console.error(`NETWORK_ERROR ${msg}`);
  } else {
    console.error(error);
  }
  process.exit(1);
});
