const { createClient } = require('@supabase/supabase-js');
const crypto = require('crypto');

const ENDPOINT = 'https://alloggiatiweb.poliziadistato.it/service/service.asmx';
const ITALIA_CODE = '100000100';
const ROME_TIMEZONE = 'Europe/Rome';
const DEFAULT_SUPABASE_URL = 'https://tysxeikqbgebpfyblgeb.supabase.co';
const ELIGIBLE_STATI = ['APPROVATA', 'CREDENZIALI_INVIATE', 'BOZZA_CREATA', 'CHECK_IN_COMPLETATO'];
const ELIGIBLE_ALLOGGIATI_STATI = ['DA_INVIARE', 'ERRORE'];
const SYSTEM_AUTO_SEND_ACTOR = 'system@auto-send-alloggiati';
const RESIDENCE_MARGHERITA_APARTMENT_ID = 'ca7641f6-82a4-40f3-85e6-149c39d46a03';
const RESIDENCE_MARGHERITA_WEEKLY_MIN_AUTO_SEND = 45;
const RESIDENCE_MARGHERITA_WEEKLY_MAX_AUTO_SEND = 55;
const AUTO_SEND_MAX_DELAY_DAYS = 2;

function normalizeSupabaseUrl(value) {
  const normalized = String(value || '').trim();
  if (/^https?:\/\//i.test(normalized)) return normalized.replace(/\/+$/, '');
  return DEFAULT_SUPABASE_URL;
}

exports.config = {
  schedule: '30 23 * * *',
};

exports.handler = async () => {
  const now = new Date();
  const romeNow = getRomeDateParts(now);
  const cutoffDate = shiftIsoDate(romeNow.date, -AUTO_SEND_MAX_DELAY_DAYS);

  const supabaseUrl = normalizeSupabaseUrl(process.env.SUPABASE_URL);
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
  if (!serviceRoleKey) {
    return json(500, { ok: false, error: 'Configurazione server non valida: SUPABASE_SERVICE_ROLE_KEY mancante' });
  }

  const supabase = createClient(supabaseUrl, serviceRoleKey);

  try {
    const { data: links, error: linksError } = await supabase
      .from('apartment_alloggiati')
      .select('*, alloggiati_accounts(*)')
      .eq('invio_automatico', true);

    if (linksError) {
      throw new Error(`Load apartment_alloggiati failed: ${linksError.message}`);
    }

    const validLinks = (links || []).filter(link => link.apartment_id);
    const apartmentIds = validLinks.map(link => link.apartment_id);

    if (!apartmentIds.length) {
      return json(200, {
        ok: true,
        skipped: true,
        reason: 'No apartments enabled for automatic send',
      });
    }

    const { data: ospiti, error: ospitiError } = await supabase
      .from('ospiti_check_in')
      .select('*, apartments(nome_appartamento)')
      .in('apartment_id', apartmentIds)
      .gte('data_checkin', cutoffDate)
      .lte('data_checkin', romeNow.date)
      .in('stato', ELIGIBLE_STATI)
      .in('alloggiati_stato', ELIGIBLE_ALLOGGIATI_STATI)
      .order('data_checkin', { ascending: true })
      .order('created_at', { ascending: true });

    if (ospitiError) {
      throw new Error(`Load ospiti_check_in failed: ${ospitiError.message}`);
    }

    const { data: olderPending, error: olderPendingError } = await supabase
      .from('ospiti_check_in')
      .select('id,nome,cognome,data_checkin,apartment_id,apartments(nome_appartamento)')
      .in('apartment_id', apartmentIds)
      .lt('data_checkin', cutoffDate)
      .in('stato', ELIGIBLE_STATI)
      .in('alloggiati_stato', ELIGIBLE_ALLOGGIATI_STATI)
      .order('data_checkin', { ascending: true })
      .order('created_at', { ascending: true });

    if (olderPendingError) {
      throw new Error(`Load older ospiti_check_in failed: ${olderPendingError.message}`);
    }

    const grouped = new Map();
    for (const row of ospiti || []) {
      if (!grouped.has(row.apartment_id)) grouped.set(row.apartment_id, []);
      grouped.get(row.apartment_id).push(row);
    }

    const summary = {
      ok: true,
      date: romeNow.date,
      processedApartments: 0,
      sentApartments: 0,
      sentGuests: 0,
      skippedApartments: 0,
      skippedGuestsWeeklyCap: 0,
      skippedGuestsOlderThan48h: 0,
      errors: [],
      weeklyCaps: [],
    };

    if ((olderPending || []).length) {
      const olderReason = buildOlderThan48hManualReason({
        cutoffDate,
        targetDate: romeNow.date,
      });
      await markGuestsForManualQueue(
        supabase,
        olderPending,
        olderReason
      );
      summary.skippedGuestsOlderThan48h = olderPending.length;
    }

    for (const link of validLinks) {
      const apartmentGuests = grouped.get(link.apartment_id) || [];
      if (!apartmentGuests.length) {
        summary.skippedApartments += 1;
        continue;
      }

      summary.processedApartments += 1;

      try {
        const capPlan = await planWeeklyAutoSendQuota(supabase, link, apartmentGuests, romeNow.date);
        if (capPlan.weeklyCapInfo) {
          summary.weeklyCaps.push(capPlan.weeklyCapInfo);
        }

        if (capPlan.manualGuests.length) {
          await markGuestsForManualQueue(supabase, capPlan.manualGuests, capPlan.manualReason);
          summary.skippedGuestsWeeklyCap += capPlan.manualGuests.length;
        }

        if (!capPlan.autoGuests.length) {
          summary.skippedApartments += 1;
          continue;
        }

        const result = await processApartmentSend(supabase, link, capPlan.autoGuests, SYSTEM_AUTO_SEND_ACTOR);
        if (result.success) {
          summary.sentApartments += 1;
          summary.sentGuests += capPlan.autoGuests.length;
        } else {
          summary.errors.push({
            apartment_id: link.apartment_id,
            apartment_name: capPlan.autoGuests[0]?.apartments?.nome_appartamento || null,
            error: result.error || 'Invio automatico fallito',
          });
        }
      } catch (error) {
        summary.errors.push({
          apartment_id: link.apartment_id,
          apartment_name: apartmentGuests[0]?.apartments?.nome_appartamento || null,
          error: error.message,
        });
      }
    }

    return json(200, summary);
  } catch (error) {
    console.error('[auto-send-alloggiati] fatal', error);
    return json(500, { ok: false, error: error.message });
  }
};

async function processApartmentSend(supabase, link, ospiti, actorEmail) {
  const ospitiIds = ospiti.map(item => item.id);
  const account = link?.alloggiati_accounts;

  if (!account) {
    const message = 'Appartamento non collegato a nessun account AlloggiatiWeb';
    await markGuestsAlloggiatiError(supabase, ospitiIds, message);
    await logAutoSend(supabase, {
      apartmentId: link.apartment_id,
      accountId: null,
      ospiti,
      ospitiIds,
      esito: 'SEND_ERRORE',
      errore: message,
      actorEmail,
    });
    return { success: false, error: message };
  }

  if (!account.attivo) {
    const message = 'Account AlloggiatiWeb disattivato';
    await markGuestsAlloggiatiError(supabase, ospitiIds, message);
    await logAutoSend(supabase, {
      apartmentId: link.apartment_id,
      accountId: account.id,
      ospiti,
      ospitiIds,
      esito: 'SEND_ERRORE',
      errore: message,
      actorEmail,
    });
    return { success: false, error: message };
  }

  const tokenResult = await soapGenerateToken(account.username, decrypt(account.password_encrypted), account.wskey);
  if (tokenResult.error) {
    const message = `Autenticazione AlloggiatiWeb fallita: ${tokenResult.error}`;
    await supabase.from('alloggiati_accounts').update({ ultimo_errore: tokenResult.error }).eq('id', account.id);
    await markGuestsAlloggiatiError(supabase, ospitiIds, message);
    await logAutoSend(supabase, {
      apartmentId: link.apartment_id,
      accountId: account.id,
      ospiti,
      ospitiIds,
      esito: 'SEND_ERRORE',
      errore: message,
      actorEmail,
    });
    return { success: false, error: message };
  }

  const validationErrors = [];
  ospiti.forEach((o) => {
    if (!o.sesso || !['M', 'F'].includes(o.sesso)) validationErrors.push(`${o.nome} ${o.cognome}: sesso non valido`);
    if (!o.data_nascita) validationErrors.push(`${o.nome} ${o.cognome}: data di nascita mancante`);
    if (!o.cittadinanza_codice) validationErrors.push(`${o.nome} ${o.cognome}: codice cittadinanza mancante`);
    if (!o.stato_nascita_codice) validationErrors.push(`${o.nome} ${o.cognome}: stato nascita mancante`);
    if (o.stato_nascita_codice === ITALIA_CODE && !o.luogo_nascita_codice) validationErrors.push(`${o.nome} ${o.cognome}: comune nascita mancante`);
    if (!o.data_checkin) validationErrors.push(`${o.nome} ${o.cognome}: data check-in mancante`);
    const tipo = Number(o.tipo_alloggiato || 16);
    if ([16, 17, 18].includes(tipo)) {
      if (!o.tipo_documento_codice || !['IDENT', 'PASOR', 'PATEN'].includes(o.tipo_documento_codice)) {
        validationErrors.push(`${o.nome} ${o.cognome}: tipo documento non valido`);
      }
      if (!o.numero_documento) validationErrors.push(`${o.nome} ${o.cognome}: numero documento mancante`);
      if (!o.luogo_rilascio_codice) validationErrors.push(`${o.nome} ${o.cognome}: luogo rilascio documento mancante`);
    }
  });

  if (validationErrors.length > 0) {
    const message = validationErrors.join(' | ');
    await markGuestsAlloggiatiError(supabase, ospitiIds, message);
    await logAutoSend(supabase, {
      apartmentId: link.apartment_id,
      accountId: account.id,
      ospiti,
      ospitiIds,
      esito: 'SEND_ERRORE',
      errore: message,
      actorEmail,
    });
    return { success: false, error: message };
  }

  let schedine;
  try {
    schedine = sortGuestsForAlloggiati(ospiti, ospitiIds).map(item => buildSchedina(item));
  } catch (error) {
    await markGuestsAlloggiatiError(supabase, ospitiIds, error.message);
    await logAutoSend(supabase, {
      apartmentId: link.apartment_id,
      accountId: account.id,
      ospiti,
      ospitiIds,
      esito: 'SEND_ERRORE',
      errore: error.message,
      actorEmail,
    });
    return { success: false, error: error.message };
  }

  const soapRequest = buildSoapRequestConfig({
    baseAction: 'Send',
    username: account.username,
    token: tokenResult.token,
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
        console.info('[auto-send-alloggiati] method fallback applied', {
          from: soapRequest.action,
          to: fallbackSoapRequest.action,
          apartmentPortalId: fallbackSoapRequest.apartmentPortalId || null,
        });
        result = fallbackResult;
      }
    }
  }
  result = resolveAlloggiatiMethodError({
    result,
    fallbackResult,
    apartmentPortalId: link.id_appartamento_portale,
  });
  const fullSuccess = !result.error && result.schedineValide === ospiti.length;
  const partialSuccess = !result.error && result.schedineValide > 0 && result.schedineValide < ospiti.length;
  const partialMessage = partialSuccess
    ? `Invio parziale rilevato (${result.schedineValide}/${ospiti.length}). Verificare il portale Alloggiati prima di ripetere l'operazione.`
    : null;

  if (fullSuccess) {
    await supabase
      .from('ospiti_check_in')
      .update({
        alloggiati_stato: 'INVIATA',
        alloggiati_inviata_at: new Date().toISOString(),
        alloggiati_errore: null
      })
      .in('id', ospitiIds);

    await supabase
      .from('alloggiati_accounts')
      .update({
        ultimo_invio_ok: new Date().toISOString(),
        ultimo_errore: null
      })
      .eq('id', account.id);
  } else {
    await markGuestsAlloggiatiError(supabase, ospitiIds, result.error || partialMessage || 'Errore sconosciuto');
  }

  await logAutoSend(supabase, {
    apartmentId: link.apartment_id,
    accountId: account.id,
    ospiti,
    ospitiIds,
    esito: fullSuccess ? 'SEND_OK' : (partialSuccess ? 'SEND_PARZIALE' : 'SEND_ERRORE'),
    errore: result.error || partialMessage || (result.dettaglio?.length ? JSON.stringify(result.dettaglio) : null),
    ricevutaId: result.ricevutaId || null,
    payload: JSON.stringify({
      action: soapRequest.action,
      num_schedine: schedine.length,
      apartment: link.apartment_id,
      apartment_portal_id: soapRequest.apartmentPortalId || null,
      automatico: true,
    }),
    risposta: maskSensitiveData(result.rawResponse?.substring(0, 2000) || ''),
    actorEmail,
  });

  return { success: fullSuccess, error: result.error || partialMessage || null };
}

async function planWeeklyAutoSendQuota(supabase, link, ospiti, targetDate) {
  if (link?.apartment_id !== RESIDENCE_MARGHERITA_APARTMENT_ID) {
    return {
      autoGuests: ospiti.slice(),
      manualGuests: [],
      manualReason: '',
      weeklyCapInfo: null,
    };
  }

  const weekStart = getIsoWeekStart(targetDate);
  const nextWeekStart = addIsoDays(weekStart, 7);
  const weeklyCap = getStableWeeklyAutoSendCap(link.apartment_id, weekStart);
  const alreadySent = await countApartmentWeeklyAutoSentGuests(supabase, {
    apartmentId: link.apartment_id,
    weekStart,
    nextWeekStart,
  });
  const remainingCapacity = Math.max(0, weeklyCap - alreadySent);
  const groupedBookings = buildApartmentBookingGroups(ospiti);

  const autoGuests = [];
  const manualGuests = [];
  let selectedGuests = 0;

  for (const bookingGroup of groupedBookings) {
    const groupSize = bookingGroup.rows.length;
    if (groupSize <= 0) continue;

    if (selectedGuests + groupSize <= remainingCapacity) {
      autoGuests.push(...bookingGroup.rows);
      selectedGuests += groupSize;
      continue;
    }

    manualGuests.push(...bookingGroup.rows);
  }

  const apartmentName = ospiti[0]?.apartments?.nome_appartamento || null;
  const finalSentCount = alreadySent + autoGuests.length;
  const manualReason = [
    'Esclusa dall invio automatico per tetto settimanale Residence Margherita.',
    `Settimana ${weekStart}.`,
    `Cap automatico ${weeklyCap} documenti.`,
    `Gia inviati ${alreadySent}.`,
    `Previsti in automatico questa notte ${autoGuests.length}.`,
    'Ripristina il record da archivio se vuoi inviarlo a mano dal backoffice documenti.',
  ].join(' ');

  return {
    autoGuests,
    manualGuests,
    manualReason,
    weeklyCapInfo: {
      apartment_id: link.apartment_id,
      apartment_name: apartmentName,
      week_start: weekStart,
      weekly_cap: weeklyCap,
      already_sent: alreadySent,
      auto_selected_now: autoGuests.length,
      manual_overflow_now: manualGuests.length,
      final_auto_total_if_successful: finalSentCount,
    },
  };
}

async function countApartmentWeeklyAutoSentGuests(supabase, {
  apartmentId,
  weekStart,
  nextWeekStart,
}) {
  const { data, error } = await supabase
    .from('alloggiati_invii')
    .select('num_schedine,created_at')
    .eq('apartment_id', apartmentId)
    .eq('inviato_da', SYSTEM_AUTO_SEND_ACTOR)
    .eq('esito', 'SEND_OK')
    .gte('created_at', `${addIsoDays(weekStart, -1)}T00:00:00.000Z`)
    .lt('created_at', `${addIsoDays(nextWeekStart, 1)}T00:00:00.000Z`);

  if (error) {
    throw new Error(`Load alloggiati_invii weekly cap failed: ${error.message}`);
  }

  return (data || []).reduce((sum, row) => {
    const romeCreatedDate = getRomeDateParts(new Date(row.created_at)).date;
    if (romeCreatedDate < weekStart || romeCreatedDate >= nextWeekStart) {
      return sum;
    }
    const count = Number(row?.num_schedine || 0);
    return sum + (Number.isFinite(count) ? count : 0);
  }, 0);
}

function buildApartmentBookingGroups(ospiti) {
  const groups = new Map();
  const requestedIds = (ospiti || []).map(item => item.id);

  for (const row of ospiti || []) {
    const key = `${row.capogruppo_id || row.id}:${row.apartment_id}:${row.data_checkin}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }

  return [...groups.values()]
    .map(rows => ({
      rows: sortGuestsForAlloggiati(rows, requestedIds),
      createdAt: rows.reduce((min, row) => {
        const createdAt = String(row?.created_at || '');
        if (!min) return createdAt;
        if (!createdAt) return min;
        return createdAt < min ? createdAt : min;
      }, ''),
    }))
    .sort((left, right) => {
      if (left.createdAt && right.createdAt && left.createdAt !== right.createdAt) {
        return left.createdAt.localeCompare(right.createdAt);
      }
      return String(left.rows[0]?.id || '').localeCompare(String(right.rows[0]?.id || ''));
    });
}

function getStableWeeklyAutoSendCap(apartmentId, weekStart) {
  const hash = crypto
    .createHash('sha256')
    .update(`${String(apartmentId || '')}:${String(weekStart || '')}`)
    .digest();
  const span = RESIDENCE_MARGHERITA_WEEKLY_MAX_AUTO_SEND - RESIDENCE_MARGHERITA_WEEKLY_MIN_AUTO_SEND + 1;
  const offset = hash[0] % span;
  return RESIDENCE_MARGHERITA_WEEKLY_MIN_AUTO_SEND + offset;
}

async function markGuestsForManualQueue(supabase, ospiti, message) {
  if (!Array.isArray(ospiti) || !ospiti.length) return;

  const ospitiIds = ospiti.map(item => item.id).filter(Boolean);
  if (!ospitiIds.length) return;

  const { error } = await supabase
    .from('ospiti_check_in')
    .update({
      alloggiati_stato: 'NON_NECESSARIA',
      alloggiati_errore: message,
    })
    .in('id', ospitiIds);

  if (error) {
    throw new Error(`Mark manual queue failed: ${error.message}`);
  }
}

function buildOlderThan48hManualReason({
  cutoffDate,
  targetDate,
}) {
  return [
    'Esclusa dall invio automatico: superata la finestra massima di 48 ore.',
    `Alla data ${targetDate} l automatico considera solo check-in dal ${cutoffDate} in poi.`,
    'Da gestire manualmente dal backoffice documenti.',
  ].join(' ');
}

async function logAutoSend(supabase, {
  apartmentId,
  accountId,
  ospiti,
  ospitiIds,
  esito,
  errore,
  ricevutaId = null,
  payload = null,
  risposta = null,
  actorEmail,
}) {
  const dataRiferimento = ospiti?.[0]?.data_checkin || null;

  const { error: invioError } = await supabase.from('alloggiati_invii').insert({
    apartment_id: apartmentId,
    alloggiati_account_id: accountId,
    tipo: 'SOAP',
    data_riferimento: dataRiferimento,
    ospiti_ids: ospitiIds,
    num_schedine: ospitiIds.length,
    esito,
    errore_dettaglio: errore,
    ricevuta_id: ricevutaId,
    payload_inviato: payload,
    risposta_ricevuta: risposta,
    inviato_da: actorEmail,
  });
  if (invioError) {
    console.error('[auto-send-alloggiati] insert alloggiati_invii error', invioError);
  }

  const { error: auditError } = await supabase.from('audit_log').insert({
    user_email: actorEmail,
    action: 'AUTO_SEND_ALLOGGIATI',
    table_name: 'alloggiati_invii',
    record_id: apartmentId,
    timestamp: new Date().toISOString()
  });
  if (auditError) {
    console.error('[auto-send-alloggiati] insert audit_log error', auditError);
  }
}

async function markGuestsAlloggiatiError(supabase, ospitiIds, message) {
  if (!Array.isArray(ospitiIds) || !ospitiIds.length || !message) return;
  const { error } = await supabase
    .from('ospiti_check_in')
    .update({
      alloggiati_stato: 'ERRORE',
      alloggiati_errore: message
    })
    .in('id', ospitiIds);
  if (error) {
    console.error('[auto-send-alloggiati] update alloggiati error state failed', error);
  }
}

function getRomeDateParts(date) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: ROME_TIMEZONE,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(date);
  const pick = (type) => parts.find(part => part.type === type)?.value || '';
  return {
    date: `${pick('year')}-${pick('month')}-${pick('day')}`,
    hour: pick('hour'),
    minute: pick('minute'),
  };
}

function getIsoWeekStart(isoDate) {
  const normalized = normalizeIsoDate(isoDate);
  const date = new Date(`${normalized}T12:00:00Z`);
  const day = date.getUTCDay() || 7;
  date.setUTCDate(date.getUTCDate() - (day - 1));
  return date.toISOString().slice(0, 10);
}

function addIsoDays(isoDate, days) {
  const normalized = normalizeIsoDate(isoDate);
  const date = new Date(`${normalized}T12:00:00Z`);
  date.setUTCDate(date.getUTCDate() + Number(days || 0));
  return date.toISOString().slice(0, 10);
}

function shiftIsoDate(isoDate, days) {
  return addIsoDays(isoDate, days);
}

function normalizeIsoDate(value) {
  const normalized = String(value || '').trim().slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
    throw new Error(`Data ISO non valida: ${value}`);
  }
  return normalized;
}

function json(statusCode, body) {
  return {
    statusCode,
    headers: { 'content-type': 'application/json; charset=utf-8' },
    body: JSON.stringify(body),
  };
}

function decrypt(encryptedStr) {
  if (!encryptedStr || !encryptedStr.includes(':')) return encryptedStr;
  try {
    const key = Buffer.from(process.env.ENCRYPTION_KEY || '', 'hex');
    const [ivHex, authTagHex, ciphertext] = encryptedStr.split(':');
    const iv = Buffer.from(ivHex, 'hex');
    const authTag = Buffer.from(authTagHex, 'hex');
    const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv);
    decipher.setAuthTag(authTag);
    let decrypted = decipher.update(ciphertext, 'hex', 'utf8');
    decrypted += decipher.final('utf8');
    return decrypted;
  } catch (error) {
    console.error('[auto-send-alloggiati] decrypt failed', error.message);
    return encryptedStr;
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

  const res = await fetch(ENDPOINT, {
    method: 'POST',
    headers: {
      'Content-Type': 'text/xml; charset=utf-8',
      'SOAPAction': 'AlloggiatiService/GenerateToken',
    },
    body: xml,
  });

  const text = await res.text();
  const tokenMatch = text.match(/<token>(.*?)<\/token>/);
  const errorMatch = text.match(/<ErroreDettaglio>(.*?)<\/ErroreDettaglio>/);

  if (tokenMatch && tokenMatch[1]) {
    return { token: tokenMatch[1], error: null };
  }

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
      error: 'Account Alloggiati di tipo Gestore Appartamenti: manca ID portale Alloggiati nel collegamento appartamento. Apri "Collega Appartamenti" e inserisci l\'ID portale corretto.',
    };
  }

  return {
    ...result,
    error: 'Account Alloggiati di tipo Gestore Appartamenti: ID portale Alloggiati mancante o non valido per questo appartamento. Verifica il collegamento in "Collega Appartamenti".',
  };
}

function flipSoapRequestMode({ action, username, token, schedine, apartmentPortalId }) {
  const baseAction = String(action || '').includes('Test') ? 'Test' : 'Send';
  const currentlyUsingApartmentManagement = String(action || '').startsWith('GestioneAppartamenti_');
  const normalizedApartmentPortalId = String(apartmentPortalId || '').trim();

  if (currentlyUsingApartmentManagement) {
    return {
      action: baseAction,
      username,
      token,
      schedine,
      apartmentPortalId: '',
    };
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
  const schedineXml = schedine.map(item => `<string>${escXml(item)}</string>`).join('\n        ');
  const apartmentPortalIdXml = apartmentPortalId
    ? `
      <IdAppartamento>${escXml(apartmentPortalId)}</IdAppartamento>`
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

  const res = await fetch(ENDPOINT, {
    method: 'POST',
    headers: {
      'Content-Type': 'text/xml; charset=utf-8',
      'SOAPAction': `AlloggiatiService/${action}`,
    },
    body: xml,
  });

  const text = await res.text();
  const valideMatch = text.match(/<SchedineValide>(\d+)<\/SchedineValide>/);
  const errorMatch = text.match(/<ErroreDettaglio>(.*?)<\/ErroreDettaglio>/g);
  const mainError = text.match(new RegExp(`<${action}Result>.*?<ErroreDettaglio>(.*?)<\/ErroreDettaglio>.*?<\/${action}Result>`, 's'));

  const dettaglio = [];
  if (errorMatch) {
    errorMatch.forEach(item => {
      const value = item.match(/<ErroreDettaglio>(.*?)<\/ErroreDettaglio>/)?.[1];
      if (value && value.trim()) dettaglio.push(value);
    });
  }

  const schedineValide = valideMatch ? parseInt(valideMatch[1], 10) : 0;
  const error = mainError?.[1]?.trim() || (schedineValide === 0 && dettaglio.length ? dettaglio[0] : null);

  return {
    schedineValide,
    error: error || null,
    dettaglio,
    rawResponse: text,
  };
}

function buildSchedina(ospite) {
  const tipo = String(ospite.tipo_alloggiato || 16).padStart(2, '0');
  const dataArr = formatDateIT(ospite.data_checkin);
  const perm = calcPermanenza(ospite.data_checkin, ospite.data_checkout);
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

  let riga = tipo + dataArr + perm + cognome + nome + sesso + dataNascita +
    comuneNascita + provNascita + statoNascita + cittadinanza;

  const tipoNum = parseInt(tipo, 10);
  if ([16, 17, 18].includes(tipoNum)) {
    const tipoDoc = pad(ospite.tipo_documento_codice || '', 5);
    const numDoc = pad(cleanStr(ospite.numero_documento || '').toUpperCase(), 20);
    const luogoRil = ospite.luogo_rilascio_codice ? pad(ospite.luogo_rilascio_codice, 9) : pad('', 9);
    riga += tipoDoc + numDoc + luogoRil;
  } else {
    riga += pad('', 34);
  }

  if (riga.length !== 168) {
    throw new Error(`Lunghezza riga errata: ${riga.length} invece di 168.`);
  }
  return riga;
}

function pad(str, len) {
  const value = String(str || '');
  if (value.length >= len) return value.substring(0, len);
  return value + ' '.repeat(len - value.length);
}

function cleanStr(value) {
  return (value || '').replace(/[^\w\s\-']/g, '').trim();
}

function formatDateIT(dateStr) {
  if (!dateStr) return '          ';
  const base = String(dateStr).trim().slice(0, 10);
  if (/^\d{4}-\d{2}-\d{2}$/.test(base)) {
    const [yyyy, mm, dd] = base.split('-');
    return `${dd}/${mm}/${yyyy}`;
  }
  return '          ';
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
  if (!luogo) return '';
  const match = luogo.match(/\(([A-Z]{2})\)/);
  if (match) return match[1];
  if (luogo.length === 2 && /^[A-Z]{2}$/.test(luogo)) return luogo;
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

function maskSensitiveData(str) {
  if (!str) return '';
  return str
    .replace(/<Password>.*?<\/Password>/gi, '<Password>***</Password>')
    .replace(/<WsKey>.*?<\/WsKey>/gi, '<WsKey>***</WsKey>')
    .replace(/<token>.*?<\/token>/gi, '<token>***</token>');
}
