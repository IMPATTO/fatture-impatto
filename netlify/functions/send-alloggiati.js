// netlify/functions/send-alloggiati.js
// Invia schedine al web service SOAP di AlloggiatiWeb
// Flusso: GenerateToken → Test (validazione) → Send (invio reale)
//
// Variabili d'ambiente richieste:
//   SUPABASE_URL
//   SUPABASE_SERVICE_ROLE_KEY

const { createClient } = require('@supabase/supabase-js');

const ENDPOINT = 'https://alloggiatiweb.poliziadistato.it/service/service.asmx';

// ── Security: Rate limiting ──
const rateLimits = {};
function checkRateLimit(userId, maxPerHour = 60) {
  const now = Date.now();
  if (!rateLimits[userId]) rateLimits[userId] = [];
  rateLimits[userId] = rateLimits[userId].filter(t => now - t < 3600000);
  if (rateLimits[userId].length >= maxPerHour) return false;
  rateLimits[userId].push(now);
  return true;
}

exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: JSON.stringify({ error: 'Method not allowed' }) };
  }

  const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_ROLE_KEY
  );

  const authHeader = event.headers.authorization || event.headers.Authorization;
  if (!authHeader?.startsWith('Bearer ')) {
    return { statusCode: 401, body: JSON.stringify({ error: 'Unauthorized' }) };
  }
  const { data: { user }, error: userErr } = await supabase.auth.getUser(authHeader.replace('Bearer ', ''));
  if (userErr || !user) {
    return { statusCode: 401, body: JSON.stringify({ error: 'Unauthorized' }) };
  }

  if (!checkRateLimit(user.id, 60)) {
    return { statusCode: 429, body: JSON.stringify({ error: 'Troppe richieste. Riprova tra qualche minuto.' }) };
  }

  let body;
  try { body = JSON.parse(event.body); } catch { return { statusCode: 400, body: JSON.stringify({ error: 'Invalid JSON' }) }; }

  const { ospiti_ids, mode = 'test' } = body;

  if (!ospiti_ids || !Array.isArray(ospiti_ids) || ospiti_ids.length === 0) {
    return { statusCode: 400, body: JSON.stringify({ error: 'ospiti_ids richiesto (array di UUID)' }) };
  }

  try {
    const { data: ospiti, error: ospErr } = await supabase
      .from('ospiti_check_in')
      .select('*, apartments(nome_appartamento)')
      .in('id', ospiti_ids);

    if (ospErr || !ospiti?.length) {
      return { statusCode: 404, body: JSON.stringify({ error: 'Ospiti non trovati', detail: ospErr?.message }) };
    }

    const aptIds = [...new Set(ospiti.map(o => o.apartment_id))];
    if (aptIds.length > 1) {
      return { statusCode: 400, body: JSON.stringify({ error: 'Tutti gli ospiti devono appartenere allo stesso appartamento' }) };
    }
    const apartmentId = aptIds[0];

    const { data: link } = await supabase
      .from('apartment_alloggiati')
      .select('*, alloggiati_accounts(*)')
      .eq('apartment_id', apartmentId)
      .maybeSingle();

    if (!link || !link.alloggiati_accounts) {
      return { statusCode: 400, body: JSON.stringify({ error: 'Appartamento non collegato a nessun account AlloggiatiWeb' }) };
    }

    const account = link.alloggiati_accounts;
    if (!account.attivo) {
      return { statusCode: 400, body: JSON.stringify({ error: 'Account AlloggiatiWeb disattivato' }) };
    }

    // Genera token
    const tokenResult = await soapGenerateToken(account.username, account.password_encrypted, account.wskey);
    if (tokenResult.error) {
      await supabase.from('alloggiati_accounts').update({ ultimo_errore: tokenResult.error }).eq('id', account.id);
      return { statusCode: 401, body: JSON.stringify({ error: 'Autenticazione AlloggiatiWeb fallita', detail: tokenResult.error }) };
    }

    // Costruisci tracciato record (168 caratteri)
    const schedine = ospiti.map(o => buildSchedina(o));

    // Validazione locale
    const validationErrors = [];
    ospiti.forEach((o) => {
      if (!o.sesso) validationErrors.push(`${o.nome} ${o.cognome}: sesso mancante`);
      if (!o.data_nascita) validationErrors.push(`${o.nome} ${o.cognome}: data di nascita mancante`);
      if (!o.cittadinanza_codice) validationErrors.push(`${o.nome} ${o.cognome}: codice cittadinanza mancante`);
      if (!o.data_checkin) validationErrors.push(`${o.nome} ${o.cognome}: data check-in mancante`);
      const tipo = parseInt(o.tipo_alloggiato) || 16;
      if ([16, 17, 18].includes(tipo)) {
        if (!o.tipo_documento_codice) validationErrors.push(`${o.nome} ${o.cognome}: tipo documento mancante`);
        if (!o.numero_documento) validationErrors.push(`${o.nome} ${o.cognome}: numero documento mancante`);
      }
    });

    if (validationErrors.length > 0) {
      return { statusCode: 400, body: JSON.stringify({ error: 'Dati incompleti', validationErrors }) };
    }

    // Invio SOAP
    const soapAction = mode === 'send' ? 'Send' : 'Test';
    const result = await soapSendOrTest(soapAction, account.username, tokenResult.token, schedine);

    const esito = result.error ? 'ERRORE' : (result.schedineValide === ospiti.length ? 'OK' : 'PARZIALE');

    if (mode === 'send' && !result.error) {
      await supabase
        .from('ospiti_check_in')
        .update({
          alloggiati_stato: 'INVIATA',
          alloggiati_inviata_at: new Date().toISOString(),
          alloggiati_errore: null
        })
        .in('id', ospiti_ids);

      await supabase.from('alloggiati_accounts').update({
        ultimo_invio_ok: new Date().toISOString(),
        ultimo_errore: null
      }).eq('id', account.id);
    } else if (result.error) {
      await supabase
        .from('ospiti_check_in')
        .update({
          alloggiati_stato: 'ERRORE',
          alloggiati_errore: result.error
        })
        .in('id', ospiti_ids);
    }

    await supabase.from('alloggiati_invii').insert({
      apartment_id: apartmentId,
      alloggiati_account_id: account.id,
      tipo: 'SOAP',
      data_riferimento: ospiti[0].data_checkin,
      ospiti_ids: ospiti_ids,
      num_schedine: ospiti.length,
      esito,
      errore_dettaglio: result.error || (result.dettaglio?.length ? JSON.stringify(result.dettaglio) : null),
      ricevuta_id: result.ricevutaId || null,
      payload_inviato: JSON.stringify({ action: soapAction, num_schedine: schedine.length, apartment: apartmentId }),
      risposta_ricevuta: maskSensitiveData(result.rawResponse?.substring(0, 2000) || ''),
      inviato_da: user.email,
    });

    await supabase.from('audit_log').insert({
      user_email: user.email,
      action: mode === 'send' ? 'SEND_ALLOGGIATI' : 'TEST_ALLOGGIATI',
      table_name: 'alloggiati_invii',
      record_id: apartmentId,
      timestamp: new Date().toISOString()
    });

    return {
      statusCode: 200,
      body: JSON.stringify({
        success: !result.error,
        mode,
        schedineInviate: ospiti.length,
        schedineValide: result.schedineValide || 0,
        errore: result.error || null,
        dettaglio: result.dettaglio || [],
        account: account.nome_account,
        questura: account.questura,
      })
    };

  } catch (err) {
    console.error('send-alloggiati error:', err);
    return { statusCode: 500, body: JSON.stringify({ error: 'Errore interno', detail: err.message }) };
  }
};


// ── SOAP: GenerateToken ──
async function soapGenerateToken(utente, password, wskey) {
  const xml = `<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:all="AlloggiatiService">
  <soap:Body>
    <all:GenerateToken>
      <all:Utente>${escXml(utente)}</all:Utente>
      <all:Password>${escXml(password)}</all:Password>
      <all:WsKey>${escXml(wskey)}</all:WsKey>
    </all:GenerateToken>
  </soap:Body>
</soap:Envelope>`;

  const res = await fetch(ENDPOINT, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/soap+xml; charset=utf-8',
      'SOAPAction': 'AlloggiatiService/GenerateToken',
    },
    body: xml,
  });

  const text = await res.text();
  console.log('GenerateToken response:', maskSensitiveData(text.substring(0, 800)));

  const tokenMatch = text.match(/<token>([\s\S]*?)<\/token>/);
  const errorMatch = text.match(/<ErroreDettaglio>([\s\S]*?)<\/ErroreDettaglio>/);

  if (tokenMatch?.[1]?.trim()) {
    return { token: tokenMatch[1].trim(), error: null };
  }
  return { token: null, error: errorMatch?.[1]?.trim() || 'Token non ricevuto' };
}


// ── SOAP: Test / Send ──
async function soapSendOrTest(action, utente, token, schedine) {
  const schedineXml = schedine.map(s => `<all:string>${escXml(s)}</all:string>`).join('\n        ');

  const xml = `<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:all="AlloggiatiService">
  <soap:Body>
    <all:${action}>
      <all:Utente>${escXml(utente)}</all:Utente>
      <all:token>${escXml(token)}</all:token>
      <all:ElencoSchedine>
        ${schedineXml}
      </all:ElencoSchedine>
    </all:${action}>
  </soap:Body>
</soap:Envelope>`;

  const res = await fetch(ENDPOINT, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/soap+xml; charset=utf-8',
      'SOAPAction': `AlloggiatiService/${action}`,
    },
    body: xml,
  });

  const text = await res.text();
  console.log(`${action} response:`, text.substring(0, 1000));

  const valideMatch = text.match(/<SchedineValide>(\d+)<\/SchedineValide>/);
  const errorMatches = text.match(/<ErroreDettaglio>([\s\S]*?)<\/ErroreDettaglio>/g);

  const dettaglio = [];
  if (errorMatches) {
    errorMatches.forEach(m => {
      const val = m.match(/<ErroreDettaglio>([\s\S]*?)<\/ErroreDettaglio>/)?.[1]?.trim();
      if (val) dettaglio.push(val);
    });
  }

  const schedineValide = valideMatch ? parseInt(valideMatch[1]) : 0;
  const error = dettaglio.find(d => d.length > 0) || null;

  return { schedineValide, error: error || null, dettaglio, rawResponse: text };
}


// ── Tracciato record: 168 caratteri ──
function buildSchedina(ospite) {
  const tipo = String(ospite.tipo_alloggiato || 16).padStart(2, '0');
  const dataArr = formatDateIT(ospite.data_checkin);
  const perm = calcPermanenza(ospite.data_checkin, ospite.data_checkout);
  const cognome = pad(cleanStr(ospite.cognome || '').toUpperCase(), 50);
  const nome = pad(cleanStr(ospite.nome || '').toUpperCase(), 30);
  const sesso = ospite.sesso === 'M' ? '1' : ospite.sesso === 'F' ? '2' : '1';
  const dataNascita = formatDateIT(ospite.data_nascita);
  const isItaliano = ospite.cittadinanza_codice === '100000100';
  const comuneNascita = isItaliano && ospite.luogo_nascita_codice
    ? pad(ospite.luogo_nascita_codice.trim(), 9) : pad('', 9);
  const provNascita = isItaliano && ospite.luogo_nascita
    ? pad(extractProv(ospite.luogo_nascita), 2) : pad('', 2);
  const statoNascita = pad(ospite.cittadinanza_codice || '100000100', 9);
  const cittadinanza = pad(ospite.cittadinanza_codice || '100000100', 9);

  let riga = tipo + dataArr + perm + cognome + nome + sesso + dataNascita +
             comuneNascita + provNascita + statoNascita + cittadinanza;

  const tipoNum = parseInt(tipo);
  if ([16, 17, 18].includes(tipoNum)) {
    const tipoDoc = pad(ospite.tipo_documento_codice || 'IDENT', 5);
    const numDoc = pad(cleanStr(ospite.numero_documento || '').toUpperCase(), 20);
    const luogoRil = ospite.luogo_rilascio_codice
      ? pad(ospite.luogo_rilascio_codice.trim(), 9) : pad('', 9);
    riga += tipoDoc + numDoc + luogoRil;
  } else {
    riga += pad('', 34);
  }

  console.log(`Schedina ${ospite.cognome}: ${riga.length} chars`);
  return riga;
}


// ── Utilities ──
function pad(str, len) {
  const s = String(str || '');
  if (s.length >= len) return s.substring(0, len);
  return s + ' '.repeat(len - s.length);
}

function cleanStr(s) {
  return (s || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^\w\s\-']/g, '')
    .trim();
}

function formatDateIT(dateStr) {
  if (!dateStr) return '          ';
  const d = new Date(dateStr + 'T12:00:00');
  const gg = String(d.getDate()).padStart(2, '0');
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const aaaa = String(d.getFullYear());
  return `${gg}/${mm}/${aaaa}`;
}

function calcPermanenza(checkin, checkout) {
  if (!checkin || !checkout) return '01';
  const d1 = new Date(checkin + 'T12:00:00');
  const d2 = new Date(checkout + 'T12:00:00');
  const diff = Math.min(30, Math.max(1, Math.round((d2 - d1) / 86400000)));
  return String(diff).padStart(2, '0');
}

function extractProv(luogo) {
  if (!luogo) return '';
  const m = luogo.match(/\(([A-Z]{2})\)/);
  if (m) return m[1];
  if (luogo.length === 2 && /^[A-Z]{2}$/.test(luogo)) return luogo;
  return '';
}

function escXml(s) {
  return String(s || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

function maskSensitiveData(str) {
  if (!str) return '';
  return str
    .replace(/<Password>[\s\S]*?<\/Password>/gi, '<Password>***</Password>')
    .replace(/<WsKey>[\s\S]*?<\/WsKey>/gi, '<WsKey>***</WsKey>')
    .replace(/<token>[\s\S]*?<\/token>/gi, '<token>***</token>');
}
