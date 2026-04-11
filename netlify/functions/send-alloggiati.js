// netlify/functions/send-alloggiati.js
// Invia schedine al web service SOAP di AlloggiatiWeb
// Flusso: GenerateToken → Test (validazione) → Send (invio reale)
//
// Variabili d'ambiente richieste:
//   SUPABASE_URL
//   SUPABASE_SERVICE_ROLE_KEY
//   ENCRYPTION_KEY  (chiave AES-256 hex, 64 chars)

const { createClient } = require('@supabase/supabase-js');
const crypto = require('crypto');

// ── Cifratura AES-256-GCM ──
// Il valore cifrato in DB ha formato: iv_hex:authTag_hex:encrypted_hex
function encrypt(plaintext) {
  const key = Buffer.from(process.env.ENCRYPTION_KEY, 'hex');
  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  const encrypted = Buffer.concat([cipher.update(plaintext, 'utf8'), cipher.final()]);
  const authTag = cipher.getAuthTag();
  return iv.toString('hex') + ':' + authTag.toString('hex') + ':' + encrypted.toString('hex');
}

function decrypt(ciphertext) {
  // Se il valore non è nel formato cifrato (legacy in chiaro), lo restituisce as-is
  if (!ciphertext || !ciphertext.includes(':')) return ciphertext;
  const parts = ciphertext.split(':');
  if (parts.length !== 3) return ciphertext;
  const key = Buffer.from(process.env.ENCRYPTION_KEY, 'hex');
  const iv = Buffer.from(parts[0], 'hex');
  const authTag = Buffer.from(parts[1], 'hex');
  const encrypted = Buffer.from(parts[2], 'hex');
  const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv);
  decipher.setAuthTag(authTag);
  return decipher.update(encrypted) + decipher.final('utf8');
}

const ENDPOINT = 'https://alloggiatiweb.poliziadistato.it/service/service.asmx';

// ── Security: Rate limiting (in-memory, per-function instance) ──
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

  // Auth check
  const authHeader = event.headers.authorization || event.headers.Authorization;
  if (!authHeader?.startsWith('Bearer ')) {
    return { statusCode: 401, body: JSON.stringify({ error: 'Unauthorized' }) };
  }
  const { data: { user }, error: userErr } = await supabase.auth.getUser(authHeader.replace('Bearer ', ''));
  if (userErr || !user) {
    return { statusCode: 401, body: JSON.stringify({ error: 'Unauthorized' }) };
  }

  // Security: Rate limiting
  if (!checkRateLimit(user.id, 60)) {
    return { statusCode: 429, body: JSON.stringify({ error: 'Troppe richieste. Riprova tra qualche minuto.' }) };
  }

  let body;
  try { body = JSON.parse(event.body); } catch { return { statusCode: 400, body: JSON.stringify({ error: 'Invalid JSON' }) }; }

  const { ospiti_ids, mode = 'test' } = body;
  // mode: 'test' = solo validazione, 'send' = invio reale

  if (!ospiti_ids || !Array.isArray(ospiti_ids) || ospiti_ids.length === 0) {
    return { statusCode: 400, body: JSON.stringify({ error: 'ospiti_ids richiesto (array di UUID)' }) };
  }

  try {
    // 1. Recupera dati ospiti
    const { data: ospiti, error: ospErr } = await supabase
      .from('ospiti_check_in')
      .select('*, apartments(nome_appartamento)')
      .in('id', ospiti_ids);

    if (ospErr || !ospiti?.length) {
      return { statusCode: 404, body: JSON.stringify({ error: 'Ospiti non trovati', detail: ospErr?.message }) };
    }

    // 2. Verifica che tutti gli ospiti appartengano allo stesso appartamento
    const aptIds = [...new Set(ospiti.map(o => o.apartment_id))];
    if (aptIds.length > 1) {
      return { statusCode: 400, body: JSON.stringify({ error: 'Tutti gli ospiti devono appartenere allo stesso appartamento per un singolo invio' }) };
    }
    const apartmentId = aptIds[0];

    // 3. Recupera il collegamento appartamento → account
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

    // 4. Genera token — decifra la password prima dell'uso
    const passwordDecrypted = decrypt(account.password_encrypted);
    const tokenResult = await soapGenerateToken(account.username, passwordDecrypted, account.wskey);
    if (tokenResult.error) {
      // Aggiorna ultimo errore sull'account
      await supabase.from('alloggiati_accounts').update({ ultimo_errore: tokenResult.error }).eq('id', account.id);
      return { statusCode: 401, body: JSON.stringify({ error: 'Autenticazione AlloggiatiWeb fallita', detail: tokenResult.error }) };
    }

    // 5. Costruisci le stringhe schedina (tracciato record 220 caratteri)
    const schedine = ospiti.map(o => buildSchedina(o, link.id_appartamento_portale));

    // DEBUG: log lunghezza schedine
    schedine.forEach((s, i) => {
      console.log(`Schedina ${i+1}: ${s.length} chars — "${s}"`);
    });

    // 6. Validazione: controlla che nessuna schedina abbia errori di formato
    const validationErrors = [];
    ospiti.forEach((o, i) => {
      if (!o.sesso) validationErrors.push(`${o.nome} ${o.cognome}: sesso mancante`);
      if (!o.data_nascita) validationErrors.push(`${o.nome} ${o.cognome}: data di nascita mancante`);
      if (!o.cittadinanza_codice) validationErrors.push(`${o.nome} ${o.cognome}: codice cittadinanza mancante`);
      if (!o.data_checkin) validationErrors.push(`${o.nome} ${o.cognome}: data check-in mancante`);
      const tipo = o.tipo_alloggiato || 16;
      if ([16, 17, 18].includes(tipo)) {
        if (!o.tipo_documento_codice) validationErrors.push(`${o.nome} ${o.cognome}: tipo documento mancante`);
        if (!o.numero_documento) validationErrors.push(`${o.nome} ${o.cognome}: numero documento mancante`);
      }
    });

    if (validationErrors.length > 0) {
      return { statusCode: 400, body: JSON.stringify({ error: 'Dati incompleti', validationErrors }) };
    }

    // 7. Invio (Test o Send)
    const soapAction = mode === 'send' ? 'Send' : 'Test';
    const result = await soapSendOrTest(soapAction, account.username, tokenResult.token, schedine);

    // 8. Salva esito
    const esito = result.error ? 'ERRORE' : (result.schedineValide === ospiti.length ? 'OK' : 'PARZIALE');

    // Aggiorna stato ospiti
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

    // Log invio
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

    // Audit
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


// ──────────────────────────────────────────────────────
// SOAP: GenerateToken
// ──────────────────────────────────────────────────────
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

  // Parse token from response
  const tokenMatch = text.match(/<token>(.*?)<\/token>/);
  const errorMatch = text.match(/<ErroreDettaglio>(.*?)<\/ErroreDettaglio>/);

  if (tokenMatch && tokenMatch[1]) {
    return { token: tokenMatch[1], error: null };
  }

  return { token: null, error: errorMatch?.[1] || 'Token non ricevuto - risposta sconosciuta' };
}


// ──────────────────────────────────────────────────────
// SOAP: Test / Send
// ──────────────────────────────────────────────────────
async function soapSendOrTest(action, utente, token, schedine) {
  const schedineXml = schedine.map(s => `<string>${escXml(s)}</string>`).join('\n        ');

  const xml = `<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <${action} xmlns="AlloggiatiService">
      <Utente>${escXml(utente)}</Utente>
      <token>${escXml(token)}</token>
      <ElencoSchedine>
        ${schedineXml}
      </ElencoSchedine>
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
  console.log('SOAP response raw:', text.substring(0, 1000));
  const valideMatch = text.match(/<SchedineValide>(\d+)<\/SchedineValide>/);
  const errorMatch = text.match(/<ErroreDettaglio>(.*?)<\/ErroreDettaglio>/g);
  const mainError = text.match(new RegExp(`<${action}Result>.*?<ErroreDettaglio>(.*?)<\/ErroreDettaglio>.*?<\/${action}Result>`, 's'));

  const dettaglio = [];
  if (errorMatch) {
    errorMatch.forEach(m => {
      const val = m.match(/<ErroreDettaglio>(.*?)<\/ErroreDettaglio>/)?.[1];
      if (val && val.trim()) dettaglio.push(val);
    });
  }

  const schedineValide = valideMatch ? parseInt(valideMatch[1]) : 0;
  const error = mainError?.[1]?.trim() || (schedineValide === 0 && dettaglio.length ? dettaglio[0] : null);

  return {
    schedineValide,
    error: error || null,
    dettaglio,
    rawResponse: text,
  };
}


// ──────────────────────────────────────────────────────
// Tracciato record: 236 caratteri per ospite
// ──────────────────────────────────────────────────────
// Formato (per ospite singolo/capofamiglia/capogruppo = 236 chars):
//   Pos  Len  Campo
//   1    2    Tipo alloggiato (16/17/18/19/20)
//   3    10   Data arrivo (gg/mm/aaaa)
//   13   2    Permanenza (gg)
//   15   50   Cognome
//   65   30   Nome
//   95   1    Sesso (1=M, 2=F)
//   96   10   Data nascita (gg/mm/aaaa)
//   106  9    Comune nascita (codice ISTAT, o 9 spazi se estero)
//   115  2    Provincia nascita (sigla, o 2 spazi se estero)
//   117  9    Stato nascita (codice 9 char)
//   126  9    Cittadinanza (codice 9 char)
//   -- Solo per tipo 16/17/18 (104 chars), per 19/20 → 104 spazi --
//   135  5    Tipo documento (IDENT/PASOR/PATEN ecc)
//   140  20   Numero documento
//   160  9    Luogo rilascio doc - comune (o 9 spazi)
//   169  2    Luogo rilascio doc - provincia (o 2 spazi)
//   171  30   Indirizzo residenza
//   201  9    Comune residenza (codice ISTAT, o 9 spazi)
//   210  2    Provincia residenza (o 2 spazi)
//   212  9    Stato residenza (codice 9 char)
//   -- Per File Unico (multi-appartamento) si aggiungono 6 chars --
//   Non li aggiungiamo qui perché usiamo il metodo Send, non FileUnico

function buildSchedina(ospite, idAppartamento) {
  const tipo = String(ospite.tipo_alloggiato || 16).padStart(2, '0');

  // Data arrivo
  const dataArr = formatDateIT(ospite.data_checkin);

  // Permanenza (in giorni)
  const perm = calcPermanenza(ospite.data_checkin, ospite.data_checkout);

  // Cognome (50 chars)
  const cognome = pad(cleanStr(ospite.cognome || '').toUpperCase(), 50);

  // Nome (30 chars)
  const nome = pad(cleanStr(ospite.nome || '').toUpperCase(), 30);

  // Sesso (1=M, 2=F)
  const sesso = ospite.sesso === 'M' ? '1' : ospite.sesso === 'F' ? '2' : '1';

  // Data nascita
  const dataNascita = formatDateIT(ospite.data_nascita);

  // Comune nascita (9 chars) - vuoto se nato all'estero
  const natoInItalia = ospite.cittadinanza_codice === '100000100' ||
                       (ospite.luogo_nascita_codice && !ospite.luogo_nascita_codice.startsWith(' '));
  const comuneNascita = natoInItalia && ospite.luogo_nascita_codice
    ? pad(ospite.luogo_nascita_codice, 9) : pad('', 9);

  // Provincia nascita (2 chars)
  const provNascita = natoInItalia && ospite.luogo_nascita
    ? pad(extractProv(ospite.luogo_nascita), 2) : pad('', 2);

  // Stato nascita (9 chars)
  const statoNascita = pad(ospite.cittadinanza_codice || '100000100', 9);

  // Cittadinanza (9 chars)
  const cittadinanza = pad(ospite.cittadinanza_codice || '100000100', 9);

  // Parte obbligatoria (132 chars)
  let riga = tipo + dataArr + perm + cognome + nome + sesso + dataNascita +
             comuneNascita + provNascita + statoNascita + cittadinanza;

  const tipoNum = parseInt(tipo);

  if ([16, 17, 18].includes(tipoNum)) {
    // Documento (5 chars)
    const tipoDoc = pad(ospite.tipo_documento_codice || 'IDENT', 5);

    // Numero documento (20 chars)
    const numDoc = pad(cleanStr(ospite.numero_documento || '').toUpperCase(), 20);

    // Luogo rilascio documento (9 chars) — codice comune o stato
    const luogoRilCom = ospite.luogo_rilascio_codice
      ? pad(ospite.luogo_rilascio_codice, 9) : pad('', 9);

    riga += tipoDoc + numDoc + luogoRilCom;
    // TOTALE: 134 + 5 + 20 + 9 = 168 chars ✅ tracciato RESIDENCE Send/Test

  } else {
    // Familiari/membri (tipo 19/20): 34 blank per tipo doc + num doc + luogo rilascio
    riga += pad('', 34);
    // TOTALE: 134 + 34 = 168 chars ✅
  }

  return riga;
}


// ── Utilities ──

function pad(str, len) {
  const s = String(str || '');
  if (s.length >= len) return s.substring(0, len);
  return s + ' '.repeat(len - s.length);
}

function cleanStr(s) {
  // Rimuovi caratteri speciali, mantieni lettere e spazi
  return (s || '').replace(/[^\w\s\-']/g, '').trim();
}

function formatDateIT(dateStr) {
  if (!dateStr) return '          '; // 10 spazi
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
  const diff = Math.max(1, Math.round((d2 - d1) / 86400000));
  return String(diff).padStart(2, '0');
}

function extractProv(luogo) {
  if (!luogo) return '';
  // Prova a estrarre sigla provincia se presente tra parentesi: "Roma (RM)"
  const m = luogo.match(/\(([A-Z]{2})\)/);
  if (m) return m[1];
  // Oppure se è già una sigla di 2 chars
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

// Security: mask passwords and tokens in log data
function maskSensitiveData(str) {
  if (!str) return '';
  return str
    .replace(/<Password>.*?<\/Password>/gi, '<Password>***</Password>')
    .replace(/<WsKey>.*?<\/WsKey>/gi, '<WsKey>***</WsKey>')
    .replace(/<token>.*?<\/token>/gi, '<token>***</token>');
}
