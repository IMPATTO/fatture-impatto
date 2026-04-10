// netlify/functions/get-tabelle-alloggiati.js
// Scarica le tabelle di riferimento da AlloggiatiWeb e le importa in Supabase
//
// Variabili d'ambiente richieste:
//   SUPABASE_URL
//   SUPABASE_SERVICE_ROLE_KEY
//   ALLOGGIATI_USERNAME   (account PS004343 - Residence Montefeltro)
//   ALLOGGIATI_PASSWORD
//   ALLOGGIATI_WSKEY
//
// Endpoint:
//   POST /.netlify/functions/get-tabelle-alloggiati
//   Body: { "tipo": "Luoghi" | "Nazioni" | "Tipi_Documento" | "all" }
//   Header: Authorization: Bearer <supabase_token>
//
// Le tabelle vengono salvate in Supabase:
//   codici_comuni      ← tipo=Luoghi
//   codici_stati       ← tipo=Nazioni
//   codici_documenti   ← tipo=Tipi_Documento

const { createClient } = require('@supabase/supabase-js');

const ENDPOINT = 'https://alloggiatiweb.poliziadistato.it/service/service.asmx';

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

  let body;
  try { body = JSON.parse(event.body); } catch {
    return { statusCode: 400, body: JSON.stringify({ error: 'Invalid JSON' }) };
  }

  const { tipo = 'all' } = body;
  const tipiDaScaricane = tipo === 'all'
    ? ['Luoghi', 'Nazioni', 'Tipi_Documento']
    : [tipo];

  const tipiValidi = ['Luoghi', 'Nazioni', 'Tipi_Documento'];
  for (const t of tipiDaScaricane) {
    if (!tipiValidi.includes(t)) {
      return { statusCode: 400, body: JSON.stringify({ error: `Tipo non valido: ${t}. Usa: ${tipiValidi.join(', ')}` }) };
    }
  }

  try {
    // 1. Genera token usando le credenziali dell'account PS004343
    const tokenResult = await soapGenerateToken(
      process.env.ALLOGGIATI_USERNAME,
      process.env.ALLOGGIATI_PASSWORD,
      process.env.ALLOGGIATI_WSKEY
    );

    if (tokenResult.error) {
      return { statusCode: 401, body: JSON.stringify({ error: 'Autenticazione fallita', detail: tokenResult.error }) };
    }

    const results = {};

    // 2. Scarica e importa ogni tabella richiesta
    for (const tipoTabella of tipiDaScaricane) {
      const csvResult = await soapDownloadTabella(
        process.env.ALLOGGIATI_USERNAME,
        tokenResult.token,
        tipoTabella
      );

      if (csvResult.error) {
        results[tipoTabella] = { error: csvResult.error };
        continue;
      }

      // 3. Parsa il CSV e importa in Supabase
      const importResult = await importTabella(supabase, tipoTabella, csvResult.csv);
      results[tipoTabella] = importResult;
    }

    // Audit
    await supabase.from('audit_log').insert({
      user_email: user.email,
      action: 'IMPORT_TABELLE_ALLOGGIATI',
      table_name: 'codici_comuni/codici_stati/codici_documenti',
      record_id: tipo,
      timestamp: new Date().toISOString()
    }).then(() => {}); // non bloccare se fallisce

    return {
      statusCode: 200,
      body: JSON.stringify({ success: true, results })
    };

  } catch (err) {
    console.error('get-tabelle-alloggiati error:', err);
    return { statusCode: 500, body: JSON.stringify({ error: 'Errore interno', detail: err.message }) };
  }
};


// ─── SOAP: GenerateToken ──────────────────────────────────────────────────────
async function soapGenerateToken(utente, password, wskey) {
  const xml = `<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
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
  const tokenMatch = text.match(/<token>(.*?)<\/token>/i);
  const errorMatch = text.match(/<ErroreDettaglio>(.*?)<\/ErroreDettaglio>/i);

  if (!tokenMatch || !tokenMatch[1]) {
    return { error: errorMatch?.[1] || 'Token non ricevuto' };
  }
  return { token: tokenMatch[1] };
}


// ─── SOAP: Tabella (download CSV) ─────────────────────────────────────────────
async function soapDownloadTabella(utente, token, tipo) {
  const xml = `<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <Tabella xmlns="AlloggiatiService">
      <Utente>${escXml(utente)}</Utente>
      <token>${escXml(token)}</token>
      <tipo>${escXml(tipo)}</tipo>
    </Tabella>
  </soap:Body>
</soap:Envelope>`;

  const res = await fetch(ENDPOINT, {
    method: 'POST',
    headers: {
      'Content-Type': 'text/xml; charset=utf-8',
      'SOAPAction': 'AlloggiatiService/Tabella',
    },
    body: xml,
  });

  const text = await res.text();
  const csvMatch = text.match(/<CSV>([\s\S]*?)<\/CSV>/i);
  const errorMatch = text.match(/<ErroreDettaglio>([\s\S]*?)<\/ErroreDettaglio>/i);
  const esitoMatch = text.match(/<esito>(true|false)<\/esito>/i);

  if (!csvMatch || esitoMatch?.[1] !== 'true') {
    return { error: errorMatch?.[1] || 'CSV non ricevuto' };
  }

  // Decodifica entità XML nel CSV
  const csv = csvMatch[1]
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .trim();

  return { csv };
}


// ─── Import: CSV → Supabase ───────────────────────────────────────────────────
async function importTabella(supabase, tipo, csv) {
  const lines = csv.split('\n').map(l => l.trim()).filter(Boolean);
  if (lines.length < 2) {
    return { error: 'CSV vuoto o non valido', righe: 0 };
  }

  const header = lines[0].split(';').map(h => h.trim().toLowerCase());

  switch (tipo) {
    case 'Luoghi':
      return await importLuoghi(supabase, lines.slice(1), header);
    case 'Nazioni':
      return await importNazioni(supabase, lines.slice(1), header);
    case 'Tipi_Documento':
      return await importTipiDocumento(supabase, lines.slice(1), header);
    default:
      return { error: `Tipo ${tipo} non gestito` };
  }
}


// ─── Import: Luoghi → codici_comuni ──────────────────────────────────────────
// CSV header atteso: codice;nome;prov  (o simile, verifica con tabella reale)
// Struttura tabella Supabase:
//   codici_comuni(codice TEXT PK, nome TEXT, provincia TEXT, updated_at TIMESTAMPTZ)
async function importLuoghi(supabase, rows, header) {
  // Log header per debug
  console.log('Luoghi header:', header);

  const records = [];
  for (const line of rows) {
    const cols = line.split(';');
    // Il CSV di Luoghi ha: codice (9 chars), nome, provincia
    // L'indice lo ricaviamo dall'header, con fallback posizionale
    const codiceIdx = header.indexOf('codice') !== -1 ? header.indexOf('codice') : 0;
    const nomeIdx   = header.indexOf('nome')   !== -1 ? header.indexOf('nome')   : 1;
    const provIdx   = header.indexOf('prov')   !== -1 ? header.indexOf('prov')   :
                      header.indexOf('provincia') !== -1 ? header.indexOf('provincia') : 2;

    const codice = cols[codiceIdx]?.trim();
    const nome   = cols[nomeIdx]?.trim();
    const prov   = cols[provIdx]?.trim();

    if (!codice || !nome) continue;

    records.push({
      codice,
      nome: toTitleCase(nome),
      provincia: prov || null,
      updated_at: new Date().toISOString()
    });
  }

  if (records.length === 0) {
    return { error: 'Nessun record valido nel CSV Luoghi', righe: 0 };
  }

  // Upsert a blocchi di 500
  let imported = 0;
  const CHUNK = 500;
  for (let i = 0; i < records.length; i += CHUNK) {
    const chunk = records.slice(i, i + CHUNK);
    const { error } = await supabase
      .from('codici_comuni')
      .upsert(chunk, { onConflict: 'codice' });
    if (error) {
      console.error('Errore upsert codici_comuni:', error);
      return { error: error.message, imported };
    }
    imported += chunk.length;
  }

  return { success: true, righe: imported };
}


// ─── Import: Nazioni → codici_stati ──────────────────────────────────────────
// Struttura tabella Supabase:
//   codici_stati(codice TEXT PK, nome TEXT, nome_it TEXT, updated_at TIMESTAMPTZ)
async function importNazioni(supabase, rows, header) {
  console.log('Nazioni header:', header);

  const records = [];
  for (const line of rows) {
    const cols = line.split(';');
    const codiceIdx = header.indexOf('codice') !== -1 ? header.indexOf('codice') : 0;
    const nomeIdx   = header.indexOf('nome')   !== -1 ? header.indexOf('nome')   : 1;
    const nomeItIdx = header.indexOf('nomeit') !== -1 ? header.indexOf('nomeit') :
                      header.indexOf('nome_it') !== -1 ? header.indexOf('nome_it') : -1;

    const codice = cols[codiceIdx]?.trim();
    const nome   = cols[nomeIdx]?.trim();
    const nomeIt = nomeItIdx !== -1 ? cols[nomeItIdx]?.trim() : null;

    if (!codice || !nome) continue;

    records.push({
      codice,
      nome: toTitleCase(nome),
      nome_it: nomeIt ? toTitleCase(nomeIt) : toTitleCase(nome),
      updated_at: new Date().toISOString()
    });
  }

  if (records.length === 0) {
    return { error: 'Nessun record valido nel CSV Nazioni', righe: 0 };
  }

  let imported = 0;
  const CHUNK = 500;
  for (let i = 0; i < records.length; i += CHUNK) {
    const chunk = records.slice(i, i + CHUNK);
    const { error } = await supabase
      .from('codici_stati')
      .upsert(chunk, { onConflict: 'codice' });
    if (error) {
      console.error('Errore upsert codici_stati:', error);
      return { error: error.message, imported };
    }
    imported += chunk.length;
  }

  return { success: true, righe: imported };
}


// ─── Import: Tipi_Documento → codici_documenti ───────────────────────────────
// Struttura tabella Supabase:
//   codici_documenti(codice TEXT PK, descrizione TEXT, updated_at TIMESTAMPTZ)
async function importTipiDocumento(supabase, rows, header) {
  console.log('Tipi_Documento header:', header);

  const records = [];
  for (const line of rows) {
    const cols = line.split(';');
    const codiceIdx = header.indexOf('codice') !== -1 ? header.indexOf('codice') : 0;
    const descIdx   = header.indexOf('descrizione') !== -1 ? header.indexOf('descrizione') :
                      header.indexOf('nome') !== -1 ? header.indexOf('nome') : 1;

    const codice = cols[codiceIdx]?.trim();
    const desc   = cols[descIdx]?.trim();

    if (!codice) continue;

    records.push({
      codice,
      descrizione: desc || codice,
      updated_at: new Date().toISOString()
    });
  }

  if (records.length === 0) {
    return { error: 'Nessun record valido nel CSV Tipi_Documento', righe: 0 };
  }

  const { error } = await supabase
    .from('codici_documenti')
    .upsert(records, { onConflict: 'codice' });

  if (error) {
    console.error('Errore upsert codici_documenti:', error);
    return { error: error.message, righe: 0 };
  }

  return { success: true, righe: records.length };
}


// ─── Utilities ────────────────────────────────────────────────────────────────
function escXml(s) {
  return String(s || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

function toTitleCase(str) {
  if (!str) return str;
  return str.toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
}
