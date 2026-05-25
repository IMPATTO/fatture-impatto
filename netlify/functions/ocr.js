// netlify/functions/ocr.js
// Accetta una o piu immagini base64 e restituisce sempre un array JSON valido.

const OCR_ALLOWED_MIME_TYPES = new Set([
  'image/gif',
  'image/jpeg',
  'image/png',
  'image/webp',
]);

const PRIVATE_FIELDS = [
  'nome',
  'cognome',
  'data_nascita',
  'sesso',
  'cittadinanza',
  'stato_nascita',
  'tipo_documento',
  'numero_documento',
  'luogo_nascita',
  'luogo_rilascio',
  'codice_fiscale',
  'indirizzo',
  'cap',
  'citta',
  'provincia',
];

const COMPANY_FIELDS = [
  'ragione_sociale',
  'partita_iva',
  'codice_fiscale',
  'indirizzo',
  'cap',
  'citta',
  'provincia',
  'paese',
  'sdi',
  'pec',
];

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: CORS, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, headers: CORS, body: JSON.stringify({ error: 'Method not allowed' }) };
  }

  try {
    const body = JSON.parse(event.body || '{}');
    const tipo = body.tipo === 'azienda' ? 'azienda' : 'privato';
    const images = normalizeImages(body);
    if (!process.env.ANTHROPIC_API_KEY) {
      console.error('OCR error: missing ANTHROPIC_API_KEY');
      return {
        statusCode: 503,
        headers: CORS,
        body: JSON.stringify({ error: 'OCR temporarily unavailable' }),
      };
    }

    if (!images.length) {
      return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'Missing images' }) };
    }

    const supportedImages = images.filter((image) => OCR_ALLOWED_MIME_TYPES.has(detectMediaType(image)));
    if (!supportedImages.length) {
      return {
        statusCode: 200,
        headers: CORS,
        body: JSON.stringify([emptyResult(tipo)]),
      };
    }

    const prompt = buildPrompt(tipo);
    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 1200,
        messages: [{
          role: 'user',
          content: supportedImages
            .map((image) => ({ type: 'image', source: { type: 'base64', media_type: detectMediaType(image), data: image.data } }))
            .concat([{ type: 'text', text: prompt }]),
        }],
      }),
    });

    const responseText = await res.text();
    if (!res.ok) {
      console.error('OCR upstream error:', res.status, responseText.slice(0, 500));
      return {
        statusCode: 503,
        headers: CORS,
        body: JSON.stringify({ error: 'OCR temporarily unavailable' }),
      };
    }
    const data = safeParseJsonObject(responseText);
    const text = data.content?.[0]?.text || '[]';
    const parsed = safeParseJson(text);
    const normalized = normalizeOcrResponse(parsed, tipo, supportedImages.length);

    return {
      statusCode: 200,
      headers: CORS,
      body: JSON.stringify(normalized),
    };
  } catch (err) {
    console.error('OCR error:', err);
    return {
      statusCode: 200,
      headers: CORS,
      body: JSON.stringify([emptyResult('privato')]),
    };
  }
};

function normalizeImages(body) {
  if (Array.isArray(body.images)) {
    return body.images.map((item) => {
      if (item && typeof item === 'object') {
        return {
          data: String(item.data || '').trim(),
          mime_type: String(item.mime_type || item.type || '').trim().toLowerCase(),
        };
      }
      return {
        data: String(item || '').trim(),
        mime_type: '',
      };
    }).filter((item) => item.data);
  }
  if (body.image) {
    return [{
      data: String(body.image).trim(),
      mime_type: String(body.mime_type || body.type || '').trim().toLowerCase(),
    }].filter((item) => item.data);
  }
  return [];
}

function buildPrompt(tipo) {
  if (tipo === 'azienda') {
    return `Analizza tutte le immagini come un unico documento aziendale. Restituisci SOLO JSON valido nel formato:
[{"ragione_sociale":"","partita_iva":"","codice_fiscale":"","indirizzo":"","cap":"","citta":"","provincia":"","paese":"","sdi":"","pec":""}]
Regole:
- usa sempre un array JSON
- se un campo manca usa stringa vuota
- nessun testo extra, nessun markdown`;
  }

  return `Analizza tutte le immagini come documenti della stessa persona e restituisci SOLO JSON valido nel formato:
[{"nome":"","cognome":"","data_nascita":"YYYY-MM-DD","sesso":"","cittadinanza":"","stato_nascita":"","tipo_documento":"","numero_documento":"","luogo_nascita":"","luogo_rilascio":"","codice_fiscale":"","indirizzo":"","cap":"","citta":"","provincia":""}]
Regole:
- usa sempre un array JSON
- tipo_documento: usa "Carta d'identita", "Passaporto" oppure "Patente"
- sesso: usa solo "M" o "F"
- se il documento indica uno stato/paese di nascita estero, mettilo in stato_nascita
- se la nascita e estera e vedi solo una citta, prova comunque a valorizzare stato_nascita con il paese corretto leggibile dal documento
- per documenti esteri, in luogo_rilascio preferisci sempre il paese/stato emittente (es. "Francia"), non il nome della citta, della questura o dell'autorita, se il paese e leggibile
- data_nascita: formato YYYY-MM-DD, altrimenti stringa vuota
- se un campo manca usa stringa vuota
- nessun testo extra, nessun markdown`;
}

function safeParseJson(text) {
  const cleaned = String(text || '').replace(/```json|```/g, '').trim();
  const match = cleaned.match(/\[[\s\S]*\]|\{[\s\S]*\}/);
  if (!match) return [];
  try {
    return JSON.parse(match[0]);
  } catch {
    return [];
  }
}

function safeParseJsonObject(text) {
  try {
    return JSON.parse(String(text || '{}'));
  } catch {
    return {};
  }
}

function normalizeOcrResponse(parsed, tipo, minLength) {
  const rows = Array.isArray(parsed) ? parsed : parsed && typeof parsed === 'object' ? [parsed] : [];
  const normalized = rows.map((row) => sanitizeRow(row, tipo)).filter(Boolean);
  if (normalized.length) return normalized;
  return Array.from({ length: Math.max(1, minLength) }, () => emptyResult(tipo));
}

function sanitizeRow(row, tipo) {
  const fields = tipo === 'azienda' ? COMPANY_FIELDS : PRIVATE_FIELDS;
  const output = emptyResult(tipo);
  if (!row || typeof row !== 'object') return output;
  for (const field of fields) {
    output[field] = normalizeField(field, row[field]);
  }
  return output;
}

function normalizeField(field, value) {
  const normalized = String(value || '').trim();
  if (!normalized) return '';
  if (field === 'sesso') return normalized.toUpperCase().startsWith('F') ? 'F' : normalized.toUpperCase().startsWith('M') ? 'M' : '';
  if (field === 'data_nascita') return /^\d{4}-\d{2}-\d{2}$/.test(normalized) ? normalized : '';
  if (field === 'cittadinanza') {
    const key = normalized
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[’`´']/g, '')
      .replace(/\s+/g, ' ')
      .trim()
      .toLowerCase();
    if (['italia','italy','italiana','italiano','cittadinanza italiana','nazionalita italiana','nazionalita: italia'].includes(key)) return 'Italia';
  }
  if (field === 'stato_nascita') {
    const key = normalized
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[’`´']/g, '')
      .replace(/\s+/g, ' ')
      .trim()
      .toLowerCase();
    if (['italia','italy','italiana','italiano'].includes(key)) return 'Italia';
  }
  if (field === 'provincia') return normalized.toUpperCase();
  if (field === 'codice_fiscale') return normalized.toUpperCase();
  return normalized;
}

function emptyResult(tipo) {
  const fields = tipo === 'azienda' ? COMPANY_FIELDS : PRIVATE_FIELDS;
  return fields.reduce((acc, field) => {
    acc[field] = '';
    return acc;
  }, {});
}

function detectMediaType(image) {
  const mimeType = String(image?.mime_type || '').trim().toLowerCase();
  if (OCR_ALLOWED_MIME_TYPES.has(mimeType)) return mimeType;

  const base64 = String(image?.data || image || '');
  if (base64.startsWith('/9j/')) return 'image/jpeg';
  if (base64.startsWith('iVBOR')) return 'image/png';
  if (base64.startsWith('UklGR')) return 'image/webp';
  if (base64.startsWith('R0lGOD')) return 'image/gif';
  return 'image/jpeg';
}
