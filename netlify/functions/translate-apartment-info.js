const { createClient } = require('@supabase/supabase-js');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

const LANG_LABELS = {
  EN: 'English',
  DE: 'German',
  FR: 'French',
  ES: 'Spanish',
  RU: 'Russian',
};

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: CORS, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return jsonResponse(405, { error: 'Method not allowed' });
  }

  if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
    return jsonResponse(500, { error: 'Configurazione Supabase mancante' });
  }
  if (!process.env.ANTHROPIC_API_KEY) {
    return jsonResponse(500, { error: 'Configurazione traduzioni mancante' });
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

  const apartmentId = String(body.apartment_id || '').trim();
  const source = normalizeSource(body.source);
  const targetLangs = Array.from(new Set((body.target_langs || []).map((lang) => String(lang || '').trim().toUpperCase())))
    .filter((lang) => LANG_LABELS[lang]);

  if (!apartmentId) {
    return jsonResponse(400, { error: 'apartment_id richiesto' });
  }
  if (!targetLangs.length) {
    return jsonResponse(400, { error: 'target_langs richiesto' });
  }

  try {
    const translations = await translateFields(source, targetLangs);
    const rows = targetLangs.map((lang) => ({
      apartment_id: apartmentId,
      lingua: lang,
      come_arrivare: translations[lang]?.come_arrivare || '',
      dove_parcheggiare: translations[lang]?.dove_parcheggiare || '',
      come_entrare: null,
      regole_della_casa: translations[lang]?.regole_della_casa || '',
      cosa_fare_se_luce_spenta: translations[lang]?.cosa_fare_se_luce_spenta || '',
      cosa_fare_se_chiuso_fuori: translations[lang]?.cosa_fare_se_chiuso_fuori || '',
      numeri_utili: translations[lang]?.numeri_utili || {},
      updated_at: new Date().toISOString(),
    }));

    for (const row of rows) {
      const { data: existing, error: lookupError } = await supabase
        .from('apartment_info')
        .select('id')
        .eq('apartment_id', row.apartment_id)
        .eq('lingua', row.lingua)
        .maybeSingle();

      if (lookupError) {
        return jsonResponse(500, { error: 'Errore lettura traduzioni esistenti', detail: lookupError.message });
      }

      const payload = {
        come_arrivare: row.come_arrivare,
        dove_parcheggiare: row.dove_parcheggiare,
        come_entrare: row.come_entrare,
        regole_della_casa: row.regole_della_casa,
        cosa_fare_se_luce_spenta: row.cosa_fare_se_luce_spenta,
        cosa_fare_se_chiuso_fuori: row.cosa_fare_se_chiuso_fuori,
        numeri_utili: row.numeri_utili,
        updated_at: row.updated_at,
      };

      const { error } = existing?.id
        ? await supabase.from('apartment_info').update(payload).eq('id', existing.id)
        : await supabase.from('apartment_info').insert(row);

      if (error) {
        return jsonResponse(500, { error: 'Errore salvataggio traduzioni', detail: error.message });
      }
    }

    return jsonResponse(200, {
      success: true,
      translated_langs: targetLangs,
    });
  } catch (err) {
    console.error('[translate-apartment-info] error:', err);
    return jsonResponse(500, { error: 'Errore traduzione', detail: err.message });
  }
};

function normalizeSource(source) {
  const safe = source && typeof source === 'object' ? source : {};
  return {
    come_arrivare: cleanText(safe.come_arrivare),
    dove_parcheggiare: cleanText(safe.dove_parcheggiare),
    regole_della_casa: cleanText(safe.regole_della_casa),
    cosa_fare_se_luce_spenta: cleanText(safe.cosa_fare_se_luce_spenta),
    cosa_fare_se_chiuso_fuori: cleanText(safe.cosa_fare_se_chiuso_fuori),
    numeri_utili: normalizeContacts(safe.numeri_utili),
  };
}

function cleanText(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function normalizeContacts(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
  return Object.entries(value).reduce((acc, [key, raw]) => {
    const label = String(key || '').trim();
    const contact = String(raw || '').trim();
    if (label && contact) acc[label] = contact;
    return acc;
  }, {});
}

async function translateFields(source, targetLangs) {
  const prompt = buildPrompt(source, targetLangs);
  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': process.env.ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
    },
    body: JSON.stringify({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 2200,
      messages: [{
        role: 'user',
        content: [{ type: 'text', text: prompt }],
      }],
    }),
  });

  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.error?.message || data.error || 'Errore API traduzione');
  }

  const text = data.content?.[0]?.text || '';
  const parsed = safeParseJson(text);
  if (!parsed || typeof parsed !== 'object') {
    throw new Error('Risposta traduzione non valida');
  }

  const out = {};
  for (const lang of targetLangs) {
    const row = parsed[lang];
    if (!row || typeof row !== 'object') {
      throw new Error(`Traduzione mancante per ${lang}`);
    }
    out[lang] = {
      come_arrivare: cleanText(row.come_arrivare),
      dove_parcheggiare: cleanText(row.dove_parcheggiare),
      regole_della_casa: cleanText(row.regole_della_casa),
      cosa_fare_se_luce_spenta: cleanText(row.cosa_fare_se_luce_spenta),
      cosa_fare_se_chiuso_fuori: cleanText(row.cosa_fare_se_chiuso_fuori),
      numeri_utili: translateContacts(source.numeri_utili, row.numeri_utili),
    };
  }
  return out;
}

function buildPrompt(source, targetLangs) {
  const schema = targetLangs.map((lang) => `"${lang}":{"come_arrivare":"","dove_parcheggiare":"","regole_della_casa":"","cosa_fare_se_luce_spenta":"","cosa_fare_se_chiuso_fuori":"","numeri_utili":{}}`).join(',');
  return [
    'Translate these apartment portal instructions from Italian into the requested languages.',
    'Return ONLY valid JSON, with no markdown and no extra text.',
    'Keep URLs, phone numbers, apartment names, building codes, access codes, and brand names exactly as they are.',
    'Preserve line breaks when useful.',
    'Translate contact labels naturally for the target language, but keep each phone number unchanged.',
    `Requested languages: ${targetLangs.map((lang) => `${lang} (${LANG_LABELS[lang]})`).join(', ')}`,
    `Source JSON: ${JSON.stringify(source)}`,
    `Output schema: {${schema}}`,
  ].join('\n');
}

function translateContacts(sourceContacts, translatedContacts) {
  if (!translatedContacts || typeof translatedContacts !== 'object' || Array.isArray(translatedContacts)) {
    return sourceContacts;
  }

  const numbers = new Set(Object.values(sourceContacts));
  const out = {};

  for (const [key, value] of Object.entries(translatedContacts)) {
    const label = String(key || '').trim();
    const number = String(value || '').trim();
    if (!label || !number) continue;
    out[label] = number;
  }

  if (!Object.keys(out).length) return sourceContacts;

  const missing = [...numbers].filter((number) => !Object.values(out).includes(number));
  if (missing.length) return sourceContacts;

  return out;
}

function safeParseJson(text) {
  const cleaned = String(text || '').replace(/```json|```/g, '').trim();
  const match = cleaned.match(/\{[\s\S]*\}/);
  if (!match) return null;
  try {
    return JSON.parse(match[0]);
  } catch {
    return null;
  }
}

function jsonResponse(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}
