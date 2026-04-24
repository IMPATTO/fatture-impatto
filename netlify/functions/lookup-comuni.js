const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

function normalizeLookupValue(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[’`´']/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: CORS, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return respond(405, { error: 'Method not allowed' });
  }

  let body;
  try {
    body = JSON.parse(event.body || '{}');
  } catch {
    return respond(400, { error: 'Invalid JSON body' });
  }

  const q = String(body.q || '').replace(/[’`´]/g, "'").replace(/\s+/g, ' ').trim();
  const provincia = String(body.provincia || '').trim().toUpperCase().slice(0, 2);
  if (q.length < 2) return respond(200, { matches: [] });
  const normalizedQuery = normalizeLookupValue(q);
  const prefix = q.slice(0, Math.min(q.length, 4));

  let data;
  let error;
  let capColumnAvailable = true;

  ({ data, error } = await supabase
    .from('codici_comuni')
    .select('codice,nome,provincia,cap')
    .ilike('nome', `${prefix}%`)
    .order('provincia', { ascending: true })
    .limit(50));

  if (error && /cap/i.test(String(error.message || ''))) {
    capColumnAvailable = false;
    ({ data, error } = await supabase
      .from('codici_comuni')
      .select('codice,nome,provincia')
      .ilike('nome', `${prefix}%`)
      .order('provincia', { ascending: true })
      .limit(50));
  }

  if (error) {
    console.error('lookup-comuni error:', error);
    return respond(500, { error: 'Lookup failed' });
  }

  let matches = (Array.isArray(data) ? data : [])
    .filter((row) => normalizeLookupValue(row.nome) === normalizedQuery);

  if (provincia) {
    const narrowed = matches.filter((row) => String(row.provincia || '').trim().toUpperCase().slice(0, 2) === provincia);
    if (narrowed.length) matches = narrowed;
  }

  const capSupported = capColumnAvailable && matches.some((row) => !!String(row.cap || '').trim());

  return respond(200, {
    matches: matches.map((row) => ({
      codice: row.codice,
      nome: row.nome,
      provincia: row.provincia,
      cap: row.cap || null,
    })),
    cap_supported: capSupported,
    cap_column_available: capColumnAvailable,
  });
};

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}
