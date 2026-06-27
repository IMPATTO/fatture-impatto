const { createClient } = require('@supabase/supabase-js');
const {
  normalizeComuneName,
  preferCurrentComuneRows,
} = require('./_current-comuni-index');

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
  return normalizeComuneName(
    sanitizeComuneLikeValue(value)
      .replace(/[’`´']/g, '')
  );
}

function sanitizeComuneLikeValue(value) {
  let raw = String(value || '').replace(/[’`´]/g, "'").replace(/\s+/g, ' ').trim();
  if (!raw) return '';
  raw = raw.replace(/^(?:luogo(?:\s+di)?\s+nascita|birth\s*place|place\s+of\s+birth|born\s+in|nato\s+a|nata\s+a)\s*[:.-]?\s*/i, '');
  raw = raw.replace(/^(?:luogo(?:\s+di)?\s+rilascio|place\s+of\s+issue|issued\s+(?:at|by)|authority\s+of\s+issue)\s*[:.-]?\s*/i, '');
  raw = raw.replace(/^(rilasciato\s+(?:da|presso)\s+)/i, '');
  raw = raw.replace(/^(comune|questura|prefettura|motorizzazione|anagrafe)\s+di\s+/i, '');
  raw = raw.replace(/^(comune|questura|prefettura|motorizzazione|anagrafe)\s+/i, '');
  raw = raw.replace(/\b\d{5}\b/g, ' ').replace(/\s+/g, ' ').trim();
  raw = raw.replace(/\(([A-Za-z]{2})\)\s*$/, ' ').trim();
  raw = raw.replace(/[-/]\s*[A-Za-z]{2}\s*$/, ' ').trim();
  const commaParts = raw.split(',').map((part) => String(part || '').trim()).filter(Boolean);
  if (commaParts.length > 1) {
    const lastPart = String(commaParts[commaParts.length - 1] || '').trim();
    const prevPart = String(commaParts[commaParts.length - 2] || '').trim();
    raw = /^[A-Za-z]{2}$/.test(lastPart.toUpperCase()) && prevPart
      ? prevPart
      : lastPart;
  }
  const tokens = raw.split(/\s+/).filter(Boolean);
  if (tokens.length > 1) {
    const lastToken = String(tokens[tokens.length - 1] || '').trim().toUpperCase().replace(/[^A-Z]/g, '');
    if (/^[A-Z]{2}$/.test(lastToken)) tokens.pop();
  }
  return tokens.join(' ').replace(/\s+/g, ' ').trim();
}

function escapeLikeValue(value) {
  return String(value || '').replace(/[%_]/g, '\\$&');
}

function buildComuneLookupPatterns(value) {
  const sanitized = sanitizeComuneLikeValue(value);
  if (!sanitized) return [];

  const patterns = [];
  const pushPattern = (pattern) => {
    if (!pattern || patterns.includes(pattern)) return;
    patterns.push(pattern);
  };

  pushPattern(sanitized);

  const prefix = sanitized.slice(0, Math.min(sanitized.length, 4)).trim();
  if (prefix.length >= 2) pushPattern(`${escapeLikeValue(prefix)}%`);

  const tokenCandidates = sanitized
    .split(/\s+/)
    .map((token) => token.replace(/[^A-Za-zÀ-ÿ']/g, '').trim())
    .filter(Boolean)
    .sort((left, right) => right.length - left.length);

  tokenCandidates.forEach((token) => {
    if (token.length >= 3) pushPattern(`%${escapeLikeValue(token)}%`);
  });

  return patterns;
}

async function fetchComuneRows(pattern, { includeCap = true } = {}) {
  const columns = includeCap ? 'codice,nome,provincia,cap' : 'codice,nome,provincia';
  const { data, error } = await supabase
    .from('codici_comuni')
    .select(columns)
    .ilike('nome', pattern)
    .order('provincia', { ascending: true })
    .limit(50);
  return { data: Array.isArray(data) ? data : [], error };
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
  const patterns = buildComuneLookupPatterns(q);
  if (!normalizedQuery || !patterns.length) return respond(200, { matches: [] });

  const collectedRows = [];
  const seenCodes = new Set();
  let error;
  let capColumnAvailable = true;

  for (const pattern of patterns) {
    let result = await fetchComuneRows(pattern, { includeCap: capColumnAvailable });
    error = result.error;
    if (error && capColumnAvailable && /cap/i.test(String(error.message || ''))) {
      capColumnAvailable = false;
      result = await fetchComuneRows(pattern, { includeCap: false });
      error = result.error;
    }
    if (error) break;

    result.data.forEach((row) => {
      const code = String(row?.codice || '').trim();
      if (!code || seenCodes.has(code)) return;
      seenCodes.add(code);
      collectedRows.push(row);
    });
  }

  if (error) {
    console.error('lookup-comuni error:', error);
    return respond(500, { error: 'Lookup failed' });
  }

  let matches = collectedRows
    .filter((row) => normalizeLookupValue(row.nome) === normalizedQuery);

  if (provincia) {
    const narrowed = matches.filter((row) => String(row.provincia || '').trim().toUpperCase().slice(0, 2) === provincia);
    if (narrowed.length) matches = narrowed;
  }

  matches = await preferCurrentComuneRows(matches, normalizedQuery, {
    normalize: normalizeLookupValue,
  });

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

exports.__test__ = {
  buildComuneLookupPatterns,
  normalizeLookupValue,
  sanitizeComuneLikeValue,
};
