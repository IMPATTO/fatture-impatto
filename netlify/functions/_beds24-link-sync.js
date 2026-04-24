const BEDS24_URL = 'https://api.beds24.com/v2';

const MANUAL_OVERRIDES = {
  'APT-01': '324751',
  'APT-02': '324617',
  'APT-03': '324765',
  'APT-04': '324615',
  'APT-05': '324584',
  'APT-06': '324716',
  'APT-07': '324630',
  'APT-10': '324589',
  'APT-11': '324764',
  'APT-12': '324729',
  'APT-13': '324704',
  'APT-15': '324648',
  'APT-17': '324653',
  'APT-18': '324683',
  'APT-22': '324548',
  'APT-23': '324747',
  'APT-24': '324746',
  'APT-25': '324674',
  'APT-31': '324752',
  'APT-32': '324544',
  'APT-33': '324585',
  'APT-34': '324639',
  'APT-35': '324625',
  'APT-37': '324735',
  'APT-38': '324726',
  'APT-39': '324636',
  'APT-41': '324757',
  'APT-43': '324361',
  'RESIDENCE-2': '324753',
  'MONTEFELTRO': '324755',
  'VIA-GIUSEPPE-MAZZINI-169-CATTOLICA': '324637',
};

const STOPWORDS = new Set([
  'apt', 'appartamento', 'appartamenti', 'residence', 'residenza', 'con', 'di', 'del',
  'della', 'il', 'la', 'lo', 'the', 'via', 'viale', 'piazza', 'corso', 'vicolo',
  'piano', 'primo', 'terra', 'interno', 'balcone', 'cortile', 'vista', 'mare',
  'trilocale', 'bilocale', 'monolocale', 'cattolica', 'rimini', 'riccione',
  'viserba', 'viserbella', 'misano', 'adriatico', 'miramare', 'fanano', 'gradara',
  'valtournenche', 'san', 'giovanni', 'marignano', 'pesaro', 'gabicce', 'montefeltro'
]);

function normalizeText(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/d'/g, 'd ')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function tokenize(value) {
  return normalizeText(value)
    .split(' ')
    .filter((token) => token && !STOPWORDS.has(token));
}

function extractStreetNumber(value) {
  const normalized = normalizeText(value);
  const match = normalized.match(/\b(via|viale|piazza|corso|vicolo|frazione|strada)\s+([a-z ]+?)\s+(\d+[a-z\/-]*)\b/);
  if (!match) return null;
  return {
    street: match[2].trim(),
    number: match[3].trim(),
  };
}

function extractHints(value) {
  const normalized = normalizeText(value);
  const hints = new Set();
  if (/\bprimo\b|\bsopra\b|\bprimo piano\b/.test(normalized)) hints.add('upper');
  if (/\bpiano terra\b|\bsotto\b|\bterra\b/.test(normalized)) hints.add('lower');
  if (/\bdavanti\b|\bfronte\b|\bstrada\b/.test(normalized)) hints.add('front');
  if (/\bdietro\b|\binterno\b|\bcortile\b/.test(normalized)) hints.add('inner');
  if (/\bmezzo\b|\bcentrale\b/.test(normalized)) hints.add('middle');
  return hints;
}

async function getBeds24AccessToken(refreshToken) {
  const res = await fetch(`${BEDS24_URL}/authentication/token`, {
    method: 'GET',
    headers: {
      accept: 'application/json',
      refreshToken,
    },
  });

  if (!res.ok) {
    throw new Error(`Beds24 auth failed (${res.status}): ${await res.text()}`);
  }

  const data = await res.json();
  if (!data?.token) throw new Error('Beds24 auth failed: token mancante');
  return data.token;
}

async function fetchBeds24Properties(refreshToken) {
  const accessToken = await getBeds24AccessToken(refreshToken);
  const res = await fetch(`${BEDS24_URL}/properties`, {
    headers: {
      accept: 'application/json',
      token: accessToken,
    },
  });

  if (!res.ok) {
    throw new Error(`Beds24 properties failed (${res.status}): ${await res.text()}`);
  }

  const data = await res.json();
  return (data?.data || []).map((property) => ({
    id: String(property.id),
    name: property.name || '',
    address: property.address || '',
    city: property.city || '',
    normalized_name: normalizeText(property.name),
    normalized_address: normalizeText(property.address),
    tokens: tokenize(`${property.name} ${property.address} ${property.city}`),
    street_number: extractStreetNumber(`${property.address} ${property.name}`),
    hints: extractHints(`${property.name} ${property.address}`),
  }));
}

function scoreMatch(apartment, property) {
  let score = 0;
  const reasons = [];
  const apartmentText = normalizeText(`${apartment.nome_appartamento} ${apartment.indirizzo_completo || ''} ${apartment.struttura_nome || ''}`);
  const apartmentTokens = tokenize(`${apartment.nome_appartamento} ${apartment.indirizzo_completo || ''}`);
  const apartmentStreet = extractStreetNumber(`${apartment.indirizzo_completo || ''} ${apartment.nome_appartamento || ''}`);
  const apartmentHints = extractHints(`${apartment.nome_appartamento || ''} ${apartment.indirizzo_completo || ''}`);
  const apartmentCity = normalizeText(apartment.struttura_nome || apartment.nome_appartamento || '');

  if (apartmentStreet && property.street_number) {
    if (apartmentStreet.street === property.street_number.street) {
      score += 40;
      reasons.push('via');
      if (apartmentStreet.number === property.street_number.number) {
        score += 35;
        reasons.push('numero civico');
      }
    }
  }

  if (apartmentCity && (property.normalized_name.includes(apartmentCity) || normalizeText(property.city).includes(apartmentCity))) {
    score += 15;
    reasons.push('citta');
  }

  const sharedTokens = apartmentTokens.filter((token) => property.tokens.includes(token));
  if (sharedTokens.length) {
    score += Math.min(25, sharedTokens.length * 5);
    reasons.push(`token:${sharedTokens.slice(0, 4).join(',')}`);
  }

  if (property.normalized_name && apartmentText.includes(property.normalized_name)) {
    score += 25;
    reasons.push('nome contenuto');
  }
  if (apartment.codice_interno && MANUAL_OVERRIDES[apartment.codice_interno] === property.id) {
    score += 1000;
    reasons.push('override');
  }

  if (apartmentHints.size && property.hints.size) {
    const overlappingHints = [...apartmentHints].filter((hint) => property.hints.has(hint));
    if (overlappingHints.length) {
      score += 20;
      reasons.push(`hint:${overlappingHints.join(',')}`);
    }
  }

  return { score, reasons };
}

function matchApartments(apartments, properties) {
  const propertyById = new Map(properties.map((property) => [property.id, property]));
  const usedPropertyIds = new Set();
  const matches = [];
  const unmatched = [];

  for (const apartment of apartments) {
    const overrideId = MANUAL_OVERRIDES[apartment.codice_interno || ''];
    if (overrideId && propertyById.has(overrideId)) {
      usedPropertyIds.add(overrideId);
      matches.push({
        apartment,
        property: propertyById.get(overrideId),
        score: 1000,
        reasons: ['override'],
      });
      continue;
    }

    const candidates = properties
      .filter((property) => !usedPropertyIds.has(property.id))
      .map((property) => ({ property, ...scoreMatch(apartment, property) }))
      .sort((a, b) => b.score - a.score);

    const best = candidates[0];
    const second = candidates[1];
    if (!best || best.score < 65 || (second && best.score - second.score < 15 && best.score < 120)) {
      unmatched.push({
        apartment,
        top_candidates: candidates.slice(0, 3).map((candidate) => ({
          beds24_room_id: candidate.property.id,
          property_name: candidate.property.name,
          score: candidate.score,
          reasons: candidate.reasons,
        })),
      });
      continue;
    }

    usedPropertyIds.add(best.property.id);
    matches.push({
      apartment,
      property: best.property,
      score: best.score,
      reasons: best.reasons,
    });
  }

  const unmatchedProperties = properties.filter((property) => !usedPropertyIds.has(property.id));
  return { matches, unmatched, unmatchedProperties };
}

async function loadApartments(supabase) {
  const { data, error } = await supabase
    .from('apartments')
    .select('id,codice_interno,nome_appartamento,indirizzo_completo,struttura_nome,provincia,beds24_room_id,attivo')
    .order('nome_appartamento');

  if (error) throw new Error(`Errore lettura apartments: ${error.message}`);
  return data || [];
}

async function applyBeds24Matches(supabase, report) {
  const updates = [];
  const clears = [];
  const kept = [];

  for (const row of report.matches) {
    const currentId = row.apartment.beds24_room_id ? String(row.apartment.beds24_room_id) : null;
    if (currentId === row.property.id) {
      kept.push({ apartment_id: row.apartment.id, beds24_room_id: row.property.id });
      continue;
    }
    const { error } = await supabase
      .from('apartments')
      .update({ beds24_room_id: row.property.id })
      .eq('id', row.apartment.id);
    if (error) throw new Error(`Errore update ${row.apartment.nome_appartamento}: ${error.message}`);
    updates.push({
      apartment_id: row.apartment.id,
      apartment_name: row.apartment.nome_appartamento,
      beds24_room_id: row.property.id,
      beds24_name: row.property.name,
      reasons: row.reasons,
    });
  }

  const matchedIds = new Set(report.matches.map((row) => row.apartment.id));
  const apartmentsToClear = report.apartments.filter((apartment) => apartment.beds24_room_id && !matchedIds.has(apartment.id));
  for (const apartment of apartmentsToClear) {
    const { error } = await supabase
      .from('apartments')
      .update({ beds24_room_id: null })
      .eq('id', apartment.id);
    if (error) throw new Error(`Errore clear ${apartment.nome_appartamento}: ${error.message}`);
    clears.push({
      apartment_id: apartment.id,
      apartment_name: apartment.nome_appartamento,
      previous_beds24_room_id: apartment.beds24_room_id,
    });
  }

  return { updates, clears, kept };
}

async function insertAuditLog(supabase, payload) {
  const { error } = await supabase.from('audit_log').insert(payload);
  if (error) {
    console.warn('[sync-beds24-links] audit_log insert failed:', error.message);
  }
}

function buildReport(apartments, properties) {
  const matching = matchApartments(apartments, properties);
  return {
    generated_at: new Date().toISOString(),
    summary: {
      apartments_total: apartments.length,
      beds24_properties_total: properties.length,
      matched: matching.matches.length,
      unmatched_apartments: matching.unmatched.length,
      unmatched_beds24_properties: matching.unmatchedProperties.length,
    },
    apartments,
    ...matching,
  };
}

module.exports = {
  MANUAL_OVERRIDES,
  fetchBeds24Properties,
  loadApartments,
  buildReport,
  applyBeds24Matches,
  insertAuditLog,
};
