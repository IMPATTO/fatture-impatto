const { createClient } = require('@supabase/supabase-js');
const { resolvePublicCheckinKeyAlias } = require('./_public-checkin-key-rotation');

const DEFAULT_SUPABASE_URL = 'https://tysxeikqbgebpfyblgeb.supabase.co';
const PUBLIC_PORTAL_BASE_URL = process.env.PUBLIC_PORTAL_BASE_URL || '';
const DEFAULT_PORTAL_BASE_URL = String(
  process.env.SITE_URL_PORTALE
    || process.env.URL
    || process.env.DEPLOY_PRIME_URL
    || '',
).trim();
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

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
    return respond(405, { error: 'Method not allowed' });
  }

  const supabaseUrl = resolveSupabaseUrl(process.env.SUPABASE_URL);
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

  if (!supabaseUrl || !serviceRoleKey) {
    return respond(500, { error: 'Missing Supabase server configuration' });
  }

  let payload;
  try {
    payload = normalizeRecoveryPayload(JSON.parse(event.body || '{}'));
  } catch (_error) {
    return respond(400, { error: 'Invalid JSON body' });
  }

  const validationErrors = validateRecoveryPayload(payload);
  if (validationErrors.length) {
    return respond(400, { error: 'Validation failed', fields: validationErrors });
  }

  const supabase = createClient(supabaseUrl, serviceRoleKey);
  const { apartment, error: apartmentError } = await resolveApartmentReference(supabase, payload.apartment_ref);
  if (apartmentError) {
    console.error('recover-public-checkin apartment lookup error:', apartmentError);
    return respond(500, { error: 'Apartment lookup failed' });
  }
  if (!apartment?.id) {
    return respond(404, { error: 'Check-in not found', recovered: false });
  }

  try {
    const { data: candidates, error } = await supabase
      .from('ospiti_check_in')
      .select('id, portale_token, apartment_id, email, telefono, nome, cognome, data_checkin, data_checkout, stato, capogruppo_id, created_at')
      .eq('apartment_id', apartment.id)
      .is('capogruppo_id', null)
      .eq('email', payload.email)
      .eq('telefono', payload.telefono)
      .eq('nome', payload.nome)
      .eq('cognome', payload.cognome)
      .eq('data_checkin', payload.data_checkin)
      .eq('data_checkout', payload.data_checkout)
      .neq('stato', 'SCARTATA')
      .order('created_at', { ascending: false })
      .limit(5);

    if (error) {
      console.error('recover-public-checkin lookup error:', error);
      return respond(500, { error: 'Recovery lookup failed' });
    }

    const expectedChildren = Math.max(0, Number(payload.additional_guest_count || 0));
    const list = Array.isArray(candidates) ? candidates : [];
    for (const candidate of list) {
      const childCount = await countChildRecords(supabase, candidate.id);
      if (childCount !== expectedChildren) continue;
      return respond(200, {
        ok: true,
        recovered: true,
        record: {
          id: candidate.id,
          apartment_id: candidate.apartment_id,
          stato: candidate.stato,
        },
        child_records: Array.from({ length: childCount }, (_unused, index) => ({ index })),
        uploaded_documents: null,
        portal_url: buildPortalUrl(resolvePortalBaseUrl(event), candidate.portale_token, apartment.public_checkin_key),
      });
    }

    return respond(404, { ok: false, recovered: false, error: 'Check-in not found' });
  } catch (error) {
    console.error('recover-public-checkin error:', error);
    return respond(500, { error: 'Recovery failed', detail: error.message });
  }
};

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}

function resolveSupabaseUrl(rawValue) {
  const candidate = String(rawValue || '').trim();
  if (!candidate) return DEFAULT_SUPABASE_URL;
  try {
    const url = new URL(candidate);
    if (!/^https?:$/i.test(url.protocol)) return DEFAULT_SUPABASE_URL;
    return url.toString().replace(/\/+$/, '');
  } catch (_error) {
    return DEFAULT_SUPABASE_URL;
  }
}

function normalizeRecoveryPayload(body) {
  const additionalGuests = Array.isArray(body?.additional_guests) ? body.additional_guests : [];
  return {
    apartment_ref: String(body?.apartment_ref || '').trim(),
    email: String(body?.email || '').trim().toLowerCase(),
    telefono: String(body?.telefono || '').trim(),
    nome: String(body?.nome || '').trim(),
    cognome: String(body?.cognome || '').trim(),
    data_checkin: String(body?.data_checkin || '').trim(),
    data_checkout: String(body?.data_checkout || '').trim(),
    additional_guest_count: additionalGuests.length,
  };
}

function validateRecoveryPayload(payload) {
  const fields = [];
  if (!payload.apartment_ref) fields.push({ field: 'apartment_id', message: 'Appartamento mancante' });
  if (!payload.email) fields.push({ field: 'email', message: 'Email obbligatoria' });
  if (!payload.telefono) fields.push({ field: 'telefono', message: 'Telefono obbligatorio' });
  if (!payload.nome) fields.push({ field: 'nome', message: 'Nome obbligatorio' });
  if (!payload.cognome) fields.push({ field: 'cognome', message: 'Cognome obbligatorio' });
  if (!payload.data_checkin) fields.push({ field: 'checkin', message: 'Check-in obbligatorio' });
  if (!payload.data_checkout) fields.push({ field: 'checkout', message: 'Check-out obbligatorio' });
  return fields;
}

async function resolveApartmentReference(supabase, rawReference) {
  const reference = resolvePublicCheckinKeyAlias(rawReference);
  if (!reference) return { apartment: null, error: null };

  let query = supabase
    .from('apartments')
    .select('id, attivo, public_checkin_key')
    .eq('attivo', true);

  if (UUID_RE.test(reference)) query = query.eq('id', reference);
  else query = query.eq('public_checkin_key', reference);

  const { data, error } = await query.maybeSingle();
  if (error) return { apartment: null, error };
  return { apartment: data || null, error: null };
}

async function countChildRecords(supabase, parentId) {
  const { count, error } = await supabase
    .from('ospiti_check_in')
    .select('id', { count: 'exact', head: true })
    .eq('capogruppo_id', parentId);
  if (error) {
    console.error('recover-public-checkin child count error:', error);
    return -1;
  }
  return Number(count || 0);
}

function resolvePortalBaseUrl(event) {
  if (PUBLIC_PORTAL_BASE_URL) return PUBLIC_PORTAL_BASE_URL;
  if (DEFAULT_PORTAL_BASE_URL) return DEFAULT_PORTAL_BASE_URL;
  if (event.headers.origin) return event.headers.origin;
  if (event.headers.host) return `https://${event.headers.host}`;
  return 'http://localhost:8888';
}

function buildPortalUrl(baseUrl, token, publicCheckinKey) {
  const url = new URL('/portale.html', `${String(baseUrl || 'http://localhost:8888').replace(/\/+$/, '')}/`);
  if (token) url.searchParams.set('token', token);
  if (publicCheckinKey) url.searchParams.set('apt', publicCheckinKey);
  return url.toString();
}
