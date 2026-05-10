const { createClient } = require('@supabase/supabase-js');
const { getAuthContextFromHeaders, normalizeSupabaseUrl } = require('./_lib/shared-auth');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Content-Type': 'application/json',
};

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers: CORS, body: '' };
  }

  const auth = await getAuthContextFromHeaders(event.headers, {
    allowedSharedRoles: ['pr_luca'],
    allowInternalSupabase: true,
  });
  if (!auth) {
    return respond(401, { error: 'Autenticazione mancante o non valida' });
  }

  const supabaseUrl = normalizeSupabaseUrl(process.env.SUPABASE_RUNTIME_URL || process.env.SUPABASE_URL);
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
  if (!supabaseUrl || !serviceRoleKey) {
    return respond(500, { error: 'Configurazione Supabase mancante' });
  }

  const admin = createClient(supabaseUrl, serviceRoleKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  });

  try {
    if (event.httpMethod === 'GET') {
      const [apartmentsRes, bookingsRes, requestsRes] = await Promise.all([
        admin.from('rm_apartments').select('*').order('sort_order'),
        admin.from('rm_bookings').select('*').neq('status', 'rejected').neq('status', 'cancelled').order('checkin'),
        admin
          .from('partner_booking_requests')
          .select('*')
          .eq('partner', 'luca')
          .neq('status', 'cancelled')
          .order('created_at', { ascending: false }),
      ]);

      const firstError = apartmentsRes.error || bookingsRes.error || requestsRes.error;
      if (firstError) throw firstError;

      return respond(200, {
        apartments: apartmentsRes.data || [],
        bookings: bookingsRes.data || [],
        requests: requestsRes.data || [],
      });
    }

    if (event.httpMethod !== 'POST') {
      return respond(405, { error: 'Method not allowed' });
    }

    let body;
    try {
      body = JSON.parse(event.body || '{}');
    } catch (_error) {
      return respond(400, { error: 'JSON non valido' });
    }

    const payload = normalizeRequestPayload(body);
    const { data, error } = await admin
      .from('partner_booking_requests')
      .insert(payload)
      .select('*')
      .single();
    if (error) throw error;

    return respond(200, { ok: true, request: data });
  } catch (error) {
    console.error('[pr-luca-data] error:', error);
    return respond(500, { error: error.message || 'Errore interno pr-luca-data' });
  }
};

function normalizeRequestPayload(input) {
  const payload = {
    partner: 'luca',
    scope: String(input.scope || '').trim(),
    apartment_ref: String(input.apartment_ref || '').trim(),
    apartment_label: String(input.apartment_label || '').trim(),
    beds24_room_id: String(input.beds24_room_id || '').trim() || null,
    customer_name: String(input.customer_name || '').trim(),
    checkin: String(input.checkin || '').trim(),
    checkout: String(input.checkout || '').trim(),
    pax: Number.parseInt(input.pax, 10),
    notes: String(input.notes || '').trim() || null,
    status: 'pending',
    sync_status: 'pending',
  };

  if (!['residence', 'beds24'].includes(payload.scope)) {
    throw new Error('scope non valido');
  }
  if (!payload.apartment_ref) throw new Error('apartment_ref obbligatorio');
  if (!payload.apartment_label) throw new Error('apartment_label obbligatorio');
  if (!payload.customer_name) throw new Error('customer_name obbligatorio');
  if (!payload.checkin || !payload.checkout) throw new Error('checkin e checkout obbligatori');
  if (payload.checkout <= payload.checkin) throw new Error('checkout deve essere dopo checkin');
  if (!Number.isInteger(payload.pax) || payload.pax < 1) throw new Error('pax non valido');

  return payload;
}

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}
