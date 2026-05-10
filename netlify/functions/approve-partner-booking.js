const { createClient } = require('@supabase/supabase-js');
const { getInternalSupabaseUserFromHeaders, normalizeSupabaseUrl } = require('./_lib/shared-auth');

const BEDS24_URL = 'https://api.beds24.com/v2';

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

let beds24TokenCache = null;

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers: CORS, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return respond(405, { error: 'Method not allowed' });
  }

  const env = {
    SUPABASE_URL: normalizeSupabaseUrl(process.env.SUPABASE_RUNTIME_URL || process.env.SUPABASE_URL),
    SUPABASE_SERVICE_ROLE_KEY: process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY,
    BEDS24_API_KEY: process.env.BEDS24_API_KEY,
  };

  if (!env.SUPABASE_URL || !env.SUPABASE_SERVICE_ROLE_KEY) {
    return respond(500, { error: 'Configurazione Supabase mancante' });
  }

  const auth = await getInternalSupabaseUserFromHeaders(event.headers);
  if (!auth) {
    return respond(401, { error: 'Autenticazione interna richiesta' });
  }
  const approverEmail = auth.email;

  const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

  let body;
  try {
    body = JSON.parse(event.body || '{}');
  } catch (_error) {
    return respond(400, { error: 'JSON non valido' });
  }

  const requestId = String(body.requestId || '').trim();
  const action = String(body.action || '').trim().toLowerCase();
  const rejectionReason = String(body.reason || '').trim();

  if (!requestId) return respond(400, { error: 'requestId obbligatorio' });
  if (!['approve', 'reject'].includes(action)) {
    return respond(400, { error: 'action deve essere approve o reject' });
  }
  if (action === 'reject' && !rejectionReason) {
    return respond(400, { error: 'Motivo obbligatorio per il rifiuto' });
  }

  try {
    const { data: request, error: requestError } = await supabase
      .from('partner_booking_requests')
      .select('*')
      .eq('id', requestId)
      .maybeSingle();

    if (requestError) {
      return respond(500, { error: requestError.message });
    }
    if (!request) {
      return respond(404, { error: 'Richiesta non trovata' });
    }

    if (request.status === 'confirmed' && action === 'approve') {
      return respond(200, { success: true, request, alreadyApplied: true });
    }
    if (request.status === 'rejected' && action === 'reject') {
      return respond(200, { success: true, request, alreadyApplied: true });
    }

    if (action === 'reject') {
      const { data: updated, error: rejectError } = await supabase
        .from('partner_booking_requests')
        .update({
          status: 'rejected',
          sync_status: request.scope === 'residence' ? 'not_required' : 'not_required',
          approved_at: new Date().toISOString(),
          approved_by: approverEmail,
          sync_error: rejectionReason,
        })
        .eq('id', requestId)
        .select('*')
        .single();

      if (rejectError) {
        return respond(500, { error: rejectError.message });
      }

      await safeAuditLog(supabase, {
        user_email: approverEmail,
        action: 'REJECT_PARTNER_BOOKING_REQUEST',
        table_name: 'partner_booking_requests',
        record_id: requestId,
        timestamp: new Date().toISOString(),
        payload: {
          partner: request.partner,
          scope: request.scope,
          reason: rejectionReason,
        },
      });

      return respond(200, { success: true, request: updated });
    }

    if (request.scope === 'residence') {
      const result = await approveResidenceRequest(supabase, request, approverEmail);
      return respond(200, { success: true, request: result });
    }

    if (!env.BEDS24_API_KEY) {
      return respond(500, { error: 'BEDS24_API_KEY mancante' });
    }

    const result = await approveBeds24Request(supabase, env, request, approverEmail);
    return respond(200, { success: true, request: result });
  } catch (error) {
    console.error('[approve-partner-booking] error', error);
    return respond(500, { error: error.message || 'Errore approvazione richiesta' });
  }
};

async function approveResidenceRequest(supabase, request, approverEmail) {
  let residenceBookingId = request.residence_booking_id || null;

  if (!residenceBookingId) {
    const { data: inserted, error: insertError } = await supabase
      .from('rm_bookings')
      .insert({
        apartment_id: request.apartment_ref,
        group_id: null,
        segment: null,
        name: request.customer_name,
        checkin: request.checkin,
        checkout: request.checkout,
        pax: request.pax,
        status: 'confirmed',
        source: 'pr-luca',
        notes: request.notes || 'Richiesta Luca approvata dal backoffice',
        approved_at: new Date().toISOString(),
        approved_by: approverEmail,
      })
      .select('id')
      .single();

    if (insertError) {
      throw new Error(`Errore inserimento Residence: ${insertError.message}`);
    }

    residenceBookingId = inserted.id;
  }

  const { data: updated, error: updateError } = await supabase
    .from('partner_booking_requests')
    .update({
      status: 'confirmed',
      sync_status: 'applied',
      approved_at: new Date().toISOString(),
      approved_by: approverEmail,
      synced_at: new Date().toISOString(),
      sync_error: null,
      residence_booking_id: residenceBookingId,
    })
    .eq('id', request.id)
    .select('*')
    .single();

  if (updateError) {
    throw new Error(`Errore update richiesta Residence: ${updateError.message}`);
  }

  await safeAuditLog(supabase, {
    user_email: approverEmail,
    action: 'APPROVE_PARTNER_BOOKING_REQUEST',
    table_name: 'partner_booking_requests',
    record_id: request.id,
    timestamp: new Date().toISOString(),
    payload: {
      partner: request.partner,
      scope: request.scope,
      residence_booking_id: residenceBookingId,
      apartment_ref: request.apartment_ref,
    },
  });

  return updated;
}

async function approveBeds24Request(supabase, env, request, approverEmail) {
  const { data: apartment, error: apartmentError } = await supabase
    .from('apartments')
    .select('id,nome_appartamento,beds24_room_id')
    .eq('id', request.apartment_ref)
    .maybeSingle();

  if (apartmentError) {
    throw new Error(`Errore lettura appartamento Beds24: ${apartmentError.message}`);
  }
  if (!apartment?.beds24_room_id) {
    throw new Error('Appartamento Beds24 non collegato o inesistente');
  }

  const payload = {
    roomId: String(apartment.beds24_room_id),
    dateFrom: request.checkin,
    dateTo: request.checkout,
    status: 'blocked',
    firstName: request.customer_name,
    lastName: 'Luca',
    guestName: request.customer_name,
    description: request.notes || `Richiesta Luca approvata · ${request.customer_name}`,
  };

  const res = await beds24Fetch('/bookings', env, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });

  const rawText = await res.text();
  let parsed = null;
  try {
    parsed = rawText ? JSON.parse(rawText) : null;
  } catch (_error) {
    parsed = null;
  }

  if (!res.ok) {
    const detail = parsed?.error || parsed?.message || rawText || `Beds24 error ${res.status}`;
    await supabase
      .from('partner_booking_requests')
      .update({
        sync_status: 'error',
        sync_error: detail,
        approved_at: new Date().toISOString(),
        approved_by: approverEmail,
      })
      .eq('id', request.id);
    throw new Error(detail);
  }

  const createdBookingId =
    parsed?.bookId ||
    parsed?.id ||
    parsed?.data?.bookId ||
    parsed?.data?.id ||
    parsed?.data?.[0]?.bookId ||
    parsed?.data?.[0]?.id ||
    null;

  const { data: updated, error: updateError } = await supabase
    .from('partner_booking_requests')
    .update({
      status: 'confirmed',
      sync_status: 'applied',
      approved_at: new Date().toISOString(),
      approved_by: approverEmail,
      synced_at: new Date().toISOString(),
      sync_error: null,
      beds24_room_id: String(apartment.beds24_room_id),
      beds24_booking_id: createdBookingId ? String(createdBookingId) : null,
    })
    .eq('id', request.id)
    .select('*')
    .single();

  if (updateError) {
    throw new Error(`Errore update richiesta Beds24: ${updateError.message}`);
  }

  await safeAuditLog(supabase, {
    user_email: approverEmail,
    action: 'APPROVE_PARTNER_BOOKING_REQUEST',
    table_name: 'partner_booking_requests',
    record_id: request.id,
    timestamp: new Date().toISOString(),
    payload: {
      partner: request.partner,
      scope: request.scope,
      apartment_ref: request.apartment_ref,
      beds24_room_id: apartment.beds24_room_id,
      beds24_booking_id: createdBookingId,
    },
  });

  return updated;
}

async function safeAuditLog(supabase, payload) {
  try {
    await supabase.from('audit_log').insert(payload);
  } catch (_error) {
    // Best effort only.
  }
}

async function beds24Fetch(path, env, options = {}) {
  const request = async (token) => fetch(`${BEDS24_URL}${path}`, {
    ...options,
    headers: {
      accept: 'application/json',
      ...(options.headers || {}),
      token,
    },
  });

  let response = await request(await getBeds24Token(env.BEDS24_API_KEY, false));
  if (response.status !== 401) return response;

  const refreshedToken = await getBeds24Token(env.BEDS24_API_KEY, true);
  if (refreshedToken && refreshedToken !== env.BEDS24_API_KEY) {
    response = await request(refreshedToken);
  }

  return response;
}

async function getBeds24Token(rawKey, forceRefresh) {
  if (!rawKey) return null;

  if (!forceRefresh) {
    if (beds24TokenCache && beds24TokenCache.sourceKey === rawKey && beds24TokenCache.expiresAt > Date.now()) {
      return beds24TokenCache.token;
    }
    return rawKey;
  }

  const res = await fetch(`${BEDS24_URL}/authentication/token`, {
    method: 'GET',
    headers: {
      accept: 'application/json',
      refreshToken: rawKey,
    },
  });

  if (!res.ok) return rawKey;

  const data = await res.json();
  if (!data?.token) return rawKey;

  beds24TokenCache = {
    sourceKey: rawKey,
    token: data.token,
    expiresAt: Date.now() + Math.max(Number(data.expiresIn || 3600) - 60, 60) * 1000,
  };
  return data.token;
}

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}
