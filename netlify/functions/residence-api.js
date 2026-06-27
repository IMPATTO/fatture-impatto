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
    allowedSharedRoles: ['residence_partner', 'residence_admin'],
    allowInternalSupabase: false,
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
      const includeAll = auth.role === 'residence_admin';
      const [apartmentsRes, bookingsRes, unavailabilityRes] = await Promise.all([
        admin.from('rm_apartments').select('*').order('sort_order'),
        includeAll
          ? admin.from('rm_bookings').select('*').order('checkin')
          : admin.from('rm_bookings').select('*').neq('status', 'rejected').neq('status', 'cancelled').order('checkin'),
        admin.from('rm_unavailability').select('*'),
      ]);

      const firstError = apartmentsRes.error || bookingsRes.error || unavailabilityRes.error;
      if (firstError) {
        throw firstError;
      }

      return respond(200, {
        apartments: apartmentsRes.data || [],
        bookings: bookingsRes.data || [],
        unavailability: unavailabilityRes.data || [],
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

    const action = String(body.action || '').trim().toLowerCase();
    if (!action) {
      return respond(400, { error: 'action obbligatoria' });
    }

    switch (action) {
      case 'create':
        return respond(200, await createResidenceBookings(admin, auth, body));
      case 'update':
        return respond(200, await updateResidenceBooking(admin, auth, body));
      case 'delete':
        return respond(200, await deleteResidenceBookings(admin, auth, body));
      case 'approve':
        return respond(200, await approveResidenceBooking(admin, auth, body));
      case 'reject':
        return respond(200, await rejectResidenceBooking(admin, auth, body));
      default:
        return respond(400, { error: 'action non supportata' });
    }
  } catch (error) {
    console.error('[residence-api] error:', error);
    return respond(500, { error: error.message || 'Errore interno residence-api' });
  }
};

async function createResidenceBookings(admin, auth, body) {
  const inputList = Array.isArray(body.bookings) ? body.bookings : body.booking ? [body.booking] : [];
  if (!inputList.length) {
    throw new Error('bookings obbligatorio');
  }

  const sanitized = inputList.map((row, index) => normalizeBookingInput(row, {
    forceSource: auth.role === 'residence_partner' ? 'checco' : 'system',
    forceStatus: auth.role === 'residence_partner' ? 'pending' : undefined,
    allowStatus: auth.role === 'residence_admin',
    defaultGroupId: inputList.length > 1 ? String(body.group_id || `g_${Date.now()}`) : null,
    defaultSegment: inputList.length > 1 ? `${index + 1}/${inputList.length}` : null,
  }));

  if (auth.role === 'residence_admin') {
    sanitized.forEach((row) => {
      if (row.status === 'confirmed') {
        row.approved_at = new Date().toISOString();
        row.approved_by = auth.actor;
      }
    });
  }

  const { data, error } = await admin
    .from('rm_bookings')
    .insert(sanitized)
    .select('*');
  if (error) throw new Error(error.message);

  await insertAudit(admin, {
    action: 'create',
    booking_id: data?.[0]?.id || null,
    actor: auth.actor,
    details: { count: sanitized.length, source: sanitized[0]?.source || null },
  });
  await syncResidenceCalendarToPms(admin);

  return { ok: true, bookings: data || [] };
}

async function updateResidenceBooking(admin, auth, body) {
  const bookingId = String(body.id || '').trim();
  if (!bookingId) {
    throw new Error('id obbligatorio');
  }

  const { data: existing, error: existingError } = await admin
    .from('rm_bookings')
    .select('*')
    .eq('id', bookingId)
    .maybeSingle();
  if (existingError) throw new Error(existingError.message);
  if (!existing) throw new Error('Prenotazione non trovata');

  if (auth.role === 'residence_partner') {
    if (existing.source !== 'checco') throw new Error('Puoi modificare solo richieste Kekko');
    if (existing.status === 'confirmed') throw new Error('Le prenotazioni confermate non sono modificabili');
  }

  const payload = normalizeBookingInput(body.booking || body, {
    forceSource: existing.source || (auth.role === 'residence_partner' ? 'checco' : 'system'),
    forceStatus: auth.role === 'residence_partner' ? 'pending' : undefined,
    preserveGroupId: existing.group_id,
    preserveSegment: existing.segment,
    allowStatus: auth.role === 'residence_admin',
  });
  delete payload.group_id;
  delete payload.segment;
  delete payload.source;

  if (auth.role === 'residence_admin' && payload.status === 'confirmed') {
    payload.approved_at = new Date().toISOString();
    payload.approved_by = auth.actor;
  }

  const { data, error } = await admin
    .from('rm_bookings')
    .update(payload)
    .eq('id', bookingId)
    .select('*')
    .single();
  if (error) throw new Error(error.message);

  await insertAudit(admin, {
    action: 'modify',
    booking_id: bookingId,
    actor: auth.actor,
    details: payload,
  });
  await syncResidenceCalendarToPms(admin);

  return { ok: true, booking: data };
}

async function deleteResidenceBookings(admin, auth, body) {
  const requestedIds = Array.isArray(body.ids) ? body.ids.map((value) => String(value || '').trim()).filter(Boolean) : [];
  const bookingId = String(body.id || '').trim();
  const ids = requestedIds.length ? requestedIds : bookingId ? [bookingId] : [];
  if (!ids.length) throw new Error('id o ids obbligatorio');

  const { data: rows, error: rowsError } = await admin
    .from('rm_bookings')
    .select('*')
    .in('id', ids);
  if (rowsError) throw new Error(rowsError.message);
  if (!rows?.length) throw new Error('Prenotazioni non trovate');

  if (auth.role === 'residence_partner') {
    const invalid = rows.find((row) => row.source !== 'checco' || row.status === 'confirmed');
    if (invalid) {
      throw new Error('Puoi eliminare solo richieste Kekko non confermate');
    }
  }

  const { error } = await admin.from('rm_bookings').delete().in('id', rows.map((row) => row.id));
  if (error) throw new Error(error.message);

  await insertAudit(admin, {
    action: 'delete',
    booking_id: rows[0]?.id || null,
    actor: auth.actor,
    details: { ids: rows.map((row) => row.id), count: rows.length },
  });
  await syncResidenceCalendarToPms(admin);

  return { ok: true, deleted_ids: rows.map((row) => row.id) };
}

async function approveResidenceBooking(admin, auth, body) {
  if (auth.role !== 'residence_admin') {
    throw new Error('Solo admin puo approvare');
  }

  const bookingId = String(body.id || '').trim();
  if (!bookingId) throw new Error('id obbligatorio');

  const ids = await resolveGroupedBookingIds(admin, bookingId);
  const payload = {
    status: 'confirmed',
    approved_at: new Date().toISOString(),
    approved_by: auth.actor,
  };
  const { data, error } = await admin
    .from('rm_bookings')
    .update(payload)
    .in('id', ids)
    .select('*');
  if (error) throw new Error(error.message);

  await insertAudit(admin, {
    action: 'approve',
    booking_id: bookingId,
    actor: auth.actor,
    details: { ids },
  });
  await syncResidenceCalendarToPms(admin);

  return { ok: true, bookings: data || [], approved_ids: ids };
}

async function rejectResidenceBooking(admin, auth, body) {
  if (auth.role !== 'residence_admin') {
    throw new Error('Solo admin puo rifiutare');
  }

  const bookingId = String(body.id || '').trim();
  const reason = String(body.reason || '').trim();
  if (!bookingId) throw new Error('id obbligatorio');

  const ids = await resolveGroupedBookingIds(admin, bookingId);
  const { data: rows, error: rowsError } = await admin
    .from('rm_bookings')
    .select('*')
    .in('id', ids);
  if (rowsError) throw new Error(rowsError.message);

  const updates = (rows || []).map((row) => ({
    id: row.id,
    status: 'rejected',
    notes: reason ? `[Rifiutata: ${reason}] ${String(row.notes || '').trim()}`.trim() : row.notes || '',
  }));

  for (const update of updates) {
    const { error } = await admin
      .from('rm_bookings')
      .update({ status: update.status, notes: update.notes })
      .eq('id', update.id);
    if (error) throw new Error(error.message);
  }

  await insertAudit(admin, {
    action: 'reject',
    booking_id: bookingId,
    actor: auth.actor,
    details: { ids, reason: reason || null },
  });
  await syncResidenceCalendarToPms(admin);

  return { ok: true, rejected_ids: ids };
}

async function resolveGroupedBookingIds(admin, bookingId) {
  const { data: row, error } = await admin
    .from('rm_bookings')
    .select('id,group_id')
    .eq('id', bookingId)
    .maybeSingle();
  if (error) throw new Error(error.message);
  if (!row) throw new Error('Prenotazione non trovata');
  if (!row.group_id) return [row.id];

  const { data: grouped, error: groupedError } = await admin
    .from('rm_bookings')
    .select('id')
    .eq('group_id', row.group_id);
  if (groupedError) throw new Error(groupedError.message);
  return (grouped || []).map((item) => item.id);
}

function normalizeBookingInput(input, options = {}) {
  const depositAmount = parseMoneyValue(input.deposit_amount ?? input.depositAmount);
  const balanceDueAtCheckin = parseMoneyValue(input.balance_due_at_checkin ?? input.balanceDueAtCheckin);
  const payload = {
    apartment_id: String(input.apartment_id || input.apartmentId || '').trim(),
    name: String(input.name || '').trim(),
    checkin: String(input.checkin || '').trim(),
    checkout: String(input.checkout || '').trim(),
    pax: Number.parseInt(input.pax, 10),
    notes: String(input.notes || '').trim(),
    source: options.forceSource || String(input.source || '').trim() || 'system',
  };

  if (!payload.apartment_id) throw new Error('apartment_id obbligatorio');
  if (!payload.name) throw new Error('name obbligatorio');
  if (!payload.checkin || !payload.checkout) throw new Error('checkin e checkout obbligatori');
  if (!Number.isInteger(payload.pax) || payload.pax < 1) throw new Error('pax non valido');
  if (payload.checkout <= payload.checkin) throw new Error('checkout deve essere dopo checkin');

  if (options.allowStatus) {
    payload.status = String(input.status || options.forceStatus || 'pending').trim() || 'pending';
  } else {
    payload.status = options.forceStatus || 'pending';
  }

  const groupId = String(options.preserveGroupId || input.group_id || options.defaultGroupId || '').trim();
  const segment = String(options.preserveSegment || input.segment || options.defaultSegment || '').trim();
  if (groupId) payload.group_id = groupId;
  if (segment) payload.segment = segment;
  payload.deposit_amount = depositAmount;
  payload.balance_due_at_checkin = balanceDueAtCheckin;

  return payload;
}

function parseMoneyValue(value) {
  if (value === '' || value == null) return null;
  const parsed = Number(String(value).replace(',', '.'));
  if (!Number.isFinite(parsed)) {
    throw new Error('Importo pagamento non valido');
  }
  return Number(parsed.toFixed(2));
}

async function insertAudit(admin, payload) {
  try {
    await admin.from('rm_audit').insert({
      action: payload.action,
      booking_id: payload.booking_id || null,
      actor: payload.actor || 'system',
      details: payload.details || {},
    });
  } catch (error) {
    console.warn('[residence-api] audit insert failed:', error.message);
  }
}

async function syncResidenceCalendarToPms(admin) {
  const result = await admin.rpc('sync_rm_calendar_to_pms');
  if (result.error) {
    throw new Error(`Sync calendario interno fallita: ${result.error.message}`);
  }
}

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}
