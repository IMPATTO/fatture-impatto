const crypto = require('crypto');
const { createClient } = require('@supabase/supabase-js');
const {
  getBearerToken,
  getSupabaseUserFromToken,
  normalizeSupabaseUrl,
} = require('./_lib/shared-auth');
const { resolvePmsCalendarScope, canEditApartment } = require('./_lib/pms-calendar-access');

const BEDS24_URL = 'https://api.beds24.com/v2';
const ALLOWED_OPERATIONS = new Set(['create', 'update', 'delete']);

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json; charset=utf-8',
};

let beds24TokenCache = null;

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: CORS, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return respond(405, { error: 'Method not allowed' });
  }

  const token = getBearerToken(event.headers || {});
  if (!token) {
    return respond(401, { error: 'Unauthorized' });
  }

  const user = await getSupabaseUserFromToken(token);
  if (!user?.id) {
    return respond(401, { error: 'Unauthorized' });
  }
  const userEmail = String(user.email || '').trim().toLowerCase();

  let body;
  try {
    body = JSON.parse(event.body || '{}');
  } catch (_error) {
    return respond(400, { error: 'Invalid JSON' });
  }

  const operation = String(body?.operation || '').trim().toLowerCase();
  const booking = body?.booking;
  if (!ALLOWED_OPERATIONS.has(operation)) {
    return respond(400, { error: 'operation must be create, update, or delete' });
  }
  if (!booking || typeof booking !== 'object' || Array.isArray(booking)) {
    return respond(400, { error: 'booking object required' });
  }

  const validationErrors = validateBookingInput(operation, booking);
  if (validationErrors.length) {
    return respond(400, { error: 'Validation failed', details: validationErrors });
  }

  const env = {
    SUPABASE_URL: normalizeSupabaseUrl(process.env.SUPABASE_RUNTIME_URL || process.env.SUPABASE_URL),
    SUPABASE_SERVICE_ROLE_KEY: process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY,
    BEDS24_REFRESH_TOKEN: String(process.env.BEDS24_REFRESH_TOKEN || '').trim(),
  };
  if (!env.SUPABASE_URL || !env.SUPABASE_SERVICE_ROLE_KEY) {
    return respond(500, { error: 'Missing env vars' });
  }

  const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);
  const scope = await resolvePmsCalendarScope(supabase, userEmail);
  if (!scope.canEditAny) {
    return respond(403, { error: 'Forbidden: no editable calendar access configured' });
  }

  const bookingId = normalizeText(booking.beds24_booking_id);
  let existingBooking = null;
  if (bookingId && (operation === 'update' || operation === 'delete')) {
    const existingResult = await supabase
      .from('bookings')
      .select('*')
      .eq('beds24_booking_id', bookingId)
      .maybeSingle();
    if (existingResult.error) {
      console.warn('[BOOKING-PUSH] local booking lookup failed', existingResult.error.message);
    } else {
      existingBooking = existingResult.data || null;
    }
  }
  const isResidenceMirror = Boolean(existingBooking && isResidenceMirrorBooking(existingBooking));

  let unit = null;
  let apartment = null;
  if (booking.apartment_unit_id) {
    const mapping = await fetchUnitAndApartment(supabase, booking.apartment_unit_id);
    if (mapping.error) {
      return respond(500, { error: 'Failed to fetch apartment unit', detail: mapping.error });
    }
    if (!mapping.unit) {
      return respond(400, { error: 'Invalid apartment_unit_id' });
    }
    if (!mapping.unit.beds24_room_id && !(isResidenceMirror && operation !== 'create')) {
      return respond(400, { error: 'Apartment unit has no beds24_room_id mapping' });
    }
    unit = mapping.unit;
    apartment = mapping.apartment;
  } else if (operation === 'create') {
    return respond(400, { error: 'Validation failed', details: ['booking.apartment_unit_id is required for create'] });
  } else if (existingBooking?.apartment_unit_id) {
    const mapping = await fetchUnitAndApartment(supabase, existingBooking.apartment_unit_id);
    if (!mapping.error) {
      unit = mapping.unit;
      apartment = mapping.apartment;
    }
  }

  const targetApartmentId = booking.apartment_unit_id
    ? apartment?.id
    : (existingBooking?.apartment_id || apartment?.id || null);
  if (!targetApartmentId || !canEditApartment(scope, targetApartmentId)) {
    return respond(403, { error: 'Forbidden: booking is outside your editable scope' });
  }

  const targetApartmentUnitId = booking.apartment_unit_id || existingBooking?.apartment_unit_id || null;
  const targetArrival = normalizeDate(booking.arrival) || normalizeDate(existingBooking?.check_in);
  const targetDeparture = normalizeDate(booking.departure) || normalizeDate(existingBooking?.check_out);
  const needsOverlapCheck = operation === 'create'
    || Boolean(booking.apartment_unit_id)
    || Boolean(booking.arrival)
    || Boolean(booking.departure);

  if (needsOverlapCheck) {
    if (!targetApartmentUnitId || !targetArrival || !targetDeparture) {
      return respond(400, {
        error: 'Cannot validate overlap without apartment_unit_id, arrival and departure context',
      });
    }
    if (targetArrival >= targetDeparture) {
      return respond(400, { error: 'arrival must be before departure' });
    }
    const conflicts = await findConflictingBookings(
      supabase,
      targetApartmentUnitId,
      targetArrival,
      targetDeparture,
      operation === 'update' ? bookingId : null,
    );
    if (conflicts.error) {
      return respond(500, { error: 'Failed to validate overlap', detail: conflicts.error });
    }
    if (conflicts.rows.length) {
      return respond(409, {
        error: 'Overbooking detected',
        conflicting_bookings: conflicts.rows.map((row) => ({
          beds24_booking_id: row.beds24_booking_id,
          arrival: row.check_in,
          departure: row.check_out,
        })),
      });
    }
  }

  if (isResidenceMirror && operation !== 'create') {
    const residenceResult = await applyResidenceMirrorMutation({
      supabase,
      operation,
      booking,
      existingBooking,
      unit,
    });
    if (!residenceResult.ok) {
      return respond(residenceResult.statusCode || 502, {
        error: residenceResult.error || 'Residence sync failed',
        detail: residenceResult.detail || '',
      });
    }
    return respond(200, compactObject({
      success: true,
      operation,
      beds24_booking_id: bookingId,
      beds24_updated: false,
      local_cache_updated: true,
      warning: residenceResult.warning,
      fields_modified: operation === 'update' ? listModifiedFields(booking) : undefined,
    }));
  }

  if (!env.BEDS24_REFRESH_TOKEN) {
    return respond(500, { error: 'Missing env vars' });
  }

  let beds24Token;
  try {
    beds24Token = await getBeds24AccessToken(env.BEDS24_REFRESH_TOKEN);
  } catch (error) {
    console.error('[BEDS24-BOOKING-AUTH-FAIL]', error.message);
    return respond(502, { error: 'Beds24 auth failed', detail: error.message });
  }

  let beds24Response;
  let beds24Result;
  let beds24RequestBody = null;
  let beds24BookingId = bookingId;
  let localCacheUpdated = false;
  let warning = '';

  if (operation === 'create') {
    beds24RequestBody = [buildCreatePayload({ booking, unit })];
    ({ response: beds24Response, payload: beds24Result } = await postBeds24Bookings(beds24Token, beds24RequestBody));
    if (!beds24Response.ok || beds24Response.status !== 201) {
      return respond(502, {
        error: 'Beds24 create failed',
        status: beds24Response.status,
        detail: beds24Result,
      });
    }
    if (!Array.isArray(beds24Result) || !beds24Result[0]?.success) {
      return respond(502, { error: 'Beds24 create failed', detail: beds24Result });
    }
    beds24BookingId = extractBookingId(beds24Result);
    if (!beds24BookingId) {
      return respond(502, { error: 'Beds24 create succeeded but booking id missing', detail: beds24Result });
    }
    const localResult = await createLocalBooking({
      supabase,
      beds24BookingId,
      booking,
      unit,
      apartment,
      beds24Result,
      beds24RequestBody,
    });
    localCacheUpdated = localResult.ok;
    const residenceSync = localCacheUpdated
      ? await syncPmsBookingIntoResidence({
        supabase,
        beforeBooking: null,
        afterBooking: await fetchLocalBookingByBeds24Id(supabase, beds24BookingId),
      })
      : { changed: false, warning: '' };
    warning = mergeWarnings(localResult.warning, residenceSync.warning);
    return respond(200, compactObject({
      success: true,
      operation,
      beds24_booking_id: beds24BookingId,
      beds24_updated: true,
      local_cache_updated: localCacheUpdated,
      warning,
    }));
  }

  if (operation === 'update') {
    beds24RequestBody = [buildUpdatePayload({ booking, unit })];
    ({ response: beds24Response, payload: beds24Result } = await postBeds24Bookings(beds24Token, beds24RequestBody));
    if (!beds24Response.ok || beds24Response.status !== 201) {
      return respond(502, {
        error: 'Beds24 update failed',
        status: beds24Response.status,
        detail: beds24Result,
      });
    }
    if (!Array.isArray(beds24Result) || !beds24Result[0]?.success) {
      return respond(502, { error: 'Beds24 update failed', detail: beds24Result });
    }
    const localResult = await updateLocalBooking({
      supabase,
      bookingId,
      booking,
      existingBooking,
      unit,
      apartment,
      beds24Result,
      beds24RequestBody,
    });
    localCacheUpdated = localResult.ok;
    const residenceSync = localCacheUpdated
      ? await syncPmsBookingIntoResidence({
        supabase,
        beforeBooking: existingBooking,
        afterBooking: await fetchLocalBookingByBeds24Id(supabase, bookingId),
      })
      : { changed: false, warning: '' };
    warning = mergeWarnings(localResult.warning, residenceSync.warning);
    return respond(200, compactObject({
      success: true,
      operation,
      beds24_booking_id: bookingId,
      beds24_updated: true,
      local_cache_updated: localCacheUpdated,
      warning,
      fields_modified: listModifiedFields(booking),
    }));
  }

  ({ response: beds24Response, payload: beds24Result } = await deleteBeds24Booking(beds24Token, bookingId));
  if (!beds24Response.ok || beds24Response.status !== 200) {
    return respond(502, {
      error: 'Beds24 delete failed',
      status: beds24Response.status,
      detail: beds24Result,
    });
  }
  const localResult = await cancelLocalBooking({
    supabase,
    bookingId,
    existingBooking,
    beds24Result,
  });
  localCacheUpdated = localResult.ok;
  const residenceSync = localCacheUpdated
    ? await syncPmsBookingIntoResidence({
      supabase,
      beforeBooking: existingBooking,
      afterBooking: existingBooking ? { ...existingBooking, status: 'cancelled' } : null,
    })
    : { changed: false, warning: '' };
  warning = mergeWarnings(localResult.warning, residenceSync.warning);

  return respond(200, compactObject({
    success: true,
    operation,
    beds24_booking_id: bookingId,
    beds24_updated: true,
    local_cache_updated: localCacheUpdated,
    warning,
  }));
};

function validateBookingInput(operation, booking) {
  const errors = [];
  if (operation === 'create') {
    if (!normalizeText(booking.apartment_unit_id)) errors.push('booking.apartment_unit_id is required');
    if (!normalizeDate(booking.arrival)) errors.push('booking.arrival is required (YYYY-MM-DD)');
    if (!normalizeDate(booking.departure)) errors.push('booking.departure is required (YYYY-MM-DD)');
    if (!normalizeText(booking.first_name)) errors.push('booking.first_name is required');
    if (!normalizeText(booking.last_name)) errors.push('booking.last_name is required');
    if (normalizeDate(booking.arrival) && normalizeDate(booking.departure) && booking.arrival >= booking.departure) {
      errors.push('booking.arrival must be before booking.departure');
    }
  }

  if (operation === 'update') {
    if (!normalizeText(booking.beds24_booking_id)) errors.push('booking.beds24_booking_id is required');
    const mutableFields = [
      'apartment_unit_id',
      'arrival',
      'departure',
      'first_name',
      'last_name',
      'email',
      'phone',
      'num_adult',
      'num_child',
      'price',
      'notes',
      'status',
    ];
    const hasMutableField = mutableFields.some((field) => booking[field] !== undefined);
    if (!hasMutableField) errors.push('booking update requires at least one mutable field');
    const arrival = normalizeDate(booking.arrival);
    const departure = normalizeDate(booking.departure);
    if (arrival && departure && arrival >= departure) {
      errors.push('booking.arrival must be before booking.departure');
    }
  }

  if (operation === 'delete') {
    if (!normalizeText(booking.beds24_booking_id)) errors.push('booking.beds24_booking_id is required');
  }

  if (booking.arrival && !isIsoDate(booking.arrival)) errors.push('booking.arrival must be YYYY-MM-DD');
  if (booking.departure && !isIsoDate(booking.departure)) errors.push('booking.departure must be YYYY-MM-DD');
  if (booking.email !== undefined && booking.email !== null && typeof booking.email !== 'string') errors.push('booking.email must be a string');
  if (booking.phone !== undefined && booking.phone !== null && typeof booking.phone !== 'string') errors.push('booking.phone must be a string');
  if (booking.first_name !== undefined && booking.first_name !== null && typeof booking.first_name !== 'string') errors.push('booking.first_name must be a string');
  if (booking.last_name !== undefined && booking.last_name !== null && typeof booking.last_name !== 'string') errors.push('booking.last_name must be a string');
  if (booking.notes !== undefined && booking.notes !== null && typeof booking.notes !== 'string') errors.push('booking.notes must be a string');
  if (booking.status !== undefined && booking.status !== null && typeof booking.status !== 'string') errors.push('booking.status must be a string');

  if (booking.price !== undefined && booking.price !== null && (!Number.isFinite(Number(booking.price)) || Number(booking.price) < 0)) {
    errors.push('booking.price must be a number >= 0');
  }
  if (booking.num_adult !== undefined && booking.num_adult !== null && (!Number.isInteger(Number(booking.num_adult)) || Number(booking.num_adult) < 0)) {
    errors.push('booking.num_adult must be an integer >= 0');
  }
  if (booking.num_child !== undefined && booking.num_child !== null && (!Number.isInteger(Number(booking.num_child)) || Number(booking.num_child) < 0)) {
    errors.push('booking.num_child must be an integer >= 0');
  }

  return errors;
}

async function fetchUnitAndApartment(supabase, apartmentUnitId) {
  const unitResult = await supabase
    .from('apartment_units')
    .select('id, apartment_id, beds24_room_id, beds24_unit_index')
    .eq('id', apartmentUnitId)
    .maybeSingle();
  if (unitResult.error) {
    return { error: unitResult.error.message, unit: null, apartment: null };
  }
  const unit = unitResult.data || null;
  if (!unit) return { error: null, unit: null, apartment: null };

  const apartmentResult = await supabase
    .from('apartments')
    .select('id, beds24_property_id')
    .eq('id', unit.apartment_id)
    .maybeSingle();
  if (apartmentResult.error) {
    return { error: apartmentResult.error.message, unit, apartment: null };
  }
  return { error: null, unit, apartment: apartmentResult.data || null };
}

async function findConflictingBookings(supabase, apartmentUnitId, arrival, departure, excludedBeds24BookingId) {
  let query = supabase
    .from('bookings')
    .select('id, beds24_booking_id, check_in, check_out')
    .eq('apartment_unit_id', apartmentUnitId)
    .neq('status', 'cancelled')
    .lt('check_in', departure)
    .gt('check_out', arrival)
    .order('check_in', { ascending: true });

  if (excludedBeds24BookingId) {
    query = query.neq('beds24_booking_id', excludedBeds24BookingId);
  }

  const result = await query;
  if (result.error) {
    return { error: result.error.message, rows: [] };
  }
  return { error: null, rows: result.data || [] };
}

function buildCreatePayload({ booking, unit }) {
  return compactObject({
    roomId: Number(unit.beds24_room_id),
    status: normalizeText(booking.status) || 'confirmed',
    arrival: normalizeDate(booking.arrival),
    departure: normalizeDate(booking.departure),
    numAdult: booking.num_adult != null ? Number(booking.num_adult) : 2,
    numChild: booking.num_child != null ? Number(booking.num_child) : 0,
    firstName: normalizeText(booking.first_name),
    lastName: normalizeText(booking.last_name),
    email: normalizeText(booking.email) || undefined,
    phone: normalizeText(booking.phone) || undefined,
    price: booking.price != null ? Number(booking.price) : undefined,
    notes: normalizeText(booking.notes) || undefined,
  });
}

function buildUpdatePayload({ booking, unit }) {
  return compactObject({
    id: normalizeText(booking.beds24_booking_id),
    roomId: unit?.beds24_room_id ? Number(unit.beds24_room_id) : undefined,
    status: booking.status !== undefined ? normalizeText(booking.status) : undefined,
    arrival: booking.arrival !== undefined ? normalizeDate(booking.arrival) : undefined,
    departure: booking.departure !== undefined ? normalizeDate(booking.departure) : undefined,
    numAdult: booking.num_adult !== undefined ? Number(booking.num_adult) : undefined,
    numChild: booking.num_child !== undefined ? Number(booking.num_child) : undefined,
    firstName: booking.first_name !== undefined ? normalizeText(booking.first_name) : undefined,
    lastName: booking.last_name !== undefined ? normalizeText(booking.last_name) : undefined,
    email: booking.email !== undefined ? normalizeText(booking.email) || null : undefined,
    phone: booking.phone !== undefined ? normalizeText(booking.phone) || null : undefined,
    price: booking.price !== undefined ? Number(booking.price) : undefined,
    notes: booking.notes !== undefined ? normalizeText(booking.notes) || null : undefined,
  });
}

async function postBeds24Bookings(token, requestBody) {
  const response = await fetch(`${BEDS24_URL}/bookings`, {
    method: 'POST',
    headers: {
      token,
      Accept: 'application/json',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(requestBody),
  });
  const payload = await parseJsonSafely(response);
  console.log('[BEDS24-BOOKING-PUSH][POST]', JSON.stringify(requestBody), response.status, truncateForLog(JSON.stringify(payload)));
  return { response, payload };
}

async function deleteBeds24Booking(token, bookingId) {
  const response = await fetch(`${BEDS24_URL}/bookings/${encodeURIComponent(bookingId)}`, {
    method: 'DELETE',
    headers: {
      token,
      Accept: 'application/json',
    },
  });
  const payload = await parseJsonSafely(response);
  console.log('[BEDS24-BOOKING-PUSH][DELETE]', bookingId, response.status, truncateForLog(JSON.stringify(payload)));
  return { response, payload };
}

async function createLocalBooking({ supabase, beds24BookingId, booking, unit, apartment, beds24Result, beds24RequestBody }) {
  const now = new Date().toISOString();
  const hasNotes = booking.notes !== undefined && normalizeText(booking.notes) !== '';
  const localRow = {
    beds24_booking_id: beds24BookingId,
    beds24_property_id: normalizeText(apartment?.beds24_property_id) || normalizeText(unit?.beds24_room_id) || 'unknown',
    beds24_room_id: normalizeText(unit?.beds24_room_id) || 'unknown',
    beds24_unit_id: Number(unit?.beds24_unit_index || 1),
    apartment_id: unit?.apartment_id || null,
    apartment_unit_id: unit?.id || normalizeText(booking.apartment_unit_id) || null,
    guest_first_name: normalizeText(booking.first_name) || null,
    guest_last_name: normalizeText(booking.last_name) || null,
    guest_email: normalizeText(booking.email) || null,
    guest_phone: normalizeText(booking.phone) || null,
    num_adults: booking.num_adult != null ? Number(booking.num_adult) : 2,
    num_children: booking.num_child != null ? Number(booking.num_child) : 0,
    check_in: normalizeDate(booking.arrival),
    check_out: normalizeDate(booking.departure),
    status: normalizeText(booking.status) || 'confirmed',
    channel: 'manual',
    channel_normalized: 'manual',
    total_price: booking.price != null ? Number(booking.price) : null,
    currency: 'EUR',
    source: 'pms-manual',
    source_updated_at: now,
    source_created_at: now,
    synced_at: now,
    raw_payload: {
      manual_request: booking,
      beds24_request: beds24RequestBody,
      beds24_response: beds24Result,
      notes_not_persisted_in_columns: hasNotes,
    },
  };

  const result = await supabase
    .from('bookings')
    .upsert(localRow, { onConflict: 'beds24_booking_id' });
  if (result.error) {
    console.error('[LOCAL-BOOKING-CREATE-FAIL]', result.error.message);
    return {
      ok: false,
      warning: 'Beds24 updated successfully but local bookings cache insert failed.',
    };
  }
  return { ok: true, warning: localRow.raw_payload.notes_not_persisted_in_columns ? 'Booking note saved only in raw_payload local cache.' : '' };
}

async function updateLocalBooking({ supabase, bookingId, booking, existingBooking, unit, apartment, beds24Result, beds24RequestBody }) {
  if (!existingBooking) {
    console.warn('[LOCAL-BOOKING-UPDATE-SKIP] booking not found locally', bookingId);
    return {
      ok: false,
      warning: 'Beds24 updated successfully but local booking was not found to update.',
    };
  }

  const now = new Date().toISOString();
  const patch = {
    source_updated_at: now,
    synced_at: now,
    raw_payload: {
      ...(existingBooking.raw_payload || {}),
      last_manual_request: booking,
      last_beds24_request: beds24RequestBody,
      last_beds24_response: beds24Result,
      notes_not_persisted_in_columns: booking.notes !== undefined,
    },
  };

  if (unit?.apartment_id) patch.apartment_id = unit.apartment_id;
  if (unit?.id) patch.apartment_unit_id = unit.id;
  if (unit?.beds24_room_id) patch.beds24_room_id = String(unit.beds24_room_id);
  if (apartment?.beds24_property_id) patch.beds24_property_id = String(apartment.beds24_property_id);
  if (unit?.beds24_unit_index != null) patch.beds24_unit_id = Number(unit.beds24_unit_index || 1);
  if (booking.arrival !== undefined) patch.check_in = normalizeDate(booking.arrival);
  if (booking.departure !== undefined) patch.check_out = normalizeDate(booking.departure);
  if (booking.first_name !== undefined) patch.guest_first_name = normalizeText(booking.first_name) || null;
  if (booking.last_name !== undefined) patch.guest_last_name = normalizeText(booking.last_name) || null;
  if (booking.email !== undefined) patch.guest_email = normalizeText(booking.email) || null;
  if (booking.phone !== undefined) patch.guest_phone = normalizeText(booking.phone) || null;
  if (booking.num_adult !== undefined) patch.num_adults = Number(booking.num_adult);
  if (booking.num_child !== undefined) patch.num_children = Number(booking.num_child);
  if (booking.status !== undefined) patch.status = normalizeText(booking.status) || existingBooking.status;
  if (booking.price !== undefined) patch.total_price = Number(booking.price);

  const result = await supabase
    .from('bookings')
    .update(patch)
    .eq('beds24_booking_id', bookingId);

  if (result.error) {
    console.error('[LOCAL-BOOKING-UPDATE-FAIL]', result.error.message);
    return {
      ok: false,
      warning: 'Beds24 updated successfully but local bookings cache update failed.',
    };
  }
  return { ok: true, warning: booking.notes !== undefined ? 'Booking note saved only in raw_payload local cache.' : '' };
}

async function cancelLocalBooking({ supabase, bookingId, existingBooking, beds24Result }) {
  if (!existingBooking) {
    console.warn('[LOCAL-BOOKING-CANCEL-SKIP] booking not found locally', bookingId);
    return {
      ok: false,
      warning: 'Beds24 deleted successfully but local booking was not found to cancel.',
    };
  }

  const result = await supabase
    .from('bookings')
    .update({
      status: 'cancelled',
      source_updated_at: new Date().toISOString(),
      synced_at: new Date().toISOString(),
      raw_payload: {
        ...(existingBooking.raw_payload || {}),
        last_beds24_delete_response: beds24Result,
        last_manual_delete: true,
      },
    })
    .eq('beds24_booking_id', bookingId);

  if (result.error) {
    console.error('[LOCAL-BOOKING-CANCEL-FAIL]', result.error.message);
    return {
      ok: false,
      warning: 'Beds24 deleted successfully but local bookings cache update failed.',
    };
  }

  return { ok: true, warning: '' };
}

async function fetchLocalBookingByBeds24Id(supabase, beds24BookingId) {
  if (!normalizeText(beds24BookingId)) return null;
  const result = await supabase
    .from('bookings')
    .select('*')
    .eq('beds24_booking_id', normalizeText(beds24BookingId))
    .maybeSingle();
  if (result.error) {
    console.warn('[LOCAL-BOOKING-FETCH-FAIL]', result.error.message);
    return null;
  }
  return result.data || null;
}

async function applyResidenceMirrorMutation({ supabase, operation, booking, existingBooking, unit }) {
  const rmBookingId = getResidenceMirrorBookingId(existingBooking);
  if (!rmBookingId) {
    return {
      ok: false,
      statusCode: 400,
      error: 'Calendario Kekko mapping mancante per questa prenotazione',
    };
  }

  if (operation === 'delete' || !unit || isCancelledLikeStatus(booking.status)) {
    const deleteResult = await supabase
      .from('rm_bookings')
      .delete()
      .eq('id', rmBookingId);
    if (deleteResult.error) {
      return {
        ok: false,
        statusCode: 500,
        error: 'Eliminazione su calendario Kekko fallita',
        detail: deleteResult.error.message,
      };
    }
  } else {
    const mappingResult = await supabase
      .from('rm_internal_unit_sync_targets')
      .select('rm_apartment_id')
      .eq('source_apartment_unit_id', unit.id)
      .eq('active', true)
      .maybeSingle();
    if (mappingResult.error) {
      return {
        ok: false,
        statusCode: 500,
        error: 'Lookup sync target Kekko fallito',
        detail: mappingResult.error.message,
      };
    }
    if (!mappingResult.data?.rm_apartment_id) {
      return {
        ok: false,
        statusCode: 400,
        error: 'Unita non collegata al calendario Kekko',
      };
    }

    const patch = {
      apartment_id: mappingResult.data.rm_apartment_id,
      name: buildResidenceNameFromBookingSnapshot({
        guest_first_name: booking.first_name ?? existingBooking?.guest_first_name,
        guest_last_name: booking.last_name ?? existingBooking?.guest_last_name,
      }),
      checkin: normalizeDate(booking.arrival) || normalizeDate(existingBooking?.check_in),
      checkout: normalizeDate(booking.departure) || normalizeDate(existingBooking?.check_out),
      pax: buildResidencePaxFromBookingSnapshot({
        num_adults: booking.num_adult ?? existingBooking?.num_adults,
        num_children: booking.num_child ?? existingBooking?.num_children,
      }),
    };
    const notes = normalizeText(booking.notes)
      || extractResidenceNotesFromBooking(existingBooking);
    if (notes) patch.notes = notes;

    const updateResult = await supabase
      .from('rm_bookings')
      .update(patch)
      .eq('id', rmBookingId);
    if (updateResult.error) {
      return {
        ok: false,
        statusCode: 500,
        error: 'Aggiornamento su calendario Kekko fallito',
        detail: updateResult.error.message,
      };
    }
  }

  const syncResult = await syncResidenceCalendarToPms(supabase);
  if (!syncResult.ok) return syncResult;
  return { ok: true, warning: '' };
}

async function syncPmsBookingIntoResidence({ supabase, beforeBooking, afterBooking }) {
  const unitIds = uniqueTexts([
    beforeBooking?.apartment_unit_id,
    afterBooking?.apartment_unit_id,
  ]);
  if (!unitIds.length) return { changed: false, warning: '' };

  const mappingResult = await supabase
    .from('rm_internal_unit_sync_targets')
    .select('rm_apartment_id,source_apartment_unit_id')
    .in('source_apartment_unit_id', unitIds)
    .eq('active', true);
  if (mappingResult.error) {
    console.warn('[RM-INTERNAL-SYNC-LOOKUP-FAIL]', mappingResult.error.message);
    return { changed: false, warning: 'Sync verso calendario Kekko non riuscita.' };
  }

  const mappingByUnitId = new Map(
    (mappingResult.data || []).map((row) => [normalizeText(row.source_apartment_unit_id), row]),
  );
  const beforeMapping = mappingByUnitId.get(normalizeText(beforeBooking?.apartment_unit_id)) || null;
  const afterMapping = mappingByUnitId.get(normalizeText(afterBooking?.apartment_unit_id)) || null;
  const beforeSyntheticId = beforeMapping && beforeBooking ? buildInternalResidenceSystemId(beforeBooking) : '';
  const afterSyntheticId = afterMapping && afterBooking ? buildInternalResidenceSystemId(afterBooking) : '';

  if (beforeSyntheticId && (!afterSyntheticId || isCancelledLikeStatus(afterBooking?.status))) {
    const deleteResult = await supabase
      .from('rm_bookings')
      .delete()
      .eq('id', beforeSyntheticId)
      .eq('source', 'system');
    if (deleteResult.error) {
      console.warn('[RM-INTERNAL-SYNC-DELETE-FAIL]', deleteResult.error.message);
      return { changed: false, warning: 'Prenotazione salvata, ma la rimozione dal calendario Kekko non e riuscita.' };
    }
  }

  if (!afterSyntheticId || !afterBooking || isCancelledLikeStatus(afterBooking.status)) {
    return { changed: Boolean(beforeSyntheticId), warning: '' };
  }

  const row = {
    id: afterSyntheticId,
    apartment_id: afterMapping.rm_apartment_id,
    name: buildResidenceNameFromBookingSnapshot(afterBooking),
    checkin: normalizeDate(afterBooking.check_in),
    checkout: normalizeDate(afterBooking.check_out),
    pax: buildResidencePaxFromBookingSnapshot(afterBooking),
    status: mapPmsStatusToResidenceStatus(afterBooking.status),
    source: 'system',
    notes: buildInternalResidenceSyncNotes(afterBooking),
  };
  const duplicateResult = await supabase
    .from('rm_bookings')
    .select('id,name,source,status')
    .eq('apartment_id', row.apartment_id)
    .eq('checkin', row.checkin)
    .eq('checkout', row.checkout)
    .neq('source', 'system')
    .not('status', 'eq', 'cancelled');
  if (duplicateResult.error) {
    console.warn('[RM-INTERNAL-SYNC-DUPLICATE-LOOKUP-FAIL]', duplicateResult.error.message);
  } else {
    const hasMatchingResidenceBooking = (duplicateResult.data || []).some((item) => (
      normalizeText(item?.name).toLowerCase() === normalizeText(row.name).toLowerCase()
    ));
    if (hasMatchingResidenceBooking) {
      const deleteDuplicateResult = await supabase
        .from('rm_bookings')
        .delete()
        .eq('id', afterSyntheticId)
        .eq('source', 'system');
      if (deleteDuplicateResult.error) {
        console.warn('[RM-INTERNAL-SYNC-DUPLICATE-DELETE-FAIL]', deleteDuplicateResult.error.message);
        return { changed: false, warning: 'Prenotazione salvata, ma la pulizia della doppia su calendario Kekko non e riuscita.' };
      }
      return { changed: true, warning: '' };
    }
  }
  const upsertResult = await supabase
    .from('rm_bookings')
    .upsert(row, { onConflict: 'id' });
  if (upsertResult.error) {
    console.warn('[RM-INTERNAL-SYNC-UPSERT-FAIL]', upsertResult.error.message);
    return { changed: false, warning: 'Prenotazione salvata, ma la sync verso calendario Kekko non e riuscita.' };
  }

  return { changed: true, warning: '' };
}

async function syncResidenceCalendarToPms(supabase) {
  const result = await supabase.rpc('sync_rm_calendar_to_pms');
  if (result.error) {
    console.error('[RM-CALENDAR-TO-PMS-SYNC-FAIL]', result.error.message);
    return {
      ok: false,
      statusCode: 502,
      error: 'Sync immediata con il calendario interno fallita',
      detail: result.error.message,
    };
  }
  return { ok: true };
}

function listModifiedFields(booking) {
  const fieldMap = {
    apartment_unit_id: 'apartment_unit_id',
    arrival: 'arrival',
    departure: 'departure',
    first_name: 'first_name',
    last_name: 'last_name',
    email: 'email',
    phone: 'phone',
    num_adult: 'num_adult',
    num_child: 'num_child',
    price: 'price',
    notes: 'notes',
    status: 'status',
  };
  return Object.keys(fieldMap).filter((key) => booking[key] !== undefined);
}

async function getBeds24AccessToken(refreshToken) {
  if (beds24TokenCache && beds24TokenCache.expiresAt > Date.now() + 15 * 1000) {
    return beds24TokenCache.token;
  }

  const response = await fetch(`${BEDS24_URL}/authentication/token`, {
    method: 'GET',
    headers: {
      refreshToken,
      Accept: 'application/json',
    },
  });

  if (!response.ok) {
    const detail = await safeReadText(response);
    throw new Error(`Autenticazione Beds24 fallita (${response.status}): ${detail}`);
  }

  const data = await response.json();
  const token = String(data?.token || data?.data?.token || '').trim();
  if (!token) {
    throw new Error('Token Beds24 non presente nella risposta di autenticazione');
  }

  const expiresInSeconds = Number(data?.expiresIn || data?.data?.expiresIn || 3600);
  beds24TokenCache = {
    token,
    expiresAt: Date.now() + Math.max(60, expiresInSeconds - 60) * 1000,
  };
  return beds24TokenCache.token;
}

function extractBookingId(payload) {
  if (!payload) return null;
  const rows = Array.isArray(payload) ? payload : [payload];
  for (const row of rows) {
    const direct = row?.id ?? row?.bookingId ?? row?.booking_id;
    if (direct != null && String(direct).trim()) return String(direct).trim();
    const nested = row?.new?.id ?? row?.new?.bookingId ?? row?.new?.booking_id;
    if (nested != null && String(nested).trim()) return String(nested).trim();
    if (Array.isArray(row?.info)) {
      for (const infoRow of row.info) {
        const infoId = infoRow?.id ?? infoRow?.bookingId ?? infoRow?.booking_id;
        if (infoId != null && String(infoId).trim()) return String(infoId).trim();
      }
    }
  }
  return null;
}

async function parseJsonSafely(response) {
  const text = await safeReadText(response);
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch (_error) {
    return { raw: text };
  }
}

function normalizeText(value) {
  if (value == null) return '';
  return String(value).trim();
}

function normalizeDate(value) {
  const text = normalizeText(value);
  return isIsoDate(text) ? text : '';
}

function isIsoDate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || ''));
}

function getResidenceMirrorBookingId(booking) {
  const rawId = normalizeText(booking?.raw_payload?.rm_booking_id);
  if (rawId) return rawId;
  const prefixedBookingId = normalizeText(booking?.beds24_booking_id);
  if (prefixedBookingId.toLowerCase().startsWith('rmk:')) {
    return prefixedBookingId.slice(4);
  }
  return '';
}

function isResidenceMirrorBooking(booking) {
  if (!getResidenceMirrorBookingId(booking)) return false;
  const source = normalizeText(booking?.source).toLowerCase();
  const channel = normalizeText(booking?.channel).toLowerCase();
  return source === 'rm-calendar' || channel === 'calendario kekko';
}

function buildResidenceNameFromBookingSnapshot(booking) {
  const first = normalizeText(booking?.guest_first_name ?? booking?.first_name);
  const last = normalizeText(booking?.guest_last_name ?? booking?.last_name);
  return [first, last].filter(Boolean).join(' ').trim() || 'Ospite PMS';
}

function buildResidencePaxFromBookingSnapshot(booking) {
  const adults = Number(booking?.num_adults ?? booking?.num_adult ?? 0);
  const children = Number(booking?.num_children ?? booking?.num_child ?? 0);
  return Math.max(1, adults + children);
}

function extractResidenceNotesFromBooking(booking) {
  return normalizeText(
    booking?.raw_payload?.last_manual_request?.notes
      || booking?.raw_payload?.manual_request?.notes
      || booking?.raw_payload?.notes
      || '',
  );
}

function mapPmsStatusToResidenceStatus(status) {
  const normalized = normalizeText(status).toLowerCase();
  if (normalized === 'black') return 'staff';
  if (normalized === 'request' || normalized === 'new' || normalized === 'inquiry') return 'pending';
  if (normalized === 'cancelled') return 'cancelled';
  return 'confirmed';
}

function buildInternalResidenceSyncNotes(booking) {
  return [
    'Sync automatico interno da PMS',
    `PMS booking: ${normalizeText(booking?.beds24_booking_id) || normalizeText(booking?.id) || 'n/d'}`,
    `Canale: ${normalizeText(booking?.channel) || 'n/d'}`,
    `Stato PMS: ${normalizeText(booking?.status) || 'n/d'}`,
  ].join(' | ');
}

function buildInternalResidenceSystemId(booking) {
  const seed = normalizeText(booking?.beds24_booking_id) || normalizeText(booking?.id);
  return stableUuid(`rminternal|${seed}`);
}

function stableUuid(seed) {
  const hash = crypto.createHash('md5').update(String(seed || '')).digest('hex');
  return `${hash.slice(0, 8)}-${hash.slice(8, 12)}-${hash.slice(12, 16)}-${hash.slice(16, 20)}-${hash.slice(20, 32)}`;
}

function uniqueTexts(values) {
  return [...new Set((values || []).map((value) => normalizeText(value)).filter(Boolean))];
}

function isCancelledLikeStatus(status) {
  return normalizeText(status).toLowerCase() === 'cancelled';
}

function mergeWarnings(...warnings) {
  return warnings.map((value) => normalizeText(value)).filter(Boolean).join(' ');
}

function compactObject(obj) {
  return Object.fromEntries(
    Object.entries(obj).filter(([, value]) => value !== undefined && value !== ''),
  );
}

function truncateForLog(value) {
  const text = String(value || '');
  return text.length > 2000 ? `${text.slice(0, 2000)}…` : text;
}

async function safeReadText(response) {
  try {
    return await response.text();
  } catch (_error) {
    return '';
  }
}

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}
