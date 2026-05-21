/**
 * Sync read-only -> write controllata delle booking Beds24 verso Supabase.
 * Parametri: dryRun=1|0, apartmentId, propertyId, from, to, modifiedSince, onlyResidences=1.
 * Esempio curl dry-run: curl "http://localhost:8888/.netlify/functions/beds24-sync-bookings?dryRun=1"
 * Esempio curl apartment: curl "http://localhost:8888/.netlify/functions/beds24-sync-bookings?apartmentId=UUID&dryRun=1"
 * Esempio curl property: curl "http://localhost:8888/.netlify/functions/beds24-sync-bookings?propertyId=324753&onlyResidences=1&dryRun=1"
 * Usa BEDS24_API_KEY con lo stesso pattern auth del progetto: scambio token via /authentication/token.
 * Dry-run e il default: fare sempre preview prima di qualsiasi execute.
 * Scrive solo su bookings, sync_jobs e sync_state. Nessun DELETE, nessun cron, nessun webhook.
 */

const { createClient } = require('@supabase/supabase-js');
const {
  getBearerToken,
  getSupabaseUserFromToken,
  normalizeSupabaseUrl,
} = require('./_lib/shared-auth');
const { resolvePmsCalendarScope } = require('./_lib/pms-calendar-access');

const BEDS24_URL = 'https://api.beds24.com/v2';
const OCCUPYING_STATUSES = new Set(['confirmed', 'new', 'request', 'black']);

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Content-Type': 'application/json; charset=utf-8',
};

let beds24TokenCache = null;

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers: CORS, body: '' };
  }

  if (event.httpMethod !== 'GET') {
    return respond(405, { error: 'Method not allowed' });
  }

  const query = event.queryStringParameters || {};
  const dryRun = String(query.dryRun || '1').trim() !== '0';
  const apartmentId = normalizeText(query.apartmentId);
  const propertyId = normalizeText(query.propertyId);
  const onlyResidences = String(query.onlyResidences || '').trim() === '1';

  if (apartmentId && propertyId) {
    return respond(400, {
      error: 'Parametri incompatibili',
      detail: 'apartmentId e propertyId sono mutuamente esclusivi',
    });
  }

  const today = isoDateUtc(new Date());
  const from = normalizeDateString(query.from) || isoDateUtc(addDaysUtc(parseIsoDateUtc(today), -90));
  const to = normalizeDateString(query.to) || isoDateUtc(addDaysUtc(parseIsoDateUtc(today), 365));
  const modifiedSince = normalizeIsoString(query.modifiedSince);

  const env = {
    SUPABASE_URL: normalizeSupabaseUrl(process.env.SUPABASE_RUNTIME_URL || process.env.SUPABASE_URL),
    SUPABASE_SERVICE_ROLE_KEY: process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY,
    BEDS24_API_KEY: process.env.BEDS24_API_KEY,
  };

  if (!env.SUPABASE_URL || !env.SUPABASE_SERVICE_ROLE_KEY) {
    return respond(500, { error: 'Configurazione Supabase mancante' });
  }
  if (!env.BEDS24_API_KEY) {
    return respond(500, { error: 'BEDS24_API_KEY mancante' });
  }

  const token = getBearerToken(event.headers || {});
  if (!token) {
    return respond(401, { error: 'Unauthorized' });
  }

  const user = await getSupabaseUserFromToken(token);
  if (!user?.id) {
    return respond(401, { error: 'Unauthorized' });
  }

  const output = {
    mode: dryRun ? 'dry-run' : 'execute',
    filter: {
      apartmentId,
      propertyId,
      onlyResidences,
      modifiedSince,
    },
    window: { from, to },
    summary: {
      properties_processed: 0,
      bookings_received: 0,
      bookings_planned_create: 0,
      bookings_planned_update: 0,
      bookings_upserted: 0,
      bookings_skipped_stale: 0,
      bookings_orphaned: 0,
      orphan_breakdown: {
        room_not_mapped: 0,
        no_unit_available_in_range: 0,
      },
      assignments: {
        direct_match: 0,
        sticky: 0,
        first_fit: 0,
        orphan_no_mapping: 0,
        orphan_no_unit: 0,
      },
      errors: 0,
    },
    preview: [],
    orphans: [],
    errors_detail: [],
    sync_job_id: null,
  };

  const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);
  const scope = await resolvePmsCalendarScope(supabase, user.email || '');
  if (!scope.canEditAny) {
    return respond(403, {
      error: 'Forbidden: no editable calendar access configured',
      ...output,
    });
  }

  let syncJobId = null;
  let targetProperties = [];
  let targetApartmentIds = [];

  try {
    await getBeds24AccessToken(env);
  } catch (error) {
    return respond(500, {
      error: 'Autenticazione Beds24 fallita',
      detail: error.message,
      ...output,
    });
  }

  try {
    const mappings = await loadActiveMappings(supabase);
    const units = await loadActiveUnits(supabase);
    const indexes = buildIndexes(mappings, units);

    if (scope.isGlobalEditor) {
      targetProperties = determineTargetProperties({
        apartmentId,
        propertyId,
        onlyResidences,
        propertyToMappings: indexes.propertyToMappings,
        apartmentToProperties: indexes.apartmentToProperties,
      });

      targetApartmentIds = determineTargetApartmentIds({
        apartmentId,
        targetProperties,
        propertyToMappings: indexes.propertyToMappings,
      });
    } else {
      const allowedApartmentIds = apartmentId
        ? [apartmentId]
        : scope.editableApartmentIds.slice();

      if (apartmentId && !scope.editableApartmentIds.includes(apartmentId)) {
        return respond(403, {
          error: 'Forbidden: apartment not editable for this user',
          ...output,
        });
      }

      const allowedProperties = [...new Set(
        allowedApartmentIds.flatMap((id) => Array.from(indexes.apartmentToProperties.get(id) || []))
      )];

      if (propertyId && !allowedProperties.includes(propertyId)) {
        return respond(403, {
          error: 'Forbidden: property not editable for this user',
          ...output,
        });
      }

      targetProperties = propertyId ? [propertyId] : allowedProperties;
      targetApartmentIds = propertyId
        ? allowedApartmentIds.filter((id) => Array.from(indexes.apartmentToProperties.get(id) || []).includes(propertyId))
        : allowedApartmentIds;
    }

    if (!targetProperties.length) {
      return respond(404, {
        error: 'Nessuna property target trovata',
        detail: 'Verifica filtri e mapping attivi in channel_property_mappings',
        ...output,
      });
    }

    if (!dryRun) {
      syncJobId = await createSyncJob(supabase, {
        apartmentId,
        propertyId,
        from,
        to,
      });
      output.sync_job_id = syncJobId;
    }

    for (const targetPropertyId of targetProperties) {
      let beds24Bookings = [];
      try {
        const payload = await fetchBeds24Json(
          buildBookingsPath({
            propertyId: targetPropertyId,
            from,
            to,
            modifiedSince,
          }),
          env
        );
        beds24Bookings = asDataArray(payload);
      } catch (error) {
        output.summary.errors += 1;
        output.errors_detail.push({
          type: 'beds24_property_fetch_error',
          property_id: targetPropertyId,
          message: error.message,
        });
        continue;
      }

      output.summary.properties_processed += 1;
      output.summary.bookings_received += beds24Bookings.length;

      const existingRows = await loadExistingBookingsForProperty(supabase, targetPropertyId);
      const propertyState = buildExistingState(existingRows);
      const normalizedBookings = beds24Bookings
        .map((booking) => normalizeIncomingBooking(booking, targetPropertyId))
        .filter((booking) => booking !== null)
        .sort(compareNormalizedBookings);

      for (const booking of normalizedBookings) {
        const existing = propertyState.existingByBookingId.get(booking.beds24_booking_id) || null;
        const stale = existing && isIncomingStale(existing.source_updated_at, booking.source_updated_at);

        if (stale) {
          output.summary.bookings_skipped_stale += 1;
          output.preview.push(buildPreviewRow({
            booking,
            action: 'skip_stale',
            assignment: {
              method: existing.apartment_unit_id ? 'sticky' : 'orphan_no_mapping',
              reason: null,
              selected_unit_label: null,
            },
          }));
          continue;
        }

        const assignment = assignBooking({
          booking,
          existing,
          roomToUnits: indexes.roomToUnits,
          activeUnitById: indexes.activeUnitById,
          occupancyByUnit: propertyState.occupancyByUnit,
        });

        const row = buildBookingRow(booking, assignment);
        const action = existing ? 'update' : 'create';

        if (action === 'create') {
          output.summary.bookings_planned_create += 1;
        } else {
          output.summary.bookings_planned_update += 1;
        }

        output.summary.assignments[assignment.method] += 1;
        if (assignment.method === 'orphan_no_mapping' || assignment.method === 'orphan_no_unit') {
          output.summary.bookings_orphaned += 1;
          if (assignment.reason && output.summary.orphan_breakdown[assignment.reason] !== undefined) {
            output.summary.orphan_breakdown[assignment.reason] += 1;
          }
          output.orphans.push({
            booking_id: booking.beds24_booking_id,
            property_id: booking.beds24_property_id,
            room_id: booking.beds24_room_id,
            arrival: booking.check_in,
            departure: booking.check_out,
            status: booking.status,
            reason: assignment.reason,
            method: assignment.method,
          });
        }

        if (existing && !stale) {
          removeOccupancyForBooking(propertyState.occupancyByUnit, booking.beds24_booking_id);
        }

        if (assignment.apartment_unit_id && OCCUPYING_STATUSES.has(booking.status)) {
          addOccupancy(propertyState.occupancyByUnit, assignment.apartment_unit_id, {
            bookingId: booking.beds24_booking_id,
            checkIn: booking.check_in,
            checkOut: booking.check_out,
          });
        }

        output.preview.push(buildPreviewRow({
          booking,
          action: assignment.method.startsWith('orphan_') ? 'orphan' : action,
          assignment,
        }));

        if (!dryRun) {
          try {
            const { error } = await supabase
              .from('bookings')
              .upsert(row, {
                onConflict: 'beds24_booking_id',
              });
            if (error) throw error;
            output.summary.bookings_upserted += 1;
          } catch (error) {
            output.summary.errors += 1;
            output.errors_detail.push({
              type: 'booking_upsert_error',
              property_id: booking.beds24_property_id,
              booking_id: booking.beds24_booking_id,
              message: error.message,
            });
          }
        }
      }
    }

    if (!dryRun && syncJobId) {
      const finalStatus = output.summary.errors > 0
        ? (output.summary.bookings_upserted > 0 ? 'partial' : 'failed')
        : 'success';

      await finalizeSyncJob(supabase, syncJobId, {
        status: finalStatus,
        rows_read: output.summary.bookings_received,
        rows_upserted: output.summary.bookings_upserted,
        rows_skipped_stale: output.summary.bookings_skipped_stale,
        rows_orphaned: output.summary.bookings_orphaned,
        error_message: output.summary.errors > 0 ? summarizeErrors(output.errors_detail) : null,
      });

      await updateSyncState(supabase, {
        apartmentIds: targetApartmentIds,
        modifiedSince,
        hasErrors: output.summary.errors > 0,
        errorMessage: output.summary.errors > 0 ? summarizeErrors(output.errors_detail) : null,
      });
    }

    const responseStatus = output.summary.errors > 0 && output.summary.properties_processed === 0 ? 500 : 200;
    return respond(responseStatus, output);
  } catch (error) {
    output.summary.errors += 1;

    if (!dryRun && syncJobId) {
      await safeFinalizeSyncJob(supabase, syncJobId, {
        status: 'failed',
        rows_read: output.summary.bookings_received,
        rows_upserted: output.summary.bookings_upserted,
        rows_skipped_stale: output.summary.bookings_skipped_stale,
        rows_orphaned: output.summary.bookings_orphaned,
        error_message: error.message,
      });
      await safeUpdateSyncState(supabase, {
        apartmentIds: targetApartmentIds,
        modifiedSince,
        hasErrors: true,
        errorMessage: error.message,
      });
    }

    return respond(500, {
      error: 'Beds24 sync bookings fallita',
      detail: error.message,
      ...output,
    });
  }
};

async function loadActiveMappings(supabase) {
  const { data, error } = await supabase
    .from('channel_property_mappings')
    .select('id,apartment_id,channel,external_property_id,external_room_id,external_room_qty,external_room_name,is_primary,active')
    .eq('active', true)
    .eq('channel', 'beds24')
    .order('external_property_id', { ascending: true })
    .order('external_room_id', { ascending: true });

  if (error) {
    throw new Error(`Errore lettura channel_property_mappings: ${error.message}`);
  }
  return Array.isArray(data) ? data : [];
}

async function loadActiveUnits(supabase) {
  const { data, error } = await supabase
    .from('apartment_units')
    .select('id,apartment_id,unit_label,room_type_label,beds24_room_id,beds24_unit_index,max_guests,active')
    .eq('active', true)
    .order('beds24_room_id', { ascending: true })
    .order('beds24_unit_index', { ascending: true })
    .order('unit_label', { ascending: true });

  if (error) {
    throw new Error(`Errore lettura apartment_units: ${error.message}`);
  }
  return Array.isArray(data) ? data : [];
}

function buildIndexes(mappings, units) {
  const propertyToMappings = new Map();
  const apartmentToProperties = new Map();
  const roomToUnits = new Map();
  const mappingToApartment = new Map();
  const activeUnitById = new Map();

  for (const mapping of mappings) {
    const propertyKey = normalizeText(mapping.external_property_id);
    const roomKey = normalizeText(mapping.external_room_id);
    if (!propertyKey || !roomKey) continue;

    if (!propertyToMappings.has(propertyKey)) propertyToMappings.set(propertyKey, []);
    propertyToMappings.get(propertyKey).push(mapping);

    if (!apartmentToProperties.has(mapping.apartment_id)) {
      apartmentToProperties.set(mapping.apartment_id, new Set());
    }
    apartmentToProperties.get(mapping.apartment_id).add(propertyKey);
    mappingToApartment.set(`${propertyKey}:${roomKey}`, mapping.apartment_id);
  }

  for (const unit of units) {
    const roomKey = normalizeText(unit.beds24_room_id);
    if (!roomKey) continue;
    if (!roomToUnits.has(roomKey)) roomToUnits.set(roomKey, []);
    roomToUnits.get(roomKey).push(unit);
    activeUnitById.set(unit.id, unit);
  }

  for (const unitsForRoom of roomToUnits.values()) {
    unitsForRoom.sort((a, b) => {
      const ai = normalizePositiveInt(a.beds24_unit_index) || 999999;
      const bi = normalizePositiveInt(b.beds24_unit_index) || 999999;
      if (ai !== bi) return ai - bi;
      return String(a.unit_label || '').localeCompare(String(b.unit_label || ''));
    });
  }

  return {
    propertyToMappings,
    apartmentToProperties,
    roomToUnits,
    mappingToApartment,
    activeUnitById,
  };
}

function determineTargetProperties({ apartmentId, propertyId, onlyResidences, propertyToMappings, apartmentToProperties }) {
  let properties;

  if (propertyId) {
    properties = [propertyId];
  } else if (apartmentId) {
    properties = Array.from(apartmentToProperties.get(apartmentId) || []);
  } else {
    properties = Array.from(propertyToMappings.keys());
  }

  if (onlyResidences) {
    properties = properties.filter((propId) => {
      const mappings = propertyToMappings.get(propId) || [];
      return mappings.some((mapping) => Number(mapping.external_room_qty || 0) > 1);
    });
  }

  return properties.sort();
}

function determineTargetApartmentIds({ apartmentId, targetProperties, propertyToMappings }) {
  if (apartmentId) return [apartmentId];
  const set = new Set();
  for (const propertyKey of targetProperties) {
    const mappings = propertyToMappings.get(propertyKey) || [];
    for (const mapping of mappings) {
      if (mapping.apartment_id) set.add(mapping.apartment_id);
    }
  }
  return Array.from(set);
}

function buildBookingsPath({ propertyId, from, to, modifiedSince }) {
  const params = new URLSearchParams({
    propertyId: String(propertyId),
    arrivalFrom: from,
    arrivalTo: to,
    includeInvoice: 'false',
  });
  if (modifiedSince) {
    params.set('modifiedSince', modifiedSince);
  }
  return `/bookings?${params.toString()}`;
}

async function loadExistingBookingsForProperty(supabase, propertyId) {
  const { data, error } = await supabase
    .from('bookings')
    .select('id,beds24_booking_id,apartment_unit_id,apartment_id,source_updated_at,check_in,check_out,status')
    .eq('beds24_property_id', String(propertyId));

  if (error) {
    throw new Error(`Errore lettura bookings esistenti (${propertyId}): ${error.message}`);
  }
  return Array.isArray(data) ? data : [];
}

function buildExistingState(rows) {
  const existingByBookingId = new Map();
  const occupancyByUnit = new Map();

  for (const row of rows) {
    const bookingId = normalizeText(row.beds24_booking_id);
    if (!bookingId) continue;
    existingByBookingId.set(bookingId, row);

    if (!row.apartment_unit_id) continue;
    if (!OCCUPYING_STATUSES.has(normalizeBookingStatus(row.status))) continue;

    addOccupancy(occupancyByUnit, row.apartment_unit_id, {
      bookingId,
      checkIn: normalizeDateString(row.check_in),
      checkOut: normalizeDateString(row.check_out),
    });
  }

  return { existingByBookingId, occupancyByUnit };
}

function normalizeIncomingBooking(rawBooking, fallbackPropertyId) {
  const beds24BookingId = normalizeText(firstDefined(rawBooking?.bookId, rawBooking?.id, rawBooking?.bookingId));
  const checkIn = normalizeDateString(firstDefined(rawBooking?.arrival, rawBooking?.checkIn));
  const checkOut = normalizeDateString(firstDefined(rawBooking?.departure, rawBooking?.checkOut));
  const roomId = normalizeText(firstDefined(rawBooking?.roomId, rawBooking?.roomID));
  const propertyId = normalizeText(firstDefined(rawBooking?.propertyId, rawBooking?.propertyID, fallbackPropertyId));

  if (!beds24BookingId || !checkIn || !checkOut || !roomId || !propertyId) {
    return null;
  }

  const originalUnitId = firstDefined(rawBooking?.unitId, rawBooking?.unitID);
  const normalizedStatus = normalizeBookingStatus(firstDefined(rawBooking?.status, rawBooking?.statusCode));
  const totalPrice = normalizeNullableNumber(
    firstDefined(rawBooking?.price, rawBooking?.totalPrice, rawBooking?.invoiceeAmount, rawBooking?.total)
  );
  const channel = normalizeText(
    firstDefined(rawBooking?.referer, rawBooking?.channel, rawBooking?.source, rawBooking?.apiSource)
  );
  const sourceUpdatedAt = normalizeIsoString(
    firstDefined(rawBooking?.modifiedTime, rawBooking?.modified, rawBooking?.bookingTime)
  ) || new Date().toISOString();
  const sourceCreatedAt = normalizeIsoString(firstDefined(rawBooking?.bookingTime, rawBooking?.created));

  return {
    raw: rawBooking,
    beds24_booking_id: beds24BookingId,
    beds24_property_id: propertyId,
    beds24_room_id: roomId,
    original_unit_id: originalUnitId,
    effective_unit_id: normalizeEffectiveUnitId(originalUnitId),
    check_in: checkIn,
    check_out: checkOut,
    status: normalizedStatus,
    channel,
    channel_normalized: normalizeChannel(channel, totalPrice),
    total_price: totalPrice,
    currency: normalizeText(firstDefined(rawBooking?.currency, rawBooking?.currencyCode)) || 'EUR',
    source_updated_at: sourceUpdatedAt,
    source_created_at: sourceCreatedAt,
    guest_first_name: normalizeText(firstDefined(rawBooking?.firstName, rawBooking?.guestFirstName, rawBooking?.fname)),
    guest_last_name: normalizeText(firstDefined(rawBooking?.lastName, rawBooking?.guestLastName, rawBooking?.lname)),
    guest_email: normalizeText(firstDefined(rawBooking?.email, rawBooking?.guestEmail, rawBooking?.mail)),
    guest_phone: normalizeText(firstDefined(rawBooking?.phone, rawBooking?.mobile, rawBooking?.guestPhone)),
    num_adults: normalizeNullableInt(firstDefined(rawBooking?.numAdult, rawBooking?.adults, rawBooking?.adult)),
    num_children: normalizeNullableInt(firstDefined(rawBooking?.numChild, rawBooking?.children, rawBooking?.child)),
  };
}

function assignBooking({ booking, existing, roomToUnits, activeUnitById, occupancyByUnit }) {
  const candidates = (roomToUnits.get(booking.beds24_room_id) || []).slice();

  if (existing && existing.apartment_unit_id) {
    const stickyUnit = activeUnitById.get(existing.apartment_unit_id);
    if (stickyUnit && normalizeText(stickyUnit.beds24_room_id) === booking.beds24_room_id) {
      return buildAssignment({
        method: 'sticky',
        reason: null,
        candidates,
        selectedUnit: stickyUnit,
        effectiveUnitId: booking.effective_unit_id,
        originalUnitId: booking.original_unit_id,
      });
    }
  }

  if (candidates.length === 0) {
    return buildAssignment({
      method: 'orphan_no_mapping',
      reason: 'room_not_mapped',
      candidates,
      selectedUnit: null,
      effectiveUnitId: booking.effective_unit_id,
      originalUnitId: booking.original_unit_id,
    });
  }

  if (candidates.length === 1) {
    const candidate = candidates[0];
    if (!hasOverlap(occupancyByUnit.get(candidate.id) || [], booking, booking.beds24_booking_id)) {
      return buildAssignment({
        method: 'direct_match',
        reason: null,
        candidates,
        selectedUnit: candidate,
        effectiveUnitId: booking.effective_unit_id,
        originalUnitId: booking.original_unit_id,
      });
    }

    return buildAssignment({
      method: 'orphan_no_unit',
      reason: 'no_unit_available_in_range',
      candidates,
      selectedUnit: null,
      effectiveUnitId: booking.effective_unit_id,
      originalUnitId: booking.original_unit_id,
    });
  }

  for (const candidate of candidates) {
    if (!hasOverlap(occupancyByUnit.get(candidate.id) || [], booking, booking.beds24_booking_id)) {
      return buildAssignment({
        method: 'first_fit',
        reason: null,
        candidates,
        selectedUnit: candidate,
        effectiveUnitId: booking.effective_unit_id,
        originalUnitId: booking.original_unit_id,
      });
    }
  }

  return buildAssignment({
    method: 'orphan_no_unit',
    reason: 'no_unit_available_in_range',
    candidates,
    selectedUnit: null,
    effectiveUnitId: booking.effective_unit_id,
    originalUnitId: booking.original_unit_id,
  });
}

function buildAssignment({ method, reason, candidates, selectedUnit, effectiveUnitId, originalUnitId }) {
  return {
    method,
    reason,
    candidates_considered: candidates.length,
    apartment_id: selectedUnit ? selectedUnit.apartment_id : null,
    apartment_unit_id: selectedUnit ? selectedUnit.id : null,
    selected_unit_index: selectedUnit ? normalizePositiveInt(selectedUnit.beds24_unit_index) : null,
    selected_unit_label: selectedUnit ? selectedUnit.unit_label || null : null,
    effective_unit_id: effectiveUnitId,
    original_unit_id: originalUnitId == null ? null : Number(originalUnitId),
  };
}

function buildBookingRow(booking, assignment) {
  return {
    beds24_booking_id: booking.beds24_booking_id,
    beds24_property_id: booking.beds24_property_id,
    beds24_room_id: booking.beds24_room_id,
    beds24_unit_id: booking.effective_unit_id,
    apartment_id: assignment.apartment_id,
    apartment_unit_id: assignment.apartment_unit_id,
    guest_first_name: booking.guest_first_name,
    guest_last_name: booking.guest_last_name,
    guest_email: booking.guest_email,
    guest_phone: booking.guest_phone,
    num_adults: booking.num_adults,
    num_children: booking.num_children,
    check_in: booking.check_in,
    check_out: booking.check_out,
    status: booking.status,
    channel: booking.channel,
    channel_normalized: booking.channel_normalized,
    total_price: booking.total_price,
    currency: booking.currency,
    source: 'beds24',
    source_updated_at: booking.source_updated_at,
    source_created_at: booking.source_created_at,
    synced_at: new Date().toISOString(),
    raw_payload: {
      ...booking.raw,
      __assignment: {
        method: assignment.method,
        reason: assignment.reason,
        candidates_considered: assignment.candidates_considered,
        selected_unit_index: assignment.selected_unit_index,
        selected_unit_label: assignment.selected_unit_label,
        effective_unit_id: assignment.effective_unit_id,
        original_unit_id: assignment.original_unit_id,
        decided_at: new Date().toISOString(),
      },
    },
  };
}

function buildPreviewRow({ booking, action, assignment }) {
  return {
    booking_id: booking.beds24_booking_id,
    property_id: booking.beds24_property_id,
    room_id: booking.beds24_room_id,
    arrival: booking.check_in,
    departure: booking.check_out,
    status: booking.status,
    channel_normalized: booking.channel_normalized,
    action,
    assignment_method: assignment.method,
    selected_unit_label: assignment.selected_unit_label || null,
    reason: assignment.reason || null,
  };
}

async function createSyncJob(supabase, { apartmentId, propertyId, from, to }) {
  const row = {
    scope: 'bookings',
    trigger: 'manual',
    target_apartment_id: apartmentId,
    target_external_property_id: propertyId,
    date_range_start: from,
    date_range_end: to,
    status: 'running',
    rows_read: 0,
    rows_upserted: 0,
    rows_skipped_stale: 0,
    rows_orphaned: 0,
    error_message: null,
    finished_at: null,
  };

  const { data, error } = await supabase
    .from('sync_jobs')
    .insert(row)
    .select('id')
    .single();

  if (error) {
    throw new Error(`Errore creazione sync_jobs: ${error.message}`);
  }
  return data.id;
}

async function finalizeSyncJob(supabase, syncJobId, payload) {
  const { error } = await supabase
    .from('sync_jobs')
    .update({
      status: payload.status,
      rows_read: payload.rows_read,
      rows_upserted: payload.rows_upserted,
      rows_skipped_stale: payload.rows_skipped_stale,
      rows_orphaned: payload.rows_orphaned,
      error_message: payload.error_message,
      finished_at: new Date().toISOString(),
    })
    .eq('id', syncJobId);

  if (error) {
    throw new Error(`Errore update sync_jobs: ${error.message}`);
  }
}

async function safeFinalizeSyncJob(supabase, syncJobId, payload) {
  try {
    await finalizeSyncJob(supabase, syncJobId, payload);
  } catch (error) {
    console.error('[beds24-sync-bookings] sync job finalize error', {
      syncJobId,
      message: error.message,
    });
  }
}

async function updateSyncState(supabase, { apartmentIds, modifiedSince, hasErrors, errorMessage }) {
  const timestamp = new Date().toISOString();
  for (const apartmentId of apartmentIds) {
    if (!apartmentId) continue;
    const base = {
      apartment_id: apartmentId,
      scope: 'bookings',
      updated_at: timestamp,
    };

    if (hasErrors) {
      const { data: existing, error: existingError } = await supabase
        .from('sync_state')
        .select('consecutive_errors')
        .eq('apartment_id', apartmentId)
        .eq('scope', 'bookings')
        .maybeSingle();
      if (existingError) {
        throw new Error(`Errore lettura sync_state (${apartmentId}): ${existingError.message}`);
      }

      const nextErrors = Number(existing?.consecutive_errors || 0) + 1;
      const row = {
        ...base,
        last_error: errorMessage,
        last_error_at: timestamp,
        consecutive_errors: nextErrors,
      };
      const { error: upsertError } = await supabase.from('sync_state').upsert(row, {
        onConflict: 'apartment_id,scope',
      });
      if (upsertError) {
        throw new Error(`Errore upsert sync_state (${apartmentId}): ${upsertError.message}`);
      }
      continue;
    }

    const row = {
      ...base,
      last_success_at: timestamp,
      last_error: null,
      last_error_at: null,
      consecutive_errors: 0,
    };
    if (modifiedSince) {
      row.last_incremental_sync_at = timestamp;
    } else {
      row.last_full_sync_at = timestamp;
    }
    const { error: upsertError } = await supabase.from('sync_state').upsert(row, {
      onConflict: 'apartment_id,scope',
    });
    if (upsertError) {
      throw new Error(`Errore upsert sync_state (${apartmentId}): ${upsertError.message}`);
    }
  }
}

async function safeUpdateSyncState(supabase, payload) {
  try {
    await updateSyncState(supabase, payload);
  } catch (error) {
    console.error('[beds24-sync-bookings] sync state update error', {
      message: error.message,
    });
  }
}

async function fetchBeds24Json(path, env) {
  const token = await getBeds24AccessToken(env);
  const res = await fetch(`${BEDS24_URL}${path}`, {
    method: 'GET',
    headers: {
      Accept: 'application/json',
      token,
    },
  });

  if (!res.ok) {
    const detail = await safeReadText(res);
    throw new Error(`Beds24 error ${res.status}: ${detail}`);
  }

  return await res.json();
}

async function getBeds24AccessToken(env) {
  if (beds24TokenCache && beds24TokenCache.expiresAt > Date.now() + 15 * 1000) {
    return beds24TokenCache.token;
  }

  const res = await fetch(`${BEDS24_URL}/authentication/token`, {
    method: 'GET',
    headers: {
      Accept: 'application/json',
      refreshToken: env.BEDS24_API_KEY,
    },
  });

  if (!res.ok) {
    const detail = await safeReadText(res);
    throw new Error(`Autenticazione Beds24 fallita (${res.status}): ${detail}`);
  }

  const payload = await res.json();
  const token = normalizeText(payload?.token) || normalizeText(payload?.data?.token);
  if (!token) {
    throw new Error('Token Beds24 non presente nella risposta di autenticazione');
  }

  const expiresInSeconds = Number(payload?.expiresIn || payload?.data?.expiresIn || 3600);
  beds24TokenCache = {
    token,
    expiresAt: Date.now() + Math.max(60, expiresInSeconds - 60) * 1000,
  };
  return beds24TokenCache.token;
}

function addOccupancy(occupancyByUnit, unitId, interval) {
  if (!unitId || !interval.checkIn || !interval.checkOut) return;
  if (!occupancyByUnit.has(unitId)) occupancyByUnit.set(unitId, []);
  occupancyByUnit.get(unitId).push(interval);
}

function removeOccupancyForBooking(occupancyByUnit, bookingId) {
  if (!bookingId) return;
  for (const [unitId, intervals] of occupancyByUnit.entries()) {
    const filtered = intervals.filter((interval) => interval.bookingId !== bookingId);
    if (filtered.length > 0) {
      occupancyByUnit.set(unitId, filtered);
    } else {
      occupancyByUnit.delete(unitId);
    }
  }
}

function hasOverlap(intervals, booking, ignoreBookingId) {
  const start = parseIsoDateUtc(booking.check_in);
  const end = parseIsoDateUtc(booking.check_out);
  if (!start || !end) return true;

  for (const interval of intervals) {
    if (ignoreBookingId && interval.bookingId === ignoreBookingId) continue;
    const otherStart = parseIsoDateUtc(interval.checkIn);
    const otherEnd = parseIsoDateUtc(interval.checkOut);
    if (!otherStart || !otherEnd) continue;
    if (start < otherEnd && otherStart < end) {
      return true;
    }
  }
  return false;
}

function isIncomingStale(existingSourceUpdatedAt, incomingSourceUpdatedAt) {
  const existingTime = Date.parse(existingSourceUpdatedAt || '');
  const incomingTime = Date.parse(incomingSourceUpdatedAt || '');
  if (!Number.isFinite(existingTime) || !Number.isFinite(incomingTime)) return false;
  return existingTime >= incomingTime;
}

function compareNormalizedBookings(a, b) {
  const dateCompare = String(a.check_in).localeCompare(String(b.check_in));
  if (dateCompare !== 0) return dateCompare;
  const aNum = Number(a.beds24_booking_id);
  const bNum = Number(b.beds24_booking_id);
  if (Number.isFinite(aNum) && Number.isFinite(bNum)) {
    return aNum - bNum;
  }
  return String(a.beds24_booking_id).localeCompare(String(b.beds24_booking_id));
}

function normalizeBookingStatus(value) {
  if (value === 1 || value === '1') return 'confirmed';
  if (value === 2 || value === '2') return 'new';
  if (value === 3 || value === '3') return 'request';
  if (value === 0 || value === '0') return 'cancelled';

  const text = normalizeText(value);
  if (!text) return 'unknown';
  const lower = text.toLowerCase();

  if (lower === 'confirmed') return 'confirmed';
  if (lower === 'new') return 'new';
  if (lower === 'request') return 'request';
  if (lower === 'cancelled' || lower === 'cancelled_due_to_payment') return 'cancelled';
  if (lower === 'inquiry') return 'inquiry';
  if (lower === 'black') return 'black';

  return lower;
}

function normalizeChannel(channel, totalPrice) {
  const raw = normalizeText(channel);
  if (!raw) return null;
  const lower = raw.toLowerCase();

  if (lower.includes('booking')) return 'booking';
  if (lower.includes('airbnb')) return 'airbnb';
  if (lower.includes('expedia')) return 'expedia';
  if (lower === 'direct') return 'direct';
  if (lower === 'api') {
    return Number(totalPrice || 0) > 0 ? 'api' : 'block_or_internal';
  }
  return lower.replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '') || lower;
}

function normalizeEffectiveUnitId(value) {
  const numeric = normalizeNullableInt(value);
  if (!numeric || numeric === 0) return 1;
  return numeric;
}

function normalizeText(value) {
  const text = String(value ?? '').trim();
  return text ? text : null;
}

function normalizeDateString(value) {
  const text = normalizeText(value);
  if (!text) return null;
  const match = text.match(/^(\d{4}-\d{2}-\d{2})/);
  return match ? match[1] : null;
}

function normalizeIsoString(value) {
  const text = normalizeText(value);
  if (!text) return null;
  const time = Date.parse(text);
  if (!Number.isFinite(time)) return null;
  return new Date(time).toISOString();
}

function normalizePositiveInt(value) {
  if (value === null || value === undefined || value === '') return null;
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  const int = Math.trunc(num);
  return int > 0 ? int : null;
}

function normalizeNullableInt(value) {
  if (value === null || value === undefined || value === '') return null;
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  return Math.trunc(num);
}

function normalizeNullableNumber(value) {
  if (value === null || value === undefined || value === '') return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function firstDefined(...values) {
  for (const value of values) {
    if (value !== undefined && value !== null) return value;
  }
  return null;
}

function summarizeErrors(errors) {
  if (!Array.isArray(errors) || !errors.length) return null;
  return errors.map((item) => `${item.type}: ${item.message}`).slice(0, 5).join(' | ');
}

function asDataArray(payload) {
  if (Array.isArray(payload?.data)) return payload.data;
  if (Array.isArray(payload)) return payload;
  return [];
}

function parseIsoDateUtc(value) {
  const text = normalizeDateString(value);
  if (!text) return null;
  return new Date(`${text}T00:00:00Z`);
}

function addDaysUtc(date, days) {
  const clone = new Date(date.getTime());
  clone.setUTCDate(clone.getUTCDate() + days);
  return clone;
}

function isoDateUtc(date) {
  return date.toISOString().slice(0, 10);
}

async function safeReadText(res) {
  try {
    return await res.text();
  } catch (_error) {
    return '';
  }
}

function respond(statusCode, body) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(body, null, 2),
  };
}
