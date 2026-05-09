/**
 * Read-only diagnostica Beds24 per ispezionare la struttura reale dell'account.
 * Esempio browser: /.netlify/functions/beds24-inspect?propertyId=324764
 * Esempio browser: /.netlify/functions/beds24-inspect?all=1
 * Esempio browser: /.netlify/functions/beds24-inspect?focus=multiunit
 * Esempio browser: /.netlify/functions/beds24-inspect?focus=multiunit&propertyId=324764
 * Esempio curl: curl "http://localhost:8888/.netlify/functions/beds24-inspect?propertyId=324764"
 * Env usate: solo BEDS24_API_KEY, seguendo il pattern gia presente nel progetto.
 * Il valore di BEDS24_API_KEY sembra usato come refresh token o token API riutilizzabile.
 * Con ?focus=multiunit la finestra booking si allarga automaticamente.
 * Nessuna scrittura Supabase, nessuna scrittura Beds24, nessun side effect.
 * Restituisce solo JSON pretty-printed; status 200 se almeno una parte diagnostica riesce.
 */

const BEDS24_URL = 'https://api.beds24.com/v2';
const MAX_PROPERTIES_ALL = 8;
const BOOKINGS_SINGLE_LIMIT = 10;
const BOOKINGS_ALL_LIMIT = 3;
const OFFERS_SINGLE_LIMIT = 3;
const OFFERS_ALL_LIMIT = 1;

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
  const propertyId = String(query.propertyId || '').trim();
  const inspectAll = String(query.all || '').trim() === '1';
  const focus = String(query.focus || '').trim().toLowerCase();
  const focusMultiunit = focus === 'multiunit';
  if (!propertyId && !inspectAll && !focusMultiunit) {
    return respond(400, {
      error: 'Parametri mancanti',
      detail: 'Passa ?propertyId=XXX oppure ?all=1 oppure ?focus=multiunit',
    });
  }

  const rawAuthKey = String(process.env.BEDS24_API_KEY || '').trim();
  if (!rawAuthKey) {
    return respond(500, {
      error: 'Configurazione Beds24 mancante',
      detail: 'Variabile env richiesta: BEDS24_API_KEY',
    });
  }

  const today = isoDateUtc(new Date());
  const calendarTo = isoDateUtc(addDaysUtc(parseIsoDateUtc(today), 13));
  const nextMonthStart = startOfNextMonthUtc(today);
  const nextMonthEnd = endOfMonthUtc(nextMonthStart);
  const offerDeparture = isoDateUtc(addDaysUtc(parseIsoDateUtc(today), 1));
  const bookingsPast30 = isoDateUtc(addDaysUtc(parseIsoDateUtc(today), -30));
  const bookingsPast90 = isoDateUtc(addDaysUtc(parseIsoDateUtc(today), -90));
  const bookingsPlus90 = isoDateUtc(addDaysUtc(parseIsoDateUtc(today), 90));
  const bookingsPlus180 = isoDateUtc(addDaysUtc(parseIsoDateUtc(today), 180));

  const authMeta = buildAuthMeta(rawAuthKey);
  const output = {
    requested_at: new Date().toISOString(),
    mode: propertyId ? 'single-property' : 'all-properties',
    request: {
      propertyId: propertyId || null,
      all: inspectAll,
      focus: focus || null,
      limits: {
        max_properties_all: MAX_PROPERTIES_ALL,
        bookings_single_limit: BOOKINGS_SINGLE_LIMIT,
        bookings_all_limit: BOOKINGS_ALL_LIMIT,
        offers_single_limit: OFFERS_SINGLE_LIMIT,
        offers_all_limit: OFFERS_ALL_LIMIT,
      },
      windows: {
        bookings_next_month: {
          arrivalFrom: nextMonthStart,
          arrivalTo: nextMonthEnd,
        },
        calendar_next_14_days: {
          from: today,
          to: calendarTo,
        },
        offers_probe: {
          arrival: today,
          departure: offerDeparture,
          numAdults: 2,
        },
      },
    },
    auth: {
      env_used: 'BEDS24_API_KEY',
      pattern_auth_used: authMeta.pattern_auth_used,
      token_masked: authMeta.token_masked,
      token_hint: authMeta.token_hint,
      warnings: authMeta.warnings,
    },
    legacy_calendar_audit: {
      found: true,
      file: '/Users/impattosrl/Desktop/fatture-impatto/app.html',
      route: '/app.html',
      status: 'attivo ma isolato dal backoffice principale',
      reuse_as_ui: 'si, come riferimento UI ma non come backend definitivo',
      leave_isolated_now: 'si',
      future_delete_recommendation: 'non ora; rivalutare solo dopo il nuovo calendario centralizzato su cache Supabase',
      notes: [
        'Usa get-calendar in lettura live verso Beds24.',
        'Non e la route backoffice centrale richiesta.',
        'Può essere riusato in parte come UX/timeline, ma non come base dati.',
      ],
    },
    diagnostics: {
      auth_probe: null,
      endpoint_support: [],
      warnings: [],
      errors: [],
    },
    properties: [],
    multiunit_focus: null,
    multi_unit_detection: {
      has_multi_unit: false,
      suspicious_properties: [],
      reasons_catalog: [
        'roomTypes > 1',
        'unitId presente nei booking',
        'subRooms/units presenti nei payload',
        'nomi room duplicati o molto simili',
        'calendarRoomId diversi dai roomId osservati',
      ],
    },
    bookings_sample: [],
    calendar_sample: [],
    offers_sample: [],
    modified_since_test: {
      supported: 'unknown',
      endpoint: '/bookings',
      params: null,
      result_summary: null,
      error: null,
    },
    fields_inventory: {
      property_fields: [],
      room_fields: [],
      booking_fields: [],
      calendar_fields: [],
      offers_fields: [],
    },
    summary: null,
  };

  try {
    output.diagnostics.auth_probe = await probeAuth(rawAuthKey);
  } catch (error) {
    return respond(500, {
      error: 'Diagnostica Beds24 fallita',
      detail: error.message,
      auth: output.auth,
      diagnostics: {
        auth_probe: {
          ok: false,
          error: error.message,
        },
      },
    });
  }

  let propertiesPayload;
  try {
    if (propertyId) {
      propertiesPayload = await fetchBeds24Json(`/properties?id=${encodeURIComponent(propertyId)}&includeAllRooms=true`);
    } else {
      propertiesPayload = await fetchBeds24Json('/properties?includeAllRooms=true');
    }
  } catch (error) {
    return respond(500, {
      error: 'Diagnostica Beds24 fallita',
      detail: error.message,
      auth: output.auth,
      diagnostics: output.diagnostics,
    });
  }

  let properties = asDataArray(propertiesPayload);
  if (propertyId) {
    properties = properties.filter((item) => String(item?.id || '') === propertyId);
  }
  const effectiveLimit = focusMultiunit ? 100 : MAX_PROPERTIES_ALL;
  if (inspectAll && properties.length > effectiveLimit) {
    output.diagnostics.warnings.push(`Modalita all=1 limitata alle prime ${effectiveLimit} property.`);
    properties = properties.slice(0, effectiveLimit);
  }
  if (!properties.length) {
    return respond(500, {
      error: 'Nessuna property Beds24 disponibile per la diagnostica richiesta',
      auth: output.auth,
      diagnostics: output.diagnostics,
    });
  }

  if (focusMultiunit) {
    const multiunitOutput = await inspectMultiunitFocus({
      properties,
      propertyId,
      bookingsFrom: bookingsPast90,
      bookingsTo: bookingsPlus180,
      calendarFrom: today,
      calendarTo,
      output,
    });

    output.multiunit_focus = multiunitOutput;
    output.summary = multiunitOutput.summary;

    if (!multiunitOutput.filtered_properties.length) {
      output.diagnostics.warnings.push('Nessuna property filtrata come multi-unit con le regole correnti.');
    }

    return respond(200, output);
  }

  const inspectionResults = [];
  for (const property of properties) {
    const result = await inspectProperty({
      property,
      inspectAll,
      calendarFrom: today,
      calendarTo,
      bookingFrom: nextMonthStart,
      bookingTo: nextMonthEnd,
      offerArrival: today,
      offerDeparture,
      output,
    });
    inspectionResults.push(result);
  }

  output.properties = inspectionResults.map((item) => item.property_summary);
  output.bookings_sample = inspectionResults.flatMap((item) => item.bookings_sample);
  output.calendar_sample = inspectionResults.flatMap((item) => item.calendar_sample);
  output.offers_sample = inspectionResults.flatMap((item) => item.offers_sample);
  output.multi_unit_detection = buildMultiUnitDetection(inspectionResults);
  output.fields_inventory = mergeFieldInventories(inspectionResults);

  const modifiedSinceProbe = await probeModifiedSince({
    propertyId: String(properties[0]?.id || ''),
    bookingFrom: nextMonthStart,
    bookingTo: nextMonthEnd,
  });
  output.modified_since_test = modifiedSinceProbe;
  output.diagnostics.endpoint_support.push({
    name: 'modified_since_test',
    ok: modifiedSinceProbe.supported !== false,
    detail: modifiedSinceProbe.result_summary || modifiedSinceProbe.error || null,
  });

  const authDetailsProbe = await probeOptionalEndpoint({
    name: 'authentication_details',
    path: '/authentication/details',
    transform: (payload) => ({
      top_level_keys: Object.keys(payload || {}).sort(),
      sample: sanitizeExample(payload),
      fields: describeObject(payload),
    }),
  });
  output.diagnostics.endpoint_support.push(authDetailsProbe.support);
  if (authDetailsProbe.data) {
    output.diagnostics.authentication_details = authDetailsProbe.data;
  }

  const accountDetailsProbe = await probeOptionalEndpoint({
    name: 'account_details',
    path: '/account',
    transform: (payload) => ({
      top_level_keys: Object.keys(payload || {}).sort(),
      sample: sanitizeExample(payload),
      fields: describeObject(payload),
    }),
  });
  output.diagnostics.endpoint_support.push(accountDetailsProbe.support);
  if (accountDetailsProbe.data) {
    output.diagnostics.account_details = accountDetailsProbe.data;
  }

  const propertySuccessCount = inspectionResults.filter((item) => item.ok).length;
  const propertyPartialCount = inspectionResults.filter((item) => !item.ok && item.partial_ok).length;
  const hasAnySuccess = propertySuccessCount > 0 || propertyPartialCount > 0;

  output.summary = {
    properties_requested: propertyId ? 1 : properties.length,
    properties_inspected: inspectionResults.length,
    properties_ok: propertySuccessCount,
    properties_partial_ok: propertyPartialCount,
    properties_failed: inspectionResults.length - propertySuccessCount - propertyPartialCount,
    bookings_samples_count: output.bookings_sample.length,
    calendar_samples_count: output.calendar_sample.length,
    offers_samples_count: output.offers_sample.length,
  };

  if (!hasAnySuccess) {
    return respond(500, {
      error: 'Impossibile completare la diagnostica Beds24',
      auth: output.auth,
      diagnostics: output.diagnostics,
      summary: output.summary,
    });
  }

  return respond(200, output);
};

async function inspectMultiunitFocus({ properties, propertyId, bookingsFrom, bookingsTo, calendarFrom, calendarTo, output }) {
  const filtered = properties.filter((property) => {
    if (propertyId && String(property?.id || '') !== propertyId) return false;
    return isInterestingForMultiunit(property);
  });

  const blocks = [];
  const strongIds = [];
  const weakIds = [];
  const namingIds = [];
  const unitIdObserved = new Set();
  let maxQtyObserved = 0;
  let maxRoomsPerProperty = 0;
  let roomsWithDuplicateNames = 0;

  for (const property of filtered) {
    const roomTypes = Array.isArray(property?.roomTypes)
      ? property.roomTypes
      : Array.isArray(property?.rooms)
      ? property.rooms
      : [];
    const propertyName = String(property?.name || '');
    const propertyRooms = roomTypes.map((room) => {
      const qty = normalizeNullableNumber(room?.qty);
      if (qty != null) maxQtyObserved = Math.max(maxQtyObserved, qty);
      return {
        room_id: firstDefined(room?.id, null),
        room_name: room?.name || null,
        qty,
        max_people: firstDefined(normalizeNullableNumber(room?.maxPeople), normalizeNullableNumber(room?.maxOccupancy), null),
        max_adult: firstDefined(normalizeNullableNumber(room?.maxAdult), normalizeNullableNumber(room?.maxAdults), null),
        max_child: firstDefined(normalizeNullableNumber(room?.maxChild), normalizeNullableNumber(room?.maxChildren), null),
        unit_count: firstDefined(
          normalizeNullableNumber(room?.unitCount),
          normalizeNullableNumber(room?.units?.length),
          null
        ),
        calendar_room_id: extractSingleCalendarRoomId(room),
        units_field_present: Array.isArray(room?.units) ? room.units.length >= 0 : room?.units != null,
        units_field_value: sanitizeLargePayload(room?.units ?? null),
        subRooms_field_present: Array.isArray(room?.subRooms) ? room.subRooms.length >= 0 : room?.subRooms != null,
        subRooms_field_value: sanitizeLargePayload(room?.subRooms ?? null),
        all_room_keys: Object.keys(room || {}).sort(),
      };
    });

    const duplicateRoomNames = findDuplicateRoomNames(propertyRooms.map((room) => room.room_name));
    roomsWithDuplicateNames += duplicateRoomNames.length;
    maxRoomsPerProperty = Math.max(maxRoomsPerProperty, propertyRooms.length);

    const strongReasons = [];
    const weakReasons = [];
    if (propertyRooms.some((room) => Number(room.qty || 0) > 1)) strongReasons.push('qty > 1');
    if (propertyRooms.some((room) => Number(room.unit_count || 0) > 1)) strongReasons.push('unitCount > 1');
    if (propertyRooms.some((room) => hasMeaningfulCollection(room.units_field_value))) strongReasons.push('units non vuoto');
    if (propertyRooms.some((room) => hasMeaningfulCollection(room.subRooms_field_value))) strongReasons.push('subRooms non vuoto');
    if (propertyRooms.length > 1 && strongReasons.length === 0) weakReasons.push('rooms_count > 1 con qty=1');

    const namingSignal = hasResidenceNamingSignal(propertyName);
    if (namingSignal) namingIds.push(String(property?.id || ''));

    const firstRoomId = propertyRooms[0]?.room_id != null ? String(propertyRooms[0].room_id) : null;
    let bookingsBlock = {
      endpoint: '/bookings',
      params: {
        propertyId: String(property?.id || ''),
        arrivalFrom: bookingsFrom,
        arrivalTo: bookingsTo,
      },
      sample_count: 0,
      booking_unit_ids_unique: [],
      booking_unit_ids_vary: false,
      sample: [],
      error: null,
    };

    try {
      const bookingsPayload = await fetchBookingsForProperty(String(property?.id || ''), bookingsFrom, bookingsTo);
      const bookingRows = asDataArray(bookingsPayload).slice(0, 30);
      const unitIds = uniqueNonEmpty(bookingRows.map((row) => row?.unitId));
      unitIds.forEach((id) => unitIdObserved.add(id));
      bookingsBlock = {
        ...bookingsBlock,
        sample_count: bookingRows.length,
        booking_unit_ids_unique: unitIds,
        booking_unit_ids_vary: unitIds.length > 1,
        sample: bookingRows.map((row) => ({
          booking_id: firstDefined(row?.bookId, row?.id, row?.bookingId, null),
          propertyId: firstDefined(row?.propertyId, row?.propertyID, null),
          roomId: firstDefined(row?.roomId, row?.roomID, null),
          unitId: firstDefined(row?.unitId, row?.unitID, null),
          arrival: firstDefined(row?.arrival, row?.checkIn, row?.from, null),
          departure: firstDefined(row?.departure, row?.checkOut, row?.to, null),
          status: firstDefined(row?.status, row?.bookingStatus, null),
          channel: firstDefined(row?.referer, row?.channel, row?.source, row?.apiSource, null),
          all_booking_keys: Object.keys(row || {}).sort(),
          payload: anonymizeSensitive(sanitizeLargePayload(row)),
        })),
        payload_keys_observed: describeMergedObjects(bookingRows),
      };
    } catch (error) {
      bookingsBlock.error = error.message;
      output.diagnostics.errors.push({
        propertyId: String(property?.id || ''),
        scope: 'multiunit_bookings',
        message: error.message,
      });
    }

    if (bookingsBlock.booking_unit_ids_vary) {
      strongReasons.push('booking unitId multipli sulla stessa room');
    }

    let calendarBlock = {
      endpoint: '/inventory/rooms/calendar',
      params: {
        roomId: firstRoomId,
        from: calendarFrom,
        to: calendarTo,
      },
      payload: null,
      looks_like_override_only: null,
      keys_present: [],
      error: null,
    };

    if (firstRoomId) {
      try {
        const calendarPayload = await fetchCalendarForRoom(firstRoomId, calendarFrom, calendarTo);
        const matchingRow = asDataArray(calendarPayload).find((row) => String(row?.roomId || '') === firstRoomId) || { roomId: firstRoomId, calendar: [] };
        const calendarEntries = Array.isArray(matchingRow?.calendar) ? matchingRow.calendar : [];
        calendarBlock = {
          ...calendarBlock,
          payload: sanitizeLargePayload(matchingRow),
          looks_like_override_only: detectOverrideOnlyCalendar(calendarEntries),
          keys_present: Object.keys(calendarEntries[0] || {}).sort(),
        };
      } catch (error) {
        calendarBlock.error = error.message;
        output.diagnostics.errors.push({
          propertyId: String(property?.id || ''),
          roomId: firstRoomId,
          scope: 'multiunit_calendar',
          message: error.message,
        });
      }
    }

    const isStrong = strongReasons.length > 0;
    const isWeak = !isStrong && weakReasons.length > 0;
    if (isStrong) strongIds.push(String(property?.id || ''));
    if (isWeak) weakIds.push(String(property?.id || ''));

    blocks.push({
      property_id: firstDefined(property?.id, null),
      property_name: propertyName || null,
      city: property?.city || null,
      rooms_count: propertyRooms.length,
      rooms: propertyRooms,
      is_multi_unit_strong: isStrong,
      is_multi_unit_weak: isWeak,
      naming_pattern_signals_residence: namingSignal,
      duplicate_room_names: duplicateRoomNames,
      raw_property_payload_keys: Object.keys(property || {}).sort(),
      bookings_probe: bookingsBlock,
      calendar_probe: calendarBlock,
    });
  }

  return {
    focus: 'multiunit',
    filtered_properties: blocks,
    summary: {
      total_properties_analyzed: filtered.length,
      properties_strong_multiunit: strongIds,
      properties_weak_multiunit: weakIds,
      properties_naming_residence: namingIds,
      max_qty_observed: maxQtyObserved,
      max_rooms_per_property: maxRoomsPerProperty,
      unitId_values_observed_in_bookings: Array.from(unitIdObserved).sort(),
      rooms_with_duplicate_names: roomsWithDuplicateNames,
      recommendation_signal: inferRecommendationSignal({
        strongIds,
        weakIds,
        unitIds: Array.from(unitIdObserved),
      }),
    },
  };
}

async function inspectProperty({
  property,
  inspectAll,
  calendarFrom,
  calendarTo,
  bookingFrom,
  bookingTo,
  offerArrival,
  offerDeparture,
  output,
}) {
  const propertyId = String(property?.id || '');
  const propertyName = String(property?.name || '');
  const roomTypes = Array.isArray(property?.roomTypes)
    ? property.roomTypes
    : Array.isArray(property?.rooms)
    ? property.rooms
    : [];
  const roomIds = uniqueNonEmpty(roomTypes.map((room) => room?.id));
  const calendarRoomIds = uniqueNonEmpty(roomTypes.flatMap((room) => extractCalendarRoomIds(room)));
  const roomFieldInventory = describeMergedObjects(roomTypes);

  const propertySummary = {
    property_id: propertyId || null,
    property_name: propertyName || null,
    room_ids: roomIds,
    calendar_room_ids: calendarRoomIds,
    rooms_count: roomTypes.length,
    detection_multi_unit: {
      seems_multi_unit: roomTypes.length > 1,
      reasons: detectMultiUnitReasons(property, roomTypes, calendarRoomIds),
    },
    observed_property_fields: describeObject(property),
    observed_room_fields: roomFieldInventory,
    property_payload: inspectAll ? sanitizeExample(property) : sanitizeLargePayload(property),
  };

  const result = {
    ok: false,
    partial_ok: false,
    property_summary: propertySummary,
    bookings_sample: [],
    calendar_sample: [],
    offers_sample: [],
    property_fields: describeObject(property),
    room_fields: roomFieldInventory,
    booking_fields: [],
    calendar_fields: [],
    offers_fields: [],
    errors: [],
  };

  const bookingLimit = inspectAll ? BOOKINGS_ALL_LIMIT : BOOKINGS_SINGLE_LIMIT;
  try {
    const bookingsPayload = await fetchBookingsForProperty(propertyId, bookingFrom, bookingTo);
    const bookingRows = asDataArray(bookingsPayload).slice(0, bookingLimit);
    const bookingFields = describeMergedObjects(bookingRows);
    result.booking_fields = bookingFields;
    result.bookings_sample = bookingRows.map((row) => ({
      property_id: propertyId,
      property_name: propertyName,
      highlighted: highlightBooking(row),
      payload: inspectAll ? anonymizeSensitive(sanitizeExample(row)) : anonymizeSensitive(sanitizeLargePayload(row)),
    }));
  } catch (error) {
    result.errors.push({ scope: 'bookings', message: error.message });
    output.diagnostics.errors.push({
      propertyId,
      scope: 'bookings',
      message: error.message,
    });
  }

  for (const room of roomTypes) {
    const roomId = String(room?.id || '').trim();
    if (!roomId) continue;
    try {
      const calendarPayload = await fetchCalendarForRoom(roomId, calendarFrom, calendarTo);
      const matchingRow = asDataArray(calendarPayload).find((row) => String(row?.roomId || '') === roomId) || null;
      const calendarEntries = Array.isArray(matchingRow?.calendar) ? matchingRow.calendar : [];
      if (!result.calendar_fields.length) {
        result.calendar_fields = describeMergedObjects(calendarEntries);
      }
      result.calendar_sample.push({
        property_id: propertyId,
        property_name: propertyName,
        room_id: roomId,
        room_name: room?.name || null,
        looks_like_override_only: detectOverrideOnlyCalendar(calendarEntries),
        keys_present: Object.keys(calendarEntries[0] || {}).sort(),
        payload: inspectAll
          ? sanitizeExample({ roomId, calendar: calendarEntries.slice(0, 3) })
          : sanitizeLargePayload(matchingRow || { roomId, calendar: [] }),
      });

      const offersProbe = await probeOffersForRoom({
        roomId,
        propertyId,
        arrival: offerArrival,
        departure: offerDeparture,
        limit: inspectAll ? OFFERS_ALL_LIMIT : OFFERS_SINGLE_LIMIT,
      });
      if (offersProbe.sample.length) {
        if (!result.offers_fields.length) {
          result.offers_fields = offersProbe.fields;
        }
        result.offers_sample.push({
          property_id: propertyId,
          property_name: propertyName,
          room_id: roomId,
          room_name: room?.name || null,
          endpoint: offersProbe.endpoint,
          payload: inspectAll ? sanitizeExample(offersProbe.sample) : sanitizeLargePayload(offersProbe.sample),
        });
      } else if (offersProbe.error) {
        output.diagnostics.errors.push({
          propertyId,
          roomId,
          scope: 'offers',
          message: offersProbe.error,
        });
      }
    } catch (error) {
      result.errors.push({ scope: 'calendar', room_id: roomId, message: error.message });
      output.diagnostics.errors.push({
        propertyId,
        roomId,
        scope: 'calendar',
        message: error.message,
      });
    }
  }

  result.ok = result.bookings_sample.length > 0 && result.calendar_sample.length > 0;
  result.partial_ok = !result.ok && (result.bookings_sample.length > 0 || result.calendar_sample.length > 0 || result.offers_sample.length > 0);
  return result;
}

function buildAuthMeta(rawKey) {
  const warnings = [];
  warnings.push('Nel progetto il nome env `BEDS24_API_KEY` e ambiguo: in piu punti viene usato come refresh token da scambiare con `/authentication/token`.');
  return {
    pattern_auth_used: 'prima usa il valore di BEDS24_API_KEY come token diretto; su 401 o in helper dedicati lo tratta come refresh token e chiama GET /authentication/token; poi usa il token ottenuto nell header `token`.',
    token_masked: maskSecret(rawKey),
    token_hint: looksLikeRefreshToken(rawKey) ? 'valore compatibile con refresh token / long-lived token' : 'valore corto, potrebbe essere token diretto o configurazione non standard',
    warnings,
  };
}

async function probeAuth(rawKey) {
  const token = await getBeds24AccessToken({ env: process.env, forceRefresh: true });
  return {
    ok: true,
    env_used: 'BEDS24_API_KEY',
    refresh_token_masked: maskSecret(rawKey),
    access_token_masked: maskSecret(token),
    pattern_confirmed: 'GET /authentication/token con header refreshToken, poi chiamate API con header token',
  };
}

async function probeModifiedSince({ propertyId, bookingFrom, bookingTo }) {
  const modifiedSince = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString();
  const params = new URLSearchParams({
    arrivalFrom: bookingFrom,
    arrivalTo: bookingTo,
    includeInvoice: 'false',
    propertyId,
    modifiedSince,
  });
  try {
    const payload = await fetchBeds24Json(`/bookings?${params.toString()}`);
    const rows = asDataArray(payload);
    return {
      supported: true,
      endpoint: '/bookings',
      params: {
        propertyId,
        arrivalFrom: bookingFrom,
        arrivalTo: bookingTo,
        modifiedSince,
      },
      result_summary: `Richiesta accettata; ${rows.length} record restituiti`,
      error: null,
    };
  } catch (error) {
    const message = String(error.message || '');
    const unsupported = /unknown|invalid|unsupported|parameter|modifiedsince/i.test(message);
    return {
      supported: unsupported ? false : 'unknown',
      endpoint: '/bookings',
      params: {
        propertyId,
        arrivalFrom: bookingFrom,
        arrivalTo: bookingTo,
        modifiedSince,
      },
      result_summary: null,
      error: message,
    };
  }
}

async function probeOptionalEndpoint({ name, path, transform }) {
  try {
    const payload = await fetchBeds24Json(path);
    return {
      support: {
        name,
        ok: true,
        endpoint: path,
        detail: 'Endpoint disponibile',
      },
      data: transform(payload),
    };
  } catch (error) {
    return {
      support: {
        name,
        ok: false,
        endpoint: path,
        detail: error.message,
      },
      data: null,
    };
  }
}

async function probeOffersForRoom({ roomId, propertyId, arrival, departure, limit }) {
  const endpoints = [
    `/inventory/offers?roomId=${encodeURIComponent(roomId)}&arrival=${encodeURIComponent(arrival)}&departure=${encodeURIComponent(departure)}&numAdults=2`,
    `/inventory/rooms/offers?roomId=${encodeURIComponent(roomId)}&arrival=${encodeURIComponent(arrival)}&departure=${encodeURIComponent(departure)}&numAdults=2`,
    `/inventory/offers?propertyId=${encodeURIComponent(propertyId)}&roomId=${encodeURIComponent(roomId)}&arrival=${encodeURIComponent(arrival)}&departure=${encodeURIComponent(departure)}&numAdults=2`,
  ];

  let lastError = null;
  for (const path of endpoints) {
    try {
      const payload = await fetchBeds24Json(path);
      const rows = asDataArray(payload).slice(0, limit);
      return {
        endpoint: path.split('?')[0],
        sample: rows,
        fields: describeMergedObjects(rows),
        error: null,
      };
    } catch (error) {
      lastError = error.message;
    }
  }

  return {
    endpoint: endpoints[0].split('?')[0],
    sample: [],
    fields: [],
    error: lastError || 'Offers endpoint non disponibile',
  };
}

async function fetchBookingsForProperty(propertyId, arrivalFrom, arrivalTo) {
  const params = new URLSearchParams({
    arrivalFrom,
    arrivalTo,
    includeInvoice: 'false',
    propertyId,
  });
  return await fetchBeds24Json(`/bookings?${params.toString()}`);
}

async function fetchCalendarForRoom(roomId, from, to) {
  const params = new URLSearchParams({
    from,
    to,
  });
  params.append('roomId', roomId);
  return await fetchBeds24Json(`/inventory/rooms/calendar?${params.toString()}`);
}

async function fetchBeds24Json(path) {
  const response = await beds24Fetch(path);
  const text = await response.text();
  let parsed = null;
  try {
    parsed = text ? JSON.parse(text) : null;
  } catch (_error) {
    parsed = null;
  }

  if (!response.ok) {
    throw createBeds24Error(response.status, extractBeds24Error(parsed, text));
  }

  return parsed;
}

async function beds24Fetch(path, options = {}) {
  const env = options.env || process.env;
  const requestOptions = {
    method: options.method || 'GET',
    headers: {
      accept: 'application/json',
      ...(options.headers || {}),
    },
  };

  if (options.body != null) {
    requestOptions.body = options.body;
  }

  const attempt = async (forceRefresh) => {
    const token = await getBeds24AccessToken({ env, forceRefresh });
    return await fetch(`${BEDS24_URL}${path}`, {
      ...requestOptions,
      headers: {
        ...requestOptions.headers,
        token,
      },
    });
  };

  let response = await attempt(false);
  if (response.status !== 401) return response;
  response = await attempt(true);
  return response;
}

function getRefreshTokenFromEnv(env = process.env) {
  const refreshToken = String(env.BEDS24_REFRESH_TOKEN || env.BEDS24_API_KEY || '').trim();
  if (!refreshToken) {
    throw new Error('Configurazione Beds24 mancante: imposta BEDS24_API_KEY');
  }
  return refreshToken;
}

async function getBeds24AccessToken(options = {}) {
  const env = options.env || process.env;
  const forceRefresh = options.forceRefresh === true;
  const refreshToken = getRefreshTokenFromEnv(env);

  if (!forceRefresh && beds24TokenCache && beds24TokenCache.refreshToken === refreshToken && beds24TokenCache.expiresAt > Date.now()) {
    return beds24TokenCache.token;
  }

  if (!forceRefresh && !looksLikeRefreshToken(refreshToken)) {
    return refreshToken;
  }

  const res = await fetch(`${BEDS24_URL}/authentication/token`, {
    method: 'GET',
    headers: {
      accept: 'application/json',
      refreshToken,
    },
  });

  const text = await res.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch (_error) {
    data = null;
  }

  if (!res.ok || !data?.token) {
    if (!forceRefresh && !beds24TokenCache) {
      return refreshToken;
    }
    throw new Error(`Beds24 auth failed (${res.status}): ${extractBeds24Error(data, text)}`);
  }

  const expiresIn = Number(data?.expiresIn || 3600);
  beds24TokenCache = {
    refreshToken,
    token: data.token,
    expiresAt: Date.now() + Math.max(expiresIn - 60, 60) * 1000,
  };
  return data.token;
}

function buildMultiUnitDetection(results) {
  const suspicious = results
    .map((item) => ({
      property_id: item.property_summary.property_id,
      property_name: item.property_summary.property_name,
      has_multi_unit: item.property_summary.detection_multi_unit.seems_multi_unit,
      reasons: item.property_summary.detection_multi_unit.reasons,
    }))
    .filter((item) => item.has_multi_unit || item.reasons.length > 0);

  return {
    has_multi_unit: suspicious.some((item) => item.has_multi_unit),
    suspicious_properties: suspicious,
  };
}

function mergeFieldInventories(results) {
  return {
    property_fields: dedupeFieldDescriptions(results.flatMap((item) => item.property_fields || [])),
    room_fields: dedupeFieldDescriptions(results.flatMap((item) => item.room_fields || [])),
    booking_fields: dedupeFieldDescriptions(results.flatMap((item) => item.booking_fields || [])),
    calendar_fields: dedupeFieldDescriptions(results.flatMap((item) => item.calendar_fields || [])),
    offers_fields: dedupeFieldDescriptions(results.flatMap((item) => item.offers_fields || [])),
  };
}

function dedupeFieldDescriptions(fields) {
  const map = new Map();
  for (const field of fields || []) {
    if (!field?.key) continue;
    if (!map.has(field.key)) map.set(field.key, field);
  }
  return Array.from(map.values()).sort((a, b) => a.key.localeCompare(b.key, 'it'));
}

function describeMergedObjects(rows) {
  const firstValues = new Map();
  for (const row of rows || []) {
    if (!row || typeof row !== 'object' || Array.isArray(row)) continue;
    for (const [key, value] of Object.entries(row)) {
      if (!firstValues.has(key) && value !== undefined) {
        firstValues.set(key, value);
      }
    }
  }
  return Array.from(firstValues.entries())
    .map(([key, value]) => ({
      key,
      type: inferType(value),
      example: sanitizeExample(value),
    }))
    .sort((a, b) => a.key.localeCompare(b.key, 'it'));
}

function describeObject(obj) {
  return Object.keys(obj || {})
    .sort()
    .map((key) => ({
      key,
      type: inferType(obj[key]),
      example: sanitizeExample(obj[key]),
    }));
}

function detectMultiUnitReasons(property, roomTypes, calendarRoomIds) {
  const reasons = [];
  if (roomTypes.length > 1) reasons.push(`roomTypes > 1 (${roomTypes.length})`);
  if (calendarRoomIds.length > 1) reasons.push(`calendarRoomIds osservati > 1 (${calendarRoomIds.length})`);
  if (roomTypes.some((room) => room && typeof room === 'object' && (room.units || room.subRooms))) {
    reasons.push('presenza di units/subRooms nei room payload');
  }
  const roomNames = roomTypes.map((room) => normalizeName(room?.name)).filter(Boolean);
  if (new Set(roomNames).size !== roomNames.length && roomNames.length > 1) {
    reasons.push('nomi room duplicati o quasi duplicati');
  }
  if (property && typeof property === 'object' && (property.units || property.subRooms)) {
    reasons.push('presenza di units/subRooms nel property payload');
  }
  return reasons;
}

function normalizeName(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .trim();
}

function highlightBooking(row) {
  return {
    booking_id: firstDefined(row?.bookId, row?.id, row?.bookingId),
    propertyId: firstDefined(row?.propertyId, row?.propertyID),
    roomId: firstDefined(row?.roomId, row?.roomID),
    unitId: firstDefined(row?.unitId, row?.unitID),
    arrival: firstDefined(row?.arrival, row?.checkIn, row?.from),
    departure: firstDefined(row?.departure, row?.checkOut, row?.to),
    status: firstDefined(row?.status, row?.bookingStatus),
    channel: firstDefined(row?.referer, row?.channel, row?.source, row?.apiSource),
    price_total: firstDefined(row?.price, row?.totalPrice, row?.invoiceeAmount, row?.total),
    modified_timestamp: firstDefined(row?.modified, row?.modifiedTime, row?.updatedAt, row?.bookingTime),
  };
}

function sanitizeLargePayload(value) {
  if (Array.isArray(value)) {
    return value.slice(0, 5).map((item) => sanitizeLargePayload(item));
  }
  if (!value || typeof value !== 'object') return sanitizeExample(value);

  const out = {};
  for (const key of Object.keys(value).sort().slice(0, 30)) {
    const current = value[key];
    if (Array.isArray(current)) {
      out[key] = current.slice(0, 5).map((item) => sanitizeExample(item));
    } else if (current && typeof current === 'object') {
      out[key] = sanitizeExample(current);
    } else {
      out[key] = sanitizeExample(current);
    }
  }
  return out;
}

function sanitizeExample(value) {
  if (value === null || value === undefined) return value;
  if (typeof value === 'string') return value.length > 180 ? `${value.slice(0, 177)}...` : value;
  if (typeof value === 'number' || typeof value === 'boolean') return value;
  if (Array.isArray(value)) return value.slice(0, 3).map((item) => sanitizeExample(item));
  if (typeof value === 'object') {
    const out = {};
    for (const key of Object.keys(value).sort().slice(0, 12)) {
      out[key] = sanitizeExample(value[key]);
    }
    return out;
  }
  return String(value);
}

function anonymizeSensitive(value) {
  if (Array.isArray(value)) return value.map(anonymizeSensitive);
  if (!value || typeof value !== 'object') return value;

  const out = {};
  for (const [key, current] of Object.entries(value)) {
    const lower = key.toLowerCase();
    if (typeof current === 'string' && (
      lower === 'firstname' ||
      lower === 'lastname' ||
      lower === 'guestname' ||
      lower === 'guestfirstname' ||
      lower === 'guestlastname' ||
      lower === 'name'
    )) {
      out[key] = '[REDACTED]';
      continue;
    }
    if (typeof current === 'string' && lower.includes('email')) {
      out[key] = '[REDACTED]';
      continue;
    }
    if (typeof current === 'string' && (lower.includes('phone') || lower.includes('mobile') || lower.includes('tel'))) {
      out[key] = '[REDACTED]';
      continue;
    }
    if (typeof current === 'string' && (lower.includes('address') || lower.includes('indirizzo') || lower.includes('notes') || lower.includes('comment'))) {
      out[key] = '[REDACTED]';
      continue;
    }
    out[key] = anonymizeSensitive(current);
  }
  return out;
}

function extractCalendarRoomIds(room) {
  const ids = [];
  if (!room || typeof room !== 'object') return ids;
  for (const [key, value] of Object.entries(room)) {
    const lower = key.toLowerCase();
    if (lower === 'calendarroomid' || lower === 'calendar_room_id') {
      if (value != null && value !== '') ids.push(String(value));
    }
  }
  return uniqueNonEmpty(ids);
}

function visitValues(value, key, cb) {
  if (value === null || value === undefined) return;
  if (Array.isArray(value)) {
    value.forEach((item) => visitValues(item, key, cb));
    return;
  }
  if (typeof value === 'object') {
    for (const [childKey, childValue] of Object.entries(value)) {
      visitValues(childValue, childKey, cb);
    }
    return;
  }
  cb(key, value);
}

function detectOverrideOnlyCalendar(entries) {
  if (!entries.length) return true;
  return entries.every((entry) => {
    const keys = Object.keys(entry || {}).map((key) => key.toLowerCase());
    return keys.includes('from') || keys.includes('to') || keys.includes('date');
  });
}

function asDataArray(payload) {
  if (Array.isArray(payload?.data)) return payload.data;
  if (Array.isArray(payload)) return payload;
  return [];
}

function extractBeds24Error(parsed, rawText) {
  if (Array.isArray(parsed)) {
    return parsed.map((item) => item?.error || item?.message || JSON.stringify(item)).join('; ');
  }
  if (parsed?.error || parsed?.message) {
    return parsed.error || parsed.message;
  }
  return rawText || 'Errore Beds24 sconosciuto';
}

function createBeds24Error(status, message) {
  const error = new Error(message || `Beds24 error ${status}`);
  error.status = status;
  return error;
}

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload, null, 2),
  };
}

function inferType(value) {
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'array';
  return typeof value;
}

function uniqueNonEmpty(values) {
  return Array.from(new Set((values || []).map((value) => String(value || '').trim()).filter(Boolean)));
}

function firstDefined(...values) {
  return values.find((value) => value !== undefined && value !== null && value !== '');
}

function looksLikeRefreshToken(value) {
  const text = String(value || '').trim();
  return text.length >= 24;
}

function maskSecret(value) {
  const raw = String(value || '');
  if (!raw) return '';
  if (raw.length <= 8) return `${raw.slice(0, 2)}***`;
  return `${raw.slice(0, 4)}***${raw.slice(-4)}`;
}

function maskEmail(value) {
  const email = String(value || '').trim();
  const [local, domain] = email.split('@');
  if (!local || !domain) return email;
  return `${local.slice(0, 2)}***@${domain}`;
}

function maskPhone(value) {
  const digits = String(value || '').replace(/\D+/g, '');
  if (digits.length <= 4) return '***';
  return `${digits.slice(0, 2)}***${digits.slice(-2)}`;
}

function normalizeNullableNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function extractSingleCalendarRoomId(room) {
  const ids = extractCalendarRoomIds(room);
  if (!ids.length) return null;
  const roomId = room?.id != null ? String(room.id) : null;
  const different = ids.find((id) => id !== roomId);
  return different || ids[0] || null;
}

function hasResidenceNamingSignal(name) {
  return /\b(residence|residenza|palace|suites|apartments)\b/i.test(String(name || ''));
}

function isInterestingForMultiunit(property) {
  const roomTypes = Array.isArray(property?.roomTypes)
    ? property.roomTypes
    : Array.isArray(property?.rooms)
    ? property.rooms
    : [];
  if (roomTypes.length > 1) return true;
  if (roomTypes.some((room) => Number(room?.qty || 0) > 1)) return true;
  if (roomTypes.some((room) => hasMeaningfulCollection(room?.units) || hasMeaningfulCollection(room?.subRooms) || Number(room?.unitCount || 0) > 0)) return true;
  if (hasResidenceNamingSignal(property?.name)) return true;
  return false;
}

function hasMeaningfulCollection(value) {
  if (Array.isArray(value)) return value.length > 0;
  if (value && typeof value === 'object') return Object.keys(value).length > 0;
  return value != null && value !== '';
}

function findDuplicateRoomNames(names) {
  const map = new Map();
  for (const name of names || []) {
    const key = normalizeName(name);
    if (!key) continue;
    map.set(key, (map.get(key) || 0) + 1);
  }
  return Array.from(map.entries())
    .filter(([, count]) => count > 1)
    .map(([name]) => name);
}

function inferRecommendationSignal({ strongIds, weakIds, unitIds }) {
  if ((strongIds || []).length > 0 || (unitIds || []).length > 1) return 'multi_unit_required';
  if ((weakIds || []).length > 0) return 'ambiguous_needs_decision';
  return 'single_unit_safe';
}

function parseIsoDateUtc(value) {
  const [year, month, day] = String(value).split('-').map((part) => Number(part));
  return new Date(Date.UTC(year, month - 1, day));
}

function isoDateUtc(date) {
  return [
    date.getUTCFullYear(),
    String(date.getUTCMonth() + 1).padStart(2, '0'),
    String(date.getUTCDate()).padStart(2, '0'),
  ].join('-');
}

function addDaysUtc(date, days) {
  const next = new Date(date.getTime());
  next.setUTCDate(next.getUTCDate() + days);
  return next;
}

function startOfNextMonthUtc(todayIso) {
  const date = parseIsoDateUtc(todayIso);
  return isoDateUtc(new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 1)));
}

function endOfMonthUtc(monthStartIso) {
  const date = parseIsoDateUtc(monthStartIso);
  return isoDateUtc(new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 0)));
}
