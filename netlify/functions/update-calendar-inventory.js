const { createClient } = require('@supabase/supabase-js');

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
    SUPABASE_URL: process.env.SUPABASE_URL,
    SUPABASE_SERVICE_ROLE_KEY: process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY,
    BEDS24_API_KEY: process.env.BEDS24_API_KEY,
  };

  if (!env.SUPABASE_URL || !env.SUPABASE_SERVICE_ROLE_KEY) {
    return respond(500, { error: 'Configurazione Supabase mancante' });
  }
  if (!env.BEDS24_API_KEY) {
    return respond(500, { error: 'BEDS24_API_KEY mancante' });
  }

  const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);
  const authHeader = event.headers.authorization || event.headers.Authorization;
  if (!authHeader?.startsWith('Bearer ')) {
    return respond(401, { error: 'Unauthorized' });
  }

  const token = authHeader.replace('Bearer ', '').trim();
  const { data: authData, error: authError } = await supabase.auth.getUser(token);
  if (authError || !authData?.user) {
    return respond(401, { error: 'Unauthorized' });
  }

  let body;
  try {
    body = JSON.parse(event.body || '{}');
  } catch (_error) {
    return respond(400, { error: 'JSON non valido' });
  }

  const apartmentId = String(body.apartmentId || '').trim();
  const propertyId = String(body.propertyId || '').trim();
  const dateFrom = String(body.dateFrom || '').trim();
  const dateTo = String(body.dateTo || '').trim();
  const price = body.price === null || body.price === '' || body.price === undefined ? null : Number(body.price);
  const minStay = body.minStay === null || body.minStay === '' || body.minStay === undefined ? null : Number(body.minStay);
  const hasClosed = Object.prototype.hasOwnProperty.call(body || {}, 'closed');
  const dryRun = body.dryRun === true;
  const confirmLiveWrite = body.confirmLiveWrite === true;
  const includeRollbackPreview = body.includeRollbackPreview === true;
  const persistDryRunRecord = body.persistDryRunRecord === true;
  const closed = hasClosed ? body.closed === true : null;

  if (!apartmentId || !propertyId || !dateFrom || !dateTo) {
    return respond(400, { error: 'apartmentId, propertyId, dateFrom e dateTo sono obbligatori' });
  }
  if (!isIsoDate(dateFrom) || !isIsoDate(dateTo) || dateFrom > dateTo) {
    return respond(400, { error: 'Intervallo date non valido' });
  }
  if (price !== null && (!Number.isFinite(price) || price < 0)) {
    return respond(400, { error: 'Prezzo non valido' });
  }
  if (minStay !== null && (!Number.isFinite(minStay) || minStay < 1)) {
    return respond(400, { error: 'Min stay non valido' });
  }
  if (hasClosed && typeof body.closed !== 'boolean') {
    return respond(400, { error: 'closed deve essere booleano' });
  }
  if (price === null && minStay === null && !hasClosed) {
    return respond(400, { error: 'Nessun campo inventory da aggiornare: serve almeno uno tra price, minStay o closed' });
  }
  if (!dryRun && !confirmLiveWrite) {
    return respond(400, { error: 'Scrittura live non consentita: usa dryRun:true oppure confirmLiveWrite:true' });
  }
  if (!dryRun && dayDiffInclusive(dateFrom, dateTo) > 7) {
    return respond(400, { error: 'Scrittura live consentita solo su range piccoli (max 7 giorni)' });
  }

  try {
    const { data: apartment, error: apartmentError } = await supabase
      .from('apartments')
      .select('id,nome_appartamento,beds24_room_id')
      .eq('id', apartmentId)
      .limit(1)
      .maybeSingle();

    if (apartmentError) {
      return respond(500, { error: `Errore lettura appartamento: ${apartmentError.message}` });
    }
    if (!apartment?.id) {
      return respond(404, { error: 'Appartamento non trovato' });
    }
    if (String(apartment.beds24_room_id || '') !== propertyId) {
      return respond(400, { error: 'propertyId non coerente con il mapping Beds24 dell’appartamento' });
    }

    const propertyPath = `/properties?id=${encodeURIComponent(propertyId)}&includeAllRooms=true`;
    const propertyRes = await beds24Fetch(propertyPath, env);
    if (!propertyRes.ok) {
      const detail = await safeReadErrorBody(propertyRes);
      console.warn('[update-calendar-inventory] property lookup error', {
        status: propertyRes.status,
        propertyId,
        apartmentId,
        dateFrom,
        dateTo,
        detail,
      });
      return respond(propertyRes.status, { error: `Errore lettura property Beds24 (${propertyRes.status})`, detail });
    }
    const propertyPayload = await propertyRes.json();
    const property = (propertyPayload?.data || propertyPayload || [])[0];
    if (!property?.roomTypes?.length) {
      return respond(400, { error: 'Nessuna room type Beds24 disponibile per questa property' });
    }

    const selectedRoom = property.roomTypes[0];
    const warning = property.roomTypes.length > 1
      ? `Property ${propertyId} ha ${property.roomTypes.length} room type. Aggiorno la prima room type disponibile (${selectedRoom.id}).`
      : null;
    console.info('[update-calendar-inventory] property lookup ok', {
      status: propertyRes.status,
      propertyId,
      apartmentId,
      roomId: String(selectedRoom.id),
      roomTypeCount: property.roomTypes.length,
      dateFrom,
      dateTo,
      dryRun,
      confirmLiveWrite,
      includeRollbackPreview,
    });

    const calendarEntry = {
      from: dateFrom,
      to: dateTo,
    };
    if (price !== null) calendarEntry.price1 = Math.round(price);
    if (minStay !== null) calendarEntry.minStay = Math.round(minStay);
    if (closed !== null) calendarEntry.closed = closed;

    const payload = [
      {
        roomId: String(selectedRoom.id),
        calendar: [calendarEntry],
      },
    ];

    if (dryRun) {
      let snapshotBefore = null;
      let rollbackPayload = null;
      let operationalSnapshot = null;
      let dryRunRecordId = null;
      if (includeRollbackPreview) {
        snapshotBefore = await readInventorySnapshot(env, String(selectedRoom.id), dateFrom, dateTo, 'before-preview', propertyId);
        rollbackPayload = buildRollbackPayload(String(selectedRoom.id), snapshotBefore);
        operationalSnapshot = await readOperationalSnapshot(env, String(selectedRoom.id), propertyId, dateFrom, dateTo, payload);
        console.info('[update-calendar-inventory] rollback preview prepared', {
          apartmentId,
          propertyId,
          roomId: String(selectedRoom.id),
          dateFrom,
          dateTo,
          rollbackReady: Boolean(rollbackPayload),
          beforeCalendarRecords: snapshotBefore.calendarRecordCount,
          beforeAvailabilityDates: snapshotBefore.availabilityDateCount,
        });
      }
      if (persistDryRunRecord) {
        dryRunRecordId = await createInventoryChangeRecord(supabase, {
          apartment_id: apartmentId,
          property_id: propertyId,
          room_id: String(selectedRoom.id),
          date_from: dateFrom,
          date_to: dateTo,
          before_snapshot: operationalSnapshot,
          write_payload: payload,
          after_snapshot: null,
          rollback_payload: rollbackPayload,
          status: 'dry_run',
          created_by: authData.user.email,
          error_message: null,
        });
      }
      console.info('[update-calendar-inventory] dry run', {
        apartmentId,
        propertyId,
        roomId: String(selectedRoom.id),
        dateFrom,
        dateTo,
      });
      return respond(200, {
        success: true,
        dryRun: true,
        apartmentId,
        propertyId,
        roomId: String(selectedRoom.id),
        warning,
        payload,
        snapshotBefore,
        operationalSnapshot,
        rollbackPayload,
        inventoryChangeId: dryRunRecordId,
        diagnostics: {
          propertyLookupStatus: propertyRes.status,
          roomTypeCount: property.roomTypes.length,
          dateFrom,
          dateTo,
          liveWriteEnabled: false,
          includeRollbackPreview,
          persistDryRunRecord,
          rollbackReady: Boolean(rollbackPayload),
        },
      });
    }

    const snapshotBefore = await readInventorySnapshot(env, String(selectedRoom.id), dateFrom, dateTo, 'before', propertyId);
    const rollbackPayload = buildRollbackPayload(String(selectedRoom.id), snapshotBefore);
    const operationalSnapshot = await readOperationalSnapshot(env, String(selectedRoom.id), propertyId, dateFrom, dateTo, payload);

    console.info('[update-calendar-inventory] live write prepared', {
      apartmentId,
      propertyId,
      roomId: String(selectedRoom.id),
      dateFrom,
      dateTo,
      beforeCalendarRecords: snapshotBefore.calendarRecordCount,
      beforeAvailabilityDates: snapshotBefore.availabilityDateCount,
      rollbackReady: Boolean(rollbackPayload),
    });

    const inventoryChangeId = await createInventoryChangeRecord(supabase, {
      apartment_id: apartmentId,
      property_id: propertyId,
      room_id: String(selectedRoom.id),
      date_from: dateFrom,
      date_to: dateTo,
      before_snapshot: operationalSnapshot,
      write_payload: payload,
      after_snapshot: null,
      rollback_payload: rollbackPayload,
      status: 'pending',
      created_by: authData.user.email,
      error_message: null,
    });

    const updateRes = await beds24Fetch('/inventory/rooms/calendar', env, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    const updateText = await updateRes.text();
    let updateData = null;
    try {
      updateData = updateText ? JSON.parse(updateText) : null;
    } catch (_error) {
      updateData = null;
    }
    console.info('[update-calendar-inventory] beds24 response', {
      status: updateRes.status,
      apartmentId,
      propertyId,
      roomId: String(selectedRoom.id),
      dateFrom,
      dateTo,
      modified: updateData?.modified ?? null,
      success: updateRes.ok,
    });

    if (!updateRes.ok) {
      const detail = updateData?.error || updateData?.message || updateText || `Beds24 error ${updateRes.status}`;
      console.warn('[update-calendar-inventory] beds24 error', {
        status: updateRes.status,
        apartmentId,
        propertyId,
        roomId: String(selectedRoom.id),
        dateFrom,
        dateTo,
        detail,
      });
      await updateInventoryChangeRecord(supabase, inventoryChangeId, {
        status: 'beds24_error',
        error_message: detail,
      });
      return respond(updateRes.status, {
        error: detail,
      });
    }

    const snapshotAfter = await readInventorySnapshot(env, String(selectedRoom.id), dateFrom, dateTo, 'after', propertyId);
    const operationalSnapshotAfter = await readOperationalSnapshot(env, String(selectedRoom.id), propertyId, dateFrom, dateTo, payload);

    await updateInventoryChangeRecord(supabase, inventoryChangeId, {
      status: 'applied',
      after_snapshot: operationalSnapshotAfter,
      error_message: null,
    });

    await safeInsertAuditLog(supabase, {
      user_email: authData.user.email,
      action: 'UPDATE_CALENDAR_INVENTORY',
      table_name: 'apartments',
      record_id: apartmentId,
      timestamp: new Date().toISOString(),
      payload: {
        apartmentId,
        propertyId,
        roomId: String(selectedRoom.id),
        dateFrom,
        dateTo,
        price: price === null ? undefined : Math.round(price),
        minStay: minStay === null ? undefined : Math.round(minStay),
        closed: closed === null ? undefined : closed,
        snapshotBefore,
        snapshotAfter,
      },
    });

    return respond(200, {
      success: true,
      apartmentId,
      propertyId,
      roomId: String(selectedRoom.id),
      modified: updateData?.modified ?? null,
      warning,
      applied: {
        dateFrom,
        dateTo,
        price: price === null ? null : Math.round(price),
        minStay: minStay === null ? null : Math.round(minStay),
        closed,
      },
      snapshotBefore,
      snapshotAfter,
      rollbackPayload,
      response: updateData,
    });
  } catch (error) {
    return respond(500, { error: error.message });
  }
};

function isIsoDate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(value);
}

function dayDiffInclusive(from, to) {
  const start = new Date(`${from}T00:00:00`);
  const end = new Date(`${to}T00:00:00`);
  return Math.round((end - start) / 86400000) + 1;
}

async function readInventorySnapshot(env, roomId, dateFrom, dateTo, stage, propertyId = null) {
  const calendarPath = `/inventory/rooms/calendar?roomId=${encodeURIComponent(roomId)}&from=${encodeURIComponent(dateFrom)}&to=${encodeURIComponent(dateTo)}`;
  const availabilityPath = `/inventory/rooms/availability?roomId=${encodeURIComponent(roomId)}&dateFrom=${encodeURIComponent(dateFrom)}&dateTo=${encodeURIComponent(dateTo)}`;

  const snapshot = {
    stage,
    propertyId,
    roomId,
    dateFrom,
    dateTo,
    calendarStatus: null,
    calendarRecordCount: 0,
    calendarDays: [],
    calendarDiagnostics: null,
    availabilityStatus: null,
    availabilityDateCount: 0,
    availabilityDays: [],
    availabilityDiagnostics: null,
    warnings: [],
  };

  const calendarRes = await beds24Fetch(calendarPath, env);
  snapshot.calendarStatus = calendarRes.status;
  if (!calendarRes.ok) {
    const detail = await safeReadErrorBody(calendarRes);
    snapshot.warnings.push(`calendar:${detail || calendarRes.status}`);
    console.warn('[update-calendar-inventory] snapshot calendar error', {
      stage,
      roomId,
      dateFrom,
      dateTo,
      status: calendarRes.status,
      detail,
    });
  } else {
    const payload = await calendarRes.json();
    snapshot.calendarDiagnostics = buildInventoryDiagnostics(payload, {
      propertyId,
      roomId,
      dateFrom,
      dateTo,
      status: calendarRes.status,
    });
    const extracted = extractCalendarDays(payload, roomId, dateFrom, dateTo);
    snapshot.calendarRecordCount = extracted.recordCount;
    snapshot.calendarDays = extracted.days;
    console.info('[update-calendar-inventory] snapshot calendar', {
      stage,
      propertyId,
      roomId,
      dateFrom,
      dateTo,
      status: calendarRes.status,
      recordCount: snapshot.calendarRecordCount,
      dayCount: snapshot.calendarDays.length,
      topLevelKeys: snapshot.calendarDiagnostics.topLevelKeys,
      relevantArrays: snapshot.calendarDiagnostics.relevantArrays.map((item) => ({
        path: item.path,
        length: item.length,
      })),
    });
  }

  const availabilityRes = await beds24Fetch(availabilityPath, env);
  snapshot.availabilityStatus = availabilityRes.status;
  if (!availabilityRes.ok) {
    const detail = await safeReadErrorBody(availabilityRes);
    snapshot.warnings.push(`availability:${detail || availabilityRes.status}`);
    console.warn('[update-calendar-inventory] snapshot availability error', {
      stage,
      roomId,
      dateFrom,
      dateTo,
      status: availabilityRes.status,
      detail,
    });
  } else {
    const payload = await availabilityRes.json();
    const rows = payload?.data || payload || [];
    snapshot.availabilityDiagnostics = buildInventoryDiagnostics(payload, {
      propertyId,
      roomId,
      dateFrom,
      dateTo,
      status: availabilityRes.status,
    });
    snapshot.availabilityDays = rows.flatMap((row) =>
      Object.entries(row.availability || {})
        .filter(([date]) => date >= dateFrom && date <= dateTo)
        .map(([date, available]) => ({ date, available: Boolean(available) }))
    );
    snapshot.availabilityDateCount = snapshot.availabilityDays.length;
    console.info('[update-calendar-inventory] snapshot availability', {
      stage,
      roomId,
      dateFrom,
      dateTo,
      status: availabilityRes.status,
      recordCount: rows.length,
      dayCount: snapshot.availabilityDays.length,
      topLevelKeys: snapshot.availabilityDiagnostics.topLevelKeys,
    });
  }

  return snapshot;
}

async function readOperationalSnapshot(env, roomId, propertyId, dateFrom, dateTo, payload) {
  const pricingContext = {
    source: 'offers',
    numAdults: 2,
    dateFrom,
    dateTo,
  };
  const calendarOverrideSnapshot = await readCalendarOverrideSnapshot(env, roomId, propertyId, dateFrom, dateTo);
  const availabilitySnapshot = await readAvailabilitySnapshot(env, roomId, propertyId, dateFrom, dateTo);
  const offerDays = [];
  for (const date of eachDate(dateFrom, dateTo)) {
    const departure = formatIsoDateUtc(addDaysUtc(parseIsoDateUtc(date), 1));
    const params = new URLSearchParams({
      roomId: String(roomId),
      arrival: date,
      departure,
      numAdults: '2',
    });
    const offerRes = await beds24Fetch(`/inventory/offers?${params.toString()}`, env);
    if (!offerRes.ok) continue;
    const offerPayload = await offerRes.json();
    const firstOffer = (offerPayload?.data || offerPayload || []).find((row) => String(row.roomId || '') === String(roomId));
    if (!firstOffer) continue;
    offerDays.push({
      date,
      price: normalizeNumber(firstDefined(firstOffer.price, firstOffer.totalPrice, firstOffer.roomPrice, firstOffer.amount)),
      available: normalizeInteger(firstOffer.unitsAvailable) > 0,
    });
  }

  return {
    propertyId,
    roomId,
    dateFrom,
    dateTo,
    pricingContext,
    calendarOverrideSnapshot,
    offersSnapshot: {
      source: 'inventory/offers',
      numAdults: 2,
      offerPriceDays: offerDays,
    },
    availabilitySnapshot,
    writePayload: payload,
    capturedAt: new Date().toISOString(),
  };
}

async function readCalendarOverrideSnapshot(env, roomId, propertyId, dateFrom, dateTo) {
  const calendarPath = `/inventory/rooms/calendar?roomId=${encodeURIComponent(roomId)}&from=${encodeURIComponent(dateFrom)}&to=${encodeURIComponent(dateTo)}`;
  const calendarRes = await beds24Fetch(calendarPath, env);
  if (!calendarRes.ok) {
    return {
      status: calendarRes.status,
      propertyId,
      roomId,
      dateFrom,
      dateTo,
      overrideDays: [],
      warnings: [await safeReadErrorBody(calendarRes)],
    };
  }
  const payload = await calendarRes.json();
  const extracted = extractCalendarDays(payload, roomId, dateFrom, dateTo);
  return {
    status: calendarRes.status,
    propertyId,
    roomId,
    dateFrom,
    dateTo,
    overrideDays: extracted.days,
  };
}

async function readAvailabilitySnapshot(env, roomId, propertyId, dateFrom, dateTo) {
  const availabilityPath = `/inventory/rooms/availability?roomId=${encodeURIComponent(roomId)}&dateFrom=${encodeURIComponent(dateFrom)}&dateTo=${encodeURIComponent(dateTo)}`;
  const availabilityRes = await beds24Fetch(availabilityPath, env);
  if (!availabilityRes.ok) {
    return {
      status: availabilityRes.status,
      propertyId,
      roomId,
      dateFrom,
      dateTo,
      availabilityDays: [],
      warnings: [await safeReadErrorBody(availabilityRes)],
    };
  }
  const availabilityPayload = await availabilityRes.json();
  const rows = availabilityPayload?.data || availabilityPayload || [];
  return {
    status: availabilityRes.status,
    propertyId,
    roomId,
    dateFrom,
    dateTo,
    availabilityDays: rows.flatMap((row) =>
      Object.entries(row.availability || {})
        .filter(([date]) => date >= dateFrom && date <= dateTo)
        .map(([date, available]) => ({ date, available: Boolean(available) }))
    ),
  };
}

function normalizeSnapshotCalendarEntries(entries) {
  const normalized = [];
  entries.forEach((entry) => {
    if (entry?.date) {
      normalized.push(compactSnapshotEntry({
        date: entry.date,
        price1: firstDefined(entry.price1, entry.price, entry.roomPrice),
        minStay: firstDefined(entry.minStay, entry.minimumStay),
        closed: firstDefined(entry.closed, entry.bookable === false ? true : undefined),
      }));
      return;
    }
    if (entry?.from && entry?.to) {
      for (const date of eachDate(entry.from, entry.to)) {
        normalized.push(compactSnapshotEntry({
          date,
          price1: firstDefined(entry.price1, entry.price, entry.roomPrice),
          minStay: firstDefined(entry.minStay, entry.minimumStay),
          closed: firstDefined(entry.closed, entry.bookable === false ? true : undefined),
        }));
      }
    }
  });
  return normalized;
}

function extractCalendarDays(raw, roomId, dateFrom, dateTo) {
  const roomKey = String(roomId || '');
  const topRows = Array.isArray(raw?.data) ? raw.data : (Array.isArray(raw) ? raw : []);
  const matchingRows = topRows.filter((row) => {
    const rowRoomId = String(row?.roomId || row?.id || '');
    return !roomKey || !rowRoomId || rowRoomId === roomKey;
  });
  const rowsToScan = matchingRows.length ? matchingRows : topRows;

  const candidateArrays = [
    ...rowsToScan.flatMap((row, index) => findCalendarArrays(row, `data[${index}]`)),
    ...findCalendarArrays(raw, 'root'),
  ];

  const days = [];
  const seen = new Set();
  candidateArrays.forEach(({ value }) => {
    normalizeSnapshotCalendarEntries(value)
      .filter((entry) => entry?.date >= dateFrom && entry?.date <= dateTo)
      .forEach((entry) => {
        const key = `${entry.date}:${entry.price1 ?? ''}:${entry.minStay ?? ''}:${entry.closed ?? ''}`;
        if (seen.has(key)) return;
        seen.add(key);
        days.push(entry);
      });
  });

  days.sort((a, b) => a.date.localeCompare(b.date));

  return {
    recordCount: rowsToScan.length || topRows.length || candidateArrays.length,
    days,
  };
}

function findCalendarArrays(value, basePath, depth = 0) {
  if (!value || depth > 3) return [];
  const matches = [];
  if (Array.isArray(value)) {
    if (looksLikeCalendarArray(value)) {
      matches.push({ path: basePath, value });
    }
    return matches;
  }
  if (typeof value !== 'object') return matches;

  Object.entries(value).forEach(([key, child]) => {
    const path = basePath ? `${basePath}.${key}` : key;
    if (Array.isArray(child)) {
      if (looksLikeCalendarArray(child) || ['calendar', 'calendarDays', 'days', 'dates'].includes(key)) {
        matches.push({ path, value: child });
      }
      child.forEach((item, index) => {
        matches.push(...findCalendarArrays(item, `${path}[${index}]`, depth + 1));
      });
      return;
    }
    if (child && typeof child === 'object') {
      matches.push(...findCalendarArrays(child, path, depth + 1));
    }
  });

  return dedupeArrayMatches(matches);
}

function looksLikeCalendarArray(items) {
  if (!Array.isArray(items) || !items.length) return false;
  return items.some((item) => item && typeof item === 'object' && (
    item.date ||
    (item.from && item.to) ||
    item.price1 !== undefined ||
    item.minStay !== undefined ||
    item.closed !== undefined ||
    item.bookable !== undefined
  ));
}

function dedupeArrayMatches(matches) {
  const seen = new Set();
  return matches.filter((match) => {
    const key = `${match.path}:${Array.isArray(match.value) ? match.value.length : 'na'}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function buildInventoryDiagnostics(raw, context) {
  const topLevelKeys = raw && typeof raw === 'object' && !Array.isArray(raw) ? Object.keys(raw) : [];
  const relevantArrays = findRelevantArrays(raw);
  return {
    propertyId: context.propertyId || null,
    roomId: context.roomId || null,
    dateFrom: context.dateFrom,
    dateTo: context.dateTo,
    status: context.status,
    topLevelKeys,
    relevantArrays,
  };
}

function findRelevantArrays(raw) {
  const found = [];
  walkArrays(raw, 'root', found, 0);
  return found.slice(0, 12).map((item) => ({
    path: item.path,
    length: item.length,
    sample: item.sample,
  }));
}

function walkArrays(value, path, found, depth) {
  if (depth > 3 || value == null) return;
  if (Array.isArray(value)) {
    found.push({
      path,
      length: value.length,
      sample: value.slice(0, 2).map((item) => sanitizeDiagnosticValue(item)),
    });
    value.slice(0, 2).forEach((item, index) => walkArrays(item, `${path}[${index}]`, found, depth + 1));
    return;
  }
  if (typeof value !== 'object') return;
  Object.entries(value).forEach(([key, child]) => {
    walkArrays(child, path === 'root' ? key : `${path}.${key}`, found, depth + 1);
  });
}

function sanitizeDiagnosticValue(value, depth = 0) {
  if (value == null || depth > 2) return value;
  if (typeof value === 'string') {
    return value.length > 140 ? `${value.slice(0, 140)}...` : value;
  }
  if (typeof value === 'number' || typeof value === 'boolean') return value;
  if (Array.isArray(value)) {
    return value.slice(0, 2).map((item) => sanitizeDiagnosticValue(item, depth + 1));
  }
  if (typeof value === 'object') {
    const out = {};
    Object.entries(value).slice(0, 12).forEach(([key, child]) => {
      if (['message', 'comments', 'notes', 'apiMessage', 'invoiceItems', 'description'].includes(key)) return;
      out[key] = sanitizeDiagnosticValue(child, depth + 1);
    });
    return out;
  }
  return String(value);
}

function compactSnapshotEntry(entry) {
  return {
    date: entry.date,
    ...(entry.price1 !== undefined && entry.price1 !== null ? { price1: Number(entry.price1) } : {}),
    ...(entry.minStay !== undefined && entry.minStay !== null ? { minStay: Number(entry.minStay) } : {}),
    ...(entry.closed !== undefined && entry.closed !== null ? { closed: Boolean(entry.closed) } : {}),
  };
}

function buildRollbackPayload(roomId, snapshotBefore) {
  if (!snapshotBefore?.calendarDays?.length) return null;
  return [
    {
      roomId,
      calendar: snapshotBefore.calendarDays.map((day) => ({
        from: day.date,
        to: day.date,
        ...(day.price1 !== undefined ? { price1: day.price1 } : {}),
        ...(day.minStay !== undefined ? { minStay: day.minStay } : {}),
        ...(day.closed !== undefined ? { closed: day.closed } : {}),
      })),
    },
  ];
}

function eachDate(from, to) {
  const dates = [];
  let cursor = parseIsoDateUtc(from);
  const end = parseIsoDateUtc(to);
  while (cursor.getTime() <= end.getTime()) {
    dates.push(formatIsoDateUtc(cursor));
    cursor = addDaysUtc(cursor, 1);
  }
  return dates;
}

function parseIsoDateUtc(value) {
  const [year, month, day] = value.split('-').map((part) => Number(part));
  return new Date(Date.UTC(year, month - 1, day));
}

function formatIsoDateUtc(date) {
  return [
    date.getUTCFullYear(),
    String(date.getUTCMonth() + 1).padStart(2, '0'),
    String(date.getUTCDate()).padStart(2, '0'),
  ].join('-');
}

function addDaysUtc(date, days) {
  const copy = new Date(date.getTime());
  copy.setUTCDate(copy.getUTCDate() + days);
  return copy;
}

function firstDefined(...values) {
  return values.find((value) => value !== undefined && value !== null);
}

function normalizeNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeInteger(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.round(parsed) : null;
}

async function safeReadErrorBody(response) {
  try {
    const text = await response.text();
    if (!text) return '';
    try {
      const parsed = JSON.parse(text);
      return parsed?.error || parsed?.message || text;
    } catch (_error) {
      return text;
    }
  } catch (_error) {
    return '';
  }
}

async function safeInsertAuditLog(supabase, payload) {
  try {
    const { error } = await supabase.from('audit_log').insert(payload);
    if (error) {
      console.error('[update-calendar-inventory] audit_log error', error);
    }
  } catch (error) {
    console.error('[update-calendar-inventory] audit_log fatal', error);
  }
}

async function createInventoryChangeRecord(supabase, payload) {
  try {
    const { data, error } = await supabase
      .from('beds24_inventory_changes')
      .insert(payload)
      .select('id')
      .single();
    if (error) {
      console.error('[update-calendar-inventory] beds24_inventory_changes insert error', error);
      return null;
    }
    return data?.id || null;
  } catch (error) {
    console.error('[update-calendar-inventory] beds24_inventory_changes insert fatal', error);
    return null;
  }
}

async function updateInventoryChangeRecord(supabase, id, payload) {
  if (!id) return;
  try {
    const { error } = await supabase
      .from('beds24_inventory_changes')
      .update(payload)
      .eq('id', id);
    if (error) {
      console.error('[update-calendar-inventory] beds24_inventory_changes update error', error);
    }
  } catch (error) {
    console.error('[update-calendar-inventory] beds24_inventory_changes update fatal', error);
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
