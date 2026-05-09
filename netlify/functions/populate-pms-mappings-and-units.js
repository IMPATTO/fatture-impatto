const { createClient } = require('@supabase/supabase-js');

const BEDS24_URL = 'https://api.beds24.com/v2';

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
  const apartmentId = String(query.apartmentId || '').trim() || null;
  const onlyResidences = String(query.onlyResidences || '').trim() === '1';

  const env = {
    SUPABASE_URL: process.env.SUPABASE_URL || 'https://tysxeikqbgebpfyblgeb.supabase.co',
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
  const output = {
    mode: dryRun ? 'dry-run' : 'execute',
    filter: {
      apartmentId,
      onlyResidences,
    },
    summary: {
      apartments_read: 0,
      single_unit_count: 0,
      residence_count: 0,
      mappings_planned: 0,
      units_planned: 0,
      mappings_upserted: 0,
      units_upserted: 0,
      orphans: 0,
      errors: 0,
    },
    preview: [],
    orphans: [],
    sync_job_id: null,
  };

  let syncJobId = null;
  let syncJobFailedToStart = null;

  try {
    const apartments = await loadApartments(supabase, apartmentId);
    if (apartmentId && !apartments.length) {
      return respond(404, {
        error: 'Apartment non trovato',
        mode: output.mode,
        filter: output.filter,
      });
    }

    const beds24PropertiesPayload = await fetchBeds24Json('/properties?includeAllRooms=true', env);
    const beds24Properties = asDataArray(beds24PropertiesPayload);
    const beds24Index = buildBeds24Index(beds24Properties);
    if (beds24Index.roomById.size === 0) {
      output.preview.push({
        diagnostic_warning: 'beds24Index.roomById is empty',
        properties_received: beds24Properties.length,
        first_property_keys: beds24Properties[0] ? Object.keys(beds24Properties[0]).sort() : [],
      });
    }
    const classification = classifyApartments(apartments);
    const processList = onlyResidences ? classification.residences : classification.residences.concat(classification.singleUnits);

    output.summary.apartments_read = processList.length;
    output.summary.single_unit_count = onlyResidences ? 0 : classification.singleUnits.length;
    output.summary.residence_count = classification.residences.length;

    if (!dryRun) {
      try {
        syncJobId = await createSyncJob(supabase, apartmentId);
        output.sync_job_id = syncJobId;
      } catch (error) {
        syncJobFailedToStart = error;
        output.summary.errors += 1;
      }
    }

    for (const entry of processList) {
      if (entry.kind === 'SINGLE_UNIT') {
        const plan = planSingleUnit(entry.apartment, beds24Index);
        collectPlan(output, plan);
        if (!dryRun) {
          await executePlan({
            plan,
            supabase,
            output,
          });
        }
      }

      if (entry.kind === 'CONTENITORE_RESIDENCE') {
        const plan = planResidence(entry.container, entry.children, beds24Index);
        collectPlan(output, plan);
        if (!dryRun) {
          await executePlan({
            plan,
            supabase,
            output,
          });
        }
      }
    }

    if (!dryRun && syncJobId) {
      const finalStatus = output.summary.errors > 0
        ? (output.summary.mappings_upserted > 0 || output.summary.units_upserted > 0 ? 'partial' : 'failed')
        : 'success';
      await finalizeSyncJob(supabase, syncJobId, {
        status: finalStatus,
        rows_read: output.summary.apartments_read,
        rows_upserted: output.summary.mappings_upserted + output.summary.units_upserted,
        rows_orphaned: output.summary.orphans,
        error_message: output.summary.errors > 0 ? 'Completato con errori o orphan da verificare' : null,
      });
    }

    if (!dryRun && !syncJobId && syncJobFailedToStart) {
      return respond(500, {
        error: 'Impossibile creare sync_jobs',
        detail: syncJobFailedToStart.message,
        ...output,
      });
    }

    return respond(200, output);
  } catch (error) {
    output.summary.errors += 1;
    if (!dryRun && syncJobId) {
      await safeFinalizeSyncJob(supabase, syncJobId, {
        status: 'failed',
        rows_read: output.summary.apartments_read,
        rows_upserted: output.summary.mappings_upserted + output.summary.units_upserted,
        rows_orphaned: output.summary.orphans,
        error_message: error.message,
      });
    }
    return respond(500, {
      error: 'Populate PMS mappings/units fallita',
      detail: error.message,
      ...output,
    });
  }
};

async function loadApartments(supabase, apartmentId) {
  const baseSelect = 'id,nome_appartamento,struttura_nome,attivo,beds24_property_id,beds24_room_id,beds24_calendar_room_id';

  if (!apartmentId) {
    const { data, error } = await supabase
      .from('apartments')
      .select(baseSelect)
      .order('nome_appartamento', { ascending: true });

    if (error) {
      throw new Error(`Errore lettura apartments: ${error.message}`);
    }
    return Array.isArray(data) ? data : [];
  }

  const { data: target, error: targetError } = await supabase
    .from('apartments')
    .select(baseSelect)
    .eq('id', apartmentId)
    .maybeSingle();

  if (targetError) {
    throw new Error(`Errore lettura apartment target: ${targetError.message}`);
  }
  if (!target) {
    return [];
  }

  const propertyId = normalizeText(target.beds24_property_id);
  if (!propertyId) {
    return [target];
  }

  const { data: siblings, error: siblingsError } = await supabase
    .from('apartments')
    .select(baseSelect)
    .eq('beds24_property_id', propertyId)
    .order('nome_appartamento', { ascending: true });

  if (siblingsError) {
    throw new Error(`Errore lettura apartment siblings: ${siblingsError.message}`);
  }

  const rows = Array.isArray(siblings) ? siblings : [];
  const deduped = new Map(rows.map((row) => [row.id, row]));
  deduped.set(target.id, target);
  return Array.from(deduped.values());
}

function classifyApartments(apartments) {
  const byProperty = new Map();
  for (const apartment of apartments) {
    const propertyId = normalizeText(apartment.beds24_property_id);
    if (!propertyId) continue;
    if (!byProperty.has(propertyId)) byProperty.set(propertyId, []);
    byProperty.get(propertyId).push(apartment);
  }

  const residences = [];
  const singleUnits = [];

  for (const apartment of apartments) {
    const propertyId = normalizeText(apartment.beds24_property_id);
    const roomId = normalizeText(apartment.beds24_room_id);
    const calendarRoomId = normalizeText(apartment.beds24_calendar_room_id);
    const siblings = byProperty.get(propertyId) || [];
    const children = siblings.filter((row) =>
      row.id !== apartment.id
      && row.attivo === false
      && normalizeText(row.beds24_room_id) === null
      && normalizeText(row.beds24_calendar_room_id) !== null
    );

    const isResidenceContainer = apartment.attivo === true
      && propertyId
      && roomId
      && roomId === propertyId
      && children.length > 0;

    if (isResidenceContainer) {
      residences.push({
        kind: 'CONTENITORE_RESIDENCE',
        container: apartment,
        children,
      });
      continue;
    }

    const isSingleUnit = apartment.attivo === true
      && !isResidenceContainer
      && Boolean(calendarRoomId || roomId);

    if (isSingleUnit) {
      singleUnits.push({
        kind: 'SINGLE_UNIT',
        apartment,
      });
    }
  }

  return { residences, singleUnits };
}

function buildBeds24Index(properties) {
  const propertyById = new Map();
  const roomById = new Map();

  for (const property of properties) {
    const propertyId = normalizeText(firstDefined(property?.id, property?.propertyId));
    if (propertyId) {
      propertyById.set(propertyId, property);
    }

    const rooms = Array.isArray(property?.roomTypes)
      ? property.roomTypes
      : Array.isArray(property?.rooms)
      ? property.rooms
      : Array.isArray(property?.unitTypes)
      ? property.unitTypes
      : [];

    for (const room of rooms) {
      const roomId = normalizeText(firstDefined(room?.id, room?.roomId));
      if (!roomId) continue;
      roomById.set(roomId, {
        ...room,
        __propertyId: propertyId,
        __propertyName: property?.name || null,
      });
    }
  }

  return { propertyById, roomById };
}

function planSingleUnit(apartment, beds24Index) {
  const propertyId = normalizeText(apartment.beds24_property_id);
  const externalRoomId = normalizeText(apartment.beds24_calendar_room_id) || normalizeText(apartment.beds24_room_id);
  const plan = {
    apartment_id: apartment.id,
    apartment_name: apartment.nome_appartamento || null,
    type: 'single-unit',
    mappings: [],
    units: [],
    orphans: [],
  };

  if (!propertyId || !externalRoomId) {
    plan.orphans.push(buildOrphan(apartment, 'single_unit_missing_mapping', {
      beds24_property_id: propertyId,
      external_room_id: externalRoomId,
    }));
    return plan;
  }

  if (externalRoomId === propertyId) {
    plan.orphans.push(buildOrphan(apartment, 'single_unit_room_equals_property_protected', {
      beds24_property_id: propertyId,
      external_room_id: externalRoomId,
    }));
    return plan;
  }

  const beds24Room = beds24Index.roomById.get(externalRoomId);
  if (!beds24Room) {
    plan.orphans.push(buildOrphan(apartment, 'beds24_room_not_found', {
      beds24_property_id: propertyId,
      external_room_id: externalRoomId,
    }));
    return plan;
  }

  plan.mappings.push({
    apartment_id: apartment.id,
    channel: 'beds24',
    external_property_id: propertyId,
    external_room_id: externalRoomId,
    external_room_qty: normalizePositiveInt(beds24Room.qty) || 1,
    external_room_name: normalizeText(beds24Room.name) || apartment.nome_appartamento || null,
    is_primary: true,
    active: true,
    notes: null,
  });

  plan.units.push({
    apartment_id: apartment.id,
    unit_label: 'Default',
    room_type_label: null,
    beds24_room_id: externalRoomId,
    beds24_unit_index: 1,
    max_guests: normalizePositiveInt(firstDefined(beds24Room.maxPeople, beds24Room.maxOccupancy)),
    active: true,
    notes: null,
  });

  return plan;
}

function planResidence(container, children, beds24Index) {
  const propertyId = normalizeText(container.beds24_property_id);
  const plan = {
    apartment_id: container.id,
    apartment_name: container.nome_appartamento || null,
    type: 'residence',
    mappings: [],
    units: [],
    orphans: [],
  };

  for (const child of children) {
    const externalRoomId = normalizeText(child.beds24_calendar_room_id);
    if (!propertyId || !externalRoomId) {
      plan.orphans.push(buildOrphan(child, 'residence_child_missing_mapping', {
        container_apartment_id: container.id,
        beds24_property_id: propertyId,
        external_room_id: externalRoomId,
      }));
      continue;
    }

    const beds24Room = beds24Index.roomById.get(externalRoomId);
    if (!beds24Room) {
      plan.orphans.push(buildOrphan(child, 'beds24_room_not_found', {
        container_apartment_id: container.id,
        beds24_property_id: propertyId,
        external_room_id: externalRoomId,
      }));
      continue;
    }

    const qty = normalizePositiveInt(firstDefined(beds24Room.qty, beds24Room.unitCount)) || 1;
    const roomTypeLabel = normalizeText(child.nome_appartamento) || normalizeText(beds24Room.name) || `Room ${externalRoomId}`;

    plan.mappings.push({
      apartment_id: container.id,
      channel: 'beds24',
      external_property_id: propertyId,
      external_room_id: externalRoomId,
      external_room_qty: qty,
      external_room_name: roomTypeLabel,
      is_primary: false,
      active: true,
      notes: null,
    });

    for (let i = 1; i <= qty; i += 1) {
      plan.units.push({
        apartment_id: container.id,
        unit_label: `${roomTypeLabel} #${i}`,
        room_type_label: roomTypeLabel,
        beds24_room_id: externalRoomId,
        beds24_unit_index: i,
        max_guests: normalizePositiveInt(firstDefined(beds24Room.maxPeople, beds24Room.maxOccupancy)),
        active: true,
        notes: null,
      });
    }
  }

  return plan;
}

function collectPlan(output, plan) {
  output.summary.mappings_planned += plan.mappings.length;
  output.summary.units_planned += plan.units.length;
  output.summary.orphans += plan.orphans.length;
  output.preview.push({
    apartment_id: plan.apartment_id,
    apartment_name: plan.apartment_name,
    type: plan.type,
    mappings: plan.mappings,
    units: plan.units,
    orphans: plan.orphans,
  });
  output.orphans.push(...plan.orphans);
}

async function executePlan({ plan, supabase, output }) {
  for (const mappingRow of plan.mappings) {
    try {
      const { error } = await supabase
        .from('channel_property_mappings')
        .upsert(mappingRow, {
          onConflict: 'channel,external_property_id,external_room_id',
        });
      if (error) throw error;
      output.summary.mappings_upserted += 1;
    } catch (error) {
      output.summary.errors += 1;
      output.orphans.push({
        type: 'mapping_upsert_error',
        apartment_id: plan.apartment_id,
        apartment_name: plan.apartment_name,
        message: error.message,
        row: mappingRow,
      });
    }
  }

  for (const unitRow of plan.units) {
    try {
      const { error } = await supabase
        .from('apartment_units')
        .upsert(unitRow, {
          onConflict: 'apartment_id,unit_label',
        });
      if (error) throw error;
      output.summary.units_upserted += 1;
    } catch (error) {
      output.summary.errors += 1;
      output.orphans.push({
        type: 'unit_upsert_error',
        apartment_id: plan.apartment_id,
        apartment_name: plan.apartment_name,
        message: error.message,
        row: unitRow,
      });
    }
  }
}

async function createSyncJob(supabase, apartmentId) {
  const row = {
    scope: 'mappings_and_units',
    trigger: 'manual',
    target_apartment_id: apartmentId,
    target_external_property_id: null,
    date_range_start: null,
    date_range_end: null,
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
      rows_skipped_stale: 0,
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
    console.error('[populate-pms-mappings-and-units] sync job finalize error', {
      syncJobId,
      message: error.message,
    });
  }
}

function buildOrphan(apartment, reason, extra) {
  return {
    type: reason,
    apartment_id: apartment.id,
    apartment_name: apartment.nome_appartamento || null,
    struttura_nome: apartment.struttura_nome || null,
    beds24_property_id: normalizeText(apartment.beds24_property_id),
    beds24_room_id: normalizeText(apartment.beds24_room_id),
    beds24_calendar_room_id: normalizeText(apartment.beds24_calendar_room_id),
    ...extra,
  };
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

function asDataArray(payload) {
  if (Array.isArray(payload?.data)) return payload.data;
  if (Array.isArray(payload)) return payload;
  return [];
}

function normalizeText(value) {
  const text = String(value ?? '').trim();
  return text ? text : null;
}

function normalizePositiveInt(value) {
  if (value === null || value === undefined || value === '') return null;
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  const int = Math.trunc(num);
  return int > 0 ? int : null;
}

function firstDefined(...values) {
  for (const value of values) {
    if (value !== undefined && value !== null) {
      return value;
    }
  }
  return null;
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
