import { createClient } from '@supabase/supabase-js';
import { resolveRuntimeConfig } from './lib/netlify-runtime-config.mjs';

const AUTOMATION_ID = 'manual-reprice-today-50';
const ACTOR = `manual:${AUTOMATION_ID}`;
const DISCOUNT_PCT = 50;
const BEDS24_URL = 'https://api.beds24.com/v2';
const SAFETY_MIN_NIGHT_PRICE_EUR = 25;

let beds24TokenCache = null;

function parseArgs(argv) {
  const args = {};
  for (const token of argv) {
    if (!token.startsWith('--')) continue;
    const [key, value] = token.slice(2).split('=');
    args[key] = value === undefined ? true : value;
  }
  return args;
}

function parseBoolean(value, fallback = false) {
  if (value === undefined) return fallback;
  if (typeof value === 'boolean') return value;
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'y', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'n', 'off'].includes(normalized)) return false;
  return fallback;
}

function isoDate(date) {
  return new Date(date.getTime() - (date.getTimezoneOffset() * 60000)).toISOString().slice(0, 10);
}

function roundCurrency(value) {
  return Math.round(Number(value) * 100) / 100;
}

function normalizePrice(value) {
  const price = Number(value);
  if (!Number.isFinite(price) || price <= 0) return null;
  return roundCurrency(price);
}

function applyDiscount(value) {
  const price = normalizePrice(value);
  if (price == null) return null;
  return Math.max(SAFETY_MIN_NIGHT_PRICE_EUR, roundCurrency(price * (1 - (DISCOUNT_PCT / 100))));
}

function unique(values) {
  return [...new Set(values)];
}

function summarizeSkips(skips) {
  const counts = new Map();
  for (const skip of skips) {
    counts.set(skip.reason, (counts.get(skip.reason) || 0) + 1);
  }
  return [...counts.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0], 'it'))
    .map(([reason, count]) => ({ reason, count }));
}

async function getBeds24AccessToken(refreshToken, forceRefresh = false) {
  if (!forceRefresh && beds24TokenCache && beds24TokenCache.refreshToken === refreshToken && beds24TokenCache.expiresAt > Date.now()) {
    return beds24TokenCache.token;
  }

  const res = await fetch(`${BEDS24_URL}/authentication/token`, {
    method: 'GET',
    headers: {
      refreshToken,
      accept: 'application/json',
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
    const detail = data?.error || data?.message || text || `Beds24 auth failed (${res.status})`;
    throw new Error(detail);
  }

  beds24TokenCache = {
    refreshToken,
    token: data.token,
    expiresAt: Date.now() + Math.max(30, Number(data.expiresIn || 3600) - 60) * 1000,
  };
  return beds24TokenCache.token;
}

async function pushBeds24Calendar(token, payload) {
  const response = await fetch(`${BEDS24_URL}/inventory/rooms/calendar`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      accept: 'application/json',
      token,
    },
    body: JSON.stringify(payload),
  });

  const text = await response.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch (_error) {
    data = null;
  }

  if (!response.ok) {
    const detail = data?.error || data?.message || text || `Beds24 error ${response.status}`;
    throw new Error(detail);
  }

  return data;
}

async function loadContext(client, todayIso) {
  const [
    { data: apartments, error: apartmentsError },
    { data: units, error: unitsError },
    { data: calendarDays, error: calendarError },
    { data: priorChanges, error: changesError },
  ] = await Promise.all([
    client
      .from('apartments')
      .select('id,nome_appartamento,attivo,beds24_room_id')
      .eq('attivo', true),
    client
      .from('apartment_units')
      .select('id,apartment_id,unit_label,beds24_room_id,beds24_unit_index,active')
      .eq('active', true),
    client
      .from('calendar_days')
      .select('apartment_unit_id,date,price,min_stay,available,closed')
      .eq('date', todayIso),
    client
      .from('beds24_inventory_changes')
      .select('room_id,date_from,date_to,status,write_payload')
      .eq('created_by', ACTOR)
      .neq('status', 'beds24_error')
      .eq('date_from', todayIso)
      .eq('date_to', todayIso),
  ]);

  if (apartmentsError) throw apartmentsError;
  if (unitsError) throw unitsError;
  if (calendarError) throw calendarError;
  if (changesError) throw changesError;

  const apartmentMap = new Map((apartments || []).map((row) => [String(row.id), row]));
  const hydratedUnits = (units || [])
    .filter((unit) => apartmentMap.has(String(unit.apartment_id)))
    .map((unit) => ({
      ...unit,
      apartment_name: apartmentMap.get(String(unit.apartment_id))?.nome_appartamento || 'Appartamento senza nome',
      apartment_property_id: apartmentMap.get(String(unit.apartment_id))?.beds24_room_id || null,
    }));

  return {
    today: todayIso,
    units: hydratedUnits,
    calendarDays: calendarDays || [],
    priorChanges: priorChanges || [],
  };
}

function makeSkip(unit, date, reason) {
  return {
    apartmentId: unit.apartment_id,
    apartmentName: unit.apartment_name,
    unitId: unit.id,
    unitLabel: unit.unit_label,
    date,
    reason,
  };
}

function buildPlan(context) {
  const roomIdGroups = new Map();
  for (const unit of context.units) {
    const roomId = String(unit.beds24_room_id || '').trim();
    if (!roomId) continue;
    if (!roomIdGroups.has(roomId)) roomIdGroups.set(roomId, []);
    roomIdGroups.get(roomId).push(unit);
  }

  const calendarByUnitDate = new Map();
  for (const row of context.calendarDays) {
    calendarByUnitDate.set(`${row.apartment_unit_id}:${row.date}`, row);
  }

  const priorTouchedByUnitDate = new Set();
  for (const row of context.priorChanges || []) {
    const payload = row.write_payload && typeof row.write_payload === 'object'
      ? row.write_payload
      : {};
    const payloadUnitId = payload.apartment_unit_id || null;
    if (payloadUnitId) {
      priorTouchedByUnitDate.add(`${payloadUnitId}:${context.today}`);
    }
  }

  const applied = [];
  const skipped = [];

  for (const unit of context.units) {
    const roomId = String(unit.beds24_room_id || '').trim();
    if (!roomId) {
      skipped.push(makeSkip(unit, context.today, 'beds24_mapping_incomplete'));
      continue;
    }

    const sharedUnits = roomIdGroups.get(roomId) || [];
    if (sharedUnits.length !== 1) {
      skipped.push(makeSkip(unit, context.today, 'beds24_mapping_ambiguous'));
      continue;
    }

    const day = calendarByUnitDate.get(`${unit.id}:${context.today}`) || null;
    if (!day) {
      skipped.push(makeSkip(unit, context.today, 'calendar_day_missing'));
      continue;
    }
    if (priorTouchedByUnitDate.has(`${unit.id}:${context.today}`)) {
      skipped.push(makeSkip(unit, context.today, 'already_touched_by_manual_reprice'));
      continue;
    }

    const currentPrice = normalizePrice(day.price);
    if (currentPrice == null) {
      skipped.push(makeSkip(unit, context.today, 'price_missing_or_invalid'));
      continue;
    }

    const newPrice = applyDiscount(currentPrice);
    if (newPrice == null || newPrice >= currentPrice) {
      skipped.push(makeSkip(unit, context.today, 'price_not_reducible'));
      continue;
    }

    applied.push({
      apartmentId: unit.apartment_id,
      apartmentName: unit.apartment_name,
      apartmentPropertyId: unit.apartment_property_id,
      unitId: unit.id,
      unitLabel: unit.unit_label,
      roomId,
      date: context.today,
      currentPrice,
      currentMinStay: Number.isFinite(Number(day.min_stay)) ? Number(day.min_stay) : null,
      currentAvailable: day.available,
      currentClosed: day.closed,
      newPrice,
    });
  }

  return { applied, skipped };
}

function buildBeds24Payload(applied) {
  const byRoom = new Map();
  for (const item of applied) {
    if (!byRoom.has(item.roomId)) {
      byRoom.set(item.roomId, {
        roomId: Number.isFinite(Number(item.roomId)) ? Number(item.roomId) : String(item.roomId),
        calendar: [],
      });
    }
    byRoom.get(item.roomId).calendar.push({
      from: item.date,
      to: item.date,
      price1: item.newPrice,
    });
  }
  return [...byRoom.values()];
}

async function upsertCalendarDays(client, applied) {
  const timestamp = new Date().toISOString();
  const rows = applied.map((item) => ({
    apartment_unit_id: item.unitId,
    date: item.date,
    price: item.newPrice,
    source_updated_at: timestamp,
    updated_at: timestamp,
  }));

  if (!rows.length) return;
  const { error } = await client
    .from('calendar_days')
    .upsert(rows, { onConflict: 'apartment_unit_id,date' });
  if (error) throw error;
}

async function createInventoryChangeLogs(client, applied, runAtIso) {
  if (!applied.length) return [];
  const rows = applied.map((item) => ({
    apartment_id: item.apartmentId,
    property_id: item.apartmentPropertyId || null,
    room_id: item.roomId,
    date_from: item.date,
    date_to: item.date,
    before_snapshot: {
      source: 'calendar_days',
      price: item.currentPrice,
      min_stay: item.currentMinStay,
      available: item.currentAvailable,
      closed: item.currentClosed,
    },
    write_payload: {
      automation_id: AUTOMATION_ID,
      apartment_unit_id: item.unitId,
      apartment_name: item.apartmentName,
      unit_label: item.unitLabel,
      date: item.date,
      previous_price: item.currentPrice,
      new_price: item.newPrice,
      discount_pct: DISCOUNT_PCT,
      triggered_at: runAtIso,
    },
    after_snapshot: null,
    rollback_payload: {
      roomId: item.roomId,
      calendar: [
        {
          from: item.date,
          to: item.date,
          price1: item.currentPrice,
        },
      ],
    },
    status: 'pending',
    created_by: ACTOR,
    error_message: null,
  }));

  const { data, error } = await client
    .from('beds24_inventory_changes')
    .insert(rows)
    .select('id');
  if (error) throw error;
  return (data || []).map((row) => row.id);
}

async function updateInventoryChangeLogs(client, ids, patch) {
  if (!ids?.length) return;
  const { error } = await client
    .from('beds24_inventory_changes')
    .update(patch)
    .in('id', ids);
  if (error) throw error;
}

function buildSummary(report) {
  return {
    nightsUpdated: report.applied.length,
    apartmentsTouched: unique(report.applied.map((item) => item.apartmentId)).length,
    skippedCases: report.skipped.length,
    skipReasons: summarizeSkips(report.skipped),
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const dryRun = parseBoolean(args['dry-run'], false);
  const todayIso = String(args.today || isoDate(new Date()));

  const { supabaseUrl, supabaseKey, beds24RefreshToken } = resolveRuntimeConfig();
  if (!supabaseUrl || !supabaseKey) {
    throw new Error('SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY/SUPABASE_SERVICE_KEY sono obbligatori');
  }
  if (!dryRun && !beds24RefreshToken) {
    throw new Error('BEDS24_REFRESH_TOKEN o BEDS24_API_KEY e obbligatorio per la scrittura live');
  }

  const client = createClient(supabaseUrl, supabaseKey, {
    auth: { persistSession: false, autoRefreshToken: false },
  });

  const context = await loadContext(client, todayIso);
  const plan = buildPlan(context);
  const runAtIso = new Date().toISOString();

  const report = {
    automationId: AUTOMATION_ID,
    today: todayIso,
    dryRun,
    applied: plan.applied,
    skipped: plan.skipped,
    warnings: [],
  };

  if (!dryRun && plan.applied.length) {
    const token = await getBeds24AccessToken(beds24RefreshToken);
    const payload = buildBeds24Payload(plan.applied);
    const logIds = await createInventoryChangeLogs(client, plan.applied, runAtIso);
    try {
      await pushBeds24Calendar(token, payload);
      try {
        await upsertCalendarDays(client, plan.applied);
      } catch (error) {
        report.warnings.push(`calendar_days_cache_update_failed: ${error.message || error}`);
      }
      await updateInventoryChangeLogs(client, logIds, {
        status: 'applied',
        error_message: report.warnings.length ? report.warnings.join(' | ') : null,
      });
    } catch (error) {
      try {
        await updateInventoryChangeLogs(client, logIds, {
          status: 'beds24_error',
          error_message: error.message || 'Beds24 write failed',
        });
      } catch (_updateError) {
        // Keep original failure.
      }
      throw error;
    }
  }

  report.summary = buildSummary(report);
  console.log(JSON.stringify(report, null, 2));
}

main().catch((error) => {
  console.error(error.message || error);
  process.exit(1);
});
