import { createClient } from '@supabase/supabase-js';
import { resolveRuntimeConfig } from './lib/netlify-runtime-config.mjs';

const AUTOMATION_ID = 'ottimizzatore-notti-vendute';
const AUTOMATION_STEP = 'force-today-minstay';
const AUTOMATION_ACTOR = `automation:${AUTOMATION_ID}:${AUTOMATION_STEP}`;
const BEDS24_URL = 'https://api.beds24.com/v2';

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

function parseIsoDate(value) {
  return new Date(`${value}T00:00:00`);
}

function addDays(date, days) {
  const next = new Date(date);
  next.setDate(next.getDate() + days);
  return next;
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

function renderMarkdown(report) {
  const lines = [
    `# ${AUTOMATION_STEP}`,
    '',
    `- today: ${report.today}`,
    `- dry_run: ${report.dryRun}`,
    `- nights_updated: ${report.summary.nightsUpdated}`,
    `- apartments_touched: ${report.summary.apartmentsTouched}`,
    `- min_stay_set: ${report.summary.minStaySet}`,
    `- skipped_cases: ${report.summary.skippedCases}`,
  ];

  if (report.warnings?.length) {
    lines.push(`- warnings: ${report.warnings.join(' | ')}`);
  }

  lines.push('');
  lines.push('## Skip reasons');
  if (!report.summary.skipReasons.length) {
    lines.push('');
    lines.push('Nessun caso saltato.');
  } else {
    for (const item of report.summary.skipReasons) {
      lines.push(`- ${item.reason}: ${item.count}`);
    }
  }

  lines.push('');
  lines.push('## Applied');
  if (!report.applied.length) {
    lines.push('');
    lines.push('Nessuna notte aggiornata.');
  } else {
    for (const item of report.applied) {
      lines.push(`- ${item.apartmentName} / ${item.unitLabel} / ${item.date}: min stay ${item.currentMinStay ?? '-'} -> ${item.newMinStay}`);
    }
  }

  return `${lines.join('\n')}\n`;
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
    { data: occupancyDays, error: occupancyError },
  ] = await Promise.all([
    client
      .from('apartments')
      .select('id,nome_appartamento,attivo,beds24_room_id')
      .eq('attivo', true),
    client
      .from('apartment_units')
      .select('id,apartment_id,unit_label,beds24_room_id,active')
      .eq('active', true),
    client
      .from('calendar_days')
      .select('apartment_unit_id,date,min_stay,available,closed')
      .eq('date', todayIso),
    client
      .from('v_calendar_occupancy')
      .select('apartment_unit_id,date')
      .eq('date', todayIso),
  ]);

  if (apartmentsError) throw apartmentsError;
  if (unitsError) throw unitsError;
  if (calendarError) throw calendarError;
  if (occupancyError) throw occupancyError;

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
    occupancyDays: occupancyDays || [],
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

  const occupiedByUnitDate = new Set(
    (context.occupancyDays || []).map((row) => `${row.apartment_unit_id}:${row.date}`)
  );

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

    const key = `${unit.id}:${context.today}`;
    const day = calendarByUnitDate.get(key) || null;
    if (!day) {
      skipped.push(makeSkip(unit, context.today, 'calendar_day_missing'));
      continue;
    }
    if (day.closed === true) {
      skipped.push(makeSkip(unit, context.today, 'date_closed'));
      continue;
    }
    if (day.available !== true) {
      skipped.push(makeSkip(unit, context.today, 'date_not_open'));
      continue;
    }
    if (occupiedByUnitDate.has(key)) {
      skipped.push(makeSkip(unit, context.today, 'date_occupied'));
      continue;
    }

    const currentMinStay = Number.isFinite(Number(day.min_stay)) ? Number(day.min_stay) : null;
    if (currentMinStay === 1) {
      skipped.push(makeSkip(unit, context.today, 'already_min_stay_1'));
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
      currentMinStay,
      newMinStay: 1,
    });
  }

  return { applied, skipped };
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
      minStay: item.newMinStay,
    });
  }
  return [...byRoom.values()];
}

async function upsertCalendarDays(client, applied) {
  if (!applied.length) return;
  const timestamp = new Date().toISOString();
  const rows = applied.map((item) => ({
    apartment_unit_id: item.unitId,
    date: item.date,
    min_stay: item.newMinStay,
    source_updated_at: timestamp,
    updated_at: timestamp,
  }));

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
      min_stay: item.currentMinStay,
    },
    write_payload: {
      automation_id: AUTOMATION_ID,
      automation_step: AUTOMATION_STEP,
      apartment_unit_id: item.unitId,
      apartment_name: item.apartmentName,
      unit_label: item.unitLabel,
      date: item.date,
      previous_min_stay: item.currentMinStay,
      new_min_stay: item.newMinStay,
      triggered_at: runAtIso,
    },
    after_snapshot: null,
    rollback_payload: {
      roomId: item.roomId,
      calendar: [
        {
          from: item.date,
          to: item.date,
          ...(item.currentMinStay != null ? { minStay: item.currentMinStay } : {}),
        },
      ],
    },
    status: 'pending',
    created_by: AUTOMATION_ACTOR,
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
    minStaySet: report.applied.length,
    skippedCases: report.skipped.length,
    skipReasons: summarizeSkips(report.skipped),
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    console.log('Usage: node scripts/force-today-minstay.mjs [--today=YYYY-MM-DD] [--dry-run=true|false] [--format=json|markdown] [--allow-minstay=true|false]');
    process.exit(0);
  }

  const dryRun = parseBoolean(args['dry-run'], false);
  const format = String(args.format || 'json').trim().toLowerCase();
  const todayIso = String(args.today || isoDate(new Date()));
  const allowMinStayAutomation = parseBoolean(args['allow-minstay'], parseBoolean(process.env.ALLOW_MIN_STAY_AUTOMATION, false));

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
  const plan = allowMinStayAutomation ? buildPlan(context) : { applied: [], skipped: [] };
  const report = {
    today: todayIso,
    dryRun,
    applied: plan.applied,
    skipped: plan.skipped,
    warnings: allowMinStayAutomation
      ? []
      : ['Min stay automation disabilitata di default. Usa --allow-minstay=true o ALLOW_MIN_STAY_AUTOMATION=true per abilitarla.'],
  };

  if (!dryRun && plan.applied.length) {
    const runAtIso = new Date().toISOString();
    const logIds = await createInventoryChangeLogs(client, plan.applied, runAtIso);
    try {
      const token = await getBeds24AccessToken(beds24RefreshToken);
      const payload = buildBeds24Payload(plan.applied);
      await pushBeds24Calendar(token, payload);
      await upsertCalendarDays(client, plan.applied);
      await updateInventoryChangeLogs(client, logIds, {
        status: 'applied',
        after_snapshot: { min_stay: 1, applied_at: runAtIso },
      });
    } catch (error) {
      await updateInventoryChangeLogs(client, logIds, {
        status: 'beds24_error',
        error_message: error.message || String(error),
      });
      throw error;
    }
  }

  report.summary = buildSummary(report);

  if (format === 'markdown') {
    process.stdout.write(renderMarkdown(report));
    return;
  }

  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}

main().catch((error) => {
  console.error(error.message || error);
  process.exit(1);
});
