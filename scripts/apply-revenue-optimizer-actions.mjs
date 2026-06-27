import { execFileSync } from 'node:child_process';
import { createClient } from '@supabase/supabase-js';
import { getEnvValue, resolveRuntimeConfig } from './lib/netlify-runtime-config.mjs';

const AUTOMATION_ID = 'ottimizzatore-notti-vendute';
const AUTOMATION_STEP = 'apply-revenue-optimizer-actions';
const AUTOMATION_ACTOR = `automation:${AUTOMATION_ID}:${AUTOMATION_STEP}`;
const BEDS24_URL = 'https://api.beds24.com/v2';
const MAX_WINDOW_DAYS = 15;
const PRICE_FLOOR_TAG = 'price-floor';

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

function parseIntArg(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
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

function clampWindowDays(days) {
  return Math.max(1, Math.min(MAX_WINDOW_DAYS, days));
}

function normalizePrice(value) {
  const price = Number(value);
  if (!Number.isFinite(price) || price <= 0) return null;
  return Math.round(price);
}

function normalizeNullableInteger(value) {
  if (value === null || value === undefined || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.round(parsed) : null;
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
    `- strategy: ${report.strategy}`,
    `- window_days: ${report.windowDays}`,
    `- optimizer_proposed_actions: ${report.optimizerSummary?.proposedActions ?? 0}`,
    `- nights_updated: ${report.summary.nightsUpdated}`,
    `- apartments_touched: ${report.summary.apartmentsTouched}`,
    `- gap_fill_nights: ${report.summary.gapFillNights}`,
    `- low_occupancy_nights: ${report.summary.lowOccupancyNights}`,
    `- same_day_price_actions: ${report.summary.sameDayPriceActions}`,
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
    for (const item of report.applied.slice(0, 80)) {
      const minStayDelta = item.newMinStay != null && item.newMinStay !== item.currentMinStay
        ? `, min stay ${item.currentMinStay ?? '-'} -> ${item.newMinStay}`
        : '';
      lines.push(`- ${item.apartmentName} / ${item.unitLabel} / ${item.date}: prezzo ${item.currentPrice ?? 'n.d.'} -> ${item.newPrice ?? 'n.d.'}${minStayDelta} [${item.tags.join(', ')}]`);
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

function runOptimizer(strategy, windowDays, todayIso, runtimeConfig = {}) {
  const args = [
    'scripts/revenue-optimizer.mjs',
    `--strategy=${strategy}`,
    `--window-days=${windowDays}`,
    '--format=json',
  ];
  if (todayIso) args.push(`--today=${todayIso}`);

  const stdout = execFileSync(process.execPath, args, {
    cwd: process.cwd(),
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
    env: {
      ...process.env,
      ...(runtimeConfig.supabaseUrl ? { SUPABASE_URL: runtimeConfig.supabaseUrl } : {}),
      ...(runtimeConfig.supabaseKey ? { SUPABASE_SERVICE_ROLE_KEY: runtimeConfig.supabaseKey } : {}),
    },
    maxBuffer: 1024 * 1024 * 12,
  });

  return JSON.parse(stdout);
}

async function loadContext(client, todayIso, windowDays) {
  const endIso = isoDate(addDays(parseIsoDate(todayIso), windowDays - 1));
  const todayStartIso = `${todayIso}T00:00:00Z`;
  const [
    { data: apartments, error: apartmentsError },
    { data: units, error: unitsError },
    { data: calendarDays, error: calendarError },
    { data: occupancyDays, error: occupancyError },
    { data: recentChanges, error: recentChangesError },
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
      .select('apartment_unit_id,date,price,min_stay,available,closed')
      .gte('date', todayIso)
      .lte('date', endIso),
    client
      .from('v_calendar_occupancy')
      .select('apartment_unit_id,date')
      .gte('date', todayIso)
      .lte('date', endIso),
    client
      .from('beds24_inventory_changes')
      .select('date_from,created_at,status,write_payload')
      .eq('created_by', AUTOMATION_ACTOR)
      .eq('status', 'applied')
      .gte('created_at', todayStartIso),
  ]);

  if (apartmentsError) throw apartmentsError;
  if (unitsError) throw unitsError;
  if (calendarError) throw calendarError;
  if (occupancyError) throw occupancyError;
  if (recentChangesError) throw recentChangesError;

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
    end: endIso,
    units: hydratedUnits,
    calendarDays: calendarDays || [],
    occupancyDays: occupancyDays || [],
    recentChanges: recentChanges || [],
  };
}

function makeSkip(action, reason) {
  return {
    apartmentId: action.apartmentId || null,
    apartmentName: action.apartmentName || 'Appartamento senza nome',
    unitId: action.unitId || null,
    unitLabel: action.unitLabel || 'Default',
    date: action.dateFrom || null,
    reason,
    tags: Array.isArray(action.tags) ? action.tags : [],
  };
}

function buildRecentAppliedMap(changes) {
  const map = new Map();
  for (const change of changes || []) {
    const payload = change?.write_payload || {};
    const unitId = String(payload.apartment_unit_id || '').trim();
    const date = String(change?.date_from || payload.date || '').trim();
    if (!unitId || !date) continue;

    const key = `${unitId}:${date}`;
    const existing = map.get(key);
    const createdAt = String(change?.created_at || '');
    if (!existing || createdAt > String(existing.created_at || '')) {
      map.set(key, change);
    }
  }
  return map;
}

function wasAppliedToday(change, currentPrice, currentMinStay, currentClosed) {
  if (!change?.write_payload) return false;

  const loggedPrice = normalizePrice(change.write_payload.new_price);
  const loggedMinStay = normalizeNullableInteger(change.write_payload.new_min_stay);
  const loggedClosed = change.write_payload.new_closed === null || change.write_payload.new_closed === undefined
    ? null
    : change.write_payload.new_closed === true;

  const priceProtected = loggedPrice == null || currentPrice == null || currentPrice <= loggedPrice;
  const minStayProtected = loggedMinStay == null || currentMinStay == null || currentMinStay <= loggedMinStay;
  const closedProtected = loggedClosed === null || currentClosed === loggedClosed;

  return priceProtected && minStayProtected && closedProtected;
}

function buildPlan(actions, context, { allowMinStayAutomation = false } = {}) {
  const roomIdGroups = new Map();
  for (const unit of context.units) {
    const roomId = String(unit.beds24_room_id || '').trim();
    if (!roomId) continue;
    if (!roomIdGroups.has(roomId)) roomIdGroups.set(roomId, []);
    roomIdGroups.get(roomId).push(unit);
  }

  const unitById = new Map(context.units.map((unit) => [String(unit.id), unit]));
  const calendarByUnitDate = new Map();
  for (const row of context.calendarDays) {
    calendarByUnitDate.set(`${row.apartment_unit_id}:${row.date}`, row);
  }
  const occupiedByUnitDate = new Set(
    (context.occupancyDays || []).map((row) => `${row.apartment_unit_id}:${row.date}`)
  );
  const recentAppliedByUnitDate = buildRecentAppliedMap(context.recentChanges);

  const applied = [];
  const skipped = [];

  for (const action of actions) {
    const unit = unitById.get(String(action.unitId || ''));
    if (!unit) {
      skipped.push(makeSkip(action, 'unit_missing'));
      continue;
    }

    const roomId = String(unit.beds24_room_id || '').trim();
    if (!roomId) {
      skipped.push(makeSkip(action, 'beds24_mapping_incomplete'));
      continue;
    }

    const sharedUnits = roomIdGroups.get(roomId) || [];
    if (sharedUnits.length !== 1) {
      skipped.push(makeSkip(action, 'beds24_mapping_ambiguous'));
      continue;
    }

    const date = String(action.dateFrom || '').trim();
    if (!date || date !== action.dateTo) {
      skipped.push(makeSkip(action, 'date_range_not_supported'));
      continue;
    }

    const key = `${unit.id}:${date}`;
    const day = calendarByUnitDate.get(key) || null;
    if (!day) {
      skipped.push(makeSkip(action, 'calendar_day_missing'));
      continue;
    }
    if (day.closed === true) {
      skipped.push(makeSkip(action, 'date_closed'));
      continue;
    }
    if (day.available !== true) {
      skipped.push(makeSkip(action, 'date_not_open'));
      continue;
    }
    if (occupiedByUnitDate.has(key)) {
      skipped.push(makeSkip(action, 'date_occupied'));
      continue;
    }

    const currentPrice = normalizePrice(day.price);
    const currentMinStay = normalizeNullableInteger(day.min_stay);
    const currentClosed = day.closed === true;

    if (wasAppliedToday(recentAppliedByUnitDate.get(key), currentPrice, currentMinStay, currentClosed)) {
      skipped.push(makeSkip(action, 'already_applied_today'));
      continue;
    }

    const newPrice = normalizePrice(action.recommendedPrice);
    const recommendedMinStay = normalizeNullableInteger(action.recommendedMinStay);
    const newMinStay = allowMinStayAutomation ? recommendedMinStay : null;
    const newClosed = action.recommendedClosed === null || action.recommendedClosed === undefined
      ? null
      : action.recommendedClosed === true;

    if (newPrice == null && newMinStay == null && newClosed === null) {
      skipped.push(makeSkip(action, 'no_action_fields'));
      continue;
    }

    if (newPrice == null && action.tags?.includes('today')) {
      if (newMinStay == null || currentMinStay === newMinStay) {
        skipped.push(makeSkip(action, 'same_day_price_missing'));
        continue;
      }
    }

    if (newPrice == null && !action.tags?.includes('today')) {
      skipped.push(makeSkip(action, 'price_missing_or_invalid'));
      continue;
    }

    const priceUnchanged = newPrice == null || currentPrice === newPrice;
    const minStayUnchanged = newMinStay == null || currentMinStay === newMinStay;
    const closedUnchanged = newClosed === null || currentClosed === newClosed;

    if (priceUnchanged && minStayUnchanged && closedUnchanged) {
      skipped.push(makeSkip(action, 'already_aligned'));
      continue;
    }

    const isPriceFloorCorrection = Array.isArray(action.tags) && action.tags.includes(PRICE_FLOOR_TAG);
    if (
      currentPrice != null
      && newPrice != null
      && newPrice >= currentPrice
      && minStayUnchanged
      && closedUnchanged
      && !isPriceFloorCorrection
    ) {
      skipped.push(makeSkip(action, 'price_not_reducible'));
      continue;
    }

    applied.push({
      apartmentId: unit.apartment_id,
      apartmentName: unit.apartment_name,
      apartmentPropertyId: unit.apartment_property_id,
      unitId: unit.id,
      unitLabel: unit.unit_label,
      roomId,
      date,
      currentPrice,
      currentMinStay,
      currentClosed,
      newPrice,
      newMinStay,
      newClosed,
      tags: Array.isArray(action.tags) ? action.tags : [],
      rationale: action.rationale || '',
      discountPct: Number.isFinite(Number(action.discountPct)) ? Number(action.discountPct) : null,
      priority: Number.isFinite(Number(action.priority)) ? Number(action.priority) : 0,
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
    const entry = {
      from: item.date,
      to: item.date,
    };
    if (item.newPrice != null) entry.price1 = item.newPrice;
    if (item.newMinStay != null) entry.minStay = item.newMinStay;
    if (item.newClosed != null) entry.closed = item.newClosed;
    byRoom.get(item.roomId).calendar.push(entry);
  }
  return [...byRoom.values()];
}

async function upsertCalendarDays(client, applied) {
  if (!applied.length) return;
  const timestamp = new Date().toISOString();
  const rows = applied.map((item) => {
    const row = {
      apartment_unit_id: item.unitId,
      date: item.date,
      source_updated_at: timestamp,
      updated_at: timestamp,
    };
    if (item.newPrice != null) row.price = item.newPrice;
    if (item.newMinStay != null) row.min_stay = item.newMinStay;
    if (item.newClosed != null) row.closed = item.newClosed;
    return row;
  });

  const { error } = await client
    .from('calendar_days')
    .upsert(rows, { onConflict: 'apartment_unit_id,date' });
  if (error) throw error;
}

async function createInventoryChangeLogs(client, applied, runAtIso, strategy, windowDays) {
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
      closed: item.currentClosed,
    },
    write_payload: {
      automation_id: AUTOMATION_ID,
      automation_step: AUTOMATION_STEP,
      strategy,
      window_days: windowDays,
      apartment_unit_id: item.unitId,
      apartment_name: item.apartmentName,
      unit_label: item.unitLabel,
      date: item.date,
      previous_price: item.currentPrice,
      new_price: item.newPrice,
      previous_min_stay: item.currentMinStay,
      new_min_stay: item.newMinStay,
      previous_closed: item.currentClosed,
      new_closed: item.newClosed,
      tags: item.tags,
      rationale: item.rationale,
      discount_pct: item.discountPct,
      priority: item.priority,
      triggered_at: runAtIso,
    },
    after_snapshot: null,
    rollback_payload: {
      roomId: item.roomId,
      calendar: [
        {
          from: item.date,
          to: item.date,
          ...(item.currentPrice != null ? { price1: item.currentPrice } : {}),
          ...(item.currentMinStay != null ? { minStay: item.currentMinStay } : {}),
          ...(item.currentClosed != null ? { closed: item.currentClosed } : {}),
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
    gapFillNights: report.applied.filter((item) => item.tags.includes('gap-fill')).length,
    lowOccupancyNights: report.applied.filter((item) => item.tags.includes('low-occupancy')).length,
    sameDayPriceActions: report.applied.filter((item) => item.tags.includes('today')).length,
    skippedCases: report.skipped.length,
    skipReasons: summarizeSkips(report.skipped),
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const dryRun = parseBoolean(args['dry-run'], false);
  const format = String(args.format || 'json').trim().toLowerCase();
  const strategy = String(args.strategy || 'combined').trim() || 'combined';
  const windowDays = clampWindowDays(parseIntArg(args['window-days'], MAX_WINDOW_DAYS));
  const todayIso = String(args.today || isoDate(new Date()));
  const allowMinStayAutomation = parseBoolean(args['allow-minstay'], parseBoolean(getEnvValue('ALLOW_MIN_STAY_AUTOMATION'), false));

  const { supabaseUrl, supabaseKey, beds24RefreshToken } = resolveRuntimeConfig();
  if (!supabaseUrl || !supabaseKey) {
    throw new Error('SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY/SUPABASE_SERVICE_KEY sono obbligatori');
  }
  if (!dryRun && !beds24RefreshToken) {
    throw new Error('BEDS24_REFRESH_TOKEN o BEDS24_API_KEY e obbligatorio per la scrittura live');
  }

  const optimizerResult = runOptimizer(strategy, windowDays, todayIso, { supabaseUrl, supabaseKey });
  const client = createClient(supabaseUrl, supabaseKey, {
    auth: { persistSession: false, autoRefreshToken: false },
  });

  const context = await loadContext(client, todayIso, windowDays);
  const plan = buildPlan(Array.isArray(optimizerResult.actions) ? optimizerResult.actions : [], context, { allowMinStayAutomation });
  const runAtIso = new Date().toISOString();

  const report = {
    automationId: AUTOMATION_ID,
    automationStep: AUTOMATION_STEP,
    today: todayIso,
    dryRun,
    strategy,
    windowDays,
    optimizerSummary: optimizerResult.summary || {},
    applied: plan.applied,
    skipped: plan.skipped,
    warnings: allowMinStayAutomation
      ? []
      : ['Min stay automation disabilitata di default. Usa --allow-minstay=true o ALLOW_MIN_STAY_AUTOMATION=true per abilitarla.'],
  };

  if (!dryRun && plan.applied.length) {
    const token = await getBeds24AccessToken(beds24RefreshToken);
    const payload = buildBeds24Payload(plan.applied);
    const logIds = await createInventoryChangeLogs(client, plan.applied, runAtIso, strategy, windowDays);
    try {
      await pushBeds24Calendar(token, payload);
      try {
        await upsertCalendarDays(client, plan.applied);
      } catch (error) {
        report.warnings.push(`calendar_days_cache_update_failed: ${error.message || error}`);
      }
      await updateInventoryChangeLogs(client, logIds, {
        status: 'applied',
        after_snapshot: {
          applied_at: runAtIso,
          strategy,
          window_days: windowDays,
        },
        error_message: report.warnings.length ? report.warnings.join(' | ') : null,
      });
    } catch (error) {
      try {
        await updateInventoryChangeLogs(client, logIds, {
          status: 'beds24_error',
          error_message: error.message || 'Beds24 write failed',
        });
      } catch (_updateError) {
        // Keep the original failure as the surfaced error.
      }
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
