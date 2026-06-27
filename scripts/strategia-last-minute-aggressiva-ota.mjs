import { createClient } from '@supabase/supabase-js';
import { resolveRuntimeConfig } from './lib/netlify-runtime-config.mjs';

const AUTOMATION_ID = 'strategia-last-minute-aggressiva-ota';
const AUTOMATION_ACTOR = `automation:${AUTOMATION_ID}`;
const BEDS24_URL = 'https://api.beds24.com/v2';
const WINDOW_DAYS = 5;
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

function parseIsoDate(value) {
  return new Date(`${value}T00:00:00`);
}

function addDays(date, days) {
  const next = new Date(date);
  next.setDate(next.getDate() + days);
  return next;
}

function buildTargetDates(todayIso) {
  return Array.from({ length: WINDOW_DAYS }, (_entry, index) => isoDate(addDays(parseIsoDate(todayIso), index)));
}

function dayOffset(todayIso, dateIso) {
  return Math.round((parseIsoDate(dateIso) - parseIsoDate(todayIso)) / 86400000);
}

function roundCurrency(value) {
  return Math.round(Number(value) * 100) / 100;
}

function normalizePrice(value) {
  const price = Number(value);
  if (!Number.isFinite(price) || price <= 0) return null;
  return roundCurrency(price);
}

function normalizeNullableInteger(value) {
  if (value === null || value === undefined || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.round(parsed) : null;
}

function discountForOffset(offset) {
  if (offset <= 0) return 20;
  if (offset === 1) return 18;
  if (offset === 2) return 16;
  if (offset === 3) return 14;
  return 12;
}

function minStayForOffset(offset) {
  if (offset <= 2) return 1;
  return 2;
}

function applyDiscount(value, discountPct) {
  const price = normalizePrice(value);
  if (price == null) return null;
  return Math.max(SAFETY_MIN_NIGHT_PRICE_EUR, roundCurrency(price * (1 - (discountPct / 100))));
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
    `# ${AUTOMATION_ID}`,
    '',
    `- today: ${report.today}`,
    `- dry_run: ${report.dryRun}`,
    `- target_dates: ${report.targetDates.join(', ')}`,
    `- nights_updated: ${report.summary.nightsUpdated}`,
    `- apartments_touched: ${report.summary.apartmentsTouched}`,
    `- min_stay_eased: ${report.summary.minStayEased}`,
    `- verified_rows: ${report.summary.verifiedRows}`,
    `- verification_mismatches: ${report.summary.verificationMismatches}`,
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
      const minStayDelta = item.newMinStay != null && item.newMinStay !== item.currentMinStay
        ? `, min stay ${item.currentMinStay ?? '-'} -> ${item.newMinStay}`
        : '';
      lines.push(`- ${item.apartmentName} / ${item.unitLabel} / ${item.date}: ${item.currentPrice} -> ${item.newPrice}${minStayDelta} (${item.discountPct}% off)`);
    }
  }

  if (report.verification?.mismatches?.length) {
    lines.push('');
    lines.push('## Verification mismatches');
    for (const item of report.verification.mismatches) {
      lines.push(`- ${item.apartmentName} / ${item.unitLabel} / ${item.date}: ${item.message}`);
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

async function loadContext(client, todayIso, targetDates) {
  const startIso = targetDates[0];
  const endIso = targetDates[targetDates.length - 1];

  const [
    { data: apartments, error: apartmentsError },
    { data: units, error: unitsError },
    { data: calendarDays, error: calendarError },
    { data: occupancyDays, error: occupancyError },
    { data: priorChanges, error: changesError },
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
      .gte('date', startIso)
      .lte('date', endIso),
    client
      .from('v_calendar_occupancy')
      .select('apartment_unit_id,date')
      .gte('date', startIso)
      .lte('date', endIso),
    client
      .from('beds24_inventory_changes')
      .select('room_id,date_from,date_to,status,created_by,write_payload')
      .eq('created_by', AUTOMATION_ACTOR)
      .neq('status', 'beds24_error')
      .lte('date_from', endIso)
      .gte('date_to', startIso),
  ]);

  if (apartmentsError) throw apartmentsError;
  if (unitsError) throw unitsError;
  if (calendarError) throw calendarError;
  if (occupancyError) throw occupancyError;
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
    targetDates,
    units: hydratedUnits,
    calendarDays: calendarDays || [],
    occupancyDays: occupancyDays || [],
    priorChanges: priorChanges || [],
  };
}

function eachDate(startIso, endIso) {
  const dates = [];
  for (let date = parseIsoDate(startIso); date <= parseIsoDate(endIso); date = addDays(date, 1)) {
    dates.push(isoDate(date));
  }
  return dates;
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

function buildPlan(context, { allowMinStayAutomation = false } = {}) {
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

  const priorTouchedByUnitDate = new Set();
  for (const row of context.priorChanges) {
    const payload = row.write_payload && typeof row.write_payload === 'object'
      ? row.write_payload
      : {};
    const payloadAutomationId = payload.automation_id || null;
    const payloadUnitId = payload.apartment_unit_id || null;
    if (payloadAutomationId && payloadAutomationId !== AUTOMATION_ID) continue;
    for (const date of eachDate(row.date_from, row.date_to)) {
      if (payloadUnitId) {
        priorTouchedByUnitDate.add(`${payloadUnitId}:${date}`);
      } else if (row.room_id) {
        for (const unit of roomIdGroups.get(String(row.room_id)) || []) {
          priorTouchedByUnitDate.add(`${unit.id}:${date}`);
        }
      }
    }
  }

  const applied = [];
  const skipped = [];

  for (const unit of context.units) {
    const roomId = String(unit.beds24_room_id || '').trim();
    if (!roomId) {
      for (const date of context.targetDates) {
        skipped.push(makeSkip(unit, date, 'beds24_mapping_incomplete'));
      }
      continue;
    }

    const sharedUnits = roomIdGroups.get(roomId) || [];
    if (sharedUnits.length !== 1) {
      for (const date of context.targetDates) {
        skipped.push(makeSkip(unit, date, 'beds24_mapping_ambiguous'));
      }
      continue;
    }

    for (const date of context.targetDates) {
      const key = `${unit.id}:${date}`;
      const day = calendarByUnitDate.get(key) || null;
      if (!day) {
        skipped.push(makeSkip(unit, date, 'calendar_day_missing'));
        continue;
      }
      if (day.closed === true) {
        skipped.push(makeSkip(unit, date, 'date_closed'));
        continue;
      }
      if (day.available !== true) {
        skipped.push(makeSkip(unit, date, 'date_not_open'));
        continue;
      }
      if (occupiedByUnitDate.has(key)) {
        skipped.push(makeSkip(unit, date, 'date_occupied'));
        continue;
      }
      if (priorTouchedByUnitDate.has(key)) {
        skipped.push(makeSkip(unit, date, 'already_touched_by_automation'));
        continue;
      }

      const currentPrice = normalizePrice(day.price);
      if (currentPrice == null) {
        skipped.push(makeSkip(unit, date, 'price_missing_or_invalid'));
        continue;
      }

      const currentMinStay = normalizeNullableInteger(day.min_stay);
      const offset = dayOffset(context.today, date);
      const discountPct = discountForOffset(offset);
      const newPrice = applyDiscount(currentPrice, discountPct);
      const targetMinStay = allowMinStayAutomation ? minStayForOffset(offset) : null;
      const newMinStay = targetMinStay == null
        ? null
        : (currentMinStay == null ? targetMinStay : Math.min(currentMinStay, targetMinStay));

      const priceUnchanged = newPrice == null || newPrice >= currentPrice;
      const minStayUnchanged = currentMinStay === newMinStay;
      if (priceUnchanged && minStayUnchanged) {
        skipped.push(makeSkip(unit, date, 'already_aligned'));
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
        newPrice,
        currentMinStay,
        newMinStay,
        discountPct,
      });
    }
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
      price1: item.newPrice,
    };
    if (item.newMinStay != null) entry.minStay = item.newMinStay;
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
      price: item.newPrice,
      source_updated_at: timestamp,
      updated_at: timestamp,
    };
    if (item.newMinStay != null) row.min_stay = item.newMinStay;
    return row;
  });

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
    },
    write_payload: {
      automation_id: AUTOMATION_ID,
      apartment_unit_id: item.unitId,
      apartment_name: item.apartmentName,
      unit_label: item.unitLabel,
      date: item.date,
      previous_price: item.currentPrice,
      new_price: item.newPrice,
      previous_min_stay: item.currentMinStay,
      new_min_stay: item.newMinStay,
      discount_pct: item.discountPct,
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

async function verifyCalendarDays(client, applied) {
  if (!applied.length) {
    return { verifiedRows: 0, mismatches: [] };
  }

  const unitIds = unique(applied.map((item) => item.unitId));
  const dates = unique(applied.map((item) => item.date));
  const dateFrom = dates.slice().sort()[0];
  const dateTo = dates.slice().sort().at(-1);
  const { data, error } = await client
    .from('calendar_days')
    .select('apartment_unit_id,date,price,min_stay')
    .in('apartment_unit_id', unitIds)
    .gte('date', dateFrom)
    .lte('date', dateTo);
  if (error) throw error;

  const rowMap = new Map((data || []).map((row) => [`${row.apartment_unit_id}:${row.date}`, row]));
  const mismatches = [];
  let verifiedRows = 0;

  for (const item of applied) {
    const row = rowMap.get(`${item.unitId}:${item.date}`);
    if (!row) {
      mismatches.push({
        apartmentName: item.apartmentName,
        unitLabel: item.unitLabel,
        date: item.date,
        message: 'riga calendar_days assente dopo la scrittura',
      });
      continue;
    }

    const priceMatches = normalizePrice(row.price) === item.newPrice;
    const minStayMatches = item.newMinStay == null || normalizeNullableInteger(row.min_stay) === item.newMinStay;
    if (priceMatches && minStayMatches) {
      verifiedRows += 1;
      continue;
    }

    const expectedLabel = item.newMinStay == null
      ? `atteso prezzo ${item.newPrice}`
      : `atteso prezzo ${item.newPrice} / min stay ${item.newMinStay}`;
    const foundLabel = item.newMinStay == null
      ? `trovato prezzo ${normalizePrice(row.price) ?? 'n.d.'}`
      : `trovato prezzo ${normalizePrice(row.price) ?? 'n.d.'} / min stay ${normalizeNullableInteger(row.min_stay) ?? 'n.d.'}`;
    mismatches.push({
      apartmentName: item.apartmentName,
      unitLabel: item.unitLabel,
      date: item.date,
      message: `${expectedLabel}, ${foundLabel}`,
    });
  }

  return { verifiedRows, mismatches };
}

function buildSummary(report) {
  return {
    nightsUpdated: report.applied.length,
    apartmentsTouched: unique(report.applied.map((item) => item.apartmentId)).length,
    minStayEased: report.applied.filter((item) => item.newMinStay != null && item.currentMinStay !== item.newMinStay).length,
    verifiedRows: report.verification?.verifiedRows || 0,
    verificationMismatches: report.verification?.mismatches?.length || 0,
    skippedCases: report.skipped.length,
    skipReasons: summarizeSkips(report.skipped),
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    console.log('Usage: node scripts/strategia-last-minute-aggressiva-ota.mjs [--today=YYYY-MM-DD] [--dry-run=true|false] [--format=json|markdown] [--allow-minstay=true|false]');
    process.exit(0);
  }

  const dryRun = parseBoolean(args['dry-run'], false);
  const format = String(args.format || 'json').trim().toLowerCase();
  const todayIso = String(args.today || isoDate(new Date()));
  const targetDates = buildTargetDates(todayIso);
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

  const context = await loadContext(client, todayIso, targetDates);
  const plan = buildPlan(context, { allowMinStayAutomation });
  const runAtIso = new Date().toISOString();

  const report = {
    automationId: AUTOMATION_ID,
    today: todayIso,
    dryRun,
    targetDates,
    applied: plan.applied,
    skipped: plan.skipped,
    verification: {
      verifiedRows: 0,
      mismatches: [],
    },
    warnings: allowMinStayAutomation
      ? []
      : ['Min stay automation disabilitata di default. Usa --allow-minstay=true o ALLOW_MIN_STAY_AUTOMATION=true per abilitarla.'],
  };

  if (!dryRun && plan.applied.length) {
    const token = await getBeds24AccessToken(beds24RefreshToken);
    const payload = buildBeds24Payload(plan.applied);
    const logIds = await createInventoryChangeLogs(client, plan.applied, runAtIso);
    try {
      await pushBeds24Calendar(token, payload);
      try {
        await upsertCalendarDays(client, plan.applied);
        report.verification = await verifyCalendarDays(client, plan.applied);
      } catch (error) {
        report.warnings.push(`calendar_days_verification_failed: ${error.message || error}`);
      }
      await updateInventoryChangeLogs(client, logIds, {
        status: report.verification.mismatches.length ? 'applied' : 'applied',
        after_snapshot: {
          applied_at: runAtIso,
          verified_rows: report.verification.verifiedRows,
          verification_mismatches: report.verification.mismatches.length,
        },
        error_message: [
          ...report.warnings,
          ...report.verification.mismatches.map((item) => `${item.unitLabel} ${item.date}: ${item.message}`),
        ].join(' | ') || null,
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
