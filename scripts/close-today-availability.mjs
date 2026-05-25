import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createClient } from '@supabase/supabase-js';
import { resolveRuntimeConfig } from './lib/netlify-runtime-config.mjs';

const AUTOMATION_ID = 'chiudi-disponibilita-oggi-2215';
const AUTOMATION_ACTOR = `automation:${AUTOMATION_ID}`;
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

function normalizeRoomId(value) {
  const normalized = String(value || '').trim();
  return normalized || null;
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
    .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0], 'it'))
    .map(([reason, count]) => ({ reason, count }));
}

function renderMarkdown(report) {
  const lines = [
    `# ${AUTOMATION_ID}`,
    '',
    `- today: ${report.today}`,
    `- dry_run: ${report.dryRun}`,
    `- units_closed: ${report.summary.unitsClosed}`,
    `- apartments_touched: ${report.summary.apartmentsTouched}`,
    `- rooms_closed: ${report.summary.roomsClosed}`,
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
    lines.push('Nessuna disponibilita chiusa.');
  } else {
    for (const item of report.applied) {
      lines.push(`- ${item.apartmentName} / ${item.unitLabel} / ${item.date}: open -> closed`);
    }
  }

  return `${lines.join('\n')}\n`;
}

function normalizeHeaders(headers) {
  if (!headers) return [];
  if (headers instanceof Headers) return [...headers.entries()];
  if (Array.isArray(headers)) return headers.map(([key, value]) => [String(key), String(value)]);
  return Object.entries(headers).map(([key, value]) => [String(key), String(value)]);
}

function parseCurlHeaderText(raw) {
  const normalized = String(raw || '').replace(/\r\n/g, '\n');
  const blocks = normalized
    .split(/(?=HTTP\/[0-9.]+\s+\d+)/)
    .map((block) => block.trim())
    .filter(Boolean);
  const lastBlock = blocks.at(-1) || '';
  const lines = lastBlock.split('\n').filter(Boolean);
  const statusLine = lines.shift() || '';
  const status = Number.parseInt(statusLine.split(/\s+/)[1] || '0', 10);
  const headers = new Headers();

  for (const line of lines) {
    const separatorIndex = line.indexOf(':');
    if (separatorIndex === -1) continue;
    const key = line.slice(0, separatorIndex).trim();
    const value = line.slice(separatorIndex + 1).trim();
    headers.append(key, value);
  }

  return { status, headers };
}

async function fetchWithCurl(input, init = {}) {
  const url = typeof input === 'string' || input instanceof URL
    ? String(input)
    : String(input?.url || '');

  const method = String(init.method || input?.method || 'GET').toUpperCase();
  const headerEntries = [
    ...normalizeHeaders(input?.headers),
    ...normalizeHeaders(init.headers),
  ];

  const bodyValue = init.body ?? input?.body ?? null;
  const body = bodyValue == null
    ? null
    : typeof bodyValue === 'string'
      ? bodyValue
      : bodyValue instanceof Uint8Array || Buffer.isBuffer(bodyValue)
        ? bodyValue
        : String(bodyValue);

  const tempDir = mkdtempSync(path.join(os.tmpdir(), 'close-today-curl-'));
  const headerPath = path.join(tempDir, 'headers.txt');

  try {
    const args = ['--silent', '--show-error', '--location', '--request', method, '--dump-header', headerPath];
    for (const [key, value] of headerEntries) {
      args.push('--header', `${key}: ${value}`);
    }
    if (body != null) args.push('--data-binary', '@-');
    args.push(url);

    const stdout = execFileSync('curl', args, {
      encoding: 'utf8',
      input: body ?? undefined,
      stdio: ['pipe', 'pipe', 'pipe'],
      maxBuffer: 1024 * 1024 * 10,
    });

    const { status, headers } = parseCurlHeaderText(readFileSync(headerPath, 'utf8'));
    const textBody = String(stdout || '');

    return {
      ok: status >= 200 && status < 300,
      status,
      headers,
      text: async () => textBody,
      json: async () => (textBody ? JSON.parse(textBody) : null),
    };
  } finally {
    rmSync(tempDir, { recursive: true, force: true });
  }
}

async function getBeds24AccessToken(refreshToken, forceRefresh = false) {
  if (!forceRefresh && beds24TokenCache && beds24TokenCache.refreshToken === refreshToken && beds24TokenCache.expiresAt > Date.now()) {
    return beds24TokenCache.token;
  }

  const res = await fetchWithCurl(`${BEDS24_URL}/authentication/token`, {
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
  const response = await fetchWithCurl(`${BEDS24_URL}/inventory/rooms/calendar`, {
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
      .from('v_calendar_occupancy')
      .select('apartment_unit_id,date')
      .eq('date', todayIso),
    client
      .from('beds24_inventory_changes')
      .select('room_id,date_from,date_to,status,write_payload')
      .eq('created_by', AUTOMATION_ACTOR)
      .neq('status', 'beds24_error')
      .eq('date_from', todayIso)
      .eq('date_to', todayIso),
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
    units: hydratedUnits,
    calendarDays: calendarDays || [],
    occupancyDays: occupancyDays || [],
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
    const roomId = normalizeRoomId(unit.beds24_room_id);
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
  for (const row of context.priorChanges || []) {
    const payload = row.write_payload && typeof row.write_payload === 'object'
      ? row.write_payload
      : {};
    const payloadUnitId = payload.apartment_unit_id || null;
    if (payloadUnitId) {
      priorTouchedByUnitDate.add(`${payloadUnitId}:${context.today}`);
      continue;
    }
    if (row.room_id) {
      for (const unit of roomIdGroups.get(String(row.room_id)) || []) {
        priorTouchedByUnitDate.add(`${unit.id}:${context.today}`);
      }
    }
  }

  const roomOpenCountBefore = new Map();
  for (const unit of context.units) {
    const roomId = normalizeRoomId(unit.beds24_room_id);
    if (!roomId) continue;
    const key = `${unit.id}:${context.today}`;
    const day = calendarByUnitDate.get(key) || null;
    const isOpen = day && day.closed !== true && day.available === true && !occupiedByUnitDate.has(key);
    if (!isOpen) continue;
    roomOpenCountBefore.set(roomId, (roomOpenCountBefore.get(roomId) || 0) + 1);
  }

  const applied = [];
  const skipped = [];

  for (const unit of context.units) {
    const roomId = normalizeRoomId(unit.beds24_room_id);
    if (!roomId) {
      skipped.push(makeSkip(unit, context.today, 'beds24_mapping_incomplete'));
      continue;
    }

    const key = `${unit.id}:${context.today}`;
    const day = calendarByUnitDate.get(key) || null;
    if (!day) {
      skipped.push(makeSkip(unit, context.today, 'calendar_day_missing'));
      continue;
    }
    if (priorTouchedByUnitDate.has(key)) {
      skipped.push(makeSkip(unit, context.today, 'already_touched_by_automation'));
      continue;
    }
    if (occupiedByUnitDate.has(key)) {
      skipped.push(makeSkip(unit, context.today, 'date_occupied'));
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

    applied.push({
      apartmentId: unit.apartment_id,
      apartmentName: unit.apartment_name,
      apartmentPropertyId: unit.apartment_property_id,
      unitId: unit.id,
      unitLabel: unit.unit_label,
      roomId,
      date: context.today,
      currentPrice: Number.isFinite(Number(day.price)) ? Number(day.price) : null,
      currentMinStay: Number.isFinite(Number(day.min_stay)) ? Number(day.min_stay) : null,
      currentAvailable: day.available === true,
      currentClosed: day.closed === true,
      roomOpenCountBefore: roomOpenCountBefore.get(roomId) || 0,
    });
  }

  return { applied, skipped };
}

function buildBeds24Payload(applied) {
  const rooms = unique(applied.map((item) => item.roomId));
  return rooms.map((roomId) => ({
    roomId: Number.isFinite(Number(roomId)) ? Number(roomId) : roomId,
    calendar: [
      {
        from: applied[0]?.date,
        to: applied[0]?.date,
        numAvail: 0,
      },
    ],
  }));
}

async function upsertCalendarDays(client, applied) {
  if (!applied.length) return;

  const timestamp = new Date().toISOString();
  const rows = applied.map((item) => ({
    apartment_unit_id: item.unitId,
    date: item.date,
    closed: true,
    available: false,
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
      price: item.currentPrice,
      min_stay: item.currentMinStay,
      available: item.currentAvailable,
      closed: item.currentClosed,
      room_open_count_before: item.roomOpenCountBefore,
    },
    write_payload: {
      automation_id: AUTOMATION_ID,
      apartment_unit_id: item.unitId,
      apartment_name: item.apartmentName,
      unit_label: item.unitLabel,
      date: item.date,
      previous_available: item.currentAvailable,
      previous_closed: item.currentClosed,
      new_available: false,
      new_closed: true,
      triggered_at: runAtIso,
    },
    after_snapshot: null,
    rollback_payload: {
      roomId: item.roomId,
      calendar: [
        {
          from: item.date,
          to: item.date,
          numAvail: item.roomOpenCountBefore,
        },
      ],
      local_row: {
        apartment_unit_id: item.unitId,
        date: item.date,
        available: item.currentAvailable,
        closed: item.currentClosed,
      },
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
    unitsClosed: report.applied.length,
    apartmentsTouched: unique(report.applied.map((item) => item.apartmentId)).length,
    roomsClosed: unique(report.applied.map((item) => item.roomId)).length,
    skippedCases: report.skipped.length,
    skipReasons: summarizeSkips(report.skipped),
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    console.log('Usage: node scripts/close-today-availability.mjs [--today=YYYY-MM-DD] [--dry-run=true|false] [--format=json|markdown]');
    process.exit(0);
  }

  const dryRun = parseBoolean(args['dry-run'], false);
  const format = String(args.format || 'json').trim().toLowerCase();
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
        after_snapshot: {
          available: false,
          closed: true,
          applied_at: runAtIso,
        },
        error_message: report.warnings.length ? report.warnings.join(' | ') : null,
      });
    } catch (error) {
      try {
        await updateInventoryChangeLogs(client, logIds, {
          status: 'beds24_error',
          error_message: error.message || String(error),
        });
      } catch (logError) {
        report.warnings.push(`inventory_change_log_update_failed: ${logError.message || logError}`);
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
