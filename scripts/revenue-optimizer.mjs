import { createClient } from '@supabase/supabase-js';

const MAX_WINDOW_DAYS = 15;
const DEFAULT_HISTORY_DAYS = 30;
const OCCUPANCY_LOW_THRESHOLD = 0.6;

function parseArgs(argv) {
  const args = {};
  for (const token of argv) {
    if (!token.startsWith('--')) continue;
    const [key, value] = token.slice(2).split('=');
    args[key] = value === undefined ? true : value;
  }
  return args;
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

function diffDays(from, to) {
  return Math.round((parseIsoDate(to) - parseIsoDate(from)) / 86400000);
}

function clampWindowDays(days) {
  return Math.max(1, Math.min(MAX_WINDOW_DAYS, days));
}

function weekdayLabel(date) {
  const day = parseIsoDate(date).getDay();
  return day === 5 || day === 6 ? 'weekend' : 'weekday';
}

function buildDateRange(startIso, windowDays) {
  return Array.from({ length: windowDays }, (_, index) => isoDate(addDays(parseIsoDate(startIso), index)));
}

function discountForLeadDays(daysUntil) {
  if (daysUntil <= 0) return 20;
  if (daysUntil === 1) return 18;
  if (daysUntil === 2) return 16;
  if (daysUntil === 3) return 15;
  if (daysUntil === 4) return 13;
  if (daysUntil === 5) return 12;
  if (daysUntil === 6) return 11;
  if (daysUntil === 7) return 10;
  if (daysUntil <= 10) return 8;
  return 6;
}

function normalizePrice(value) {
  const price = Number(value);
  if (!Number.isFinite(price) || price <= 0) return null;
  return Math.round(price);
}

function applyDiscount(basePrice, discountPct) {
  const price = normalizePrice(basePrice);
  if (price == null) return null;
  return Math.max(1, Math.round(price * (1 - (discountPct / 100))));
}

function openDay(row) {
  return Boolean(row?.available) && !Boolean(row?.closed);
}

function makeAction(unit, row, overrides = {}) {
  const discountPct = overrides.discountPct ?? 0;
  const currentPrice = normalizePrice(row.price);
  const recommendedPrice = overrides.price ?? applyDiscount(currentPrice, discountPct);
  return {
    apartmentId: unit.apartment_id,
    apartmentName: unit.apartment_name,
    unitId: unit.id,
    unitLabel: unit.unit_label,
    roomId: unit.beds24_room_id,
    dateFrom: row.date,
    dateTo: row.date,
    currentPrice,
    recommendedPrice,
    recommendedMinStay: overrides.minStay ?? null,
    recommendedClosed: overrides.closed ?? null,
    discountPct,
    tags: overrides.tags || [],
    rationale: overrides.rationale || '',
    priority: overrides.priority ?? 50,
  };
}

function dedupeActions(actions) {
  const map = new Map();
  for (const action of actions) {
    const key = `${action.unitId}:${action.dateFrom}`;
    const existing = map.get(key);
    if (!existing || action.priority > existing.priority) {
      map.set(key, action);
    }
  }
  return [...map.values()].sort((a, b) => {
    if (b.priority !== a.priority) return b.priority - a.priority;
    if (a.apartmentName !== b.apartmentName) return a.apartmentName.localeCompare(b.apartmentName, 'it');
    return a.dateFrom.localeCompare(b.dateFrom);
  });
}

function detectShortGaps(rows) {
  const gaps = [];
  let cursor = 0;
  while (cursor < rows.length) {
    if (!openDay(rows[cursor])) {
      cursor += 1;
      continue;
    }
    let end = cursor;
    while (end + 1 < rows.length && openDay(rows[end + 1])) end += 1;
    const length = end - cursor + 1;
    const boundedLeft = cursor > 0 && !openDay(rows[cursor - 1]);
    const boundedRight = end < rows.length - 1 && !openDay(rows[end + 1]);
    if (length <= 2 && boundedLeft && boundedRight) {
      gaps.push({ start: cursor, end, length });
    }
    cursor = end + 1;
  }
  return gaps;
}

function groupInventoryChanges(changes) {
  const byUnit = new Map();
  for (const row of changes) {
    const key = String(row.apartment_id || '');
    if (!byUnit.has(key)) byUnit.set(key, []);
    byUnit.get(key).push(row);
  }
  return byUnit;
}

function buildUnitContext(units, calendarDays, inventoryChanges, todayIso, windowDays) {
  const dates = buildDateRange(todayIso, windowDays);
  const calendarByUnit = new Map();
  for (const row of calendarDays) {
    const key = String(row.apartment_unit_id);
    if (!calendarByUnit.has(key)) calendarByUnit.set(key, new Map());
    calendarByUnit.get(key).set(row.date, row);
  }
  const changesByApartment = groupInventoryChanges(inventoryChanges);

  return units.map((unit) => {
    const rows = dates.map((date) => {
      const raw = calendarByUnit.get(String(unit.id))?.get(date) || null;
      return {
        apartment_unit_id: unit.id,
        date,
        price: raw?.price ?? null,
        min_stay: raw?.min_stay ?? null,
        available: raw?.available ?? null,
        closed: raw?.closed ?? false,
      };
    });
    const openRows = rows.filter(openDay);
    const occupancyLow = openRows.length / Math.max(1, rows.length) >= OCCUPANCY_LOW_THRESHOLD;
    return {
      ...unit,
      rows,
      openRows,
      openRatio: Number((openRows.length / Math.max(1, rows.length)).toFixed(2)),
      occupancyLow,
      recentChanges: changesByApartment.get(String(unit.apartment_id)) || [],
      shortGaps: detectShortGaps(rows),
    };
  });
}

function buildProgressiveActions(unitContexts, todayIso) {
  const actions = [];
  for (const unit of unitContexts) {
    for (const row of unit.rows) {
      if (!openDay(row)) continue;
      const daysUntil = diffDays(todayIso, row.date);
      if (daysUntil < 0 || daysUntil > 7) continue;
      const discountPct = discountForLeadDays(daysUntil);
      actions.push(makeAction(unit, row, {
        discountPct,
        priority: 70 - daysUntil,
        tags: ['progressive'],
        rationale: `Data libera a ${daysUntil} giorni dal check-in: sconto progressivo del ${discountPct}%`,
      }));
    }
  }
  return actions;
}

function buildTodayLastMinuteActions(unitContexts, todayIso) {
  const actions = [];
  for (const unit of unitContexts) {
    const row = unit.rows.find((item) => item.date === todayIso);
    if (!row || !openDay(row)) continue;
    actions.push(makeAction(unit, row, {
      discountPct: 20,
      minStay: 1,
      closed: false,
      priority: 100,
      tags: ['last-minute', 'today'],
      rationale: 'Data di oggi libera: apertura last minute con min stay 1 e sconto 20%',
    }));
  }
  return actions;
}

function buildWeeklyTenActions(unitContexts, todayIso) {
  const today = parseIsoDate(todayIso);
  const weekDay = today.getDay();
  const mondayOffset = weekDay === 0 ? -6 : 1 - weekDay;
  const monday = isoDate(addDays(today, mondayOffset));
  const sunday = isoDate(addDays(parseIsoDate(monday), 6));
  const actions = [];

  for (const unit of unitContexts) {
    for (const row of unit.rows) {
      if (!openDay(row)) continue;
      if (row.date < monday || row.date > sunday) continue;
      actions.push(makeAction(unit, row, {
        discountPct: 10,
        priority: 55,
        tags: ['weekly-10'],
        rationale: 'Sconto settimanale standard del 10% tra lunedi e domenica',
      }));
    }
  }
  return actions;
}

function buildGapActions(unitContexts) {
  const actions = [];
  for (const unit of unitContexts) {
    for (const gap of unit.shortGaps) {
      for (let index = gap.start; index <= gap.end; index += 1) {
        const row = unit.rows[index];
        const discountPct = gap.length === 1 ? 18 : 12;
        actions.push(makeAction(unit, row, {
          discountPct,
          minStay: 1,
          priority: gap.length === 1 ? 95 : 85,
          tags: ['gap-fill', `gap-${gap.length}`],
          rationale: `Buco di ${gap.length} notte${gap.length > 1 ? 'i' : ''} tra date non vendibili: min stay 1 e sconto ${discountPct}%`,
        }));
      }
    }
  }
  return actions;
}

function buildLowOccupancyActions(unitContexts, todayIso) {
  const actions = [];
  for (const unit of unitContexts) {
    if (!unit.occupancyLow) continue;
    for (const row of unit.rows) {
      if (!openDay(row)) continue;
      const daysUntil = diffDays(todayIso, row.date);
      if (daysUntil < 0 || daysUntil > 10) continue;
      const weekend = weekdayLabel(row.date) === 'weekend';
      const discountPct = weekend ? 8 : 12;
      actions.push(makeAction(unit, row, {
        discountPct,
        priority: weekend ? 48 : 52,
        tags: ['low-occupancy', weekend ? 'weekend' : 'weekday'],
        rationale: `Occupazione bassa nei prossimi 15 giorni (${Math.round(unit.openRatio * 100)}% di notti aperte): spinta ${weekend ? 'prudente' : 'decisa'} su ${weekend ? 'weekend' : 'feriale'}`,
      }));
    }
  }
  return actions;
}

function buildWeekdayWeekendActions(unitContexts, todayIso) {
  const actions = [];
  for (const unit of unitContexts) {
    for (const row of unit.rows) {
      if (!openDay(row)) continue;
      const daysUntil = diffDays(todayIso, row.date);
      if (daysUntil < 0 || daysUntil > 10) continue;
      const weekend = weekdayLabel(row.date) === 'weekend';
      const discountPct = weekend ? (daysUntil <= 5 ? 6 : 4) : (daysUntil <= 5 ? 10 : 6);
      actions.push(makeAction(unit, row, {
        discountPct,
        priority: weekend ? 42 : 44,
        tags: ['weekday-weekend', weekend ? 'weekend' : 'weekday'],
        rationale: `Taratura ${weekend ? 'weekend' : 'feriale'} per data libera a ${daysUntil} giorni`,
      }));
    }
  }
  return actions;
}

function buildUnsoldPriorityActions(unitContexts, todayIso) {
  const cutoff = isoDate(addDays(parseIsoDate(todayIso), -21));
  const actions = [];
  for (const unit of unitContexts) {
    const recentTouched = unit.recentChanges.some((change) => (change.created_at || '').slice(0, 10) >= cutoff);
    if (recentTouched || unit.openRatio < 0.5) continue;
    for (const row of unit.rows) {
      if (!openDay(row)) continue;
      const daysUntil = diffDays(todayIso, row.date);
      if (daysUntil < 0 || daysUntil > MAX_WINDOW_DAYS - 1) continue;
      actions.push(makeAction(unit, row, {
        discountPct: weekdayLabel(row.date) === 'weekend' ? 5 : 8,
        priority: 46,
        tags: ['unsold-priority'],
        rationale: 'Alloggio mai toccato di recente dagli sconti: lieve accelerazione sulle date ancora aperte',
      }));
    }
  }
  return actions;
}

function summarize(unitContexts, actions) {
  const openNights = unitContexts.reduce((total, unit) => total + unit.openRows.length, 0);
  const lowOccupancyUnits = unitContexts.filter((unit) => unit.occupancyLow).length;
  const oneNightGaps = unitContexts.reduce((total, unit) => total + unit.shortGaps.filter((gap) => gap.length === 1).length, 0);
  const twoNightGaps = unitContexts.reduce((total, unit) => total + unit.shortGaps.filter((gap) => gap.length === 2).length, 0);
  const neverTouched = unitContexts.filter((unit) => unit.recentChanges.length === 0).length;
  return {
    apartments: new Set(unitContexts.map((unit) => unit.apartment_id)).size,
    units: unitContexts.length,
    openNights,
    lowOccupancyUnits,
    oneNightGaps,
    twoNightGaps,
    neverTouched,
    proposedActions: actions.length,
  };
}

function renderMarkdown(strategy, windowDays, todayIso, summary, actions, unitContexts) {
  const lines = [];
  lines.push(`# Revenue Optimizer`);
  lines.push('');
  lines.push(`- strategy: ${strategy}`);
  lines.push(`- today: ${todayIso}`);
  lines.push(`- window_days: ${windowDays}`);
  lines.push(`- apartments: ${summary.apartments}`);
  lines.push(`- units: ${summary.units}`);
  lines.push(`- open_nights: ${summary.openNights}`);
  lines.push(`- low_occupancy_units: ${summary.lowOccupancyUnits}`);
  lines.push(`- one_night_gaps: ${summary.oneNightGaps}`);
  lines.push(`- two_night_gaps: ${summary.twoNightGaps}`);
  lines.push(`- never_touched_recently: ${summary.neverTouched}`);
  lines.push(`- proposed_actions: ${summary.proposedActions}`);
  lines.push('');
  lines.push(`## Top Actions`);

  if (!actions.length) {
    lines.push('');
    lines.push(`Nessuna azione consigliata nella finestra di ${windowDays} giorni.`);
  } else {
    for (const action of actions.slice(0, 50)) {
      lines.push(`- ${action.apartmentName} / ${action.unitLabel} / ${action.dateFrom}: prezzo ${action.currentPrice ?? 'n.d.'} -> ${action.recommendedPrice ?? 'n.d.'}, sconto ${action.discountPct}%, min stay ${action.recommendedMinStay ?? '-'} [${action.tags.join(', ')}]`);
      lines.push(`  motivo: ${action.rationale}`);
    }
  }

  const riskyUnits = unitContexts
    .filter((unit) => unit.openRatio >= 0.7)
    .sort((a, b) => b.openRatio - a.openRatio)
    .slice(0, 10);

  lines.push('');
  lines.push('## Alloggi Da Sorvegliare');
  if (!riskyUnits.length) {
    lines.push('');
    lines.push('Nessun alloggio con pressione alta nella finestra analizzata.');
  } else {
    for (const unit of riskyUnits) {
      lines.push(`- ${unit.apartment_name} / ${unit.unit_label}: ${Math.round(unit.openRatio * 100)}% notti ancora aperte nei prossimi ${windowDays} giorni`);
    }
  }

  return `${lines.join('\n')}\n`;
}

async function loadSupabaseData(client, todayIso, windowDays) {
  const endIso = isoDate(addDays(parseIsoDate(todayIso), windowDays - 1));
  const historyCutoff = isoDate(addDays(parseIsoDate(todayIso), -DEFAULT_HISTORY_DAYS));

  const [{ data: apartments, error: apartmentsError }, { data: units, error: unitsError }, { data: calendarDays, error: calendarError }, { data: inventoryChanges, error: changesError }] = await Promise.all([
    client
      .from('apartments')
      .select('id,nome_appartamento,attivo')
      .eq('attivo', true),
    client
      .from('apartment_units')
      .select('id,apartment_id,unit_label,beds24_room_id,active')
      .eq('active', true)
      .not('beds24_room_id', 'is', null),
    client
      .from('calendar_days')
      .select('apartment_unit_id,date,price,min_stay,available,closed')
      .gte('date', todayIso)
      .lte('date', endIso),
    client
      .from('beds24_inventory_changes')
      .select('apartment_id,created_at,status')
      .gte('created_at', `${historyCutoff}T00:00:00Z`),
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
    }));

  return {
    units: hydratedUnits,
    calendarDays: calendarDays || [],
    inventoryChanges: (inventoryChanges || []).filter((row) => row.status !== 'beds24_error'),
  };
}

function planForStrategy(strategy, unitContexts, todayIso) {
  switch (strategy) {
    case 'weekly-10':
      return dedupeActions(buildWeeklyTenActions(unitContexts, todayIso));
    case 'today-last-minute':
      return dedupeActions(buildTodayLastMinuteActions(unitContexts, todayIso));
    case 'progressive':
      return dedupeActions(buildProgressiveActions(unitContexts, todayIso));
    case 'gaps':
      return dedupeActions(buildGapActions(unitContexts));
    case 'low-occupancy':
      return dedupeActions(buildLowOccupancyActions(unitContexts, todayIso));
    case 'weekday-weekend':
      return dedupeActions(buildWeekdayWeekendActions(unitContexts, todayIso));
    case 'unsold-priority':
      return dedupeActions(buildUnsoldPriorityActions(unitContexts, todayIso));
    case 'combined':
      return dedupeActions([
        ...buildGapActions(unitContexts),
        ...buildTodayLastMinuteActions(unitContexts, todayIso),
        ...buildProgressiveActions(unitContexts, todayIso),
        ...buildLowOccupancyActions(unitContexts, todayIso),
        ...buildWeekdayWeekendActions(unitContexts, todayIso),
        ...buildUnsoldPriorityActions(unitContexts, todayIso),
      ]);
    case 'report':
      return dedupeActions([
        ...buildGapActions(unitContexts),
        ...buildLowOccupancyActions(unitContexts, todayIso),
        ...buildUnsoldPriorityActions(unitContexts, todayIso),
      ]);
    default:
      throw new Error(`Strategia non supportata: ${strategy}`);
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    console.log('Usage: node scripts/revenue-optimizer.mjs --strategy=combined --window-days=15 [--today=YYYY-MM-DD] [--format=json|markdown]');
    process.exit(0);
  }

  const strategy = String(args.strategy || 'combined');
  const format = String(args.format || 'markdown');
  const windowDays = clampWindowDays(parseIntArg(args['window-days'], MAX_WINDOW_DAYS));
  const todayIso = String(args.today || isoDate(new Date()));
  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;

  if (!supabaseUrl || !supabaseKey) {
    throw new Error('SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY/SUPABASE_SERVICE_KEY sono obbligatori');
  }

  const client = createClient(supabaseUrl, supabaseKey, {
    auth: { persistSession: false, autoRefreshToken: false },
  });

  const source = await loadSupabaseData(client, todayIso, windowDays);
  const unitContexts = buildUnitContext(source.units, source.calendarDays, source.inventoryChanges, todayIso, windowDays);
  const actions = planForStrategy(strategy, unitContexts, todayIso);
  const summary = summarize(unitContexts, actions);

  if (format === 'json') {
    console.log(JSON.stringify({
      strategy,
      today: todayIso,
      windowDays,
      summary,
      actions,
    }, null, 2));
    return;
  }

  process.stdout.write(renderMarkdown(strategy, windowDays, todayIso, summary, actions, unitContexts));
}

main().catch((error) => {
  console.error(error.message || error);
  process.exit(1);
});
