import fs from 'node:fs/promises';
import path from 'node:path';
import { resolveRuntimeConfig } from './lib/netlify-runtime-config.mjs';

const SUPABASE_URL = 'https://tysxeikqbgebpfyblgeb.supabase.co';

function parseArgs(argv) {
  const args = {};
  for (const token of argv) {
    if (!token.startsWith('--')) continue;
    const [key, value] = token.slice(2).split('=');
    args[key] = value === undefined ? true : value;
  }
  return args;
}

function isoDate(date) {
  return new Date(date.getTime() - (date.getTimezoneOffset() * 60000)).toISOString().slice(0, 10);
}

function parseIsoDate(value) {
  return new Date(`${value}T00:00:00`);
}

function startOfMonth(date) {
  return new Date(date.getFullYear(), date.getMonth(), 1);
}

function addMonths(date, months) {
  return new Date(date.getFullYear(), date.getMonth() + months, 1);
}

function addDays(date, days) {
  const next = new Date(date);
  next.setDate(next.getDate() + days);
  return next;
}

function formatMonthLabel(date, locale = 'it-IT') {
  return new Intl.DateTimeFormat(locale, { month: 'long' }).format(date);
}

function resolveReportConfig(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  const reportDate = String(args['report-date'] || isoDate(new Date()));
  const reportDateObj = parseIsoDate(reportDate);
  const firstMonthStart = startOfMonth(reportDateObj);
  const secondMonthStart = addMonths(firstMonthStart, 1);
  const thirdMonthStart = addMonths(firstMonthStart, 2);

  const workdir = path.resolve(String(args.workdir || process.cwd()));
  const rangeStart = String(args['range-start'] || isoDate(firstMonthStart));
  const splitDate = String(args['split-date'] || isoDate(secondMonthStart));
  const rangeEnd = String(args['range-end'] || isoDate(addDays(thirdMonthStart, -1)));
  const firstLabel = String(args['first-label'] || formatMonthLabel(firstMonthStart));
  const secondLabel = String(args['second-label'] || formatMonthLabel(secondMonthStart));

  return {
    workdir,
    reportDate,
    rangeStart,
    splitDate,
    rangeEnd,
    firstLabel,
    secondLabel,
  };
}

function inferType(label = '') {
  const value = String(label).toLowerCase();
  if (value.includes('monolocale') || value.includes('studio')) return 'Monolocale';
  if (value.includes('bilocale')) return 'Bilocale';
  if (value.includes('trilocale')) return 'Trilocale';
  if (value.includes('stanza')) return 'Stanza';
  if (value.includes('cucina')) return 'Con cucina';
  if (value.includes('default')) return 'Appartamento';
  return 'Appartamento';
}

function round(value) {
  return Math.round(value * 100) / 100;
}

function avg(values) {
  if (!values.length) return null;
  return round(values.reduce((sum, value) => sum + value, 0) / values.length);
}

function median(values) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? round((sorted[mid - 1] + sorted[mid]) / 2)
    : sorted[mid];
}

function min(values) {
  return values.length ? Math.min(...values) : null;
}

function max(values) {
  return values.length ? Math.max(...values) : null;
}

function uniqueSorted(values) {
  return [...new Set(values)].sort((a, b) => a - b);
}

function isWeekend(dateIso) {
  const day = new Date(`${dateIso}T00:00:00Z`).getUTCDay();
  return day === 5 || day === 6;
}

function buildHeaders(apiKey) {
  return {
    apikey: apiKey,
    Authorization: `Bearer ${apiKey}`,
    Accept: 'application/json',
  };
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  const text = await response.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch (_error) {
    data = null;
  }
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText} ${text}`.trim());
  }
  return data;
}

async function fetchSupabaseRows(table, query, apiKey) {
  const url = `${SUPABASE_URL}/rest/v1/${table}?${query}`;
  return await fetchJson(url, { headers: buildHeaders(apiKey) });
}

async function fetchBeds24AccessToken(refreshToken) {
  const response = await fetch('https://api.beds24.com/v2/authentication/token', {
    method: 'GET',
    headers: {
      Accept: 'application/json',
      refreshToken,
    },
  });
  const text = await response.text();
  const data = text ? JSON.parse(text) : null;
  if (!response.ok || !data?.token) {
    throw new Error(`Beds24 auth failed: ${text || response.status}`);
  }
  return data.token;
}

async function fetchBeds24Properties(token) {
  const payload = await fetchJson(
    'https://api.beds24.com/v2/properties?includeAllRooms=true&includePriceRules=true&includeOffers=true',
    { headers: { Accept: 'application/json', token } }
  );
  return Array.isArray(payload?.data) ? payload.data : [];
}

function summarizeMonth(rows) {
  const pricedRows = rows.filter((row) => Number.isFinite(row.price));
  const prices = pricedRows.map((row) => row.price);
  const minStayValues = rows
    .map((row) => Number.isFinite(row.min_stay) ? row.min_stay : null)
    .filter((value) => value != null);
  const weekendPrices = pricedRows.filter((row) => isWeekend(row.date)).map((row) => row.price);
  const weekdayPrices = pricedRows.filter((row) => !isWeekend(row.date)).map((row) => row.price);
  return {
    totalDays: rows.length,
    pricedDays: pricedRows.length,
    closedDays: rows.filter((row) => row.closed === true).length,
    avgPrice: avg(prices),
    medianPrice: median(prices),
    minPrice: min(prices),
    maxPrice: max(prices),
    uniquePrices: uniqueSorted(prices),
    minStayValues: uniqueSorted(minStayValues),
    weekendAvg: avg(weekendPrices),
    weekdayAvg: avg(weekdayPrices),
  };
}

function buildStrategy(monthName, stats) {
  if (!stats.pricedDays) {
    return `${monthName}: nessun prezzo giornaliero sincronizzato`;
  }
  const notes = [];
  if (stats.uniquePrices.length <= 1) {
    notes.push(`tariffa piatta a ${stats.uniquePrices[0]} EUR`);
  } else {
    notes.push(`forchetta ${stats.minPrice}-${stats.maxPrice} EUR`);
  }
  if (stats.weekendAvg != null && stats.weekdayAvg != null) {
    const delta = round(stats.weekendAvg - stats.weekdayAvg);
    if (delta >= 10) notes.push(`weekend premium di circa ${delta} EUR`);
  }
  if (stats.minStayValues.length) {
    notes.push(`min stay ${stats.minStayValues.join('/')}`);
  }
  if (stats.closedDays) {
    notes.push(`${stats.closedDays} giorni chiusi`);
  }
  return `${monthName}: ${notes.join(', ')}`;
}

function csvEscape(value) {
  const text = value == null ? '' : String(value);
  if (!/[",\n]/.test(text)) return text;
  return `"${text.replace(/"/g, '""')}"`;
}

function roomTypeMeta(propertyById, roomId) {
  for (const property of propertyById.values()) {
    const room = (property.roomTypes || []).find((item) => String(item.id) === String(roomId));
    if (room) {
      return { property, room };
    }
  }
  return { property: null, room: null };
}

function sortUnits(a, b) {
  return a.struttura_nome.localeCompare(b.struttura_nome, 'it')
    || a.nome_appartamento.localeCompare(b.nome_appartamento, 'it')
    || a.unit_label.localeCompare(b.unit_label, 'it');
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    console.log('Usage: node scripts/export-beds24-opus-summary.mjs [--report-date=YYYY-MM-DD] [--range-start=YYYY-MM-DD] [--split-date=YYYY-MM-DD] [--range-end=YYYY-MM-DD] [--first-label=Nome] [--second-label=Nome] [--workdir=/path/to/repo]');
    process.exit(0);
  }

  const reportConfig = resolveReportConfig(process.argv.slice(2));
  const { supabaseKey, beds24RefreshToken } = resolveRuntimeConfig({
    projectRoot: reportConfig.workdir,
    defaultSupabaseUrl: SUPABASE_URL,
  });

  if (!supabaseKey) {
    throw new Error('SUPABASE_SERVICE_ROLE_KEY mancante');
  }
  if (!beds24RefreshToken) {
    throw new Error('BEDS24_API_KEY/BEDS24_REFRESH_TOKEN mancante');
  }

  const [apartments, apartmentUnits, calendarDays, beds24Token] = await Promise.all([
    fetchSupabaseRows(
      'apartments',
      'select=id,nome_appartamento,struttura_nome,attivo,beds24_property_id,beds24_room_id,beds24_calendar_room_id&attivo=eq.true&order=nome_appartamento',
      supabaseKey
    ),
    fetchSupabaseRows(
      'apartment_units',
      'select=id,apartment_id,unit_label,beds24_room_id,beds24_unit_index,active&active=eq.true&order=apartment_id.asc',
      supabaseKey
    ),
    fetchSupabaseRows(
      'calendar_days',
      `select=apartment_unit_id,date,price,min_stay,closed,available,source_updated_at&date=gte.${reportConfig.rangeStart}&date=lte.${reportConfig.rangeEnd}&order=apartment_unit_id.asc,date.asc&limit=5000`,
      supabaseKey
    ),
    fetchBeds24AccessToken(beds24RefreshToken),
  ]);

  const beds24Properties = await fetchBeds24Properties(beds24Token);

  const apartmentById = new Map(apartments.map((row) => [row.id, row]));
  const propertyById = new Map(beds24Properties.map((row) => [String(row.id), row]));
  const daysByUnitId = new Map();
  for (const row of calendarDays) {
    if (!daysByUnitId.has(row.apartment_unit_id)) daysByUnitId.set(row.apartment_unit_id, []);
    daysByUnitId.get(row.apartment_unit_id).push(row);
  }

  const unitSummaries = apartmentUnits
    .map((unit) => {
      const apartment = apartmentById.get(unit.apartment_id);
      if (!apartment) return null;
      const meta = roomTypeMeta(propertyById, unit.beds24_room_id);
      const property = meta.property || (apartment.beds24_property_id ? propertyById.get(String(apartment.beds24_property_id)) : null);
      const room = meta.room || null;
      const rows = daysByUnitId.get(unit.id) || [];
      const firstRows = rows.filter((row) => row.date < reportConfig.splitDate);
      const secondRows = rows.filter((row) => row.date >= reportConfig.splitDate);
      const firstStats = summarizeMonth(firstRows);
      const secondStats = summarizeMonth(secondRows);
      const typeLabel = inferType(unit.unit_label !== 'Default' ? unit.unit_label : (room?.name || apartment.nome_appartamento));
      return {
        struttura_nome: apartment.struttura_nome || '',
        nome_appartamento: apartment.nome_appartamento,
        unit_label: unit.unit_label || 'Default',
        typeLabel,
        roomType: room?.roomType || null,
        maxPeople: room?.maxPeople || null,
        rackRate: Number.isFinite(Number(room?.rackRate)) ? Number(room.rackRate) : null,
        roomMinStay: Number.isFinite(Number(room?.minStay)) ? Number(room.minStay) : null,
        priceRuleCount: Array.isArray(room?.priceRules) ? room.priceRules.filter((item) => item?.id === 1 || item?.name || item?.offer).length : 0,
        firstStats,
        secondStats,
        firstStrategy: buildStrategy(reportConfig.firstLabel, firstStats),
        secondStrategy: buildStrategy(reportConfig.secondLabel, secondStats),
        sourceUpdatedAt: rows.at(-1)?.source_updated_at || null,
      };
    })
    .filter(Boolean)
    .sort(sortUnits);

  const reportLines = [];
  reportLines.push('# Riepilogo prezzi Beds24 per Opus');
  reportLines.push('');
  reportLines.push(`Report generato il ${reportConfig.reportDate}.`);
  reportLines.push(`Periodo analizzato: dal ${reportConfig.rangeStart} al ${reportConfig.rangeEnd}.`);
  reportLines.push('');
  reportLines.push('## Sintesi strategica');
  reportLines.push('');

  const pricedUnits = unitSummaries.filter((item) => item.firstStats.pricedDays || item.secondStats.pricedDays);
  const noPriceUnits = unitSummaries.filter((item) => !item.firstStats.pricedDays && !item.secondStats.pricedDays);
  const secondHigher = pricedUnits.filter((item) => item.firstStats.avgPrice != null && item.secondStats.avgPrice != null && item.secondStats.avgPrice > item.firstStats.avgPrice);
  const weekendPremium = pricedUnits.filter((item) => {
    const firstDelta = (item.firstStats.weekendAvg ?? 0) - (item.firstStats.weekdayAvg ?? 0);
    const secondDelta = (item.secondStats.weekendAvg ?? 0) - (item.secondStats.weekdayAvg ?? 0);
    return firstDelta >= 10 || secondDelta >= 10;
  });

  reportLines.push(`- Unità analizzate: ${unitSummaries.length}`);
  reportLines.push(`- Unità con prezzi giornalieri sincronizzati: ${pricedUnits.length}`);
  reportLines.push(`- Unità senza prezzi sincronizzati ma con configurazione Beds24 presente: ${noPriceUnits.length}`);
  reportLines.push(`- Unità con ${reportConfig.secondLabel} mediamente più alto di ${reportConfig.firstLabel}: ${secondHigher.length}`);
  reportLines.push(`- Unità con premium weekend evidente: ${weekendPremium.length}`);
  reportLines.push('');
  reportLines.push('## Dettaglio per appartamento');
  reportLines.push('');

  for (const item of unitSummaries) {
    reportLines.push(`### ${item.nome_appartamento}${item.unit_label !== 'Default' ? ` / ${item.unit_label}` : ''}`);
    reportLines.push(`- Struttura: ${item.struttura_nome || 'n/d'}`);
    reportLines.push(`- Tipologia: ${item.typeLabel}${item.roomType ? ` (${item.roomType})` : ''}${item.maxPeople ? `, max ${item.maxPeople} ospiti` : ''}`);
    reportLines.push(`- Rack rate Beds24: ${item.rackRate != null ? `${item.rackRate} EUR` : 'n/d'}`);
    reportLines.push(`- Min stay base Beds24: ${item.roomMinStay != null ? item.roomMinStay : 'n/d'}`);
    reportLines.push(`- ${reportConfig.firstLabel}: ${item.firstStats.pricedDays ? `media ${item.firstStats.avgPrice} EUR, min ${item.firstStats.minPrice}, max ${item.firstStats.maxPrice}, min stay ${item.firstStats.minStayValues.join('/') || 'n/d'}, chiusi ${item.firstStats.closedDays}/${item.firstStats.totalDays}` : 'nessun prezzo giornaliero sincronizzato'}`);
    reportLines.push(`- ${reportConfig.secondLabel}: ${item.secondStats.pricedDays ? `media ${item.secondStats.avgPrice} EUR, min ${item.secondStats.minPrice}, max ${item.secondStats.maxPrice}, min stay ${item.secondStats.minStayValues.join('/') || 'n/d'}, chiusi ${item.secondStats.closedDays}/${item.secondStats.totalDays}` : 'nessun prezzo giornaliero sincronizzato'}`);
    reportLines.push(`- Strategia ${reportConfig.firstLabel}: ${item.firstStrategy.replace(new RegExp(`^${reportConfig.firstLabel}:\\s*`, 'u'), '')}`);
    reportLines.push(`- Strategia ${reportConfig.secondLabel}: ${item.secondStrategy.replace(new RegExp(`^${reportConfig.secondLabel}:\\s*`, 'u'), '')}`);
    if (!item.firstStats.pricedDays && !item.secondStats.pricedDays) {
      reportLines.push('- Nota: in Supabase risultano solo disponibilità/chiusure; per questa unità la sintesi si basa sulla configurazione Beds24 e non su prezzi giornalieri sincronizzati.');
    }
    reportLines.push('');
  }

  const csvLines = [
    [
      'struttura_nome',
      'nome_appartamento',
      'unit_label',
      'tipologia',
      'month',
      'avg_price',
      'min_price',
      'max_price',
      'weekend_avg',
      'weekday_avg',
      'min_stay_values',
      'closed_days',
      'priced_days',
      'rack_rate',
      'room_min_stay',
      'source_updated_at',
    ].join(','),
  ];

  for (const item of unitSummaries) {
    for (const [month, stats] of [[reportConfig.firstLabel, item.firstStats], [reportConfig.secondLabel, item.secondStats]]) {
      csvLines.push([
        item.struttura_nome,
        item.nome_appartamento,
        item.unit_label,
        item.typeLabel,
        month,
        stats.avgPrice,
        stats.minPrice,
        stats.maxPrice,
        stats.weekendAvg,
        stats.weekdayAvg,
        stats.minStayValues.join('/'),
        stats.closedDays,
        stats.pricedDays,
        item.rackRate,
        item.roomMinStay,
        item.sourceUpdatedAt,
      ].map(csvEscape).join(','));
    }
  }

  const reportDir = path.join(reportConfig.workdir, 'reports');
  const mdPath = path.join(reportDir, `beds24-opus-confronto-${reportConfig.reportDate}.md`);
  const csvPath = path.join(reportDir, `beds24-opus-confronto-${reportConfig.reportDate}.csv`);

  await fs.mkdir(reportDir, { recursive: true });
  await fs.writeFile(mdPath, `${reportLines.join('\n')}\n`, 'utf8');
  await fs.writeFile(csvPath, `${csvLines.join('\n')}\n`, 'utf8');

  console.log(JSON.stringify({
    markdown: mdPath,
    csv: csvPath,
    units: unitSummaries.length,
    pricedUnits: pricedUnits.length,
    noPriceUnits: noPriceUnits.length,
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
