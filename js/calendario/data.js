import {
  CHANNEL_CONFIG,
  CITY_PRIORITY,
  KNOWN_CITY_MAP,
  OCCUPYING_STATUSES,
  S,
  STATUS_LABELS,
} from './state.js?v=20260627a';
import {
  formatDate,
  formatPrice,
  getMonthBounds,
  groupBy,
  isoDateLocal,
  normalizeText,
  sum,
  titleCase,
  unique,
  compareBookingsForRender,
  compareOrphans,
} from './utils.js?v=20260627a';

const KEKKO_UNIT_CATALOG = [
  { sourceId: 'M1', title: '104 Mono con giardino', floor: 'PT', capacity: 3, groupName: 'Monolocale · Piano Terra', note: '', sortOrder: 2, aliases: ['104', 'Mono PT con giardino', '104 Mono con giardino'] },
  { sourceId: 'M2', title: '210 Mono', floor: '2°', capacity: 2, groupName: 'Monolocale · Secondo Piano', note: '', sortOrder: 10, aliases: ['Mono 1° A', '210', '210 Mono', 'Mono 210'] },
  { sourceId: 'M3', title: '208 Mono vista mare', floor: '2°', capacity: 2, groupName: 'Monolocale · Secondo Piano', note: '', sortOrder: 9, aliases: ['Mono 1° B', '208', '208 Mono', 'Mono 208', '208 Mono vista mare'] },
  { sourceId: 'M4', title: 'Mono 3°', floor: '3°', capacity: 2, groupName: 'Monolocali', note: 'standard base', sortOrder: 16, aliases: ['Mono 3°'] },
  { sourceId: 'BD1', title: 'Dependance 1', floor: 'Dep', capacity: 5, groupName: 'Bilocali · Dependance', note: '', sortOrder: 90, aliases: ['Dependance 1'] },
  { sourceId: 'BD2', title: 'Dependance 2', floor: 'Dep', capacity: 5, groupName: 'Bilocali · Dependance', note: '', sortOrder: 91, aliases: ['Dependance 2'] },
  { sourceId: 'BPT1', title: '108 Bilo con giardino', floor: 'PT', capacity: 6, groupName: 'Bilocale · Piano Terra', note: '', sortOrder: 4, aliases: ['Bilo PT 1', 'Bilo PT 1 con giardino', '108', '108 Bilo con giardino'] },
  { sourceId: 'BPT2', title: '106 Bilo con giardino', floor: 'PT', capacity: 6, groupName: 'Bilocale · Piano Terra', note: '', sortOrder: 3, aliases: ['Bilo PT 2', 'Bilo PT 2 con giardino', '106', '106 Bilo con giardino'] },
  { sourceId: 'BPT3', title: '102 Bilo senza giardino', floor: 'PT', capacity: 6, groupName: 'Bilocale · Piano Terra', note: '', sortOrder: 1, aliases: ['Bilo PT 3', 'Bilo PT 3 senza giardino', '102', '102 Bilo senza giardino'] },
  { sourceId: 'BPT4', title: '110 Bilo senza giardino', floor: 'PT', capacity: 6, groupName: 'Bilocale · Piano Terra', note: '', sortOrder: 5, aliases: ['Bilo PT 4', 'Bilo PT 4 senza giardino', '110', '110 Bilo senza giardino'] },
  { sourceId: 'B1A', title: '214 Bilo dietro', floor: '2°', capacity: 6, groupName: 'Bilocale · Secondo Piano', note: '', sortOrder: 12, aliases: ['Bilo 1° A', '214', '214 Bilo interno', '214 Bilo dietro'] },
  { sourceId: 'B1B', title: '212 Bilo vista ingresso', floor: '2°', capacity: 6, groupName: 'Bilocale · Secondo Piano', note: '', sortOrder: 11, aliases: ['Bilo 1° B', '212', '212 Bilo ingresso', '212 Bilo vista ingresso'] },
  { sourceId: 'B1C', title: '206', floor: '2°', capacity: 6, groupName: 'Bilocale · Secondo Piano', note: '', sortOrder: 8, aliases: ['Bilo 1° C', '206', '206 Bilo vista mare', 'Bilo 206'] },
  { sourceId: 'B202', title: '202 (stella) bilo vista mare', floor: '2°', capacity: 7, groupName: 'Bilocale · Secondo Piano', note: '', sortOrder: 6, aliases: ['202', '202 ★', 'Bilo 1° 202 ★', '202 Bilo vista mare', '202 (stella) bilo vista mare'] },
  { sourceId: 'T1', title: '204 Trilo', floor: '2°', capacity: 6, groupName: 'Trilocale · Secondo Piano', note: '', sortOrder: 7, aliases: ['Trilo 1°', 'Trilo 1 piano', '204', '204 Trilo', 'Trilo 204'] },
  { sourceId: 'T3A', title: '304 Trilo vista mare', floor: '3°', capacity: 8, groupName: 'Trilocali', note: '', sortOrder: 13, aliases: ['Trilo 3° A', '304', '304 Trilo', '304 Trilo vista mare'] },
  { sourceId: 'T3B', title: '306 Trilo Grande Vista Mare', floor: '3°', capacity: 8, groupName: 'Trilocali', note: '', sortOrder: 14, aliases: ['Trilo 3° B', '306', '306 Trilo grande', '306 Trilo Grande Vista Mare'] },
  { sourceId: 'T3C_FAMILY', title: '308 Trilo', floor: '3°', capacity: 8, groupName: 'Trilocali', note: 'Family Hotel · stagione', sortOrder: 15, aliases: ['Trilo 3° C', 'Trilo 3° C Family', '308', '308 Trilo'] },
  { sourceId: 'T3D_STAFF', title: 'Trilo 3° D', floor: '3°', capacity: 8, groupName: 'Trilocali', note: 'Staff · stagione', sortOrder: 17, aliases: ['Trilo 3° D'] },
];

const KEKKO_IMPORT_NOTE_PATTERN = /import manuale da calendario kekko/i;
const KEKKO_REF_PATTERN = /Riferimento calendario Kekko:\s*([A-Z0-9_]+)\s*\//i;
const VILLA_MARGHERITA_PROPERTY_ID = '324753';
const KEKKO_UNIT_BY_SOURCE_ID = new Map(KEKKO_UNIT_CATALOG.map((item) => [item.sourceId, item]));
const KEKKO_UNIT_BY_LABEL = new Map();

for (const item of KEKKO_UNIT_CATALOG) {
  for (const label of [item.title, ...(item.aliases || [])]) {
    KEKKO_UNIT_BY_LABEL.set(normalizeKekkoKey(label), item);
  }
}

function normalizeKekkoKey(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ');
}

function getKekkoPresentationByLabel(value) {
  const key = normalizeKekkoKey(value);
  if (!key) return null;
  return KEKKO_UNIT_BY_LABEL.get(key) || null;
}

function isMontefeltroApartment(apartment) {
  const source = `${apartment?.nome_appartamento || ''} ${apartment?.struttura_nome || ''}`.toLowerCase();
  return source.includes('montefeltro');
}

function extractLeadingUnitNumber(value) {
  const match = String(value || '').trim().match(/^(\d{1,2})\b/);
  return match ? Number(match[1]) : null;
}

function isVillaMargheritaApartment(apartment) {
  if (!apartment) return false;
  if (String(apartment.beds24_property_id || '').trim() === VILLA_MARGHERITA_PROPERTY_ID) return true;
  const source = `${apartment?.nome_appartamento || ''} ${apartment?.struttura_nome || ''}`.toLowerCase();
  return source.includes('villa margherita') || source.includes('principe di piemonte');
}

function shouldDefaultExpandResidence(apartment, units = []) {
  if ((units?.length || 0) <= 1) return false;
  return isMontefeltroApartment(apartment) || isVillaMargheritaApartment(apartment);
}

function applyDefaultResidenceExpansionState() {
  for (const apartment of S.apartments) {
    const apartmentId = String(apartment.id);
    const units = S.unitsByApartment.get(apartmentId) || [];
    if (shouldDefaultExpandResidence(apartment, units)) {
      S.expandedResidences.add(apartmentId);
    }
  }
}

function resolveApartmentContext(unit, row = null, apartment = null) {
  if (apartment) return apartment;
  if (row?.apartment) return row.apartment;
  if (row?.apartment_id) return S.apartmentMap.get(String(row.apartment_id)) || null;
  if (unit?.apartment_id) return S.apartmentMap.get(String(unit.apartment_id)) || null;
  return null;
}

function isGenericUnitLabel(value) {
  const normalized = normalizeKekkoKey(value);
  return !normalized
    || normalized === 'default'
    || normalized.startsWith('default ')
    || normalized === 'room'
    || normalized === 'unit';
}

function isSegmentMarker(value) {
  return /^\(?\d+\/\d+\)?$/.test(String(value || '').trim());
}

function isPlaceholderGuestPart(value) {
  const normalized = String(value || '').trim();
  if (!normalized) return true;
  return normalized.toLowerCase() === 'kekko' || isSegmentMarker(normalized);
}

function extractKekkoApartmentIdFromNotes(notes) {
  const match = String(notes || '').match(KEKKO_REF_PATTERN);
  return match?.[1] ? String(match[1]).trim().toUpperCase() : null;
}

function collectBookingNotes(row) {
  return [
    row?.notes,
    row?.raw_payload?.notes,
    row?.raw_payload?.request?.notes,
    row?.raw_payload?.manual_request?.notes,
    row?.raw_payload?.last_manual_request?.notes,
  ].filter(Boolean);
}

export function getKekkoApartmentIdFromBooking(row) {
  if (row?.raw_payload?.source?.apartment_id) {
    return String(row.raw_payload.source.apartment_id).trim().toUpperCase();
  }
  for (const notes of collectBookingNotes(row)) {
    const match = extractKekkoApartmentIdFromNotes(notes);
    if (match) return match;
  }
  return null;
}

export function getKekkoUnitPresentation(unit, row = null, apartment = null) {
  const contextApartment = resolveApartmentContext(unit, row, apartment);
  if (!isVillaMargheritaApartment(contextApartment)) return null;

  const directMatch = getKekkoPresentationByLabel(unit?.unit_label);
  if (directMatch) return directMatch;

  const roomTypeMatch = getKekkoPresentationByLabel(unit?.room_type_label);
  if (roomTypeMatch) return roomTypeMatch;

  const assignedMatch = getKekkoPresentationByLabel(row?.raw_payload?.__assignment?.selected_unit_label);
  if (assignedMatch) return assignedMatch;

  const sourceId = getKekkoApartmentIdFromBooking(row);
  if (sourceId && KEKKO_UNIT_BY_SOURCE_ID.has(sourceId)) {
    return KEKKO_UNIT_BY_SOURCE_ID.get(sourceId);
  }

  return null;
}

function getUnitSortOrder(unit, apartment = null) {
  if (isMontefeltroApartment(apartment)) {
    const numericOrder = extractLeadingUnitNumber(unit?.unit_label);
    if (Number.isFinite(numericOrder)) return numericOrder;
  }
  return getKekkoUnitPresentation(unit, null, apartment)?.sortOrder ?? (1000 + Number(unit?.beds24_unit_index || 0));
}

function buildUnitDisplayMeta(unit, apartment) {
  const presentation = getKekkoUnitPresentation(unit, null, apartment);
  if (presentation) return `${presentation.floor} · ${presentation.capacity} pax`;
  return `${apartment.city} · ${unit?.max_guests || '—'} ospiti max`;
}

function buildUnitDisplayTitle(unit, apartment) {
  if (isMontefeltroApartment(apartment)) {
    const unitLabel = normalizeText(unit?.unit_label || '');
    if (unitLabel) return unitLabel;
  }
  const presentation = getKekkoUnitPresentation(unit, null, apartment);
  if (presentation) return presentation.title;

  const roomType = normalizeText(unit?.room_type_label || '');
  const unitLabel = normalizeText(unit?.unit_label || '');
  if (roomType && !isGenericUnitLabel(roomType)) return roomType;
  if (unitLabel && !isGenericUnitLabel(unitLabel)) return unitLabel;
  return apartment.displayName;
}

function buildUnitDisplayNote(unit, apartment) {
  if (isMontefeltroApartment(apartment)) {
    return '';
  }
  const presentation = getKekkoUnitPresentation(unit, null, apartment);
  if (!presentation) {
    const roomType = normalizeText(unit?.room_type_label || '');
    const unitLabel = normalizeText(unit?.unit_label || '');
    const title = buildUnitDisplayTitle(unit, apartment);
    if (roomType && !isGenericUnitLabel(roomType) && roomType !== title) return roomType;
    if (unitLabel && !isGenericUnitLabel(unitLabel) && unitLabel !== title) return unitLabel;
    return '';
  }
  if (isVillaMargheritaApartment(apartment)) {
    return [describeVillaMargheritaFloor(presentation.floor), presentation.note].filter(Boolean).join(' · ');
  }
  return [presentation.groupName, presentation.note].filter(Boolean).join(' · ');
}

function describeVillaMargheritaFloor(floor) {
  const value = String(floor || '').trim();
  if (value === 'PT') return 'Piano Terra';
  if (value === '2°') return 'Secondo Piano';
  if (value === '3°') return 'Terzo Piano';
  if (value === 'Dep') return 'Dependance';
  return value;
}

function buildBookingDisplayName(row) {
  const isKekko = isKekkoImportedBooking(row);
  const sourceName = normalizeText(row?.raw_payload?.source?.name || '');
  if (isKekko && sourceName) return sourceName;

  const first = normalizeText(row?.guest_first_name || '');
  const last = normalizeText(row?.guest_last_name || '');

  if (isKekko && first && isPlaceholderGuestPart(last)) return first;
  return [first, last].filter(Boolean).join(' ').trim();
}

function buildBookingDisplaySegment(row) {
  if (!isKekkoImportedBooking(row)) return null;
  const value = normalizeText(row?.guest_last_name || '');
  const match = value.match(/^\(?(\d+\/\d+)\)?$/);
  return match?.[1] || null;
}

export function isKekkoImportedBooking(row) {
  if (String(row?.raw_payload?.source?.source || '').trim().toLowerCase() === 'checco') return true;
  if (getKekkoApartmentIdFromBooking(row)) return true;
  return collectBookingNotes(row).some((notes) => KEKKO_IMPORT_NOTE_PATTERN.test(String(notes || '')));
}

export async function loadStaticData() {
  const apartmentsQuery = window.sb
    .from('apartments')
    .select('id,nome_appartamento,struttura_nome,provincia,beds24_property_id,attivo')
    .eq('attivo', true);

  const unitsQuery = window.sb
    .from('apartment_units')
    .select('id,apartment_id,unit_label,room_type_label,beds24_room_id,beds24_unit_index,max_guests,active')
    .eq('active', true)
    .order('apartment_id')
    .order('beds24_unit_index');

  const [{ data: apartments, error: apartmentsError }, { data: units, error: unitsError }] = await Promise.all([
    apartmentsQuery,
    unitsQuery,
  ]);

  if (apartmentsError) throw apartmentsError;
  if (unitsError) throw unitsError;

  const scopedApartmentIds = S.calendarAccessMode === 'global-editor'
    ? null
    : new Set([...S.calendarAccessibleApartmentIds].filter(Boolean));

  S.apartments = (apartments || [])
    .filter((row) => !scopedApartmentIds || scopedApartmentIds.has(String(row.id)))
    .filter((row) => !/test/i.test(row.nome_appartamento || ''))
    .map(enrichApartment)
    .sort(compareApartment);
  S.apartmentMap = new Map(S.apartments.map((item) => [String(item.id), item]));
  S.units = (units || []).filter((item) => S.apartmentMap.has(String(item.apartment_id)));
  S.unitMap = new Map(S.units.map((item) => [String(item.id), item]));
  S.unitsByApartment = groupBy(S.units, (item) => String(item.apartment_id), (a, b) => {
    const apartment = S.apartmentMap.get(String(a.apartment_id)) || null;
    const sortCompare = getUnitSortOrder(a, apartment) - getUnitSortOrder(b, apartment);
    if (sortCompare !== 0) return sortCompare;
    const indexCompare = Number(a.beds24_unit_index || 0) - Number(b.beds24_unit_index || 0);
    return indexCompare !== 0 ? indexCompare : String(a.unit_label || '').localeCompare(String(b.unit_label || ''), 'it');
  });
  applyDefaultResidenceExpansionState();

  S.staticLoaded = true;
}

export async function loadMonthData({ freshInventory = false } = {}) {
  const { start, end } = getMonthBounds(S.monthDate);
  const inventoryParams = new URLSearchParams({
    dateFrom: start,
    dateTo: end,
  });
  if (freshInventory) inventoryParams.set('fresh', '1');
  const inventoryUrl = `/.netlify/functions/get-calendar-calendario?${inventoryParams.toString()}`;

  const bookingsQuery = window.sb
    .from('bookings')
    .select('id,beds24_booking_id,guest_first_name,guest_last_name,guest_email,guest_phone,num_adults,num_children,channel,channel_normalized,status,check_in,check_out,total_price,currency,source_updated_at,source_created_at,synced_at,apartment_id,apartment_unit_id,raw_payload')
    .lte('check_in', end)
    .gt('check_out', start)
    .in('status', OCCUPYING_STATUSES)
    .not('apartment_unit_id', 'is', null)
    .order('check_in')
    .order('beds24_booking_id');

  const orphanQuery = window.sb
    .from('bookings')
    .select('beds24_booking_id,beds24_property_id,beds24_room_id,check_in,check_out,channel,channel_normalized,status,raw_payload', { count: 'exact' })
    .is('apartment_unit_id', null)
    .in('status', OCCUPYING_STATUSES)
    .order('check_in')
    .order('beds24_booking_id');

  const lastSyncQuery = window.sb
    .from('bookings')
    .select('synced_at')
    .order('synced_at', { ascending: false })
    .limit(1);

  const [
    { data: bookings, error: bookingsError },
    { data: orphanRows, error: orphanError, count: orphanCount },
    { data: lastSyncRows, error: syncError },
    { data: calendarDays, error: calendarDaysError },
    inventoryPayload,
  ] = await Promise.all([
    bookingsQuery,
    orphanQuery,
    lastSyncQuery,
    fetchAllCalendarDays(start, end),
    fetch(inventoryUrl, {
      cache: freshInventory ? 'no-store' : 'default',
      headers: S.session?.access_token
        ? { Authorization: `Bearer ${S.session.access_token}` }
        : {},
    })
      .then(async (response) => {
        const payload = await response.json().catch(() => ({}));
        if (!response.ok) {
          console.warn('calendario inventory unavailable', payload.error || response.statusText || response.status);
          return { inventoryDays: [] };
        }
        return payload;
      })
      .catch((error) => {
        console.warn('calendario inventory fetch failed', error);
        return { inventoryDays: [] };
      }),
  ]);

  if (bookingsError) throw bookingsError;
  if (orphanError) throw orphanError;
  if (syncError) throw syncError;
  if (calendarDaysError) {
    console.warn('calendar_days query failed', calendarDaysError);
  }

  S.inventoryDays = (inventoryPayload?.inventoryDays || []).slice();
  S.inventoryByRoomDate = buildInventoryIndex(S.inventoryDays);
  S.inventoryWarnings = unique((inventoryPayload?.warnings || []).map((value) => String(value || '').trim()).filter(Boolean));
  S.calendarDays = (calendarDays || []).slice();
  S.calendarDayByUnitDate = new Map(
    (calendarDays || []).map((row) => [`${row.apartment_unit_id}:${row.date}`, row])
  );
  S.bookings = (bookings || []).map(enrichBooking).sort(compareBookingsForRender);
  S.bookingMap = new Map(S.bookings.map((item) => [String(item.beds24_booking_id), item]));
  S.bookingsByApartment = groupBy(S.bookings, (item) => String(item.apartment_id));
  S.bookingsByUnit = groupBy(S.bookings, (item) => String(item.apartment_unit_id));
  S.orphanRows = (orphanRows || []).map(enrichOrphan).sort(compareOrphans);
  S.orphanCount = Number(orphanCount || 0);
  S.lastSync = lastSyncRows?.[0]?.synced_at || null;
}

async function fetchAllCalendarDays(start, end) {
  const pageSize = 1000;
  const rows = [];

  for (let from = 0; ; from += pageSize) {
    const { data, error } = await window.sb
      .from('calendar_days')
      .select('apartment_unit_id, date, price, min_stay, available, closed')
      .gte('date', start)
      .lte('date', end)
      .order('date', { ascending: true })
      .order('apartment_unit_id', { ascending: true })
      .range(from, from + pageSize - 1);

    if (error) {
      return { data: rows, error };
    }

    if (Array.isArray(data) && data.length) {
      rows.push(...data);
    }

    if (!Array.isArray(data) || data.length < pageSize) {
      return { data: rows, error: null };
    }
  }
}

export function enrichApartment(row) {
  const derivedCity = deriveCity(row);
  return {
    ...row,
    city: derivedCity,
    displayName: row.nome_appartamento || 'Struttura senza nome',
  };
}

export function deriveCity(row) {
  const source = [row.nome_appartamento, row.struttura_nome, row.provincia].filter(Boolean).join(' · ');
  for (const [pattern, city] of KNOWN_CITY_MAP) {
    if (pattern.test(source)) return normalizeCityName(city);
  }
  const trimmed = String(row.struttura_nome || row.provincia || row.nome_appartamento || '').trim();
  if (!trimmed) return 'Altro';
  const firstToken = trimmed.split(/[-,]/)[0].trim();
  return normalizeCityName(titleCase(firstToken || 'Altro'));
}

export function enrichBooking(row) {
  const guestCount = Number(row.num_adults || 0) + Number(row.num_children || 0);
  const notes = collectBookingNotes(row)[0] || '';
  const unit = S.unitMap.get(String(row.apartment_unit_id)) || null;
  return {
    ...row,
    apartment: S.apartmentMap.get(String(row.apartment_id)) || null,
    unit,
    guestCount,
    notes: normalizeText(notes),
    unitPresentation: getKekkoUnitPresentation(unit, row),
    kekkoApartmentId: getKekkoApartmentIdFromBooking(row),
    isKekkoImported: isKekkoImportedBooking(row),
    displayGuestName: buildBookingDisplayName(row),
    displaySegment: buildBookingDisplaySegment(row),
  };
}

export function enrichOrphan(row) {
  return {
    ...row,
    reason: row.raw_payload?.__assignment?.reason || '—',
  };
}

export function buildInventoryIndex(rows) {
  const map = new Map();
  for (const row of rows || []) {
    if (!row?.date) continue;
    const roomId = String(row.roomId || '').trim();
    const propertyId = String(row.propertyId || '').trim();
    if (roomId) {
      map.set(`${roomId}:${row.date}`, row);
    }
    if (propertyId) {
      map.set(`${propertyId}:${row.date}`, row);
    }
  }
  return map;
}

export function initializeFilterDefaults() {
  const cityOptions = getAvailableCities();
  if (!S.filters.cities.size) {
    S.filters.cities = new Set(cityOptions);
  } else {
    S.filters.cities = new Set([...S.filters.cities].filter((city) => cityOptions.includes(city)));
    if (!S.filters.cities.size) S.filters.cities = new Set(cityOptions);
  }

  const channels = getAvailableChannels();
  if (!S.filters.channels.size) {
    S.filters.channels = new Set(channels);
  } else {
    S.filters.channels = new Set([...S.filters.channels].filter((channel) => channels.includes(channel)));
    if (!S.filters.channels.size) S.filters.channels = new Set(channels);
  }

  const statuses = getAvailableStatuses();
  S.filters.statuses = new Set([...S.filters.statuses].filter((status) => statuses.includes(status)));
  if (!S.filters.statuses.size) S.filters.statuses = new Set(statuses);
}

export function getAvailableCities() {
  const cities = new Set();
  const skipped = [];
  for (const apartment of S.apartments) {
    const units = S.unitsByApartment.get(String(apartment.id)) || [];
    if (!units.length) {
      skipped.push(apartment.nome_appartamento);
      continue;
    }
    cities.add(apartment.city);
  }
  if (skipped.length && !S._skipWarningLogged) {
    console.warn(
      `Calendario: ${skipped.length} apartment senza unit (saltati):`,
      skipped
    );
    S._skipWarningLogged = true;
  }
  return [...cities].sort(compareCity);
}

export function getAvailableChannels() {
  return unique(S.bookings.map((booking) => booking.channel_normalized || booking.channel || 'fallback'))
    .sort((a, b) => channelSortLabel(a).localeCompare(channelSortLabel(b), 'it'));
}

export function getAvailableStatuses() {
  return unique(S.bookings.map((booking) => booking.status))
    .sort((a, b) => (STATUS_LABELS[a] || a).localeCompare(STATUS_LABELS[b] || b, 'it'));
}

export function countVisibleApartmentsByCity(city) {
  return S.apartments.filter((apartment) => apartment.city === city && (S.unitsByApartment.get(String(apartment.id)) || []).length > 0).length;
}

export function countBookingsByChannel(channel) {
  return S.bookings.filter((booking) => (booking.channel_normalized || booking.channel || 'fallback') === channel).length;
}

export function countBookingsByStatus(status) {
  return S.bookings.filter((booking) => booking.status === status).length;
}

export function getVisibleApartmentRows() {
  const rows = [];
  for (const apartment of S.apartments) {
    if (!S.filters.cities.has(apartment.city)) continue;
    const units = S.unitsByApartment.get(String(apartment.id)) || [];
    if (!units.length) continue;
    const visibleUnitRows = units.map((unit) => {
      const bookings = getBookingsForUnit(unit.id).filter(matchesFilters);
      return {
        kind: 'unit',
        apartment,
        unit,
        units,
        bookings,
        maxGuests: unit.max_guests || null,
        displayTitle: buildUnitDisplayTitle(unit, apartment),
        displayMeta: buildUnitDisplayMeta(unit, apartment),
        displayNote: buildUnitDisplayNote(unit, apartment),
        displayOrder: getUnitSortOrder(unit, apartment),
      };
    });

    const isResidence = units.length > 1;
    if (!isResidence) {
      rows.push({
        kind: 'single',
        apartment,
        unit: units[0],
        units,
        bookings: visibleUnitRows[0]?.bookings || [],
        maxGuests: units[0]?.max_guests || null,
        displayTitle: visibleUnitRows[0]?.displayTitle || units[0]?.unit_label || apartment.displayName,
        displayMeta: visibleUnitRows[0]?.displayMeta || `${apartment.city} · ${units[0]?.max_guests || '—'} ospiti max`,
        displayNote: visibleUnitRows[0]?.displayNote || '',
        displayOrder: visibleUnitRows[0]?.displayOrder || 0,
      });
      continue;
    }

    rows.push({
      kind: 'residence',
      apartment,
      units,
      bookings: [],
      maxGuests: sum(units.map((unit) => Number(unit.max_guests || 0))) || null,
      displayOrder: 0,
    });
    if (S.expandedResidences.has(String(apartment.id))) {
      rows.push(...visibleUnitRows);
    }
  }
  return rows;
}

export function groupVisibleRowsByCity() {
  const rows = getVisibleApartmentRows();
  const groups = new Map();
  for (const row of rows) {
    if (!groups.has(row.apartment.city)) groups.set(row.apartment.city, []);
    groups.get(row.apartment.city).push(row);
  }
  return [...groups.entries()].sort((a, b) => compareCity(a[0], b[0])).map(([city, cityRows]) => ({
    city,
    rows: cityRows.sort((left, right) => {
      const nameCompare = left.apartment.displayName.localeCompare(right.apartment.displayName, 'it');
      if (nameCompare !== 0) return nameCompare;
      const orderCompare = Number(left.displayOrder || 0) - Number(right.displayOrder || 0);
      if (orderCompare !== 0) return orderCompare;
      return String(left.displayTitle || left.unit?.unit_label || '').localeCompare(String(right.displayTitle || right.unit?.unit_label || ''), 'it');
    }),
  }));
}

export function groupVisibleBookingsForMobile() {
  const map = new Map();
  for (const booking of getVisibleBookings()) {
    const apartmentId = String(booking.apartment_id);
    if (!map.has(apartmentId)) {
      map.set(apartmentId, {
        apartment: booking.apartment,
        bookings: [],
        meta: booking.apartment?.city || '',
      });
    }
    map.get(apartmentId).bookings.push(booking);
  }
  return [...map.values()].sort((a, b) => a.apartment.displayName.localeCompare(b.apartment.displayName, 'it'));
}

export function getVisibleBookings() {
  return S.bookings.filter(matchesFilters).sort(compareBookingsForRender);
}

export function countVisibleBookingsForApartment(apartmentId) {
  const key = String(apartmentId || '');
  return S.bookings.filter((booking) => (
    String(booking.apartment_id) === key
    && matchesFilters(booking)
  )).length;
}

export function matchesFilters(booking) {
  const apartment = booking.apartment;
  if (!apartment) return false;
  if (!S.filters.cities.has(apartment.city)) return false;
  const channelKey = booking.channel_normalized || booking.channel || 'fallback';
  if (!S.filters.channels.has(channelKey)) return false;
  if (!S.filters.statuses.has(booking.status)) return false;
  return true;
}

export function countOccupiedUnitsForDate(units, date) {
  const iso = isoDateLocal(date);
  let occupied = 0;
  for (const unit of units) {
    const bookings = getBookingsForUnit(unit.id).filter(matchesFilters);
    const hasBooking = bookings.some((booking) => booking.check_in <= iso && booking.check_out > iso);
    if (hasBooking) occupied += 1;
  }
  return occupied;
}

export function countUnavailableUnitsForDate(units, date) {
  let unavailable = 0;
  for (const unit of units) {
    const state = getInventoryStateForUnit(unit, date);
    if (state.closed) unavailable += 1;
  }
  return unavailable;
}

export function getBookingsForUnit(unitId) {
  return (S.bookingsByUnit.get(String(unitId)) || []).slice().sort(compareBookingsForRender);
}

export function getInventoryStateForUnit(unit, date) {
  const roomId = String(unit?.beds24_room_id || '').trim();
  const iso = typeof date === 'string' ? date : isoDateLocal(date);
  const cachedDay = S.calendarDayByUnitDate.get(`${unit?.id || ''}:${iso}`) || null;
  const cachedClosed = isCachedDayClosed(cachedDay);
  const cachedAvailable = resolveCachedDayAvailable(cachedDay);
  if (!roomId) {
    return {
      hasInventory: false,
      closed: cachedClosed,
      available: cachedAvailable,
      hasBooking: false,
      price: cachedDay?.price ?? null,
      minStay: cachedDay?.min_stay ?? null,
    };
  }
  const row = S.inventoryByRoomDate.get(`${roomId}:${iso}`) || null;
  if (!row) {
    return {
      hasInventory: false,
      closed: cachedClosed,
      available: cachedAvailable,
      hasBooking: false,
      price: cachedDay?.price ?? null,
      minStay: cachedDay?.min_stay ?? null,
    };
  }
  const inventoryAvailabilityKnown = hasInventoryAvailability(row);
  const inventoryClosed = row.closed === true || row.available === false;
  const inventoryAvailable = resolveInventoryAvailable(row);
  return {
    hasInventory: true,
    closed: inventoryAvailabilityKnown ? inventoryClosed : cachedClosed,
    available: inventoryAvailabilityKnown ? inventoryAvailable : cachedAvailable,
    hasBooking: Boolean(row.hasBooking),
    price: cachedDay?.price ?? row.price ?? null,
    minStay: cachedDay?.min_stay ?? row.minStay ?? null,
  };
}

export function getDayAvailabilityState(row, date) {
  const stateFromInventory = getInventoryStateForUnit(row.unit, date);
  const iso = isoDateLocal(date);
  const hasBooking = row.bookings.some((booking) => booking.check_in <= iso && booking.check_out > iso);
  const cachedDay = S.calendarDayByUnitDate.get(`${row.unit?.id || ''}:${iso}`);
  const cachedClosed = isCachedDayClosed(cachedDay);
  return {
    ...stateFromInventory,
    hasBooking: stateFromInventory.hasBooking || hasBooking,
    supabaseHasBooking: hasBooking,
    price: stateFromInventory.price ?? cachedDay?.price ?? null,
    minStay: stateFromInventory.minStay ?? cachedDay?.min_stay ?? null,
    closed: stateFromInventory.closed || (!stateFromInventory.hasInventory && cachedClosed),
  };
}

function isCachedDayClosed(cachedDay) {
  return cachedDay?.closed === true || cachedDay?.available === false;
}

function hasCachedDayAvailability(cachedDay) {
  return cachedDay?.closed === true
    || cachedDay?.closed === false
    || cachedDay?.available === true
    || cachedDay?.available === false;
}

function hasInventoryAvailability(row) {
  return row?.closed === true
    || row?.closed === false
    || row?.available === true
    || row?.available === false;
}

function resolveCachedDayAvailable(cachedDay) {
  if (cachedDay?.available === true || cachedDay?.available === false) {
    return cachedDay.available;
  }
  if (cachedDay?.closed === true) return false;
  if (cachedDay?.closed === false) return true;
  return null;
}

function resolveInventoryAvailable(row) {
  if (row?.available === true || row?.available === false) {
    return row.available;
  }
  if (row?.closed === true) return false;
  if (row?.closed === false) return true;
  return null;
}

export function buildDayCellLabel(row, date, state) {
  const bits = [
    row.displayTitle || row.unit?.unit_label || row.apartment.displayName,
    formatDate(isoDateLocal(date)),
  ];
  if (state.closed) bits.push('non vendibile');
  else bits.push('disponibile');
  if (state.hasBooking) bits.push('con prenotazione');
  if (state.minStay) bits.push(`min stay ${state.minStay}`);
  if (state.price != null) bits.push(`${formatPrice(state.price)} EUR`);
  return bits.join(' · ');
}

export function compareApartment(a, b) {
  const cityCompare = compareCity(a.city, b.city);
  if (cityCompare !== 0) return cityCompare;
  return a.displayName.localeCompare(b.displayName, 'it');
}

export function compareCity(a, b) {
  const aIndex = CITY_PRIORITY.indexOf(normalizeCityName(a));
  const bIndex = CITY_PRIORITY.indexOf(normalizeCityName(b));
  if (aIndex >= 0 && bIndex >= 0) return aIndex - bIndex;
  if (aIndex >= 0) return -1;
  if (bIndex >= 0) return 1;
  return String(a).localeCompare(String(b), 'it');
}

function normalizeCityName(value) {
  const city = String(value || '').trim();
  if (/^valle\s+d[' ]aosta$/i.test(city) || /^val\s+d[' ]aosta$/i.test(city)) {
    return "Val d'Aosta";
  }
  return city;
}

function channelSortLabel(value) {
  if (!value) return 'Altro';
  return CHANNEL_CONFIG[value]?.label || titleCase(String(value).replace(/_/g, ' '));
}
