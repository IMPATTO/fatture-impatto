import {
  CHANNEL_CONFIG,
  CITY_PRIORITY,
  KNOWN_CITY_MAP,
  OCCUPYING_STATUSES,
  S,
  STATUS_LABELS,
} from './state.js?v=20260521e';
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
} from './utils.js?v=20260520f';

const KEKKO_UNIT_CATALOG = [
  { sourceId: 'M1', title: '104', floor: 'PT', capacity: 3, groupName: 'Monolocali', note: 'il migliore', sortOrder: 1, aliases: ['104'] },
  { sourceId: 'M2', title: 'Mono 1° A', floor: '1°', capacity: 2, groupName: 'Monolocali', note: '', sortOrder: 2, aliases: ['Mono 1° A'] },
  { sourceId: 'M3', title: 'Mono 1° B', floor: '1°', capacity: 2, groupName: 'Monolocali', note: '', sortOrder: 3, aliases: ['Mono 1° B'] },
  { sourceId: 'M4', title: 'Mono 3°', floor: '3°', capacity: 2, groupName: 'Monolocali', note: 'standard base', sortOrder: 4, aliases: ['Mono 3°'] },
  { sourceId: 'BD1', title: 'Dependance 1', floor: 'Dep', capacity: 5, groupName: 'Bilocali · Dependance', note: '', sortOrder: 10, aliases: ['Dependance 1'] },
  { sourceId: 'BD2', title: 'Dependance 2', floor: 'Dep', capacity: 5, groupName: 'Bilocali · Dependance', note: '', sortOrder: 11, aliases: ['Dependance 2'] },
  { sourceId: 'BPT1', title: 'Bilo PT 1', floor: 'PT', capacity: 6, groupName: 'Bilocali · Piano Terra', note: '', sortOrder: 20, aliases: ['Bilo PT 1'] },
  { sourceId: 'BPT2', title: 'Bilo PT 2', floor: 'PT', capacity: 6, groupName: 'Bilocali · Piano Terra', note: '', sortOrder: 21, aliases: ['Bilo PT 2'] },
  { sourceId: 'BPT3', title: 'Bilo PT 3', floor: 'PT', capacity: 6, groupName: 'Bilocali · Piano Terra', note: 'app.5', sortOrder: 22, aliases: ['Bilo PT 3'] },
  { sourceId: 'BPT4', title: 'Bilo PT 4', floor: 'PT', capacity: 6, groupName: 'Bilocali · Piano Terra', note: '', sortOrder: 23, aliases: ['Bilo PT 4'] },
  { sourceId: 'B1A', title: 'Bilo 1° A', floor: '1°', capacity: 6, groupName: 'Bilocali · Primo Piano', note: '', sortOrder: 30, aliases: ['Bilo 1° A'] },
  { sourceId: 'B1B', title: 'Bilo 1° B', floor: '1°', capacity: 6, groupName: 'Bilocali · Primo Piano', note: '', sortOrder: 31, aliases: ['Bilo 1° B'] },
  { sourceId: 'B1C', title: 'Bilo 1° C', floor: '1°', capacity: 6, groupName: 'Bilocali · Primo Piano', note: '', sortOrder: 32, aliases: ['Bilo 1° C'] },
  { sourceId: 'B202', title: '202 ★', floor: '1°', capacity: 7, groupName: 'Bilocali · Primo Piano', note: 'vista mare', sortOrder: 33, aliases: ['202'] },
  { sourceId: 'T1', title: 'Trilo 1° ★', floor: '1°', capacity: 6, groupName: 'Trilocali', note: 'vista mare', sortOrder: 40, aliases: ['Trilo 1°'] },
  { sourceId: 'T3A', title: 'Trilo 3° A', floor: '3°', capacity: 8, groupName: 'Trilocali', note: '', sortOrder: 41, aliases: ['Trilo 3° A'] },
  { sourceId: 'T3B', title: 'Trilo 3° B', floor: '3°', capacity: 8, groupName: 'Trilocali', note: '', sortOrder: 42, aliases: ['Trilo 3° B'] },
  { sourceId: 'T3C_FAMILY', title: 'Trilo 3° C', floor: '3°', capacity: 8, groupName: 'Trilocali', note: 'Family Hotel · stagione', sortOrder: 43, aliases: ['Trilo 3° C', 'Trilo 3° C Family'] },
  { sourceId: 'T3D_STAFF', title: 'Trilo 3° D', floor: '3°', capacity: 8, groupName: 'Trilocali', note: 'Staff · stagione', sortOrder: 44, aliases: ['Trilo 3° D'] },
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

  const directKey = normalizeKekkoKey(unit?.unit_label);
  if (directKey && KEKKO_UNIT_BY_LABEL.has(directKey)) {
    return KEKKO_UNIT_BY_LABEL.get(directKey);
  }

  const assignedKey = normalizeKekkoKey(row?.raw_payload?.__assignment?.selected_unit_label);
  if (assignedKey && KEKKO_UNIT_BY_LABEL.has(assignedKey)) {
    return KEKKO_UNIT_BY_LABEL.get(assignedKey);
  }

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
  return [presentation.groupName, presentation.note].filter(Boolean).join(' · ');
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

  S.apartments = (apartments || [])
    .filter((row) => !/test/i.test(row.nome_appartamento || ''))
    .map(enrichApartment)
    .sort(compareApartment);
  S.apartmentMap = new Map(S.apartments.map((item) => [String(item.id), item]));
  S.units = (units || []).slice();
  S.unitMap = new Map(S.units.map((item) => [String(item.id), item]));
  S.unitsByApartment = groupBy(S.units, (item) => String(item.apartment_id), (a, b) => {
    const apartment = S.apartmentMap.get(String(a.apartment_id)) || null;
    const sortCompare = getUnitSortOrder(a, apartment) - getUnitSortOrder(b, apartment);
    if (sortCompare !== 0) return sortCompare;
    const indexCompare = Number(a.beds24_unit_index || 0) - Number(b.beds24_unit_index || 0);
    return indexCompare !== 0 ? indexCompare : String(a.unit_label || '').localeCompare(String(b.unit_label || ''), 'it');
  });

  S.staticLoaded = true;
}

export async function loadMonthData() {
  const { start, end } = getMonthBounds(S.monthDate);
  const inventoryUrl = `/.netlify/functions/get-calendar-calendario?dateFrom=${encodeURIComponent(start)}&dateTo=${encodeURIComponent(end)}`;

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
    if (pattern.test(source)) return city;
  }
  const trimmed = String(row.struttura_nome || row.provincia || row.nome_appartamento || '').trim();
  if (!trimmed) return 'Altro';
  const firstToken = trimmed.split(/[-,]/)[0].trim();
  return titleCase(firstToken || 'Altro');
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
    if (!row?.propertyId || !row?.date) continue;
    map.set(`${row.propertyId}:${row.date}`, row);
  }
  return map;
}

export function initializeFilterDefaults() {
  const cityOptions = getAvailableCities();
  if (!S.filters.cities.size) {
    S.filters.cities = new Set(cityOptions.slice(0, 8));
  } else {
    S.filters.cities = new Set([...S.filters.cities].filter((city) => cityOptions.includes(city)));
    if (!S.filters.cities.size) S.filters.cities = new Set(cityOptions.slice(0, 8));
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
  if (!roomId) {
    return {
      closed: cachedClosed,
      available: cachedDay?.available ?? null,
      hasBooking: false,
      price: cachedDay?.price ?? null,
      minStay: cachedDay?.min_stay ?? null,
    };
  }
  const row = S.inventoryByRoomDate.get(`${roomId}:${iso}`) || null;
  if (!row) {
    return {
      closed: cachedClosed,
      available: cachedDay?.available ?? null,
      hasBooking: false,
      price: cachedDay?.price ?? null,
      minStay: cachedDay?.min_stay ?? null,
    };
  }
  return {
    closed: row.closed === true || row.available === false || cachedClosed,
    available: row.available ?? cachedDay?.available ?? null,
    hasBooking: Boolean(row.hasBooking),
    price: row.price ?? cachedDay?.price ?? null,
    minStay: row.minStay ?? cachedDay?.min_stay ?? null,
  };
}

export function getDayAvailabilityState(row, date) {
  const stateFromInventory = getInventoryStateForUnit(row.unit, date);
  const iso = isoDateLocal(date);
  const hasBooking = row.bookings.some((booking) => booking.check_in <= iso && booking.check_out > iso);
  const cachedDay = S.calendarDayByUnitDate.get(`${row.unit?.id || ''}:${iso}`);
  return {
    ...stateFromInventory,
    hasBooking: stateFromInventory.hasBooking || hasBooking,
    price: stateFromInventory.price ?? cachedDay?.price ?? null,
    minStay: stateFromInventory.minStay ?? cachedDay?.min_stay ?? null,
    closed: stateFromInventory.closed || isCachedDayClosed(cachedDay),
  };
}

function isCachedDayClosed(cachedDay) {
  return cachedDay?.closed === true || cachedDay?.available === false;
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
  const aIndex = CITY_PRIORITY.indexOf(a);
  const bIndex = CITY_PRIORITY.indexOf(b);
  if (aIndex >= 0 && bIndex >= 0) return aIndex - bIndex;
  if (aIndex >= 0) return -1;
  if (bIndex >= 0) return 1;
  return String(a).localeCompare(String(b), 'it');
}

function channelSortLabel(value) {
  if (!value) return 'Altro';
  return CHANNEL_CONFIG[value]?.label || titleCase(String(value).replace(/_/g, ' '));
}
