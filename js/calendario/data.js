import {
  CHANNEL_CONFIG,
  CITY_PRIORITY,
  KNOWN_CITY_MAP,
  OCCUPYING_STATUSES,
  S,
  STATUS_LABELS,
} from './state.js';
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
} from './utils.js';

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
    const indexCompare = Number(a.beds24_unit_index || 0) - Number(b.beds24_unit_index || 0);
    return indexCompare !== 0 ? indexCompare : String(a.unit_label || '').localeCompare(String(b.unit_label || ''), 'it');
  });

  S.staticLoaded = true;
}

export async function loadMonthData() {
  const { start, end } = getMonthBounds(S.monthDate);
  const inventoryUrl = `/.netlify/functions/get-calendar?dateFrom=${encodeURIComponent(start)}&dateTo=${encodeURIComponent(end)}`;

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

  const calendarDaysQuery = window.sb
    .from('calendar_days')
    .select('apartment_unit_id, date, price, min_stay, available, closed')
    .gte('date', start)
    .lte('date', end);

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
    calendarDaysQuery,
    fetch(inventoryUrl)
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
  const notes = row.raw_payload?.notes || row.raw_payload?.['notes'] || row.raw_payload?.__assignment?.notes || '';
  return {
    ...row,
    apartment: S.apartmentMap.get(String(row.apartment_id)) || null,
    unit: S.unitMap.get(String(row.apartment_unit_id)) || null,
    guestCount,
    notes: normalizeText(notes),
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
      });
      continue;
    }

    rows.push({
      kind: 'residence',
      apartment,
      units,
      bookings: [],
      maxGuests: sum(units.map((unit) => Number(unit.max_guests || 0))) || null,
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
      if (left.unit && right.unit) {
        return String(left.unit.unit_label || '').localeCompare(String(right.unit.unit_label || ''), 'it');
      }
      return 0;
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
  if (!roomId) {
    return {
      closed: cachedDay?.closed === true,
      available: cachedDay?.available ?? null,
      hasBooking: false,
      price: cachedDay?.price ?? null,
      minStay: cachedDay?.min_stay ?? null,
    };
  }
  const row = S.inventoryByRoomDate.get(`${roomId}:${iso}`) || null;
  if (!row) {
    return {
      closed: cachedDay?.closed === true,
      available: cachedDay?.available ?? null,
      hasBooking: false,
      price: cachedDay?.price ?? null,
      minStay: cachedDay?.min_stay ?? null,
    };
  }
  return {
    closed: row.closed === true || row.available === false || cachedDay?.closed === true,
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
    closed: stateFromInventory.closed || (cachedDay?.closed === true),
  };
}

export function buildDayCellLabel(row, date, state) {
  const bits = [
    row.apartment.displayName,
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
