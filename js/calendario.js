/**
 * Calendario PMS centrale read-only.
 * Componenti principali:
 * - auth/sessione via window.sb
 * - fetch dati da apartments, apartment_units, bookings e inventoryDays
 * - timeline mensile con heatmap residence collassati e barre booking per unita
 * - pannello laterale dettaglio booking, modale orphan, export CSV e refresh Beds24
 * Query:
 * - apartments attivi + apartment_units attive una volta per sessione
 * - bookings visibili, orphan, ultimo sync e inventoryDays a ogni cambio mese/refresh
 * Eventi:
 * - cambio mese, filtri, espansione residence, click booking, export CSV, refresh sync
 * Nota:
 * - il DB reale non espone una colonna `citta` su apartments, quindi la pagina deriva
 *   il gruppo citta da nome_appartamento / struttura_nome / provincia senza toccare lo schema.
 */

const sb = window.sb;

const OCCUPYING_STATUSES = ['confirmed', 'new', 'request', 'black'];
const STATUS_LABELS = {
  confirmed: 'Confermata',
  new: 'Nuova',
  request: 'Richiesta',
  black: 'Blocco',
  inquiry: 'Richiesta info',
  cancelled: 'Cancellata',
};
const CITY_PRIORITY = [
  'Cattolica',
  'Rimini',
  'Riccione',
  'Misano Adriatico',
  'San Giovanni in Marignano',
  'Pesaro',
  'Gabicce Mare',
  'Gradara',
  'Montelabbate',
  'Fanano',
  'Valtournenche',
  'Verona',
];
const KNOWN_CITY_MAP = [
  [/cattolica/i, 'Cattolica'],
  [/rimini|viserbella|viserba|rivazzurra|miramare/i, 'Rimini'],
  [/riccione/i, 'Riccione'],
  [/misano/i, 'Misano Adriatico'],
  [/san giovanni in marignano/i, 'San Giovanni in Marignano'],
  [/pesaro/i, 'Pesaro'],
  [/gabicce/i, 'Gabicce Mare'],
  [/gradara/i, 'Gradara'],
  [/montelabbate|montellabbate/i, 'Montelabbate'],
  [/fanano/i, 'Fanano'],
  [/valtournenche/i, 'Valtournenche'],
  [/verona/i, 'Verona'],
];
const CHANNEL_CONFIG = {
  booking: { className: 'channel-booking', label: 'Booking.com' },
  airbnb: { className: 'channel-airbnb', label: 'Airbnb' },
  expedia: { className: 'channel-expedia', label: 'Expedia' },
  direct: { className: 'channel-direct', label: 'Diretto' },
  api: { className: 'channel-api', label: 'API' },
  block_or_internal: { className: 'channel-block_or_internal', label: 'Blocco / interno' },
  il_lupo_affitta: { className: 'channel-il_lupo_affitta', label: 'Il Lupo Affitta' },
  jessica_caldari: { className: 'channel-jessica_caldari', label: 'Jessica Caldari' },
  serena_cerulli: { className: 'channel-serena_cerulli', label: 'Serena Cerulli' },
  app: { className: 'channel-app', label: 'App' },
};

const S = {
  session: null,
  authReady: false,
  staticLoaded: false,
  loading: false,
  syncing: false,
  monthDate: startOfMonth(new Date()),
  apartments: [],
  apartmentMap: new Map(),
  units: [],
  unitMap: new Map(),
  unitsByApartment: new Map(),
  bookings: [],
  bookingMap: new Map(),
  bookingsByApartment: new Map(),
  bookingsByUnit: new Map(),
  inventoryDays: [],
  inventoryByRoomDate: new Map(),
  orphanRows: [],
  orphanCount: 0,
  lastSync: null,
  _skipWarningLogged: false,
  expandedResidences: new Set(),
  selectedBookingId: null,
  errorMessage: '',
  filters: {
    cities: new Set(),
    channels: new Set(),
    statuses: new Set(OCCUPYING_STATUSES),
  },
};

const ELS = {};

document.addEventListener('DOMContentLoaded', init);

async function init() {
  cacheElements();
  bindShellEvents();
  await initializeSession();
}

function cacheElements() {
  [
    'loginOverlay', 'loginForm', 'loginEmail', 'loginPassword', 'loginError', 'loginSubmit',
    'app', 'navUser', 'logoutBtn', 'navbarToggle', 'navbarMenu', 'rangeInfo',
    'prevMonthBtn', 'nextMonthBtn', 'monthPickerBtn', 'monthPickerInput',
    'refreshBtn', 'exportBtn', 'lastSyncLabel', 'sidebarToggle', 'pageBody', 'sidebar', 'cityFilters', 'channelFilters',
    'statusFilters', 'timelineHeader', 'timelineBody', 'timelineShell', 'timelineScroll',
    'mobileList', 'loadingState', 'emptyState', 'errorState', 'orphanBanner',
    'bookingDrawer', 'drawerBackdrop', 'drawerCloseBtn', 'drawerCloseBtnFooter',
    'drawerTitle', 'drawerBody', 'orphanModal', 'orphanModalBody', 'orphanModalTitle',
    'orphanModalClose', 'orphanModalCloseFooter', 'tooltip',
  ].forEach((id) => {
    ELS[id] = document.getElementById(id);
  });
}

function bindShellEvents() {
  ELS.loginForm?.addEventListener('submit', handleLogin);
  ELS.logoutBtn?.addEventListener('click', () => sb.auth.signOut());
  ELS.navbarToggle?.addEventListener('click', toggleMenu);
  ELS.sidebarToggle?.addEventListener('click', toggleSidebar);
  ELS.prevMonthBtn?.addEventListener('click', () => shiftMonth(-1));
  ELS.nextMonthBtn?.addEventListener('click', () => shiftMonth(1));
  ELS.monthPickerBtn?.addEventListener('click', () => ELS.monthPickerInput?.showPicker ? ELS.monthPickerInput.showPicker() : ELS.monthPickerInput.click());
  ELS.monthPickerInput?.addEventListener('change', handleMonthPicker);
  ELS.refreshBtn?.addEventListener('click', refreshFromBeds24);
  ELS.exportBtn?.addEventListener('click', exportVisibleCsv);
  ELS.orphanBanner?.addEventListener('click', openOrphanModal);
  ELS.orphanBanner?.addEventListener('keydown', (event) => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      openOrphanModal();
    }
  });
  ELS.drawerBackdrop?.addEventListener('click', closeDrawer);
  ELS.drawerCloseBtn?.addEventListener('click', closeDrawer);
  ELS.drawerCloseBtnFooter?.addEventListener('click', closeDrawer);
  ELS.orphanModalClose?.addEventListener('click', closeOrphanModal);
  ELS.orphanModalCloseFooter?.addEventListener('click', closeOrphanModal);
  ELS.orphanModal?.addEventListener('click', (event) => {
    if (event.target === ELS.orphanModal) closeOrphanModal();
  });
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') {
      closeDrawer();
      closeOrphanModal();
      closeMenu();
    }
  });
}

async function initializeSession() {
  const { data: { session } } = await sb.auth.getSession();
  applySession(session);

  sb.auth.onAuthStateChange((_event, nextSession) => {
    applySession(nextSession);
  });
}

function applySession(session) {
  S.session = session || null;
  S.authReady = true;
  if (!session) {
    ELS.app.classList.add('hidden');
    ELS.loginOverlay.classList.remove('hidden');
    ELS.loginOverlay.setAttribute('aria-hidden', 'false');
    ELS.navUser.textContent = '—';
    return;
  }

  ELS.loginOverlay.classList.add('hidden');
  ELS.loginOverlay.setAttribute('aria-hidden', 'true');
  ELS.app.classList.remove('hidden');
  ELS.navUser.textContent = session.user?.email || 'Operatore';
  renderStaticShell();
  void ensureDataLoaded();
}

async function handleLogin(event) {
  event.preventDefault();
  setLoginError('');
  ELS.loginSubmit.disabled = true;
  try {
    const { error } = await sb.auth.signInWithPassword({
      email: ELS.loginEmail.value.trim(),
      password: ELS.loginPassword.value,
    });
    if (error) throw error;
  } catch (error) {
    setLoginError(error.message || 'Accesso non riuscito');
  } finally {
    ELS.loginSubmit.disabled = false;
  }
}

function setLoginError(message) {
  ELS.loginError.textContent = message || '';
}

function toggleMenu() {
  const open = ELS.navbarMenu.classList.toggle('open');
  ELS.navbarToggle.setAttribute('aria-expanded', String(open));
}

function toggleSidebar() {
  const collapsed = ELS.pageBody.classList.toggle('sidebar-collapsed');
  ELS.sidebarToggle.setAttribute('aria-expanded', String(!collapsed));
  ELS.sidebarToggle.setAttribute(
    'aria-label',
    collapsed ? 'Mostra filtri' : 'Nascondi filtri'
  );
}

function closeMenu() {
  ELS.navbarMenu.classList.remove('open');
  ELS.navbarToggle?.setAttribute('aria-expanded', 'false');
}

async function ensureDataLoaded({ monthOnly = false } = {}) {
  if (!S.session) return;
  setError('');
  setLoading(true);
  try {
    if (!S.staticLoaded || !monthOnly) {
      if (!S.staticLoaded) {
        await loadStaticData();
      }
    }
    await loadMonthData();
    initializeFilterDefaults();
  } catch (error) {
    console.error('calendario load error', error);
    setError(error.message || 'Errore nel caricamento del calendario');
  } finally {
    setLoading(false);
    renderAll();
  }
}

async function loadStaticData() {
  const apartmentsQuery = sb
    .from('apartments')
    .select('id,nome_appartamento,struttura_nome,provincia,beds24_property_id,attivo')
    .eq('attivo', true);

  const unitsQuery = sb
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

async function loadMonthData() {
  const { start, end } = getMonthBounds(S.monthDate);
  const inventoryUrl = `/.netlify/functions/get-calendar?dateFrom=${encodeURIComponent(start)}&dateTo=${encodeURIComponent(end)}`;

  const bookingsQuery = sb
    .from('bookings')
    .select('id,beds24_booking_id,guest_first_name,guest_last_name,guest_email,guest_phone,num_adults,num_children,channel,channel_normalized,status,check_in,check_out,total_price,currency,source_updated_at,source_created_at,synced_at,apartment_id,apartment_unit_id,raw_payload')
    .lte('check_in', end)
    .gt('check_out', start)
    .in('status', OCCUPYING_STATUSES)
    .not('apartment_unit_id', 'is', null)
    .order('check_in')
    .order('beds24_booking_id');

  const orphanQuery = sb
    .from('bookings')
    .select('beds24_booking_id,beds24_property_id,beds24_room_id,check_in,check_out,channel,channel_normalized,status,raw_payload', { count: 'exact' })
    .is('apartment_unit_id', null)
    .in('status', OCCUPYING_STATUSES)
    .order('check_in')
    .order('beds24_booking_id');

  const lastSyncQuery = sb
    .from('bookings')
    .select('synced_at')
    .order('synced_at', { ascending: false })
    .limit(1);

  const [
    { data: bookings, error: bookingsError },
    { data: orphanRows, error: orphanError, count: orphanCount },
    { data: lastSyncRows, error: syncError },
    inventoryPayload,
  ] = await Promise.all([
    bookingsQuery,
    orphanQuery,
    lastSyncQuery,
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

  S.inventoryDays = (inventoryPayload?.inventoryDays || []).slice();
  S.inventoryByRoomDate = buildInventoryIndex(S.inventoryDays);
  S.bookings = (bookings || []).map(enrichBooking).sort(compareBookingsForRender);
  S.bookingMap = new Map(S.bookings.map((item) => [String(item.beds24_booking_id), item]));
  S.bookingsByApartment = groupBy(S.bookings, (item) => String(item.apartment_id));
  S.bookingsByUnit = groupBy(S.bookings, (item) => String(item.apartment_unit_id));
  S.orphanRows = (orphanRows || []).map(enrichOrphan).sort(compareOrphans);
  S.orphanCount = Number(orphanCount || 0);
  S.lastSync = lastSyncRows?.[0]?.synced_at || null;
}

function enrichApartment(row) {
  const derivedCity = deriveCity(row);
  return {
    ...row,
    city: derivedCity,
    displayName: row.nome_appartamento || 'Struttura senza nome',
  };
}

function deriveCity(row) {
  const source = [row.nome_appartamento, row.struttura_nome, row.provincia].filter(Boolean).join(' · ');
  for (const [pattern, city] of KNOWN_CITY_MAP) {
    if (pattern.test(source)) return city;
  }
  const trimmed = String(row.struttura_nome || row.provincia || row.nome_appartamento || '').trim();
  if (!trimmed) return 'Altro';
  const firstToken = trimmed.split(/[-,]/)[0].trim();
  return titleCase(firstToken || 'Altro');
}

function enrichBooking(row) {
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

function enrichOrphan(row) {
  return {
    ...row,
    reason: row.raw_payload?.__assignment?.reason || '—',
  };
}

function buildInventoryIndex(rows) {
  const map = new Map();
  for (const row of rows || []) {
    if (!row?.propertyId || !row?.date) continue;
    map.set(`${row.propertyId}:${row.date}`, row);
  }
  return map;
}

function initializeFilterDefaults() {
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

function renderStaticShell() {
  ELS.monthPickerBtn.textContent = formatMonthLabel(S.monthDate);
  ELS.monthPickerInput.value = toMonthInputValue(S.monthDate);
  ELS.rangeInfo.textContent = formatRangeLabel(S.monthDate);
}

function renderAll() {
  renderStaticShell();
  renderFilters();
  renderSyncMeta();
  renderOrphanBanner();
  renderStates();
  renderTimeline();
  renderMobileList();
  renderOrphanModalBody();
  if (S.selectedBookingId && !S.bookingMap.has(String(S.selectedBookingId))) {
    closeDrawer();
  } else if (S.selectedBookingId) {
    renderDrawer();
  }
}

function renderFilters() {
  renderCheckboxGroup({
    target: ELS.cityFilters,
    options: getAvailableCities().map((city) => ({
      value: city,
      label: city,
      count: countVisibleApartmentsByCity(city),
      checked: S.filters.cities.has(city),
      onChange: () => toggleCityFilter(city),
    })),
  });

  renderCheckboxGroup({
    target: ELS.channelFilters,
    options: getAvailableChannels().map((channel) => ({
      value: channel,
      label: channelLabel(channel),
      count: countBookingsByChannel(channel),
      checked: S.filters.channels.has(channel),
      onChange: () => toggleSetValue(S.filters.channels, channel, renderAll),
    })),
  });

  renderCheckboxGroup({
    target: ELS.statusFilters,
    options: getAvailableStatuses().map((status) => ({
      value: status,
      label: STATUS_LABELS[status] || titleCase(status),
      count: countBookingsByStatus(status),
      checked: S.filters.statuses.has(status),
      onChange: () => toggleSetValue(S.filters.statuses, status, renderAll),
    })),
  });
}

function renderCheckboxGroup({ target, options }) {
  target.innerHTML = options.map((option) => `
    <label class="check-item">
      <input type="checkbox" data-filter-value="${esc(option.value)}" ${option.checked ? 'checked' : ''}>
      <span class="check-label">
        <span>${esc(option.label)}</span>
        <span class="check-count">${esc(option.count)}</span>
      </span>
    </label>
  `).join('');

  [...target.querySelectorAll('input[type="checkbox"]')].forEach((input, index) => {
    input.addEventListener('change', options[index].onChange);
  });
}

function renderSyncMeta() {
  ELS.lastSyncLabel.textContent = S.lastSync ? `${formatDateTime(S.lastSync)} (${minutesAgoLabel(S.lastSync)})` : 'Nessun sync ancora registrato';
  ELS.refreshBtn.disabled = S.syncing;
  ELS.refreshBtn.innerHTML = S.syncing ? '<span class="spinner" aria-hidden="true"></span> Aggiornamento…' : 'Aggiorna da Beds24';
}

function renderOrphanBanner() {
  if (!S.orphanCount) {
    ELS.orphanBanner.classList.add('hidden');
    ELS.orphanBanner.textContent = '';
    return;
  }
  ELS.orphanBanner.classList.remove('hidden');
  ELS.orphanBanner.innerHTML = `<strong>${S.orphanCount}</strong> prenotazioni in conflitto da risolvere su Beds24`;
}

function renderStates() {
  ELS.loadingState.classList.toggle('hidden', !S.loading);
  ELS.errorState.classList.toggle('hidden', !S.errorMessage);
  ELS.errorState.innerHTML = S.errorMessage ? `
    <h2>Errore di caricamento</h2>
    <p>${esc(S.errorMessage)}</p>
    <button type="button" class="btn btn-primary" id="retryBtn">Riprova</button>
  ` : '';
  ELS.errorState.querySelector('#retryBtn')?.addEventListener('click', () => ensureDataLoaded({ monthOnly: true }));

  const visibleRows = getVisibleApartmentRows();
  const shouldShowEmpty = !S.loading && !S.errorMessage && !visibleRows.length;
  ELS.emptyState.classList.toggle('hidden', !shouldShowEmpty);
  ELS.timelineShell.classList.toggle('hidden', shouldShowEmpty || !!S.errorMessage);
  ELS.mobileList.classList.toggle('hidden', shouldShowEmpty || !!S.errorMessage);
}

function renderTimeline() {
  const monthDays = getMonthDays(S.monthDate);
  document.documentElement.style.setProperty('--days-count', String(monthDays.length));
  ELS.timelineHeader.innerHTML = buildTimelineHeader(monthDays);

  const grouped = groupVisibleRowsByCity();
  const rowsHtml = [];
  for (const group of grouped) {
    rowsHtml.push(`<div class="section-row">${esc(group.city)}</div>`);
    for (const row of group.rows) {
      rowsHtml.push(buildTimelineRow(row, monthDays));
    }
  }
  ELS.timelineBody.innerHTML = rowsHtml.join('');

  bindTimelineEvents();
}

function buildTimelineHeader(monthDays) {
  return `
    <div class="timeline-grid">
      <div class="timeline-row">
        <div class="timeline-corner">Strutture</div>
        <div class="day-header-row">
          ${monthDays.map((date) => {
            const weekend = isWeekend(date) ? ' weekend' : '';
            const today = isSameDate(date, new Date()) ? ' today' : '';
            return `
              <div class="day-header${weekend}${today}">
                <div class="day-name">${esc(formatWeekday(date))}</div>
                <div class="day-num">${esc(String(date.getDate()))}</div>
              </div>
            `;
          }).join('')}
        </div>
      </div>
    </div>
  `;
}

function buildTimelineRow(row, monthDays) {
  const isResidence = row.kind === 'residence';
  const labelTitle = isResidence
    ? `<strong>${esc(row.apartment.displayName)}</strong>`
    : esc(row.apartment.displayName);
  const meta = isResidence
    ? `${row.apartment.city} · ${row.units.length} unita`
    : `${row.apartment.city} · ${row.maxGuests || '—'} ospiti max`;
  const note = row.kind === 'unit'
    ? esc(row.unit.room_type_label || row.unit.unit_label || '')
    : (isResidence ? 'Heatmap occupazione giornaliera' : '');

  return `
    <div class="timeline-grid">
      <div class="timeline-row ${isResidence ? 'is-residence' : ''}" data-row-kind="${esc(row.kind)}" data-apartment-id="${esc(row.apartment.id)}" ${row.unit ? `data-unit-id="${esc(row.unit.id)}"` : ''}>
        <div class="timeline-label">
          ${isResidence ? `<button type="button" class="residence-toggle" data-toggle-residence="${esc(row.apartment.id)}" aria-expanded="${String(S.expandedResidences.has(String(row.apartment.id)))}" aria-label="Espandi ${esc(row.apartment.displayName)}">${S.expandedResidences.has(String(row.apartment.id)) ? '−' : '+'}</button>` : '<span style="width:28px;flex:0 0 auto;"></span>'}
          <div class="label-content">
            <p class="label-title">${labelTitle}</p>
            <p class="label-meta">${esc(meta)}</p>
            ${note ? `<p class="label-note">${note}</p>` : ''}
          </div>
        </div>
        <div class="timeline-days">
          ${row.kind === 'residence' ? buildHeatCells(row, monthDays) : buildDayCells(row, monthDays)}
          <div class="row-bars">${row.kind === 'residence' ? '' : buildBookingBars(row.bookings, monthDays)}</div>
        </div>
      </div>
    </div>
  `;
}

function buildHeatCells(row, monthDays) {
  const total = row.units.length || 1;
  return monthDays.map((date) => {
    const occupied = countOccupiedUnitsForDate(row.units, date);
    const unavailable = countUnavailableUnitsForDate(row.units, date);
    const ratio = total ? occupied / total : 0;
    const bucket = ratio === 0 ? 0 : ratio < 0.34 ? 1 : ratio < 0.67 ? 2 : ratio < 1 ? 3 : 4;
    const weekend = isWeekend(date) ? ' weekend' : '';
    const today = isSameDate(date, new Date()) ? ' today' : '';
    const closed = unavailable === total ? ' closed-all' : unavailable > 0 ? ' closed-some' : '';
    return `<div class="day-cell heat-cell heat-fill-${bucket}${weekend}${today}${closed}" aria-label="${occupied} occupate su ${total}${unavailable ? `, ${unavailable} non vendibili` : ''}">${occupied}/${total}</div>`;
  }).join('');
}

function buildDayCells(row, monthDays) {
  return monthDays.map((date) => {
    const state = getDayAvailabilityState(row, date);
    const weekend = isWeekend(date) ? ' weekend' : '';
    const today = isSameDate(date, new Date()) ? ' today' : '';
    const unavailable = state.closed ? ' unavail' : '';
    const booked = state.hasBooking ? ' has-booking' : '';
    const badge = state.closed
      ? `<span class="cell-badge" aria-hidden="true">${state.hasBooking ? '•' : '×'}</span>`
      : '';
    return `
      <div
        class="day-cell${weekend}${today}${unavailable}${booked}"
        data-date="${esc(isoDateLocal(date))}"
        data-apartment-id="${esc(row.apartment.id)}"
        data-unit-id="${esc(row.unit?.id || '')}"
        aria-label="${esc(buildDayCellLabel(row, date, state))}"
      >${badge}</div>
    `;
  }).join('');
}

function buildBookingBars(bookings, monthDays) {
  const start = monthDays[0];
  const endExclusive = addDays(monthDays[monthDays.length - 1], 1);
  return bookings.map((booking) => {
    const span = bookingSpanWithinMonth(booking, start, endExclusive);
    if (!span) return '';
    const dayWidth = getDayWidth();
    const left = span.startIndex * dayWidth + 2;
    const width = Math.max(span.days * dayWidth - 4, dayWidth - 8);
    const channelKey = booking.channel_normalized || 'fallback';
    const channelClass = CHANNEL_CONFIG[channelKey]?.className || 'channel-fallback';
    const soft = ['new', 'request'].includes(booking.status) ? ' is-soft' : '';
    const label = booking.channel_normalized === 'block_or_internal'
      ? 'BLOCCO'
      : `${booking.guest_last_name || booking.guest_first_name || 'Ospite'} · ${Math.max(booking.guestCount || 0, 0)}p`;
    const tooltip = buildBookingTooltip(booking);
    return `
      <button
        type="button"
        class="booking-bar ${channelClass}${soft}"
        data-booking-id="${esc(booking.beds24_booking_id)}"
        data-tooltip="${esc(tooltip)}"
        style="left:${left}px;width:${width}px;"
      >
        <span class="bar-label">${esc(label)}</span>
      </button>
    `;
  }).join('');
}

function buildBookingTooltip(booking) {
  const price = booking.total_price != null ? `${formatPrice(booking.total_price)} ${booking.currency || 'EUR'}` : 'Prezzo non disponibile';
  return [
    `${booking.guest_last_name || booking.guest_first_name || 'Prenotazione'}`,
    `${formatDate(booking.check_in)} → ${formatDate(booking.check_out)} · ${nightsBetween(booking.check_in, booking.check_out)} notti`,
    `${channelLabel(booking.channel_normalized)} · ${STATUS_LABELS[booking.status] || booking.status}`,
    price,
  ].join('\n');
}

function bindTimelineEvents() {
  ELS.timelineBody.querySelectorAll('[data-toggle-residence]').forEach((button) => {
    button.addEventListener('click', () => {
      const apartmentId = button.getAttribute('data-toggle-residence');
      if (S.expandedResidences.has(apartmentId)) {
        S.expandedResidences.delete(apartmentId);
      } else {
        S.expandedResidences.add(apartmentId);
      }
      renderTimeline();
    });
  });

  const bars = ELS.timelineBody.querySelectorAll('.booking-bar');
  bars.forEach((bar) => {
    const tooltip = bar.getAttribute('data-tooltip');
    bar.addEventListener('mouseenter', (event) => showTooltip(tooltip, event));
    bar.addEventListener('mousemove', (event) => moveTooltip(event));
    bar.addEventListener('mouseleave', hideTooltip);
    bar.addEventListener('focus', (event) => showTooltip(tooltip, event));
    bar.addEventListener('blur', hideTooltip);
    bar.addEventListener('click', () => openDrawer(bar.getAttribute('data-booking-id')));
  });
}

function renderMobileList() {
  const groups = groupVisibleBookingsForMobile();
  ELS.mobileList.innerHTML = groups.length ? groups.map((group) => `
    <article class="mobile-card">
      <h3>${esc(group.apartment.displayName)}</h3>
      <p class="mobile-meta">${esc(group.apartment.city)} · ${esc(group.meta)}</p>
      ${group.bookings.map((booking) => `
        <div class="mobile-booking">
          <span class="pill">${esc(channelLabel(booking.channel_normalized))}</span>
          <h4>${esc(booking.guest_last_name || booking.guest_first_name || 'BLOCCO')}</h4>
          <p class="mobile-meta">${formatDate(booking.check_in)} → ${formatDate(booking.check_out)} · ${nightsBetween(booking.check_in, booking.check_out)} notti</p>
          <p class="mobile-meta">${esc(STATUS_LABELS[booking.status] || booking.status)} · ${esc(booking.unit?.unit_label || '—')}</p>
          <button type="button" class="btn btn-ghost btn-small" data-mobile-booking="${esc(booking.beds24_booking_id)}">Apri dettagli</button>
        </div>
      `).join('')}
    </article>
  `).join('') : '';

  ELS.mobileList.querySelectorAll('[data-mobile-booking]').forEach((button) => {
    button.addEventListener('click', () => openDrawer(button.getAttribute('data-mobile-booking')));
  });
}

function renderDrawer() {
  const booking = S.bookingMap.get(String(S.selectedBookingId));
  if (!booking) return;

  ELS.drawerTitle.textContent = booking.guest_last_name || booking.guest_first_name || booking.beds24_booking_id;
  ELS.drawerBody.innerHTML = `
    <div class="detail-grid">
      <section class="detail-block">
        <h3>Identita</h3>
        <dl class="detail-list">
          ${detailRow('Booking ID', booking.beds24_booking_id)}
          ${detailRow('Ospite', [booking.guest_first_name, booking.guest_last_name].filter(Boolean).join(' ') || '—')}
          ${detailRow('Email', booking.guest_email || '—')}
          ${detailRow('Telefono', booking.guest_phone || '—')}
        </dl>
      </section>
      <section class="detail-block">
        <h3>Soggiorno</h3>
        <dl class="detail-list">
          ${detailRow('Check-in', formatDate(booking.check_in))}
          ${detailRow('Check-out', formatDate(booking.check_out))}
          ${detailRow('Notti', String(nightsBetween(booking.check_in, booking.check_out)))}
          ${detailRow('Adulti / bambini', `${booking.num_adults || 0} / ${booking.num_children || 0}`)}
        </dl>
      </section>
      <section class="detail-block">
        <h3>Canale e stato</h3>
        <dl class="detail-list">
          ${detailRow('Canale raw', booking.channel || '—')}
          ${detailRow('Canale normalizzato', channelLabel(booking.channel_normalized))}
          ${detailRow('Stato', STATUS_LABELS[booking.status] || booking.status)}
          ${detailRow('Totale', booking.total_price != null ? `${formatPrice(booking.total_price)} ${booking.currency || 'EUR'}` : '—')}
        </dl>
      </section>
      <section class="detail-block">
        <h3>Alloggio</h3>
        <dl class="detail-list">
          ${detailRow('Apartment', booking.apartment?.displayName || '—')}
          ${detailRow('Apartment ID', booking.apartment_id || '—')}
          ${detailRow('Unita', booking.unit?.unit_label || '—')}
          ${detailRow('Apartment unit ID', booking.apartment_unit_id || '—')}
        </dl>
      </section>
      <section class="detail-block">
        <h3>Tracciamento</h3>
        <dl class="detail-list">
          ${detailRow('Creato sorgente', formatDateTime(booking.source_created_at))}
          ${detailRow('Aggiornato sorgente', formatDateTime(booking.source_updated_at))}
          ${detailRow('Sincronizzato', formatDateTime(booking.synced_at))}
          ${detailRow('Note', booking.notes || '—')}
        </dl>
      </section>
    </div>
  `;

  ELS.bookingDrawer.classList.add('open');
  ELS.bookingDrawer.setAttribute('aria-hidden', 'false');
  ELS.drawerBackdrop.classList.remove('hidden');
}

function detailRow(label, value) {
  return `<div class="detail-row"><dt>${esc(label)}</dt><dd>${esc(value)}</dd></div>`;
}

function openDrawer(bookingId) {
  S.selectedBookingId = bookingId;
  renderDrawer();
}

function closeDrawer() {
  S.selectedBookingId = null;
  ELS.bookingDrawer.classList.remove('open');
  ELS.bookingDrawer.setAttribute('aria-hidden', 'true');
  ELS.drawerBackdrop.classList.add('hidden');
}

function openOrphanModal() {
  ELS.orphanModal.classList.remove('hidden');
  ELS.orphanModal.setAttribute('aria-hidden', 'false');
}

function closeOrphanModal() {
  ELS.orphanModal.classList.add('hidden');
  ELS.orphanModal.setAttribute('aria-hidden', 'true');
}

function renderOrphanModalBody() {
  ELS.orphanModalTitle.textContent = S.orphanCount ? `${S.orphanCount} conflitti aperti` : 'Nessun conflitto';
  if (!S.orphanCount) {
    ELS.orphanModalBody.innerHTML = '<p>Nessuna prenotazione in conflitto nel dataset corrente.</p>';
    return;
  }

  ELS.orphanModalBody.innerHTML = `
    <table class="orphan-table">
      <thead>
        <tr>
          <th>Booking ID</th>
          <th>Property</th>
          <th>Room</th>
          <th>Check-in</th>
          <th>Check-out</th>
          <th>Canale</th>
          <th>Stato</th>
          <th>Motivo</th>
        </tr>
      </thead>
      <tbody>
        ${S.orphanRows.map((row) => `
          <tr>
            <td>${esc(row.beds24_booking_id)}</td>
            <td>${esc(row.beds24_property_id || '—')}</td>
            <td>${esc(row.beds24_room_id || '—')}</td>
            <td>${esc(formatDate(row.check_in))}</td>
            <td>${esc(formatDate(row.check_out))}</td>
            <td>${esc(channelLabel(row.channel_normalized))}</td>
            <td>${esc(STATUS_LABELS[row.status] || row.status)}</td>
            <td>${esc(row.reason || '—')}</td>
          </tr>
        `).join('')}
      </tbody>
    </table>
  `;
}

async function refreshFromBeds24() {
  if (S.syncing) return;
  S.syncing = true;
  renderSyncMeta();
  try {
    const url = '/.netlify/functions/beds24-sync-bookings?dryRun=0';
    const response = await fetch(url, { method: 'GET' });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(payload.error || payload.detail || 'Aggiornamento Beds24 non riuscito');
    }
    await ensureDataLoaded({ monthOnly: true });
  } catch (error) {
    console.error('calendario refresh error', error);
    alert(error.message || 'Aggiornamento non riuscito');
  } finally {
    S.syncing = false;
    renderSyncMeta();
  }
}

function exportVisibleCsv() {
  const rows = getVisibleBookings().map((booking) => ({
    beds24_booking_id: booking.beds24_booking_id,
    apartment: booking.apartment?.displayName || '',
    unit: booking.unit?.unit_label || '',
    citta: booking.apartment?.city || '',
    check_in: booking.check_in,
    check_out: booking.check_out,
    num_notti: nightsBetween(booking.check_in, booking.check_out),
    num_adults: booking.num_adults || 0,
    num_children: booking.num_children || 0,
    status: booking.status,
    channel: booking.channel_normalized || booking.channel || '',
    total_price: booking.total_price ?? '',
    currency: booking.currency || '',
    guest_last_name: booking.guest_last_name || '',
    source_updated_at: booking.source_updated_at || '',
  }));

  const headers = ['beds24_booking_id', 'apartment', 'unit', 'citta', 'check_in', 'check_out', 'num_notti', 'num_adults', 'num_children', 'status', 'channel', 'total_price', 'currency', 'guest_last_name', 'source_updated_at'];
  const csvLines = [
    headers.join(','),
    ...rows.map((row) => headers.map((key) => csvCell(row[key])).join(',')),
  ];
  const blob = new Blob([`\uFEFF${csvLines.join('\n')}`], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = `calendario-${toMonthInputValue(S.monthDate)}.csv`;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function csvCell(value) {
  const text = String(value ?? '');
  return `"${text.replace(/"/g, '""')}"`;
}

function shiftMonth(delta) {
  S.monthDate = startOfMonth(new Date(S.monthDate.getFullYear(), S.monthDate.getMonth() + delta, 1));
  ELS.monthPickerInput.value = toMonthInputValue(S.monthDate);
  void ensureDataLoaded({ monthOnly: true });
}

function handleMonthPicker() {
  const value = ELS.monthPickerInput.value;
  if (!value) return;
  const [year, month] = value.split('-').map(Number);
  S.monthDate = startOfMonth(new Date(year, month - 1, 1));
  void ensureDataLoaded({ monthOnly: true });
}

function setLoading(value) {
  S.loading = value;
}

function setError(message) {
  S.errorMessage = message || '';
}

function getAvailableCities() {
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

function getAvailableChannels() {
  return unique(S.bookings.map((booking) => booking.channel_normalized || booking.channel || 'fallback')).sort((a, b) => channelLabel(a).localeCompare(channelLabel(b), 'it'));
}

function getAvailableStatuses() {
  return unique(S.bookings.map((booking) => booking.status)).sort((a, b) => (STATUS_LABELS[a] || a).localeCompare(STATUS_LABELS[b] || b, 'it'));
}

function countVisibleApartmentsByCity(city) {
  return S.apartments.filter((apartment) => apartment.city === city && (S.unitsByApartment.get(String(apartment.id)) || []).length > 0).length;
}

function countBookingsByChannel(channel) {
  return S.bookings.filter((booking) => (booking.channel_normalized || booking.channel || 'fallback') === channel).length;
}

function countBookingsByStatus(status) {
  return S.bookings.filter((booking) => booking.status === status).length;
}

function toggleCityFilter(city) {
  const selected = S.filters.cities;
  if (selected.has(city)) {
    if (selected.size === 1) {
      renderFilters();
      return;
    }
    selected.delete(city);
  } else {
    if (selected.size >= 8) {
      renderFilters();
      alert('Puoi selezionare al massimo 8 citta alla volta.');
      return;
    }
    selected.add(city);
  }
  renderAll();
}

function toggleSetValue(set, value, rerender) {
  if (set.has(value)) {
    if (set.size === 1) {
      renderFilters();
      return;
    }
    set.delete(value);
  } else {
    set.add(value);
  }
  rerender();
}

function getVisibleApartmentRows() {
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

function groupVisibleRowsByCity() {
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

function groupVisibleBookingsForMobile() {
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

function getVisibleBookings() {
  return S.bookings.filter(matchesFilters).sort(compareBookingsForRender);
}

function matchesFilters(booking) {
  const apartment = booking.apartment;
  if (!apartment) return false;
  if (!S.filters.cities.has(apartment.city)) return false;
  const channelKey = booking.channel_normalized || booking.channel || 'fallback';
  if (!S.filters.channels.has(channelKey)) return false;
  if (!S.filters.statuses.has(booking.status)) return false;
  return true;
}

function countOccupiedUnitsForDate(units, date) {
  const iso = isoDateLocal(date);
  let occupied = 0;
  for (const unit of units) {
    const bookings = getBookingsForUnit(unit.id).filter(matchesFilters);
    const hasBooking = bookings.some((booking) => booking.check_in <= iso && booking.check_out > iso);
    if (hasBooking) occupied += 1;
  }
  return occupied;
}

function countUnavailableUnitsForDate(units, date) {
  let unavailable = 0;
  for (const unit of units) {
    const state = getInventoryStateForUnit(unit, date);
    if (state.closed) unavailable += 1;
  }
  return unavailable;
}

function getBookingsForUnit(unitId) {
  return (S.bookingsByUnit.get(String(unitId)) || []).slice().sort(compareBookingsForRender);
}

function getInventoryStateForUnit(unit, date) {
  const roomId = String(unit?.beds24_room_id || '').trim();
  if (!roomId) return { closed: false, available: null, hasBooking: false, price: null, minStay: null };
  const iso = typeof date === 'string' ? date : isoDateLocal(date);
  const row = S.inventoryByRoomDate.get(`${roomId}:${iso}`) || null;
  if (!row) return { closed: false, available: null, hasBooking: false, price: null, minStay: null };
  return {
    closed: row.closed === true || row.available === false,
    available: row.available,
    hasBooking: Boolean(row.hasBooking),
    price: row.price,
    minStay: row.minStay,
  };
}

function getDayAvailabilityState(row, date) {
  const state = getInventoryStateForUnit(row.unit, date);
  const iso = isoDateLocal(date);
  const hasBooking = row.bookings.some((booking) => booking.check_in <= iso && booking.check_out > iso);
  return {
    ...state,
    hasBooking: state.hasBooking || hasBooking,
  };
}

function buildDayCellLabel(row, date, state) {
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

function compareApartment(a, b) {
  const cityCompare = compareCity(a.city, b.city);
  if (cityCompare !== 0) return cityCompare;
  return a.displayName.localeCompare(b.displayName, 'it');
}

function compareCity(a, b) {
  const aIndex = CITY_PRIORITY.indexOf(a);
  const bIndex = CITY_PRIORITY.indexOf(b);
  if (aIndex >= 0 && bIndex >= 0) return aIndex - bIndex;
  if (aIndex >= 0) return -1;
  if (bIndex >= 0) return 1;
  return String(a).localeCompare(String(b), 'it');
}

function compareBookingsForRender(a, b) {
  const inCompare = String(a.check_in).localeCompare(String(b.check_in));
  if (inCompare !== 0) return inCompare;
  return String(a.beds24_booking_id).localeCompare(String(b.beds24_booking_id), 'it');
}

function compareOrphans(a, b) {
  const inCompare = String(a.check_in).localeCompare(String(b.check_in));
  if (inCompare !== 0) return inCompare;
  return String(a.beds24_booking_id).localeCompare(String(b.beds24_booking_id), 'it');
}

function bookingSpanWithinMonth(booking, monthStart, monthEndExclusive) {
  const bookingStart = parseIsoDate(booking.check_in);
  const bookingEnd = parseIsoDate(booking.check_out);
  const start = bookingStart > monthStart ? bookingStart : monthStart;
  const end = bookingEnd < monthEndExclusive ? bookingEnd : monthEndExclusive;
  const days = diffDays(start, end);
  if (days <= 0) return null;
  return {
    startIndex: diffDays(monthStart, start),
    days,
  };
}

function channelLabel(value) {
  if (!value) return 'Altro';
  return CHANNEL_CONFIG[value]?.label || titleCase(String(value).replace(/_/g, ' '));
}

function formatMonthLabel(date) {
  return date.toLocaleDateString('it-IT', { month: 'long', year: 'numeric' }).replace(/^\w/, (c) => c.toUpperCase());
}

function formatRangeLabel(date) {
  const { start, end } = getMonthBounds(date);
  const from = formatDayMonthYear(parseIsoDate(start));
  const to = formatDayMonthYear(parseIsoDate(end));
  return `Dal ${from} al ${to}`;
}

function formatWeekday(date) {
  return date.toLocaleDateString('it-IT', { weekday: 'short' }).replace('.', '').toUpperCase();
}

function formatDayMonthYear(date) {
  return date.toLocaleDateString('it-IT', { day: 'numeric', month: 'long', year: 'numeric' });
}

function formatDate(value) {
  if (!value) return '—';
  return parseIsoDate(value).toLocaleDateString('it-IT', { day: '2-digit', month: '2-digit', year: 'numeric' });
}

function formatDateTime(value) {
  if (!value) return '—';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString('it-IT', { day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit' });
}

function formatPrice(value) {
  return new Intl.NumberFormat('it-IT', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(Number(value || 0));
}

function minutesAgoLabel(value) {
  const then = new Date(value);
  const diff = Math.max(0, Math.round((Date.now() - then.getTime()) / 60000));
  if (diff < 1) return 'meno di un minuto fa';
  if (diff === 1) return '1 minuto fa';
  if (diff < 60) return `${diff} minuti fa`;
  const hours = Math.round(diff / 60);
  return hours === 1 ? '1 ora fa' : `${hours} ore fa`;
}

function nightsBetween(checkIn, checkOut) {
  return diffDays(parseIsoDate(checkIn), parseIsoDate(checkOut));
}

function getMonthBounds(date) {
  const startDate = startOfMonth(date);
  const endDate = new Date(startDate.getFullYear(), startDate.getMonth() + 1, 0);
  return {
    start: isoDateLocal(startDate),
    end: isoDateLocal(endDate),
  };
}

function getMonthDays(date) {
  const first = startOfMonth(date);
  const days = [];
  const cursor = new Date(first);
  while (cursor.getMonth() === first.getMonth()) {
    days.push(new Date(cursor));
    cursor.setDate(cursor.getDate() + 1);
  }
  return days;
}

function getDayWidth() {
  const value = getComputedStyle(document.documentElement)
    .getPropertyValue('--day-width').trim();
  return parseInt(value, 10) || 48;
}

function startOfMonth(date) {
  return new Date(date.getFullYear(), date.getMonth(), 1);
}

function parseIsoDate(value) {
  const [year, month, day] = String(value).split('-').map(Number);
  return new Date(year, month - 1, day);
}

function isoDateLocal(date) {
  const year = date.getFullYear();
  const month = `${date.getMonth() + 1}`.padStart(2, '0');
  const day = `${date.getDate()}`.padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function toMonthInputValue(date) {
  return `${date.getFullYear()}-${`${date.getMonth() + 1}`.padStart(2, '0')}`;
}

function addDays(date, amount) {
  const next = new Date(date);
  next.setDate(next.getDate() + amount);
  return next;
}

function diffDays(a, b) {
  const ms = parseIsoDate(isoDateLocal(b)).getTime() - parseIsoDate(isoDateLocal(a)).getTime();
  return Math.round(ms / 86400000);
}

function isWeekend(date) {
  const day = date.getDay();
  return day === 0 || day === 6;
}

function isSameDate(left, right) {
  return isoDateLocal(left) === isoDateLocal(right);
}

function normalizeText(value) {
  const text = String(value ?? '').trim();
  return text || '';
}

function titleCase(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function sum(values) {
  return values.reduce((total, value) => total + Number(value || 0), 0);
}

function esc(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function groupBy(items, keyFn, sortFn = null) {
  const map = new Map();
  for (const item of items) {
    const key = keyFn(item);
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(item);
  }
  if (sortFn) {
    for (const entry of map.values()) {
      entry.sort(sortFn);
    }
  }
  return map;
}

function unique(values) {
  return [...new Set(values)];
}

function showTooltip(text, event) {
  if (!text) return;
  ELS.tooltip.textContent = text;
  ELS.tooltip.classList.remove('hidden');
  moveTooltip(event);
}

function moveTooltip(event) {
  if (ELS.tooltip.classList.contains('hidden')) return;
  ELS.tooltip.style.left = `${Math.min(window.innerWidth - 320, event.clientX + 16)}px`;
  ELS.tooltip.style.top = `${Math.min(window.innerHeight - 80, event.clientY + 16)}px`;
}

function hideTooltip() {
  ELS.tooltip.classList.add('hidden');
}
