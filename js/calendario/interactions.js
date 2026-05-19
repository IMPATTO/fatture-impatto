import {
  getVisibleBookings,
  initializeFilterDefaults,
  loadMonthData,
  loadStaticData,
} from './data.js?v=20260518a';
import { CITY_PRIORITY, ELS, PMS_EDITOR_EMAILS, S, SIDEBAR_STORAGE_KEY } from './state.js?v=20260518a';
import {
  nightsBetween,
  startOfMonth,
  toMonthInputValue,
} from './utils.js?v=20260518a';

const RENDER = {
  renderAll: null,
  renderFilters: null,
  renderStaticShell: null,
  renderSyncMeta: null,
  closeDrawer: null,
  openOrphanModal: null,
  closeOrphanModal: null,
};

export async function init() {
  cacheElements();
  await import('./render.js?v=20260518a');
  bindShellEvents();
  restoreSidebarState();
  handleScrollHeaderToggle();
  await initializeSession();
}

export function registerRenderBindings(bindings) {
  Object.assign(RENDER, bindings);
}

export function cacheElements() {
  [
    'loginOverlay', 'loginForm', 'loginEmail', 'loginPassword', 'loginError', 'loginSubmit',
    'app', 'navUser', 'logoutBtn', 'navbarToggle', 'navbarMenu', 'rangeInfo',
    'prevMonthBtn', 'nextMonthBtn', 'monthPickerBtn', 'monthPickerInput',
    'refreshBtn', 'syncPricesBtn', 'exportBtn', 'lastSyncLabel', 'lastSyncBadge', 'sidebarToggle', 'pageBody', 'pageHeader', 'sidebar', 'cityFilters', 'channelFilters',
    'miniHeader', 'miniMonthLabel', 'miniPrevMonth', 'miniNextMonth', 'miniRefreshBtn',
    'statusFilters', 'availabilityFilter', 'timelineHeader', 'timelineBody', 'timelineShell', 'timelineScroll',
    'timelineTopScrollbar', 'timelineTopScrollbarInner',
    'mobileList', 'loadingState', 'emptyState', 'errorState', 'orphanBanner',
    'bookingDrawer', 'drawerBackdrop', 'drawerCloseBtn', 'drawerCloseBtnFooter',
    'drawerTitle', 'drawerBody', 'orphanModal', 'orphanModalBody', 'orphanModalTitle', 'todayMarker',
    'orphanModalClose', 'orphanModalCloseFooter', 'tooltip',
    'editCellModal', 'editCellTitle', 'editCellClose', 'editCellCancel',
    'editCellSubmit', 'editCellForm', 'editCellApartmentLabel', 'editCellDateLabel',
    'editCellPrice', 'editCellMinStay', 'editCellStatus', 'editCellError',
    'bulkEditBtn', 'bulkModal', 'bulkClose', 'bulkCancel', 'bulkSubmit',
    'bulkApartmentFilter', 'bulkApartmentList', 'bulkApartmentsCount',
    'bulkSelectAll', 'bulkDeselectAll',
    'bulkDateFrom', 'bulkDateTo', 'bulkOnlyWeekends', 'bulkDatesCount',
    'bulkPriceSet', 'bulkPriceDelta', 'bulkPricePercent',
    'bulkMinStayMode', 'bulkMinStayValue',
    'bulkAvailabilityMode',
    'bulkPreview', 'bulkError',
    'dragSelectToolbar', 'dragSelectCount', 'dragSelectEdit', 'dragSelectBooking', 'dragSelectClear',
    'bookingFormModal', 'bookingFormTitle', 'bookingFormClose', 'bookingFormCancel',
    'bookingFormSubmit', 'bookingFormForm',
    'bookingFormApartment', 'bookingFormArrival', 'bookingFormDeparture',
    'bookingFormAdults', 'bookingFormChildren',
    'bookingFormFirstName', 'bookingFormLastName', 'bookingFormEmail', 'bookingFormPhone',
    'bookingFormPrice', 'bookingFormNotes', 'bookingFormStatus', 'bookingFormStatusRow',
    'bookingFormConflict', 'bookingFormConflictList', 'bookingFormError',
  ].forEach((id) => {
    ELS[id] = document.getElementById(id);
  });
}

export function bindShellEvents() {
  ELS.loginForm?.addEventListener('submit', handleLogin);
  ELS.logoutBtn?.addEventListener('click', () => window.sb.auth.signOut());
  ELS.navbarToggle?.addEventListener('click', toggleMenu);
  ELS.sidebarToggle?.addEventListener('click', toggleSidebar);
  ELS.prevMonthBtn?.addEventListener('click', () => shiftMonth(-1));
  ELS.nextMonthBtn?.addEventListener('click', () => shiftMonth(1));
  ELS.miniPrevMonth?.addEventListener('click', () => shiftMonth(-1));
  ELS.miniNextMonth?.addEventListener('click', () => shiftMonth(1));
  ELS.monthPickerBtn?.addEventListener('click', () => ELS.monthPickerInput?.showPicker ? ELS.monthPickerInput.showPicker() : ELS.monthPickerInput.click());
  ELS.monthPickerInput?.addEventListener('change', handleMonthPicker);
  ELS.refreshBtn?.addEventListener('click', refreshFromBeds24);
  ELS.syncPricesBtn?.addEventListener('click', syncPricesWeek);
  ELS.miniRefreshBtn?.addEventListener('click', refreshFromBeds24);
  ELS.exportBtn?.addEventListener('click', exportVisibleCsv);
  ELS.availabilityFilter?.addEventListener('change', () => {
    S.filters.availableNights = Number(ELS.availabilityFilter.value) || 0;
    RENDER.renderAll?.();
  });
  ELS.orphanBanner?.addEventListener('click', () => RENDER.openOrphanModal?.());
  ELS.orphanBanner?.addEventListener('keydown', (event) => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      RENDER.openOrphanModal?.();
    }
  });
  ELS.drawerBackdrop?.addEventListener('click', () => RENDER.closeDrawer?.());
  ELS.drawerCloseBtn?.addEventListener('click', () => RENDER.closeDrawer?.());
  ELS.drawerCloseBtnFooter?.addEventListener('click', () => RENDER.closeDrawer?.());
  ELS.orphanModalClose?.addEventListener('click', () => RENDER.closeOrphanModal?.());
  ELS.orphanModalCloseFooter?.addEventListener('click', () => RENDER.closeOrphanModal?.());
  ELS.orphanModal?.addEventListener('click', (event) => {
    if (event.target === ELS.orphanModal) RENDER.closeOrphanModal?.();
  });
  ELS.editCellClose?.addEventListener('click', closeEditModal);
  ELS.editCellCancel?.addEventListener('click', closeEditModal);
  ELS.editCellForm?.addEventListener('submit', handleEditCellSubmit);
  ELS.editCellModal?.addEventListener('click', (event) => {
    if (event.target === ELS.editCellModal) closeEditModal();
  });
  ELS.bulkEditBtn?.addEventListener('click', openBulkModal);
  ELS.bulkClose?.addEventListener('click', closeBulkModal);
  ELS.bulkCancel?.addEventListener('click', closeBulkModal);
  ELS.bulkSubmit?.addEventListener('click', handleBulkSubmit);
  ELS.bulkModal?.addEventListener('click', (event) => {
    if (event.target === ELS.bulkModal) closeBulkModal();
  });
  ELS.bulkSelectAll?.addEventListener('click', () => bulkToggleAll(true));
  ELS.bulkDeselectAll?.addEventListener('click', () => bulkToggleAll(false));
  ELS.bulkApartmentFilter?.addEventListener('input', updateBulkApartmentList);
  ELS.bulkDateFrom?.addEventListener('change', updateBulkPreview);
  ELS.bulkDateTo?.addEventListener('change', updateBulkPreview);
  ELS.bulkOnlyWeekends?.addEventListener('change', updateBulkPreview);
  document.querySelectorAll('input[name="bulkPriceMode"]').forEach((radio) => {
    radio.addEventListener('change', updateBulkPriceMode);
  });
  ELS.bulkPriceSet?.addEventListener('input', updateBulkPreview);
  ELS.bulkPriceDelta?.addEventListener('input', updateBulkPreview);
  ELS.bulkPricePercent?.addEventListener('input', updateBulkPreview);
  ELS.bulkMinStayMode?.addEventListener('change', updateBulkMinStayMode);
  ELS.bulkMinStayValue?.addEventListener('input', updateBulkPreview);
  ELS.bulkAvailabilityMode?.addEventListener('change', updateBulkPreview);
  document.addEventListener('click', (event) => {
    if (!S.bulk?.open) return;
    if (!ELS.bulkModal) return;
    if (ELS.bulkModal.contains(event.target)) return;
    if (ELS.bulkEditBtn && ELS.bulkEditBtn.contains(event.target)) return;
    if (ELS.dragSelectEdit && ELS.dragSelectEdit.contains(event.target)) return;
    if (ELS.dragSelectBooking && ELS.dragSelectBooking.contains(event.target)) return;
    closeBulkModal();
  });
  ELS.dragSelectEdit?.addEventListener('click', openBulkFromSelection);
  ELS.dragSelectBooking?.addEventListener('click', openBookingFromSelection);
  ELS.dragSelectClear?.addEventListener('click', clearDragSelection);
  ELS.bookingFormClose?.addEventListener('click', closeBookingForm);
  ELS.bookingFormCancel?.addEventListener('click', closeBookingForm);
  ELS.bookingFormForm?.addEventListener('submit', handleBookingFormSubmit);
  ELS.bookingFormModal?.addEventListener('click', (event) => {
    if (event.target === ELS.bookingFormModal) closeBookingForm();
  });
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') {
      RENDER.closeDrawer?.();
      RENDER.closeOrphanModal?.();
      closeEditModal();
      closeBulkModal();
      closeBookingForm();
      closeMenu();
    }
  });
  window.addEventListener('scroll', handleScrollHeaderToggle, { passive: true });
  ELS.timelineScroll?.addEventListener('scroll', handleTimelineScroll, { passive: true });
  ELS.timelineTopScrollbar?.addEventListener('scroll', handleTopTimelineScrollbarScroll, { passive: true });
  window.addEventListener('resize', () => {
    syncStickyTimelineHeader();
    syncTimelineScrollChrome();
  }, { passive: true });
}

const SCROLL_THRESHOLD_SHOW = 200;
const SCROLL_THRESHOLD_HIDE = 80;
let _miniHeaderVisible = false;

export async function initializeSession() {
  const { data: { session } } = await window.sb.auth.getSession();
  applySession(session);

  window.sb.auth.onAuthStateChange((_event, nextSession) => {
    applySession(nextSession);
  });
}

export function applySession(session) {
  S.session = session || null;
  S.authReady = true;
  const email = (session?.user?.email || '').toLowerCase();
  S.isPmsEditor = PMS_EDITOR_EMAILS.has(email);
  if (ELS.bulkEditBtn) {
    ELS.bulkEditBtn.style.display = S.isPmsEditor ? '' : 'none';
  }
  if (!session) {
    S.lastNightlySync = null;
    renderSyncBadge();
    clearDragSelection();
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
  RENDER.renderStaticShell?.();
  void loadLastSyncStatus();
  void ensureDataLoaded();
}

export async function handleLogin(event) {
  event.preventDefault();
  setLoginError('');
  ELS.loginSubmit.disabled = true;
  try {
    const { error } = await window.sb.auth.signInWithPassword({
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

export function setLoginError(message) {
  ELS.loginError.textContent = message || '';
}

export function toggleMenu() {
  const open = ELS.navbarMenu.classList.toggle('open');
  ELS.navbarToggle.setAttribute('aria-expanded', String(open));
}

export function toggleSidebar() {
  const collapsed = ELS.pageBody.classList.toggle('sidebar-collapsed');
  ELS.sidebarToggle.setAttribute('aria-expanded', String(!collapsed));
  ELS.sidebarToggle.setAttribute(
    'aria-label',
    collapsed ? 'Mostra filtri' : 'Nascondi filtri'
  );
  try {
    localStorage.setItem(SIDEBAR_STORAGE_KEY, collapsed ? '1' : '0');
  } catch (e) {
    // localStorage può fallire in incognito Safari, ignora
  }
}

export function restoreSidebarState() {
  try {
    const stored = localStorage.getItem(SIDEBAR_STORAGE_KEY);
    if (stored === '1') {
      ELS.pageBody?.classList.add('sidebar-collapsed');
      ELS.sidebarToggle?.setAttribute('aria-expanded', 'false');
      ELS.sidebarToggle?.setAttribute('aria-label', 'Mostra filtri');
    }
  } catch (e) {
    // localStorage può fallire in incognito Safari, ignora
  }
}

export function closeMenu() {
  ELS.navbarMenu.classList.remove('open');
  ELS.navbarToggle?.setAttribute('aria-expanded', 'false');
}

export async function ensureDataLoaded({ monthOnly = false } = {}) {
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
    RENDER.renderAll?.();
    void loadLastSyncStatus();
  }
}

export async function refreshFromBeds24() {
  if (S.syncing) return;
  S.syncing = true;
  RENDER.renderSyncMeta?.();
  try {
    const token = S.session?.access_token;
    if (!token) throw new Error('Sessione non valida');

    const bookingsUrl = '/.netlify/functions/beds24-sync-bookings?dryRun=0';
    const bookingsResponse = await fetch(bookingsUrl, {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${token}`,
      },
    });
    const bookingsPayload = await bookingsResponse.json().catch(() => ({}));
    if (!bookingsResponse.ok) {
      throw new Error(bookingsPayload.error || bookingsPayload.detail || 'Aggiornamento Beds24 non riuscito');
    }

    await ensureDataLoaded({ monthOnly: true });
  } catch (error) {
    console.error('calendario refresh error', error);
    alert(error.message || 'Aggiornamento non riuscito');
  } finally {
    S.syncing = false;
    RENDER.renderSyncMeta?.();
    void loadLastSyncStatus();
  }
}

export async function syncPricesWeek() {
  if (S.syncingPrices) return;
  S.syncingPrices = true;
  updatePricesSyncBtn();
  try {
    const token = S.session?.access_token;
    if (!token) throw new Error('Sessione non valida');

    const url = '/.netlify/functions/sync-beds24-calendar-background?days=60';
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${token}`,
      },
    });

    if (response.status !== 202 && !response.ok) {
      const payload = await response.json().catch(() => ({}));
      throw new Error(payload.error || `Sync prezzi failed: ${response.status}`);
    }

    alert('Sync prezzi avviato in background. Tra 2-3 minuti aggiorna la pagina per vedere i prezzi dei prossimi 60 giorni.');
  } catch (error) {
    console.error('sync prezzi error', error);
    alert(error.message || 'Sync prezzi non riuscito');
  } finally {
    S.syncingPrices = false;
    updatePricesSyncBtn();
    void loadLastSyncStatus();
  }
}

async function loadLastSyncStatus() {
  if (!S.session || !window.sb) return;

  try {
    const legacy = await window.sb
      .from('sync_jobs')
      .select('status, finished_at, error_message')
      .eq('scope', 'cron_calendar_prices')
      .order('finished_at', { ascending: false })
      .limit(2);

    if (legacy.error) {
      throw legacy.error;
    }

    applyLastSyncRows(
      (legacy.data || []).map((row) => ({
        status: row.status,
        finished_at: row.finished_at,
        error_message: row.error_message || '',
      }))
    );
  } catch (error) {
    console.warn('[SYNC-BADGE-LOAD-FAIL]', error);
    S.lastNightlySync = null;
    renderSyncBadge();
  }
}

function applyLastSyncRows(rows) {
  if (!rows.length) {
    S.lastNightlySync = null;
    renderSyncBadge();
    return;
  }

  const last = rows[0];
  const prev = rows[1];
  const finishedAt = last.finished_at ? new Date(last.finished_at) : null;
  const ageHours = finishedAt ? (Date.now() - finishedAt.getTime()) / 3600000 : null;

  S.lastNightlySync = {
    status: last.status,
    finished_at: last.finished_at,
    error_message: last.error_message || '',
    ageHours,
    consecutiveFailures: (last.status === 'failed' && prev?.status === 'failed') ? 2 : (last.status === 'failed' ? 1 : 0),
  };

  renderSyncBadge();
}

function renderSyncBadge() {
  if (!ELS.lastSyncBadge) return;

  if (!S.lastNightlySync) {
    ELS.lastSyncBadge.classList.add('hidden');
    ELS.lastSyncBadge.textContent = '';
    return;
  }

  const { status, finished_at, ageHours, consecutiveFailures } = S.lastNightlySync;
  const dateStr = finished_at
    ? new Date(finished_at).toLocaleString('it-IT', {
      day: '2-digit',
      month: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
    })
    : '—';

  let label = `Sync ${dateStr}`;
  let cls = '';

  if (consecutiveFailures >= 2) {
    label = `Sync FALLITO ${dateStr}`;
    cls = 'error';
  } else if (status === 'failed') {
    label = `Sync fallito ${dateStr}`;
    cls = 'warn';
  } else if (status === 'running') {
    label = 'Sync in corso';
    cls = 'warn';
  } else if (ageHours != null && ageHours > 26) {
    label = `Sync vecchio ${dateStr}`;
    cls = 'warn';
  }

  ELS.lastSyncBadge.textContent = label;
  ELS.lastSyncBadge.className = `last-sync-badge${cls ? ` ${cls}` : ''}`;
}

export function exportVisibleCsv() {
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

export function csvCell(value) {
  const text = String(value ?? '');
  return `"${text.replace(/"/g, '""')}"`;
}

export function shiftMonth(delta) {
  S.monthDate = startOfMonth(new Date(S.monthDate.getFullYear(), S.monthDate.getMonth() + delta, 1));
  ELS.monthPickerInput.value = toMonthInputValue(S.monthDate);
  syncMiniHeaderLabel();
  void ensureDataLoaded({ monthOnly: true });
}

export function handleMonthPicker() {
  const value = ELS.monthPickerInput.value;
  if (!value) return;
  const [year, month] = value.split('-').map(Number);
  S.monthDate = startOfMonth(new Date(year, month - 1, 1));
  syncMiniHeaderLabel();
  void ensureDataLoaded({ monthOnly: true });
}

export function setLoading(value) {
  S.loading = value;
}

export function setError(message) {
  S.errorMessage = message || '';
}

export function handleRetryClick() {
  void ensureDataLoaded({ monthOnly: true });
}

export function openEditModal({
  apartmentUnitId,
  date,
  currentPrice,
  currentMinStay,
  currentClosed,
  apartmentLabel,
}) {
  if (!S.isPmsEditor) {
    console.warn('[EDIT] User not PMS editor');
    return;
  }

  S.editing.open = true;
  S.editing.apartment_unit_id = apartmentUnitId;
  S.editing.date = date;
  S.editing.initialPrice = currentPrice;
  S.editing.initialMinStay = currentMinStay;
  S.editing.initialClosed = currentClosed;
  S.editing.submitting = false;
  S.editing.error = '';

  if (ELS.editCellApartmentLabel) ELS.editCellApartmentLabel.textContent = apartmentLabel || '—';
  if (ELS.editCellDateLabel) ELS.editCellDateLabel.textContent = date;
  if (ELS.editCellPrice) ELS.editCellPrice.value = '';
  if (ELS.editCellMinStay) ELS.editCellMinStay.value = '';
  if (ELS.editCellStatus) ELS.editCellStatus.value = 'unchanged';
  if (ELS.editCellError) {
    ELS.editCellError.classList.add('hidden');
    ELS.editCellError.textContent = '';
  }
  if (ELS.editCellSubmit) {
    ELS.editCellSubmit.disabled = false;
    ELS.editCellSubmit.textContent = 'Salva su Beds24';
  }

  ELS.editCellModal?.classList.remove('hidden');
  ELS.editCellModal?.setAttribute('aria-hidden', 'false');

  setTimeout(() => ELS.editCellPrice?.focus(), 50);
}

export function closeEditModal() {
  S.editing.open = false;
  ELS.editCellModal?.classList.add('hidden');
  ELS.editCellModal?.setAttribute('aria-hidden', 'true');
}

export function openBookingFormCreate({ apartmentUnitId, arrival, departure } = {}) {
  if (!S.isPmsEditor) return;

  S.bookingForm = {
    open: true,
    mode: 'create',
    bookingId: null,
    submitting: false,
    error: '',
    conflictDetails: null,
  };

  populateApartmentDropdown();
  if (ELS.bookingFormApartment) {
    ELS.bookingFormApartment.disabled = false;
    ELS.bookingFormApartment.value = apartmentUnitId || '';
  }
  if (ELS.bookingFormArrival) ELS.bookingFormArrival.value = arrival || '';
  if (ELS.bookingFormDeparture) ELS.bookingFormDeparture.value = departure || '';
  if (ELS.bookingFormAdults) ELS.bookingFormAdults.value = '2';
  if (ELS.bookingFormChildren) ELS.bookingFormChildren.value = '0';
  if (ELS.bookingFormFirstName) ELS.bookingFormFirstName.value = '';
  if (ELS.bookingFormLastName) ELS.bookingFormLastName.value = '';
  if (ELS.bookingFormEmail) ELS.bookingFormEmail.value = '';
  if (ELS.bookingFormPhone) ELS.bookingFormPhone.value = '';
  if (ELS.bookingFormPrice) ELS.bookingFormPrice.value = '';
  if (ELS.bookingFormNotes) ELS.bookingFormNotes.value = '';
  if (ELS.bookingFormStatusRow) ELS.bookingFormStatusRow.classList.remove('hidden');
  if (ELS.bookingFormStatus) ELS.bookingFormStatus.value = 'confirmed';
  if (ELS.bookingFormTitle) ELS.bookingFormTitle.textContent = 'Nuova booking';
  if (ELS.bookingFormSubmit) {
    ELS.bookingFormSubmit.textContent = 'Crea booking';
    ELS.bookingFormSubmit.disabled = false;
  }

  hideBookingErrors();
  ELS.bookingFormModal?.classList.remove('hidden');
  ELS.bookingFormModal?.setAttribute('aria-hidden', 'false');
  setTimeout(() => ELS.bookingFormFirstName?.focus(), 50);
}

export function openBookingFormUpdate(booking) {
  if (!S.isPmsEditor || !booking) return;

  S.bookingForm = {
    open: true,
    mode: 'update',
    bookingId: booking.beds24_booking_id,
    submitting: false,
    error: '',
    conflictDetails: null,
  };

  populateApartmentDropdown();
  if (ELS.bookingFormApartment) {
    ELS.bookingFormApartment.value = booking.apartment_unit_id || '';
    ELS.bookingFormApartment.disabled = true;
  }
  if (ELS.bookingFormArrival) ELS.bookingFormArrival.value = booking.check_in?.slice(0, 10) || '';
  if (ELS.bookingFormDeparture) ELS.bookingFormDeparture.value = booking.check_out?.slice(0, 10) || '';
  if (ELS.bookingFormAdults) ELS.bookingFormAdults.value = String(booking.num_adults || 2);
  if (ELS.bookingFormChildren) ELS.bookingFormChildren.value = String(booking.num_children || 0);
  if (ELS.bookingFormFirstName) ELS.bookingFormFirstName.value = booking.guest_first_name || '';
  if (ELS.bookingFormLastName) ELS.bookingFormLastName.value = booking.guest_last_name || '';
  if (ELS.bookingFormEmail) ELS.bookingFormEmail.value = booking.guest_email || '';
  if (ELS.bookingFormPhone) ELS.bookingFormPhone.value = booking.guest_phone || '';
  if (ELS.bookingFormPrice) ELS.bookingFormPrice.value = booking.total_price != null ? String(booking.total_price) : '';
  if (ELS.bookingFormNotes) {
    ELS.bookingFormNotes.value = booking.raw_payload?.last_manual_request?.notes
      || booking.raw_payload?.manual_request?.notes
      || booking.raw_payload?.notes
      || '';
  }
  if (ELS.bookingFormStatusRow) ELS.bookingFormStatusRow.classList.add('hidden');
  if (ELS.bookingFormTitle) ELS.bookingFormTitle.textContent = `Modifica booking #${booking.beds24_booking_id}`;
  if (ELS.bookingFormSubmit) {
    ELS.bookingFormSubmit.textContent = 'Salva modifiche';
    ELS.bookingFormSubmit.disabled = false;
  }

  hideBookingErrors();
  ELS.bookingFormModal?.classList.remove('hidden');
  ELS.bookingFormModal?.setAttribute('aria-hidden', 'false');
  setTimeout(() => ELS.bookingFormFirstName?.focus(), 50);
}

export function closeBookingForm() {
  S.bookingForm.open = false;
  ELS.bookingFormModal?.classList.add('hidden');
  ELS.bookingFormModal?.setAttribute('aria-hidden', 'true');
}

function populateApartmentDropdown() {
  if (!ELS.bookingFormApartment) return;

  const byCity = new Map();
  for (const apartment of S.apartments) {
    const units = S.unitsByApartment.get(String(apartment.id)) || [];
    for (const unit of units) {
      const city = apartment.city || 'Altre';
      if (!byCity.has(city)) byCity.set(city, []);
      byCity.get(city).push({ apartment, unit });
    }
  }

  const cities = [...byCity.keys()].sort(compareCityNames);
  let html = '<option value="">Seleziona apartment unit…</option>';
  for (const city of cities) {
    html += `<optgroup label="${escapeHtml(city)}">`;
    const items = byCity.get(city).slice().sort((a, b) => {
      const left = `${a.apartment.displayName || ''} ${a.unit.unit_label || ''}`;
      const right = `${b.apartment.displayName || ''} ${b.unit.unit_label || ''}`;
      return left.localeCompare(right, 'it');
    });
    for (const item of items) {
      const label = `${item.apartment.displayName || '—'}${item.unit.unit_label ? ` — ${item.unit.unit_label}` : ''}`;
      html += `<option value="${escapeHtml(item.unit.id)}">${escapeHtml(label)}</option>`;
    }
    html += '</optgroup>';
  }
  ELS.bookingFormApartment.innerHTML = html;
}

function hideBookingErrors() {
  if (ELS.bookingFormConflict) ELS.bookingFormConflict.classList.add('hidden');
  if (ELS.bookingFormConflictList) ELS.bookingFormConflictList.innerHTML = '';
  if (ELS.bookingFormError) {
    ELS.bookingFormError.classList.add('hidden');
    ELS.bookingFormError.textContent = '';
  }
}

function showBookingError(message) {
  if (!ELS.bookingFormError) return;
  ELS.bookingFormError.textContent = message;
  ELS.bookingFormError.classList.remove('hidden');
}

function showBookingConflict(conflicts) {
  if (!ELS.bookingFormConflict || !ELS.bookingFormConflictList) return;
  ELS.bookingFormConflictList.innerHTML = conflicts.map((conflict) => (
    `<li>Booking #${escapeHtml(conflict.beds24_booking_id)}: ${escapeHtml(conflict.arrival)} → ${escapeHtml(conflict.departure)}</li>`
  )).join('');
  ELS.bookingFormConflict.classList.remove('hidden');
}

export async function handleEditCellSubmit(event) {
  event.preventDefault();
  if (!S.isPmsEditor || !S.editing.apartment_unit_id || !S.editing.date) return;
  if (S.editing.submitting) return;

  const priceRaw = ELS.editCellPrice?.value?.trim();
  const minStayRaw = ELS.editCellMinStay?.value?.trim();
  const statusRaw = ELS.editCellStatus?.value;

  const change = {
    apartment_unit_id: S.editing.apartment_unit_id,
    date_from: S.editing.date,
    date_to: S.editing.date,
  };

  if (priceRaw !== '' && priceRaw !== undefined) {
    const price = Number(priceRaw);
    if (Number.isFinite(price) && price >= 0) change.price = price;
  }
  if (minStayRaw !== '' && minStayRaw !== undefined) {
    const minStay = Number(minStayRaw);
    if (Number.isFinite(minStay) && minStay >= 1) change.min_stay = minStay;
  }
  if (statusRaw === 'open') change.closed = false;
  else if (statusRaw === 'closed') change.closed = true;

  if (change.price === undefined && change.min_stay === undefined && change.closed === undefined) {
    showEditError('Inserisci almeno un valore da modificare');
    return;
  }

  S.editing.submitting = true;
  if (ELS.editCellSubmit) {
    ELS.editCellSubmit.disabled = true;
    ELS.editCellSubmit.textContent = 'Salvando…';
  }
  if (ELS.editCellError) ELS.editCellError.classList.add('hidden');

  try {
    const token = S.session?.access_token;
    if (!token) throw new Error('Sessione non valida');

    const response = await fetch('/.netlify/functions/beds24-push-calendar', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({ changes: [change] }),
    });
    const payload = await response.json().catch(() => ({}));

    if (!response.ok) {
      const errMsg = payload.error || `Errore ${response.status}`;
      const details = Array.isArray(payload.details) ? `: ${payload.details.join(', ')}` : '';
      throw new Error(errMsg + details);
    }
    if (!payload.success) {
      throw new Error('Salvataggio non riuscito');
    }

    const cacheKey = `${change.apartment_unit_id}:${change.date_from}`;
    const existing = S.calendarDayByUnitDate.get(cacheKey) || {};
    const updated = {
      ...existing,
      apartment_unit_id: change.apartment_unit_id,
      date: change.date_from,
    };
    if (change.price !== undefined) updated.price = change.price;
    if (change.min_stay !== undefined) updated.min_stay = change.min_stay;
    if (change.closed !== undefined) updated.closed = change.closed;
    S.calendarDayByUnitDate.set(cacheKey, updated);

    closeEditModal();
    RENDER.renderAll?.();
  } catch (error) {
    console.error('[EDIT-FAIL]', error);
    showEditError(error.message || 'Errore di rete');
    if (ELS.editCellSubmit) {
      ELS.editCellSubmit.disabled = false;
      ELS.editCellSubmit.textContent = 'Salva su Beds24';
    }
  } finally {
    S.editing.submitting = false;
  }
}

function showEditError(message) {
  if (!ELS.editCellError) return;
  ELS.editCellError.textContent = message;
  ELS.editCellError.classList.remove('hidden');
}

export async function handleBookingFormSubmit(event) {
  event.preventDefault();
  if (S.bookingForm.submitting) return;

  hideBookingErrors();

  const apartmentUnitId = ELS.bookingFormApartment?.value || '';
  const arrival = ELS.bookingFormArrival?.value || '';
  const departure = ELS.bookingFormDeparture?.value || '';
  const numAdult = Number(ELS.bookingFormAdults?.value) || 0;
  const numChild = Number(ELS.bookingFormChildren?.value) || 0;
  const firstName = (ELS.bookingFormFirstName?.value || '').trim();
  const lastName = (ELS.bookingFormLastName?.value || '').trim();
  const email = (ELS.bookingFormEmail?.value || '').trim();
  const phone = (ELS.bookingFormPhone?.value || '').trim();
  const priceRaw = ELS.bookingFormPrice?.value || '';
  const price = priceRaw !== '' ? Number(priceRaw) : null;
  const notes = (ELS.bookingFormNotes?.value || '').trim();
  const status = (ELS.bookingFormStatus?.value || 'confirmed').trim();

  if (S.bookingForm.mode === 'create' && !apartmentUnitId) {
    showBookingError('Seleziona un apartment');
    return;
  }
  if (!arrival || !departure) {
    showBookingError('Inserisci date check-in e check-out');
    return;
  }
  if (arrival >= departure) {
    showBookingError('Check-in deve essere precedente al check-out');
    return;
  }
  if (numAdult < 1) {
    showBookingError('Almeno 1 adulto richiesto');
    return;
  }
  if (!firstName || !lastName) {
    showBookingError('Nome e cognome obbligatori');
    return;
  }
  if (price != null && (!Number.isFinite(price) || price < 0)) {
    showBookingError('Prezzo non valido');
    return;
  }

  S.bookingForm.submitting = true;
  if (ELS.bookingFormSubmit) {
    ELS.bookingFormSubmit.disabled = true;
    ELS.bookingFormSubmit.textContent = S.bookingForm.mode === 'create' ? 'Creando…' : 'Salvando…';
  }

  try {
    const token = S.session?.access_token;
    if (!token) throw new Error('Sessione non valida');

    const booking = {
      apartment_unit_id: apartmentUnitId,
      arrival,
      departure,
      num_adult: numAdult,
      num_child: numChild,
      first_name: firstName,
      last_name: lastName,
    };
    if (email) booking.email = email;
    if (phone) booking.phone = phone;
    if (price != null) booking.price = price;
    if (notes) booking.notes = notes;

    if (S.bookingForm.mode === 'create') {
      booking.status = status || 'confirmed';
    } else {
      booking.beds24_booking_id = S.bookingForm.bookingId;
    }

    const response = await fetch('/.netlify/functions/beds24-push-booking', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({
        operation: S.bookingForm.mode,
        booking,
      }),
    });
    const result = await response.json().catch(() => ({}));

    if (response.status === 409 && Array.isArray(result.conflicting_bookings)) {
      showBookingConflict(result.conflicting_bookings);
      showBookingError('Le date selezionate si sovrappongono ad altre booking esistenti');
      return;
    }
    if (!response.ok) {
      const errMsg = result.error || `Errore ${response.status}`;
      const details = Array.isArray(result.details) ? `: ${result.details.join(', ')}` : '';
      throw new Error(errMsg + details);
    }
    if (!result.success) {
      throw new Error('Salvataggio non riuscito');
    }

    await reloadCurrentMonth();
    closeBookingForm();
    alert(
      S.bookingForm.mode === 'create'
        ? `Booking creata. ID Beds24: ${result.beds24_booking_id}`
        : 'Modifiche salvate',
    );
  } catch (error) {
    console.error('[BOOKING-FORM-FAIL]', error);
    showBookingError(error.message || 'Errore di rete');
  } finally {
    S.bookingForm.submitting = false;
    if (ELS.bookingFormSubmit) {
      ELS.bookingFormSubmit.disabled = false;
      ELS.bookingFormSubmit.textContent = S.bookingForm.mode === 'create' ? 'Crea booking' : 'Salva modifiche';
    }
  }
}

async function reloadCurrentMonth() {
  try {
    await ensureDataLoaded({ monthOnly: true });
    RENDER.renderAll?.();
    RENDER.closeDrawer?.();
  } catch (error) {
    console.warn('[RELOAD-FAIL]', error);
  }
}

export function openBulkModal() {
  if (!S.isPmsEditor) return;

  S.bulk.open = true;
  S.bulk.selectedApartmentIds = new Set();
  S.bulk.dateFrom = null;
  S.bulk.dateTo = null;
  S.bulk.onlyWeekends = false;
  S.bulk.priceMode = 'none';
  S.bulk.priceValue = '';
  S.bulk.minStayMode = 'none';
  S.bulk.minStayValue = '';
  S.bulk.availabilityMode = 'none';
  S.bulk.submitting = false;
  S.bulk.error = '';
  S.bulk.apartmentFilter = '';

  updateBulkApartmentList();

  if (ELS.bulkApartmentFilter) ELS.bulkApartmentFilter.value = '';
  if (ELS.bulkDateFrom) ELS.bulkDateFrom.value = '';
  if (ELS.bulkDateTo) ELS.bulkDateTo.value = '';
  if (ELS.bulkOnlyWeekends) ELS.bulkOnlyWeekends.checked = false;
  document.querySelectorAll('input[name="bulkPriceMode"]').forEach((radio) => {
    radio.checked = radio.value === 'none';
  });
  if (ELS.bulkPriceSet) {
    ELS.bulkPriceSet.value = '';
    ELS.bulkPriceSet.disabled = true;
  }
  if (ELS.bulkPriceDelta) {
    ELS.bulkPriceDelta.value = '';
    ELS.bulkPriceDelta.disabled = true;
  }
  if (ELS.bulkPricePercent) {
    ELS.bulkPricePercent.value = '';
    ELS.bulkPricePercent.disabled = true;
  }
  if (ELS.bulkMinStayMode) ELS.bulkMinStayMode.value = 'none';
  if (ELS.bulkMinStayValue) {
    ELS.bulkMinStayValue.value = '';
    ELS.bulkMinStayValue.disabled = true;
  }
  if (ELS.bulkAvailabilityMode) ELS.bulkAvailabilityMode.value = 'none';
  if (ELS.bulkError) {
    ELS.bulkError.classList.add('hidden');
    ELS.bulkError.textContent = '';
  }
  if (ELS.bulkSubmit) {
    ELS.bulkSubmit.disabled = true;
    ELS.bulkSubmit.textContent = 'Applica modifica';
  }

  updateBulkPreview();
  ELS.bulkModal?.classList.remove('hidden');
  ELS.bulkModal?.setAttribute('aria-hidden', 'false');
}

export function closeBulkModal() {
  S.bulk.open = false;
  ELS.bulkModal?.classList.add('hidden');
  ELS.bulkModal?.setAttribute('aria-hidden', 'true');
}

export function updateBulkApartmentList() {
  if (!ELS.bulkApartmentList) return;
  const filter = (ELS.bulkApartmentFilter?.value || '').toLowerCase().trim();
  S.bulk.apartmentFilter = filter;

  const byCity = new Map();
  for (const apartment of getFilteredBulkApartments(filter)) {
    const city = apartment.city || 'Altre';
    if (!byCity.has(city)) byCity.set(city, []);
    byCity.get(city).push(apartment);
  }

  const cities = [...byCity.keys()].sort(compareCityNames);
  let html = '';
  for (const city of cities) {
    html += `<div class="bulk-apartment-city">${escapeHtml(city)}</div>`;
    const apartments = byCity
      .get(city)
      .slice()
      .sort((a, b) => String(a.displayName || '').localeCompare(String(b.displayName || ''), 'it'));
    for (const apartment of apartments) {
      const apartmentId = String(apartment.id);
      const checked = S.bulk.selectedApartmentIds.has(apartmentId) ? 'checked' : '';
      html += `
        <label class="bulk-apartment-item">
          <input type="checkbox" data-apartment-id="${escapeHtml(apartmentId)}" ${checked}>
          <span>${escapeHtml(apartment.displayName || '—')}</span>
        </label>
      `;
    }
  }
  ELS.bulkApartmentList.innerHTML = html || '<p class="bulk-preview-empty">Nessun apartment trovato.</p>';

  ELS.bulkApartmentList.querySelectorAll('input[type="checkbox"]').forEach((input) => {
    input.addEventListener('change', () => {
      const apartmentId = input.getAttribute('data-apartment-id');
      if (!apartmentId) return;
      if (input.checked) S.bulk.selectedApartmentIds.add(apartmentId);
      else S.bulk.selectedApartmentIds.delete(apartmentId);
      updateBulkApartmentsCount();
      updateBulkPreview();
    });
  });

  updateBulkApartmentsCount();
}

function updateBulkApartmentsCount() {
  if (!ELS.bulkApartmentsCount) return;
  const count = S.bulk.selectedApartmentIds.size;
  ELS.bulkApartmentsCount.textContent = `${count} apartment${count === 1 ? '' : 's'} selezionati`;
}

function bulkToggleAll(checked) {
  S.bulk.selectedApartmentIds.clear();
  if (checked) {
    const filter = (ELS.bulkApartmentFilter?.value || '').toLowerCase().trim();
    getFilteredBulkApartments(filter).forEach((apartment) => {
      S.bulk.selectedApartmentIds.add(String(apartment.id));
    });
  }
  updateBulkApartmentList();
  updateBulkPreview();
}

function updateBulkPriceMode(event) {
  const mode = event?.target?.value
    || document.querySelector('input[name="bulkPriceMode"]:checked')?.value
    || 'none';
  S.bulk.priceMode = mode;
  if (ELS.bulkPriceSet) ELS.bulkPriceSet.disabled = mode !== 'set';
  if (ELS.bulkPriceDelta) ELS.bulkPriceDelta.disabled = mode !== 'delta';
  if (ELS.bulkPricePercent) ELS.bulkPricePercent.disabled = mode !== 'percent';
  updateBulkPreview();
}

function updateBulkMinStayMode(event) {
  const mode = event?.target?.value || ELS.bulkMinStayMode?.value || 'none';
  S.bulk.minStayMode = mode;
  if (ELS.bulkMinStayValue) ELS.bulkMinStayValue.disabled = mode !== 'set';
  updateBulkPreview();
}

function computeBulkSelection() {
  const apartmentIds = [...S.bulk.selectedApartmentIds];
  const dateFromStr = ELS.bulkDateFrom?.value || '';
  const dateToStr = ELS.bulkDateTo?.value || '';
  const onlyWeekends = !!ELS.bulkOnlyWeekends?.checked;

  if (!apartmentIds.length || !dateFromStr || !dateToStr || dateFromStr > dateToStr) {
    return { cells: [], skippedBookings: 0, units: [], dates: [] };
  }

  const dates = [];
  for (let cursor = parseIsoDateUtc(dateFromStr), end = parseIsoDateUtc(dateToStr); cursor <= end; cursor = addDaysUtc(cursor, 1)) {
    const dayIdx = cursor.getUTCDay();
    if (onlyWeekends && dayIdx !== 0 && dayIdx !== 6) continue;
    dates.push(formatIsoDateUtc(cursor));
  }

  const units = [];
  apartmentIds.forEach((apartmentId) => {
    const apartmentUnits = S.unitsByApartment.get(String(apartmentId)) || [];
    units.push(...apartmentUnits);
  });

  const cells = [];
  let skippedBookings = 0;
  for (const unit of units) {
    const unitBookings = S.bookingsByUnit.get(String(unit.id)) || [];
    for (const date of dates) {
      const hasBooking = unitBookings.some((booking) => booking.check_in <= date && booking.check_out > date);
      if (hasBooking) {
        skippedBookings += 1;
        continue;
      }
      cells.push({ unit, date });
    }
  }

  return { cells, skippedBookings, units, dates };
}

function updateBulkPreview() {
  if (!ELS.bulkPreview) return;

  const { cells, skippedBookings, dates } = computeBulkSelection();
  S.bulk.dateFrom = ELS.bulkDateFrom?.value || null;
  S.bulk.dateTo = ELS.bulkDateTo?.value || null;
  S.bulk.onlyWeekends = !!ELS.bulkOnlyWeekends?.checked;
  if (ELS.bulkDatesCount) {
    ELS.bulkDatesCount.textContent = `${dates.length} giorn${dates.length === 1 ? 'o' : 'i'}`;
  }

  const priceMode = S.bulk.priceMode;
  let priceConfigured = false;
  let priceVal = null;
  if (priceMode === 'set') {
    priceVal = Number(ELS.bulkPriceSet?.value);
    priceConfigured = Number.isFinite(priceVal) && priceVal >= 0;
  } else if (priceMode === 'delta') {
    priceVal = Number(ELS.bulkPriceDelta?.value);
    priceConfigured = Number.isFinite(priceVal) && priceVal !== 0;
  } else if (priceMode === 'percent') {
    priceVal = Number(ELS.bulkPricePercent?.value);
    priceConfigured = Number.isFinite(priceVal) && priceVal !== 0;
  }

  const minStayMode = ELS.bulkMinStayMode?.value || 'none';
  let minStayVal = null;
  let minStayConfigured = false;
  if (minStayMode === 'set') {
    minStayVal = Number(ELS.bulkMinStayValue?.value);
    minStayConfigured = Number.isFinite(minStayVal) && minStayVal >= 1;
  }

  const availMode = ELS.bulkAvailabilityMode?.value || 'none';
  const availConfigured = availMode === 'open' || availMode === 'closed';
  const anyAction = priceConfigured || minStayConfigured || availConfigured;

  if (!cells.length || !anyAction) {
    ELS.bulkPreview.innerHTML = '<p class="bulk-preview-empty">Compila i campi sopra per vedere l\'anteprima</p>';
    ELS.bulkPreview.classList.remove('over-limit');
    if (ELS.bulkSubmit) {
      ELS.bulkSubmit.disabled = true;
      ELS.bulkSubmit.textContent = 'Applica modifica';
    }
    return;
  }

  const overLimit = cells.length > 500;
  let avgInfo = '';
  if (priceConfigured) {
    const samples = cells
      .slice(0, 100)
      .map(({ unit, date }) => S.calendarDayByUnitDate.get(`${unit.id}:${date}`)?.price)
      .filter((value) => value != null);
    if (samples.length) {
      const avgCurrent = samples.reduce((sum, value) => sum + Number(value), 0) / samples.length;
      let newAvg = null;
      if (priceMode === 'set') newAvg = priceVal;
      else if (priceMode === 'delta') newAvg = avgCurrent + priceVal;
      else if (priceMode === 'percent') newAvg = avgCurrent * (1 + priceVal / 100);
      if (newAvg != null) {
        avgInfo = `
          <div class="bulk-preview-stat"><span>Prezzo medio attuale:</span><strong>€${avgCurrent.toFixed(2)}</strong></div>
          <div class="bulk-preview-stat"><span>Prezzo medio stimato:</span><strong>€${newAvg.toFixed(2)}</strong></div>
        `;
      }
    }
  }

  ELS.bulkPreview.innerHTML = `
    <div class="bulk-preview-stat"><span>Apartment selezionati:</span><strong>${S.bulk.selectedApartmentIds.size}</strong></div>
    <div class="bulk-preview-stat"><span>Giorni nel range:</span><strong>${dates.length}</strong></div>
    <div class="bulk-preview-stat"><span>Celle da modificare:</span><strong>${cells.length}</strong></div>
    <div class="bulk-preview-stat"><span>Celle con booking (skip):</span><strong>${skippedBookings}</strong></div>
    ${avgInfo}
    ${overLimit ? '<p style="color:#a32; font-weight:600; margin-top:10px;">Troppe celle. Massimo 500 per operazione. Riduci la selezione.</p>' : ''}
  `;
  ELS.bulkPreview.classList.toggle('over-limit', overLimit);
  if (ELS.bulkSubmit) {
    ELS.bulkSubmit.disabled = overLimit;
    ELS.bulkSubmit.textContent = 'Applica modifica';
  }
}

export async function handleBulkSubmit() {
  if (S.bulk.submitting) return;

  const { cells } = computeBulkSelection();
  if (!cells.length || cells.length > 500) return;

  const priceMode = S.bulk.priceMode;
  const priceVal = priceMode === 'set'
    ? Number(ELS.bulkPriceSet?.value)
    : priceMode === 'delta'
      ? Number(ELS.bulkPriceDelta?.value)
      : priceMode === 'percent'
        ? Number(ELS.bulkPricePercent?.value)
        : null;
  const minStayMode = ELS.bulkMinStayMode?.value || 'none';
  const minStayVal = minStayMode === 'set' ? Number(ELS.bulkMinStayValue?.value) : null;
  const availMode = ELS.bulkAvailabilityMode?.value || 'none';

  const changes = cells.map(({ unit, date }) => {
    const cached = S.calendarDayByUnitDate.get(`${unit.id}:${date}`);
    const change = {
      apartment_unit_id: unit.id,
      date_from: date,
      date_to: date,
    };
    if (priceMode === 'set' && Number.isFinite(priceVal) && priceVal >= 0) {
      change.price = priceVal;
    } else if (priceMode === 'delta' && Number.isFinite(priceVal) && cached?.price != null) {
      change.price = Math.max(0, Number(cached.price) + priceVal);
    } else if (priceMode === 'percent' && Number.isFinite(priceVal) && cached?.price != null) {
      change.price = Math.max(0, Math.round(Number(cached.price) * (1 + priceVal / 100) * 100) / 100);
    }
    if (minStayMode === 'set' && Number.isFinite(minStayVal) && minStayVal >= 1) {
      change.min_stay = minStayVal;
    }
    if (availMode === 'open') change.closed = false;
    else if (availMode === 'closed') change.closed = true;
    return change;
  });

  const validChanges = changes.filter((change) => (
    change.price !== undefined
    || change.min_stay !== undefined
    || change.closed !== undefined
  ));
  if (!validChanges.length) {
    showBulkError('Nessuna modifica valida (le celle selezionate potrebbero non avere prezzo per applicare delta/%)');
    return;
  }

  S.bulk.submitting = true;
  if (ELS.bulkSubmit) {
    ELS.bulkSubmit.disabled = true;
    ELS.bulkSubmit.textContent = `Salvando ${validChanges.length} celle…`;
  }
  if (ELS.bulkError) ELS.bulkError.classList.add('hidden');

  try {
    const token = S.session?.access_token;
    if (!token) throw new Error('Sessione non valida');

    const response = await fetch('/.netlify/functions/beds24-push-calendar', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({ changes: validChanges }),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      const errMsg = payload.error || `Errore ${response.status}`;
      const details = Array.isArray(payload.details) ? `: ${payload.details.join(', ')}` : '';
      throw new Error(errMsg + details);
    }
    if (!payload.success) {
      throw new Error('Salvataggio non riuscito');
    }

    validChanges.forEach((change) => {
      const key = `${change.apartment_unit_id}:${change.date_from}`;
      const existing = S.calendarDayByUnitDate.get(key) || {};
      const updated = {
        ...existing,
        apartment_unit_id: change.apartment_unit_id,
        date: change.date_from,
      };
      if (change.price !== undefined) updated.price = change.price;
      if (change.min_stay !== undefined) updated.min_stay = change.min_stay;
      if (change.closed !== undefined) updated.closed = change.closed;
      S.calendarDayByUnitDate.set(key, updated);
    });

    closeBulkModal();
    clearDragSelection();
    alert(`Modifica massiva completata: ${validChanges.length} celle aggiornate.`);
    RENDER.renderAll?.();
  } catch (error) {
    console.error('[BULK-FAIL]', error);
    showBulkError(error.message || 'Errore di rete');
    if (ELS.bulkSubmit) {
      ELS.bulkSubmit.disabled = false;
      ELS.bulkSubmit.textContent = 'Applica modifica';
    }
  } finally {
    S.bulk.submitting = false;
  }
}

function showBulkError(message) {
  if (!ELS.bulkError) return;
  ELS.bulkError.textContent = message;
  ELS.bulkError.classList.remove('hidden');
}

export function clearDragSelection() {
  S.dragSelection.selectedCells.clear();
  S.dragSelection.active = false;
  S.dragSelection.startCell = null;
  S.dragSelection.endCell = null;
  document.querySelectorAll('.day-cell.drag-selected').forEach((cell) => cell.classList.remove('drag-selected'));
  syncDragSelectionToolbar();
}

export function syncDragSelectionToolbar() {
  const count = S.dragSelection.selectedCells.size;
  if (count > 0) {
    ELS.dragSelectToolbar?.classList.remove('hidden');
    if (ELS.dragSelectCount) {
      ELS.dragSelectCount.textContent = `${count} cell${count === 1 ? 'a' : 'e'} selezionat${count === 1 ? 'a' : 'e'}`;
    }
  } else {
    ELS.dragSelectToolbar?.classList.add('hidden');
  }
}

function openBulkFromSelection() {
  if (!S.dragSelection.selectedCells.size) return;

  const apartmentIds = new Set();
  let minDate = null;
  let maxDate = null;
  for (const cellKey of S.dragSelection.selectedCells) {
    const separator = cellKey.indexOf(':');
    const unitId = cellKey.slice(0, separator);
    const date = cellKey.slice(separator + 1);
    const unit = S.unitMap.get(unitId);
    if (unit) apartmentIds.add(String(unit.apartment_id));
    if (!minDate || date < minDate) minDate = date;
    if (!maxDate || date > maxDate) maxDate = date;
  }

  openBulkModal();
  S.bulk.selectedApartmentIds = apartmentIds;
  if (ELS.bulkDateFrom) ELS.bulkDateFrom.value = minDate || '';
  if (ELS.bulkDateTo) ELS.bulkDateTo.value = maxDate || '';
  updateBulkApartmentList();
  updateBulkPreview();
  clearDragSelection();
}

function openBookingFromSelection() {
  if (!S.dragSelection.selectedCells.size) return;

  let apartmentUnitId = null;
  let minDate = null;
  let maxDate = null;
  for (const cellKey of S.dragSelection.selectedCells) {
    const separator = cellKey.indexOf(':');
    const unitId = cellKey.slice(0, separator);
    const date = cellKey.slice(separator + 1);
    if (!apartmentUnitId) apartmentUnitId = unitId;
    if (!minDate || date < minDate) minDate = date;
    if (!maxDate || date > maxDate) maxDate = date;
  }

  const departure = maxDate
    ? formatIsoDateUtc(addDaysUtc(parseIsoDateUtc(maxDate), 1))
    : '';

  clearDragSelection();
  openBookingFormCreate({
    apartmentUnitId,
    arrival: minDate,
    departure,
  });
}

function updatePricesSyncBtn() {
  const btn = ELS.syncPricesBtn;
  if (!btn) return;
  btn.disabled = !!S.syncingPrices;
  btn.textContent = S.syncingPrices ? 'Sync in corso…' : 'Sync prezzi';
}

function getFilteredBulkApartments(filter) {
  return S.apartments.filter((apartment) => {
    const units = S.unitsByApartment.get(String(apartment.id)) || [];
    if (!units.length) return false;
    if (!filter) return true;
    return String(apartment.displayName || '').toLowerCase().includes(filter)
      || String(apartment.city || '').toLowerCase().includes(filter);
  });
}

function compareCityNames(a, b) {
  const aIndex = CITY_PRIORITY.indexOf(a);
  const bIndex = CITY_PRIORITY.indexOf(b);
  if (aIndex !== -1 || bIndex !== -1) {
    if (aIndex === -1) return 1;
    if (bIndex === -1) return -1;
    if (aIndex !== bIndex) return aIndex - bIndex;
  }
  return a.localeCompare(b, 'it');
}

function parseIsoDateUtc(value) {
  return new Date(`${value}T00:00:00Z`);
}

function addDaysUtc(date, days) {
  const next = new Date(date);
  next.setUTCDate(next.getUTCDate() + days);
  return next;
}

function formatIsoDateUtc(date) {
  return date.toISOString().slice(0, 10);
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

export function handleScrollHeaderToggle() {
  const y = window.scrollY;
  let nextVisible = _miniHeaderVisible;

  if (!_miniHeaderVisible && y > SCROLL_THRESHOLD_SHOW) {
    nextVisible = true;
  } else if (_miniHeaderVisible && y < SCROLL_THRESHOLD_HIDE) {
    nextVisible = false;
  }

  if (nextVisible !== _miniHeaderVisible) {
    _miniHeaderVisible = nextVisible;

    ELS.pageHeader?.classList.toggle('compact', nextVisible);
    ELS.miniHeader?.classList.toggle('visible', nextVisible);
    ELS.miniHeader?.setAttribute('aria-hidden', String(!nextVisible));
    if (nextVisible && ELS.miniMonthLabel && S.monthDate) {
      ELS.miniMonthLabel.textContent = ELS.monthPickerBtn?.textContent || '';
    }
  }

  const headerHeight = nextVisible ? 88 : 48;
  document.documentElement.style.setProperty('--timeline-sticky-top', `${headerHeight}px`);
  syncStickyTimelineHeader();
}

export function toggleCityFilter(city, callbacks = {}) {
  const { onLimitOrSame, onChange } = callbacks;
  const selected = S.filters.cities;
  if (selected.has(city)) {
    if (selected.size === 1) {
      onLimitOrSame?.();
      return;
    }
    selected.delete(city);
  } else {
    if (selected.size >= 8) {
      onLimitOrSame?.();
      alert('Puoi selezionare al massimo 8 citta alla volta.');
      return;
    }
    selected.add(city);
  }
  onChange?.();
}

export function toggleSetValue(set, value, rerender) {
  if (set.has(value)) {
    if (set.size === 1) {
      return;
    }
    set.delete(value);
  } else {
    set.add(value);
  }
  rerender?.();
}

function syncMiniHeaderLabel() {
  if (ELS.miniMonthLabel) {
    ELS.miniMonthLabel.textContent = ELS.monthPickerBtn?.textContent || '';
  }
}

function handleTimelineScroll() {
  syncStickyTimelineHeader();
  syncTimelineScrollChrome();
  const marker = ELS.todayMarker;
  if (!marker || !marker.classList.contains('visible')) return;
  const baseLeft = Number(marker.dataset.baseLeft || 0);
  const scrollLeft = ELS.timelineScroll?.scrollLeft || 0;
  marker.style.left = `${baseLeft - scrollLeft}px`;
}

let timelineScrollSyncLocked = false;

function handleTopTimelineScrollbarScroll() {
  if (timelineScrollSyncLocked) return;
  if (!ELS.timelineScroll || !ELS.timelineTopScrollbar) return;
  timelineScrollSyncLocked = true;
  ELS.timelineScroll.scrollLeft = ELS.timelineTopScrollbar.scrollLeft;
  handleTimelineScroll();
  requestAnimationFrame(() => {
    timelineScrollSyncLocked = false;
  });
}

export function syncTimelineScrollChrome({ contentWidth, desiredScrollLeft } = {}) {
  const main = ELS.timelineScroll;
  const top = ELS.timelineTopScrollbar;
  const inner = ELS.timelineTopScrollbarInner;
  if (!main || !top || !inner) return;

  const width = Math.max(
    Number(contentWidth || 0),
    Number(main.scrollWidth || 0),
  );
  inner.style.width = `${width}px`;

  const shouldShow = width > (main.clientWidth + 6);
  top.classList.toggle('hidden', !shouldShow);
  document.documentElement.style.setProperty(
    '--timeline-top-scrollbar-height',
    shouldShow ? `${Math.ceil(top.offsetHeight || 0)}px` : '0px',
  );
  if (!shouldShow) return;

  const nextScrollLeft = Number.isFinite(desiredScrollLeft) ? desiredScrollLeft : main.scrollLeft;
  timelineScrollSyncLocked = true;
  if (Number.isFinite(desiredScrollLeft)) {
    main.scrollLeft = desiredScrollLeft;
  }
  top.scrollLeft = nextScrollLeft;
  requestAnimationFrame(() => {
    timelineScrollSyncLocked = false;
  });
}

export function syncStickyTimelineHeader() {
  const timelineHeader = ELS.timelineHeader;
  const timelineShell = ELS.timelineShell;
  const timelineScroll = ELS.timelineScroll;
  const timelineBody = ELS.timelineBody;
  const timelineTopScrollbar = ELS.timelineTopScrollbar;
  if (!timelineHeader || !timelineShell || !timelineScroll || !timelineBody) return;
  if (!timelineHeader.firstElementChild) return;

  const stickyTop = Number.parseInt(
    getComputedStyle(document.documentElement).getPropertyValue('--timeline-sticky-top').trim(),
    10,
  ) || 48;
  const shellRect = timelineShell.getBoundingClientRect();
  const scrollRect = timelineScroll.getBoundingClientRect();
  const headerHeight = timelineHeader.offsetHeight || 68;
  const shouldFix = shellRect.top <= stickyTop && shellRect.bottom > (stickyTop + headerHeight + 24);

  if (!shouldFix) {
    timelineHeader.classList.remove('is-fixed');
    timelineHeader.style.top = '';
    timelineHeader.style.left = '';
    timelineHeader.style.width = '';
    timelineHeader.style.height = '';
    timelineHeader.style.transform = '';
    timelineBody.style.paddingTop = '';
    const grid = timelineHeader.firstElementChild;
    if (grid) {
      grid.style.transform = '';
    }
    return;
  }

  timelineHeader.classList.add('is-fixed');
  timelineHeader.style.top = `${stickyTop}px`;
  timelineHeader.style.left = `${scrollRect.left}px`;
  timelineHeader.style.width = `${scrollRect.width}px`;
  timelineHeader.style.height = `${headerHeight}px`;
  timelineBody.style.paddingTop = `${headerHeight}px`;

  const grid = timelineHeader.firstElementChild;
  if (grid) {
    grid.style.transform = `translateX(-${timelineScroll.scrollLeft || 0}px)`;
  }
}

if (typeof window !== 'undefined') {
  window._calendarioEditModal = { open: openEditModal };
  window._calendarioBookingForm = window._calendarioBookingForm || {};
  window._calendarioBookingForm.openUpdate = openBookingFormUpdate;
  window._calendarioBookingForm.reloadAndCloseDrawer = reloadCurrentMonth;
}
