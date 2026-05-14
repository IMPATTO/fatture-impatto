import {
  getVisibleBookings,
  initializeFilterDefaults,
  loadMonthData,
  loadStaticData,
} from './data.js';
import { ELS, PMS_EDITOR_EMAILS, S, SIDEBAR_STORAGE_KEY } from './state.js';
import {
  nightsBetween,
  startOfMonth,
  toMonthInputValue,
} from './utils.js';

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
  await import('./render.js');
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
    'refreshBtn', 'syncPricesBtn', 'exportBtn', 'lastSyncLabel', 'sidebarToggle', 'pageBody', 'pageHeader', 'sidebar', 'cityFilters', 'channelFilters',
    'miniHeader', 'miniMonthLabel', 'miniPrevMonth', 'miniNextMonth', 'miniRefreshBtn',
    'statusFilters', 'timelineHeader', 'timelineBody', 'timelineShell', 'timelineScroll',
    'mobileList', 'loadingState', 'emptyState', 'errorState', 'orphanBanner',
    'bookingDrawer', 'drawerBackdrop', 'drawerCloseBtn', 'drawerCloseBtnFooter',
    'drawerTitle', 'drawerBody', 'orphanModal', 'orphanModalBody', 'orphanModalTitle', 'todayMarker',
    'orphanModalClose', 'orphanModalCloseFooter', 'tooltip',
    'editCellModal', 'editCellTitle', 'editCellClose', 'editCellCancel',
    'editCellSubmit', 'editCellForm', 'editCellApartmentLabel', 'editCellDateLabel',
    'editCellPrice', 'editCellMinStay', 'editCellStatus', 'editCellError',
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
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') {
      RENDER.closeDrawer?.();
      RENDER.closeOrphanModal?.();
      closeEditModal();
      closeMenu();
    }
  });
  window.addEventListener('scroll', handleScrollHeaderToggle, { passive: true });
  ELS.timelineScroll?.addEventListener('scroll', handleTimelineScroll, { passive: true });
}

const MINI_HEADER_SHOW_THRESHOLD = 140;
const MINI_HEADER_HIDE_THRESHOLD = 90;
let miniHeaderVisible = false;

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
  RENDER.renderStaticShell?.();
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
  }
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

function updatePricesSyncBtn() {
  const btn = ELS.syncPricesBtn;
  if (!btn) return;
  btn.disabled = !!S.syncingPrices;
  btn.textContent = S.syncingPrices ? 'Sync in corso…' : 'Sync prezzi';
}

export function handleScrollHeaderToggle() {
  const scrollY = window.scrollY;
  if (!miniHeaderVisible && scrollY > MINI_HEADER_SHOW_THRESHOLD) {
    miniHeaderVisible = true;
  } else if (miniHeaderVisible && scrollY < MINI_HEADER_HIDE_THRESHOLD) {
    miniHeaderVisible = false;
  }
  ELS.pageHeader?.classList.toggle('compact', miniHeaderVisible);
  ELS.miniHeader?.classList.toggle('visible', miniHeaderVisible);
  ELS.miniHeader?.setAttribute('aria-hidden', String(!miniHeaderVisible));
  document.documentElement.style.setProperty('--timeline-sticky-top', miniHeaderVisible ? '88px' : '48px');
  syncMiniHeaderLabel();
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
  const marker = ELS.todayMarker;
  if (!marker || !marker.classList.contains('visible')) return;
  const baseLeft = Number(marker.dataset.baseLeft || 0);
  const scrollLeft = ELS.timelineScroll?.scrollLeft || 0;
  marker.style.left = `${baseLeft - scrollLeft}px`;
}

if (typeof window !== 'undefined') {
  window._calendarioEditModal = { open: openEditModal };
}
