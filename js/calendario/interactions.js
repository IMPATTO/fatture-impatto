import {
  getVisibleBookings,
  initializeFilterDefaults,
  loadMonthData,
  loadStaticData,
} from './data.js';
import { ELS, S, SIDEBAR_STORAGE_KEY } from './state.js';
import {
  nightsBetween,
  startOfMonth,
  toMonthInputValue,
} from './utils.js';
import {
  closeDrawer,
  openOrphanModal,
  closeOrphanModal,
} from './render.js';

const RENDER = {
  renderAll: null,
  renderFilters: null,
  renderStaticShell: null,
  renderSyncMeta: null,
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
    'refreshBtn', 'exportBtn', 'lastSyncLabel', 'sidebarToggle', 'pageBody', 'pageHeader', 'sidebar', 'cityFilters', 'channelFilters',
    'miniHeader', 'miniMonthLabel', 'miniPrevMonth', 'miniNextMonth', 'miniRefreshBtn',
    'statusFilters', 'timelineHeader', 'timelineBody', 'timelineShell', 'timelineScroll',
    'mobileList', 'loadingState', 'emptyState', 'errorState', 'orphanBanner',
    'bookingDrawer', 'drawerBackdrop', 'drawerCloseBtn', 'drawerCloseBtnFooter',
    'drawerTitle', 'drawerBody', 'orphanModal', 'orphanModalBody', 'orphanModalTitle', 'todayMarker',
    'orphanModalClose', 'orphanModalCloseFooter', 'tooltip',
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
  ELS.miniRefreshBtn?.addEventListener('click', refreshFromBeds24);
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
  window.addEventListener('scroll', handleScrollHeaderToggle, { passive: true });
  ELS.timelineScroll?.addEventListener('scroll', handleTimelineScroll, { passive: true });
}

const SCROLL_THRESHOLD = 120;

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
    RENDER.renderSyncMeta?.();
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

export function handleScrollHeaderToggle() {
  const scrolled = window.scrollY > SCROLL_THRESHOLD;
  ELS.pageHeader?.classList.toggle('compact', scrolled);
  ELS.miniHeader?.classList.toggle('visible', scrolled);
  ELS.miniHeader?.setAttribute('aria-hidden', String(!scrolled));
  document.documentElement.style.setProperty('--timeline-sticky-top', scrolled ? '88px' : '48px');
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
