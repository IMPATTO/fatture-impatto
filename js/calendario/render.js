import {
  handleRetryClick,
  registerRenderBindings,
  toggleCityFilter,
  toggleSetValue,
} from './interactions.js';
import { CHANNEL_CONFIG, ELS, S, STATUS_LABELS } from './state.js';
import {
  buildDayCellLabel,
  countBookingsByChannel,
  countBookingsByStatus,
  countOccupiedUnitsForDate,
  countUnavailableUnitsForDate,
  countVisibleApartmentsByCity,
  getAvailableChannels,
  getAvailableCities,
  getAvailableStatuses,
  getDayAvailabilityState,
  getVisibleApartmentRows,
  getVisibleBookings,
  groupVisibleBookingsForMobile,
  groupVisibleRowsByCity,
} from './data.js';
import {
  addDays,
  bookingSpanWithinMonth,
  channelLabel,
  esc,
  formatDate,
  formatDateTime,
  formatMonthLabel,
  formatPrice,
  formatRangeLabel,
  formatWeekday,
  getDayWidth,
  getMonthDays,
  isoDateLocal,
  isSameDate,
  isWeekend,
  minutesAgoLabel,
  nightsBetween,
  titleCase,
} from './utils.js';

export function renderStaticShell() {
  ELS.monthPickerBtn.textContent = formatMonthLabel(S.monthDate);
  ELS.monthPickerInput.value = `${S.monthDate.getFullYear()}-${`${S.monthDate.getMonth() + 1}`.padStart(2, '0')}`;
  ELS.rangeInfo.textContent = formatRangeLabel(S.monthDate);
  if (ELS.miniMonthLabel) {
    ELS.miniMonthLabel.textContent = ELS.monthPickerBtn.textContent;
  }
}

export function renderAll() {
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

export function renderFilters() {
  renderCheckboxGroup({
    target: ELS.cityFilters,
    options: getAvailableCities().map((city) => ({
      value: city,
      label: city,
      count: countVisibleApartmentsByCity(city),
      checked: S.filters.cities.has(city),
      onChange: () => toggleCityFilter(city, {
        onLimitOrSame: renderFilters,
        onChange: renderAll,
      }),
    })),
  });

  renderCheckboxGroup({
    target: ELS.channelFilters,
    options: getAvailableChannels().map((channel) => ({
      value: channel,
      label: channelLabel(channel, CHANNEL_CONFIG),
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

export function renderCheckboxGroup({ target, options }) {
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

export function renderSyncMeta() {
  ELS.lastSyncLabel.textContent = S.lastSync ? `${formatDateTime(S.lastSync)} (${minutesAgoLabel(S.lastSync)})` : 'Nessun sync ancora registrato';
  ELS.refreshBtn.disabled = S.syncing;
  ELS.refreshBtn.innerHTML = S.syncing ? '<span class="spinner" aria-hidden="true"></span> Aggiornamento…' : 'Aggiorna da Beds24';
  if (ELS.miniRefreshBtn) {
    ELS.miniRefreshBtn.disabled = S.syncing;
    ELS.miniRefreshBtn.textContent = S.syncing ? 'Aggiorna…' : 'Aggiorna';
  }
}

export function renderOrphanBanner() {
  if (!S.orphanCount) {
    ELS.orphanBanner.classList.add('hidden');
    ELS.orphanBanner.textContent = '';
    return;
  }
  ELS.orphanBanner.classList.remove('hidden');
  ELS.orphanBanner.innerHTML = `<strong>${S.orphanCount}</strong> prenotazioni in conflitto da risolvere su Beds24`;
}

export function renderStates() {
  ELS.loadingState.classList.toggle('hidden', !S.loading);
  ELS.errorState.classList.toggle('hidden', !S.errorMessage);
  ELS.errorState.innerHTML = S.errorMessage ? `
    <h2>Errore di caricamento</h2>
    <p>${esc(S.errorMessage)}</p>
    <button type="button" class="btn btn-primary" id="retryBtn">Riprova</button>
  ` : '';
  ELS.errorState.querySelector('#retryBtn')?.addEventListener('click', handleRetryClick);

  const visibleRows = getVisibleApartmentRows();
  const shouldShowEmpty = !S.loading && !S.errorMessage && !visibleRows.length;
  ELS.emptyState.classList.toggle('hidden', !shouldShowEmpty);
  ELS.timelineShell.classList.toggle('hidden', shouldShowEmpty || !!S.errorMessage);
  ELS.mobileList.classList.toggle('hidden', shouldShowEmpty || !!S.errorMessage);
}

export function renderTimeline() {
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
  updateTodayMarker(monthDays);
}

export function buildTimelineHeader(monthDays) {
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

export function buildTimelineRow(row, monthDays) {
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

export function buildHeatCells(row, monthDays) {
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

export function buildDayCells(row, monthDays) {
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

export function buildBookingBars(bookings, monthDays) {
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
    const beforeMark = span.continuesBefore ? '<span class="bar-chevron bar-chevron-left">‹</span>' : '';
    const afterMark = span.continuesAfter ? '<span class="bar-chevron bar-chevron-right">›</span>' : '';
    return `
      <button
        type="button"
        class="booking-bar ${channelClass}${soft}${span.continuesBefore ? ' continues-before' : ''}${span.continuesAfter ? ' continues-after' : ''}"
        data-booking-id="${esc(booking.beds24_booking_id)}"
        data-tooltip="${esc(tooltip)}"
        style="left:${left}px;width:${width}px;"
      >
        ${beforeMark}
        <span class="bar-label">${esc(label)}</span>
        ${afterMark}
      </button>
    `;
  }).join('');
}

export function buildBookingTooltip(booking) {
  const price = booking.total_price != null ? `${formatPrice(booking.total_price)} ${booking.currency || 'EUR'}` : 'Prezzo non disponibile';
  return [
    `${booking.guest_last_name || booking.guest_first_name || 'Prenotazione'}`,
    `${formatDate(booking.check_in)} → ${formatDate(booking.check_out)} · ${nightsBetween(booking.check_in, booking.check_out)} notti`,
    `${channelLabel(booking.channel_normalized, CHANNEL_CONFIG)} · ${STATUS_LABELS[booking.status] || booking.status}`,
    price,
  ].join('\n');
}

export function bindTimelineEvents() {
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

export function renderMobileList() {
  const groups = groupVisibleBookingsForMobile();
  ELS.mobileList.innerHTML = groups.length ? groups.map((group) => `
    <article class="mobile-card">
      <h3>${esc(group.apartment.displayName)}</h3>
      <p class="mobile-meta">${esc(group.apartment.city)} · ${esc(group.meta)}</p>
      ${group.bookings.map((booking) => `
        <div class="mobile-booking">
          <span class="pill">${esc(channelLabel(booking.channel_normalized, CHANNEL_CONFIG))}</span>
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

export function renderDrawer() {
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
          ${detailRow('Canale normalizzato', channelLabel(booking.channel_normalized, CHANNEL_CONFIG))}
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

export function detailRow(label, value) {
  return `<div class="detail-row"><dt>${esc(label)}</dt><dd>${esc(value)}</dd></div>`;
}

export function openDrawer(bookingId) {
  S.selectedBookingId = bookingId;
  renderDrawer();
}

export function closeDrawer() {
  S.selectedBookingId = null;
  ELS.bookingDrawer.classList.remove('open');
  ELS.bookingDrawer.setAttribute('aria-hidden', 'true');
  ELS.drawerBackdrop.classList.add('hidden');
}

export function openOrphanModal() {
  ELS.orphanModal.classList.remove('hidden');
  ELS.orphanModal.setAttribute('aria-hidden', 'false');
}

export function closeOrphanModal() {
  ELS.orphanModal.classList.add('hidden');
  ELS.orphanModal.setAttribute('aria-hidden', 'true');
}

export function renderOrphanModalBody() {
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
            <td>${esc(channelLabel(row.channel_normalized, CHANNEL_CONFIG))}</td>
            <td>${esc(STATUS_LABELS[row.status] || row.status)}</td>
            <td>${esc(row.reason || '—')}</td>
          </tr>
        `).join('')}
      </tbody>
    </table>
  `;
}

export function showTooltip(text, event) {
  if (!text) return;
  ELS.tooltip.textContent = text;
  ELS.tooltip.classList.remove('hidden');
  moveTooltip(event);
}

export function moveTooltip(event) {
  if (ELS.tooltip.classList.contains('hidden')) return;
  ELS.tooltip.style.left = `${Math.min(window.innerWidth - 320, event.clientX + 16)}px`;
  ELS.tooltip.style.top = `${Math.min(window.innerHeight - 80, event.clientY + 16)}px`;
}

export function hideTooltip() {
  ELS.tooltip.classList.add('hidden');
}

registerRenderBindings({
  renderAll,
  renderFilters,
  renderStaticShell,
  renderSyncMeta,
  closeDrawer,
  openOrphanModal,
  closeOrphanModal,
});

function updateTodayMarker(monthDays) {
  const marker = ELS.todayMarker;
  if (!marker) return;
  const today = new Date();
  const todayIndex = monthDays.findIndex((date) => isSameDate(date, today));
  if (todayIndex < 0) {
    marker.classList.remove('visible');
    marker.dataset.baseLeft = '';
    return;
  }
  const dayWidth = getDayWidth();
  const labelWidth = parseInt(
    getComputedStyle(document.documentElement).getPropertyValue('--label-width').trim(),
    10
  ) || 320;
  const baseLeft = labelWidth + (todayIndex * dayWidth);
  const scrollLeft = ELS.timelineScroll?.scrollLeft || 0;
  marker.dataset.baseLeft = String(baseLeft);
  marker.style.left = `${baseLeft - scrollLeft}px`;
  marker.classList.add('visible');
}
