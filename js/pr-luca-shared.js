(function () {
  const LUCA_BEDS24_APARTMENTS = [
    {
      apartmentId: 'efdea905-f375-464b-bbc4-7aaf38bea078',
      propertyId: '324764',
      alias: 'Bovio 25',
      detail: 'Cattolica',
    },
    {
      apartmentId: '23d2c7f4-6367-4a5a-a1e9-25b7e567576f',
      propertyId: '324589',
      alias: 'Bovio 3',
      detail: 'Cattolica',
    },
    {
      apartmentId: '73c3f928-d57e-4457-91de-108500ea4bc9',
      propertyId: '324615',
      alias: 'Amalfi sotto',
      detail: 'Cattolica',
    },
    {
      apartmentId: '53f358dd-408c-465c-adc7-05fd040435bf',
      propertyId: '324584',
      alias: 'Amalfi sopra',
      detail: 'Cattolica',
    },
    {
      apartmentId: 'e223c334-6223-435c-a5a7-668d8b52717d',
      propertyId: '324765',
      alias: 'Cremona davanti',
      detail: 'Cattolica',
    },
    {
      apartmentId: 'a4c433ba-3c5f-404a-a079-416e6b8116c0',
      propertyId: '324617',
      alias: 'Cremona dietro',
      detail: 'Cattolica',
    },
    {
      apartmentId: '909f0eb5-8de9-455e-9db5-278943af5ed2',
      propertyId: '324704',
      alias: 'Rasi Spinelli',
      detail: 'Cattolica',
    },
    {
      apartmentId: '263b0313-26d9-46f5-8932-3b5fe873a5e1',
      propertyId: '324544',
      alias: 'Misano trilocale',
      detail: 'Misano Adriatico',
    },
    {
      apartmentId: '896a4f4b-a821-4e44-b220-6fe32d34facc',
      propertyId: '324585',
      alias: 'Misano bilocale',
      detail: 'Misano Adriatico',
    },
    {
      apartmentId: '77703eec-c49a-4fd6-8372-8e33c8d5de0b',
      propertyId: '324674',
      alias: 'Miramare',
      detail: 'Rimini',
    },
    {
      apartmentId: '4ad996a4-ee53-49e4-bc63-fe3c874378db',
      propertyId: '324752',
      alias: 'Rivazzurra',
      detail: 'Rimini',
    },
    {
      apartmentId: '1b0c48b5-8102-4e6a-bdf5-1ae7fd1fb247',
      propertyId: '324751',
      alias: 'Via Fiume',
      detail: 'Cattolica',
    },
    {
      apartmentId: null,
      propertyId: null,
      alias: "Viale d'Annunzio",
      detail: 'Da collegare a Beds24',
      disabled: true,
    },
  ];

  function esc(value) {
    return String(value ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function todayIso() {
    return new Date().toISOString().slice(0, 10);
  }

  function addDays(dateStr, amount) {
    const d = new Date(`${dateStr}T00:00:00`);
    d.setDate(d.getDate() + amount);
    return d.toISOString().slice(0, 10);
  }

  function startOfMonth(dateStr) {
    const d = new Date(`${dateStr}T00:00:00`);
    return new Date(d.getFullYear(), d.getMonth(), 1).toISOString().slice(0, 10);
  }

  function endOfMonth(dateStr) {
    const d = new Date(`${dateStr}T00:00:00`);
    return new Date(d.getFullYear(), d.getMonth() + 1, 0).toISOString().slice(0, 10);
  }

  function shiftMonth(dateStr, delta) {
    const d = new Date(`${startOfMonth(dateStr)}T00:00:00`);
    return new Date(d.getFullYear(), d.getMonth() + delta, 1).toISOString().slice(0, 10);
  }

  function eachDate(dateFrom, dateToExclusive) {
    const list = [];
    const current = new Date(`${dateFrom}T00:00:00`);
    const end = new Date(`${dateToExclusive}T00:00:00`);
    while (current < end) {
      list.push(current.toISOString().slice(0, 10));
      current.setDate(current.getDate() + 1);
    }
    return list;
  }

  function overlapSpan(dateFrom, dateToExclusive, bookingStart, bookingEnd) {
    const visibleDates = eachDate(dateFrom, dateToExclusive);
    const startIndex = visibleDates.findIndex((date) => date >= bookingStart && date < bookingEnd);
    if (startIndex < 0) return null;
    let span = 0;
    for (const date of visibleDates.slice(startIndex)) {
      if (date >= bookingEnd) break;
      if (date >= bookingStart) span += 1;
    }
    return span ? { startIndex, span } : null;
  }

  function monthLabel(dateStr) {
    const d = new Date(`${dateStr}T00:00:00`);
    return d.toLocaleDateString('it-IT', { month: 'long', year: 'numeric' });
  }

  function fmtDate(dateStr) {
    if (!dateStr) return '—';
    const d = new Date(`${dateStr}T00:00:00`);
    return d.toLocaleDateString('it-IT', { day: '2-digit', month: '2-digit', year: 'numeric' });
  }

  function normalizeResidenceRows(rows) {
    return (rows || [])
      .filter((row) => row.blocked !== true)
      .sort((a, b) => Number(a.sort_order || 0) - Number(b.sort_order || 0))
      .map((row) => ({
        rowKey: `res:${row.id}`,
        scope: 'residence',
        apartmentRef: row.id,
        propertyId: null,
        label: row.name,
        detail: `${row.group_name}${row.external ? ' · esterno' : ''}`,
        disabled: false,
      }));
  }

  function normalizeBeds24Rows(calendarApartments) {
    const byPropertyId = new Map((calendarApartments || []).map((row) => [String(row.beds24_room_id), row]));
    return LUCA_BEDS24_APARTMENTS.map((cfg) => {
      const live = cfg.propertyId ? byPropertyId.get(String(cfg.propertyId)) : null;
      return {
        rowKey: cfg.propertyId ? `b24:${cfg.propertyId}` : `b24:missing:${cfg.alias}`,
        scope: 'beds24',
        apartmentRef: cfg.apartmentId,
        propertyId: cfg.propertyId,
        label: cfg.alias,
        detail: live?.name || cfg.detail,
        disabled: Boolean(cfg.disabled || !cfg.apartmentId || !cfg.propertyId),
      };
    });
  }

  function normalizeResidenceBookings(rows) {
    return (rows || []).map((row) => ({
      id: row.id,
      rowKey: `res:${row.apartment_id}`,
      checkin: row.checkin,
      checkout: row.checkout,
      label: row.name,
      status: row.status || 'confirmed',
      tone:
        row.source === 'checco'
          ? 'checco'
          : row.source === 'pr-luca'
            ? 'luca'
            : 'other',
      kind: 'booking',
      sourceLabel:
        row.source === 'checco'
          ? 'Kekko'
          : row.source === 'pr-luca'
            ? 'Luca'
            : row.source === 'pr-serena'
              ? 'Serena'
              : row.source === 'system'
                ? 'Sistema'
                : 'Altro',
      notes: row.notes || '',
    }));
  }

  function normalizeBeds24Bookings(rows) {
    const allowed = new Set(LUCA_BEDS24_APARTMENTS.filter((row) => row.propertyId).map((row) => String(row.propertyId)));
    return (rows || [])
      .filter((row) => allowed.has(String(row.propertyId || row.roomId || '')))
      .map((row) => ({
        id: String(row.id || `${row.propertyId}-${row.checkIn}-${row.checkOut}`),
        rowKey: `b24:${String(row.propertyId || row.roomId)}`,
        checkin: row.checkIn,
        checkout: row.checkOut,
        label: row.guestName || 'Blocco Beds24',
        status: 'confirmed',
        tone: 'beds24',
        kind: 'booking',
        sourceLabel: row.channel || 'Beds24',
        notes: row.roomName || '',
      }));
  }

  function normalizePendingRequests(rows) {
    return (rows || [])
      .filter((row) => row.status === 'pending')
      .map((row) => ({
        id: row.id,
        rowKey:
          row.scope === 'residence'
            ? `res:${row.apartment_ref}`
            : row.beds24_room_id
              ? `b24:${row.beds24_room_id}`
              : null,
        checkin: row.checkin,
        checkout: row.checkout,
        label: row.customer_name,
        status: 'pending',
        tone: 'luca',
        kind: 'request',
        sourceLabel: 'In attesa',
        notes: row.notes || '',
      }))
      .filter((row) => row.rowKey);
  }

  async function fetchSnapshot(sb, dateFrom, dateToExclusive) {
    const calendarUrl = `/.netlify/functions/get-calendar?dateFrom=${encodeURIComponent(dateFrom)}&dateTo=${encodeURIComponent(dateToExclusive)}`;
    const [residenceRowsRes, residenceBookingsRes, requestsRes, beds24Res] = await Promise.all([
      sb.from('rm_apartments').select('*').order('sort_order'),
      sb.from('rm_bookings').select('*').neq('status', 'rejected').neq('status', 'cancelled').order('checkin'),
      sb.from('partner_booking_requests').select('*').eq('partner', 'luca').neq('status', 'cancelled').order('created_at', { ascending: false }),
      fetch(calendarUrl).then(async (res) => {
        const payload = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(payload.error || 'Errore caricamento calendario Beds24');
        return payload;
      }),
    ]);

    if (residenceRowsRes.error) throw residenceRowsRes.error;
    if (residenceBookingsRes.error) throw residenceBookingsRes.error;
    if (requestsRes.error) throw requestsRes.error;

    const residenceRows = normalizeResidenceRows(residenceRowsRes.data || []);
    const beds24Rows = normalizeBeds24Rows(beds24Res.apartments || []);
    const rows = [...residenceRows, ...beds24Rows];
    const bookings = [
      ...normalizeResidenceBookings(residenceBookingsRes.data || []),
      ...normalizeBeds24Bookings(beds24Res.bookings || []),
      ...normalizePendingRequests(requestsRes.data || []),
    ];

    return {
      rows,
      bookings,
      requests: requestsRes.data || [],
      residenceRows,
      beds24Rows,
      beds24Warnings: Array.isArray(beds24Res.warnings) ? beds24Res.warnings : [],
    };
  }

  function bookingClass(item) {
    const parts = ['booking'];
    parts.push(item.kind === 'request' ? 'is-request' : 'is-booking');
    parts.push(item.tone || 'other');
    parts.push(item.status || 'confirmed');
    return parts.join(' ');
  }

  function renderCalendar(grid, rows, bookings, dateFrom, dateToExclusive) {
    const days = eachDate(dateFrom, dateToExclusive);
    grid.innerHTML = '';
    grid.style.gridTemplateColumns = `240px repeat(${days.length}, minmax(30px, 1fr))`;

    const corner = document.createElement('div');
    corner.className = 'cal-corner';
    corner.textContent = 'Appartamento';
    grid.appendChild(corner);

    days.forEach((date) => {
      const head = document.createElement('div');
      const d = new Date(`${date}T00:00:00`);
      head.className = `cal-head${date === todayIso() ? ' today' : ''}`;
      head.innerHTML = `<span>${d.getDate()}</span><small>${d.toLocaleDateString('it-IT', { weekday: 'short' }).slice(0, 3)}</small>`;
      grid.appendChild(head);
    });

    rows.forEach((row, rowIndex) => {
      const label = document.createElement('div');
      label.className = `cal-label${row.disabled ? ' disabled' : ''}`;
      label.innerHTML = `<strong>${esc(row.label)}</strong><small>${esc(row.detail || '')}</small>`;
      label.style.gridRow = String(rowIndex + 2);
      grid.appendChild(label);

      days.forEach((date) => {
        const cell = document.createElement('div');
        cell.className = `cal-cell${date === todayIso() ? ' today' : ''}`;
        cell.style.gridRow = String(rowIndex + 2);
        grid.appendChild(cell);
      });
    });

    bookings.forEach((booking) => {
      const rowIndex = rows.findIndex((row) => row.rowKey === booking.rowKey);
      if (rowIndex < 0) return;
      const spanInfo = overlapSpan(dateFrom, dateToExclusive, booking.checkin, booking.checkout);
      if (!spanInfo) return;

      const div = document.createElement('button');
      div.type = 'button';
      div.className = bookingClass(booking);
      div.style.gridRow = String(rowIndex + 2);
      div.style.gridColumn = `${spanInfo.startIndex + 2} / span ${spanInfo.span}`;
      div.innerHTML = `<span>${esc(booking.label)}</span><small>${esc(booking.sourceLabel || '')}</small>`;
      div.title = `${booking.label} · ${fmtDate(booking.checkin)} → ${fmtDate(booking.checkout)}${booking.notes ? `\n${booking.notes}` : ''}`;
      grid.appendChild(div);
    });
  }

  window.prLucaShared = {
    LUCA_BEDS24_APARTMENTS,
    esc,
    todayIso,
    addDays,
    startOfMonth,
    endOfMonth,
    shiftMonth,
    monthLabel,
    fmtDate,
    fetchSnapshot,
    renderCalendar,
  };
})();
