export const OCCUPYING_STATUSES = ['confirmed', 'new', 'request', 'black'];

export const STATUS_LABELS = {
  confirmed: 'Confermata',
  new: 'Nuova',
  request: 'Richiesta',
  black: 'Blocco',
  inquiry: 'Richiesta info',
  cancelled: 'Cancellata',
};

export const CITY_PRIORITY = [
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

export const KNOWN_CITY_MAP = [
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

export const CHANNEL_CONFIG = {
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

export const SIDEBAR_STORAGE_KEY = 'calendario:sidebar:collapsed';

export const PMS_EDITOR_EMAILS = new Set([
  'fatturazione@illupoaffitta.com',
  'contabilita@illupoaffitta.com',
  'info@marcovenzon.com',
  'veronica.dieta@gmail.com',
  'jessica.appartamenticaldari@gmail.com',
  'cerulliserena@gmail.com',
]);

const now = new Date();

export const S = {
  session: null,
  authReady: false,
  staticLoaded: false,
  loading: false,
  syncing: false,
  syncingPrices: false,
  isPmsEditor: false,
  monthDate: new Date(now.getFullYear(), now.getMonth(), 1),
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
  calendarDays: [],
  calendarDayByUnitDate: new Map(),
  orphanRows: [],
  orphanCount: 0,
  lastSync: null,
  lastNightlySync: null,
  _skipWarningLogged: false,
  expandedResidences: new Set(),
  collapsedCities: new Set(),
  selectedBookingId: null,
  errorMessage: '',
  editing: {
    open: false,
    apartment_unit_id: null,
    date: null,
    initialPrice: null,
    initialMinStay: null,
    initialClosed: false,
    submitting: false,
    error: '',
  },
  bookingForm: {
    open: false,
    mode: 'create',
    bookingId: null,
    submitting: false,
    error: '',
    conflictDetails: null,
  },
  bulk: {
    open: false,
    selectedApartmentIds: new Set(),
    dateFrom: null,
    dateTo: null,
    onlyWeekends: false,
    priceMode: 'none',
    priceValue: '',
    minStayMode: 'none',
    minStayValue: '',
    availabilityMode: 'none',
    submitting: false,
    error: '',
    apartmentFilter: '',
  },
  dragSelection: {
    active: false,
    startCell: null,
    endCell: null,
    selectedCells: new Set(),
  },
  filters: {
    cities: new Set(),
    channels: new Set(),
    statuses: new Set(OCCUPYING_STATUSES),
    availableNights: 0,
  },
};

export const ELS = {};
