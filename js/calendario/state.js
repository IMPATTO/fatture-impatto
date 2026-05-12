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

const now = new Date();

export const S = {
  session: null,
  authReady: false,
  staticLoaded: false,
  loading: false,
  syncing: false,
  syncingPrices: false,
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

export const ELS = {};
