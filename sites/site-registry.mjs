export const siteRegistry = {
  'fatt-docs': {
    label: 'Fatturazione + Documenti',
    envVar: 'SITE_URL_FATT_DOCS',
    entryPage: 'backoffice.html',
    pages: [
      'backoffice.html',
      'backoffice-documenti.html',
    ],
    assets: [
      'js/supabase-client.js',
      'comuni.csv',
      'stati.csv',
    ],
    functions: [
      'build-istat-monthly',
      'create-fattura-fic',
      'create-fattura-from-text',
      'export-alloggiati-report',
      'export-istat-marche-xml',
      'save-istat-config',
      'send-alloggiati',
      'transcribe-invoice-audio',
    ],
    includedFiles: [
      'manifest.json',
      'privacy-policy.html',
    ],
  },
  operativita: {
    label: 'Operativita',
    envVar: 'SITE_URL_OPERATIVITA',
    entryPage: 'backoffice-operativita.html',
    pages: [
      'backoffice-operativita.html',
      'backoffice-richieste.html',
      'backoffice-beds24-messaggi.html',
    ],
    assets: [
      'js/supabase-client.js',
      'js/backoffice-operativita.js',
    ],
    functions: [
      'get-backoffice-richieste',
      'get-operativita-apartments',
    ],
  },
  portale: {
    label: 'Portale',
    envVar: 'SITE_URL_PORTALE',
    entryPage: 'index.html',
    includeSites: [
      'residence-pr',
    ],
    pages: [
      'backoffice-portale.html',
      'backoffice-calendario.html',
      'portale.html',
      'index.html',
      'carica-documenti-fattura.html',
    ],
    assets: [
      'js/supabase-client.js',
      'js/calendario.js',
      'css/calendario.css',
    ],
    functions: [
      'apply-apartment-links',
      'audit-apartment-links',
      'beds24-sync-bookings',
      'get-calendar',
      'get-public-portal-data',
      'populate-pms-mappings-and-units',
      'save-istat-config',
      'submit-public-checkin',
      'submit-public-invoice-documents',
      'translate-apartment-info',
    ],
    includedFiles: [
      'manifest.json',
      'privacy-policy.html',
    ],
  },
  contabilita: {
    label: 'Contabilita',
    envVar: 'SITE_URL_CONTABILITA',
    entryPage: 'backoffice-contabilita.html',
    pages: [
      'backoffice-contabilita.html',
      'backoffice-amministrazione-appartamenti.html',
    ],
    assets: [
      'js/supabase-client.js',
    ],
    functions: [
      'create-contract-registration-accounting',
      'create-fattura-fic',
      'create-owner-payment-accounting',
      'inbound-bollette-email',
      'inbound-bollette-telegram',
      'inbound-contabilita-email',
      'monthly-export-commercialista',
      'upload-bollette-manual',
    ],
  },
  calendario: {
    label: 'Calendario',
    envVar: 'SITE_URL_CALENDARIO',
    entryPage: 'backoffice-calendario.html',
    pages: [
      'backoffice-calendario.html',
    ],
    assets: [
      'js/supabase-client.js',
      'js/calendario.js',
      'css/calendario.css',
    ],
    functions: [
      'beds24-inspect',
      'beds24-rate-diagnostics',
      'beds24-sync-bookings',
      'get-calendar',
      'populate-pms-mappings-and-units',
      'update-calendar-inventory',
    ],
  },
  'residence-pr': {
    label: 'Residence + PR',
    envVar: 'SITE_URL_RESIDENCE_PR',
    entryPage: 'residence-backoffice.html',
    pages: [
      'residence-kekko.html',
      'residence-backoffice.html',
      'pr_kekko_riccione.html',
      'pr_luca_riccione.html',
      'backoffice-pr-luca.html',
    ],
    assets: [
      'js/supabase-client.js',
      'js/shared-app-auth.js',
      'js/pr-luca-shared.js',
    ],
    functions: [
      'approve-partner-booking',
      'get-calendar',
      'pr-luca-data',
      'residence-api',
      'shared-app-login',
    ],
  },
};

export function getAllPageOwners() {
  const owners = new Map();
  for (const [siteKey, site] of Object.entries(siteRegistry)) {
    for (const page of site.pages || []) {
      if (!owners.has(page)) {
        owners.set(page, siteKey);
      }
    }
  }
  return owners;
}
