export const sharedFunctionSites = {
  'create-fattura-fic': ['contabilita', 'fatt-docs'],
  'get-calendar': ['calendario', 'residence-pr'],
  'save-istat-config': ['fatt-docs', 'portale'],
};

export const siteRegistry = {
  'fatt-docs': {
    label: 'Fatturazione + Documenti',
    envVar: 'SITE_URL_FATT_DOCS',
    fallbackUrl: 'https://fatture.illupoaffitta.com',
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
    label: 'Richieste + Beds24 + Messaggi',
    envVar: 'SITE_URL_OPERATIVITA',
    fallbackUrl: 'https://operativita.illupoaffitta.com',
    entryPage: 'backoffice-operativita.html',
    pages: [
      'backoffice-operativita.html',
      'backoffice-richieste.html',
      'backoffice-beds24-messaggi.html',
    ],
    assets: [
      'js/site-links.js',
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
    fallbackUrl: 'https://checkin.illupoaffitta.com',
    entryPage: 'index.html',
    pages: [
      'backoffice-portale.html',
      'portale.html',
      'index.html',
      'carica-documenti-fattura.html',
    ],
    assets: [
      'js/site-links.js',
      'js/supabase-client.js',
      'stati.csv',
    ],
    functions: [
      'apply-apartment-links',
      'audit-apartment-links',
      'get-backoffice-portale-data',
      'get-public-portal-data',
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
    fallbackUrl: 'https://contabilita.illupoaffitta.com',
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
    fallbackUrl: 'https://calendario.illupoaffitta.com',
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
      'sync-beds24-calendar-background',
      'update-calendar-inventory',
    ],
  },
  'residence-pr': {
    label: 'PR Kekko + Residence',
    envVar: 'SITE_URL_RESIDENCE_PR',
    fallbackUrl: 'https://checkin.illupoaffitta.com',
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
    ],
    functions: [
      'get-calendar',
      'residence-api',
      'shared-app-login',
    ],
  },
};

function unique(items) {
  return [...new Set((items || []).filter(Boolean))];
}

function formatDuplicateMap(title, duplicates) {
  const lines = duplicates.map(([name, owners]) => `- ${name}: ${owners.join(', ')}`);
  return `${title}\n${lines.join('\n')}`;
}

function collectOwners(field) {
  const owners = new Map();
  for (const [siteKey, site] of Object.entries(siteRegistry)) {
    for (const value of unique(site[field])) {
      const currentOwners = owners.get(value) || [];
      currentOwners.push(siteKey);
      owners.set(value, currentOwners);
    }
  }
  return owners;
}

function sameMembers(left, right) {
  if (left.length !== right.length) return false;
  return left.every((value, index) => value === right[index]);
}

export function assertValidSiteRegistry() {
  const errors = [];
  const pageOwners = collectOwners('pages');
  const functionOwners = collectOwners('functions');

  for (const [siteKey, site] of Object.entries(siteRegistry)) {
    if ((site.includeSites || []).length) {
      errors.push(`- ${siteKey}: includeSites non e piu supportato`);
    }

    if (!site.entryPage) {
      errors.push(`- ${siteKey}: entryPage mancante`);
    } else if (!(site.pages || []).includes(site.entryPage)) {
      errors.push(`- ${siteKey}: entryPage "${site.entryPage}" non inclusa nelle pages`);
    }
  }

  const duplicatePages = [...pageOwners.entries()]
    .filter(([, owners]) => owners.length > 1)
    .sort(([left], [right]) => left.localeCompare(right));
  if (duplicatePages.length) {
    errors.push(formatDuplicateMap('Collisioni pages', duplicatePages));
  }

  const disallowedDuplicateFunctions = [...functionOwners.entries()]
    .filter(([, owners]) => owners.length > 1)
    .filter(([fnName, owners]) => {
      const allowedOwners = sharedFunctionSites[fnName];
      if (!allowedOwners) return true;
      return !sameMembers([...owners].sort(), [...allowedOwners].sort());
    })
    .sort(([left], [right]) => left.localeCompare(right));
  if (disallowedDuplicateFunctions.length) {
    errors.push(formatDuplicateMap('Collisioni functions non autorizzate', disallowedDuplicateFunctions));
  }

  for (const [fnName, allowedOwners] of Object.entries(sharedFunctionSites)) {
    const actualOwners = functionOwners.get(fnName);
    if (!actualOwners) {
      errors.push(`- shared function "${fnName}" dichiarata ma assente dal registry`);
      continue;
    }
    if (!sameMembers([...actualOwners].sort(), [...allowedOwners].sort())) {
      errors.push(`- shared function "${fnName}" ha owners ${actualOwners.join(', ')} ma attesi ${allowedOwners.join(', ')}`);
    }
  }

  if (errors.length) {
    throw new Error(`Site registry non valido.\n\n${errors.join('\n\n')}`);
  }
}

export function getAllPageOwners() {
  assertValidSiteRegistry();
  return new Map(
    [...collectOwners('pages').entries()]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([page, owners]) => [page, owners[0]]),
  );
}

export function getAllFunctionOwners() {
  assertValidSiteRegistry();
  return new Map(
    [...collectOwners('functions').entries()]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([fnName, owners]) => [fnName, [...owners]]),
  );
}
