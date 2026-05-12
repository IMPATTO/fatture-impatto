import { getAllPageOwners, siteRegistry } from '../sites/site-registry.mjs';

export const legacySiteUrls = Object.freeze({
  checkinNetlify: 'https://checkinillupoaffitta.netlify.app',
});

export const legacySitePages = Object.freeze({
  checkinNetlify: [
    'backoffice.html',
    'backoffice-calendario.html',
    'portale.html',
    'pr_kekko_riccione.html',
    'residence-backoffice.html',
  ],
});

const extraCanonicalRoutes = Object.freeze({
  portale: [
    'privacy-policy.html',
    'manifest.json',
  ],
});

const deprecatedProjectLinkSections = Object.freeze([
  {
    key: 'pr-luca',
    label: 'PR Luca (deprecated)',
    siteKey: 'residence-pr',
    routes: [
      'backoffice-pr-luca.html',
      'pr_luca_riccione.html',
    ],
  },
]);

const standaloneProjectLinkSections = Object.freeze([
  {
    key: 'villa-margherita',
    label: 'Villa Margherita',
    urls: [
      'https://villamargheritarimini.com/',
      'https://villamargheritarimini.com/monolocali-rimini.html',
      'https://villamargheritarimini.com/bilocali-rimini.html',
      'https://villamargheritarimini.com/trilocali-rimini.html',
      'https://villamargheritarimini.com/residence-rimini-terme.html',
      'https://villamargheritarimini.com/offerte-vacanze-rimini.html',
    ],
  },
]);

export const canonicalSiteUrls = Object.freeze(
  Object.fromEntries(
    Object.entries(siteRegistry).map(([siteKey, site]) => [siteKey, String(site.fallbackUrl || '').replace(/\/+$/, '')]),
  ),
);

export function joinSiteUrl(baseUrl, route) {
  const normalizedBaseUrl = String(baseUrl || '').replace(/\/+$/, '');
  const normalizedRoute = String(route || '').replace(/^\/+/, '');
  return new URL(normalizedRoute, `${normalizedBaseUrl}/`).toString();
}

export function getCanonicalSiteUrl(siteKey) {
  return canonicalSiteUrls[siteKey] || canonicalSiteUrls.portale || '';
}

export function buildCheckinUrl(publicCheckinKey, siteKey = 'portale') {
  if (!publicCheckinKey) return '';
  const params = new URLSearchParams({
    apt: String(publicCheckinKey),
    mode: 'checkin',
  });
  return `${getCanonicalSiteUrl(siteKey)}/?${params.toString()}`;
}

export function buildOpenPortalUrl(publicCheckinKey, siteKey = 'portale') {
  if (!publicCheckinKey) return '';
  const params = new URLSearchParams({
    apt: String(publicCheckinKey),
    mode: 'open',
  });
  return `${getCanonicalSiteUrl(siteKey)}/portale.html?${params.toString()}`;
}

export function buildPortalTokenUrl(token, publicCheckinKey = '', siteKey = 'portale') {
  if (!token) return '';
  const params = new URLSearchParams({ token: String(token) });
  if (publicCheckinKey) params.set('apt', String(publicCheckinKey));
  return `${getCanonicalSiteUrl(siteKey)}/portale.html?${params.toString()}`;
}

export function getKnownSiteUrlPrefixes() {
  return [
    ...Object.values(canonicalSiteUrls),
    ...Object.values(legacySiteUrls),
    ...standaloneProjectLinkSections.flatMap((section) => section.urls.map((url) => String(url).replace(/\/+(?:index\.html)?$/, ''))),
  ].filter(Boolean);
}

function getDeprecatedRoutesBySite() {
  const deprecatedRoutesBySite = new Map();

  for (const section of deprecatedProjectLinkSections) {
    const currentRoutes = deprecatedRoutesBySite.get(section.siteKey) || new Set();
    for (const route of section.routes || []) {
      currentRoutes.add(route);
    }
    deprecatedRoutesBySite.set(section.siteKey, currentRoutes);
  }

  return deprecatedRoutesBySite;
}

export function getProjectLinksSections() {
  const pageOwners = getAllPageOwners();
  const sections = [];
  const deprecatedRoutesBySite = getDeprecatedRoutesBySite();

  for (const [siteKey, site] of Object.entries(siteRegistry)) {
    const urls = [];
    const deprecatedRoutes = deprecatedRoutesBySite.get(siteKey) || new Set();

    for (const [route, ownerSiteKey] of pageOwners.entries()) {
      if (ownerSiteKey !== siteKey) continue;
      if (deprecatedRoutes.has(route)) continue;
      urls.push(joinSiteUrl(getCanonicalSiteUrl(siteKey), route));
    }

    for (const route of extraCanonicalRoutes[siteKey] || []) {
      urls.push(joinSiteUrl(getCanonicalSiteUrl(siteKey), route));
    }

    sections.push({
      key: siteKey,
      label: site.label,
      urls: [...new Set(urls)].sort(),
    });
  }

  sections.push({
    key: 'legacy-checkin-netlify',
    label: 'Legacy Check-in Netlify',
    urls: (legacySitePages.checkinNetlify || [])
      .map((route) => joinSiteUrl(legacySiteUrls.checkinNetlify, route))
      .sort(),
  });

  for (const section of standaloneProjectLinkSections) {
    sections.push({
      key: section.key,
      label: section.label,
      urls: [...new Set(section.urls || [])].sort(),
    });
  }

  return sections;
}

export function getProjectLinkUrls() {
  return [...new Set(getProjectLinksSections().flatMap((section) => section.urls))];
}

export function getDeprecatedProjectLinksSections() {
  return deprecatedProjectLinkSections.map((section) => ({
    key: section.key,
    label: section.label,
    urls: [...new Set((section.routes || []).map((route) => joinSiteUrl(getCanonicalSiteUrl(section.siteKey), route)))].sort(),
  }));
}

export function getDeprecatedProjectLinkUrls() {
  return [...new Set(getDeprecatedProjectLinksSections().flatMap((section) => section.urls))];
}
