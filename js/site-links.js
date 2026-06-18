;(function () {
  const payload = {
  "canonicalSiteUrls": {
    "fatt-docs": "https://fatture.illupoaffitta.com",
    "operativita": "https://operativita.illupoaffitta.com",
    "portale": "https://checkin.illupoaffitta.com",
    "contabilita": "https://contabilita.illupoaffitta.com",
    "calendario": "https://calendario.illupoaffitta.com",
    "residence-pr": "https://checkin.illupoaffitta.com",
    "villa-margherita": "https://villamargheritarimini.com"
  },
  "legacySiteUrls": {
    "checkinNetlify": "https://checkinillupoaffitta.netlify.app"
  },
  "legacySitePages": {
    "checkinNetlify": [
      "backoffice.html",
      "backoffice-calendario.html",
      "portale.html",
      "pr_kekko_riccione.html",
      "residence-backoffice.html"
    ]
  }
};

  function getCanonicalSiteUrl(siteKey) {
    return payload.canonicalSiteUrls[siteKey] || payload.canonicalSiteUrls.portale || '';
  }

  function getLegacySiteUrl(siteKey) {
    return payload.legacySiteUrls[siteKey] || '';
  }

  function getHostname(url) {
    try {
      return new URL(String(url || '')).hostname.toLowerCase();
    } catch (_error) {
      return '';
    }
  }

  function getCanonicalSiteHost(siteKey) {
    return getHostname(getCanonicalSiteUrl(siteKey));
  }

  function getLegacySiteHost(siteKey) {
    return getHostname(getLegacySiteUrl(siteKey));
  }

  function buildCheckinUrl(publicCheckinKey, siteKey = 'portale') {
    if (!publicCheckinKey) return '';
    const params = new URLSearchParams({
      apt: String(publicCheckinKey),
      mode: 'checkin',
    });
    return `${getCanonicalSiteUrl(siteKey)}/?${params.toString()}`;
  }

  function buildOpenPortalUrl(publicCheckinKey, siteKey = 'portale') {
    if (!publicCheckinKey) return '';
    const params = new URLSearchParams({
      apt: String(publicCheckinKey),
      mode: 'open',
    });
    return `${getCanonicalSiteUrl(siteKey)}/portale.html?${params.toString()}`;
  }

  function buildPortalTokenUrl(token, publicCheckinKey = '', siteKey = 'portale') {
    if (!token) return '';
    const params = new URLSearchParams({ token: String(token) });
    if (publicCheckinKey) params.set('apt', String(publicCheckinKey));
    return `${getCanonicalSiteUrl(siteKey)}/portale.html?${params.toString()}`;
  }

  function buildSitePageUrl(siteKey, route = '') {
    const normalizedRoute = String(route || '').replace(/^\/+/, '');
    return `${getCanonicalSiteUrl(siteKey)}/${normalizedRoute}`;
  }

  function applySiteLinks(root = document) {
    const scope = root && typeof root.querySelectorAll === 'function' ? root : document;
    scope.querySelectorAll('[data-site-link]').forEach((node) => {
      const token = String(node.getAttribute('data-site-link') || '').trim();
      if (!token) return;
      const separatorIndex = token.indexOf(':');
      if (separatorIndex <= 0) return;
      const siteKey = token.slice(0, separatorIndex);
      const route = token.slice(separatorIndex + 1);
      if (!siteKey || !route) return;
      node.setAttribute('href', buildSitePageUrl(siteKey, route));
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => applySiteLinks(), { once: true });
  } else {
    applySiteLinks();
  }

  window.siteLinks = {
    canonicalSiteUrls: payload.canonicalSiteUrls,
    legacySiteUrls: payload.legacySiteUrls,
    applySiteLinks,
    getCanonicalSiteUrl,
    getCanonicalSiteHost,
    getLegacySiteUrl,
    getLegacySiteHost,
    buildCheckinUrl,
    buildOpenPortalUrl,
    buildPortalTokenUrl,
    buildSitePageUrl,
  };
})();
