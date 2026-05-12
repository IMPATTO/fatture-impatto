;(function () {
  const payload = {
  "canonicalSiteUrls": {
    "fatt-docs": "https://fatture.illupoaffitta.com",
    "operativita": "https://operativita.illupoaffitta.com",
    "portale": "https://checkin.illupoaffitta.com",
    "contabilita": "https://contabilita.illupoaffitta.com",
    "calendario": "https://calendario.illupoaffitta.com",
    "residence-pr": "https://checkin.illupoaffitta.com"
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

  window.siteLinks = {
    canonicalSiteUrls: payload.canonicalSiteUrls,
    legacySiteUrls: payload.legacySiteUrls,
    getCanonicalSiteUrl,
    buildCheckinUrl,
    buildOpenPortalUrl,
    buildPortalTokenUrl,
  };
})();
