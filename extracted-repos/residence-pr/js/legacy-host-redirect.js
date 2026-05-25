;(function () {
  const redirects = [
  {
    "key": "legacy-checkin-root",
    "sourceHost": "checkinillupoaffitta.netlify.app",
    "route": "/",
    "canonicalUrl": "https://checkin.illupoaffitta.com/"
  },
  {
    "key": "legacy-checkin-fatture",
    "sourceHost": "checkinillupoaffitta.netlify.app",
    "route": "/backoffice.html",
    "canonicalUrl": "https://fatture.illupoaffitta.com/backoffice.html"
  },
  {
    "key": "legacy-checkin-calendario",
    "sourceHost": "checkinillupoaffitta.netlify.app",
    "route": "/backoffice-calendario.html",
    "canonicalUrl": "https://calendario.illupoaffitta.com/backoffice-calendario.html"
  },
  {
    "key": "legacy-checkin-portale",
    "sourceHost": "checkinillupoaffitta.netlify.app",
    "route": "/portale.html",
    "canonicalUrl": "https://checkin.illupoaffitta.com/portale.html"
  },
  {
    "key": "legacy-checkin-pr-kekko",
    "sourceHost": "checkinillupoaffitta.netlify.app",
    "route": "/pr_kekko_riccione.html",
    "canonicalUrl": "https://checkin.illupoaffitta.com/pr_kekko_riccione.html"
  },
  {
    "key": "legacy-checkin-residence-backoffice",
    "sourceHost": "checkinillupoaffitta.netlify.app",
    "route": "/residence-backoffice.html",
    "canonicalUrl": "https://checkin.illupoaffitta.com/residence-backoffice.html"
  }
];
  const loggingUrl = "https://checkin.illupoaffitta.com/.netlify/functions/log-legacy-redirect";
  const currentHost = String(window.location.hostname || '').toLowerCase();
  const currentPath = String(window.location.pathname || '/');
  const match = redirects.find((entry) => entry.sourceHost === currentHost && entry.route === currentPath);

  if (!match) return;

  const targetUrl = new URL(match.canonicalUrl);
  targetUrl.search = window.location.search || '';
  targetUrl.hash = window.location.hash || '';

  try {
    const payload = JSON.stringify({
      key: match.key,
      source_host: currentHost,
      route: currentPath,
      canonical_url: targetUrl.toString(),
      search: window.location.search || '',
      hash: window.location.hash || '',
      referrer: document.referrer || '',
      redirected_at: new Date().toISOString(),
      user_agent: navigator.userAgent || '',
    });

    if (typeof navigator.sendBeacon === 'function') {
      const body = new Blob([payload], { type: 'application/json' });
      navigator.sendBeacon(loggingUrl, body);
    } else if (typeof fetch === 'function') {
      fetch(loggingUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: payload,
        keepalive: true,
        mode: 'cors',
      }).catch(function () { return null; });
    }
  } catch (_error) {
    // Never block legacy redirects on telemetry issues.
  }

  if (targetUrl.toString() !== window.location.href) {
    window.location.replace(targetUrl.toString());
  }
})();
