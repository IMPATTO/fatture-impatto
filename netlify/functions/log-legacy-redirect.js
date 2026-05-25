const { createClient } = require('@supabase/supabase-js');

const DEFAULT_SUPABASE_URL = 'https://tysxeikqbgebpfyblgeb.supabase.co';
const SUPABASE_URL = process.env.SUPABASE_URL || DEFAULT_SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

const ALLOWED_SOURCE_HOSTS = new Set([
  'checkinillupoaffitta.netlify.app',
]);

const ALLOWED_ROUTES = new Set([
  '/',
  '/backoffice.html',
  '/backoffice-calendario.html',
  '/portale.html',
  '/pr_kekko_riccione.html',
  '/residence-backoffice.html',
]);

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: CORS, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return respond(405, { error: 'Method not allowed' });
  }

  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    return respond(202, { ok: false, skipped: true, reason: 'supabase_config_missing' });
  }

  let body = {};
  try {
    body = JSON.parse(event.body || '{}');
  } catch (_error) {
    return respond(400, { error: 'Invalid JSON' });
  }

  const sourceHost = String(body.source_host || '').trim().toLowerCase();
  const route = normalizeRoute(body.route);
  const key = String(body.key || '').trim() || `${sourceHost}${route}`;

  if (!ALLOWED_SOURCE_HOSTS.has(sourceHost) || !ALLOWED_ROUTES.has(route)) {
    return respond(202, { ok: false, skipped: true, reason: 'outside_legacy_allowlist' });
  }

  try {
    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
      auth: { autoRefreshToken: false, persistSession: false },
    });

    const payload = {
      user_email: '',
      action: 'legacy_redirect_hit',
      table_name: 'legacy_compatibility',
      record_id: key,
      created_at: new Date().toISOString(),
      new_data: {
        source_host: sourceHost,
        route,
        canonical_url: String(body.canonical_url || '').trim(),
        search: String(body.search || '').slice(0, 500),
        hash: String(body.hash || '').slice(0, 200),
        referrer: String(body.referrer || '').slice(0, 1000),
        redirected_at: String(body.redirected_at || '').trim(),
        user_agent: String(body.user_agent || event.headers['user-agent'] || '').slice(0, 1000),
      },
      ip_address: String(
        event.headers['x-nf-client-connection-ip']
          || event.headers['x-forwarded-for']
          || ''
      ).slice(0, 200),
    };

    const { error } = await supabase.from('audit_log').insert(payload);
    if (error) {
      console.error('[log-legacy-redirect] audit_log error:', error);
      return respond(202, { ok: false, skipped: true, reason: 'audit_log_insert_failed' });
    }

    return respond(202, { ok: true });
  } catch (error) {
    console.error('[log-legacy-redirect] fatal:', error);
    return respond(202, { ok: false, skipped: true, reason: 'runtime_error' });
  }
};

function normalizeRoute(value) {
  const trimmed = String(value || '/').trim();
  if (!trimmed || trimmed === '/') return '/';
  return `/${trimmed.replace(/^\/+/, '')}`;
}

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}
