const { createClient } = require('@supabase/supabase-js');
const { getInternalSupabaseUserFromHeaders, normalizeSupabaseUrl } = require('./_lib/shared-auth');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers: CORS, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return respond(405, { error: 'Method not allowed' });
  }

  const auth = await getInternalSupabaseUserFromHeaders(event.headers);
  if (!auth) {
    return respond(401, { error: 'Autenticazione interna richiesta' });
  }

  const supabaseUrl = normalizeSupabaseUrl(process.env.SUPABASE_RUNTIME_URL || process.env.SUPABASE_URL);
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
  if (!supabaseUrl || !serviceRoleKey) {
    return respond(500, { error: 'Configurazione Supabase mancante' });
  }

  let body;
  try {
    body = JSON.parse(event.body || '{}');
  } catch (_error) {
    return respond(400, { error: 'JSON non valido' });
  }

  const requestedFiles = Array.isArray(body.files) ? body.files : body.file ? [body.file] : [];
  if (!requestedFiles.length) {
    return respond(400, { error: 'files obbligatorio' });
  }

  const ttlSeconds = clampTtl(body.ttl_seconds);
  const admin = createClient(supabaseUrl, serviceRoleKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  });

  try {
    const files = [];

    for (const item of requestedFiles) {
      const bucket = String(item.bucket || item.storage_bucket || '').trim();
      const path = sanitizeStoragePath(item.path || item.storage_path || item.file_url || '');
      if (!bucket || !path) {
        files.push({ bucket, path, signed_url: '', error: 'bucket o path mancanti' });
        continue;
      }

      const { data, error } = await admin.storage.from(bucket).createSignedUrl(path, ttlSeconds);
      if (error) {
        files.push({ bucket, path, signed_url: '', error: error.message || 'Errore creazione signed URL' });
        continue;
      }

      files.push({
        bucket,
        path,
        signed_url: data?.signedUrl || '',
        expires_in: ttlSeconds,
      });
    }

    return respond(200, { ok: true, files });
  } catch (error) {
    console.error('[get-accounting-file-url] error:', error);
    return respond(500, { error: 'Errore interno get-accounting-file-url', detail: error.message });
  }
};

function clampTtl(value) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) return 15 * 60;
  return Math.min(Math.max(parsed, 60), 60 * 60);
}

function sanitizeStoragePath(value) {
  return String(value || '')
    .trim()
    .replace(/^private:\/\//i, '')
    .replace(/^\/+/, '')
    .replace(/\.\.+/g, '');
}

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}
