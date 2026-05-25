const { createClient } = require('@supabase/supabase-js');
const { getAuthContextFromHeaders } = require('./_lib/shared-auth');

const DEFAULT_SUPABASE_URL = 'https://tysxeikqbgebpfyblgeb.supabase.co';
const SUPABASE_URL = process.env.SUPABASE_URL || DEFAULT_SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const CHECKIN_DOCUMENTS_BUCKET = process.env.PUBLIC_CHECKIN_DOCUMENTS_BUCKET || 'checkin-documents';

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: CORS, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return respond(405, { error: 'Method not allowed' });
  }

  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    return respond(500, { error: 'Configurazione Supabase mancante' });
  }

  const auth = await getAuthContextFromHeaders(event.headers || {});
  if (!auth) {
    return respond(401, { error: 'Unauthorized' });
  }

  let body;
  try {
    body = JSON.parse(event.body || '{}');
  } catch (_error) {
    return respond(400, { error: 'Invalid JSON body' });
  }

  const storagePath = String(body.storage_path || '').trim();
  if (!storagePath || !storagePath.startsWith('public-checkin/')) {
    return respond(400, { error: 'storage_path non valido' });
  }

  try {
    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
      auth: { autoRefreshToken: false, persistSession: false },
    });
    const { data, error } = await supabase.storage
      .from(CHECKIN_DOCUMENTS_BUCKET)
      .createSignedUrl(storagePath, 60 * 10, {
        download: String(body.file_name || '').trim() || undefined,
      });

    if (error) throw error;
    if (!data?.signedUrl) {
      return respond(404, { error: 'Documento non trovato' });
    }

    return respond(200, { signed_url: data.signedUrl });
  } catch (error) {
    console.error('[get-checkin-document-url] error:', error);
    return respond(500, { error: 'Impossibile aprire il documento', detail: error.message });
  }
};

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}
