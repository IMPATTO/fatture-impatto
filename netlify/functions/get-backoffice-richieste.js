const { createClient } = require('@supabase/supabase-js');
const {
  getSupabaseRuntimeConfig,
  isInternalAllowedEmail,
} = require('./_lib/shared-auth');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'GET, PATCH, OPTIONS',
  'Content-Type': 'application/json',
};

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: CORS, body: '' };
  }

  if (!['GET', 'PATCH'].includes(event.httpMethod)) {
    return respond(405, { error: 'Method not allowed' });
  }

  const runtimeConfig = getSupabaseRuntimeConfig();
  const supabaseUrl = runtimeConfig.url;
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
  const anonKey = runtimeConfig.anonKey;

  if (!supabaseUrl || !serviceRoleKey || !anonKey) {
    return respond(500, { error: 'Configurazione Supabase mancante' });
  }

  const authHeader = event.headers.authorization || event.headers.Authorization;
  if (!authHeader?.startsWith('Bearer ')) {
    return respond(401, { error: 'Autenticazione mancante' });
  }

  const token = authHeader.replace('Bearer ', '').trim();
  const authClient = createClient(supabaseUrl, anonKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  });
  const { data: authData, error: authError } = await authClient.auth.getUser(token);
  const email = String(authData?.user?.email || '').trim().toLowerCase();
  if (authError || !email) {
    return respond(401, { error: 'Sessione non valida' });
  }
  if (!isInternalAllowedEmail(email)) {
    return respond(403, { error: 'Accesso non autorizzato' });
  }

  const admin = createClient(supabaseUrl, serviceRoleKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  });

  try {
    if (event.httpMethod === 'GET') {
      const { data, error } = await admin
        .from('richieste_appartamenti')
        .select('*')
        .order('created_at', { ascending: false, nullsFirst: false })
        .limit(500);

      if (error) {
        return respond(500, { error: 'Errore lettura richieste', detail: error.message });
      }

      return respond(200, { requests: Array.isArray(data) ? data : [] });
    }

    let body;
    try {
      body = JSON.parse(event.body || '{}');
    } catch (_error) {
      return respond(400, { error: 'JSON non valido' });
    }

    const id = String(body.id || '').trim();
    const stato = String(body.stato || '').trim();
    if (!id) return respond(400, { error: 'id richiesto' });
    if (!stato) return respond(400, { error: 'stato richiesto' });

    const { data, error } = await admin
      .from('richieste_appartamenti')
      .update({ stato })
      .eq('id', id)
      .select('*')
      .single();

    if (error) {
      return respond(500, { error: 'Errore aggiornamento stato', detail: error.message });
    }

    return respond(200, { ok: true, request: data });
  } catch (error) {
    console.error('[get-backoffice-richieste] error:', error);
    return respond(500, { error: 'Errore interno', detail: error.message });
  }
};
