const { createClient } = require('@supabase/supabase-js');

const { getInternalSupabaseUserFromHeaders } = require('./_lib/shared-auth');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Content-Type': 'application/json',
};

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: CORS, body: '' };
  }

  if (event.httpMethod !== 'GET') {
    return respond(405, { error: 'Method not allowed' });
  }

  const supabaseUrl = process.env.SUPABASE_URL || 'https://tysxeikqbgebpfyblgeb.supabase.co';
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;

  if (!supabaseUrl || !serviceRoleKey) {
    return respond(500, { error: 'Configurazione Supabase mancante' });
  }

  const authHeader = event.headers.authorization || event.headers.Authorization;
  if (!authHeader?.startsWith('Bearer ')) {
    return respond(401, { error: 'Autenticazione mancante' });
  }

  const internalUser = await getInternalSupabaseUserFromHeaders(event.headers);
  if (!internalUser?.email) {
    return respond(403, { error: 'Accesso non autorizzato' });
  }

  const admin = createClient(supabaseUrl, serviceRoleKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  });

  const [
    apartmentsRes,
    accountsRes,
    linksRes,
    istatRes,
    portalRowsRes,
  ] = await Promise.all([
    admin
      .from('apartments')
      .select('id,nome_appartamento,codice_interno,struttura_nome,attivo,public_checkin_key')
      .order('struttura_nome', { ascending: true, nullsFirst: false })
      .order('nome_appartamento', { ascending: true }),
    admin
      .from('alloggiati_accounts')
      .select('id,nome_account,questura,attivo')
      .order('nome_account'),
    admin
      .from('apartment_alloggiati')
      .select('*'),
    admin
      .from('apartment_istat_config_public')
      .select('id,apartment_id,attivo,regione,sistema,portal_url,codice_struttura,auth_type,username,note,supports_file_import,supports_webservice,requires_open_close,deadline_rule')
      .order('created_at', { ascending: false }),
    admin
      .from('ospiti_check_in')
      .select('id,nome,cognome,email,data_checkin,created_at,stato,apartment_id,portale_token,apartments(nome_appartamento,public_checkin_key)')
      .not('portale_token', 'is', null)
      .order('created_at', { ascending: false })
      .limit(200),
  ]);

  const firstError = [
    apartmentsRes.error,
    accountsRes.error,
    linksRes.error,
    istatRes.error,
    portalRowsRes.error,
  ].find(Boolean);

  if (firstError) {
    return respond(500, {
      error: 'Errore caricamento dati Portale',
      detail: firstError.message,
    });
  }

  return respond(200, {
    viewer: internalUser.email,
    apartments: apartmentsRes.data || [],
    accounts: accountsRes.data || [],
    links: linksRes.data || [],
    istatConfigs: istatRes.data || [],
    portalRows: portalRowsRes.data || [],
  });
};

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}
