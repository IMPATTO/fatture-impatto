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

  const auth = await getInternalSupabaseUserFromHeaders(event.headers);
  if (!auth) {
    return respond(401, { error: 'Autenticazione interna richiesta' });
  }

  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
  if (!process.env.SUPABASE_URL || !serviceRoleKey) {
    return respond(500, { error: 'Configurazione Supabase mancante' });
  }

  const supabase = createClient(
    process.env.SUPABASE_URL,
    serviceRoleKey
  );

  const { data, error } = await supabase
    .from('apartments')
    .select('nome_appartamento,codice_interno,struttura_nome,public_checkin_key,attivo')
    .eq('attivo', true)
    .not('public_checkin_key', 'is', null)
    .order('struttura_nome', { ascending: true, nullsFirst: false })
    .order('nome_appartamento', { ascending: true });

  if (error) {
    return respond(500, { error: 'Errore caricamento appartamenti', detail: error.message });
  }

  return respond(200, { apartments: data || [] });
};

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}
