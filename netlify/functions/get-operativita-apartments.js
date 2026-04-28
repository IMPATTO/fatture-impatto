const { createClient } = require('@supabase/supabase-js');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
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

  if (event.httpMethod !== 'GET') {
    return respond(405, { error: 'Method not allowed' });
  }

  const supabaseUrl = process.env.SUPABASE_URL || 'https://tysxeikqbgebpfyblgeb.supabase.co';
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
  const anonKey = process.env.SUPABASE_ANON_KEY || 'sb_publishable_oMSD-SJgBZAA3Hql6vbxHg_0l2t9S5F';

  if (!supabaseUrl || !serviceRoleKey) {
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
  if (authError || !authData?.user?.email) {
    return respond(401, { error: 'Sessione non valida' });
  }

  const allowedEmails = new Set([
    'fatturazione@illupoaffitta.com',
    'contabilita@illupoaffitta.com',
    'info@marcovenzon.com',
    'veronica.dieta@gmail.com',
    'jessica.appartamenticaldari@gmail.com',
    'cerulliserena@gmail.com',
    'ramirezgonzalezv44@gmail.com',
  ]);

  const email = String(authData.user.email || '').trim().toLowerCase();
  if (!allowedEmails.has(email)) {
    return respond(403, { error: 'Accesso non autorizzato' });
  }

  const admin = createClient(supabaseUrl, serviceRoleKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  });

  const { data, error } = await admin
    .from('apartments')
    .select('id,nome_appartamento')
    .order('nome_appartamento', { ascending: true, nullsFirst: false });

  if (error) {
    return respond(500, { error: 'Errore lettura appartamenti', detail: error.message });
  }

  return respond(200, { apartments: data || [] });
};
