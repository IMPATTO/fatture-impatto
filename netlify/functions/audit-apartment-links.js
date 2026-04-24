const { createClient } = require('@supabase/supabase-js');
const {
  loadSyncContext,
  buildAuditReport,
  insertAuditLog,
} = require('./_apartment-link-sync');

exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') {
    return jsonResponse(405, { error: 'Method not allowed' });
  }

  const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_ROLE_KEY
  );

  const authHeader = event.headers.authorization || event.headers.Authorization;
  if (!authHeader?.startsWith('Bearer ')) {
    return jsonResponse(401, { error: 'Unauthorized' });
  }

  const token = authHeader.replace('Bearer ', '');
  const { data: { user }, error: userErr } = await supabase.auth.getUser(token);
  if (userErr || !user) {
    return jsonResponse(401, { error: 'Unauthorized' });
  }

  try {
    const context = await loadSyncContext(supabase);
    const report = buildAuditReport(context, { mode: 'audit' });
    await insertAuditLog(supabase, {
      user_email: user.email,
      action: 'APARTMENT_LINKS_AUDIT',
      table_name: 'apartments',
      record_id: null,
    });
    return jsonResponse(200, { success: true, report });
  } catch (err) {
    console.error('[audit-apartment-links] error:', err);
    return jsonResponse(500, { error: 'Errore audit collegamenti', detail: err.message });
  }
};

function jsonResponse(statusCode, body) {
  return {
    statusCode,
    headers: { 'Content-Type': 'application/json; charset=utf-8' },
    body: JSON.stringify(body),
  };
}
