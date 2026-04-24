const { createClient } = require('@supabase/supabase-js');
const {
  loadSyncContext,
  buildAuditReport,
  applyAuditRows,
  executeApply,
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
    const auditReport = buildAuditReport(context, { mode: 'audit' });
    const plan = applyAuditRows(context, auditReport.apartments);
    const applied = await executeApply(supabase, context, plan, user.email);
    const finalReport = {
      generated_at: new Date().toISOString(),
      mode: 'apply',
      schema_checks: auditReport.schema_checks,
      summary: auditReport.summary,
      apartments: applied.rows,
    };
    finalReport.summary = {
      ...buildAuditReport(await loadSyncContext(supabase), { mode: 'apply', stats: applied.stats }).summary,
      public_checkin_key_created: applied.stats.public_checkin_key_created,
      alloggiati_auto_linked: applied.stats.alloggiati_auto_linked,
      istat_created: applied.stats.istat_created,
      errors: applied.stats.errors,
    };

    await insertAuditLog(supabase, {
      user_email: user.email,
      action: 'APARTMENT_LINKS_APPLY',
      table_name: 'apartments',
      record_id: null,
    });

    return jsonResponse(200, {
      success: true,
      report: finalReport,
    });
  } catch (err) {
    console.error('[apply-apartment-links] error:', err);
    return jsonResponse(500, { error: 'Errore apply collegamenti', detail: err.message });
  }
};

function jsonResponse(statusCode, body) {
  return {
    statusCode,
    headers: { 'Content-Type': 'application/json; charset=utf-8' },
    body: JSON.stringify(body),
  };
}
