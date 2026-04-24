const { createClient } = require('@supabase/supabase-js');
const {
  fetchBeds24Properties,
  loadApartments,
  buildReport,
  applyBeds24Matches,
  insertAuditLog,
} = require('./_beds24-link-sync');

exports.handler = async (event) => {
  const isScheduled = ['schedule', 'scheduled'].includes(
    String(event.headers['x-nf-event'] || event.headers['x-netlify-event'] || '').toLowerCase()
  );

  if (!isScheduled && !['POST', 'GET'].includes(event.httpMethod)) {
    return jsonResponse(405, { error: 'Method not allowed' });
  }

  const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY
  );

  const auth = await authorizeRequest(event, supabase, isScheduled);
  if (!auth.allowed) {
    return jsonResponse(auth.statusCode || 401, { error: auth.error || 'Unauthorized' });
  }

  try {
    const apartments = await loadApartments(supabase);
    const properties = await fetchBeds24Properties(process.env.BEDS24_API_KEY);
    const report = buildReport(apartments, properties);
    const applied = await applyBeds24Matches(supabase, report);

    await insertAuditLog(supabase, {
      user_email: auth.userEmail || 'system',
      action: 'BEDS24_LINKS_SYNC',
      tabella: 'apartments',
      table_name: 'apartments',
      record_id: null,
    });

    return jsonResponse(200, {
      success: true,
      report: {
        generated_at: report.generated_at,
        summary: {
          ...report.summary,
          updated: applied.updates.length,
          cleared: applied.clears.length,
          kept: applied.kept.length,
        },
        updated: applied.updates,
        cleared: applied.clears,
        unmatched_apartments: report.unmatched,
        unmatched_beds24_properties: report.unmatchedProperties.map((property) => ({
          beds24_room_id: property.id,
          property_name: property.name,
          address: property.address,
          city: property.city,
        })),
      },
    });
  } catch (error) {
    console.error('[sync-beds24-links] error:', error);
    return jsonResponse(500, { error: 'Errore sincronizzazione Beds24', detail: error.message });
  }
};

async function authorizeRequest(event, supabase, isScheduled) {
  if (isScheduled) {
    return { allowed: true, userEmail: 'system' };
  }

  const internalKey = event.headers['x-internal-key'];
  if (internalKey && (internalKey === process.env.INTERNAL_API_KEY || internalKey === process.env.INTERNAL_KEY)) {
    return { allowed: true, userEmail: 'system' };
  }

  const authHeader = event.headers.authorization || event.headers.Authorization;
  if (!authHeader?.startsWith('Bearer ')) {
    return { allowed: false, statusCode: 401, error: 'Unauthorized' };
  }

  const token = authHeader.replace('Bearer ', '');
  const { data: { user }, error } = await supabase.auth.getUser(token);
  if (error || !user) {
    return { allowed: false, statusCode: 401, error: 'Unauthorized' };
  }

  return { allowed: true, userEmail: user.email || 'user' };
}

function jsonResponse(statusCode, body) {
  return {
    statusCode,
    headers: { 'Content-Type': 'application/json; charset=utf-8' },
    body: JSON.stringify(body),
  };
}
