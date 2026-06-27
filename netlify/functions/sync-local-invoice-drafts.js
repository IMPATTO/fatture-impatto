const {
  authenticateRequest,
  createSupabaseAdmin,
  detectOptionalColumns,
  ensureLocalInvoiceDraftForOspiteId,
  jsonResponse,
  parseEventJson,
} = require('./_lib/fatture-fic');

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

function withCors(response) {
  return {
    ...response,
    headers: {
      ...CORS_HEADERS,
      ...(response.headers || {}),
    },
  };
}

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return withCors({ statusCode: 200, body: '' });
  }

  if (event.httpMethod !== 'POST') {
    return withCors(jsonResponse(405, { error: 'Method not allowed' }));
  }

  const parsed = parseEventJson(event);
  if (parsed.errorResponse) return withCors(parsed.errorResponse);
  const body = parsed.body || {};

  const ospitiIds = Array.isArray(body.ospiti_check_in_ids)
    ? body.ospiti_check_in_ids.map((value) => String(value || '').trim()).filter(Boolean)
    : [];
  const uniqueOspitiIds = [...new Set(ospitiIds)];

  if (!uniqueOspitiIds.length) {
    return withCors(jsonResponse(400, { error: 'ospiti_check_in_ids richiesto' }));
  }

  const supabase = createSupabaseAdmin();
  const auth = await authenticateRequest(event, supabase);
  if (auth.errorResponse) return withCors(auth.errorResponse);

  const columnSupport = await detectOptionalColumns(supabase);
  const results = [];
  let created = 0;
  let updated = 0;
  let skipped = 0;
  let failed = 0;

  for (const ospitiId of uniqueOspitiIds) {
    const result = await ensureLocalInvoiceDraftForOspiteId(supabase, ospitiId, {
      columnSupport,
      force: body.force === true,
    });

    if (!result.ok) {
      failed += 1;
      results.push({
        requested_ospiti_check_in_id: ospitiId,
        ok: false,
        error: result.error?.message || 'Errore sincronizzazione bozza locale',
      });
      continue;
    }

    if (result.created) created += 1;
    else if (result.updated) updated += 1;
    else skipped += 1;

    results.push({
      requested_ospiti_check_in_id: ospitiId,
      ospiti_check_in_id: result.ospite?.id || ospitiId,
      ok: true,
      created: !!result.created,
      updated: !!result.updated,
      skipped: !!result.skipped,
      reason: result.reason || null,
      fattura_staging_id: result.data?.id || null,
      stato: result.data?.stato || null,
    });
  }

  return withCors(jsonResponse(200, {
    ok: true,
    created,
    updated,
    skipped,
    failed,
    total: uniqueOspitiIds.length,
    requested_total: ospitiIds.length,
    results,
  }));
};
