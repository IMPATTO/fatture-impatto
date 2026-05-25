const { createClient } = require('@supabase/supabase-js');
const { getInternalSupabaseUserFromHeaders } = require('./_lib/shared-auth');
const {
  createGuestLeadRecord,
  extractFormFromAccountingDocument,
} = require('./_lib/public-invoice-guest-leads');
const { ensureLocalInvoiceDraftForOspiteId } = require('./_lib/fatture-fic');

const DEFAULT_SUPABASE_URL = 'https://tysxeikqbgebpfyblgeb.supabase.co';
const SUPABASE_URL = process.env.SUPABASE_URL || DEFAULT_SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

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

  const auth = await getInternalSupabaseUserFromHeaders(event.headers || {});
  if (!auth?.user?.email) {
    return respond(401, { error: 'Unauthorized' });
  }

  let body = {};
  try {
    body = JSON.parse(event.body || '{}');
  } catch (error) {
    return respond(400, { error: 'Invalid JSON', detail: error.message });
  }

  const dryRun = body.dry_run === true;
  const listApartments = body.list_apartments === true;
  const limit = Math.min(Math.max(Number(body.limit || 25), 1), 200);
  const fallbackApartmentId = String(body.fallback_apartment_id || '').trim() || null;
  const documentIds = Array.isArray(body.document_ids)
    ? body.document_ids.map((value) => String(value || '').trim()).filter(Boolean)
    : [];

  try {
    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
    if (listApartments) {
      const { data: apartments, error: apartmentError } = await supabase
        .from('apartments')
        .select('id, nome_appartamento, attivo')
        .eq('attivo', true)
        .order('nome_appartamento', { ascending: true })
        .limit(200);
      if (apartmentError) throw apartmentError;
      return respond(200, { ok: true, apartments: apartments || [] });
    }

    let query = supabase
      .from('contabilita_documenti')
      .select('id, supplier_name, source_email, source, subcategory, raw_payload, extracted_json, extracted_text, notes, created_at')
      .eq('source', 'public_invoice_upload')
      .eq('subcategory', 'richiesta_fattura_pubblica')
      .order('created_at', { ascending: false })
      .limit(limit);

    if (documentIds.length) {
      query = query.in('id', documentIds);
    }

    const { data: records, error } = await query;
    if (error) throw error;

    const candidates = (records || []).filter((record) => {
      const raw = record.raw_payload || {};
      return !(raw.guest_saved && raw.guest_id);
    });

    const results = [];

    for (const record of candidates) {
      const form = extractFormFromAccountingDocument(record);
      if (!form.ragione_sociale || !form.referente_nome || !form.referente_cognome || !form.email || !form.telefono || !form.piva_cliente || !form.indirizzo_fatturazione) {
        results.push({
          document_id: record.id,
          ok: false,
          skipped: true,
          reason: 'Dati insufficienti nel raw_payload del documento',
        });
        continue;
      }

      if (dryRun) {
        results.push({
          document_id: record.id,
          ok: true,
          dry_run: true,
          ragione_sociale: form.ragione_sociale,
          email: form.email,
        });
        continue;
      }

      const guestResult = await createGuestLeadRecord(supabase, form, { apartmentId: fallbackApartmentId });
      if (!guestResult.saved || !guestResult.id) {
        results.push({
          document_id: record.id,
          ok: false,
          warning: guestResult.warning || 'Creazione ospite fallita',
        });
        continue;
      }

      const draftResult = await ensureLocalInvoiceDraftForOspiteId(supabase, guestResult.id).catch((error) => ({
        ok: false,
        error,
      }));
      if (!draftResult?.ok) {
        console.error('[backfill-public-invoice-guests] local draft sync error:', draftResult?.error || draftResult);
      }

      const nextRawPayload = {
        ...(record.raw_payload || {}),
        guest_saved: true,
        guest_id: guestResult.id,
        guest_backfilled_at: new Date().toISOString(),
      };

      const updatePayload = {
        raw_payload: nextRawPayload,
      };

      if (record.extracted_json && typeof record.extracted_json === 'object') {
        updatePayload.extracted_json = {
          ...record.extracted_json,
          guest_saved: true,
          guest_id: guestResult.id,
          guest_backfilled_at: nextRawPayload.guest_backfilled_at,
        };
      }

      const existingNotes = String(record.notes || '').trim();
      const backfillNote = `ospiti_check_in: ${guestResult.id}`;
      if (!existingNotes.includes(backfillNote)) {
        updatePayload.notes = [existingNotes, backfillNote].filter(Boolean).join(' | ');
      }

      const { error: updateError } = await supabase
        .from('contabilita_documenti')
        .update(updatePayload)
        .eq('id', record.id);

      if (updateError) {
        results.push({
          document_id: record.id,
          ok: false,
          guest_id: guestResult.id,
          warning: `Ospite creato ma documento non aggiornato: ${updateError.message}`,
        });
        continue;
      }

      results.push({
        document_id: record.id,
        ok: true,
        guest_id: guestResult.id,
      });
    }

    return respond(200, {
      ok: true,
      dry_run: dryRun,
      requested: records?.length || 0,
      candidates: candidates.length,
      created: results.filter((item) => item.ok && !item.dry_run && item.guest_id).length,
      skipped: results.filter((item) => item.skipped).length,
      failed: results.filter((item) => !item.ok && !item.skipped).length,
      results,
    });
  } catch (runtimeError) {
    console.error('[backfill-public-invoice-guests] error:', runtimeError);
    return respond(500, { error: 'Backfill fallito', detail: runtimeError.message });
  }
};

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}
