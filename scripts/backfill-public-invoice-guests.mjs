import { createClient } from '@supabase/supabase-js';
import { createRequire } from 'node:module';
import { resolveRuntimeConfig } from './lib/netlify-runtime-config.mjs';

const projectRoot = process.cwd();
const runtime = resolveRuntimeConfig({ projectRoot, context: process.env.NETLIFY_CONTEXT || 'production' });

if (!runtime.supabaseUrl || !runtime.supabaseKey) {
  console.error('SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY/SUPABASE_SERVICE_KEY sono obbligatori');
  process.exit(1);
}

const dryRun = process.argv.includes('--dry-run');
const listApartments = process.argv.includes('--list-apartments');
const limitArg = process.argv.find((arg) => arg.startsWith('--limit='));
const limit = Math.min(Math.max(Number(limitArg ? limitArg.split('=')[1] : 50), 1), 500);
const apartmentArg = process.argv.find((arg) => arg.startsWith('--apartment-id='));
const fallbackApartmentId = apartmentArg ? String(apartmentArg.split('=')[1] || '').trim() : '';

const supabase = createClient(runtime.supabaseUrl, runtime.supabaseKey, {
  auth: { autoRefreshToken: false, persistSession: false },
});

const require = createRequire(import.meta.url);
const { createGuestLeadRecord, extractFormFromAccountingDocument } = require('../netlify/functions/_lib/public-invoice-guest-leads.js');

if (listApartments) {
  const { data: apartments, error: apartmentError } = await supabase
    .from('apartments')
    .select('id, nome_appartamento, attivo')
    .eq('attivo', true)
    .order('nome_appartamento', { ascending: true })
    .limit(200);

  if (apartmentError) {
    console.error('Errore lettura apartments:', apartmentError.message);
    process.exit(1);
  }

  console.log(JSON.stringify({ ok: true, apartments: apartments || [] }));
  process.exit(0);
}

const { data: records, error } = await supabase
  .from('contabilita_documenti')
  .select('id, supplier_name, source_email, source, subcategory, raw_payload, extracted_json, extracted_text, notes, created_at')
  .eq('source', 'public_invoice_upload')
  .eq('subcategory', 'richiesta_fattura_pubblica')
  .order('created_at', { ascending: false })
  .limit(limit);

if (error) {
  console.error('Errore lettura contabilita_documenti:', error.message);
  process.exit(1);
}

const candidates = (records || []).filter((record) => {
  const raw = record.raw_payload || {};
  return !(raw.guest_saved && raw.guest_id);
});

let created = 0;
let failed = 0;
let skipped = 0;

for (const record of candidates) {
  const form = extractFormFromAccountingDocument(record);
  const hasMinimumData = form.ragione_sociale && form.referente_nome && form.referente_cognome && form.email && form.telefono && form.piva_cliente && form.indirizzo_fatturazione;

  if (!hasMinimumData) {
    skipped += 1;
    console.log(JSON.stringify({
      document_id: record.id,
      ok: false,
      skipped: true,
      reason: 'Dati insufficienti nel raw_payload',
    }));
    continue;
  }

  if (dryRun) {
    console.log(JSON.stringify({
      document_id: record.id,
      ok: true,
      dry_run: true,
      ragione_sociale: form.ragione_sociale,
      email: form.email,
    }));
    continue;
  }

  const guestResult = await createGuestLeadRecord(supabase, form, { apartmentId: fallbackApartmentId || null });
  if (!guestResult.saved || !guestResult.id) {
    failed += 1;
    console.log(JSON.stringify({
      document_id: record.id,
      ok: false,
      warning: guestResult.warning || 'Creazione ospite fallita',
      errors: guestResult.errors || [],
    }));
    continue;
  }

  const guestBackfilledAt = new Date().toISOString();
  const nextRawPayload = {
    ...(record.raw_payload || {}),
    guest_saved: true,
    guest_id: guestResult.id,
    guest_backfilled_at: guestBackfilledAt,
  };

  const updatePayload = {
    raw_payload: nextRawPayload,
  };

  if (record.extracted_json && typeof record.extracted_json === 'object') {
    updatePayload.extracted_json = {
      ...record.extracted_json,
      guest_saved: true,
      guest_id: guestResult.id,
      guest_backfilled_at: guestBackfilledAt,
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
    failed += 1;
    console.log(JSON.stringify({
      document_id: record.id,
      ok: false,
      guest_id: guestResult.id,
      warning: `Ospite creato ma documento non aggiornato: ${updateError.message}`,
    }));
    continue;
  }

  created += 1;
  console.log(JSON.stringify({
    document_id: record.id,
    ok: true,
    guest_id: guestResult.id,
  }));
}

console.log(JSON.stringify({
  ok: true,
  dry_run: dryRun,
  requested: records?.length || 0,
  candidates: candidates.length,
  created,
  failed,
  skipped,
}));
