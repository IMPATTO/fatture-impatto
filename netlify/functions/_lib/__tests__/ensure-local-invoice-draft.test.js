const assert = require('assert');
const {
  ensureLocalInvoiceDraftForOspiteId,
  LOCAL_DRAFT_INCOMPLETE_STATO,
} = require('../fatture-fic');

function makeSupabaseMock({ ospite, existingStaging = null }) {
  const calls = { upserts: [], ospiteUpdates: [] };

  function from(table) {
    if (table === 'ospiti_check_in') {
      return {
        select() { return this; },
        eq() { return this; },
        maybeSingle() { return Promise.resolve({ data: ospite, error: null }); },
        update(values) {
          calls.ospiteUpdates.push(values);
          return {
            eq() { return this; },
            then(resolve) { return resolve({ error: null }); },
          };
        },
      };
    }

    if (table === 'fatture_staging') {
      return {
        select() { return this; },
        eq() { return this; },
        maybeSingle() { return Promise.resolve({ data: existingStaging, error: null }); },
        upsert(payload, opts) {
          calls.upserts.push({ payload, opts });
          const saved = { id: existingStaging?.id || 'staging-new', ...payload };
          return {
            select() { return this; },
            single() { return Promise.resolve({ data: saved, error: null }); },
          };
        },
        insert(payload) {
          calls.upserts.push({ payload, opts: null, mode: 'insert' });
          return {
            select() { return this; },
            single() { return Promise.resolve({ data: { id: 'staging-new', ...payload }, error: null }); },
          };
        },
      };
    }

    throw new Error(`Mock: tabella non gestita ${table}`);
  }

  return { from, _calls: calls };
}

const columnSupport = {
  ragione_sociale: true,
  indirizzo_fatturazione: true,
  fatture_staging_payment_status: true,
  fatture_staging_fic_document_id: true,
  fatture_staging_sezionale: true,
  fatture_staging_cliente_id: true,
  fatture_staging_input_testuale_originale: true,
  fatture_staging_parsing_payload: true,
};

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test('lead DA_VERIFICARE senza importo crea pratica DA_COMPLETARE', async () => {
  const supabase = makeSupabaseMock({
    ospite: {
      id: 'osp-1',
      nome: 'Mario',
      cognome: 'Rossi',
      stato: 'DA_VERIFICARE',
      tipo_cliente: 'azienda',
      piva_cliente: 'IT12345678901',
      iva_percentuale: 22,
      importo_lordo: 0,
      ragione_sociale: 'ACME Srl',
      indirizzo_fatturazione: 'Via Roma 1',
    },
  });

  const result = await ensureLocalInvoiceDraftForOspiteId(supabase, 'osp-1', { columnSupport });
  assert.strictEqual(result.ok, true);
  assert.strictEqual(result.skipped, false);
  assert.strictEqual(result.created, true);
  assert.strictEqual(result.incomplete, true);
  assert.strictEqual(result.reason, 'created_incomplete');
  assert.strictEqual(supabase._calls.upserts[0].payload.stato, LOCAL_DRAFT_INCOMPLETE_STATO);
  assert.strictEqual(supabase._calls.upserts[0].payload.importo_lordo, null);
});

test('check-in CHECK_IN_COMPLETATO senza importo crea pratica DA_COMPLETARE', async () => {
  const supabase = makeSupabaseMock({
    ospite: {
      id: 'osp-2',
      nome: 'Lucia',
      cognome: 'Bianchi',
      stato: 'CHECK_IN_COMPLETATO',
      tipo_cliente: 'azienda',
      piva_cliente: 'IT99999999999',
      iva_percentuale: 22,
      importo_lordo: null,
      ragione_sociale: 'Beta SpA',
      indirizzo_fatturazione: 'Via Po 2',
    },
  });

  const result = await ensureLocalInvoiceDraftForOspiteId(supabase, 'osp-2', { columnSupport });
  assert.strictEqual(result.skipped, false);
  assert.strictEqual(result.created, true);
  assert.strictEqual(result.incomplete, true);
  assert.strictEqual(supabase._calls.upserts[0].payload.stato, LOCAL_DRAFT_INCOMPLETE_STATO);
});

test('ospite APPROVATA con importo valido crea pratica con totali', async () => {
  const supabase = makeSupabaseMock({
    ospite: {
      id: 'osp-3',
      nome: 'Anna',
      cognome: 'Verdi',
      stato: 'APPROVATA',
      tipo_cliente: 'azienda',
      piva_cliente: 'IT11111111111',
      iva_percentuale: 22,
      importo_lordo: 122,
      ragione_sociale: 'Gamma Srl',
      indirizzo_fatturazione: 'Via Etna 3',
    },
  });

  const result = await ensureLocalInvoiceDraftForOspiteId(supabase, 'osp-3', { columnSupport });
  assert.strictEqual(result.skipped, false);
  assert.strictEqual(result.incomplete, false);
  assert.strictEqual(result.reason, 'created');
  assert.strictEqual(supabase._calls.upserts[0].payload.stato, 'APPROVATA');
  assert.strictEqual(supabase._calls.upserts[0].payload.importo_lordo, 122);
  assert.ok(supabase._calls.upserts[0].payload.importo_totale_con_iva > 0);
});

test('senza intento fattura resta skippato', async () => {
  const supabase = makeSupabaseMock({
    ospite: {
      id: 'osp-4',
      nome: 'Privato',
      cognome: 'Singolo',
      stato: 'CHECK_IN_COMPLETATO',
      tipo_cliente: 'privato',
      piva_cliente: '',
      iva_percentuale: 10,
      importo_lordo: 0,
      ragione_sociale: '',
      indirizzo_fatturazione: '',
    },
  });

  const result = await ensureLocalInvoiceDraftForOspiteId(supabase, 'osp-4', { columnSupport });
  assert.strictEqual(result.skipped, true);
  assert.strictEqual(result.reason, 'no_invoice_intent');
  assert.strictEqual(supabase._calls.upserts.length, 0);
});

test('stato SCARTATA resta skippato', async () => {
  const supabase = makeSupabaseMock({
    ospite: {
      id: 'osp-5',
      nome: 'X',
      cognome: 'Y',
      stato: 'SCARTATA',
      tipo_cliente: 'azienda',
      piva_cliente: 'IT22222222222',
      iva_percentuale: 22,
      importo_lordo: 100,
      ragione_sociale: 'Z Srl',
      indirizzo_fatturazione: 'Via A 1',
    },
  });

  const result = await ensureLocalInvoiceDraftForOspiteId(supabase, 'osp-5', { columnSupport });
  assert.strictEqual(result.skipped, true);
  assert.strictEqual(result.reason, 'stato_scartata');
  assert.strictEqual(supabase._calls.upserts.length, 0);
});

test('pratica esistente con importi validi non li perde in un re-run senza importo', async () => {
  const supabase = makeSupabaseMock({
    ospite: {
      id: 'osp-6',
      nome: 'M',
      cognome: 'R',
      stato: 'DA_VERIFICARE',
      tipo_cliente: 'azienda',
      piva_cliente: 'IT33333333333',
      iva_percentuale: 22,
      importo_lordo: 0,
      ragione_sociale: 'W Srl',
      indirizzo_fatturazione: 'Via B 2',
    },
    existingStaging: {
      id: 'stg-6',
      stato: 'APPROVATA',
      numero_fattura: null,
      link_fatture_cloud: null,
      importo_lordo: 244,
      iva_percentuale: 22,
      importo_totale_con_iva: 244,
    },
  });

  const result = await ensureLocalInvoiceDraftForOspiteId(supabase, 'osp-6', { columnSupport });
  assert.strictEqual(result.skipped, false);
  assert.strictEqual(result.updated, true);
  assert.strictEqual(result.incomplete, false);
  assert.strictEqual(supabase._calls.upserts[0].payload.stato, 'APPROVATA');
  assert.strictEqual(supabase._calls.upserts[0].payload.importo_lordo, 244);
  assert.strictEqual(supabase._calls.upserts[0].payload.importo_totale_con_iva, 244);
});

(async () => {
  let passed = 0;
  let failed = 0;

  for (const currentTest of tests) {
    try {
      await currentTest.fn();
      console.log(`[PASS] ${currentTest.name}`);
      passed += 1;
    } catch (error) {
      console.error(`[FAIL] ${currentTest.name}`);
      console.error(error.message);
      failed += 1;
    }
  }

  console.log(`\n${passed} passati, ${failed} falliti`);
  process.exit(failed ? 1 : 0);
})();
