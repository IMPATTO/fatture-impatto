const { test, expect } = require('playwright/test');
const submitPublicCheckinTestApi = require('../netlify/functions/submit-public-checkin.js').__test__;
const stripeTouristTaxTestApi = require('../netlify/functions/_lib/public-tourist-tax-stripe.js').__test__;

function installFetchMock(page, options = {}) {
  return page.addInitScript((config) => {
    if (config.ocrTimeoutMs) {
      window.__CHECKIN_OCR_TIMEOUT_MS = config.ocrTimeoutMs;
    }
    window.supabase = window.supabase || {
      createClient: () => ({
        auth: {
          persistSession: true,
          autoRefreshToken: true,
          detectSessionInUrl: true,
          lock: async (_name, _timeout, fn) => await fn(),
        },
      }),
    };
    window.__openedUrls = [];
    window.open = (url) => {
      window.__openedUrls.push(String(url || ''));
      return { closed: false };
    };
    window.__fetchCaptures = [];
    const realFetch = window.fetch.bind(window);

    function nextMockResponse(sequenceKey, singleKey, fallback) {
      if (Array.isArray(config[sequenceKey]) && config[sequenceKey].length) {
        return config[sequenceKey].shift();
      }
      if (config[singleKey]) return config[singleKey];
      return fallback;
    }

    function captureBody(body) {
      if (body instanceof FormData) {
        const entries = [];
        for (const [key, value] of body.entries()) {
          if (value instanceof File) {
            entries.push({
              key,
              kind: 'file',
              name: value.name,
              type: value.type,
              size: value.size,
            });
          } else {
            entries.push({
              key,
              kind: 'text',
              value: String(value),
            });
          }
        }
        return { isFormData: true, entries };
      }

      return {
        isFormData: false,
        bodyText: typeof body === 'string' ? body : '',
      };
    }

    window.fetch = async (input, init = {}) => {
      const url = typeof input === 'string' ? input : input.url;

      if (url.includes('/.netlify/functions/lookup-comuni')) {
        let body = {};
        try {
          body = JSON.parse(typeof init.body === 'string' ? init.body : '{}');
        } catch (_error) {
          body = {};
        }
        const lookupResults = config.lookupComuniResults || {};
        const rawKey = `${String(body.q || '')}|${String(body.provincia || '')}`;
        const fallbackKey = `${String(body.q || '')}|`;
        const responseConfig = lookupResults[rawKey]
          || lookupResults[fallbackKey]
          || { status: 200, body: { matches: [] } };
        const delayMs = Number(config.lookupComuniDelayMs || 0);
        if (delayMs > 0) {
          await new Promise((resolve) => setTimeout(resolve, delayMs));
        }
        return new Response(JSON.stringify(responseConfig.body || responseConfig), {
          status: responseConfig.status || 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      if (url.includes('/.netlify/functions/ocr')) {
        if (config.ocrNeverResolve) {
          return new Promise((_resolve, reject) => {
            if (init.signal && typeof init.signal.addEventListener === 'function') {
              init.signal.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')), { once: true });
            }
          });
        }
        const delayMs = Number(config.ocrDelayMs || 0);
        if (delayMs > 0) {
          await new Promise((resolve) => setTimeout(resolve, delayMs));
        }
        const responseConfig = nextMockResponse('ocrResponses', 'ocrResponse', {
          status: 200,
          body: [{}],
        });
        const body = responseConfig && responseConfig.body !== undefined
          ? responseConfig.body
          : (responseConfig || [{}]);
        const status = responseConfig && responseConfig.status
          ? responseConfig.status
          : 200;
        return new Response(JSON.stringify(body), {
          status,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      if (url.includes('/.netlify/functions/verify-public-tourist-tax-payment')) {
        const responseConfig = nextMockResponse('touristTaxVerificationResponses', 'touristTaxVerificationResponse', {
          status: 200,
          body: {
            ok: true,
            matched: true,
            status: 'matched',
            message: 'Ricevuta verificata: puoi continuare.',
            expected_amount: 12.5,
            extracted_amount: 12.5,
            difference: 0,
            tolerance_eur: 2.5,
            checked_at: '2026-06-26T12:00:00.000Z',
            provider: 'anthropic',
          },
        });
        window.__fetchCaptures.push({
          url,
          method: init.method || 'GET',
          ...captureBody(init.body),
        });
        return new Response(JSON.stringify(responseConfig.body || responseConfig), {
          status: responseConfig.status || 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      if (url.includes('/.netlify/functions/create-public-tourist-tax-stripe-checkout')) {
        if ((init.method || 'GET').toUpperCase() === 'GET') {
          const responseConfig = nextMockResponse('stripeAvailabilityResponses', 'stripeAvailabilityResponse', {
            status: 200,
            body: {
              ok: true,
              enabled: true,
            },
          });
          window.__fetchCaptures.push({
            url,
            method: init.method || 'GET',
            ...captureBody(init.body),
          });
          return new Response(JSON.stringify(responseConfig.body || responseConfig), {
            status: responseConfig.status || 200,
            headers: { 'Content-Type': 'application/json' },
          });
        }
        const responseConfig = nextMockResponse('stripeCheckoutResponses', 'stripeCheckoutResponse', {
          status: 200,
          body: {
            ok: true,
            session_id: 'cs_test_paid',
            url: 'https://checkout.stripe.com/c/pay/cs_test_paid',
            amount: 12.5,
            currency: 'EUR',
            livemode: false,
          },
        });
        window.__fetchCaptures.push({
          url,
          method: init.method || 'GET',
          ...captureBody(init.body),
        });
        return new Response(JSON.stringify(responseConfig.body || responseConfig), {
          status: responseConfig.status || 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      if (url.includes('/.netlify/functions/get-public-tourist-tax-stripe-session')) {
        const responseConfig = nextMockResponse('stripeSessionResponses', 'stripeSessionResponse', {
          status: 200,
          body: {
            ok: true,
            matched: true,
            status: 'paid',
            message: 'Pagamento Stripe verificato: puoi continuare.',
            session_id: 'cs_test_paid',
            payment_status: 'paid',
            session_status: 'complete',
            amount: 12.5,
            amount_total: 12.5,
            expected_amount: 12.5,
            difference: 0,
            currency: 'EUR',
            checked_at: '2026-06-26T12:00:00.000Z',
            provider: 'stripe',
            livemode: false,
            customer_email: 'guest@example.com',
          },
        });
        window.__fetchCaptures.push({
          url,
          method: init.method || 'GET',
          ...captureBody(init.body),
        });
        return new Response(JSON.stringify(responseConfig.body || responseConfig), {
          status: responseConfig.status || 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      if (url.includes('/.netlify/functions/create-public-checkin-upload-urls')) {
        let body = {};
        try {
          body = JSON.parse(typeof init.body === 'string' ? init.body : '{}');
        } catch (_error) {
          body = {};
        }
        const uploads = Array.isArray(body.document_uploads)
          ? body.document_uploads.map((entry, index) => ({
            client_id: entry.client_id || `doc-${index + 1}`,
            guest_scope: entry.guest_scope || 'main',
            guest_index: Number.isFinite(entry.guest_index) ? entry.guest_index : 0,
            display_order: Number.isFinite(entry.display_order) ? entry.display_order : index,
            file_name: entry.file_name || `document-${index + 1}.pdf`,
            mime_type: entry.mime_type || 'application/pdf',
            size_bytes: entry.size_bytes || 0,
            storage_bucket: 'checkin-documents',
            storage_path: `public-checkin/mock/${entry.client_id || `doc-${index + 1}`}_${index}.pdf`,
            signed_url: `https://storage.test/upload/${entry.client_id || `doc-${index + 1}`}`,
          }))
          : [];
        window.__fetchCaptures.push({
          url,
          method: init.method || 'GET',
          ...captureBody(init.body),
        });
        return new Response(JSON.stringify({
          ok: true,
          upload_batch_token: 'upload-batch-token',
          uploads,
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      if (url.startsWith('https://storage.test/upload/')) {
        window.__fetchCaptures.push({
          url,
          method: init.method || 'GET',
          ...captureBody(init.body),
        });
        return new Response('{}', {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      if (url.includes('/.netlify/functions/submit-public-checkin')) {
        const responseConfig = nextMockResponse('checkinResponses', 'checkinResponse', {
          status: 200,
          body: {
            ok: true,
            record: { id: 'test-record' },
            child_records: [],
            uploaded_documents: 1,
          },
        });
        window.__fetchCaptures.push({
          url,
          method: init.method || 'GET',
          ...captureBody(init.body),
        });
        if (responseConfig && responseConfig.throwError) {
          throw new TypeError(responseConfig.throwError);
        }
        return new Response(JSON.stringify(responseConfig.body || responseConfig), {
          status: responseConfig.status || 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      if (url.includes('/.netlify/functions/recover-public-checkin')) {
        const responseConfig = nextMockResponse('recoverResponses', 'recoverResponse', {
          status: 404,
          body: { ok: false, recovered: false, error: 'Check-in not found' },
        });
        window.__fetchCaptures.push({
          url,
          method: init.method || 'GET',
          ...captureBody(init.body),
        });
        return new Response(JSON.stringify(responseConfig.body || responseConfig), {
          status: responseConfig.status || 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      if (url.includes('/.netlify/functions/create-public-checkin-emergency-token')) {
        const responseConfig = nextMockResponse('emergencyResponses', 'emergencyResponse', {
          status: 200,
          body: {
            ok: true,
            token: 'emergency-token',
            expires_at: '2099-01-01T00:00:00.000Z',
          },
        });
        window.__fetchCaptures.push({
          url,
          method: init.method || 'GET',
          ...captureBody(init.body),
        });
        return new Response(JSON.stringify(responseConfig.body || responseConfig), {
          status: responseConfig.status || 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      if (url.includes('/.netlify/functions/submit-public-invoice-documents')) {
        const responseConfig = nextMockResponse('invoiceResponses', 'invoiceResponse', {
          status: 200,
          body: {
            ok: true,
            attachments_saved: 1,
          },
        });
        window.__fetchCaptures.push({
          url,
          method: init.method || 'GET',
          ...captureBody(init.body),
        });
        return new Response(JSON.stringify(responseConfig.body || responseConfig), {
          status: responseConfig.status || 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      if (url.includes('/.netlify/functions/get-public-portal-data')) {
        return new Response(JSON.stringify(config.portalResponse || {
          guest: null,
          apartment: { nome_appartamento: 'Appartamento Test' },
          creds: null,
          info: null,
          meta: { apartment_found: true },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      return realFetch(input, init);
    };
  }, options);
}

async function fillRequiredCheckinFields(page) {
  await page.fill('#checkin', '2026-06-10');
  await page.fill('#checkout', '2026-06-12');
  await page.fill('#numeroPersone', '1');
  await page.fill('#nome', 'Mario');
  await page.fill('#cognome', 'Rossi');
  await page.fill('#email', 'mario.rossi@example.com');
  await page.fill('#telefono', '+393331112233');
  await page.fill('#dataNascita', '1980-01-01');
  await page.fill('#luogoNascita', 'Riccione');
  await page.selectOption('#sesso', 'M');
  await page.fill('#cittadinanza', 'Italia');
  await page.selectOption('#tipoDocumento', "Carta d'identita");
  await page.fill('#numeroDocumento', 'AZ1234567');
  await page.fill('#luogoRilascioDocumento', 'Riccione');
  await page.fill('#cf', 'RSSMRA80A01H501U');
  await page.fill('#indirizzo', 'Via Roma 1');
  await page.fill('#citta', 'Riccione');
  await page.fill('#cap', '47838');
  await page.fill('#provincia', 'RN');
  await page.check('#privacyCheck');
}

async function fillAdditionalGuestRequiredFields(page, index = 0) {
  await page.fill(`#additionalGuest_${index}_nome`, 'Luigi');
  await page.fill(`#additionalGuest_${index}_cognome`, 'Verdi');
  await page.selectOption(`#additionalGuest_${index}_sesso`, 'M');
  await page.fill(`#additionalGuest_${index}_data_nascita`, '1990-02-02');
  await page.fill(`#additionalGuest_${index}_luogo_nascita`, 'Riccione');
  await page.fill(`#additionalGuest_${index}_stato_nascita`, 'Italia');
  await page.fill(`#additionalGuest_${index}_cittadinanza`, 'Italia');
}

async function fillRequiredInvoiceFields(page) {
  await page.fill('#ragione_sociale', 'Azienda Test SRL');
  await page.fill('#referente_nome', 'Lucia');
  await page.fill('#referente_cognome', 'Bianchi');
  await page.fill('#email', 'amministrazione@example.com');
  await page.fill('#telefono', '+393331112233');
  await page.fill('#piva_cliente', 'IT01234567890');
  await page.fill('#indirizzo_fatturazione', 'Via Milano 10, Milano');
  await page.check('#privacy_accepted');
}

function createComuneLookupSupabase(rows, stateRows = []) {
  return {
    from(tableName) {
      if (tableName !== 'codici_comuni' && tableName !== 'codici_stati') {
        throw new Error(`Unexpected table lookup: ${tableName}`);
      }

      const state = { pattern: '', column: 'nome' };
      const sourceRows = tableName === 'codici_stati' ? stateRows : rows;
      return {
        select() {
          return this;
        },
        ilike(column, pattern) {
          state.column = String(column || 'nome');
          state.pattern = String(pattern || '');
          return this;
        },
        order() {
          return this;
        },
        limit(limitValue) {
          return Promise.resolve({
            data: sourceRows.filter((row) => ilikeMatches(row[state.column], state.pattern)).slice(0, limitValue),
            error: null,
          });
        },
      };
    },
  };
}

function ilikeMatches(value, pattern) {
  const regex = new RegExp(
    `^${String(pattern || '')
      .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
      .replace(/%/g, '.*')
      .replace(/_/g, '.')}$`,
    'i'
  );
  return regex.test(String(value || ''));
}

test('check-in accetta PDF e invia multipart con metadati allegati', async ({ page }) => {
  await installFetchMock(page);
  await page.goto('/?apt=test-apt&mode=checkin', { waitUntil: 'domcontentloaded' });

  await page.setInputFiles('#mainFileInput', {
    name: 'documento.pdf',
    mimeType: 'application/pdf',
    buffer: Buffer.from('%PDF-1.4 test'),
  });

  await expect(page.locator('#mainOcrMsg')).toContainText('Documento allegato');
  await fillRequiredCheckinFields(page);

  await page.locator('button[data-t="continua"]').click({ force: true });
  await page.click('#submitBtn');

  await expect(page.locator('#successScreen')).toBeVisible();

  const capture = await page.evaluate(() => window.__fetchCaptures[0]);
  expect(capture.isFormData).toBe(true);

  const uploadMetadataEntry = capture.entries.find((entry) => entry.key === 'document_uploads');
  expect(uploadMetadataEntry).toBeTruthy();

  const uploadMetadata = JSON.parse(uploadMetadataEntry.value);
  expect(uploadMetadata).toHaveLength(1);
  expect(uploadMetadata[0].guest_scope).toBe('main');

  const fileEntries = capture.entries.filter((entry) => entry.kind === 'file');
  expect(fileEntries).toHaveLength(1);
  expect(fileEntries[0].name).toBe('documento.pdf');
  expect(fileEntries[0].type).toBe('application/pdf');
});

test('prenotazione Booking richiede tassa di soggiorno e allega la prova bonifico', async ({ page }) => {
  await installFetchMock(page);
  await page.goto('/?apt=test-apt&mode=checkin&src=booking', { waitUntil: 'domcontentloaded' });
  await expect(page.locator('#touristTaxCard')).toBeVisible();

  await page.setInputFiles('#mainFileInput', {
    name: 'documento.pdf',
    mimeType: 'application/pdf',
    buffer: Buffer.from('%PDF-1.4 main guest'),
  });
  await fillRequiredCheckinFields(page);

  await page.click('button[data-t="continua"]');

  await page.click('#submitBtn');
  await expect(page.locator('#topAlert')).toContainText(/tassa di soggiorno/i);
  await expect(page.locator('#panel1')).toHaveClass(/active/);
  await expect(page.locator('#successScreen')).toBeHidden();

  let captures = await page.evaluate(() => window.__fetchCaptures);
  expect(captures).toHaveLength(0);

  await page.fill('#touristTaxAmount', '12,50');
  await page.check('#touristTaxConfirm');
  await page.setInputFiles('#touristTaxProofInput', {
    name: 'bonifico.pdf',
    mimeType: 'application/pdf',
    buffer: Buffer.from('%PDF-1.4 tourist tax'),
  });

  await page.click('button[data-t="continua"]');
  await page.click('#submitBtn');
  await expect(page.locator('#successScreen')).toBeVisible();

  captures = await page.evaluate(() => window.__fetchCaptures);
  expect(captures).toHaveLength(2);
  expect(captures[0].url).toContain('/.netlify/functions/verify-public-tourist-tax-payment');
  expect(captures[1].url).toContain('/.netlify/functions/submit-public-checkin');
  expect(captures[1].isFormData).toBe(true);

  const payloadEntry = captures[1].entries.find((entry) => entry.key === 'payload');
  expect(payloadEntry).toBeTruthy();
  const payload = JSON.parse(payloadEntry.value);
  expect(payload.source_channel_hint).toBe('booking');
  expect(payload.tourist_tax_payment).toMatchObject({
    required: true,
    channel_hint: 'booking',
    method: 'bank_transfer',
    amount: 12.5,
    currency: 'EUR',
    booking_amount_confirmed: true,
    beneficiary: 'Impatto S.R.L.',
    bic: 'CCRTIT2TMAL',
    verification: {
      matched: true,
      status: 'matched',
      extracted_amount: 12.5,
    },
  });

  const uploadMetadataEntry = captures[1].entries.find((entry) => entry.key === 'document_uploads');
  expect(uploadMetadataEntry).toBeTruthy();
  const uploadMetadata = JSON.parse(uploadMetadataEntry.value);
  expect(uploadMetadata).toHaveLength(2);
  expect(uploadMetadata.map((entry) => entry.guest_scope).sort()).toEqual(['main', 'tourist_tax']);
});

test('prenotazione Booking blocca il submit se la ricevuta del bonifico non torna e invita a scrivere a Serena', async ({ page }) => {
  await installFetchMock(page, {
    touristTaxVerificationResponse: {
      status: 200,
      body: {
        ok: true,
        matched: false,
        status: 'amount_mismatch',
        message: 'Caricamento fallito: l importo letto dalla ricevuta (10,00 EUR) non coincide con il totale inserito (12,50 EUR). Contatta Serena su WhatsApp.',
        expected_amount: 12.5,
        extracted_amount: 10,
        difference: 2.5,
        tolerance_eur: 2.5,
        checked_at: '2026-06-26T12:00:00.000Z',
        provider: 'anthropic',
      },
    },
  });
  await page.goto('/?apt=test-apt&mode=checkin&src=booking', { waitUntil: 'domcontentloaded' });
  await expect(page.locator('#touristTaxCard')).toBeVisible();

  await page.setInputFiles('#mainFileInput', {
    name: 'documento.pdf',
    mimeType: 'application/pdf',
    buffer: Buffer.from('%PDF-1.4 main guest'),
  });
  await fillRequiredCheckinFields(page);
  await page.fill('#touristTaxAmount', '12,50');
  await page.check('#touristTaxConfirm');
  await page.setInputFiles('#touristTaxProofInput', {
    name: 'bonifico.pdf',
    mimeType: 'application/pdf',
    buffer: Buffer.from('%PDF-1.4 tourist tax mismatch'),
  });

  await page.click('button[data-t="continua"]');
  await page.click('#submitBtn');

  await expect(page.locator('#panel1')).toHaveClass(/active/);
  await expect(page.locator('#touristTaxVerificationStatus')).toContainText(/Caricamento fallito/i);
  await expect(page.locator('#topAlert')).toContainText(/Contatta Serena su WhatsApp/i);
  await expect(page.locator('#touristTaxWhatsappLink')).toBeVisible();

  const captures = await page.evaluate(() => window.__fetchCaptures);
  expect(captures).toHaveLength(1);
  expect(captures[0].url).toContain('/.netlify/functions/verify-public-tourist-tax-payment');
});

test('prenotazione Booking permette pagamento Stripe e invia la sessione verificata', async ({ page }) => {
  await installFetchMock(page, {
    stripeCheckoutResponse: {
      status: 200,
      body: {
        ok: true,
        session_id: 'cs_test_paid',
        url: 'https://checkout.stripe.com/c/pay/cs_test_paid',
        amount: 12.5,
        currency: 'EUR',
        livemode: false,
      },
    },
    stripeSessionResponse: {
      status: 200,
      body: {
        ok: true,
        matched: true,
        status: 'paid',
        message: 'Pagamento Stripe verificato: puoi continuare.',
        session_id: 'cs_test_paid',
        payment_status: 'paid',
        session_status: 'complete',
        amount: 12.5,
        amount_total: 12.5,
        expected_amount: 12.5,
        difference: 0,
        currency: 'EUR',
        checked_at: '2026-06-26T12:00:00.000Z',
        provider: 'stripe',
        livemode: false,
        customer_email: 'guest@example.com',
      },
    },
  });

  await page.goto('/?apt=test-apt&mode=checkin&src=booking', { waitUntil: 'domcontentloaded' });
  await page.fill('#touristTaxAmount', '12,50');
  await page.check('#touristTaxConfirm');
  await page.check('#touristTaxMethodStripe');
  await page.click('#touristTaxStripeBtn');

  let captures = await page.evaluate(() => window.__fetchCaptures);
  const checkoutCaptures = captures.filter((entry) =>
    entry.url.includes('/.netlify/functions/create-public-tourist-tax-stripe-checkout')
    && String(entry.method || '').toUpperCase() === 'POST'
  );
  expect(checkoutCaptures).toHaveLength(1);

  const openedUrls = await page.evaluate(() => window.__openedUrls);
  expect(openedUrls.some((url) => url.includes('https://checkout.stripe.com/c/pay/cs_test_paid'))).toBe(true);

  await page.goto('/?apt=test-apt&mode=checkin&src=booking&stripe_checkout=success&stripe_session_id=cs_test_paid', { waitUntil: 'domcontentloaded' });
  await expect(page.locator('#touristTaxMethodStripe')).toBeChecked();
  await expect(page.locator('#touristTaxVerificationStatus')).toContainText(/Stripe/i);
  await expect(page.locator('#touristTaxAmount')).toHaveValue('12,50');

  await page.setInputFiles('#mainFileInput', {
    name: 'documento.pdf',
    mimeType: 'application/pdf',
    buffer: Buffer.from('%PDF-1.4 main guest'),
  });
  await fillRequiredCheckinFields(page);

  await page.click('button[data-t="continua"]');
  await page.click('#submitBtn');
  await expect(page.locator('#successScreen')).toBeVisible();

  captures = await page.evaluate(() => window.__fetchCaptures);
  expect(captures).toHaveLength(2);
  expect(captures[0].url).toContain('/.netlify/functions/get-public-tourist-tax-stripe-session');
  expect(captures[1].url).toContain('/.netlify/functions/submit-public-checkin');

  const payloadEntry = captures[1].entries.find((entry) => entry.key === 'payload');
  expect(payloadEntry).toBeTruthy();
  const payload = JSON.parse(payloadEntry.value);
  expect(payload.source_channel_hint).toBe('booking');
  expect(payload.tourist_tax_payment).toMatchObject({
    required: true,
    channel_hint: 'booking',
    method: 'stripe_checkout',
    amount: 12.5,
    currency: 'EUR',
    booking_amount_confirmed: true,
    verification: {
      matched: true,
      provider: 'stripe',
      session_id: 'cs_test_paid',
      payment_status: 'paid',
    },
  });

  const uploadMetadataEntry = captures[1].entries.find((entry) => entry.key === 'document_uploads');
  expect(uploadMetadataEntry).toBeTruthy();
  const uploadMetadata = JSON.parse(uploadMetadataEntry.value);
  expect(uploadMetadata).toHaveLength(1);
  expect(uploadMetadata[0].guest_scope).toBe('main');
});

test('il link check-in con apt resta sul check-in e non apre subito il portale', async ({ page }) => {
  await installFetchMock(page);
  await page.goto('/?apt=test-apt', { waitUntil: 'domcontentloaded' });

  await expect(page.locator('#mainContent')).toBeVisible();
  await expect(page).toHaveURL(/\/\?apt=test-apt$/);
});

test('richiesta fattura invia davvero il PDF allegato', async ({ page }) => {
  await installFetchMock(page);
  await page.goto('/carica-documenti-fattura.html', { waitUntil: 'domcontentloaded' });
  await fillRequiredInvoiceFields(page);
  await page.setInputFiles('#files', {
    name: 'visura.pdf',
    mimeType: 'application/pdf',
    buffer: Buffer.from('%PDF-1.4 visura'),
  });

  await page.click('#submitBtn');
  await expect(page.locator('#success.show')).toBeVisible();

  const capture = await page.evaluate(() => window.__fetchCaptures[0]);
  expect(capture.isFormData).toBe(true);
  const fileEntries = capture.entries.filter((entry) => entry.kind === 'file');
  expect(fileEntries).toHaveLength(1);
  expect(fileEntries[0].name).toBe('visura.pdf');
});

test('check-in ritenta il salvataggio dopo un errore temporaneo', async ({ page }) => {
  await installFetchMock(page, {
    checkinResponses: [
      { status: 503, body: { error: 'Temporary outage' } },
      { status: 200, body: { ok: true, record: { id: 'test-record' }, child_records: [], uploaded_documents: 1 } },
    ],
  });
  await page.goto('/?apt=test-apt&mode=checkin', { waitUntil: 'domcontentloaded' });

  await page.setInputFiles('#mainFileInput', {
    name: 'documento.pdf',
    mimeType: 'application/pdf',
    buffer: Buffer.from('%PDF-1.4 retry'),
  });
  await fillRequiredCheckinFields(page);

  await page.click('button[data-t="continua"]');
  await page.click('#submitBtn');

  await expect(page.locator('#successScreen')).toBeVisible();
  const captures = await page.evaluate(() => window.__fetchCaptures);
  expect(captures).toHaveLength(2);
});

test('se la risposta del submit si perde ma il check-in e gia salvato, il frontend lo recupera automaticamente', async ({ page }) => {
  await installFetchMock(page, {
    checkinResponses: [
      { throwError: 'Network connection lost' },
      { throwError: 'Network connection lost' },
      { throwError: 'Network connection lost' },
    ],
    recoverResponse: {
      status: 200,
      body: {
        ok: true,
        recovered: true,
        record: { id: 'recovered-record' },
        child_records: [],
        portal_url: '/portale.html?token=recovered-token&apt=test-apt',
      },
    },
  });
  await page.goto('/?apt=test-apt&mode=checkin', { waitUntil: 'domcontentloaded' });

  await page.setInputFiles('#mainFileInput', {
    name: 'documento.pdf',
    mimeType: 'application/pdf',
    buffer: Buffer.from('%PDF-1.4 recovered'),
  });
  await fillRequiredCheckinFields(page);

  await page.click('button[data-t="continua"]');
  await page.click('#submitBtn');

  await expect(page.locator('#successScreen')).toBeVisible();
  await expect(page.locator('#toast')).toContainText('recuperato');

  const captures = await page.evaluate(() => window.__fetchCaptures);
  expect(captures).toHaveLength(4);
  expect(captures.slice(0, 3).every((entry) => entry.url.includes('/.netlify/functions/submit-public-checkin'))).toBe(true);
  expect(captures[3].url).toContain('/.netlify/functions/recover-public-checkin');
});

test('se il recupero trova il check-in solo dopo qualche secondo, il frontend continua a cercarlo prima di mostrare errore', async ({ page }) => {
  await installFetchMock(page, {
    checkinResponses: [
      { throwError: 'Network connection lost' },
      { throwError: 'Network connection lost' },
      { throwError: 'Network connection lost' },
    ],
    recoverResponses: [
      { status: 404, body: { ok: false, recovered: false, error: 'Check-in not found' } },
      { status: 404, body: { ok: false, recovered: false, error: 'Check-in not found' } },
      {
        status: 200,
        body: {
          ok: true,
          recovered: true,
          record: { id: 'delayed-recovered-record' },
          child_records: [],
          portal_url: '/portale.html?token=delayed-token&apt=test-apt',
        },
      },
    ],
  });
  await page.goto('/?apt=test-apt&mode=checkin', { waitUntil: 'domcontentloaded' });

  await fillRequiredCheckinFields(page);
  await page.setInputFiles('#mainFileInput', {
    name: 'documento.pdf',
    mimeType: 'application/pdf',
    buffer: Buffer.from('%PDF-1.4 recovered delayed'),
  });

  await page.click('button[data-t="continua"]');
  await page.click('#submitBtn');

  await expect(page.locator('#successScreen')).toBeVisible();
  await expect(page.locator('#toast')).toContainText('recuperato');

  const captures = await page.evaluate(() => window.__fetchCaptures);
  expect(captures).toHaveLength(6);
  expect(captures.slice(0, 3).every((entry) => entry.url.includes('/.netlify/functions/submit-public-checkin'))).toBe(true);
  expect(captures.slice(3).every((entry) => entry.url.includes('/.netlify/functions/recover-public-checkin'))).toBe(true);
});

test('il percorso di emergenza WhatsApp sblocca il submit senza documenti e poi mostra il portale', async ({ page }) => {
  await installFetchMock(page, {
    checkinResponse: {
      status: 200,
      body: {
        ok: true,
        record: { id: 'test-record' },
        child_records: [],
        uploaded_documents: 0,
        portal_url: '/portale.html?token=test-token&apt=test-apt',
      },
    },
  });
  await page.goto('/?apt=test-apt', { waitUntil: 'domcontentloaded' });
  await fillRequiredCheckinFields(page);

  await page.click('button[data-t="continua"]');
  await page.click('#submitBtn');

  let captures = await page.evaluate(() => window.__fetchCaptures);
  expect(captures).toHaveLength(0);

  await page.click('#documentEmergencyBtn');
  const openedUrls = await page.evaluate(() => window.__openedUrls);
  expect(openedUrls[0]).toContain('https://wa.me/393501258030');
  await expect.poll(async () => {
    const captures = await page.evaluate(() => window.__fetchCaptures.length);
    return captures;
  }).toBe(1);

  await page.click('#submitBtn');
  await expect(page.locator('#successScreen')).toBeVisible();
  await expect(page.locator('#successActions')).toContainText('WhatsApp');
  await expect(page.locator('#successActions')).toContainText('Apri il portale');
  await expect(page).toHaveURL(/\/\?apt=test-apt$/);

  captures = await page.evaluate(() => window.__fetchCaptures);
  expect(captures).toHaveLength(2);
  expect(captures[0].url).toContain('/.netlify/functions/create-public-checkin-emergency-token');
  expect(captures[1].isFormData).toBe(false);
  const payload = JSON.parse(captures[1].bodyText);
  expect(payload.document_upload_issue).toBe(true);
  expect(payload.document_emergency_token).toBe('emergency-token');
});

test('richiesta fattura ritenta il salvataggio dopo un errore temporaneo', async ({ page }) => {
  await installFetchMock(page, {
    invoiceResponses: [
      { status: 502, body: { error: 'Temporary outage' } },
      { status: 200, body: { ok: true, attachments_saved: 1 } },
    ],
  });
  await page.goto('/carica-documenti-fattura.html', { waitUntil: 'domcontentloaded' });

  await expect(page.locator('a[href="/privacy-policy.html"]')).toBeVisible();
  await fillRequiredInvoiceFields(page);
  await page.setInputFiles('#files', {
    name: 'visura.pdf',
    mimeType: 'application/pdf',
    buffer: Buffer.from('%PDF-1.4 retry'),
  });

  await page.click('#submitBtn');
  await expect(page.locator('#success.show')).toBeVisible();
  const captures = await page.evaluate(() => window.__fetchCaptures);
  expect(captures).toHaveLength(2);
});

test('portale con token renderizza card e link principali', async ({ page }) => {
  await installFetchMock(page, {
    portalResponse: {
      guest: {
        nome: 'Mario',
        cognome: 'Rossi',
        lingua: 'IT',
        data_checkin: '2026-06-10',
        data_checkout: '2026-06-12',
      },
      apartment: {
        nome_appartamento: 'Suite Test',
        indirizzo_completo: 'Via Roma 1, Riccione',
        maps_url_override: 'https://maps.example.com/test',
      },
      creds: {
        codice_accesso: '1234',
        luogo_chiavi: 'Cassetta esterna',
        istruzioni_accesso: 'Apri il cancello e prendi le chiavi.',
        video_url: 'https://www.youtube.com/watch?v=dQw4w9WgXcQ',
      },
      info: {
        come_arrivare: 'Segui il lungomare.',
        dove_parcheggiare: 'Parcheggio blu davanti al civico.',
        regole_della_casa: 'No fumo.',
        cosa_fare_se_luce_spenta: 'Controlla il contatore.',
        cosa_fare_se_chiuso_fuori: 'Chiama il supporto.',
        numeri_utili: {
          Supporto: '+393501258030',
        },
      },
      meta: { apartment_found: true, used_fallback_open: false },
    },
  });

  await page.goto('/portale.html?token=test-token&apt=test-apt', { waitUntil: 'domcontentloaded' });

  await expect(page.locator('#guest-name')).toContainText('Mario Rossi');
  await expect(page.locator('#apt-name')).toContainText('Suite Test');
  await expect(page.locator('#header-stay')).toContainText('Check-in');
  await expect(page.locator('button:has-text("Accesso Appartamento")')).toBeVisible();
  await expect(page.locator('.copy-btn').first()).toBeVisible();
});

test('portale open mode senza token apre il portale appartamento', async ({ page }) => {
  await installFetchMock(page);
  await page.goto('/portale.html?apt=test-apt&mode=open', { waitUntil: 'domcontentloaded' });

  await expect(page.locator('#app')).toBeVisible();
  await expect(page.locator('#guest-name')).toContainText('Appartamento Test');
  await expect(page.locator('#header-stay')).toContainText(/appartamento/i);
  const captures = await page.evaluate(() => window.__fetchCaptures);
  expect(captures).toHaveLength(0);
});

test('con due ospiti il submit resta bloccato finche manca almeno un documento', async ({ page }) => {
  await installFetchMock(page);
  await page.goto('/?apt=test-apt&mode=checkin', { waitUntil: 'domcontentloaded' });

  await fillRequiredCheckinFields(page);
  await page.fill('#numeroPersone', '2');
  await page.dispatchEvent('#numeroPersone', 'change');
  await fillAdditionalGuestRequiredFields(page, 0);
  await page.setInputFiles('#mainFileInput', {
    name: 'documento-principale.pdf',
    mimeType: 'application/pdf',
    buffer: Buffer.from('%PDF-1.4 main'),
  });

  await page.click('button[data-t="continua"]');
  const validationErrors = await page.evaluate(() => validateClientData({ requireDocuments: true }));
  expect(validationErrors.some((entry) => entry.id === 'documentFlowField')).toBe(true);
  await expect(page.locator('#documentFlowStatus')).toContainText('Mancano: ospite 2');

  await page.evaluate(() => submitForm());
  const captures = await page.evaluate(() => window.__fetchCaptures);
  expect(captures).toHaveLength(0);
});

test('ospite aggiuntivo nato all estero non richiede il comune di nascita', async ({ page }) => {
  await installFetchMock(page);
  await page.goto('/?apt=test-apt&mode=checkin', { waitUntil: 'domcontentloaded' });

  await fillRequiredCheckinFields(page);
  await page.fill('#numeroPersone', '2');
  await page.dispatchEvent('#numeroPersone', 'change');
  await page.fill('#additionalGuest_0_nome', 'Anna');
  await page.fill('#additionalGuest_0_cognome', 'Martin');
  await page.selectOption('#additionalGuest_0_sesso', 'F');
  await page.fill('#additionalGuest_0_data_nascita', '1992-03-03');
  await page.fill('#additionalGuest_0_stato_nascita', 'Francia');
  await page.fill('#additionalGuest_0_cittadinanza', 'Francia');
  await page.setInputFiles('#mainFileInput', {
    name: 'documento-principale.pdf',
    mimeType: 'application/pdf',
    buffer: Buffer.from('%PDF-1.4 main estero'),
  });
  await page.setInputFiles('#additionalGuest_0_file', {
    name: 'documento-ospite.pdf',
    mimeType: 'application/pdf',
    buffer: Buffer.from('%PDF-1.4 guest estero'),
  });

  const validationErrors = await page.evaluate(() => validateClientData({ requireDocuments: true }));
  expect(validationErrors.some((entry) => entry.id === 'additionalGuest_0_luogo_nascita')).toBe(false);
  expect(validationErrors.some((entry) => entry.id === 'additionalGuest_0_stato_nascita')).toBe(false);
});

test('backend risolve il comune italiano anche senza accento', async () => {
  const supabase = createComuneLookupSupabase([
    { codice: 'D704', nome: 'Forlì', provincia: 'FC' },
  ]);

  const result = await submitPublicCheckinTestApi.findUniqueComuneCode(supabase, 'Forli');
  expect(result.status).toBe('matched');
  expect(result.code).toBe('D704');
});

test('backend risolve il comune italiano con apostrofo e provincia digitata', async () => {
  const supabase = createComuneLookupSupabase([
    { codice: 'A345', nome: "L'Aquila", provincia: 'AQ' },
  ]);

  const result = await submitPublicCheckinTestApi.findUniqueComuneCode(supabase, 'L Aquila (AQ)');
  expect(result.status).toBe('matched');
  expect(result.code).toBe('A345');
});

test('backend preferisce il codice comune corrente quando il duplicato e storico', async () => {
  const supabase = createComuneLookupSupabase([
    { codice: '408040518', nome: 'Riccione', provincia: 'FO' },
    { codice: '408099013', nome: 'Riccione', provincia: 'RN' },
  ]);

  const result = await submitPublicCheckinTestApi.findUniqueComuneCode(supabase, 'Riccione');
  expect(result.status).toBe('matched');
  expect(result.code).toBe('408099013');
});

test('backend accetta una sessione Stripe completa per la tassa di soggiorno Booking', async () => {
  const verification = stripeTouristTaxTestApi.evaluateStripeCheckoutSession({
    session: {
      id: 'cs_test_paid',
      status: 'complete',
      payment_status: 'paid',
      amount_total: 1250,
      currency: 'eur',
      livemode: false,
      metadata: {
        kind: 'booking_public_checkin_tourist_tax',
        apartment_ref: 'test-apt',
        channel_hint: 'booking',
        tourist_tax_amount: '12.50',
      },
      customer_details: {
        email: 'guest@example.com',
      },
    },
    expectedAmount: 12.5,
    expectedCurrency: 'EUR',
    expectedApartmentRef: 'test-apt',
    expectedChannelHint: 'booking',
  });

  expect(verification.matched).toBe(true);
  expect(verification.status).toBe('paid');
  expect(verification.amount).toBe(12.5);
  expect(verification.session_id).toBe('cs_test_paid');
  expect(verification.payment_status).toBe('paid');
});

test('backend risolve il luogo rilascio usando la provincia di residenza quando il comune e duplicato', async () => {
  const supabase = createComuneLookupSupabase([
    { codice: '408040518', nome: 'Riccione', provincia: 'FO' },
    { codice: '408099013', nome: 'Riccione', provincia: 'RN' },
  ]);

  const result = await submitPublicCheckinTestApi.findUniqueComuneCode(supabase, 'Riccione', { fallbackProvince: 'RN' });
  expect(result.status).toBe('matched');
  expect(result.code).toBe('408099013');
});

test('backend normalizza questura di rimini come luogo rilascio italiano', async () => {
  const supabase = createComuneLookupSupabase([
    { codice: '399014061', nome: 'Rimini', provincia: 'RN' },
  ]);

  const result = await submitPublicCheckinTestApi.findIssuePlaceMatch(supabase, {
    luogo_rilascio_documento: 'Questura di Rimini',
    indirizzo_residenza: 'Via Roma 1, Rimini, 47921, RN',
  });
  expect(result.status).toBe('matched');
  expect(result.code).toBe('399014061');
});

test('backend accetta uno stato estero come luogo rilascio documento', async () => {
  const supabase = createComuneLookupSupabase([]);

  const result = await submitPublicCheckinTestApi.findIssuePlaceMatch(supabase, {
    luogo_rilascio_documento: 'Francia',
    indirizzo_residenza: '',
  });
  expect(result.status).toBe('matched');
  expect(result.code).toBeTruthy();
});

test('il sito portale include la function lookup-comuni nel bundle previsto', async () => {
  const { siteRegistry } = await import('../sites/site-registry.mjs');
  expect(siteRegistry.portale.functions).toContain('lookup-comuni');
});

test('il sito portale include comuni.csv per risolvere i duplicati storici dei comuni', async () => {
  const { siteRegistry } = await import('../sites/site-registry.mjs');
  expect(siteRegistry.portale.assets).toContain('comuni.csv');
});

test('il submit aspetta la risoluzione dei comuni italiani prima di validare', async ({ page }) => {
  await installFetchMock(page, {
    lookupComuniDelayMs: 700,
    lookupComuniResults: {
      'Riccione|': {
        status: 200,
        body: {
          matches: [
            { codice: '408099013', nome: 'Riccione', provincia: 'RN', cap: '47838' },
          ],
        },
      },
    },
  });
  await page.goto('/?apt=test-apt&mode=checkin', { waitUntil: 'domcontentloaded' });

  await page.setInputFiles('#mainFileInput', {
    name: 'documento.pdf',
    mimeType: 'application/pdf',
    buffer: Buffer.from('%PDF-1.4 race'),
  });
  await fillRequiredCheckinFields(page);

  await page.click('button[data-t="continua"]');
  await page.click('#submitBtn');

  await expect(page.locator('#successScreen')).toBeVisible();

  const payloadEntry = await page.evaluate(() => {
    const submitCapture = window.__fetchCaptures.find((entry) => entry.url.includes('/.netlify/functions/submit-public-checkin'));
    if (!submitCapture || !submitCapture.isFormData) return null;
    return submitCapture.entries.find((entry) => entry.key === 'payload') || null;
  });
  expect(payloadEntry).toBeTruthy();

  const payload = JSON.parse(payloadEntry.value);
  expect(payload.luogo_nascita_codice).toBe('408099013');
  expect(payload.luogo_rilascio_codice).toBe('408099013');
});

test('il luogo rilascio documento usa la provincia di residenza per scegliere il comune duplicato', async ({ page }) => {
  await installFetchMock(page, {
    lookupComuniResults: {
      'Riccione|RN': {
        status: 200,
        body: {
          matches: [
            { codice: '408099013', nome: 'Riccione', provincia: 'RN', cap: '47838' },
          ],
        },
      },
      'Riccione|': {
        status: 200,
        body: {
          matches: [
            { codice: '408040518', nome: 'Riccione', provincia: 'FO', cap: null },
            { codice: '408099013', nome: 'Riccione', provincia: 'RN', cap: '47838' },
          ],
        },
      },
    },
  });
  await page.goto('/?apt=test-apt&mode=checkin', { waitUntil: 'domcontentloaded' });

  await fillRequiredCheckinFields(page);
  await page.fill('#luogoRilascioDocumento', 'Riccione');
  await page.dispatchEvent('#luogoRilascioDocumento', 'input');
  await page.click('button[data-t="continua"]');

  await expect(page.locator('#issuePlaceChoiceField')).toBeHidden();
  const issueCode = await page.locator('#luogoRilascioCodice').inputValue();
  expect(issueCode).toBe('408099013');
});

test('il luogo rilascio documento normalizza questura di rimini nel lookup UI', async ({ page }) => {
  await installFetchMock(page, {
    lookupComuniResults: {
      'Riccione|RN': {
        status: 200,
        body: {
          matches: [
            { codice: '408099013', nome: 'Riccione', provincia: 'RN', cap: '47838' },
          ],
        },
      },
      'Riccione|': {
        status: 200,
        body: {
          matches: [
            { codice: '408099013', nome: 'Riccione', provincia: 'RN', cap: '47838' },
          ],
        },
      },
      'Rimini|RN': {
        status: 200,
        body: {
          matches: [
            { codice: '399014061', nome: 'Rimini', provincia: 'RN', cap: '47921' },
          ],
        },
      },
    },
  });
  await page.goto('/?apt=test-apt&mode=checkin', { waitUntil: 'domcontentloaded' });

  await fillRequiredCheckinFields(page);
  await page.fill('#citta', 'Rimini');
  await page.fill('#provincia', 'RN');
  await page.fill('#luogoRilascioDocumento', 'Questura di Rimini');
  await page.dispatchEvent('#luogoRilascioDocumento', 'input');
  await page.click('button[data-t="continua"]');

  await expect(page.locator('#issuePlaceChoiceField')).toBeHidden();
  await expect.poll(async () => page.locator('#luogoRilascioCodice').inputValue()).toBe('399014061');
});

test('ocr su documento estero compila nascita e rilascio senza bloccare il submit', async ({ page }) => {
  await installFetchMock(page, {
    ocrResponse: [{
      nome: 'Jean',
      cognome: 'Dupont',
      data_nascita: '1985-02-03',
      sesso: 'M',
      cittadinanza: 'France',
      stato_nascita: 'France',
      tipo_documento: 'Passaporto',
      numero_documento: 'FR1234567',
      luogo_nascita: 'France',
      luogo_rilascio: 'France',
    }],
  });
  await page.goto('/?apt=test-apt&mode=checkin', { waitUntil: 'domcontentloaded' });

  await page.selectOption('#paeseResidenza', 'FR');
  await page.fill('#checkin', '2026-06-10');
  await page.fill('#checkout', '2026-06-12');
  await page.fill('#numeroPersone', '1');
  await page.fill('#email', 'jean.dupont@example.com');
  await page.fill('#telefono', '+33601020304');
  await page.check('#privacyCheck');
  await page.setInputFiles('#mainFileInput', {
    name: 'passport.jpg',
    mimeType: 'image/jpeg',
    buffer: Buffer.from([0xff, 0xd8, 0xff, 0xdb, 0x00, 0x43]),
  });

  await expect.poll(async () => page.locator('#nome').inputValue()).toBe('Jean');
  await expect.poll(async () => page.locator('#natoInItalia').inputValue()).toBe('0');
  await expect.poll(async () => page.locator('#statoNascitaEstero').inputValue()).toMatch(/francia/i);
  await expect.poll(async () => page.locator('#luogoRilascioDocumento').inputValue()).toMatch(/francia/i);
  await expect(page.locator('#issuePlaceChoiceField')).toBeHidden();

  await page.click('button[data-t="continua"]');
  await page.click('#submitBtn');
  await expect(page.locator('#successScreen')).toBeVisible();

  const payloadEntry = await page.evaluate(() => {
    const submitCapture = window.__fetchCaptures.find((entry) => entry.url.includes('/.netlify/functions/submit-public-checkin'));
    if (!submitCapture || !submitCapture.isFormData) return null;
    return submitCapture.entries.find((entry) => entry.key === 'payload') || null;
  });
  expect(payloadEntry).toBeTruthy();

  const payload = JSON.parse(payloadEntry.value);
  expect(payload.nato_in_italia).toBe(false);
  expect(payload.stato_nascita).toMatch(/francia/i);
  expect(payload.luogo_rilascio_documento).toMatch(/francia/i);
});

test('cambio lingua traduce paesi e stati ma mantiene il payload canonico', async ({ page }) => {
  await installFetchMock(page);
  await page.goto('/?apt=test-apt&mode=checkin', { waitUntil: 'domcontentloaded' });

  await page.selectOption('#natoInItalia', '0');
  await page.fill('#statoNascitaEstero', 'Francia');
  await page.dispatchEvent('#statoNascitaEstero', 'change');
  await page.fill('#cittadinanza', 'Italia');
  await page.dispatchEvent('#cittadinanza', 'change');
  await page.fill('#luogoRilascioDocumento', 'Francia');
  await page.dispatchEvent('#luogoRilascioDocumento', 'input');

  await page.click('button:has-text("EN")');

  await expect.poll(async () => page.locator('#paeseResidenza option[value="GB"]').textContent()).toMatch(/United Kingdom/i);
  await expect.poll(async () => page.locator('#countryOptions option[value="France"]').count()).toBe(1);
  await expect(page.locator('#statoNascitaEstero')).toHaveValue('France');
  await expect(page.locator('#cittadinanza')).toHaveValue('Italy');
  await expect(page.locator('#luogoRilascioDocumento')).toHaveValue('France');

  const payload = await page.evaluate(() => buildPayload());
  expect(payload.stato_nascita).toMatch(/francia/i);
  expect(payload.cittadinanza).toMatch(/italia/i);
  expect(payload.luogo_rilascio_documento).toMatch(/francia/i);
});

test('codice fiscale non valido blocca il submit al primo tentativo con errore persistente', async ({ page }) => {
  await installFetchMock(page);
  await page.goto('/?apt=test-apt&mode=checkin', { waitUntil: 'domcontentloaded' });

  await page.setInputFiles('#mainFileInput', {
    name: 'documento.pdf',
    mimeType: 'application/pdf',
    buffer: Buffer.from('%PDF-1.4 invalid cf'),
  });
  await fillRequiredCheckinFields(page);
  await page.fill('#cf', 'ABC');

  await page.click('button[data-t="continua"]');

  await expect(page.locator('#cfWarning')).toBeVisible();
  await expect(page.locator('#topAlert')).toContainText(/codice fiscale/i);
  await expect(page.locator('#panel1')).toHaveClass(/active/);

  const captures = await page.evaluate(() => (
    window.__fetchCaptures.filter((entry) => entry.url.includes('/.netlify/functions/submit-public-checkin'))
  ));
  expect(captures).toHaveLength(0);
});

test('ospite UK puo inviare il check-in senza codice fiscale', async ({ page }) => {
  await installFetchMock(page);
  await page.goto('/?apt=test-apt&mode=checkin', { waitUntil: 'domcontentloaded' });

  await page.click('button:has-text("EN")');
  await page.selectOption('#paeseResidenza', 'GB');
  await page.fill('#checkin', '2026-06-24');
  await page.fill('#checkout', '2026-06-29');
  await page.fill('#numeroPersone', '1');
  await page.fill('#nome', 'Huw');
  await page.fill('#cognome', 'Jones');
  await page.fill('#email', 'huw@example.com');
  await page.fill('#telefono', '+447700900123');
  await page.fill('#dataNascita', '1985-02-03');
  await page.selectOption('#natoInItalia', '0');
  await page.fill('#statoNascitaEstero', 'UK');
  await page.dispatchEvent('#statoNascitaEstero', 'change');
  await page.selectOption('#sesso', 'M');
  await page.fill('#cittadinanza', 'UK');
  await page.dispatchEvent('#cittadinanza', 'change');
  await page.selectOption('#tipoDocumento', 'Passaporto');
  await page.fill('#numeroDocumento', 'GB1234567');
  await page.fill('#luogoRilascioDocumento', 'UK');
  await page.dispatchEvent('#luogoRilascioDocumento', 'input');
  await page.check('#privacyCheck');
  await page.setInputFiles('#mainFileInput', {
    name: 'passport.jpg',
    mimeType: 'image/jpeg',
    buffer: Buffer.from([0xff, 0xd8, 0xff, 0xd9]),
  });

  await page.click('button[data-t="continua"]');
  await page.click('#submitBtn');
  await expect(page.locator('#successScreen')).toBeVisible();

  const payloadEntry = await page.evaluate(() => {
    const submitCapture = window.__fetchCaptures.find((entry) => entry.url.includes('/.netlify/functions/submit-public-checkin'));
    if (!submitCapture || !submitCapture.isFormData) return null;
    return submitCapture.entries.find((entry) => entry.key === 'payload') || null;
  });
  expect(payloadEntry).toBeTruthy();

  const payload = JSON.parse(payloadEntry.value);
  expect(payload.paese_residenza).toBe('GB');
  expect(payload.codice_fiscale).toBe('');
  expect(payload.codice_fiscale_verificato).toBe(true);
  expect(payload.indirizzo_residenza).toBe('');
  expect(payload.stato_nascita).toMatch(/regno unito/i);
  expect(payload.cittadinanza).toMatch(/regno unito/i);
  expect(payload.luogo_rilascio_documento).toMatch(/regno unito/i);
});

test('il place of issue riconosce anche nomi estesi del paese estero', async ({ page }) => {
  await installFetchMock(page);
  await page.goto('/?apt=test-apt&mode=checkin', { waitUntil: 'domcontentloaded' });

  await page.click('button:has-text("EN")');
  await page.fill('#luogoRilascioDocumento', 'Great Britain and Northern Ireland');
  await page.dispatchEvent('#luogoRilascioDocumento', 'change');

  await expect.poll(async () => page.locator('#luogoRilascioDocumento').inputValue()).toMatch(/United Kingdom/i);
  await expect(page.locator('#luogoRilascioStatus')).toContainText(/Foreign issuing country/i);
  await expect(page.locator('#issuePlaceChoiceField')).toBeHidden();
});

test('documenti oltre il limite del gateway usano upload diretto e submit leggero', async ({ page }) => {
  await installFetchMock(page);
  await page.goto('/?apt=test-apt&mode=checkin', { waitUntil: 'domcontentloaded' });

  await page.setInputFiles('#mainFileInput', {
    name: 'documento-pesante.pdf',
    mimeType: 'application/pdf',
    buffer: Buffer.alloc((4 * 1024 * 1024) + 1024, 1),
  });
  await fillRequiredCheckinFields(page);

  await page.click('button[data-t="continua"]');
  await expect(page.locator('#documentFlowStatus')).toContainText(/Documenti completi/i);

  await page.click('#submitBtn');
  await expect(page.locator('#successScreen')).toBeVisible();

  const captures = await page.evaluate(() => window.__fetchCaptures);
  const prepCapture = captures.find((entry) => entry.url.includes('/.netlify/functions/create-public-checkin-upload-urls'));
  const storageCapture = captures.find((entry) => entry.url.startsWith('https://storage.test/upload/'));
  const submitCapture = captures.find((entry) => entry.url.includes('/.netlify/functions/submit-public-checkin'));

  expect(prepCapture).toBeTruthy();
  expect(storageCapture).toBeTruthy();
  expect(submitCapture).toBeTruthy();
  expect(submitCapture.isFormData).toBe(false);

  const payload = JSON.parse(submitCapture.bodyText);
  expect(payload.document_upload_issue).toBe(false);
  expect(payload.document_upload_batch_token).toBe('upload-batch-token');
  expect(Array.isArray(payload.document_uploads)).toBe(true);
  expect(payload.document_uploads).toHaveLength(1);
  expect(payload.document_uploads[0].storage_path).toMatch(/^public-checkin\//);
});

test('ocr su foto HEIC da iPhone prova comunque l autocompilazione', async ({ page }) => {
  await installFetchMock(page, {
    ocrResponse: [{
      nome: 'Mario',
      cognome: 'Rossi',
      data_nascita: '1980-01-01',
      sesso: 'M',
      cittadinanza: 'Italia',
      stato_nascita: 'Italia',
      tipo_documento: "Carta d'identita",
      numero_documento: 'AZ1234567',
      luogo_nascita: 'Riccione',
      luogo_rilascio: 'Riccione',
      codice_fiscale: 'RSSMRA80A01H501U',
    }],
  });
  await page.goto('/?apt=test-apt&mode=checkin', { waitUntil: 'domcontentloaded' });
  await page.evaluate(() => {
    window.prepareGuestFileForOcr = async () => new File(
      [new Uint8Array([0xff, 0xd8, 0xff, 0xd9])],
      'converted-ocr.jpg',
      { type: 'image/jpeg' },
    );
  });

  await page.setInputFiles('#mainFileInput', {
    name: 'documento.heic',
    mimeType: 'image/heic',
    buffer: Buffer.from('heic-placeholder'),
  });

  await expect.poll(async () => page.locator('#nome').inputValue()).toBe('Mario');
  await expect.poll(async () => page.locator('#luogoRilascioDocumento').inputValue()).toBe('Riccione');
});

test('ocr lento non lascia il caricamento documenti bloccato', async ({ page }) => {
  await installFetchMock(page, {
    ocrNeverResolve: true,
    ocrTimeoutMs: 50,
  });
  await page.goto('/?apt=test-apt&mode=checkin', { waitUntil: 'domcontentloaded' });

  await page.setInputFiles('#mainFileInput', {
    name: 'documento.jpg',
    mimeType: 'image/jpeg',
    buffer: Buffer.from([0xff, 0xd8, 0xff, 0xd9]),
  });

  await expect(page.locator('#mainUploadList')).toContainText('documento.jpg');
  await expect(page.locator('#documentFlowStatus')).toContainText('Documenti completi');
  await expect(page.locator('#mainOcrStatus')).toHaveClass(/success/);
  await expect(page.locator('#mainOcrMsg')).toContainText('puoi continuare');
  await expect(page.locator('#documentEmergencyBtn')).toBeHidden();
});
