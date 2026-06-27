const STRIPE_API_BASE_URL = 'https://api.stripe.com/v1';
const DEFAULT_CURRENCY = 'EUR';
const STRIPE_CHECKOUT_METHOD = 'stripe_checkout';
const STRIPE_METADATA_KIND = 'booking_public_checkin_tourist_tax';
const ZERO_DECIMAL_CURRENCIES = new Set([
  'BIF', 'CLP', 'DJF', 'GNF', 'JPY', 'KMF', 'KRW', 'MGA',
  'PYG', 'RWF', 'UGX', 'VND', 'VUV', 'XAF', 'XOF', 'XPF',
]);

async function createStripeCheckoutSession(options) {
  const payload = options || {};
  const secretKey = String(payload.secretKey || '').trim();
  if (!secretKey) {
    throw createStripeConfigError('STRIPE_SECRET_KEY mancante');
  }

  const amount = normalizeMoney(payload.amount);
  const currency = normalizeCurrency(payload.currency);
  if (!Number.isFinite(amount) || amount <= 0) {
    throw createStripeRequestError('Importo Stripe non valido.');
  }

  const successUrl = String(payload.successUrl || '').trim();
  const cancelUrl = String(payload.cancelUrl || '').trim();
  if (!successUrl || !cancelUrl) {
    throw createStripeRequestError('URL di ritorno Stripe non validi.');
  }

  const params = new URLSearchParams();
  params.set('mode', 'payment');
  params.set('success_url', successUrl);
  params.set('cancel_url', cancelUrl);
  params.set('payment_method_types[0]', 'card');
  params.set('line_items[0][quantity]', '1');
  params.set('line_items[0][price_data][currency]', currency.toLowerCase());
  params.set('line_items[0][price_data][unit_amount]', String(toMinorAmount(amount, currency)));
  params.set('line_items[0][price_data][product_data][name]', String(payload.productName || 'Tassa di soggiorno').trim() || 'Tassa di soggiorno');

  const productDescription = String(payload.productDescription || '').trim();
  if (productDescription) {
    params.set('line_items[0][price_data][product_data][description]', productDescription);
  }

  const clientReferenceId = String(payload.clientReferenceId || '').trim();
  if (clientReferenceId) {
    params.set('client_reference_id', clientReferenceId.slice(0, 200));
  }

  const customerEmail = String(payload.customerEmail || '').trim().toLowerCase();
  if (customerEmail) {
    params.set('customer_email', customerEmail);
  }

  const locale = normalizeStripeLocale(payload.locale);
  if (locale) {
    params.set('locale', locale);
  }

  const metadata = normalizeMetadata(payload.metadata);
  Object.keys(metadata).forEach((key) => {
    params.set(`metadata[${key}]`, metadata[key]);
  });

  const response = await fetch(`${STRIPE_API_BASE_URL}/checkout/sessions`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${secretKey}`,
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: params.toString(),
  });

  const session = await parseStripeResponse(response);
  if (!session.url || !session.id) {
    throw createStripeRequestError('Stripe non ha restituito una sessione valida.');
  }

  return session;
}

async function retrieveStripeCheckoutSession(options) {
  const payload = options || {};
  const secretKey = String(payload.secretKey || '').trim();
  const sessionId = normalizeStripeSessionId(payload.sessionId);
  if (!secretKey) {
    throw createStripeConfigError('STRIPE_SECRET_KEY mancante');
  }
  if (!sessionId) {
    throw createStripeRequestError('Sessione Stripe non valida.');
  }

  const response = await fetch(`${STRIPE_API_BASE_URL}/checkout/sessions/${encodeURIComponent(sessionId)}`, {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${secretKey}`,
    },
  });

  return parseStripeResponse(response);
}

function evaluateStripeCheckoutSession(options) {
  const payload = options || {};
  const session = payload.session && typeof payload.session === 'object' ? payload.session : {};
  const currency = normalizeCurrency(session.currency || payload.expectedCurrency);
  const amountPaid = amountFromMinor(session.amount_total, currency);
  const metadata = session.metadata && typeof session.metadata === 'object' ? session.metadata : {};
  const expectedAmount = normalizeMoney(payload.expectedAmount);
  const metadataAmount = normalizeMoney(metadata.tourist_tax_amount);
  const effectiveExpectedAmount = Number.isFinite(expectedAmount) && expectedAmount > 0
    ? expectedAmount
    : (Number.isFinite(metadataAmount) && metadataAmount > 0 ? metadataAmount : amountPaid);
  const expectedApartmentRef = normalizeReference(payload.expectedApartmentRef);
  const metadataApartmentRef = normalizeReference(metadata.apartment_ref);
  const expectedChannelHint = normalizeChannelHint(payload.expectedChannelHint);
  const metadataChannelHint = normalizeChannelHint(metadata.channel_hint);
  const sessionStatus = String(session.status || '').trim().toLowerCase();
  const paymentStatus = String(session.payment_status || '').trim().toLowerCase();

  if (!normalizeStripeSessionId(session.id)) {
    return buildStripeVerificationResult({
      matched: false,
      status: 'session_missing',
      message: 'Sessione Stripe non valida.',
      session,
      amountPaid,
      expectedAmount: effectiveExpectedAmount,
      currency,
    });
  }

  if (String(metadata.kind || '').trim() && String(metadata.kind || '').trim() !== STRIPE_METADATA_KIND) {
    return buildStripeVerificationResult({
      matched: false,
      status: 'session_unexpected',
      message: 'Pagamento Stripe non associato alla tassa di soggiorno.',
      session,
      amountPaid,
      expectedAmount: effectiveExpectedAmount,
      currency,
    });
  }

  if (expectedApartmentRef && metadataApartmentRef && expectedApartmentRef !== metadataApartmentRef) {
    return buildStripeVerificationResult({
      matched: false,
      status: 'apartment_mismatch',
      message: 'Pagamento Stripe non associato a questa prenotazione.',
      session,
      amountPaid,
      expectedAmount: effectiveExpectedAmount,
      currency,
    });
  }

  if (expectedChannelHint && metadataChannelHint && expectedChannelHint !== metadataChannelHint) {
    return buildStripeVerificationResult({
      matched: false,
      status: 'channel_mismatch',
      message: 'Pagamento Stripe non associato a una prenotazione Booking.',
      session,
      amountPaid,
      expectedAmount: effectiveExpectedAmount,
      currency,
    });
  }

  if (sessionStatus !== 'complete' || paymentStatus !== 'paid') {
    return buildStripeVerificationResult({
      matched: false,
      status: 'payment_incomplete',
      message: 'Completa il pagamento Stripe prima di continuare.',
      session,
      amountPaid,
      expectedAmount: effectiveExpectedAmount,
      currency,
    });
  }

  if (!Number.isFinite(amountPaid) || amountPaid <= 0) {
    return buildStripeVerificationResult({
      matched: false,
      status: 'amount_missing',
      message: 'Importo Stripe non leggibile. Contatta Serena su WhatsApp.',
      session,
      amountPaid,
      expectedAmount: effectiveExpectedAmount,
      currency,
    });
  }

  if (Number.isFinite(effectiveExpectedAmount) && effectiveExpectedAmount > 0) {
    const difference = roundMoney(Math.abs(amountPaid - effectiveExpectedAmount));
    if (difference > 0) {
      return buildStripeVerificationResult({
        matched: false,
        status: 'amount_mismatch',
        message: `Pagamento Stripe completato, ma l importo pagato (${formatMoney(amountPaid, currency)}) non coincide con il totale inserito (${formatMoney(effectiveExpectedAmount, currency)}). Contatta Serena su WhatsApp.`,
        session,
        amountPaid,
        expectedAmount: effectiveExpectedAmount,
        currency,
      });
    }
  }

  return buildStripeVerificationResult({
    matched: true,
    status: 'paid',
    message: 'Pagamento Stripe verificato: puoi continuare.',
    session,
    amountPaid,
    expectedAmount: effectiveExpectedAmount,
    currency,
  });
}

function buildStripeVerificationResult({ matched, status, message, session, amountPaid, expectedAmount, currency }) {
  const amount = Number.isFinite(amountPaid) ? roundMoney(amountPaid) : null;
  const expected = Number.isFinite(expectedAmount) ? roundMoney(expectedAmount) : null;
  return {
    matched: matched === true,
    status: String(status || '').trim().toLowerCase(),
    message: String(message || '').trim(),
    session_id: normalizeStripeSessionId(session?.id),
    payment_status: String(session?.payment_status || '').trim().toLowerCase(),
    session_status: String(session?.status || '').trim().toLowerCase(),
    amount,
    amount_total: amount,
    expected_amount: expected,
    difference: Number.isFinite(amount) && Number.isFinite(expected)
      ? roundMoney(Math.abs(amount - expected))
      : null,
    currency: normalizeCurrency(currency || session?.currency),
    livemode: session?.livemode === true,
    customer_email: String(session?.customer_details?.email || session?.customer_email || '').trim().toLowerCase(),
    checked_at: new Date().toISOString(),
    provider: 'stripe',
    metadata: session?.metadata && typeof session.metadata === 'object' ? session.metadata : {},
  };
}

function buildStripeTouristTaxMetadata(payload) {
  const source = payload || {};
  const currency = normalizeCurrency(source.currency);
  const amount = normalizeMoney(source.amount);
  return normalizeMetadata({
    kind: STRIPE_METADATA_KIND,
    apartment_ref: normalizeReference(source.apartmentRef),
    channel_hint: normalizeChannelHint(source.channelHint),
    tourist_tax_amount: Number.isFinite(amount) ? String(amount.toFixed(2)) : '',
    tourist_tax_currency: currency,
    guest_email: String(source.email || '').trim().toLowerCase(),
    guest_surname: String(source.surname || '').trim(),
  });
}

function normalizeMetadata(value) {
  const source = value && typeof value === 'object' && !Array.isArray(value) ? value : {};
  const metadata = {};
  Object.keys(source).forEach((key) => {
    const normalizedKey = String(key || '').trim();
    const normalizedValue = String(source[key] || '').trim();
    if (!normalizedKey || !normalizedValue) return;
    metadata[normalizedKey.slice(0, 40)] = normalizedValue.slice(0, 500);
  });
  return metadata;
}

function normalizeStripeLocale(value) {
  const normalized = String(value || '').trim().toLowerCase();
  return ['auto', 'it', 'en', 'de', 'fr', 'es'].includes(normalized) ? normalized : '';
}

function normalizeStripeSessionId(value) {
  const normalized = String(value || '').trim();
  return /^cs_(test|live)_[A-Za-z0-9]+$/i.test(normalized) ? normalized : '';
}

function normalizeChannelHint(value) {
  const normalized = String(value || '').trim().toLowerCase();
  return normalized === 'booking' ? 'booking' : '';
}

function normalizeReference(value) {
  return String(value || '').trim().toLowerCase();
}

function normalizeCurrency(value) {
  const normalized = String(value || DEFAULT_CURRENCY).trim().toUpperCase();
  return normalized || DEFAULT_CURRENCY;
}

function normalizeMoney(value) {
  if (value == null || value === '') return null;
  const normalized = Number(String(value).replace(/[^0-9,.-]/g, '').replace(',', '.'));
  return Number.isFinite(normalized) ? roundMoney(normalized) : null;
}

function toMinorAmount(amount, currency) {
  const normalizedCurrency = normalizeCurrency(currency);
  if (ZERO_DECIMAL_CURRENCIES.has(normalizedCurrency)) {
    return Math.round(Number(amount));
  }
  return Math.round(Number(amount) * 100);
}

function amountFromMinor(value, currency) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return null;
  const normalizedCurrency = normalizeCurrency(currency);
  if (ZERO_DECIMAL_CURRENCIES.has(normalizedCurrency)) {
    return roundMoney(numeric);
  }
  return roundMoney(numeric / 100);
}

function roundMoney(value) {
  return Math.round(Number(value) * 100) / 100;
}

function formatMoney(value, currency) {
  const amount = normalizeMoney(value);
  if (!Number.isFinite(amount)) return '';
  return amount.toLocaleString('it-IT', {
    style: 'currency',
    currency: normalizeCurrency(currency),
  });
}

async function parseStripeResponse(response) {
  const rawText = await response.text();
  const payload = safeParseJson(rawText);
  if (!response.ok) {
    const message = String(
      payload?.error?.message
      || payload?.message
      || `Stripe error ${response.status}`
    ).trim();
    if (response.status === 401 || response.status === 403) {
      throw createStripeConfigError(message);
    }
    throw createStripeRequestError(message);
  }
  if (!payload || typeof payload !== 'object') {
    throw createStripeRequestError('Risposta Stripe non valida.');
  }
  return payload;
}

function safeParseJson(value) {
  try {
    return JSON.parse(String(value || '{}'));
  } catch (_error) {
    return null;
  }
}

function createStripeRequestError(message) {
  const error = new Error(message);
  error.statusCode = 400;
  return error;
}

function createStripeConfigError(message) {
  const error = new Error(message);
  error.statusCode = 503;
  return error;
}

module.exports = {
  DEFAULT_CURRENCY,
  STRIPE_CHECKOUT_METHOD,
  STRIPE_METADATA_KIND,
  buildStripeTouristTaxMetadata,
  createStripeCheckoutSession,
  evaluateStripeCheckoutSession,
  normalizeMoney,
  normalizeStripeSessionId,
  retrieveStripeCheckoutSession,
  toMinorAmount,
  amountFromMinor,
  __test__: {
    amountFromMinor,
    buildStripeTouristTaxMetadata,
    evaluateStripeCheckoutSession,
    normalizeMoney,
    normalizeStripeSessionId,
    toMinorAmount,
  },
};
