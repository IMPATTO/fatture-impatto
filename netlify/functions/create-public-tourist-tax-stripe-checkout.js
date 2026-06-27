const {
  DEFAULT_CURRENCY,
  buildStripeTouristTaxMetadata,
  createStripeCheckoutSession,
  normalizeMoney,
} = require('./_lib/public-tourist-tax-stripe');

const STRIPE_SECRET_KEY = String(process.env.STRIPE_SECRET_KEY || '').trim();
const PUBLIC_PORTAL_BASE_URL = String(
  process.env.PUBLIC_PORTAL_BASE_URL
  || process.env.SITE_URL_PORTALE
  || ''
).trim();
const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Content-Type': 'application/json',
};

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: CORS, body: '' };
  }

  if (event.httpMethod === 'GET') {
    return respond(200, {
      ok: true,
      enabled: !!STRIPE_SECRET_KEY,
    });
  }

  if (event.httpMethod !== 'POST') {
    return respond(405, { ok: false, error: 'Method not allowed' });
  }

  try {
    const body = JSON.parse(event.body || '{}');
    const amount = normalizeMoney(body.amount || body.expected_amount);
    const currency = String(body.currency || DEFAULT_CURRENCY).trim().toUpperCase() || DEFAULT_CURRENCY;
    const apartmentRef = normalizeReference(body.apartment_ref || body.apt);
    const channelHint = normalizeChannelHint(body.source_channel_hint || body.channel || body.source);
    const returnPath = sanitizeReturnPath(body.return_path || body.returnPath || '');
    const baseUrl = resolveBaseUrl(event);

    if (!Number.isFinite(amount) || amount <= 0) {
      return respond(400, {
        ok: false,
        error: 'invalid_amount',
        message: 'Inserisci l importo della tassa di soggiorno prima di pagare con Stripe.',
      });
    }

    if (!apartmentRef) {
      return respond(400, {
        ok: false,
        error: 'invalid_apartment',
        message: 'Link Booking non valido. Riapri il link del check-in e riprova.',
      });
    }

    if (channelHint !== 'booking') {
      return respond(400, {
        ok: false,
        error: 'invalid_channel',
        message: 'Il pagamento Stripe della tassa di soggiorno e disponibile solo per le prenotazioni Booking.',
      });
    }

    if (!STRIPE_SECRET_KEY) {
      return respond(503, buildStripeUnavailableResponse());
    }

    const safeReturnPath = returnPath || buildDefaultReturnPath(apartmentRef, channelHint);
    const successUrl = buildReturnUrl(baseUrl, safeReturnPath, {
      stripe_checkout: 'success',
      stripe_session_id: '{CHECKOUT_SESSION_ID}',
    });
    const cancelUrl = buildReturnUrl(baseUrl, safeReturnPath, {
      stripe_checkout: 'cancelled',
    });

    const session = await createStripeCheckoutSession({
      secretKey: STRIPE_SECRET_KEY,
      amount,
      currency,
      successUrl,
      cancelUrl,
      clientReferenceId: `${apartmentRef}:${amount.toFixed(2)}`.slice(0, 200),
      customerEmail: String(body.email || '').trim().toLowerCase(),
      locale: normalizeLocale(body.lingua || body.lang || 'it'),
      productName: 'Tassa di soggiorno',
      productDescription: apartmentRef ? `Prenotazione Booking ${apartmentRef}` : 'Prenotazione Booking',
      metadata: buildStripeTouristTaxMetadata({
        apartmentRef,
        channelHint,
        amount,
        currency,
        email: String(body.email || '').trim().toLowerCase(),
        surname: String(body.cognome || body.surname || '').trim(),
      }),
    });

    return respond(200, {
      ok: true,
      session_id: String(session.id || '').trim(),
      url: String(session.url || '').trim(),
      amount,
      currency,
      expires_at: session.expires_at || null,
      livemode: session.livemode === true,
    });
  } catch (error) {
    console.error('create-public-tourist-tax-stripe-checkout error:', error);
    const statusCode = Number(error.statusCode || 500);
    return respond(
      statusCode,
      statusCode === 503
        ? buildStripeUnavailableResponse()
        : {
            ok: false,
            error: 'stripe_checkout_error',
            message: String(error.message || '').trim() || 'Non riesco ad aprire Stripe in questo momento. Usa il bonifico o contatta Serena su WhatsApp.',
          }
    );
  }
};

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}

function buildStripeUnavailableResponse() {
  return {
    ok: false,
    error: 'stripe_unavailable',
    message: 'Pagamento Stripe non disponibile al momento. Usa il bonifico o contatta Serena su WhatsApp.',
  };
}

function resolveBaseUrl(event) {
  if (PUBLIC_PORTAL_BASE_URL) return PUBLIC_PORTAL_BASE_URL.replace(/\/+$/, '');
  const origin = String(event.headers?.origin || '').trim();
  if (origin) return origin.replace(/\/+$/, '');
  const host = String(event.headers?.host || '').trim();
  if (host) return `https://${host}`.replace(/\/+$/, '');
  return 'https://checkin.illupoaffitta.com';
}

function sanitizeReturnPath(value) {
  const candidate = String(value || '').trim();
  if (!candidate.startsWith('/')) return '';
  if (candidate.startsWith('//')) return '';
  return candidate;
}

function buildDefaultReturnPath(apartmentRef, channelHint) {
  const params = new URLSearchParams();
  params.set('apt', apartmentRef);
  params.set('mode', 'checkin');
  if (channelHint) params.set('src', channelHint);
  return `/?${params.toString()}`;
}

function buildReturnUrl(baseUrl, returnPath, extraParams) {
  const url = new URL(returnPath || '/', `${String(baseUrl || '').replace(/\/+$/, '')}/`);
  url.searchParams.delete('stripe_checkout');
  url.searchParams.delete('stripe_session_id');
  Object.keys(extraParams || {}).forEach((key) => {
    const value = String(extraParams[key] || '').trim();
    if (value) url.searchParams.set(key, value);
  });
  return url.toString();
}

function normalizeReference(value) {
  return String(value || '').trim().toLowerCase();
}

function normalizeChannelHint(value) {
  return String(value || '').trim().toLowerCase() === 'booking' ? 'booking' : '';
}

function normalizeLocale(value) {
  const normalized = String(value || '').trim().toLowerCase();
  return ['it', 'en', 'de', 'fr', 'es'].includes(normalized) ? normalized : 'it';
}
