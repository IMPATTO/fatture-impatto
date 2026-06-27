const {
  DEFAULT_CURRENCY,
  evaluateStripeCheckoutSession,
  normalizeMoney,
  normalizeStripeSessionId,
  retrieveStripeCheckoutSession,
} = require('./_lib/public-tourist-tax-stripe');

const STRIPE_SECRET_KEY = String(process.env.STRIPE_SECRET_KEY || '').trim();
const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: CORS, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return respond(405, { ok: false, error: 'Method not allowed' });
  }

  try {
    const body = JSON.parse(event.body || '{}');
    const sessionId = normalizeStripeSessionId(body.session_id || body.sessionId);
    const apartmentRef = normalizeReference(body.apartment_ref || body.apt);
    const channelHint = normalizeChannelHint(body.source_channel_hint || body.channel || body.source);
    const expectedAmount = normalizeMoney(body.expected_amount || body.amount);
    const expectedCurrency = String(body.currency || DEFAULT_CURRENCY).trim().toUpperCase() || DEFAULT_CURRENCY;

    if (!sessionId) {
      return respond(400, {
        ok: false,
        matched: false,
        error: 'invalid_session',
        message: 'Sessione Stripe non valida.',
      });
    }

    if (!STRIPE_SECRET_KEY) {
      return respond(503, buildStripeUnavailableResponse());
    }

    const session = await retrieveStripeCheckoutSession({
      secretKey: STRIPE_SECRET_KEY,
      sessionId,
    });
    const verification = evaluateStripeCheckoutSession({
      session,
      expectedAmount,
      expectedCurrency,
      expectedApartmentRef: apartmentRef,
      expectedChannelHint: channelHint,
    });

    return respond(200, {
      ok: true,
      matched: verification.matched,
      status: verification.status,
      message: verification.message,
      session_id: verification.session_id,
      payment_status: verification.payment_status,
      session_status: verification.session_status,
      amount: verification.amount,
      amount_total: verification.amount_total,
      expected_amount: verification.expected_amount,
      difference: verification.difference,
      currency: verification.currency,
      checked_at: verification.checked_at,
      provider: verification.provider,
      livemode: verification.livemode,
      customer_email: verification.customer_email,
    });
  } catch (error) {
    console.error('get-public-tourist-tax-stripe-session error:', error);
    const statusCode = Number(error.statusCode || 500);
    return respond(
      statusCode,
      statusCode === 503
        ? buildStripeUnavailableResponse()
        : {
            ok: false,
            matched: false,
            error: 'stripe_session_error',
            message: String(error.message || '').trim() || 'Non riesco a verificare il pagamento Stripe. Usa il bonifico o contatta Serena su WhatsApp.',
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
    matched: false,
    error: 'stripe_unavailable',
    message: 'Pagamento Stripe non disponibile al momento. Usa il bonifico o contatta Serena su WhatsApp.',
  };
}

function normalizeReference(value) {
  return String(value || '').trim().toLowerCase();
}

function normalizeChannelHint(value) {
  return String(value || '').trim().toLowerCase() === 'booking' ? 'booking' : '';
}
