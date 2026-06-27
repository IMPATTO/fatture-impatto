const SUPPORTED_MIME_TYPES = new Set([
  'application/pdf',
  'image/gif',
  'image/jpeg',
  'image/png',
  'image/webp',
]);

const DEFAULT_TOLERANCE_EUR = 2.5;
const MAX_FILES = 4;
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
    return respond(405, { ok: false, matched: false, error: 'Method not allowed' });
  }

  try {
    const body = JSON.parse(event.body || '{}');
    const expectedAmount = normalizeAmount(body.expected_amount || body.amount);
    const tolerance = normalizeTolerance(body.tolerance_eur);
    const files = normalizeFiles(body.files);
    const beneficiaryHint = stringVal(body.beneficiary || '');
    const ibanHint = normalizeIban(body.iban || '');

    if (!Number.isFinite(expectedAmount) || expectedAmount <= 0) {
      return respond(400, {
        ok: false,
        matched: false,
        error: 'expected_amount_required',
        message: 'Importo bonifico non valido.',
      });
    }

    if (!files.length) {
      return respond(400, {
        ok: false,
        matched: false,
        error: 'proof_files_required',
        message: 'Carica la ricevuta o lo screenshot del bonifico.',
      });
    }

    if (!process.env.ANTHROPIC_API_KEY) {
      return respond(503, buildUnavailableResponse());
    }

    const extraction = await extractPaymentProofData({
      files,
      beneficiaryHint,
      ibanHint,
      anthropicApiKey: process.env.ANTHROPIC_API_KEY,
    });

    const verification = evaluateAmountMatch({
      expectedAmount,
      tolerance,
      extraction,
    });

    return respond(200, {
      ok: true,
      matched: verification.matched,
      status: verification.status,
      message: verification.message,
      expected_amount: expectedAmount,
      extracted_amount: verification.extractedAmount,
      difference: verification.difference,
      tolerance_eur: tolerance,
      candidate_amounts: verification.candidateAmounts,
      extraction,
      checked_at: new Date().toISOString(),
      provider: 'anthropic',
    });
  } catch (error) {
    console.error('verify-public-tourist-tax-payment error:', error);
    return respond(503, buildUnavailableResponse());
  }
};

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}

function buildUnavailableResponse() {
  return {
    ok: false,
    matched: false,
    error: 'verification_unavailable',
    message: 'Caricamento fallito. Verifica automatica non disponibile, contatta Serena su WhatsApp.',
  };
}

function normalizeFiles(value) {
  const raw = Array.isArray(value) ? value.slice(0, MAX_FILES) : [];
  return raw
    .map((entry, index) => ({
      file_name: stringVal(entry?.file_name || `prova-pagamento-${index + 1}`),
      mime_type: String(entry?.mime_type || '').trim().toLowerCase(),
      data: String(entry?.data || '').trim(),
    }))
    .filter((entry) => entry.data && SUPPORTED_MIME_TYPES.has(entry.mime_type));
}

async function extractPaymentProofData({ files, beneficiaryHint, ibanHint, anthropicApiKey }) {
  const prompt = [
    'Analizza queste prove di pagamento di un bonifico e restituisci SOLO JSON valido.',
    'Formato richiesto:',
    '{"amount_paid":null,"currency":"EUR","amounts_found":[],"beneficiary":"","iban":"","confidence":"low","notes":""}',
    'Regole:',
    '- amount_paid deve essere l importo totale effettivamente bonificato o pagato, non la commissione, non il saldo residuo, non il totale prenotazione se diverso.',
    '- amounts_found deve contenere tutti gli importi in euro chiaramente leggibili nel documento.',
    '- beneficiary e iban solo se chiaramente visibili.',
    '- se non riesci a capire l importo pagato usa amount_paid = null.',
    '- nessun markdown, nessun testo extra.',
    beneficiaryHint ? `Beneficiario atteso: ${beneficiaryHint}` : '',
    ibanHint ? `IBAN atteso: ${ibanHint}` : '',
  ].filter(Boolean).join('\n');

  const content = files.map((file) => {
    if (file.mime_type === 'application/pdf') {
      return {
        type: 'document',
        source: {
          type: 'base64',
          media_type: 'application/pdf',
          data: file.data,
        },
      };
    }
    return {
      type: 'image',
      source: {
        type: 'base64',
        media_type: file.mime_type,
        data: file.data,
      },
    };
  });
  content.push({ type: 'text', text: prompt });

  const response = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': anthropicApiKey,
      'anthropic-version': '2023-06-01',
    },
    body: JSON.stringify({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 900,
      messages: [{
        role: 'user',
        content,
      }],
    }),
  });

  const responseText = await response.text();
  if (!response.ok) {
    throw new Error(`Anthropic payment verification failed: ${response.status} ${responseText.slice(0, 200)}`);
  }

  const data = safeParseJsonObject(responseText);
  const text = data.content?.map((item) => item.text || '').join('\n') || '';
  const parsed = safeParseJson(text) || {};
  const candidateAmounts = collectCandidateAmounts(parsed);

  return {
    amount_paid: normalizeAmount(parsed.amount_paid),
    currency: stringVal(parsed.currency || 'EUR').toUpperCase() || 'EUR',
    amounts_found: candidateAmounts,
    beneficiary: stringVal(parsed.beneficiary || ''),
    iban: normalizeIban(parsed.iban || ''),
    confidence: normalizeConfidence(parsed.confidence),
    notes: stringVal(parsed.notes || ''),
  };
}

function evaluateAmountMatch({ expectedAmount, tolerance, extraction }) {
  const candidates = collectCandidateAmounts(extraction);
  if (!candidates.length) {
    return {
      matched: false,
      status: 'amount_not_found',
      message: 'Caricamento fallito: non riesco a leggere l importo nella ricevuta. Contatta Serena su WhatsApp.',
      extractedAmount: null,
      difference: null,
      candidateAmounts: [],
    };
  }

  const closestAmount = candidates.reduce((best, current) => {
    if (best === null) return current;
    return Math.abs(current - expectedAmount) < Math.abs(best - expectedAmount) ? current : best;
  }, null);
  const difference = roundMoney(Math.abs(Number(closestAmount) - Number(expectedAmount)));

  if (difference <= tolerance) {
    return {
      matched: true,
      status: 'matched',
      message: 'Ricevuta verificata: puoi continuare.',
      extractedAmount: closestAmount,
      difference,
      candidateAmounts: candidates,
    };
  }

  return {
    matched: false,
    status: 'amount_mismatch',
    message: `Caricamento fallito: l importo letto dalla ricevuta (${formatMoney(closestAmount)}) non coincide con il totale inserito (${formatMoney(expectedAmount)}). Contatta Serena su WhatsApp.`,
    extractedAmount: closestAmount,
    difference,
    candidateAmounts: candidates,
  };
}

function collectCandidateAmounts(source) {
  const values = [];
  const direct = normalizeAmount(source?.amount_paid);
  if (direct !== null) values.push(direct);
  const list = Array.isArray(source?.amounts_found) ? source.amounts_found : [];
  list.forEach((value) => {
    const normalized = normalizeAmount(value);
    if (normalized !== null) values.push(normalized);
  });
  return Array.from(new Set(values)).sort((left, right) => left - right);
}

function normalizeAmount(value) {
  if (value == null || value === '') return null;
  const normalized = Number(String(value).replace(/[^0-9,.-]/g, '').replace(',', '.'));
  return Number.isFinite(normalized) ? roundMoney(normalized) : null;
}

function normalizeTolerance(value) {
  const normalized = normalizeAmount(value);
  return normalized !== null && normalized >= 0 ? normalized : DEFAULT_TOLERANCE_EUR;
}

function roundMoney(value) {
  return Math.round(Number(value) * 100) / 100;
}

function formatMoney(value) {
  const normalized = normalizeAmount(value);
  if (normalized === null) return '0,00 EUR';
  return normalized.toLocaleString('it-IT', {
    style: 'currency',
    currency: 'EUR',
  });
}

function stringVal(value) {
  return String(value || '').trim();
}

function normalizeIban(value) {
  return stringVal(value).replace(/\s+/g, '').toUpperCase();
}

function normalizeConfidence(value) {
  const normalized = stringVal(value).toLowerCase();
  if (['high', 'medium', 'low'].includes(normalized)) return normalized;
  return 'low';
}

function safeParseJsonObject(text) {
  try {
    return JSON.parse(String(text || '{}'));
  } catch {
    return {};
  }
}

function safeParseJson(text) {
  const cleaned = String(text || '').replace(/```json|```/g, '').trim();
  const match = cleaned.match(/\{[\s\S]*\}/);
  if (!match) return null;
  try {
    return JSON.parse(match[0]);
  } catch {
    return null;
  }
}

exports.__test__ = {
  normalizeAmount,
  evaluateAmountMatch,
  collectCandidateAmounts,
  normalizeTolerance,
};
