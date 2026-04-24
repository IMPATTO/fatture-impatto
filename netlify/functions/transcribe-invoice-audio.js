const {
  authenticateRequest,
  createSupabaseAdmin,
  jsonResponse,
  normalizeString,
  parseEventJson
} = require('./_lib/fatture-fic');

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_AUDIO_MODEL = process.env.OPENAI_AUDIO_TRANSCRIBE_MODEL || 'gpt-4o-mini-transcribe';
const OPENAI_AUDIO_URL = 'https://api.openai.com/v1/audio/transcriptions';
const MAX_AUDIO_BYTES = 25 * 1024 * 1024;

exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') {
    return jsonResponse(405, { error: 'Method not allowed' });
  }

  const supabase = createSupabaseAdmin();
  const auth = await authenticateRequest(event, supabase);
  if (auth.errorResponse) return auth.errorResponse;

  const parsed = parseEventJson(event);
  if (parsed.errorResponse) return parsed.errorResponse;
  const body = parsed.body || {};

  if (!OPENAI_API_KEY) {
    return jsonResponse(500, {
      error: 'Trascrizione audio backend non configurata',
      detail: 'Variabile OPENAI_API_KEY mancante nel runtime Netlify.'
    });
  }

  const audioPayload = normalizeAudioPayload(body.audio_base64);
  const mimeType = normalizeRequestedMimeType(body.mime_type);
  const fileName = normalizeString(body.file_name || getDefaultAudioFileName(mimeType));

  if (!audioPayload) {
    return jsonResponse(400, { error: 'audio_base64 richiesto' });
  }

  let audioBuffer;
  try {
    audioBuffer = Buffer.from(audioPayload, 'base64');
  } catch (error) {
    return jsonResponse(400, {
      error: 'Audio base64 non valido',
      detail: error.message
    });
  }

  if (!audioBuffer.length) {
    return jsonResponse(400, { error: 'Audio vuoto' });
  }

  if (audioBuffer.length > MAX_AUDIO_BYTES) {
    return jsonResponse(413, {
      error: 'Audio troppo grande',
      detail: `Limite massimo ${MAX_AUDIO_BYTES} bytes`
    });
  }

  const formData = new FormData();
  formData.append('model', OPENAI_AUDIO_MODEL);
  formData.append('language', 'it');
  formData.append('prompt', 'Trascrivi richieste vocali di fatturazione in italiano. Mantieni fedelmente nomi clienti, importi, iva, esente iva, rimborso spese, sezionale, bonifico, contanti.');
  formData.append('response_format', 'json');
  formData.append('file', new Blob([audioBuffer], { type: mimeType }), fileName);

  let rawText = '';
  try {
    const response = await fetch(OPENAI_AUDIO_URL, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`
      },
      body: formData
    });

    rawText = await response.text();
    let parsedResponse = null;
    try {
      parsedResponse = rawText ? JSON.parse(rawText) : null;
    } catch (error) {
      return jsonResponse(502, {
        error: 'Risposta OpenAI non JSON',
        detail: error.message,
        raw_response: rawText
      });
    }

    if (!response.ok) {
      const mappedError = mapOpenAiTranscriptionError(response.status, parsedResponse);
      return jsonResponse(response.status, {
        error: mappedError.error,
        detail: mappedError.detail,
        upstream_detail: parsedResponse
      });
    }

    const text = normalizeString(parsedResponse?.text || '');
    if (!text) {
      return jsonResponse(422, {
        error: 'Trascrizione vuota',
        detail: parsedResponse
      });
    }

    return jsonResponse(200, {
      success: true,
      text
    });
  } catch (error) {
    return jsonResponse(500, {
      error: 'Errore chiamata OpenAI audio',
      detail: error.message,
      raw_response: rawText || null
    });
  }
};

function normalizeAudioPayload(value) {
  const raw = normalizeString(value);
  if (!raw) return '';
  const dataUrlMatch = raw.match(/^data:[^;]+;base64,(.+)$/);
  return dataUrlMatch?.[1] || raw;
}

function normalizeRequestedMimeType(value) {
  const raw = normalizeString(value).toLowerCase();
  if (!raw) return 'audio/mp4';
  if (raw === 'audio/aac' || raw === 'audio/m4a') return 'audio/mp4';
  return raw;
}

function getDefaultAudioFileName(mimeType) {
  return mimeType.includes('mp4') || mimeType.includes('aac') ? 'voice.m4a' : 'voice.webm';
}

function mapOpenAiTranscriptionError(statusCode, parsedResponse) {
  const upstreamMessage = normalizeString(
    parsedResponse?.error?.message ||
    parsedResponse?.message ||
    ''
  );
  const upstreamCode = normalizeString(
    parsedResponse?.error?.code ||
    parsedResponse?.code ||
    ''
  ).toLowerCase();

  if (statusCode === 429 && (upstreamCode === 'insufficient_quota' || /quota|billing|plan/i.test(upstreamMessage))) {
    return {
      error: 'Credito trascrizione esaurito',
      detail: 'Il credito OpenAI per la trascrizione audio e terminato o il piano non copre altre richieste.'
    };
  }

  if (statusCode === 401) {
    return {
      error: 'Configurazione OpenAI non valida',
      detail: 'La chiave API OpenAI configurata per la trascrizione audio non e valida o non e piu attiva.'
    };
  }

  return {
    error: 'Errore trascrizione audio',
    detail: upstreamMessage || 'OpenAI ha rifiutato la richiesta di trascrizione audio.'
  };
}
