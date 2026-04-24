const { ingestBollette } = require('./_bollette-ingest');

const INGEST_KEY = String(process.env.CONTABILITA_INGEST_KEY || '').trim();

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, x-contabilita-ingest-key',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };
  if (event.httpMethod !== 'POST') return { statusCode: 405, headers: CORS, body: JSON.stringify({ error: 'Method not allowed' }) };

  if (!isAuthorized(event.headers || {})) {
    return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Unauthorized' }) };
  }

  try {
    const body = JSON.parse(event.body || '{}');
    const metadata = {
      from: stringVal(body.sender || body.username || body.chat_title || 'telegram'),
      sender_name: stringVal(body.sender_name || body.chat_title),
      to: 'telegram',
      subject: stringVal(body.subject || body.caption || body.text || 'Messaggio Telegram'),
      caption: stringVal(body.caption || body.text),
      message_id: stringVal(body.message_id || body.update_id),
      body_text: stringVal(body.caption || body.text),
      received_at: new Date().toISOString(),
    };
    const files = Array.isArray(body.files) ? body.files : [];
    const result = await ingestBollette({
      source: 'telegram',
      provider: 'telegram',
      metadata,
      files,
    });

    return {
      statusCode: 200,
      headers: CORS,
      body: JSON.stringify({
        ok: true,
        provider: 'telegram',
        items: result.items.map((item) => ({
          document_id: item.document.id,
          bolletta_id: item.bill.id,
          extraction_status: item.bill.extraction_status,
          duplicate_status: item.document.duplicate_status || item.duplicate?.status || 'normal',
          duplicate_of_document_id: item.document.duplicate_of_document_id || item.duplicate?.document?.id || null,
        })),
      }),
    };
  } catch (error) {
    console.error('[inbound-bollette-telegram] error:', error);
    return {
      statusCode: 500,
      headers: CORS,
      body: JSON.stringify({ error: 'Telegram bollette failed', detail: error.message }),
    };
  }
};

function isAuthorized(headers) {
  const headerKey = String(headers['x-contabilita-ingest-key'] || headers['X-Contabilita-Ingest-Key'] || '').trim();
  const auth = String(headers.authorization || headers.Authorization || '').trim();
  if (!INGEST_KEY) return false;
  if (headerKey === INGEST_KEY) return true;
  if (auth.startsWith('Bearer ') && auth.slice(7).trim() === INGEST_KEY) return true;
  return false;
}

function stringVal(value) {
  return String(value || '').trim();
}
