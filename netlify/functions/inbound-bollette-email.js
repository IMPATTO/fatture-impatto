const Busboy = require('busboy');
const crypto = require('crypto');
const { ingestBollette } = require('./_bollette-ingest');

const INGEST_KEY = String(process.env.CONTABILITA_INGEST_KEY || '').trim();
const MAILGUN_SIGNING_KEY = String(process.env.MAILGUN_SIGNING_KEY || '').trim();

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, x-contabilita-ingest-key',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };
  if (event.httpMethod !== 'POST') return { statusCode: 405, headers: CORS, body: JSON.stringify({ error: 'Method not allowed' }) };

  try {
    const contentType = String(event.headers['content-type'] || event.headers['Content-Type'] || '').toLowerCase();
    const parsed = contentType.includes('multipart/form-data')
      ? await parseMultipart(event)
      : parseJsonPayload(event);

    if (!isAuthorized(event.headers || {}, parsed.fields)) {
      return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Unauthorized' }) };
    }

    const { metadata, files } = normalizeEmailPayload(parsed.fields, parsed.files);
    if (!files.length) {
      return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'Nessun allegato bolletta trovato' }) };
    }

    const result = await ingestBollette({
      source: 'email',
      provider: 'mailgun',
      metadata,
      files,
    });

    return {
      statusCode: 200,
      headers: CORS,
      body: JSON.stringify({
        ok: true,
        provider: 'mailgun',
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
    console.error('[inbound-bollette-email] error:', error);
    return {
      statusCode: 500,
      headers: CORS,
      body: JSON.stringify({ error: 'Inbound bollette failed', detail: error.message }),
    };
  }
};

function parseJsonPayload(event) {
  const body = JSON.parse(event.body || '{}');
  return {
    fields: body,
    files: [],
  };
}

function parseMultipart(event) {
  return new Promise((resolve, reject) => {
    const contentType = event.headers['content-type'] || event.headers['Content-Type'];
    const fields = {};
    const files = [];
    const busboy = Busboy({ headers: { 'content-type': contentType } });
    const raw = event.isBase64Encoded ? Buffer.from(event.body || '', 'base64') : Buffer.from(event.body || '');

    busboy.on('field', (name, value) => {
      if (fields[name] !== undefined) {
        if (!Array.isArray(fields[name])) fields[name] = [fields[name]];
        fields[name].push(value);
      } else {
        fields[name] = value;
      }
    });

    busboy.on('file', (name, stream, info) => {
      const chunks = [];
      stream.on('data', (chunk) => chunks.push(chunk));
      stream.on('end', () => {
        files.push({
          field_name: name,
          filename: info.filename,
          mime_type: info.mimeType,
          buffer: Buffer.concat(chunks),
          size_bytes: chunks.reduce((sum, chunk) => sum + chunk.length, 0),
        });
      });
    });

    busboy.on('finish', () => resolve({ fields, files }));
    busboy.on('error', reject);
    busboy.end(raw);
  });
}

function isAuthorized(headers, fields) {
  const headerKey = String(headers['x-contabilita-ingest-key'] || headers['X-Contabilita-Ingest-Key'] || '').trim();
  const auth = String(headers.authorization || headers.Authorization || '').trim();
  if (INGEST_KEY) {
    if (headerKey === INGEST_KEY) return true;
    if (auth.startsWith('Bearer ') && auth.slice(7).trim() === INGEST_KEY) return true;
  }
  if (MAILGUN_SIGNING_KEY) {
    return verifyMailgunSignature(fields);
  }
  return false;
}

function verifyMailgunSignature(fields) {
  const signature = String(fields.signature || '');
  const timestamp = String(fields.timestamp || '');
  const token = String(fields.token || '');
  if (!signature || !timestamp || !token) return false;
  const digest = crypto.createHmac('sha256', MAILGUN_SIGNING_KEY).update(`${timestamp}${token}`).digest('hex');
  return digest === signature;
}

function normalizeEmailPayload(fields, fileItems) {
  const attachments = [
    ...normalizeJsonAttachments(fields.attachments),
    ...fileItems.map((file) => ({
      filename: file.filename,
      mime_type: file.mime_type,
      size_bytes: file.size_bytes,
      buffer: file.buffer,
      text_content: '',
    })),
  ];

  const metadata = {
    from: lowerVal(fields.sender || fields.from || fields['from']),
    sender_name: stringVal(fields['sender-name'] || fields.sender_name),
    to: lowerVal(fields.recipient || fields.to || fields.envelope_to),
    subject: stringVal(fields.subject),
    message_id: stringVal(fields['Message-Id'] || fields.message_id || fields['message-id']),
    body_text: stringVal(fields['body-plain'] || fields.body_text || fields.text),
    received_at: new Date().toISOString(),
  };

  return { metadata, files: attachments };
}

function normalizeJsonAttachments(input) {
  const items = Array.isArray(input) ? input : [];
  return items.map((file) => ({
    filename: stringVal(file.filename || file.name),
    mime_type: stringVal(file.mime_type || file.content_type),
    size_bytes: Number(file.size_bytes || 0) || null,
    buffer: file.content_base64 ? Buffer.from(String(file.content_base64), 'base64') : null,
    url: stringVal(file.url),
    storage_path: stringVal(file.storage_path),
    text_content: stringVal(file.text_content || file.ocr_text),
  })).filter((file) => file.filename);
}

function stringVal(value) {
  return String(value || '').trim();
}

function lowerVal(value) {
  const raw = stringVal(value);
  return raw ? raw.toLowerCase() : '';
}
