const Busboy = require('busboy');
const { ingestBollette, getSupabaseService } = require('./_bollette-ingest');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };
  if (event.httpMethod !== 'POST') return { statusCode: 405, headers: CORS, body: JSON.stringify({ error: 'Method not allowed' }) };

  try {
    const authHeader = String(event.headers.authorization || event.headers.Authorization || '').trim();
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7).trim() : '';
    if (!token) return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Missing bearer token' }) };

    const supabase = getSupabaseService();
    const { data: authData, error: authError } = await supabase.auth.getUser(token);
    if (authError) throw authError;
    const user = authData?.user;
    const email = String(user?.email || '').trim().toLowerCase();
    if (email !== 'contabilita@illupoaffitta.com') {
      return { statusCode: 403, headers: CORS, body: JSON.stringify({ error: 'Utente non autorizzato' }) };
    }

    const contentType = String(event.headers['content-type'] || event.headers['Content-Type'] || '').toLowerCase();
    const parsed = contentType.includes('multipart/form-data')
      ? await parseMultipart(event)
      : parseJsonPayload(event);

    const files = normalizeFiles(parsed.files, parsed.fields.attachments);
    if (!files.length) {
      return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'Carica almeno un PDF o una foto' }) };
    }

    const subject = String(parsed.fields.subject || '').trim();
    const note = String(parsed.fields.note || '').trim();
    const metadata = {
      from: email,
      sender_name: user?.user_metadata?.full_name || 'Upload portale',
      to: 'portale',
      subject: subject || note || 'Upload manuale bolletta',
      body_text: note || '',
      note: note || '',
      message_id: `portal-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
      received_at: new Date().toISOString(),
    };

    const result = await ingestBollette({
      source: 'portal',
      provider: 'manual-upload',
      metadata,
      files,
    });

    return {
      statusCode: 200,
      headers: CORS,
      body: JSON.stringify({
        ok: true,
        provider: 'manual-upload',
        items: result.items.map((item) => ({
          document_id: item.document.id,
          bolletta_id: item.bill.id,
          file_name: item.document.file_name,
          extraction_status: item.bill.extraction_status,
          duplicate_status: item.document.duplicate_status || item.duplicate?.status || 'normal',
          duplicate_of_document_id: item.document.duplicate_of_document_id || item.duplicate?.document?.id || null,
        })),
      }),
    };
  } catch (error) {
    console.error('[upload-bollette-manual] error:', error);
    return {
      statusCode: 500,
      headers: CORS,
      body: JSON.stringify({ error: 'Manual bollette upload failed', detail: error.message }),
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

function normalizeFiles(multipartFiles, jsonAttachments) {
  const jsonFiles = Array.isArray(jsonAttachments) ? jsonAttachments : [];
  return [
    ...(Array.isArray(multipartFiles) ? multipartFiles : []),
    ...jsonFiles.map((file) => ({
      filename: String(file.filename || file.name || '').trim(),
      mime_type: String(file.mime_type || file.content_type || '').trim(),
      buffer: file.content_base64 ? Buffer.from(String(file.content_base64), 'base64') : null,
      size_bytes: Number(file.size_bytes || 0) || null,
      text_content: String(file.text_content || '').trim(),
    })),
  ].filter((file) => file.filename);
}
