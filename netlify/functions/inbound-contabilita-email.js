const { createClient } = require('@supabase/supabase-js');
const crypto = require('crypto');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const INGEST_KEY = String(process.env.CONTABILITA_INGEST_KEY || '').trim();

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, x-contabilita-ingest-key',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: CORS, body: '' };
  }
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, headers: CORS, body: JSON.stringify({ error: 'Method not allowed' }) };
  }

  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: 'Supabase env mancanti' }) };
  }

  if (!isAuthorized(event.headers || {})) {
    return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Unauthorized' }) };
  }

  let body;
  try {
    body = JSON.parse(event.body || '{}');
  } catch {
    return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'Invalid JSON' }) };
  }

  const payload = normalizePayload(body);
  if (!payload.source_email && !payload.source_subject && !payload.attachments.length) {
    return {
      statusCode: 400,
      headers: CORS,
      body: JSON.stringify({ error: 'Payload incompleto: source_email/source_subject/allegati mancanti' }),
    };
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
  const entries = buildEntries(payload, body);
  const records = entries.map((entry) => entry.documentRecord);
  const { data, error } = await persistRecords(supabase, records);

  if (error) {
    console.error('[inbound-contabilita-email] persist error:', error);
    return {
      statusCode: 500,
      headers: CORS,
      body: JSON.stringify({ error: 'Insert failed', detail: error.message }),
    };
  }

  const { error: billError } = await persistBillRecords(supabase, entries, data || []);
  if (billError) {
    console.error('[inbound-contabilita-email] bill persist error:', billError);
    return {
      statusCode: 500,
      headers: CORS,
      body: JSON.stringify({ error: 'Bill sync failed', detail: billError.message }),
    };
  }

  return {
    statusCode: 200,
    headers: CORS,
    body: JSON.stringify({
      ok: true,
      documents: data || [],
    }),
  };
};

async function persistBillRecords(supabase, entries, documents) {
  for (let index = 0; index < entries.length; index += 1) {
    const entry = entries[index];
    if (!entry.billRecord) continue;
    const documentId = documents[index]?.id;
    if (!documentId) continue;

    const payload = {
      ...entry.billRecord,
      contabilita_documento_id: documentId,
    };

    const { data: existing, error: selectError } = await supabase
      .from('contabilita_bollette')
      .select('id')
      .eq('contabilita_documento_id', documentId)
      .maybeSingle();

    if (selectError) return { error: selectError };

    if (existing?.id) {
      const { error: updateError } = await supabase
        .from('contabilita_bollette')
        .update(payload)
        .eq('id', existing.id);
      if (updateError) return { error: updateError };
      continue;
    }

    const { error: insertError } = await supabase
      .from('contabilita_bollette')
      .insert(payload);

    if (insertError) return { error: insertError };
  }

  return { error: null };
}

async function persistRecords(supabase, records) {
  const saved = [];

  for (const record of records) {
    const sourceRef = String(record.source_ref || '').trim();
    let existing = null;

    if (sourceRef) {
      const { data, error } = await supabase
        .from('contabilita_documenti')
        .select('id')
        .eq('source_ref', sourceRef)
        .maybeSingle();

      if (error) return { data: null, error };
      existing = data || null;
    }

    if (existing?.id) {
      const { data, error } = await supabase
        .from('contabilita_documenti')
        .update(record)
        .eq('id', existing.id)
        .select('id, approval_status, apartment_id, counterparty, file_name, received_at, source_ref')
        .single();

      if (error) return { data: null, error };
      saved.push(data);
      continue;
    }

    const { data, error } = await supabase
      .from('contabilita_documenti')
      .insert(record)
      .select('id, approval_status, apartment_id, counterparty, file_name, received_at, source_ref')
      .single();

    if (error) return { data: null, error };
    saved.push(data);
  }

  return { data: saved, error: null };
}

function isAuthorized(headers) {
  if (!INGEST_KEY) return false;
  const headerKey = String(headers['x-contabilita-ingest-key'] || headers['X-Contabilita-Ingest-Key'] || '').trim();
  const auth = String(headers.authorization || headers.Authorization || '').trim();
  if (headerKey && headerKey === INGEST_KEY) return true;
  if (auth.startsWith('Bearer ') && auth.slice(7).trim() === INGEST_KEY) return true;
  return false;
}

function normalizePayload(body) {
  const attachments = normalizeAttachments(body.attachments);
  const destinationEmail = lowerVal(body.destination_email || body.to || body.recipient || body.envelope_to);
  return {
    source_channel: stringVal(body.source_channel || 'email') || 'email',
    source: stringVal(body.source || 'email') || 'email',
    source_email: lowerVal(body.source_email || body.from),
    destination_email: destinationEmail,
    source_subject: stringVal(body.source_subject || body.subject),
    source_message_id: stringVal(body.source_message_id || body.message_id),
    counterparty: stringVal(body.counterparty || body.supplier_name || body.provider_name || body.sender_name),
    document_kind: normalizeDocumentKind(body.document_kind || body.document_type),
    stato: normalizeStatus(body.stato),
    apartment_id: stringVal(body.apartment_id),
    property_id: stringVal(body.property_id),
    residence_id: stringVal(body.residence_id),
    attachments,
    extracted_text: stringVal(body.extracted_text || body.ocr_text),
    ocr_payload: plainObject(body.ocr_payload) || null,
    total_amount: normalizeAmount(body.total_amount ?? body.amount_total),
    currency: stringVal(body.currency || 'EUR').toUpperCase() || 'EUR',
    document_date: normalizeDate(body.document_date || body.issue_date),
    due_date: normalizeDate(body.due_date),
    competence_date: normalizeMonth(body.competence_date || body.competence_month),
    notes: stringVal(body.notes),
    tags: normalizeTags(body.tags),
    payment_method: normalizePaymentMethod(body.payment_method),
    fiscal_status: normalizeFiscalStatus(body.fiscal_status),
    category: stringVal(body.category),
    subcategory: stringVal(body.subcategory),
    description: stringVal(body.description || body.source_subject || body.subject),
    received_at: normalizeDateTime(body.received_at) || new Date().toISOString(),
    utility_type: normalizeUtilityType(body.utility_type),
    bill_number: stringVal(body.bill_number || body.invoice_number || body.document_number),
    is_general: normalizeBoolean(body.is_general),
    payment_status: normalizeBillPaymentStatus(body.payment_status),
    accounting_status: normalizeBillAccountingStatus(body.accounting_status),
  };
}

function buildEntries(payload, rawBody) {
  const files = payload.attachments.length ? payload.attachments : [null];
  return files.map((file, index) => {
    const sourceRef = buildSourceRef(payload, file, index);
    const extractedJson = {
      source_email: payload.source_email || null,
      destination_email: payload.destination_email || null,
      source_subject: payload.source_subject || null,
      source_message_id: payload.source_message_id || null,
      counterparty: payload.counterparty || null,
      document_kind: payload.document_kind || null,
      total_amount: payload.total_amount,
      currency: payload.currency || null,
      document_date: payload.document_date || null,
      due_date: payload.due_date || null,
      competence_date: payload.competence_date || null,
      payment_method: payload.payment_method || null,
      fiscal_status: payload.fiscal_status || null,
      category: payload.category || null,
      subcategory: payload.subcategory || null,
      extracted_text: payload.extracted_text || null,
      attachment: file,
      ocr_payload: payload.ocr_payload || null,
      utility_type: payload.utility_type || inferUtilityType(payload, file, rawBody),
      bill_number: payload.bill_number || inferBillNumber(payload, file, rawBody),
    };

    const documentRecord = {
      source_channel: payload.source_channel,
      source: payload.source,
      source_ref: sourceRef,
      source_email: payload.source_email,
      source_subject: payload.source_subject,
      source_message_id: payload.source_message_id,
      counterparty: payload.counterparty,
      stato: 'DA_APPROVARE',
      approval_status: 'da_revisionare',
      apartment_id: payload.apartment_id || null,
      property_id: payload.property_id || null,
      residence_id: payload.residence_id || null,
      attachment_count: payload.attachments.length,
      attachments: file ? [file] : [],
      file_name: file?.filename || null,
      file_url: file?.url || file?.storage_path || null,
      mime_type: file?.content_type || null,
      extracted_text: payload.extracted_text,
      ocr_payload: payload.ocr_payload,
      extracted_json: extractedJson,
      reviewed_json: {},
      raw_payload: rawBody,
      total_amount: payload.total_amount,
      currency: payload.currency,
      document_kind: payload.document_kind,
      document_date: payload.document_date,
      due_date: payload.due_date,
      competence_date: payload.competence_date,
      notes: payload.notes || null,
      description: payload.description || payload.source_subject || null,
      tags: payload.tags,
      payment_method: payload.payment_method || null,
      fiscal_status: payload.fiscal_status || null,
      category: payload.category || null,
      subcategory: payload.subcategory || null,
      received_at: payload.received_at,
      updated_at: new Date().toISOString(),
    };

    return {
      documentRecord,
      billRecord: shouldCreateBillRecord(payload, file, rawBody)
        ? buildBillRecord(payload, file, rawBody)
        : null,
    };
  });
}

function buildBillRecord(payload, file, rawBody) {
  const utilityType = payload.utility_type || inferUtilityType(payload, file, rawBody);
  const billNumber = payload.bill_number || inferBillNumber(payload, file, rawBody);
  const apartmentId = payload.apartment_id || null;
  const amountTotal = payload.total_amount ?? inferAmountFromPayload(rawBody);
  const paymentStatus = payload.payment_status || 'non_pagata';
  const accountingStatus = apartmentId && payload.document_kind === 'bolletta' ? 'registrata' : payload.accounting_status || 'da_registrare';
  const linkedMovementId = apartmentId ? null : null;

  return {
    apartment_id: apartmentId,
    is_general: payload.is_general,
    utility_type: utilityType || null,
    bill_number: billNumber || null,
    issue_date: payload.document_date || null,
    due_date: payload.due_date || null,
    period_start: normalizeDate(rawBody.period_start),
    period_end: normalizeDate(rawBody.period_end),
    amount_total: amountTotal,
    payment_status: paymentStatus,
    accounting_status: accountingStatus,
    reimbursement_method: payload.reimbursement_method || null,
    reimbursed_at: payload.payment_status === 'rimborsata' ? new Date().toISOString() : null,
    linked_movement_id: linkedMovementId,
    notes: payload.notes || null,
    updated_at: new Date().toISOString(),
  };
}

function buildSourceRef(payload, file, index) {
  const base = payload.source_message_id || payload.source_subject || payload.source_email || 'contabilita-email';
  const fileKey = [file?.filename || '', file?.size_bytes || '', file?.content_type || '', file?.url || file?.storage_path || ''].join('|');
  const digest = crypto.createHash('sha1').update(`${base}|${index}|${fileKey}`).digest('hex').slice(0, 16);
  return `${base}::${index}::${digest}`;
}

function normalizeAttachments(input) {
  if (!Array.isArray(input)) return [];
  return input
    .map((item) => {
      if (!item || typeof item !== 'object') return null;
      return {
        filename: stringVal(item.filename || item.name),
        url: stringVal(item.url),
        storage_path: stringVal(item.storage_path),
        preview_url: stringVal(item.preview_url),
        content_type: stringVal(item.content_type || item.mime_type),
        size_bytes: normalizeInteger(item.size_bytes || item.size),
      };
    })
    .filter((item) => item && (item.filename || item.url || item.storage_path));
}

function normalizeDocumentKind(value) {
  const normalized = stringVal(value).toLowerCase();
  if (['bolletta', 'fattura_fornitore', 'ricevuta', 'contratto', 'altro'].includes(normalized)) return normalized;
  return 'bolletta';
}

function normalizeStatus(value) {
  const normalized = stringVal(value).toUpperCase();
  if (['DA_APPROVARE', 'DA_CORREGGERE', 'APPROVATO', 'SCARTATO'].includes(normalized)) return normalized;
  return 'DA_APPROVARE';
}

function normalizePaymentMethod(value) {
  const normalized = stringVal(value).toLowerCase();
  if (['contanti', 'bonifico', 'carta', 'pos', 'addebito', 'altro'].includes(normalized)) return normalized;
  return '';
}

function normalizeFiscalStatus(value) {
  const normalized = stringVal(value).toLowerCase();
  if (['da_fatturare', 'fatturato', 'non_soggetto', 'annullato'].includes(normalized)) return normalized;
  return '';
}

function normalizeUtilityType(value) {
  const normalized = stringVal(value).toLowerCase();
  if (['luce', 'acqua', 'gas', 'tari', 'internet', 'altro'].includes(normalized)) return normalized;
  return '';
}

function normalizeBillPaymentStatus(value) {
  const normalized = stringVal(value).toLowerCase();
  if (['non_pagata', 'pagata', 'pagata_proprietario', 'da_rimborsare', 'rimborsata', 'pagata_direttamente', 'bonifico_effettuato'].includes(normalized)) {
    if (['pagata_proprietario', 'rimborsata', 'pagata_direttamente', 'bonifico_effettuato'].includes(normalized)) return 'pagata';
    if (normalized === 'da_rimborsare') return 'non_pagata';
    return normalized;
  }
  return '';
}

function normalizeBillAccountingStatus(value) {
  const normalized = stringVal(value).toLowerCase();
  if (['da_registrare', 'registrata'].includes(normalized)) return normalized;
  return '';
}

function normalizeAmount(value) {
  if (value === null || value === undefined || value === '') return null;
  const normalized = Number(String(value).replace(',', '.'));
  return Number.isFinite(normalized) ? normalized : null;
}

function normalizeInteger(value) {
  const normalized = Number(value);
  return Number.isInteger(normalized) && normalized >= 0 ? normalized : null;
}

function normalizeDate(value) {
  const raw = stringVal(value);
  return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : null;
}

function normalizeMonth(value) {
  const raw = normalizeDate(value);
  if (!raw) return null;
  return raw.slice(8) === '01' ? raw : `${raw.slice(0, 7)}-01`;
}

function normalizeDateTime(value) {
  const raw = stringVal(value);
  if (!raw) return null;
  const date = new Date(raw);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

function normalizeTags(value) {
  if (!Array.isArray(value)) return [];
  return value.map((item) => stringVal(item)).filter(Boolean).slice(0, 20);
}

function plainObject(value) {
  return value && typeof value === 'object' && !Array.isArray(value) ? value : null;
}

function stringVal(value) {
  return String(value || '').trim();
}

function lowerVal(value) {
  const raw = stringVal(value);
  return raw ? raw.toLowerCase() : '';
}

function normalizeBoolean(value) {
  if (typeof value === 'boolean') return value;
  const normalized = stringVal(value).toLowerCase();
  return ['1', 'true', 'yes', 'si'].includes(normalized);
}

function shouldCreateBillRecord(payload, file, rawBody) {
  if (payload.document_kind === 'bolletta') return true;
  const joined = [
    payload.destination_email,
    payload.source_subject,
    payload.description,
    payload.category,
    file?.filename,
    rawBody?.to,
    rawBody?.recipient,
  ].filter(Boolean).join(' ').toLowerCase();
  return joined.includes('bollette@') || joined.includes('bolletta') || joined.includes('utenza');
}

function inferUtilityType(payload, file, rawBody) {
  const text = [
    payload.source_subject,
    payload.description,
    payload.notes,
    payload.category,
    file?.filename,
    rawBody?.extracted_text,
  ].filter(Boolean).join(' ').toLowerCase();
  if (!text) return '';
  if (text.includes('luce') || text.includes('energia') || text.includes('elettric')) return 'luce';
  if (text.includes('acqua') || text.includes('idrico')) return 'acqua';
  if (text.includes('gas') || text.includes('metano')) return 'gas';
  if (text.includes('tari') || text.includes('rifiuti')) return 'tari';
  if (text.includes('internet') || text.includes('fibra') || text.includes('telefon')) return 'internet';
  return '';
}

function inferBillNumber(payload, file, rawBody) {
  const candidates = [
    rawBody?.bill_number,
    rawBody?.invoice_number,
    rawBody?.document_number,
    payload.bill_number,
    file?.filename,
  ].map(stringVal).filter(Boolean);
  return candidates[0] || '';
}

function inferAmountFromPayload(rawBody) {
  return normalizeAmount(rawBody?.amount_total ?? rawBody?.total_amount);
}
