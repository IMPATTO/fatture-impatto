const { createClient } = require('@supabase/supabase-js');
const crypto = require('crypto');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const STORAGE_BUCKET = process.env.CONTABILITA_STORAGE_BUCKET || 'contabilita-media';

function getSupabaseService() {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error('Supabase env mancanti');
  }
  return createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
}

async function ingestBollette({ source, provider, metadata, files }) {
  const supabase = getSupabaseService();
  const normalizedFiles = normalizeFiles(files);
  const results = [];

  for (let index = 0; index < normalizedFiles.length; index += 1) {
    const file = normalizedFiles[index];
    const storedFile = await persistFile(supabase, source, metadata, file, index);
    const extraction = await extractBillData({
      file: storedFile,
      metadata,
      source,
      provider,
    });
    const sourceRef = buildSourceRef(source, metadata, storedFile, index);
    const documentPayload = buildDocumentPayload({ source, provider, metadata, storedFile, extraction, sourceRef });
    const billPayload = buildBillPayload({ metadata, storedFile, extraction });
    const duplicateInfo = await classifyDuplicate(supabase, { documentPayload, billPayload });

    if (duplicateInfo.status === 'duplicate_exact' && duplicateInfo.document?.id) {
      const document = await markExactDuplicate(supabase, duplicateInfo.document.id, duplicateInfo);
      const bill = await getBillByDocumentId(supabase, duplicateInfo.document.id);
      results.push({ document, bill, extraction, duplicate: duplicateInfo });
      continue;
    }

    const enrichedDocumentPayload = {
      ...documentPayload,
      duplicate_status: duplicateInfo.status,
      duplicate_of_document_id: duplicateInfo.document?.id || null,
      duplicate_reason: duplicateInfo.reason || null,
    };

    const document = await upsertDocument(supabase, enrichedDocumentPayload);
    const bill = await upsertBill(supabase, document.id, billPayload);
    results.push({ document, bill, extraction, duplicate: duplicateInfo });
  }

  return { ok: true, items: results };
}

function normalizeFiles(files) {
  return (Array.isArray(files) ? files : [])
    .map((file) => {
      if (!file || typeof file !== 'object') return null;
      const buffer = file.buffer
        ? Buffer.isBuffer(file.buffer) ? file.buffer : Buffer.from(file.buffer)
        : file.content_base64
          ? Buffer.from(String(file.content_base64), 'base64')
          : null;
      return {
        filename: stringVal(file.filename || file.name) || 'allegato',
        mime_type: stringVal(file.mime_type || file.content_type),
        size_bytes: numberVal(file.size_bytes || (buffer ? buffer.length : 0)),
        buffer,
        attachment_hash: buildAttachmentHash({ buffer, file }),
        url: stringVal(file.url),
        storage_path: stringVal(file.storage_path),
        preview_url: stringVal(file.preview_url),
        text_content: stringVal(file.text_content || file.ocr_text),
      };
    })
    .filter(Boolean);
}

async function persistFile(supabase, source, metadata, file, index) {
  if (!file.buffer) {
    return file;
  }
  await ensureStorageBucket(supabase);
  const safeName = file.filename.replace(/[^a-zA-Z0-9._-]/g, '_');
  const sourceRef = sanitizeStorageKeyPart(
    stringVal(metadata.message_id || metadata.source_ref || metadata.subject || `${source}-${Date.now()}`)
  );
  const path = `contabilita-bollette/${sanitizeStorageKeyPart(source)}/${new Date().toISOString().slice(0, 10)}/${sourceRef}_${index}_${safeName}`;
  const { error } = await supabase.storage.from(STORAGE_BUCKET).upload(path, file.buffer, {
    contentType: file.mime_type || 'application/octet-stream',
    upsert: true,
  });
  if (error) throw error;
  const { data } = supabase.storage.from(STORAGE_BUCKET).getPublicUrl(path);
  return {
    ...file,
    storage_path: path,
    url: file.url || data?.publicUrl || '',
    preview_url: file.preview_url || data?.publicUrl || '',
  };
}

async function ensureStorageBucket(supabase) {
  const { data: buckets, error: listError } = await supabase.storage.listBuckets();
  if (listError) throw listError;
  if (Array.isArray(buckets) && buckets.some((bucket) => bucket.name === STORAGE_BUCKET)) return;

  const { error: createError } = await supabase.storage.createBucket(STORAGE_BUCKET, {
    public: true,
    fileSizeLimit: '20MB',
    allowedMimeTypes: [
      'application/pdf',
      'image/jpeg',
      'image/png',
      'image/webp',
      'image/gif',
    ],
  });
  if (createError && !String(createError.message || '').toLowerCase().includes('already exists')) {
    throw createError;
  }
}

async function extractBillData({ file, metadata, source, provider }) {
  const fallbackText = [file.text_content, metadata.body_text, metadata.subject, metadata.caption].filter(Boolean).join('\n');
  let parsed = null;
  let rawText = fallbackText;

  if (ANTHROPIC_API_KEY && file.buffer && isSupportedForAnthropic(file.mime_type)) {
    try {
      const response = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': ANTHROPIC_API_KEY,
          'anthropic-version': '2023-06-01',
        },
        body: JSON.stringify({
          model: 'claude-sonnet-4-20250514',
          max_tokens: 1200,
          messages: [{
            role: 'user',
            content: buildAnthropicContent(file, metadata),
          }],
        }),
      });
      const data = await response.json();
      const text = data.content?.map((item) => item.text || '').join('\n') || '';
      parsed = safeParseJson(text);
      rawText = rawText || text;
    } catch (error) {
      console.error('[bollette-ingest] anthropic extract error:', error);
    }
  }

  if (!parsed && fallbackText) {
    parsed = extractFromPlainText(fallbackText);
  }

  const normalized = normalizeExtraction(parsed || {});
  return {
    ...normalized,
    raw_text: rawText || '',
    extraction_status: computeExtractionStatus(normalized),
    provider: provider || null,
    source,
  };
}

function buildAnthropicContent(file, metadata) {
  const prompt = [
    'Analizza questo documento bolletta e restituisci SOLO JSON valido.',
    'Formato richiesto:',
    '{"utility_type":"","supply_address":"","account_holder":"","amount_total":null,"period_start":"","period_end":"","supplier_name":"","bill_number":"","issue_date":"","due_date":"","raw_text_summary":""}',
    'Regole:',
    '- utility_type: luce, gas, acqua, altro',
    '- date in formato YYYY-MM-DD se affidabili',
    '- amount_total numero con punto decimale',
    '- se un campo non e affidabile usa stringa vuota o null',
    '- nessun testo extra, nessun markdown',
    metadata.subject ? `Oggetto email: ${metadata.subject}` : '',
    metadata.body_text ? `Testo email: ${metadata.body_text.slice(0, 3000)}` : '',
  ].filter(Boolean).join('\n');

  if (file.mime_type === 'application/pdf') {
    return [
      {
        type: 'document',
        source: {
          type: 'base64',
          media_type: 'application/pdf',
          data: file.buffer.toString('base64'),
        },
      },
      { type: 'text', text: prompt },
    ];
  }

  return [
    {
      type: 'image',
      source: {
        type: 'base64',
        media_type: file.mime_type || 'image/png',
        data: file.buffer.toString('base64'),
      },
    },
    { type: 'text', text: prompt },
  ];
}

function normalizeExtraction(parsed) {
  const source = parsed && typeof parsed === 'object' ? parsed : {};
  return {
    utility_type: normalizeUtilityType(source.utility_type),
    supply_address: stringVal(source.supply_address || source.indirizzo_fornitura || source.supply_point_address),
    account_holder: stringVal(source.account_holder || source.intestatario || source.holder_name),
    amount_total: normalizeAmount(source.amount_total),
    period_start: normalizeDate(source.period_start),
    period_end: normalizeDate(source.period_end),
    supplier_name: stringVal(source.supplier_name || source.counterparty || source.fornitore),
    bill_number: stringVal(source.bill_number || source.numero_bolletta || source.document_number),
    issue_date: normalizeDate(source.issue_date),
    due_date: normalizeDate(source.due_date),
    raw_text_summary: stringVal(source.raw_text_summary),
  };
}

function computeExtractionStatus(data) {
  const required = [
    data.utility_type,
    data.supply_address,
    data.account_holder,
    data.amount_total,
    data.period_start,
    data.period_end,
  ].filter((value) => value !== null && value !== '');
  if (required.length >= 5) return 'extracted';
  if (required.length >= 2 || data.supplier_name || data.bill_number || data.issue_date || data.due_date) return 'partial';
  return 'failed';
}

function extractFromPlainText(text) {
  const raw = String(text || '');
  const lower = raw.toLowerCase();
  const amountMatch = raw.match(/(?:totale|importo(?: totale)?)\s*[:€]?\s*([0-9]+[.,][0-9]{2})/i);
  const issueMatch = raw.match(/(?:emissione|data emissione|issue date)\s*[:\-]?\s*(\d{4}-\d{2}-\d{2})/i);
  const dueMatch = raw.match(/(?:scadenza|due date)\s*[:\-]?\s*(\d{4}-\d{2}-\d{2})/i);
  const periodMatch = raw.match(/(?:periodo|competenza)\s*[:\-]?\s*(\d{4}-\d{2}-\d{2})\s*(?:al|a|-)\s*(\d{4}-\d{2}-\d{2})/i);
  const holderMatch = raw.match(/(?:intestatario|account holder)\s*[:\-]?\s*(.+)/i);
  const addressMatch = raw.match(/(?:indirizzo fornitura|supply address)\s*[:\-]?\s*(.+)/i);
  const billNumberMatch = raw.match(/(?:numero bolletta|bill number)\s*[:\-]?\s*([A-Z0-9._/-]+)/i);
  const supplierMatch = raw.match(/(?:fornitore|supplier)\s*[:\-]?\s*(.+)/i);
  return {
    utility_type: lower.includes('gas') ? 'gas' : lower.includes('acqua') ? 'acqua' : lower.includes('luce') || lower.includes('energia') ? 'luce' : '',
    supply_address: addressMatch ? addressMatch[1].trim() : '',
    account_holder: holderMatch ? holderMatch[1].trim() : '',
    amount_total: amountMatch ? normalizeAmount(amountMatch[1]) : null,
    period_start: periodMatch ? normalizeDate(periodMatch[1]) : null,
    period_end: periodMatch ? normalizeDate(periodMatch[2]) : null,
    supplier_name: supplierMatch ? supplierMatch[1].trim() : '',
    bill_number: billNumberMatch ? billNumberMatch[1].trim() : '',
    issue_date: issueMatch ? normalizeDate(issueMatch[1]) : null,
    due_date: dueMatch ? normalizeDate(dueMatch[1]) : null,
  };
}

function buildDocumentPayload({ source, provider, metadata, storedFile, extraction, sourceRef }) {
  const extractedJson = {
    source_provider: provider || source,
    sender: metadata.from || '',
    recipient: metadata.to || '',
    subject: metadata.subject || '',
    caption: metadata.caption || '',
    source_message_id_original: metadata.message_id || '',
    extraction,
    file: {
      filename: storedFile.filename,
      url: storedFile.url || '',
      storage_path: storedFile.storage_path || '',
      mime_type: storedFile.mime_type || '',
      size_bytes: storedFile.size_bytes || null,
    },
  };

  return {
    source_channel: source,
    source,
    source_email: metadata.from || null,
    source_subject: metadata.subject || metadata.caption || null,
    source_message_id: sourceRef,
    original_source_message_id: metadata.message_id || null,
    source_ref: sourceRef,
    attachment_hash: storedFile.attachment_hash || null,
    counterparty: extraction.supplier_name || metadata.sender_name || metadata.from || null,
    approval_status: 'da_revisionare',
    stato: 'DA_APPROVARE',
    attachment_count: 1,
    attachments: [{
      filename: storedFile.filename,
      url: storedFile.url || null,
      storage_path: storedFile.storage_path || null,
      preview_url: storedFile.preview_url || storedFile.url || null,
      content_type: storedFile.mime_type || null,
      size_bytes: storedFile.size_bytes || null,
    }],
    file_name: storedFile.filename,
    file_url: storedFile.url || null,
    mime_type: storedFile.mime_type || null,
    extracted_text: extraction.raw_text || null,
    ocr_payload: { provider: extraction.provider || null, source },
    extracted_json: extractedJson,
    reviewed_json: {},
    total_amount: extraction.amount_total,
    currency: 'EUR',
    document_kind: 'bolletta',
    document_date: extraction.issue_date,
    due_date: extraction.due_date,
    description: metadata.subject || `Bolletta da ${source}`,
    notes: metadata.note || null,
    processed_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
    received_at: metadata.received_at || new Date().toISOString(),
  };
}

function buildBillPayload({ metadata, storedFile, extraction }) {
  return {
    apartment_id: null,
    is_general: false,
    utility_type: extraction.utility_type || 'altro',
    bill_number: extraction.bill_number || null,
    issue_date: extraction.issue_date || null,
    due_date: extraction.due_date || null,
    period_start: extraction.period_start || null,
    period_end: extraction.period_end || null,
    amount_total: extraction.amount_total,
    payment_status: 'non_pagata',
    accounting_status: 'da_registrare',
    linked_movement_id: null,
    notes: metadata.note || null,
    supply_address: extraction.supply_address || null,
    account_holder: extraction.account_holder || null,
    extraction_status: extraction.extraction_status,
    updated_at: new Date().toISOString(),
  };
}

async function upsertDocument(supabase, payload) {
  const { data: existing, error: selectError } = await supabase
    .from('contabilita_documenti')
    .select('id')
    .eq('source_ref', payload.source_ref)
    .maybeSingle();
  if (selectError) throw selectError;
  if (existing?.id) {
    const { data, error } = await supabase
      .from('contabilita_documenti')
      .update(payload)
      .eq('id', existing.id)
      .select('*')
      .single();
    if (error) throw error;
    return data;
  }
  const { data, error } = await supabase
    .from('contabilita_documenti')
    .insert(payload)
    .select('*')
    .single();
  if (error) throw error;
  return data;
}

async function upsertBill(supabase, documentId, payload) {
  const { data: existing, error: selectError } = await supabase
    .from('contabilita_bollette')
    .select('id')
    .eq('contabilita_documento_id', documentId)
    .maybeSingle();
  if (selectError) throw selectError;
  if (existing?.id) {
    const { data, error } = await supabase
      .from('contabilita_bollette')
      .update(payload)
      .eq('id', existing.id)
      .select('*')
      .single();
    if (error) throw error;
    return data;
  }
  const { data, error } = await supabase
    .from('contabilita_bollette')
    .insert({ ...payload, contabilita_documento_id: documentId })
    .select('*')
    .single();
  if (error) throw error;
  return data;
}

async function classifyDuplicate(supabase, { documentPayload, billPayload }) {
  const exactBySourceRef = await findDocumentByField(supabase, 'source_ref', documentPayload.source_ref);
  if (exactBySourceRef) {
    return {
      status: 'duplicate_exact',
      reason: 'Stesso source_ref gia acquisito',
      document: exactBySourceRef,
    };
  }

  const exactByHash = await findDocumentByField(supabase, 'attachment_hash', documentPayload.attachment_hash);
  if (exactByHash) {
    return {
      status: 'duplicate_exact',
      reason: 'Stesso attachment hash gia acquisito',
      document: exactByHash,
    };
  }

  if (documentPayload.original_source_message_id && documentPayload.file_name && documentPayload.mime_type) {
    const { data, error } = await supabase
      .from('contabilita_documenti')
      .select('id')
      .eq('original_source_message_id', documentPayload.original_source_message_id)
      .eq('file_name', documentPayload.file_name)
      .eq('mime_type', documentPayload.mime_type)
      .limit(1)
      .maybeSingle();
    if (error) throw error;
    if (data?.id) {
      return {
        status: 'duplicate_exact',
        reason: 'Stesso messaggio originale e stesso allegato gia acquisiti',
        document: data,
      };
    }
  }

  const possible = await findPossibleDuplicate(supabase, { documentPayload, billPayload });
  if (possible) {
    return {
      status: 'possible_duplicate',
      reason: possible.reason,
      document: { id: possible.id },
    };
  }

  return { status: 'normal', reason: null, document: null };
}

async function findDocumentByField(supabase, field, value) {
  if (!value) return null;
  const { data, error } = await supabase
    .from('contabilita_documenti')
    .select('id')
    .eq(field, value)
    .limit(1)
    .maybeSingle();
  if (error) throw error;
  return data?.id ? data : null;
}

async function findPossibleDuplicate(supabase, { documentPayload, billPayload }) {
  if (!billPayload.amount_total && !billPayload.bill_number) return null;
  const { data, error } = await supabase
    .from('contabilita_bollette')
    .select('id, utility_type, supply_address, account_holder, amount_total, period_start, period_end, bill_number, contabilita_documento_id, contabilita_documenti(id, counterparty, original_source_message_id)')
    .order('created_at', { ascending: false })
    .limit(200);
  if (error) throw error;

  const targetSupplier = normalizeCompare(documentPayload.counterparty);
  const targetHolder = normalizeCompare(billPayload.account_holder);
  const targetAddress = normalizeCompare(billPayload.supply_address);
  const targetBillNumber = normalizeCompare(billPayload.bill_number);

  for (const row of data || []) {
    if (
      documentPayload.original_source_message_id &&
      row.contabilita_documenti?.original_source_message_id &&
      documentPayload.original_source_message_id === row.contabilita_documenti.original_source_message_id
    ) {
      continue;
    }
    const candidateSupplier = normalizeCompare(row.contabilita_documenti?.counterparty);
    const candidateHolder = normalizeCompare(row.account_holder);
    const candidateAddress = normalizeCompare(row.supply_address);
    const candidateBillNumber = normalizeCompare(row.bill_number);
    const sameAmount = sameAmountValue(row.amount_total, billPayload.amount_total);
    const samePeriod = sameDateValue(row.period_start, billPayload.period_start) && sameDateValue(row.period_end, billPayload.period_end);
    const sameSupplier = targetSupplier && candidateSupplier && targetSupplier === candidateSupplier;
    const sameHolder = targetHolder && candidateHolder && targetHolder === candidateHolder;
    const sameAddress = targetAddress && candidateAddress && targetAddress === candidateAddress;
    const sameBillNumber = targetBillNumber && candidateBillNumber && targetBillNumber === candidateBillNumber;

    if (sameBillNumber && sameAmount && (sameSupplier || sameHolder)) {
      return { id: row.contabilita_documento_id, reason: 'Possibile doppione: stesso numero bolletta e stesso importo' };
    }
    if (sameAmount && samePeriod && sameSupplier && sameHolder && sameAddress) {
      return { id: row.contabilita_documento_id, reason: 'Possibile doppione: stessi dati principali bolletta' };
    }
  }

  return null;
}

async function markExactDuplicate(supabase, documentId, duplicateInfo) {
  const { data, error } = await supabase
    .from('contabilita_documenti')
    .update({
      duplicate_status: 'duplicate_exact',
      duplicate_of_document_id: documentId,
      duplicate_reason: duplicateInfo.reason || 'Doppione tecnico rilevato',
      processed_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    })
    .eq('id', documentId)
    .select('*')
    .single();
  if (error) throw error;
  return data;
}

async function getBillByDocumentId(supabase, documentId) {
  const { data, error } = await supabase
    .from('contabilita_bollette')
    .select('*')
    .eq('contabilita_documento_id', documentId)
    .maybeSingle();
  if (error) throw error;
  return data;
}

function buildSourceRef(source, metadata, file, index) {
  const base = stringVal(metadata.message_id || metadata.subject || metadata.from || `${source}-bollette`);
  const fileKey = [file.filename, file.mime_type, file.size_bytes, file.storage_path || file.url || ''].join('|');
  const digest = crypto.createHash('sha1').update(`${base}|${index}|${fileKey}`).digest('hex').slice(0, 16);
  return `${source}:${base}:${index}:${digest}`;
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

function isSupportedForAnthropic(mimeType) {
  return ['application/pdf', 'image/jpeg', 'image/png', 'image/webp', 'image/gif'].includes(String(mimeType || '').toLowerCase());
}

function normalizeUtilityType(value) {
  const normalized = stringVal(value).toLowerCase();
  if (['luce', 'gas', 'acqua', 'altro'].includes(normalized)) return normalized;
  return '';
}

function normalizeAmount(value) {
  if (value === null || value === undefined || value === '') return null;
  const normalized = Number(String(value).replace(/[^0-9,.-]/g, '').replace(',', '.'));
  return Number.isFinite(normalized) ? Number(normalized.toFixed(2)) : null;
}

function buildAttachmentHash({ buffer, file }) {
  if (buffer) return crypto.createHash('sha256').update(buffer).digest('hex');
  const fallback = [
    stringVal(file.filename || file.name),
    stringVal(file.mime_type || file.content_type),
    numberVal(file.size_bytes || 0) || 0,
    stringVal(file.url),
    stringVal(file.storage_path),
  ].join('|');
  return fallback ? crypto.createHash('sha256').update(fallback).digest('hex') : null;
}

function normalizeDate(value) {
  const raw = stringVal(value);
  return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : null;
}

function stringVal(value) {
  return String(value || '').trim();
}

function numberVal(value) {
  const normalized = Number(value);
  return Number.isFinite(normalized) ? normalized : null;
}

function sanitizeStorageKeyPart(value) {
  return stringVal(value).replace(/[^a-zA-Z0-9._-]/g, '_').replace(/_+/g, '_').slice(0, 120) || 'item';
}

function normalizeCompare(value) {
  return stringVal(value).toLowerCase().replace(/\s+/g, ' ').trim();
}

function sameAmountValue(a, b) {
  const aa = normalizeAmount(a);
  const bb = normalizeAmount(b);
  if (aa === null || bb === null) return false;
  return Math.abs(aa - bb) < 0.01;
}

function sameDateValue(a, b) {
  return String(a || '') === String(b || '');
}

module.exports = {
  ingestBollette,
  getSupabaseService,
};
