const { createClient } = require('@supabase/supabase-js');
const JSZip = require('jszip');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const FIC_TOKEN = process.env.FATTURE_CLOUD_TOKEN;
const FIC_COMPANY_ID = process.env.FATTURE_CLOUD_COMPANY_ID;
const COMMERCIALISTA_EMAIL = process.env.COMMERCIALISTA_EMAIL;
const EMAIL_FROM = process.env.EMAIL_FROM;
const RESEND_API_KEY = process.env.RESEND_API_KEY;
const FIC_BASE = 'https://api-v2.fattureincloud.it';
const ROME_TZ = 'Europe/Rome';
const SCHEDULE = '0 6 1 * *';
const MAX_PAGES = 24;
const PER_PAGE = 100;

exports.config = {
  schedule: SCHEDULE,
};

exports.handler = async (event) => {
  const isHttp = Boolean(event?.httpMethod);
  const manualMode = isHttp;

  if (manualMode && !['GET', 'POST'].includes(event.httpMethod)) {
    return jsonResponse(405, { error: 'Method not allowed' });
  }

  const baseConfigError = validateBaseConfig();
  if (baseConfigError) {
    return jsonResponse(500, { error: baseConfigError });
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

  let actorEmail = 'system@monthly-export-commercialista';
  if (manualMode) {
    const auth = await requireUser(supabase, event);
    if (auth.error) return auth.error;
    actorEmail = auth.user.email || actorEmail;
  }

  let params;
  try {
    params = readParams(event, manualMode);
  } catch (error) {
    return jsonResponse(400, { error: error.message });
  }

  const runtimeCheck = validateRuntimeConfig({ dryRun: params.dryRun });
  if (runtimeCheck) {
    await safeInsertLog(supabase, {
      mese_riferimento: `${params.month}-01`,
      esito: 'ERRORE',
      numero_documenti: 0,
      tipo_export: 'NO_DOCUMENTS',
      xml_source: 'none',
      email_destinatario: COMMERCIALISTA_EMAIL || null,
      dry_run: params.dryRun,
      nome_file: null,
      errore_dettaglio: runtimeCheck,
    });
    return jsonResponse(500, { error: runtimeCheck });
  }

  const { month, monthStart, monthEnd, previousMonthLabel } = params;

  try {
    const { data: existingSuccess, error: existingError } = await supabase
      .from('export_commercialista_log')
      .select('id, mese_riferimento, eseguito_at, nome_file, tipo_export')
      .eq('mese_riferimento', `${month}-01`)
      .eq('esito', 'SUCCESSO')
      .order('eseguito_at', { ascending: false })
      .limit(1)
      .maybeSingle();

    if (existingError) {
      throw new Error(`Errore controllo log export: ${existingError.message}`);
    }

    if (existingSuccess) {
      await safeInsertLog(supabase, {
        mese_riferimento: `${month}-01`,
        esito: 'SKIPPED_DUPLICATE',
        numero_documenti: 0,
        tipo_export: existingSuccess.tipo_export || 'UNKNOWN',
        xml_source: 'original',
        email_destinatario: COMMERCIALISTA_EMAIL || null,
        dry_run: false,
        nome_file: existingSuccess.nome_file || null,
        errore_dettaglio: `Invio gia registrato con successo il ${existingSuccess.eseguito_at}.`,
      });

      return jsonResponse(200, {
        success: true,
        skipped_duplicate: true,
        month,
        previous_month_label: previousMonthLabel,
        existing_success: existingSuccess,
      });
    }

    const issuedDocuments = await listMonthlyIssuedDocuments({
      monthStart,
      monthEnd,
    });

    const prepared = await buildExportArtifacts(issuedDocuments, month);
    const attachment = await buildAttachment(prepared.files, month);
    const warningLines = [...prepared.warnings];

    if (prepared.usingFallbackXml) {
      warningLines.push('Alcuni documenti non hanno XML elettronico originale disponibile e sono stati esportati in XML custom di fallback.');
    }

    if (prepared.files.length === 0) {
      warningLines.push('Nessun documento trovato nel periodo richiesto.');
    }

    const dryRun = params.dryRun;
    const canSendRealEmail = prepared.files.length > 0 && prepared.fallbacksCount === 0;
    if (!dryRun && canSendRealEmail) {
      await sendEmailWithAttachment({
        month,
        attachment,
        documentsCount: prepared.files.length,
        tipoExport: prepared.exportType,
        dryRun,
        warnings: warningLines,
      });
    }

    const logEsito = dryRun
      ? 'DRY_RUN'
      : prepared.files.length === 0
        ? 'NO_DOCUMENTS'
        : prepared.fallbacksCount > 0
          ? 'PARZIALE'
          : 'SUCCESSO';
    const logErrorDetail = warningLines.length ? warningLines.join(' | ') : null;
    await safeInsertLog(supabase, {
      mese_riferimento: `${month}-01`,
      esito: logEsito,
      numero_documenti: prepared.files.length,
      tipo_export: prepared.exportType,
      xml_source: prepared.xmlSource,
      email_destinatario: COMMERCIALISTA_EMAIL || null,
      dry_run: dryRun,
      nome_file: attachment?.filename || null,
      errore_dettaglio: buildStructuredDetail({
        baseDetail: logErrorDetail,
        originalsCount: prepared.originalsCount,
        fallbacksCount: prepared.fallbacksCount,
        canSendRealEmail,
      }),
    });

    await safeInsertAuditLog(supabase, {
      user_email: actorEmail,
      action: dryRun ? 'EXPORT_COMMERCIALISTA_DRY_RUN' : 'EXPORT_COMMERCIALISTA',
      table_name: 'export_commercialista_log',
      record_id: month,
    });

    return jsonResponse(200, {
      success: true,
      month,
      previous_month_label: previousMonthLabel,
      dry_run: dryRun,
      scheduled: !manualMode,
      numero_documenti: prepared.files.length,
      original_xml_count: prepared.originalsCount,
      fallback_xml_count: prepared.fallbacksCount,
      tipo_export: prepared.exportType,
      attachment: attachment
        ? {
            filename: attachment.filename,
            content_type: attachment.contentType,
          }
        : null,
      auto_email_sent: !dryRun && canSendRealEmail,
      warnings: warningLines,
      xml_source: prepared.xmlSource,
      note: prepared.files.length === 0
        ? 'Nessun documento trovato nel periodo richiesto.'
        : prepared.fallbacksCount > 0
          ? 'Invio automatico bloccato: presenza di XML custom fallback. Compatibilita Zucchetti non garantita senza specifica del tracciato richiesto.'
          : dryRun
            ? 'Dry run completato senza invio email. Tutti i documenti hanno XML originali disponibili.'
            : 'Invio email eseguito con soli XML originali FiC.',
    });
  } catch (error) {
    console.error('[monthly-export-commercialista] error:', error);
    await safeInsertLog(supabase, {
      mese_riferimento: `${month}-01`,
      esito: 'ERRORE',
      numero_documenti: 0,
      tipo_export: 'NO_DOCUMENTS',
      xml_source: 'none',
      email_destinatario: COMMERCIALISTA_EMAIL || null,
      dry_run: params.dryRun,
      nome_file: null,
      errore_dettaglio: error.message,
    });
    return jsonResponse(500, {
      error: 'Errore export commercialista',
      detail: error.message,
      month,
    });
  }
};

function readParams(event, manualMode) {
  if (!manualMode) {
    const previousMonth = getPreviousMonthReference(new Date());
    return {
      month: previousMonth.month,
      monthStart: previousMonth.start,
      monthEnd: previousMonth.end,
      previousMonthLabel: previousMonth.month,
      dryRun: envFlag('DRY_RUN_EXPORT_COMMERCIALISTA', false),
    };
  }

  const query = event.queryStringParameters || {};
  let body = {};
  if (event.httpMethod === 'POST' && event.body) {
    try {
      body = JSON.parse(event.body);
    } catch {
      throw new Error('Invalid JSON');
    }
  }

  const month = String(body.month || query.month || '').trim();
  const dryRun = toBoolean(body.dry_run ?? query.dry_run ?? envFlag('DRY_RUN_EXPORT_COMMERCIALISTA', false));
  const reference = month ? getMonthReference(month) : getPreviousMonthReference(new Date());

  return {
    month: reference.month,
    monthStart: reference.start,
    monthEnd: reference.end,
    previousMonthLabel: reference.month,
    dryRun,
  };
}

function validateRuntimeConfig({ dryRun }) {
  const missing = [];
  if (!FIC_TOKEN) missing.push('FATTURE_CLOUD_TOKEN');
  if (!FIC_COMPANY_ID) missing.push('FATTURE_CLOUD_COMPANY_ID');
  if (!COMMERCIALISTA_EMAIL) missing.push('COMMERCIALISTA_EMAIL');
  if (!EMAIL_FROM) missing.push('EMAIL_FROM');
  if (!dryRun && !RESEND_API_KEY) missing.push('RESEND_API_KEY');
  return missing.length ? `Variabili ambiente mancanti: ${missing.join(', ')}` : null;
}

function validateBaseConfig() {
  const missing = [];
  if (!SUPABASE_URL) missing.push('SUPABASE_URL');
  if (!SUPABASE_SERVICE_ROLE_KEY) missing.push('SUPABASE_SERVICE_ROLE_KEY');
  return missing.length ? `Variabili ambiente mancanti: ${missing.join(', ')}` : null;
}

async function requireUser(supabase, event) {
  const headers = event.headers || {};
  const authHeader = headers.authorization || headers.Authorization;
  if (!authHeader?.startsWith('Bearer ')) {
    return { error: jsonResponse(401, { error: 'Unauthorized' }) };
  }

  const token = authHeader.replace('Bearer ', '').trim();
  const { data: { user }, error } = await supabase.auth.getUser(token);
  if (error || !user) {
    return { error: jsonResponse(401, { error: 'Unauthorized' }) };
  }

  return { user };
}

function getPreviousMonthReference(now) {
  const romeParts = new Intl.DateTimeFormat('en-CA', {
    timeZone: ROME_TZ,
    year: 'numeric',
    month: '2-digit',
  }).formatToParts(now).reduce((acc, part) => {
    if (part.type !== 'literal') acc[part.type] = part.value;
    return acc;
  }, {});

  let year = Number(romeParts.year);
  let month = Number(romeParts.month) - 1;
  if (month === 0) {
    month = 12;
    year -= 1;
  }
  return getMonthReference(`${year}-${String(month).padStart(2, '0')}`);
}

function getMonthReference(monthKey) {
  if (!/^\d{4}-\d{2}$/.test(monthKey)) {
    throw new Error('month deve essere nel formato YYYY-MM');
  }
  const [year, month] = monthKey.split('-').map(Number);
  if (month < 1 || month > 12) {
    throw new Error('month non valido');
  }
  const start = `${monthKey}-01`;
  const endDate = new Date(Date.UTC(year, month, 1));
  const end = endDate.toISOString().slice(0, 10);
  return { month: monthKey, start, end };
}

async function listMonthlyIssuedDocuments({ monthStart, monthEnd }) {
  const monthDocuments = [];

  for (let page = 1; page <= MAX_PAGES; page += 1) {
    const response = await ficFetch(
      `/c/${FIC_COMPANY_ID}/issued_documents?type=invoice&fieldset=detailed&per_page=${PER_PAGE}&page=${page}`
    );

    const data = Array.isArray(response?.data) ? response.data : [];
    if (!data.length) break;

    const filtered = data.filter((item) => {
      const date = normalizeIsoDate(item?.date);
      return Boolean(date && date >= monthStart && date < monthEnd);
    });

    for (const item of filtered) {
      monthDocuments.push(item);
    }

    const pagination = response?.pagination || {};
    const lastPage = Number(pagination?.last_page || 0);
    if ((lastPage && page >= lastPage) || data.length < PER_PAGE) {
      break;
    }
  }

  return monthDocuments;
}

async function buildExportArtifacts(documents, month) {
  const files = [];
  const warnings = [];
  let originals = 0;
  let fallbacks = 0;

  for (const summaryDoc of documents) {
    const detail = await getIssuedDocument(summaryDoc.id);
    const originalXml = await tryGetOriginalEInvoiceXml(summaryDoc.id);
    const fileBase = buildDocumentFileBase(detail, month);

    if (originalXml.xml) {
      originals += 1;
      files.push({
        filename: `${fileBase}.xml`,
        contentType: 'application/xml',
        content: originalXml.xml,
        mode: 'original',
      });
      continue;
    }

    fallbacks += 1;
    warnings.push(
      `Documento ${detail.number || detail.id}: XML originale non disponibile${originalXml.reason ? ` (${originalXml.reason})` : ''}.`
    );
    files.push({
      filename: `${fileBase}.xml`,
      contentType: 'application/xml',
      content: buildFallbackXml(detail),
      mode: 'fallback',
    });
  }

  return {
    files,
    warnings,
    originalsCount: originals,
    fallbacksCount: fallbacks,
    exportType:
      files.length === 0
        ? 'NO_DOCUMENTS'
        : fallbacks > 0
          ? 'MIXED'
          : originals > 0
            ? 'ORIGINAL_XML'
            : 'CUSTOM_FALLBACK_XML',
    usingFallbackXml: fallbacks > 0,
    usingOnlyOriginalXml: originals > 0 && fallbacks === 0,
    xmlSource:
      files.length === 0
        ? 'none'
        : fallbacks > 0 && originals > 0
          ? 'mixed'
          : fallbacks > 0
            ? 'fallback_only'
            : 'original',
  };
}

async function getIssuedDocument(documentId) {
  const response = await ficFetch(`/c/${FIC_COMPANY_ID}/issued_documents/${documentId}?fieldset=detailed`);
  if (!response?.data) {
    throw new Error(`Dettaglio documento FiC non disponibile per ID ${documentId}`);
  }
  return response.data;
}

async function tryGetOriginalEInvoiceXml(documentId) {
  const url = `${FIC_BASE}/c/${FIC_COMPANY_ID}/issued_documents/${documentId}/e_invoice/xml?include_attachment=true`;
  const response = await fetch(url, {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${FIC_TOKEN}`,
      Accept: 'text/xml,application/xml,text/plain;q=0.9,*/*;q=0.8',
    },
  });

  if (!response.ok) {
    if ([404, 422].includes(response.status)) {
      return { xml: null, reason: `HTTP ${response.status}` };
    }
    const body = await response.text();
    throw new Error(`Errore recupero XML originale documento ${documentId}: HTTP ${response.status} ${body.slice(0, 300)}`);
  }

  const xml = await response.text();
  if (!xml || !xml.trim().startsWith('<')) {
    return { xml: null, reason: 'Risposta non XML' };
  }

  return { xml };
}

function buildFallbackXml(document) {
  const entity = document.entity || {};
  const payments = Array.isArray(document.payments_list) ? document.payments_list : [];
  const items = Array.isArray(document.items_list) ? document.items_list : [];

  return [
    '<?xml version="1.0" encoding="UTF-8"?>',
    `<commercialista_export source="custom-fallback" compatible_target="non-garantito" generated_at="${escapeXml(new Date().toISOString())}">`,
    '  <metadata>',
    `    <company_id>${escapeXml(String(FIC_COMPANY_ID))}</company_id>`,
    `    <document_id>${escapeXml(String(document.id || ''))}</document_id>`,
    `    <document_type>${escapeXml(String(document.type || 'invoice'))}</document_type>`,
    `    <document_number>${escapeXml(String(document.number || ''))}</document_number>`,
    `    <numeration>${escapeXml(String(document.numeration || ''))}</numeration>`,
    `    <date>${escapeXml(String(document.date || ''))}</date>`,
    `    <ei_status>${escapeXml(String(document.ei_status || ''))}</ei_status>`,
    '  </metadata>',
    '  <customer>',
    `    <name>${escapeXml(String(entity.name || ''))}</name>`,
    `    <vat_number>${escapeXml(String(entity.vat_number || ''))}</vat_number>`,
    `    <tax_code>${escapeXml(String(entity.tax_code || ''))}</tax_code>`,
    `    <email>${escapeXml(String(entity.email || ''))}</email>`,
    '  </customer>',
    '  <amounts>',
    `    <amount_net>${escapeXml(toDecimalString(document.amount_net))}</amount_net>`,
    `    <amount_vat>${escapeXml(toDecimalString(document.amount_vat))}</amount_vat>`,
    `    <amount_gross>${escapeXml(toDecimalString(document.amount_gross))}</amount_gross>`,
    `    <payments_total>${escapeXml(toDecimalString(document.payments_total))}</payments_total>`,
    '  </amounts>',
    '  <items>',
    items.map((item, index) => buildFallbackItemXml(item, index)).join('\n'),
    '  </items>',
    '  <payments>',
    payments.map((payment, index) => buildFallbackPaymentXml(payment, index)).join('\n'),
    '  </payments>',
    '  <warnings>',
    '    <warning>XML custom fallback tecnico generato per automazione interna. Compatibilita Zucchetti non garantita senza specifica del tracciato richiesto.</warning>',
    '  </warnings>',
    '</commercialista_export>',
  ].join('\n');
}

function buildFallbackItemXml(item, index) {
  return [
    `    <item index="${index + 1}">`,
    `      <name>${escapeXml(String(item.name || ''))}</name>`,
    `      <description>${escapeXml(String(item.description || ''))}</description>`,
    `      <qty>${escapeXml(toDecimalString(item.qty))}</qty>`,
    `      <net_price>${escapeXml(toDecimalString(item.net_price))}</net_price>`,
    `      <gross_price>${escapeXml(toDecimalString(item.gross_price))}</gross_price>`,
    `      <vat_rate>${escapeXml(toDecimalString(item.vat?.value))}</vat_rate>`,
    '    </item>',
  ].join('\n');
}

function buildFallbackPaymentXml(payment, index) {
  return [
    `    <payment index="${index + 1}">`,
    `      <amount>${escapeXml(toDecimalString(payment.amount))}</amount>`,
    `      <due_date>${escapeXml(String(payment.due_date || ''))}</due_date>`,
    `      <status>${escapeXml(String(payment.status || ''))}</status>`,
    `      <payment_account>${escapeXml(String(payment.payment_account?.name || ''))}</payment_account>`,
    '    </payment>',
  ].join('\n');
}

async function buildAttachment(files, month) {
  if (!files.length) {
    return null;
  }

  if (files.length === 1) {
    return {
      filename: files[0].filename,
      contentType: files[0].contentType,
      contentBase64: Buffer.from(files[0].content, 'utf8').toString('base64'),
    };
  }

  const zip = new JSZip();
  files.forEach((file) => {
    zip.file(file.filename, file.content);
  });
  const zipBuffer = await zip.generateAsync({
    type: 'nodebuffer',
    compression: 'DEFLATE',
    compressionOptions: { level: 6 },
  });

  return {
    filename: `fatture-${month}.zip`,
    contentType: 'application/zip',
    contentBase64: zipBuffer.toString('base64'),
  };
}

async function sendEmailWithAttachment({ month, attachment, documentsCount, tipoExport, dryRun, warnings }) {
  const payload = {
    from: EMAIL_FROM,
    to: [COMMERCIALISTA_EMAIL],
    subject: buildEmailSubject({ month, dryRun }),
    text: buildEmailBody({ month, documentsCount, tipoExport, dryRun, warnings }),
    attachments: attachment
      ? [
          {
            filename: attachment.filename,
            content: attachment.contentBase64,
          },
        ]
      : [],
  };

  const response = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${RESEND_API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Invio email commercialista fallito: HTTP ${response.status} ${body.slice(0, 400)}`);
  }
}

function buildEmailSubject({ month, dryRun }) {
  return dryRun ? `[DRY RUN] Export XML fatture ${month}` : `Export XML fatture ${month}`;
}

function buildEmailBody({ month, documentsCount, tipoExport, dryRun, warnings }) {
  const rows = [
    dryRun ? 'Modalita: DRY RUN' : 'Modalita: INVIO REALE',
    `Periodo: ${month}`,
    `Numero documenti: ${documentsCount}`,
    `Tipo export: ${tipoExport}`,
    'Origine XML: solo XML originali Fatture in Cloud',
  ];
  if (warnings.length) {
    rows.push(`Warning: ${warnings.join(' | ')}`);
  }
  return rows.join('\n');
}

function buildStructuredDetail({ baseDetail, originalsCount, fallbacksCount, canSendRealEmail }) {
  const parts = [
    `original_xml_count=${originalsCount}`,
    `fallback_xml_count=${fallbacksCount}`,
    `auto_email_allowed=${canSendRealEmail ? 'true' : 'false'}`,
  ];
  if (baseDetail) {
    parts.push(`detail=${baseDetail}`);
  }
  return parts.join(' | ');
}

async function ficFetch(path) {
  const response = await fetch(`${FIC_BASE}${path}`, {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${FIC_TOKEN}`,
      Accept: 'application/json',
      'Content-Type': 'application/json',
    },
  });

  const rawBody = await response.text();
  let json;
  try {
    json = rawBody ? JSON.parse(rawBody) : {};
  } catch {
    json = {};
  }

  if (!response.ok) {
    throw new Error(`Fatture in Cloud ${path} -> HTTP ${response.status} ${rawBody.slice(0, 400)}`);
  }

  return json;
}

function normalizeIsoDate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || '')) ? value : null;
}

function buildDocumentFileBase(document, month) {
  const documentNumber = sanitizeFilenamePart(document.number || document.numeration || document.id || 'senza-numero');
  return `fattura-${month}-${documentNumber}`;
}

function sanitizeFilenamePart(value) {
  return String(value || '')
    .trim()
    .replace(/[^\w.-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '') || 'documento';
}

function toDecimalString(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number.toFixed(2) : '';
}

function escapeXml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

function envFlag(name, fallback) {
  const raw = process.env[name];
  if (raw == null || raw === '') return fallback;
  return toBoolean(raw);
}

function toBoolean(value) {
  if (typeof value === 'boolean') return value;
  return ['1', 'true', 'yes', 'on'].includes(String(value || '').trim().toLowerCase());
}

async function safeInsertLog(supabase, payload) {
  try {
    const { error } = await supabase.from('export_commercialista_log').insert(payload);
    if (error) {
      console.error('[monthly-export-commercialista] export_commercialista_log error:', error);
    }
  } catch (error) {
    console.error('[monthly-export-commercialista] export_commercialista_log fatal:', error);
  }
}

async function safeInsertAuditLog(supabase, payload) {
  try {
    const { error } = await supabase.from('audit_log').insert({
      ...payload,
      timestamp: new Date().toISOString(),
    });
    if (error) {
      console.error('[monthly-export-commercialista] audit_log error:', error);
    }
  } catch (error) {
    console.error('[monthly-export-commercialista] audit_log fatal:', error);
  }
}

function jsonResponse(statusCode, body) {
  return {
    statusCode,
    headers: { 'Content-Type': 'application/json; charset=utf-8' },
    body: JSON.stringify(body),
  };
}
