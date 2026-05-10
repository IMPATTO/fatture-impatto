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
const SCHEDULE = '0 6 * * 1';
const MAX_PAGES = 24;
const PER_PAGE = 100;
const DETAIL_CONCURRENCY = 8;
const MAX_FILES_PER_EMAIL = 40;

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

  const { month, rangeStart, rangeEndExclusive, periodLabel, weekStart, periodKey } = params;

  try {
    if (params.verifyDelivery) {
      const verification = await verifyDeliveryAttempts({
        supabase,
        weekStart,
        periodLabel,
      });
      return jsonResponse(200, verification);
    }

    if (weekStart && !params.dryRun && !params.forceSend) {
      const { data: existingSuccess, error: existingError } = await supabase
        .from('export_commercialista_log')
        .select('id, mese_riferimento, settimana_riferimento, eseguito_at, nome_file, tipo_export')
        .eq('settimana_riferimento', weekStart)
        .eq('esito', 'SUCCESSO')
        .eq('dry_run', false)
        .order('eseguito_at', { ascending: false })
        .limit(1)
        .maybeSingle();

      if (existingError) {
        throw new Error(`Errore controllo log export: ${existingError.message}`);
      }

      if (existingSuccess) {
        await safeInsertLog(supabase, {
          mese_riferimento: `${month}-01`,
          settimana_riferimento: weekStart,
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
          period_label: periodLabel,
          week_start: weekStart,
          existing_success: existingSuccess,
        });
      }
    }

    const issuedDocuments = await listIssuedDocumentsInRange({
      rangeStart,
      rangeEndExclusive,
    });

    const prepared = await buildExportArtifacts(issuedDocuments, periodKey);
    const attachments = await buildAttachments(prepared.files, periodKey);
    const warningLines = [...prepared.warnings];

    if (prepared.usingFallbackXml) {
      warningLines.push('Alcuni documenti non hanno XML elettronico originale disponibile e sono stati esportati in XML custom di fallback.');
    }

    if (prepared.files.length === 0) {
      warningLines.push('Nessun documento trovato nel periodo richiesto.');
    }

    const dryRun = params.dryRun;
    const canSendRealEmail = prepared.files.length > 0 && prepared.fallbacksCount === 0;
    let sendResults = [];
    if (!dryRun && canSendRealEmail) {
      sendResults = await sendEmailWithAttachments({
        periodLabel,
        attachments,
        documentsCount: prepared.files.length,
        tipoExport: prepared.exportType,
        dryRun,
        warnings: warningLines,
      });
      await safeInsertDeliveryAttempts(supabase, {
        weekStart,
        periodLabel,
        month,
        sendResults,
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
      settimana_riferimento: weekStart,
      esito: logEsito,
      numero_documenti: prepared.files.length,
      tipo_export: prepared.exportType,
      xml_source: prepared.xmlSource,
      email_destinatario: COMMERCIALISTA_EMAIL || null,
      dry_run: dryRun,
      nome_file: attachments[0]?.filename || null,
      errore_dettaglio: buildStructuredDetail({
        baseDetail: logErrorDetail,
        originalsCount: prepared.originalsCount,
        fallbacksCount: prepared.fallbacksCount,
        canSendRealEmail,
        sendResults,
      }),
    });

    await safeInsertAuditLog(supabase, {
      user_email: actorEmail,
      action: dryRun ? 'EXPORT_COMMERCIALISTA_DRY_RUN' : 'EXPORT_COMMERCIALISTA',
      table_name: 'export_commercialista_log',
      record_id: periodKey,
    });

    return jsonResponse(200, {
      success: true,
      month,
      period_label: periodLabel,
      week_start: weekStart,
      dry_run: dryRun,
      scheduled: !manualMode,
      numero_documenti: prepared.files.length,
      original_xml_count: prepared.originalsCount,
      fallback_xml_count: prepared.fallbacksCount,
      tipo_export: prepared.exportType,
      attachments: attachments.map((attachment) => ({
        filename: attachment.filename,
        content_type: attachment.contentType,
        document_count: attachment.documentCount,
      })),
      email_from: sendResults[0]?.from || EMAIL_FROM,
      resend_email_ids: sendResults.map((result) => result.id).filter(Boolean),
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
      settimana_riferimento: weekStart,
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
    const currentMonth = getCurrentMonthReference(new Date());
    const weekStart = getWeekReference(new Date());
    return {
      month: currentMonth.month,
      rangeStart: currentMonth.start,
      rangeEndExclusive: currentMonth.end,
      periodLabel: currentMonth.month,
      weekStart,
      periodKey: weekStart,
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

  const fromDate = String(body.from_date || query.from_date || '').trim();
  const toDate = String(body.to_date || query.to_date || '').trim();
  const month = String(body.month || query.month || '').trim();
  const customLabel = String(body.label || query.label || '').trim();
  const customWeekStart = String(body.week_start || query.week_start || '').trim();
  const dryRun = toBoolean(body.dry_run ?? query.dry_run ?? envFlag('DRY_RUN_EXPORT_COMMERCIALISTA', false));
  const forceSend = toBoolean(body.force_send ?? query.force_send ?? false);
  const verifyDelivery = toBoolean(body.verify_delivery ?? query.verify_delivery ?? false);
  const reference = fromDate || toDate
    ? getCustomRangeReference({ fromDate, toDate })
    : month
      ? getMonthReference(month)
      : getCurrentMonthReference(new Date());
  const weekStart = customWeekStart || (fromDate || toDate ? reference.start : month ? null : getWeekReference(new Date()));

  return {
    month: reference.month,
    rangeStart: reference.start,
    rangeEndExclusive: reference.end,
    periodLabel: customLabel || reference.label || reference.month,
    weekStart,
    periodKey: weekStart || reference.month,
    dryRun,
    forceSend,
    verifyDelivery,
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

function getCurrentMonthReference(now) {
  const romeParts = new Intl.DateTimeFormat('en-CA', {
    timeZone: ROME_TZ,
    year: 'numeric',
    month: '2-digit',
  }).formatToParts(now).reduce((acc, part) => {
    if (part.type !== 'literal') acc[part.type] = part.value;
    return acc;
  }, {});

  return getMonthReference(`${romeParts.year}-${romeParts.month}`);
}

function getWeekReference(now) {
  const romeDate = new Intl.DateTimeFormat('en-CA', {
    timeZone: ROME_TZ,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(now);
  const [year, month, day] = romeDate.split('-').map(Number);
  const utcDate = new Date(Date.UTC(year, month - 1, day));
  const dayOfWeek = utcDate.getUTCDay() || 7;
  utcDate.setUTCDate(utcDate.getUTCDate() - (dayOfWeek - 1));
  return utcDate.toISOString().slice(0, 10);
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
  return { month: monthKey, start, end, label: monthKey };
}

function getCustomRangeReference({ fromDate, toDate }) {
  if (!normalizeIsoDate(fromDate) || !normalizeIsoDate(toDate)) {
    throw new Error('from_date e to_date devono essere nel formato YYYY-MM-DD');
  }
  if (fromDate > toDate) {
    throw new Error('from_date non puo essere successiva a to_date');
  }
  const endExclusiveDate = new Date(`${toDate}T00:00:00Z`);
  endExclusiveDate.setUTCDate(endExclusiveDate.getUTCDate() + 1);
  return {
    month: fromDate.slice(0, 7),
    start: fromDate,
    end: endExclusiveDate.toISOString().slice(0, 10),
    label: `${fromDate} -> ${toDate}`,
  };
}

async function listIssuedDocumentsInRange({ rangeStart, rangeEndExclusive }) {
  const documents = [];

  for (let page = 1; page <= MAX_PAGES; page += 1) {
    const response = await ficFetch(
      `/c/${FIC_COMPANY_ID}/issued_documents?type=invoice&fieldset=detailed&per_page=${PER_PAGE}&page=${page}`
    );

    const data = Array.isArray(response?.data) ? response.data : [];
    if (!data.length) break;

    const filtered = data.filter((item) => {
      const date = normalizeIsoDate(item?.date);
      return Boolean(date && date >= rangeStart && date < rangeEndExclusive);
    });

    for (const item of filtered) {
      documents.push(item);
    }

    const pagination = response?.pagination || {};
    const lastPage = Number(pagination?.last_page || 0);
    if ((lastPage && page >= lastPage) || data.length < PER_PAGE) {
      break;
    }
  }

  return documents;
}

async function buildExportArtifacts(documents, month) {
  const files = [];
  const warnings = [];
  let originals = 0;
  let fallbacks = 0;
  const settled = await mapWithConcurrency(documents, DETAIL_CONCURRENCY, async (summaryDoc) => {
    const detail = await getIssuedDocument(summaryDoc.id);
    const originalXml = await tryGetOriginalEInvoiceXml(summaryDoc.id);
    const fileBase = buildDocumentFileBase(detail, month);

    if (originalXml.xml) {
      return {
        file: {
          filename: `${fileBase}.xml`,
          contentType: 'application/xml',
          content: originalXml.xml,
          mode: 'original',
        },
        warning: null,
      };
    }

    return {
      file: {
        filename: `${fileBase}.xml`,
        contentType: 'application/xml',
        content: buildFallbackXml(detail),
        mode: 'fallback',
      },
      warning: `Documento ${detail.number || detail.id}: XML originale non disponibile${originalXml.reason ? ` (${originalXml.reason})` : ''}.`,
    };
  });

  for (const item of settled) {
    files.push(item.file);
    if (item.file.mode === 'original') originals += 1;
    if (item.file.mode === 'fallback') {
      fallbacks += 1;
      warnings.push(item.warning);
    }
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

async function buildAttachments(files, periodKey) {
  if (!files.length) return [];

  const totalParts = Math.max(1, Math.ceil(files.length / MAX_FILES_PER_EMAIL));
  const chunks = chunkArray(files, MAX_FILES_PER_EMAIL);
  const attachments = [];

  for (let index = 0; index < chunks.length; index += 1) {
    const chunk = chunks[index];
    const partNumber = index + 1;
    if (chunk.length === 1 && totalParts === 1) {
      attachments.push({
        filename: chunk[0].filename,
        contentType: chunk[0].contentType,
        contentBase64: Buffer.from(chunk[0].content, 'utf8').toString('base64'),
        documentCount: 1,
        partNumber,
        totalParts,
      });
      continue;
    }

    const zip = new JSZip();
    chunk.forEach((file) => {
      zip.file(file.filename, file.content);
    });
    const zipBuffer = await zip.generateAsync({
      type: 'nodebuffer',
      compression: 'DEFLATE',
      compressionOptions: { level: 6 },
    });

    const partSuffix = totalParts > 1 ? `-parte-${partNumber}-di-${totalParts}` : '';
    attachments.push({
      filename: `fatture-${sanitizeFilenamePart(periodKey)}${partSuffix}.zip`,
      contentType: 'application/zip',
      contentBase64: zipBuffer.toString('base64'),
      documentCount: chunk.length,
      partNumber,
      totalParts,
    });
  }

  return attachments;
}

async function sendEmailWithAttachments({ periodLabel, attachments, documentsCount, tipoExport, dryRun, warnings }) {
  const results = [];
  for (const attachment of attachments) {
    const payload = {
      from: EMAIL_FROM,
      to: [COMMERCIALISTA_EMAIL],
      subject: buildEmailSubject({
        periodLabel,
        dryRun,
        partNumber: attachment.partNumber,
        totalParts: attachment.totalParts,
      }),
      text: buildEmailBody({
        periodLabel,
        documentsCount,
        tipoExport,
        dryRun,
        warnings,
        partNumber: attachment.partNumber,
        totalParts: attachment.totalParts,
        partDocumentCount: attachment.documentCount,
      }),
      attachments: [
        {
          filename: attachment.filename,
          content: attachment.contentBase64,
        },
      ],
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

    let body = {};
    try {
      body = await response.json();
    } catch {
      body = {};
    }

    results.push({
      id: body?.id || null,
      from: EMAIL_FROM,
      subject: payload.subject,
      attachmentName: attachment.filename,
      documentCount: attachment.documentCount,
      recipient: COMMERCIALISTA_EMAIL,
      partNumber: attachment.partNumber,
      totalParts: attachment.totalParts,
    });
  }
  return results;
}

function buildEmailSubject({ periodLabel, dryRun, partNumber, totalParts }) {
  const base = dryRun ? `[DRY RUN] Export XML fatture ${periodLabel}` : `Export XML fatture ${periodLabel}`;
  return totalParts > 1 ? `${base} (${partNumber}/${totalParts})` : base;
}

function buildEmailBody({ periodLabel, documentsCount, tipoExport, dryRun, warnings, partNumber, totalParts, partDocumentCount }) {
  const rows = [
    dryRun ? 'Modalita: DRY RUN' : 'Modalita: INVIO REALE',
    `Periodo: ${periodLabel}`,
    `Numero documenti: ${documentsCount}`,
    `Tipo export: ${tipoExport}`,
    'Origine XML: solo XML originali Fatture in Cloud',
  ];
  if (totalParts > 1) {
    rows.push(`Parte invio: ${partNumber}/${totalParts}`);
    rows.push(`Documenti in questa parte: ${partDocumentCount}`);
  }
  if (warnings.length) {
    rows.push(`Warning: ${warnings.join(' | ')}`);
  }
  return rows.join('\n');
}

function buildStructuredDetail({ baseDetail, originalsCount, fallbacksCount, canSendRealEmail, sendResults }) {
  const parts = [
    `original_xml_count=${originalsCount}`,
    `fallback_xml_count=${fallbacksCount}`,
    `auto_email_allowed=${canSendRealEmail ? 'true' : 'false'}`,
    `email_chunks=${Array.isArray(sendResults) ? sendResults.length : 0}`,
  ];
  if (Array.isArray(sendResults) && sendResults.length) {
    parts.push(`email_from=${sendResults[0].from}`);
    parts.push(`resend_email_ids=${sendResults.map((result) => result.id).filter(Boolean).join(',')}`);
  }
  if (baseDetail) {
    parts.push(`detail=${baseDetail}`);
  }
  return parts.join(' | ');
}

async function getResendEmail(emailId) {
  const response = await fetch(`https://api.resend.com/emails/${emailId}`, {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${RESEND_API_KEY}`,
      'Content-Type': 'application/json',
    },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Resend retrieve email fallito: HTTP ${response.status} ${body.slice(0, 400)}`);
  }

  return response.json();
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

function chunkArray(items, size) {
  const chunks = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
}

async function mapWithConcurrency(items, concurrency, worker) {
  const results = new Array(items.length);
  let index = 0;

  async function runWorker() {
    while (index < items.length) {
      const currentIndex = index;
      index += 1;
      results[currentIndex] = await worker(items[currentIndex], currentIndex);
    }
  }

  const runners = Array.from({ length: Math.min(concurrency, items.length) }, () => runWorker());
  await Promise.all(runners);
  return results;
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

async function safeInsertDeliveryAttempts(supabase, { weekStart, periodLabel, month, sendResults }) {
  if (!weekStart || !sendResults.length) return;
  try {
    const rows = sendResults.map((result) => ({
      settimana_riferimento: weekStart,
      mese_riferimento: `${month}-01`,
      periodo_label: periodLabel,
      resend_email_id: result.id,
      email_destinatario: result.recipient,
      email_mittente: result.from,
      email_subject: result.subject,
      attachment_name: result.attachmentName,
      document_count: result.documentCount,
      chunk_index: result.partNumber,
      chunk_total: result.totalParts,
      resend_last_event: 'sent',
    }));
    const { error } = await supabase.from('export_commercialista_delivery_attempts').insert(rows);
    if (error) {
      console.error('[monthly-export-commercialista] export_commercialista_delivery_attempts error:', error);
    }
  } catch (error) {
    console.error('[monthly-export-commercialista] export_commercialista_delivery_attempts fatal:', error);
  }
}

async function verifyDeliveryAttempts({ supabase, weekStart, periodLabel }) {
  if (!weekStart) {
    throw new Error('week_start obbligatorio per verify_delivery');
  }

  const { data: attempts, error } = await supabase
    .from('export_commercialista_delivery_attempts')
    .select('id, resend_email_id, email_destinatario, email_subject, attachment_name, document_count, resend_last_event')
    .eq('settimana_riferimento', weekStart)
    .order('chunk_index', { ascending: true });

  if (error) {
    throw new Error(`Errore lettura delivery attempts: ${error.message}`);
  }

  const verificationRows = await mapWithConcurrency(attempts || [], 4, async (attempt) => {
    const resendEmail = await getResendEmail(attempt.resend_email_id);
    const lastEvent = String(resendEmail?.last_event || '').toLowerCase() || 'unknown';
    await safeUpdateDeliveryAttempt(supabase, attempt.id, lastEvent);
    return {
      resend_email_id: attempt.resend_email_id,
      to: attempt.email_destinatario,
      subject: attempt.email_subject,
      attachment_name: attempt.attachment_name,
      document_count: attempt.document_count,
      last_event: lastEvent,
    };
  });

  const delivered = verificationRows.every((row) => row.last_event === 'delivered');
  return {
    success: delivered,
    week_start: weekStart,
    period_label: periodLabel || weekStart,
    delivery_attempts: verificationRows,
    delivered_count: verificationRows.filter((row) => row.last_event === 'delivered').length,
    total_count: verificationRows.length,
  };
}

async function safeUpdateDeliveryAttempt(supabase, attemptId, lastEvent) {
  try {
    const { error } = await supabase
      .from('export_commercialista_delivery_attempts')
      .update({
        resend_last_event: lastEvent,
        checked_at: new Date().toISOString(),
      })
      .eq('id', attemptId);
    if (error) {
      console.error('[monthly-export-commercialista] update delivery attempt error:', error);
    }
  } catch (error) {
    console.error('[monthly-export-commercialista] update delivery attempt fatal:', error);
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
