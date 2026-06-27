import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { createRequire, Module } from 'node:module';
import { createClient } from '@supabase/supabase-js';
import { readNetlifyEnv } from './lib/netlify-runtime-config.mjs';

const require = createRequire(import.meta.url);
const PROJECT_ROOT = process.cwd();
const FUNCTION_PATH = path.join(PROJECT_ROOT, 'netlify/functions/monthly-export-commercialista.js');
const RECOVERY_ROOT = path.join(PROJECT_ROOT, 'tmp/export-commercialista-recovery');
const MASK_RE = /\*/;

function isUsable(value, { allowMasked = false } = {}) {
  const normalized = String(value || '').trim();
  if (!normalized) return false;
  return allowMasked || !MASK_RE.test(normalized);
}

function pickEnv(name, { allowMasked = false, fallbacks = [] } = {}) {
  const candidates = [
    process.env[name],
    ...fallbacks.map((fallback) => process.env[fallback]),
    readNetlifyEnv(name, { projectRoot: PROJECT_ROOT, context: 'production' }),
    ...fallbacks.map((fallback) => readNetlifyEnv(fallback, { projectRoot: PROJECT_ROOT, context: 'production' })),
  ];
  for (const candidate of candidates) {
    if (isUsable(candidate, { allowMasked })) {
      return String(candidate).trim();
    }
  }
  return '';
}

function ensureEnv() {
  process.env.SUPABASE_URL = pickEnv('SUPABASE_URL', { allowMasked: true }) || process.env.SUPABASE_URL || '';
  process.env.SUPABASE_SERVICE_KEY = pickEnv('SUPABASE_SERVICE_KEY', { fallbacks: ['SUPABASE_SERVICE_ROLE_KEY'] }) || process.env.SUPABASE_SERVICE_KEY || '';
  process.env.SUPABASE_SERVICE_ROLE_KEY = pickEnv('SUPABASE_SERVICE_ROLE_KEY', { fallbacks: ['SUPABASE_SERVICE_KEY'] }) || process.env.SUPABASE_SERVICE_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY || '';
  process.env.FATTURE_CLOUD_TOKEN = pickEnv('FATTURE_CLOUD_TOKEN') || process.env.FATTURE_CLOUD_TOKEN || '';
  process.env.FATTURE_CLOUD_COMPANY_ID = pickEnv('FATTURE_CLOUD_COMPANY_ID') || process.env.FATTURE_CLOUD_COMPANY_ID || '';
  process.env.RESEND_API_KEY = pickEnv('RESEND_API_KEY') || process.env.RESEND_API_KEY || '';
  process.env.COMMERCIALISTA_EMAIL = pickEnv('COMMERCIALISTA_EMAIL') || process.env.COMMERCIALISTA_EMAIL || '';
  process.env.EMAIL_FROM = pickEnv('EMAIL_FROM') || process.env.EMAIL_FROM || '';
}

function getReportingEnv(name, { fallbacks = [] } = {}) {
  return pickEnv(name, { allowMasked: true, fallbacks });
}

function parseWeekStartArg() {
  for (const arg of process.argv.slice(2)) {
    if (/^\d{4}-\d{2}-\d{2}$/.test(arg)) return arg;
    const match = arg.match(/^(?:--)?week_start=(\d{4}-\d{2}-\d{2})$/);
    if (match) return match[1];
  }
  return '';
}

async function loadFunctionInternals() {
  const source = await readFile(FUNCTION_PATH, 'utf8');
  const augmented = `${source}
module.exports.__internals = {
  getPreviousWeekReference,
  getWeekRangeReference,
  listIssuedDocumentsInRange,
  buildExportArtifacts,
  buildAttachments,
  sendEmailWithAttachments,
  safeInsertDeliveryAttempts,
  safeInsertLog,
  verifyDeliveryAttempts,
  getResendEmail
};
`;
  const mod = new Module(FUNCTION_PATH);
  mod.filename = FUNCTION_PATH;
  mod.paths = Module._nodeModulePaths(path.dirname(FUNCTION_PATH));
  mod._compile(augmented, FUNCTION_PATH);
  return mod.exports;
}

function isClosedStatus(esito) {
  return new Set([
    'SUCCESSO',
    'NO_DOCUMENTS',
    'DELIVERED',
    'BACKFILL_SENT_GMAIL',
    'BACKFILL_SENT_MAIL_APP',
    'BACKFILL_SENT_SPARK',
  ]).has(esito);
}

function maskEmail(email) {
  const value = String(email || '').trim();
  const atIndex = value.indexOf('@');
  if (atIndex <= 1) return value;
  return `${value.slice(0, 2)}***${value.slice(atIndex - 1)}`;
}

function buildStructuredDetail({ originalsCount, fallbacksCount, emailChunks, detail }) {
  const parts = [
    `original_xml_count=${originalsCount}`,
    `fallback_xml_count=${fallbacksCount}`,
    `email_chunks=${emailChunks}`,
  ];
  if (detail) parts.push(`detail=${detail}`);
  return parts.join(' | ');
}

async function writeRecoveryBundle({ weekStart, reference, attachments, prepared, reason }) {
  const dir = path.join(RECOVERY_ROOT, weekStart);
  await mkdir(dir, { recursive: true });
  for (const attachment of attachments) {
    const content = Buffer.from(attachment.contentBase64, 'base64');
    await writeFile(path.join(dir, attachment.filename), content);
  }
  const manifest = {
    week_start: weekStart,
    period_label: reference.label,
    recipient: process.env.COMMERCIALISTA_EMAIL,
    sender: process.env.EMAIL_FROM,
    documents_count: prepared.files.length,
    original_xml_count: prepared.originalsCount,
    fallback_xml_count: prepared.fallbacksCount,
    email_chunks: attachments.length,
    reason,
    generated_at: new Date().toISOString(),
    files: attachments.map((attachment) => ({
      filename: attachment.filename,
      content_type: attachment.contentType,
      document_count: attachment.documentCount,
    })),
  };
  await writeFile(path.join(dir, 'manifest.json'), `${JSON.stringify(manifest, null, 2)}\n`, 'utf8');
  return dir;
}

async function fetchWeekState(supabase, weekStart) {
  const [{ data: logs, error: logsError }, { data: attempts, error: attemptsError }] = await Promise.all([
    supabase
      .from('export_commercialista_log')
      .select('id, settimana_riferimento, eseguito_at, esito, dry_run, numero_documenti, email_destinatario, nome_file, errore_dettaglio, tipo_export')
      .eq('settimana_riferimento', weekStart)
      .order('eseguito_at', { ascending: false }),
    supabase
      .from('export_commercialista_delivery_attempts')
      .select('id, resend_email_id, email_destinatario, email_mittente, email_subject, attachment_name, document_count, chunk_index, chunk_total, resend_last_event, checked_at')
      .eq('settimana_riferimento', weekStart)
      .order('chunk_index', { ascending: true }),
  ]);

  if (logsError) throw new Error(`Errore lettura export_commercialista_log: ${logsError.message}`);
  if (attemptsError) throw new Error(`Errore lettura export_commercialista_delivery_attempts: ${attemptsError.message}`);

  const latestRealLog = (logs || []).find((row) => row.dry_run === false) || null;
  return { logs: logs || [], attempts: attempts || [], latestRealLog };
}

async function rerunWeek({ supabase, reference, weekStart, internals }) {
  const documents = await internals.listIssuedDocumentsInRange({
    rangeStart: reference.start,
    rangeEndExclusive: reference.end,
  });

  const prepared = await internals.buildExportArtifacts(documents, reference.month);
  const attachments = await internals.buildAttachments(prepared.files, weekStart);
  const canSendRealEmail = prepared.files.length > 0 && prepared.fallbacksCount === 0;
  const warnings = [...prepared.warnings];
  let sendResults = [];
  let blockedReason = '';

  if (prepared.files.length === 0) {
    warnings.push('Nessun documento trovato nel periodo richiesto.');
  } else if (!canSendRealEmail) {
    blockedReason = 'Presenza di XML fallback: invio automatico non confermabile.';
    warnings.push(blockedReason);
  } else {
    try {
      sendResults = await internals.sendEmailWithAttachments({
        periodLabel: reference.label,
        attachments,
        documentsCount: prepared.files.length,
        tipoExport: prepared.exportType,
        dryRun: false,
        warnings,
      });
      await internals.safeInsertDeliveryAttempts(supabase, {
        weekStart,
        periodLabel: reference.label,
        month: reference.month,
        sendResults,
      });
    } catch (error) {
      blockedReason = error.message;
    }
  }

  let esito = 'SUCCESSO';
  if (prepared.files.length === 0) {
    esito = 'NO_DOCUMENTS';
  } else if (blockedReason) {
    esito = 'ERRORE';
  }

  await internals.safeInsertLog(supabase, {
    mese_riferimento: `${reference.month}-01`,
    settimana_riferimento: weekStart,
    esito,
    numero_documenti: prepared.files.length,
    tipo_export: prepared.exportType,
    xml_source: prepared.xmlSource,
    email_destinatario: process.env.COMMERCIALISTA_EMAIL || null,
    dry_run: false,
    nome_file: attachments[0]?.filename || null,
    errore_dettaglio: buildStructuredDetail({
      originalsCount: prepared.originalsCount,
      fallbacksCount: prepared.fallbacksCount,
      emailChunks: sendResults.length,
      detail: blockedReason || warnings.join(' | '),
    }),
  });

  return { prepared, attachments, sendResults, blockedReason };
}

async function verifyAttempts({ supabase, weekStart, reference, internals, attempts }) {
  if (!attempts.length) {
    return { success: false, verificationRows: [], verificationBlockedReason: '' };
  }

  try {
    const result = await internals.verifyDeliveryAttempts({
      supabase,
      weekStart,
      periodLabel: reference.label,
    });
    return {
      success: Boolean(result?.success),
      verificationRows: Array.isArray(result?.delivery_attempts) ? result.delivery_attempts : [],
      verificationBlockedReason: '',
    };
  } catch (error) {
    return {
      success: false,
      verificationRows: attempts.map((attempt) => ({
        resend_email_id: attempt.resend_email_id,
        to: attempt.email_destinatario,
        subject: attempt.email_subject,
        attachment_name: attempt.attachment_name,
        document_count: attempt.document_count,
        last_event: attempt.resend_last_event || 'unknown',
      })),
      verificationBlockedReason: error.message,
    };
  }
}

async function main() {
  ensureEnv();
  const exportsModule = await loadFunctionInternals();
  const internals = exportsModule.__internals;
  const explicitWeekStart = parseWeekStartArg();
  const reference = explicitWeekStart
    ? internals.getWeekRangeReference(explicitWeekStart)
    : internals.getPreviousWeekReference(new Date());
  const weekStart = explicitWeekStart || reference.start;
  const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

  let state = await fetchWeekState(supabase, weekStart);
  let rerunResult = null;
  if (!state.latestRealLog || state.latestRealLog.esito === 'ERRORE') {
    rerunResult = await rerunWeek({ supabase, reference, weekStart, internals });
    state = await fetchWeekState(supabase, weekStart);
  }

  const verification = await verifyAttempts({
    supabase,
    weekStart,
    reference,
    internals,
    attempts: state.attempts,
  });

  const latestRealLog = state.latestRealLog;
  const documentsCount = Number(latestRealLog?.numero_documenti ?? rerunResult?.prepared?.files.length ?? 0);
  const recipient = latestRealLog?.email_destinatario || state.attempts[0]?.email_destinatario || getReportingEnv('COMMERCIALISTA_EMAIL') || '';
  const sender = state.attempts[0]?.email_mittente || getReportingEnv('EMAIL_FROM') || '';
  const chunkCount = verification.verificationRows.length || state.attempts.length || rerunResult?.sendResults?.length || rerunResult?.attachments?.length || 0;

  let finalStatus = latestRealLog?.esito || 'UNKNOWN';
  let bundlePath = '';
  let blockedReason = '';

  if (verification.success) {
    finalStatus = 'DELIVERED';
  } else if (latestRealLog?.esito === 'NO_DOCUMENTS') {
    finalStatus = 'NO_DOCUMENTS';
  } else if (isClosedStatus(latestRealLog?.esito)) {
    finalStatus = latestRealLog.esito;
  }

  const needsBundle = documentsCount > 0 && (
    finalStatus === 'ERRORE' ||
    (!verification.success && state.attempts.length > 0) ||
    (rerunResult?.blockedReason)
  );

  if (needsBundle) {
    const prepared = rerunResult?.prepared || (() => { throw new Error('Bundle richiesto ma artefatti non disponibili.'); })();
    const attachments = rerunResult?.attachments || [];
    blockedReason = rerunResult?.blockedReason || verification.verificationBlockedReason || 'Delivery non verificabile.';
    bundlePath = await writeRecoveryBundle({
      weekStart,
      reference,
      attachments,
      prepared,
      reason: blockedReason,
    });
    finalStatus = 'BLOCKED_BUNDLE_READY';
  } else if (verification.verificationBlockedReason) {
    blockedReason = verification.verificationBlockedReason;
  }

  const summary = {
    week_start: weekStart,
    destinatario: recipient,
    mittente: sender,
    numero_documenti: documentsCount,
    numero_email_chunk: chunkCount,
    final_status: finalStatus,
    latest_real_esito: latestRealLog?.esito || null,
    resend_last_events: verification.verificationRows.map((row) => row.last_event),
    bundle_path: bundlePath || null,
    blocked_reason: blockedReason || null,
  };

  console.log(JSON.stringify(summary, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
