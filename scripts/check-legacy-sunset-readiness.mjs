import fs from 'node:fs';
import path from 'node:path';

const repoRoot = process.cwd();
const defaultReportPath = path.join(repoRoot, 'reports', 'audit', 'legacy-redirect-usage-latest.md');
const defaultOutputPath = path.join(repoRoot, 'reports', 'audit', 'legacy-sunset-readiness-latest.md');
const LEGACY_OBSERVATION_START_DATE = '2026-05-18';

function parseArgs(argv) {
  const args = {};
  for (const token of argv) {
    if (!token.startsWith('--')) continue;
    const [key, value] = token.slice(2).split('=');
    args[key] = value === undefined ? true : value;
  }
  return args;
}

function parseMarkdownReport(source) {
  const text = String(source || '');
  const windowMatch = text.match(/Finestra analizzata:\s+(\d{4}-\d{2}-\d{2})\s+->\s+(\d{4}-\d{2}-\d{2})\./u);
  const totalMatch = text.match(/redirect totali:\s+(\d+)/u);
  const urlsMatch = text.match(/URL legacy con traffico:\s+(\d+)/u);
  const visitorsMatch = text.match(/IP distinti osservati:\s+(\d+)/u);

  if (!windowMatch || !totalMatch || !urlsMatch || !visitorsMatch) {
    throw new Error('Formato report legacy redirect non riconosciuto');
  }

  return {
    fromDate: windowMatch[1],
    toDate: windowMatch[2],
    summary: {
      totalHits: Number.parseInt(totalMatch[1], 10) || 0,
      activeLegacyUrls: Number.parseInt(urlsMatch[1], 10) || 0,
      distinctVisitors: Number.parseInt(visitorsMatch[1], 10) || 0,
      groups: [],
    },
  };
}

function diffDaysInclusive(fromDate, toDate) {
  const from = new Date(`${fromDate}T00:00:00Z`);
  const to = new Date(`${toDate}T00:00:00Z`);
  return Math.round((to - from) / 86400000) + 1;
}

function maxDate(a, b) {
  return a >= b ? a : b;
}

function renderMarkdown(result) {
  const lines = [
    '# Legacy Sunset Readiness',
    '',
    `Finestra osservata: ${result.fromDate} -> ${result.toDate} (${result.observedDays} giorni).`,
    `Telemetria affidabile dal: ${result.observationStartDate}.`,
    '',
    `- redirect totali: ${result.summary.totalHits}`,
    `- URL legacy con traffico: ${result.summary.activeLegacyUrls}`,
    `- IP distinti osservati: ${result.summary.distinctVisitors}`,
    `- readiness: ${result.ready ? 'READY' : 'NOT_READY'}`,
    '',
  ];

  if (result.ready) {
    lines.push('Il perimetro legacy e pronto per iniziare il sunset operativo.');
  } else {
    lines.push('Il perimetro legacy non e ancora pronto per il sunset automatico.');
  }

  lines.push('');
  lines.push('## Criteri');
  lines.push('');
  lines.push(`- finestra minima richiesta: ${result.minimumDays} giorni`);
  lines.push(`- zero hit richiesti: sì`);
  lines.push('');

  if (result.reasons.length) {
    lines.push('## Blocchi');
    lines.push('');
    for (const reason of result.reasons) {
      lines.push(`- ${reason}`);
    }
    lines.push('');
  }

  return `${lines.join('\n')}`;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    console.log('Usage: node scripts/check-legacy-sunset-readiness.mjs [--days=30] [--report=/abs/or/relative/path.md] [--format=json|markdown]');
    process.exit(0);
  }

  const minimumDays = Math.max(1, Number.parseInt(String(args.days || '30'), 10) || 30);
  const format = String(args.format || 'markdown').trim().toLowerCase();
  const reportPath = path.resolve(repoRoot, String(args.report || defaultReportPath));

  if (!fs.existsSync(reportPath)) {
    throw new Error(`Report legacy redirect mancante: ${reportPath}`);
  }

  const parsed = parseMarkdownReport(fs.readFileSync(reportPath, 'utf8'));
  const observedDays = diffDaysInclusive(parsed.fromDate, parsed.toDate);
  const reliableFromDate = maxDate(parsed.fromDate, LEGACY_OBSERVATION_START_DATE);
  const reliableObservedDays = parsed.toDate >= LEGACY_OBSERVATION_START_DATE
    ? diffDaysInclusive(reliableFromDate, parsed.toDate)
    : 0;
  const reasons = [];

  if (reliableObservedDays < minimumDays) {
    reasons.push(`finestra affidabile troppo corta: ${reliableObservedDays}/${minimumDays} giorni (telemetria live dal ${LEGACY_OBSERVATION_START_DATE})`);
  }
  if (parsed.summary.totalHits > 0) {
    reasons.push(`restano ${parsed.summary.totalHits} hit legacy nella finestra osservata`);
  }

  const result = {
    ...parsed,
    observedDays,
    reliableObservedDays,
    minimumDays,
    observationStartDate: LEGACY_OBSERVATION_START_DATE,
    ready: reasons.length === 0,
    reasons,
  };

  const markdown = renderMarkdown(result);
  fs.mkdirSync(path.dirname(defaultOutputPath), { recursive: true });
  fs.writeFileSync(defaultOutputPath, `${markdown}\n`, 'utf8');

  if (format === 'json') {
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  process.stdout.write(`${markdown}\n`);
  if (!result.ready) process.exit(1);
}

main();
