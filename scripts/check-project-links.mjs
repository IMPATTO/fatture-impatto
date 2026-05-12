import fs from 'node:fs';
import path from 'node:path';

import { getCanonicalSiteUrl, legacySiteUrls } from '../config/site-links.mjs';

const repoRoot = process.cwd();
const defaultConfigPath = path.join(repoRoot, 'config', 'project-links.json');
const inputPath = process.argv[2] ? path.resolve(repoRoot, process.argv[2]) : defaultConfigPath;
const timeoutMs = 15000;
const maxRedirects = 5;
const canonicalPortaleBase = getCanonicalSiteUrl('portale');
const legacyCheckinBase = legacySiteUrls.checkinNetlify;
const legacyCheckinHostPattern = new URL(legacyCheckinBase).host.replace(/\./g, '\\.');
const canonicalPortaleHostPattern = new URL(canonicalPortaleBase).host.replace(/\./g, '\\.');

const canonicalChecks = [
  {
    pattern: new RegExp(`${legacyCheckinHostPattern}\\/(backoffice|portale|index|carica-documenti-fattura)`, 'i'),
    canonicalPrefix: `${canonicalPortaleBase}/`,
  },
];

const wrongProjectRules = [
  {
    pattern: new RegExp(`(${canonicalPortaleHostPattern}|${legacyCheckinHostPattern})\\/(backoffice|portale|index|carica-documenti-fattura|residence)`, 'i'),
    forbiddenText: /villa margherita rimini/i,
    reason: 'serves Villa Margherita content on a check-in route',
  },
];

function loadUrls(filePath) {
  if (!fs.existsSync(filePath)) {
    throw new Error(`Missing links config: ${filePath}`);
  }
  const raw = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  if (!Array.isArray(raw)) {
    throw new Error(`Invalid links config, expected array: ${filePath}`);
  }
  return raw.map((value) => String(value || '').trim()).filter(Boolean);
}

function withTimeout(signal, ms) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), ms);
  if (signal) {
    signal.addEventListener('abort', () => controller.abort(), { once: true });
  }
  return {
    signal: controller.signal,
    clear: () => clearTimeout(timer),
  };
}

async function fetchText(url) {
  const { signal, clear } = withTimeout(null, timeoutMs);
  try {
    const response = await fetch(url, {
      redirect: 'follow',
      signal,
      headers: {
        'user-agent': 'fatture-impatto-link-check/1.0',
        accept: 'text/html,application/json,text/plain,*/*',
      },
    });
    const body = await response.text();
    return { response, body };
  } finally {
    clear();
  }
}

function classify(url, response, body) {
  const finalUrl = response.url || url;
  const contentType = String(response.headers.get('content-type') || '').toLowerCase();
  const isHtml = contentType.includes('text/html');
  const redirects = finalUrl !== url;
  const issues = [];
  const notes = [];

  if (!response.ok) {
    issues.push(`HTTP ${response.status}`);
  }

  if (isHtml && /page not found/i.test(body)) {
    issues.push('renders a page-not-found document');
  }

  for (const rule of wrongProjectRules) {
    if (rule.pattern.test(url) && rule.forbiddenText.test(body)) {
      issues.push(rule.reason);
    }
  }

  for (const check of canonicalChecks) {
    if (!check.pattern.test(url)) continue;
    if (finalUrl.startsWith(check.canonicalPrefix)) {
      notes.push(`redirects to canonical ${finalUrl}`);
    } else {
      notes.push(`served from legacy host ${finalUrl}`);
    }
  }

  if (redirects && !issues.length) {
    notes.push(`final URL ${finalUrl}`);
  }

  return {
    url,
    finalUrl,
    status: response.status,
    contentType,
    ok: issues.length === 0,
    issues,
    notes,
  };
}

function groupByHost(results) {
  const groups = new Map();
  for (const result of results) {
    const host = new URL(result.url).host;
    if (!groups.has(host)) groups.set(host, []);
    groups.get(host).push(result);
  }
  return groups;
}

function printSummary(results) {
  const failed = results.filter((result) => !result.ok);
  const groups = groupByHost(results);

  console.log(`Checked ${results.length} links across ${groups.size} hosts.`);
  for (const [host, list] of groups) {
    const broken = list.filter((item) => !item.ok).length;
    console.log(`- ${host}: ${list.length} checked, ${broken} failing`);
  }

  if (!failed.length) {
    console.log('\nAll links look healthy.');
    return;
  }

  console.log('\nFailing links:');
  for (const result of failed) {
    const detail = result.issues.join('; ');
    console.log(`- ${result.url} -> ${detail}`);
    if (result.finalUrl && result.finalUrl !== result.url) {
      console.log(`  final URL: ${result.finalUrl}`);
    }
  }
}

async function main() {
  const urls = loadUrls(inputPath);
  const results = [];

  for (const url of urls) {
    try {
      const { response, body } = await fetchText(url);
      results.push(classify(url, response, body));
    } catch (error) {
      results.push({
        url,
        finalUrl: url,
        status: 0,
        contentType: '',
        ok: false,
        issues: [error.name === 'AbortError' ? 'request timeout' : error.message],
        notes: [],
      });
    }
  }

  printSummary(results);

  const outputPath = path.join(repoRoot, 'reports', 'audit', 'latest-link-check.json');
  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  fs.writeFileSync(
    outputPath,
    `${JSON.stringify({ checked_at: new Date().toISOString(), config: inputPath, results }, null, 2)}\n`,
    'utf8',
  );

  if (results.some((result) => !result.ok)) {
    process.exit(1);
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
