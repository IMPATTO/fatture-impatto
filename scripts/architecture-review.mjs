import fs from 'node:fs';
import path from 'node:path';
import { execFileSync, spawnSync } from 'node:child_process';

import {
  getAllFunctionOwners,
  getAllPageOwners,
  sharedFunctionSites,
  siteRegistry,
} from '../sites/site-registry.mjs';
import {
  getDeprecatedProjectLinksSections,
  getProjectLinksSections,
} from '../config/site-links.mjs';

const repoRoot = process.cwd();
const reportsRoot = path.join(repoRoot, 'reports', 'architecture');
const dateStamp = new Date().toISOString().slice(0, 10);

function runNodeScript(label, scriptPath, args = [], envOverrides = {}) {
  const result = spawnSync(process.execPath, [scriptPath, ...args], {
    cwd: repoRoot,
    env: {
      ...process.env,
      ...envOverrides,
    },
    encoding: 'utf8',
    timeout: 5 * 60 * 1000,
  });

  const stdout = String(result.stdout || '').trim();
  const stderr = String(result.stderr || '').trim();
  const combined = [stdout, stderr].filter(Boolean).join('\n');

  return {
    label,
    ok: result.status === 0,
    status: result.status ?? 1,
    summary: summarizeOutput(combined) || (result.status === 0 ? 'ok' : 'no output'),
    output: combined,
  };
}

function summarizeOutput(text) {
  const lines = String(text || '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
  return lines.slice(-3).join(' | ');
}

function getGitStatusSummary() {
  const raw = execFileSync('git', ['status', '--short'], {
    cwd: repoRoot,
    encoding: 'utf8',
  }).trim();

  if (!raw) {
    return {
      modified: 0,
      added: 0,
      deleted: 0,
      untracked: 0,
      lines: [],
    };
  }

  const lines = raw.split('\n').filter(Boolean);
  const summary = {
    modified: 0,
    added: 0,
    deleted: 0,
    untracked: 0,
    lines,
  };

  for (const line of lines) {
    if (line.startsWith('??')) {
      summary.untracked += 1;
      continue;
    }
    const indexStatus = line[0];
    const worktreeStatus = line[1];
    if (indexStatus === 'M' || worktreeStatus === 'M') summary.modified += 1;
    if (indexStatus === 'A' || worktreeStatus === 'A') summary.added += 1;
    if (indexStatus === 'D' || worktreeStatus === 'D') summary.deleted += 1;
  }

  return summary;
}

function getStandaloneSections() {
  const knownSites = new Set(Object.keys(siteRegistry));
  return getProjectLinksSections().filter(
    (section) => !knownSites.has(section.key) && section.key !== 'legacy-checkin-netlify',
  );
}

function buildChecks() {
  const checks = [
    runNodeScript('verify:integrity', path.join('scripts', 'verify-site-integrity.mjs')),
    runNodeScript('check:deprecated-refs', path.join('scripts', 'check-deprecated-references.mjs')),
    runNodeScript('check:hardcoded-domains', path.join('scripts', 'check-hardcoded-domains.mjs')),
    runNodeScript('check:links', path.join('scripts', 'check-project-links.mjs')),
    runNodeScript(
      'check:links:deprecated',
      path.join('scripts', 'check-project-links.mjs'),
      [path.join('config', 'project-links-deprecated.json')],
    ),
  ];

  for (const siteKey of Object.keys(siteRegistry)) {
    checks.push(
      runNodeScript(`build:${siteKey}`, path.join('scripts', 'build-site.mjs'), [], {
        SITE_KEY: siteKey,
      }),
    );
  }

  return checks;
}

function determineAssessment(checks, standaloneSections, deprecatedSections, gitStatus) {
  const hasFailedChecks = checks.some((check) => !check.ok);
  const hasStructuralDebt =
    Object.keys(sharedFunctionSites).length > 0
    || standaloneSections.length > 0
    || deprecatedSections.length > 0
    || fs.existsSync(path.join(repoRoot, 'extracted-repos', 'residence-pr'));

  if (hasFailedChecks) return 'Needs attention';
  if (hasStructuralDebt || gitStatus.untracked || gitStatus.modified || gitStatus.deleted) {
    return 'Stable but still in transition';
  }
  return 'Stable';
}

function buildFindings(checks, standaloneSections, deprecatedSections, gitStatus) {
  const findings = [];
  const failedChecks = checks.filter((check) => !check.ok);

  if (failedChecks.length) {
    findings.push(
      `Ci sono ${failedChecks.length} check rossi: ${failedChecks.map((check) => `\`${check.label}\``).join(', ')}.`,
    );
  }

  const sharedFunctions = Object.entries(sharedFunctionSites);
  if (sharedFunctions.length) {
    findings.push(
      `Restano ${sharedFunctions.length} function condivise tra progetti: ${sharedFunctions.map(([fnName, owners]) => `\`${fnName}\` (${owners.join(', ')})`).join(', ')}.`,
    );
  }

  if (standaloneSections.length) {
    findings.push(
      `Ci sono ${standaloneSections.length} progetti standalone monitorati ma non ancora dentro al builder: ${standaloneSections.map((section) => `\`${section.key}\``).join(', ')}.`,
    );
  }

  if (deprecatedSections.length) {
    findings.push(
      `Restano ${deprecatedSections.reduce((total, section) => total + section.urls.length, 0)} URL legacy mantenuti per compatibilita.`,
    );
  }

  if (fs.existsSync(path.join(repoRoot, 'extracted-repos', 'residence-pr'))) {
    findings.push('Esiste ancora una snapshot estratta `extracted-repos/residence-pr`, quindi il rischio di drift non e del tutto sparito.');
  }

  const dirtyCount = gitStatus.modified + gitStatus.added + gitStatus.deleted + gitStatus.untracked;
  if (dirtyCount) {
    findings.push(
      `La working tree non e pulita: ${gitStatus.modified} modificati, ${gitStatus.added} aggiunti, ${gitStatus.deleted} cancellati, ${gitStatus.untracked} non tracciati.`,
    );
  }

  return findings;
}

function buildRecommendations(checks, standaloneSections) {
  const recommendations = [];
  const failedChecks = checks.filter((check) => !check.ok);

  if (failedChecks.some((check) => check.label === 'check:links')) {
    recommendations.push('Allineare subito il deploy live del sito che ha link rotti rispetto al bundle locale.');
  }

  const sharedFunctionNames = Object.keys(sharedFunctionSites);
  if (sharedFunctionNames.length) {
    recommendations.push(`Ridurre gradualmente le shared functions residue, iniziando da ${sharedFunctionNames.map((name) => `\`${name}\``).join(' e ')}.`);
  }

  if (standaloneSections.some((section) => section.key === 'villa-margherita')) {
    recommendations.push('Decidere se portare `villa-margherita` nel builder con namespace route per dominio o lasciarla definitivamente standalone.');
  }

  if (fs.existsSync(path.join(repoRoot, 'extracted-repos', 'residence-pr'))) {
    recommendations.push('Governare `extracted-repos/residence-pr`: o rigenerazione automatica, o rimozione della copia viva.');
  }

  recommendations.push('Applicare al database le migration gia presenti per allineare runtime e repo.');

  return recommendations;
}

function renderMarkdown({
  assessment,
  checks,
  gitStatus,
  standaloneSections,
  deprecatedSections,
  findings,
  recommendations,
}) {
  const lines = [
    `# Weekly Architecture Review`,
    '',
    `Data: ${dateStamp}`,
    '',
    `## Stato`,
    '',
    `Valutazione complessiva: **${assessment}**.`,
    '',
    `- Siti builder-managed: ${Object.keys(siteRegistry).length}`,
    `- Pagine con owner univoco: ${getAllPageOwners().size}`,
    `- Functions mappate: ${getAllFunctionOwners().size}`,
    `- Shared functions residue: ${Object.keys(sharedFunctionSites).length}`,
    `- Progetti standalone fuori builder: ${standaloneSections.length}`,
    `- Sezioni deprecated attive: ${deprecatedSections.length}`,
    '',
    `## Check`,
    '',
  ];

  for (const check of checks) {
    lines.push(`- ${check.ok ? 'OK' : 'FAIL'} \`${check.label}\` — ${check.summary}`);
  }

  lines.push('');
  lines.push('## Working Tree');
  lines.push('');
  lines.push(`- Modificati: ${gitStatus.modified}`);
  lines.push(`- Aggiunti: ${gitStatus.added}`);
  lines.push(`- Cancellati: ${gitStatus.deleted}`);
  lines.push(`- Non tracciati: ${gitStatus.untracked}`);

  if (findings.length) {
    lines.push('');
    lines.push('## Punti critici');
    lines.push('');
    for (const finding of findings) {
      lines.push(`- ${finding}`);
    }
  }

  if (recommendations.length) {
    lines.push('');
    lines.push('## Prossime mosse');
    lines.push('');
    for (const recommendation of recommendations) {
      lines.push(`- ${recommendation}`);
    }
  }

  lines.push('');
  return `${lines.join('\n')}`;
}

function main() {
  const checks = buildChecks();
  const gitStatus = getGitStatusSummary();
  const standaloneSections = getStandaloneSections();
  const deprecatedSections = getDeprecatedProjectLinksSections();
  const assessment = determineAssessment(checks, standaloneSections, deprecatedSections, gitStatus);
  const findings = buildFindings(checks, standaloneSections, deprecatedSections, gitStatus);
  const recommendations = buildRecommendations(checks, standaloneSections);

  const markdown = renderMarkdown({
    assessment,
    checks,
    gitStatus,
    standaloneSections,
    deprecatedSections,
    findings,
    recommendations,
  });

  fs.mkdirSync(reportsRoot, { recursive: true });
  fs.writeFileSync(path.join(reportsRoot, `${dateStamp}.md`), `${markdown}\n`, 'utf8');
  fs.writeFileSync(path.join(reportsRoot, 'latest.md'), `${markdown}\n`, 'utf8');

  console.log(markdown);
  console.log(`\nReport written to ${path.relative(repoRoot, path.join(reportsRoot, 'latest.md'))}`);
}

main();
