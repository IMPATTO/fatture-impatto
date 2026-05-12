import fs from 'node:fs';
import path from 'node:path';

const repoRoot = process.cwd();

const ignoredDirectories = new Set([
  '.netlify',
  '.git',
  'dist',
  'docs',
  'extracted-repos',
  'node_modules',
  'reports',
]);

const scannedExtensions = new Set([
  '.html',
  '.js',
  '.json',
  '.mjs',
  '.sql',
  '.toml',
]);

const deprecatedPatterns = [
  /pr-luca/g,
  /pr_luca/g,
  /PR Luca/g,
  /backoffice-pr-luca\.html/g,
  /pr_luca_riccione\.html/g,
];

const allowedFiles = new Set([
  'backoffice-pr-luca.html',
  'config/project-links-deprecated.json',
  'config/site-links.mjs',
  'js/pr-luca-shared.js',
  'netlify/functions/approve-partner-booking.js',
  'netlify/functions/pr-luca-data.js',
  'netlify/functions/shared-app-login.js',
  'pr_luca_riccione.html',
  'residence-backoffice.html',
  'scripts/check-deprecated-references.mjs',
  'setup-supabase.sql',
  'sites/site-registry.mjs',
]);

function walk(currentPath, results) {
  const stat = fs.statSync(currentPath);
  const relativePath = path.relative(repoRoot, currentPath);

  if (stat.isDirectory()) {
    if (ignoredDirectories.has(path.basename(currentPath))) return;
    for (const entry of fs.readdirSync(currentPath)) {
      walk(path.join(currentPath, entry), results);
    }
    return;
  }

  if (!scannedExtensions.has(path.extname(currentPath))) return;
  results.push(relativePath);
}

function findMatches(filePath) {
  const absolutePath = path.join(repoRoot, filePath);
  const content = fs.readFileSync(absolutePath, 'utf8');
  const findings = [];
  const lines = content.split('\n');

  lines.forEach((line, index) => {
    const hasMatch = deprecatedPatterns.some((pattern) => {
      pattern.lastIndex = 0;
      return pattern.test(line);
    });
    if (!hasMatch) return;
    findings.push({
      lineNumber: index + 1,
      text: line.trim(),
    });
  });

  return findings;
}

function main() {
  const files = [];
  walk(repoRoot, files);

  const violations = [];

  for (const filePath of files.sort()) {
    if (allowedFiles.has(filePath)) continue;
    const matches = findMatches(filePath);
    if (!matches.length) continue;
    violations.push({ filePath, matches });
  }

  if (!violations.length) {
    console.log('No deprecated pr-luca references found outside the allowed legacy perimeter.');
    return;
  }

  console.error('Deprecated pr-luca references found outside the allowed legacy perimeter:\n');
  for (const violation of violations) {
    console.error(`- ${violation.filePath}`);
    for (const match of violation.matches) {
      console.error(`  ${match.lineNumber}: ${match.text}`);
    }
  }

  process.exit(1);
}

main();
