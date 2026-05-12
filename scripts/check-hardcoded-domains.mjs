import fs from 'node:fs';
import path from 'node:path';

const repoRoot = process.cwd();

const ignoredDirectories = new Set([
  '.git',
  '.netlify',
  'dist',
  'docs',
  'extracted-repos',
  'node_modules',
  'reports',
  'villa-margherita',
]);

const scannedExtensions = new Set([
  '.html',
  '.js',
  '.json',
  '.mjs',
  '.sql',
  '.toml',
  '.txt',
  '.xml',
]);

const allowedFiles = new Set([
  'config/project-links-deprecated.json',
  'config/project-links.json',
  'config/site-links.mjs',
  'js/site-links.js',
  'scripts/check-hardcoded-domains.mjs',
  'sites/site-registry.mjs',
  'villa-margherita/netlify.toml',
]);

const forbiddenPatterns = [
  /https:\/\/[a-z0-9.-]*illupoaffitta\.com/gi,
  /https:\/\/checkinillupoaffitta\.netlify\.app/gi,
];

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
    const matches = [];
    for (const pattern of forbiddenPatterns) {
      pattern.lastIndex = 0;
      const found = line.match(pattern) || [];
      matches.push(...found);
    }
    if (!matches.length) return;
    findings.push({
      lineNumber: index + 1,
      matches: [...new Set(matches)],
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
    console.log('No forbidden hardcoded project domains found outside the approved config perimeter.');
    return;
  }

  console.error('Forbidden hardcoded project domains found outside the approved config perimeter:\n');
  for (const violation of violations) {
    console.error(`- ${violation.filePath}`);
    for (const match of violation.matches) {
      console.error(`  ${match.lineNumber}: [${match.matches.join(', ')}] ${match.text}`);
    }
  }

  process.exit(1);
}

main();
