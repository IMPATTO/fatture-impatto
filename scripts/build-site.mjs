import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

import { getAllPageOwners, siteRegistry } from '../sites/site-registry.mjs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, '..');
const targetSiteKey = process.env.SITE_KEY || process.argv[2];

if (!targetSiteKey || !siteRegistry[targetSiteKey]) {
  const available = Object.keys(siteRegistry).join(', ');
  console.error(`Missing or invalid SITE_KEY. Available values: ${available}`);
  process.exit(1);
}

const trackedFiles = new Set(
  execFileSync('git', ['ls-files', '-z'], { cwd: repoRoot, encoding: 'utf8' })
    .split('\0')
    .filter(Boolean),
);

const site = siteRegistry[targetSiteKey];
const outputRoot = path.join(repoRoot, 'dist', targetSiteKey);
const outputFunctionsRoot = path.join(outputRoot, 'netlify', 'functions');
const allPageOwners = getAllPageOwners();

function ensureTracked(relativePath) {
  if (!trackedFiles.has(relativePath)) {
    throw new Error(`Required file is not tracked by git: ${relativePath}`);
  }
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function copyFile(relativePath) {
  ensureTracked(relativePath);
  const source = path.join(repoRoot, relativePath);
  const destination = path.join(outputRoot, relativePath);
  ensureDir(path.dirname(destination));
  fs.copyFileSync(source, destination);
}

function copyFunctionFile(relativePath, seen = new Set()) {
  if (seen.has(relativePath)) return;
  seen.add(relativePath);
  ensureTracked(relativePath);

  const source = path.join(repoRoot, relativePath);
  const destination = path.join(outputRoot, relativePath);
  ensureDir(path.dirname(destination));
  fs.copyFileSync(source, destination);

  const code = fs.readFileSync(source, 'utf8');
  const dir = path.dirname(relativePath);
  const patterns = [
    /require\((['"])(\.{1,2}\/[^'"]+)\1\)/g,
    /from\s+(['"])(\.{1,2}\/[^'"]+)\1/g,
  ];

  for (const pattern of patterns) {
    for (const match of code.matchAll(pattern)) {
      const rawRef = match[2];
      const candidates = [rawRef, `${rawRef}.js`, `${rawRef}.mjs`, `${rawRef}.cjs`]
        .map((candidate) => path.normalize(path.join(dir, candidate)));
      const next = candidates.find((candidate) => trackedFiles.has(candidate));
      if (next) copyFunctionFile(next, seen);
    }
  }
}

function generateRedirectPage(route, destinationBaseUrl) {
  const href = destinationBaseUrl
    ? `${destinationBaseUrl.replace(/\/+$/, '')}/${route}`
    : `/${route}`;
  return `<!DOCTYPE html>
<html lang="it">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Redirect…</title>
  <meta http-equiv="refresh" content="0; url=${href}"/>
  <script>window.location.replace(${JSON.stringify(href)});</script>
</head>
<body>
  <p>Reindirizzamento in corso verso <a href="${href}">${href}</a>.</p>
</body>
</html>
`;
}

function writeRedirectPage(route, ownerSiteKey) {
  const destinationBaseUrl = process.env[siteRegistry[ownerSiteKey].envVar] || '';
  const destination = path.join(outputRoot, route);
  ensureDir(path.dirname(destination));
  fs.writeFileSync(destination, generateRedirectPage(route, destinationBaseUrl), 'utf8');
}

function writeSiteSummary() {
  const summary = {
    siteKey: targetSiteKey,
    label: site.label,
    envVar: site.envVar,
    pages: site.pages || [],
    assets: site.assets || [],
    functions: site.functions || [],
  };
  fs.writeFileSync(path.join(outputRoot, '_site-build.json'), `${JSON.stringify(summary, null, 2)}\n`, 'utf8');
}

fs.rmSync(outputRoot, { recursive: true, force: true });
ensureDir(outputRoot);
ensureDir(outputFunctionsRoot);

for (const relativePath of site.pages || []) copyFile(relativePath);
for (const relativePath of site.assets || []) copyFile(relativePath);
for (const relativePath of site.includedFiles || []) copyFile(relativePath);
for (const functionName of site.functions || []) {
  copyFunctionFile(path.join('netlify', 'functions', `${functionName}.js`));
}

for (const [route, ownerSiteKey] of allPageOwners.entries()) {
  if (ownerSiteKey === targetSiteKey) continue;
  writeRedirectPage(route, ownerSiteKey);
}

writeSiteSummary();

console.log(`Built ${site.label} into dist/${targetSiteKey}`);
