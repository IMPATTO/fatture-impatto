import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

import { assertValidSiteRegistry, getAllPageOwners, siteRegistry } from '../sites/site-registry.mjs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, '..');
const targetSiteKey = process.env.SITE_KEY || process.argv[2];

if (!targetSiteKey || !siteRegistry[targetSiteKey]) {
  const available = Object.keys(siteRegistry).join(', ');
  console.error(`Missing or invalid SITE_KEY. Available values: ${available}`);
  process.exit(1);
}

assertValidSiteRegistry();

const trackedFiles = new Set(
  execFileSync('git', ['ls-files', '-z'], { cwd: repoRoot, encoding: 'utf8' })
    .split('\0')
    .filter(Boolean),
);

const site = siteRegistry[targetSiteKey];
const outputRoot = path.join(repoRoot, 'dist', targetSiteKey);
const outputFunctionsRoot = path.join(outputRoot, 'netlify', 'functions');
const allPageOwners = getAllPageOwners();
const calendarioBuildStamp = String(
  process.env.CALENDARIO_ASSET_VERSION
  || new Date().toISOString().replace(/\D/g, '').slice(0, 14)
);

function getSiteSourceRoot(currentSite = site) {
  return path.normalize(currentSite.sourceRoot || '.');
}

function ensureTracked(relativePath) {
  if (!trackedFiles.has(relativePath)) {
    throw new Error(`Required file is not tracked by git: ${relativePath}`);
  }
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function unique(items) {
  return [...new Set(items.filter(Boolean))];
}

function walkFiles(rootDir) {
  const files = [];
  const entries = fs.readdirSync(rootDir, { withFileTypes: true });
  for (const entry of entries) {
    const absolutePath = path.join(rootDir, entry.name);
    if (entry.isDirectory()) {
      files.push(...walkFiles(absolutePath));
      continue;
    }
    files.push(absolutePath);
  }
  return files;
}

function resolveSourceRelativePath(relativePath, currentSite = site) {
  const sourceRoot = getSiteSourceRoot(currentSite);
  return path.normalize(path.join(sourceRoot, relativePath));
}

function copyFile(relativePath, currentSite = site) {
  const sourceRelativePath = resolveSourceRelativePath(relativePath, currentSite);
  ensureTracked(sourceRelativePath);
  const source = path.join(repoRoot, sourceRelativePath);
  const destination = path.join(outputRoot, relativePath);
  ensureDir(path.dirname(destination));
  fs.copyFileSync(source, destination);
}

function findRelativeDependencies(relativePath, code) {
  const dir = path.dirname(relativePath);
  const refs = new Set();
  const patterns = [
    /require\((['"])(\.{1,2}\/[^'"]+)\1\)/g,
    /from\s+(['"])(\.{1,2}\/[^'"]+)\1/g,
    /import\((['"])(\.{1,2}\/[^'"]+)\1\)/g,
    /@import\s+(?:url\()?(["'])(\.{1,2}\/[^"')]+)\1\)?/g,
  ];

  for (const pattern of patterns) {
    for (const match of code.matchAll(pattern)) {
      refs.add(match[2]);
    }
  }

  return [...refs]
    .map((rawRef) => rawRef.replace(/[?#].*$/, ''))
    .flatMap((rawRef) => [rawRef, `${rawRef}.js`, `${rawRef}.mjs`, `${rawRef}.cjs`, `${rawRef}.css`])
    .map((candidate) => path.normalize(path.join(dir, candidate)))
    .filter((candidate, index, list) => list.indexOf(candidate) === index);
}

function copyAssetFile(relativePath, seen = new Set(), currentSite = site) {
  if (seen.has(relativePath)) return;
  seen.add(relativePath);
  copyFile(relativePath, currentSite);

  const source = path.join(repoRoot, resolveSourceRelativePath(relativePath, currentSite));
  const code = fs.readFileSync(source, 'utf8');
  const dependencies = findRelativeDependencies(relativePath, code)
    .filter((candidate) => trackedFiles.has(resolveSourceRelativePath(candidate, currentSite)));

  for (const dependency of dependencies) {
    copyAssetFile(dependency, seen, currentSite);
  }
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
  const dependencies = findRelativeDependencies(relativePath, code)
    .filter((candidate) => trackedFiles.has(candidate));

  for (const dependency of dependencies) {
    if (dependency.startsWith(path.normalize('netlify/functions/'))) {
      copyFunctionFile(dependency, seen);
    } else {
      copyFile(dependency);
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
  const destinationBaseUrl =
    process.env[siteRegistry[ownerSiteKey].envVar] ||
    siteRegistry[ownerSiteKey].fallbackUrl ||
    '';
  const destination = path.join(outputRoot, route);
  ensureDir(path.dirname(destination));
  fs.writeFileSync(destination, generateRedirectPage(route, destinationBaseUrl), 'utf8');
}

function writeLocalEntryRedirect(route, localRoute) {
  const destination = path.join(outputRoot, route);
  ensureDir(path.dirname(destination));
  fs.writeFileSync(destination, generateRedirectPage(localRoute, ''), 'utf8');
}

function writeSiteSummary() {
  const summary = {
    siteKey: targetSiteKey,
    label: site.label,
    envVar: site.envVar,
    sourceRoot: site.sourceRoot || '.',
    pages: [...(site.pages || [])],
    assets: [...(site.assets || [])],
    functions: [...(site.functions || [])],
    includedFiles: [...(site.includedFiles || [])],
  };
  fs.writeFileSync(path.join(outputRoot, '_site-build.json'), `${JSON.stringify(summary, null, 2)}\n`, 'utf8');
}

function runCalendarioGuardrails() {
  execFileSync('node', ['scripts/run-calendario-guardrails.mjs'], {
    cwd: repoRoot,
    stdio: 'inherit',
    env: process.env,
  });
}

function stampCalendarioAssetVersions() {
  const assetFiles = walkFiles(outputRoot)
    .filter((absolutePath) => /\.(html|js|css)$/i.test(absolutePath));

  for (const absolutePath of assetFiles) {
    const source = fs.readFileSync(absolutePath, 'utf8');
    const next = source.replace(/\?v=[A-Za-z0-9._-]+/g, `?v=${calendarioBuildStamp}`);
    if (next !== source) {
      fs.writeFileSync(absolutePath, next, 'utf8');
    }
  }
}

if (targetSiteKey === 'calendario' && process.env.SKIP_CALENDARIO_GUARDS !== '1') {
  runCalendarioGuardrails();
}

fs.rmSync(outputRoot, { recursive: true, force: true });
ensureDir(outputRoot);
ensureDir(outputFunctionsRoot);

const sitePages = unique(site.pages || []);
const siteAssets = unique(site.assets || []);
const siteIncludedFiles = unique(site.includedFiles || []);
const siteFunctions = unique(site.functions || []);
const includedPages = new Set(sitePages);

for (const relativePath of sitePages) copyFile(relativePath, site);
for (const relativePath of siteAssets) copyAssetFile(relativePath, new Set(), site);
for (const relativePath of siteIncludedFiles) copyFile(relativePath, site);
for (const functionName of siteFunctions) {
  copyFunctionFile(path.join('netlify', 'functions', `${functionName}.js`));
}

if (site.generateCrossSiteRedirects !== false) {
  for (const [route, ownerSiteKey] of allPageOwners.entries()) {
    if (ownerSiteKey === targetSiteKey) continue;
    if (includedPages.has(route)) continue;
    writeRedirectPage(route, ownerSiteKey);
  }
}

if (!includedPages.has('index.html')) {
  const entryPage = site.entryPage;
  if (!entryPage) {
    throw new Error(`Missing entryPage for site "${targetSiteKey}"`);
  }
  if (!includedPages.has(entryPage)) {
    throw new Error(`entryPage "${entryPage}" is not part of site "${targetSiteKey}" bundle`);
  }
  writeLocalEntryRedirect('index.html', entryPage);
}

if (targetSiteKey === 'calendario') {
  stampCalendarioAssetVersions();
}

writeSiteSummary();

console.log(`Built ${site.label} into dist/${targetSiteKey}`);
