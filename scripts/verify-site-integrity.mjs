import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';

const repoRoot = process.cwd();
const trackedFiles = new Set(
  execFileSync('git', ['ls-files', '-z'], { cwd: repoRoot, encoding: 'utf8' })
    .split('\0')
    .filter(Boolean),
);

const filesToScan = [...trackedFiles].filter((file) => /\.(html|js)$/i.test(file) && fileExists(file));
const issues = [];

const LOCAL_PATH_RE = /(?:href|src)=["']([^"'#]+)["']|fetch\(\s*["']([^"']+)["']/g;
const SKIP_PREFIXES = ['http://', 'https://', 'mailto:', 'tel:', 'data:', 'javascript:'];
const CHECKABLE_EXTENSIONS = new Set([
  '.html',
  '.js',
  '.css',
  '.json',
  '.csv',
  '.png',
  '.jpg',
  '.jpeg',
  '.svg',
  '.webp',
  '.woff',
  '.woff2',
]);
const siteRootCache = new Map();

function normalizeReference(rawRef) {
  const clean = String(rawRef || '').trim();
  if (!clean) return null;
  if (clean.includes('${')) return null;
  if (SKIP_PREFIXES.some((prefix) => clean.startsWith(prefix))) return null;
  const withoutQuery = clean.split('?')[0].split('#')[0];
  if (!withoutQuery) return null;
  return withoutQuery;
}

function findNearestSiteRootDir(sourceFile) {
  const sourceDir = path.dirname(path.join(repoRoot, sourceFile));
  if (siteRootCache.has(sourceDir)) {
    return siteRootCache.get(sourceDir);
  }

  let currentDir = sourceDir;
  while (currentDir.startsWith(repoRoot)) {
    if (fs.existsSync(path.join(currentDir, 'netlify.toml'))) {
      const relativeRoot = path.relative(repoRoot, currentDir) || '.';
      siteRootCache.set(sourceDir, relativeRoot);
      return relativeRoot;
    }
    const parentDir = path.dirname(currentDir);
    if (parentDir === currentDir) break;
    currentDir = parentDir;
  }

  siteRootCache.set(sourceDir, '.');
  return '.';
}

function resolveRepoTarget(sourceFile, ref) {
  if (ref.startsWith('/.netlify/functions/')) {
    const fnName = ref.replace('/.netlify/functions/', '').replace(/\/+$/, '');
    if (!fnName) return null;
    return `netlify/functions/${fnName}.js`;
  }

  const ext = path.extname(ref);
  if (ext && !CHECKABLE_EXTENSIONS.has(ext.toLowerCase())) return null;

  if (ref.startsWith('/')) {
    const siteRootDir = findNearestSiteRootDir(sourceFile);
    const localTarget = ref.replace(/^\/+/, '');
    return path.normalize(path.join(siteRootDir, localTarget || 'index.html'));
  }

  const sourceDir = path.dirname(sourceFile);
  return path.normalize(path.join(sourceDir, ref));
}

function fileExists(repoPath) {
  return fs.existsSync(path.join(repoRoot, repoPath));
}

for (const file of filesToScan) {
  const content = fs.readFileSync(path.join(repoRoot, file), 'utf8');
  for (const match of content.matchAll(LOCAL_PATH_RE)) {
    const ref = normalizeReference(match[1] || match[2]);
    if (!ref) continue;

    const target = resolveRepoTarget(file, ref);
    if (!target) continue;
    if (target.startsWith('..')) {
      issues.push(`${file} -> ${ref} escapes repository root`);
      continue;
    }

    if (!fileExists(target)) {
      issues.push(`${file} -> ${ref} is missing (${target})`);
      continue;
    }

    if (!trackedFiles.has(target)) {
      issues.push(`${file} -> ${ref} exists locally but is not tracked by git (${target})`);
    }
  }
}

if (issues.length) {
  console.error('\nSite integrity check failed.\n');
  for (const issue of issues) {
    console.error(`- ${issue}`);
  }
  console.error('\nFix the missing/untracked references before deploying.\n');
  process.exit(1);
}

console.log(`Site integrity check passed for ${filesToScan.length} tracked HTML/JS files.`);
