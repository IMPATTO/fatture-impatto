import fs from 'node:fs';
import path from 'node:path';

const repoRoot = process.cwd();
const allowedFiles = new Set([
  'js/supabase-client.js',
  'extracted-repos/residence-pr/js/supabase-client.js',
]);
const ignoredRoots = new Set([
  '.git',
  '.netlify',
  'dist',
  'node_modules',
  'reports',
]);
const issues = [];

function walk(dirPath) {
  const entries = fs.readdirSync(dirPath, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    if (ignoredRoots.has(entry.name)) continue;

    const fullPath = path.join(dirPath, entry.name);
    if (entry.isDirectory()) {
      files.push(...walk(fullPath));
      continue;
    }

    const relativePath = relative(fullPath);
    const isHtml = /\.html$/i.test(entry.name);
    const isBrowserJs = /\.(?:js|mjs|cjs)$/i.test(entry.name) && /(^|\/)js\//.test(relativePath);

    if (entry.isFile() && (isHtml || isBrowserJs)) {
      files.push(fullPath);
    }
  }

  return files;
}

function relative(filePath) {
  return path.relative(repoRoot, filePath).replace(/\\/g, '/');
}

for (const filePath of walk(repoRoot)) {
  const relativePath = relative(filePath);
  if (allowedFiles.has(relativePath)) continue;

  const source = fs.readFileSync(filePath, 'utf8');
  if (source.includes('supabase.createClient(')) {
    issues.push(`${relativePath} creates a browser Supabase client outside the approved shared client file`);
  }
}

if (issues.length) {
  console.error('Browser Supabase client check failed.\n');
  for (const issue of issues) {
    console.error(`- ${issue}`);
  }
  console.error('\nUse window.sb from js/supabase-client.js instead of recreating the browser client inline.\n');
  process.exit(1);
}

console.log('No duplicated browser Supabase client initializations found outside the approved shared client file.');
