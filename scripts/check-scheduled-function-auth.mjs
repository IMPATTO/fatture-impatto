import fs from 'node:fs';
import path from 'node:path';

const repoRoot = process.cwd();
const functionsRoot = path.join(repoRoot, 'netlify', 'functions');
const issues = [];

function walk(dirPath) {
  const entries = fs.readdirSync(dirPath, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const fullPath = path.join(dirPath, entry.name);
    if (entry.isDirectory()) {
      files.push(...walk(fullPath));
      continue;
    }
    if (entry.isFile() && /\.(?:js|mjs|cjs)$/i.test(entry.name)) {
      files.push(fullPath);
    }
  }
  return files;
}

function relative(filePath) {
  return path.relative(repoRoot, filePath);
}

for (const filePath of walk(functionsRoot)) {
  const source = fs.readFileSync(filePath, 'utf8');
  const usesScheduleHeader =
    source.includes('x-nf-event')
    || source.includes('x-netlify-event');

  if (!usesScheduleHeader) continue;

  const hasScheduledConfig =
    /exports\.config\s*=\s*\{[\s\S]*schedule\s*:/m.test(source)
    || /module\.exports\.config\s*=\s*\{[\s\S]*schedule\s*:/m.test(source);

  if (!hasScheduledConfig) {
    issues.push(`${relative(filePath)} uses scheduled-event headers without exports.config.schedule`);
  }
}

if (issues.length) {
  console.error('Scheduled function auth check failed.\n');
  for (const issue of issues) {
    console.error(`- ${issue}`);
  }
  console.error('\nUse scheduled-event headers only in real scheduled functions.\n');
  process.exit(1);
}

console.log('No scheduled-event auth bypass patterns found outside scheduled functions.');
