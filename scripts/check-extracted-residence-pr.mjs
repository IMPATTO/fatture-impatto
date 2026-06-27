import fs from 'node:fs';
import path from 'node:path';

import {
  getResidencePrGeneratedFiles,
  RESIDENCE_PR_REMOVED_RUNTIME_FILES,
  RESIDENCE_PR_SNAPSHOT_FILES,
  RESIDENCE_PR_SNAPSHOT_TARGET,
} from './lib/residence-pr-snapshot.mjs';

const repoRoot = process.cwd();
const targetRoot = path.join(repoRoot, RESIDENCE_PR_SNAPSHOT_TARGET);
const issues = [];
const generatedFiles = getResidencePrGeneratedFiles();
const allowedSnapshotFiles = new Set([
  ...RESIDENCE_PR_SNAPSHOT_FILES,
  ...Object.keys(generatedFiles),
  '_extract-summary.json',
]);

function readFileSafe(filePath) {
  return fs.existsSync(filePath) ? fs.readFileSync(filePath, 'utf8') : null;
}

function walkFiles(rootDir, currentDir = rootDir) {
  if (!fs.existsSync(currentDir)) return [];

  const files = [];
  const entries = fs.readdirSync(currentDir, { withFileTypes: true });
  for (const entry of entries) {
    const absolutePath = path.join(currentDir, entry.name);
    if (entry.isDirectory()) {
      files.push(...walkFiles(rootDir, absolutePath));
      continue;
    }
    files.push(path.relative(rootDir, absolutePath));
  }
  return files;
}

for (const relativePath of RESIDENCE_PR_SNAPSHOT_FILES) {
  const source = path.join(repoRoot, relativePath);
  const target = path.join(targetRoot, relativePath);

  if (!fs.existsSync(source)) {
    issues.push(`source missing: ${relativePath}`);
    continue;
  }
  if (!fs.existsSync(target)) {
    issues.push(`snapshot missing: ${relativePath}`);
    continue;
  }

  const sourceContent = readFileSafe(source);
  const targetContent = readFileSafe(target);
  if (sourceContent !== targetContent) {
    issues.push(`snapshot drift: ${relativePath}`);
  }
}

for (const relativePath of RESIDENCE_PR_REMOVED_RUNTIME_FILES) {
  const target = path.join(targetRoot, relativePath);
  if (fs.existsSync(target)) {
    issues.push(`deprecated runtime file still present in snapshot: ${relativePath}`);
  }
}

for (const relativePath of walkFiles(targetRoot)) {
  if (!allowedSnapshotFiles.has(relativePath)) {
    issues.push(`unexpected file in snapshot: ${relativePath}`);
  }
}

for (const [relativePath, expectedContent] of Object.entries(generatedFiles)) {
  const target = path.join(targetRoot, relativePath);
  if (!fs.existsSync(target)) {
    issues.push(`generated file missing from snapshot: ${relativePath}`);
    continue;
  }

  const targetContent = readFileSafe(target);
  if (targetContent !== expectedContent) {
    issues.push(`generated file drift: ${relativePath}`);
  }
}

if (issues.length) {
  console.error('Residence PR snapshot check failed.\n');
  for (const issue of issues) {
    console.error(`- ${issue}`);
  }
  console.error('\nRun `npm run sync:residence-pr-snapshot` to realign the extracted snapshot.\n');
  process.exit(1);
}

console.log('Residence PR snapshot is aligned with the source selection.');
