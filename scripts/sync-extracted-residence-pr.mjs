import fs from 'node:fs';
import path from 'node:path';

import {
  getResidencePrGeneratedFiles,
  RESIDENCE_PR_REMOVED_RUNTIME_FILES,
  RESIDENCE_PR_SNAPSHOT_FILES,
  RESIDENCE_PR_SNAPSHOT_TARGET,
  RESIDENCE_PR_SNAPSHOT_WARNINGS,
} from './lib/residence-pr-snapshot.mjs';

const repoRoot = process.cwd();
const targetRoot = path.join(repoRoot, RESIDENCE_PR_SNAPSHOT_TARGET);

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
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

function copyFile(relativePath) {
  const source = path.join(repoRoot, relativePath);
  const target = path.join(targetRoot, relativePath);
  if (!fs.existsSync(source)) {
    throw new Error(`Missing source file for residence-pr snapshot: ${relativePath}`);
  }
  ensureDir(path.dirname(target));
  fs.copyFileSync(source, target);
}

function removeFile(relativePath) {
  const target = path.join(targetRoot, relativePath);
  fs.rmSync(target, { force: true, recursive: true });
}

function pruneUnexpectedFiles(generatedFiles) {
  const allowedFiles = new Set([
    ...RESIDENCE_PR_SNAPSHOT_FILES,
    ...Object.keys(generatedFiles),
    '_extract-summary.json',
  ]);
  let removed = 0;

  for (const relativePath of walkFiles(targetRoot)) {
    if (allowedFiles.has(relativePath)) continue;
    removeFile(relativePath);
    removed += 1;
  }

  return removed;
}

function writeSummary() {
  const summaryPath = path.join(targetRoot, '_extract-summary.json');
  const payload = {
    generatedAt: new Date().toISOString(),
    sourceRepo: repoRoot,
    targetRoot,
    copiedFiles: [...RESIDENCE_PR_SNAPSHOT_FILES],
    warnings: [...RESIDENCE_PR_SNAPSHOT_WARNINGS],
  };
  fs.writeFileSync(summaryPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
}

function writeGeneratedFiles() {
  const generatedFiles = getResidencePrGeneratedFiles();
  for (const [relativePath, content] of Object.entries(generatedFiles)) {
    const target = path.join(targetRoot, relativePath);
    ensureDir(path.dirname(target));
    fs.writeFileSync(target, content, 'utf8');
  }
}

function main() {
  ensureDir(targetRoot);
  const generatedFiles = getResidencePrGeneratedFiles();

  for (const relativePath of RESIDENCE_PR_SNAPSHOT_FILES) {
    copyFile(relativePath);
  }

  writeGeneratedFiles();

  for (const relativePath of RESIDENCE_PR_REMOVED_RUNTIME_FILES) {
    removeFile(relativePath);
  }

  writeSummary();
  const prunedFiles = pruneUnexpectedFiles(generatedFiles);

  console.log(`Synced ${RESIDENCE_PR_SNAPSHOT_FILES.length} files into ${RESIDENCE_PR_SNAPSHOT_TARGET}`);
  console.log(`Removed ${RESIDENCE_PR_REMOVED_RUNTIME_FILES.length + prunedFiles} deprecated or unexpected files from snapshot`);
}

main();
