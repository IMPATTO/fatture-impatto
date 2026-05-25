import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';

const repoRoot = process.cwd();
const packageJsonPath = path.join(repoRoot, 'package.json');

function loadPackageScripts() {
  const pkg = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'));
  return Object.entries(pkg.scripts || {});
}

function extractCommandTargets(command) {
  const targets = [];
  const regex = /\b(?:node|bash)\s+([^\s"'`]+)/gu;
  let match;
  while ((match = regex.exec(String(command || '')))) {
    targets.push(match[1]);
  }
  return targets;
}

function isTracked(relativePath) {
  try {
    execFileSync('git', ['ls-files', '--error-unmatch', relativePath], {
      cwd: repoRoot,
      stdio: 'ignore',
    });
    return true;
  } catch (_error) {
    return false;
  }
}

function main() {
  const missing = [];
  const untracked = [];

  for (const [scriptName, command] of loadPackageScripts()) {
    for (const target of extractCommandTargets(command)) {
      const relativePath = target.replace(/\\/g, '/');
      const absolutePath = path.join(repoRoot, relativePath);
      if (!fs.existsSync(absolutePath)) {
        missing.push({ scriptName, relativePath });
        continue;
      }
      if (!isTracked(relativePath)) {
        untracked.push({ scriptName, relativePath });
      }
    }
  }

  if (missing.length || untracked.length) {
    if (missing.length) {
      console.error('Package scripts with missing targets:');
      for (const item of missing) {
        console.error(`- ${item.scriptName}: ${item.relativePath}`);
      }
    }

    if (untracked.length) {
      console.error('Package scripts with untracked targets:');
      for (const item of untracked) {
        console.error(`- ${item.scriptName}: ${item.relativePath}`);
      }
    }

    process.exit(1);
  }

  console.log('All package script targets exist and are tracked.');
}

main();
