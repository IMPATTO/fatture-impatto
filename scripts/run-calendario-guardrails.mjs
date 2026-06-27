import { execFileSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, '..');

const steps = [
  {
    label: 'frontend inventory regression',
    cmd: 'node',
    args: ['tests/calendario-inventory-regression.test.mjs'],
  },
  {
    label: 'backend get-calendar regression',
    cmd: 'node',
    args: ['netlify/functions/__tests__/get-calendar.test.js'],
  },
  {
    label: 'beds24 push calendar regression',
    cmd: 'node',
    args: ['netlify/functions/__tests__/beds24-push-calendar.test.js'],
  },
];

for (const step of steps) {
  console.log(`\n[calendario-guard] ${step.label}`);
  execFileSync(step.cmd, step.args, {
    cwd: repoRoot,
    stdio: 'inherit',
    env: process.env,
  });
}

console.log('\n[calendario-guard] tutte le guardie calendario sono verdi');
