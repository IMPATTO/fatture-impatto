import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, '..');
const targetRoot = path.resolve(process.argv[2] || path.join(repoRoot, 'extracted-repos', 'residence-pr'));

const trackedFiles = new Set(
  execFileSync('git', ['ls-files', '-z'], { cwd: repoRoot, encoding: 'utf8' })
    .split('\0')
    .filter(Boolean),
);
const sourceWarnings = [];

const filesToCopy = [
  'backoffice-pr-luca.html',
  'pr_kekko_riccione.html',
  'pr_luca_riccione.html',
  'residence-backoffice.html',
  'residence-kekko.html',
  'js/pr-luca-shared.js',
  'js/shared-app-auth.js',
  'js/supabase-client.js',
  'netlify/functions/_lib/shared-auth.js',
  'netlify/functions/approve-partner-booking.js',
  'netlify/functions/get-calendar.js',
  'netlify/functions/pr-luca-data.js',
  'netlify/functions/residence-api.js',
  'netlify/functions/shared-app-login.js',
  'setup-supabase.sql',
  'supabase/migrations/202605050001_create_partner_booking_requests.sql',
  'supabase/migrations/202605060001_add_rm_booking_payment_fields.sql',
  'supabase/migrations/202605100004_harden_residence_and_partner_access.sql',
];

const generatedFiles = new Map([
  ['.gitignore', `node_modules
.netlify
.DS_Store
`],
  ['package.json', `${JSON.stringify({
    name: 'residence-pr-impatto',
    version: '1.0.0',
    private: true,
    scripts: {
      start: 'netlify dev',
    },
    dependencies: {
      '@supabase/supabase-js': '^2.102.1',
    },
  }, null, 2)}
`],
  ['netlify.toml', `[build]
  publish = "."

[functions]
  directory = "netlify/functions"

[[headers]]
  for = "/*"
  [headers.values]
    X-Frame-Options = "DENY"
    X-Content-Type-Options = "nosniff"
    Referrer-Policy = "strict-origin-when-cross-origin"
`],
  ['README.md', `# Residence + PR

Snapshot autonoma estratta da \`fatture-impatto\`.

## Include

- Residence Margherita lato partner e backoffice
- calendario e richieste PR Luca
- Netlify Functions minime collegate
- migration SQL minime per \`rm_*\` e \`partner_booking_requests\`

## Variabili ambiente

- \`SUPABASE_URL\`
- \`SUPABASE_RUNTIME_URL\` opzionale ma consigliata se Netlify maschera il valore
- \`SUPABASE_ANON_KEY\`
- \`SUPABASE_SERVICE_ROLE_KEY\`
- \`APP_SESSION_SECRET\`
- \`RESIDENCE_KEKKO_PASSWORDS\`
- \`RESIDENCE_ADMIN_PASSWORDS\`
- \`PR_LUCA_PASSWORDS\`
- \`BEDS24_API_KEY\`

## Avvio locale

\`\`\`bash
npm install
netlify dev
\`\`\`

## Deploy

Pubblica direttamente questa repo su Netlify usando il \`netlify.toml\` in root.
`],
]);

function ensureSourceFile(relativePath) {
  const absolutePath = path.join(repoRoot, relativePath);
  if (!fs.existsSync(absolutePath)) {
    throw new Error(`Required file is missing: ${relativePath}`);
  }
  if (!trackedFiles.has(relativePath)) {
    sourceWarnings.push(`Copied untracked working-tree file: ${relativePath}`);
  }
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function copyTrackedFile(relativePath) {
  ensureSourceFile(relativePath);
  const source = path.join(repoRoot, relativePath);
  const destination = path.join(targetRoot, relativePath);
  ensureDir(path.dirname(destination));
  fs.copyFileSync(source, destination);
}

function writeFile(relativePath, content) {
  const destination = path.join(targetRoot, relativePath);
  ensureDir(path.dirname(destination));
  fs.writeFileSync(destination, content, 'utf8');
}

fs.rmSync(targetRoot, { recursive: true, force: true });
ensureDir(targetRoot);

for (const relativePath of filesToCopy) {
  copyTrackedFile(relativePath);
}

for (const [relativePath, content] of generatedFiles.entries()) {
  writeFile(relativePath, content);
}

const summary = {
  generatedAt: new Date().toISOString(),
  sourceRepo: repoRoot,
  targetRoot,
  copiedFiles: filesToCopy,
  warnings: sourceWarnings,
};

writeFile('_extract-summary.json', `${JSON.stringify(summary, null, 2)}\n`);

console.log(`Residence + PR repo extracted into ${targetRoot}`);
