import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, '..', '..');

const SNAPSHOT_ENTRY_FILES = [
  'backoffice-pr-luca.html',
  'pr_kekko_riccione.html',
  'pr_luca_riccione.html',
  'residence-backoffice.html',
  'residence-kekko.html',
  'js/shared-app-auth.js',
  'js/supabase-client.js',
  'netlify/functions/get-calendar.js',
  'netlify/functions/residence-api.js',
  'netlify/functions/shared-app-login.js',
  'setup-supabase.sql',
  'supabase/migrations/202605050001_create_partner_booking_requests.sql',
  'supabase/migrations/202605060001_add_rm_booking_payment_fields.sql',
  'supabase/migrations/202605100004_harden_residence_and_partner_access.sql',
];

const SNAPSHOT_GENERATED_FILES = {
  '.gitignore': `node_modules
.netlify
.DS_Store
`,
  'README.md': `# Residence + PR

Snapshot autonoma estratta da \`fatture-impatto\`.

## Include

- Residence Margherita lato partner e backoffice
- storico legacy Luca servito con URL statici compatibili
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
- \`BEDS24_API_KEY\`

## Avvio locale

\`\`\`bash
npm install
netlify dev
\`\`\`

## Deploy

Pubblica direttamente questa repo su Netlify usando il \`netlify.toml\` in root.
`,
  'netlify.toml': `[build]
  publish = "."

[functions]
  directory = "netlify/functions"

[[headers]]
  for = "/*"
  [headers.values]
    X-Frame-Options = "DENY"
    X-Content-Type-Options = "nosniff"
    Referrer-Policy = "strict-origin-when-cross-origin"
`,
  'package.json': `{
  "name": "residence-pr-impatto",
  "version": "1.0.0",
  "private": true,
  "scripts": {
    "start": "netlify dev"
  },
  "dependencies": {
    "@supabase/supabase-js": "^2.102.1"
  }
}
`,
};

function unique(items) {
  return [...new Set(items.filter(Boolean))];
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

function collectRecursiveDependencies(relativePath, seen = new Set()) {
  if (seen.has(relativePath)) return [];
  seen.add(relativePath);

  const absolutePath = path.join(repoRoot, relativePath);
  const files = [relativePath];
  if (!fs.existsSync(absolutePath)) return files;

  const code = fs.readFileSync(absolutePath, 'utf8');
  const dependencies = findRelativeDependencies(relativePath, code)
    .filter((candidate) => fs.existsSync(path.join(repoRoot, candidate)));

  for (const dependency of dependencies) {
    files.push(...collectRecursiveDependencies(dependency, seen));
  }

  return files;
}

export const RESIDENCE_PR_SNAPSHOT_TARGET = 'extracted-repos/residence-pr';
export const RESIDENCE_PR_SNAPSHOT_WARNINGS = [
  'Legacy Luca runtime removed from snapshot: remaining Luca pages are static tombstones kept for URL compatibility.',
];
export const RESIDENCE_PR_REMOVED_RUNTIME_FILES = [
  '.netlify/functions/get-calendar.zip',
  '.netlify/functions/manifest.json',
  '.netlify/functions/residence-api.zip',
  '.netlify/functions/shared-app-login.zip',
  '.netlify/netlify.toml',
  'js/legacy-host-redirect.js',
];
export const RESIDENCE_PR_SNAPSHOT_FILES = unique(
  SNAPSHOT_ENTRY_FILES.flatMap((relativePath) => collectRecursiveDependencies(relativePath)),
).sort();

export function getResidencePrGeneratedFiles() {
  return { ...SNAPSHOT_GENERATED_FILES };
}
