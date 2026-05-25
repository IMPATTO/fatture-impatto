import { execFileSync } from 'node:child_process';

const repoRoot = process.cwd();
const trackedFiles = execFileSync('git', ['ls-files', '-z'], {
  cwd: repoRoot,
  encoding: 'utf8',
})
  .split('\0')
  .filter(Boolean);

const allowedFiles = new Set([
  'docs/database-rollout-checklist.md',
  'docs/stato-attuale-per-opus.md',
  'extracted-repos/residence-pr/README.md',
  'extracted-repos/residence-pr/_extract-summary.json',
  'extracted-repos/residence-pr/supabase/migrations/202605050001_create_partner_booking_requests.sql',
  'extracted-repos/residence-pr/supabase/migrations/202605100004_harden_residence_and_partner_access.sql',
  'extracted-repos/residence-pr/supabase/migrations/202605120002_harden_partner_booking_requests.sql',
  'extracted-repos/residence-pr/supabase/migrations/202605120003_archive_legacy_luca_requests.sql',
  'scripts/check-partner-booking-legacy-usage.mjs',
  'scripts/architecture-review.mjs',
  'scripts/check-extracted-residence-pr.mjs',
  'scripts/lib/residence-pr-snapshot.mjs',
  'scripts/run-partner-booking-rollout.sh',
  'scripts/run-partner-booking-rollback.sh',
  'scripts/sync-extracted-residence-pr.mjs',
  'supabase/manual/20260512_partner_booking_requests_postcheck.sql',
  'supabase/manual/20260512_partner_booking_requests_precheck.sql',
  'supabase/manual/20260512_partner_booking_requests_rollback.sql',
  'supabase/migrations/202605050001_create_partner_booking_requests.sql',
  'supabase/migrations/202605120002_harden_partner_booking_requests.sql',
  'supabase/migrations/202605120003_archive_legacy_luca_requests.sql',
  'supabase/migrations/202605210001_resolve_security_advisor_findings.sql',
]);

const matches = [];

for (const filePath of trackedFiles) {
  let output = '';
  try {
    output = execFileSync('rg', ['-n', 'partner_booking_requests', filePath], {
      cwd: repoRoot,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
    }).trim();
  } catch (_error) {
    output = '';
  }

  if (!output) continue;
  if (allowedFiles.has(filePath)) continue;
  matches.push(`${filePath}\n${output}`);
}

if (matches.length) {
  console.error('Found forbidden runtime references to partner_booking_requests outside the legacy perimeter.\n');
  for (const match of matches) {
    console.error(match);
    console.error('');
  }
  process.exit(1);
}

console.log('No forbidden partner_booking_requests references found outside the approved legacy perimeter.');
