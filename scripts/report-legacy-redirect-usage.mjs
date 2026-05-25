import fs from 'node:fs/promises';
import path from 'node:path';
import { createClient } from '@supabase/supabase-js';

import { getLegacyCompatibilityEntries } from '../config/site-links.mjs';
import { resolveRuntimeConfig } from './lib/netlify-runtime-config.mjs';

function parseArgs(argv) {
  const args = {};
  for (const token of argv) {
    if (!token.startsWith('--')) continue;
    const [key, value] = token.slice(2).split('=');
    args[key] = value === undefined ? true : value;
  }
  return args;
}

function parseIntArg(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function isoDate(date) {
  return new Date(date.getTime() - (date.getTimezoneOffset() * 60000)).toISOString().slice(0, 10);
}

function parseIsoDate(value) {
  return new Date(`${value}T00:00:00`);
}

function addDays(date, days) {
  const next = new Date(date);
  next.setDate(next.getDate() + days);
  return next;
}

function startOfDayIso(value) {
  return `${String(value).slice(0, 10)}T00:00:00.000Z`;
}

function endOfDayIso(value) {
  return `${String(value).slice(0, 10)}T23:59:59.999Z`;
}

function buildIndex(entries) {
  return new Map(entries.map((entry) => [entry.key, entry]));
}

function unique(values) {
  return [...new Set(values)];
}

function normalizePayload(row) {
  const payload = row?.new_data && typeof row.new_data === 'object' ? row.new_data : {};
  return {
    key: String(row.record_id || payload.key || '').trim(),
    sourceHost: String(payload.source_host || '').trim().toLowerCase(),
    route: normalizeRoute(payload.route),
    canonicalUrl: String(payload.canonical_url || '').trim(),
    redirectedAt: String(payload.redirected_at || row.created_at || '').trim(),
    referrer: String(payload.referrer || '').trim(),
    userAgent: String(payload.user_agent || '').trim(),
    ipAddress: String(row.ip_address || '').trim(),
  };
}

function normalizeRoute(value) {
  const trimmed = String(value || '/').trim();
  if (!trimmed || trimmed === '/') return '/';
  return `/${trimmed.replace(/^\/+/, '')}`;
}

function summarize(rows, entryIndex) {
  const byKey = new Map();

  for (const row of rows) {
    const payload = normalizePayload(row);
    const key = payload.key || `${payload.sourceHost}${payload.route}`;
    const entry = entryIndex.get(key);
    const group = byKey.get(key) || {
      key,
      label: entry?.label || 'Legacy compatibility',
      legacyUrl: entry?.legacyUrl || '',
      canonicalUrl: entry?.canonicalUrl || payload.canonicalUrl || '',
      strategy: entry?.strategy || 'unknown',
      hits: 0,
      sourceHosts: new Set(),
      routes: new Set(),
      ipAddresses: new Set(),
      userAgents: new Set(),
      referrers: new Set(),
      firstHitAt: null,
      lastHitAt: null,
    };

    group.hits += 1;
    if (payload.sourceHost) group.sourceHosts.add(payload.sourceHost);
    if (payload.route) group.routes.add(payload.route);
    if (payload.ipAddress) group.ipAddresses.add(payload.ipAddress);
    if (payload.userAgent) group.userAgents.add(payload.userAgent);
    if (payload.referrer) group.referrers.add(payload.referrer);

      const hitAt = payload.redirectedAt || row.created_at || '';
    if (hitAt) {
      if (!group.firstHitAt || hitAt < group.firstHitAt) group.firstHitAt = hitAt;
      if (!group.lastHitAt || hitAt > group.lastHitAt) group.lastHitAt = hitAt;
    }

    byKey.set(key, group);
  }

  const groups = [...byKey.values()]
    .map((group) => ({
      ...group,
      sourceHosts: [...group.sourceHosts].sort(),
      routes: [...group.routes].sort(),
      ipAddresses: [...group.ipAddresses].sort(),
      userAgents: [...group.userAgents].sort(),
      referrers: [...group.referrers].sort(),
      distinctVisitors: group.ipAddresses.size,
    }))
    .sort((a, b) => b.hits - a.hits || a.key.localeCompare(b.key, 'it'));

  return {
    totalHits: rows.length,
    activeLegacyUrls: groups.length,
    distinctVisitors: unique(groups.flatMap((group) => group.ipAddresses)).length,
    groups,
  };
}

function renderMarkdown({ fromDate, toDate, summary }) {
  const lines = [
    '# Legacy Redirect Usage',
    '',
    `Finestra analizzata: ${fromDate} -> ${toDate}.`,
    '',
    `- redirect totali: ${summary.totalHits}`,
    `- URL legacy con traffico: ${summary.activeLegacyUrls}`,
    `- IP distinti osservati: ${summary.distinctVisitors}`,
    '',
  ];

  if (!summary.groups.length) {
    lines.push('Nessun hit legacy registrato nella finestra richiesta.');
    lines.push('');
    return `${lines.join('\n')}`;
  }

  lines.push('## Dettaglio');
  lines.push('');

  for (const group of summary.groups) {
    lines.push(`### ${group.legacyUrl || group.key}`);
    lines.push(`- hits: ${group.hits}`);
    lines.push(`- distinti (IP): ${group.distinctVisitors}`);
    lines.push(`- canonical: ${group.canonicalUrl || 'n/d'}`);
    lines.push(`- strategia: ${group.strategy}`);
    lines.push(`- primo hit: ${group.firstHitAt || 'n/d'}`);
    lines.push(`- ultimo hit: ${group.lastHitAt || 'n/d'}`);
    if (group.referrers.length) {
      lines.push(`- referrer: ${group.referrers.slice(0, 5).join(' | ')}`);
    }
    if (group.userAgents.length) {
      lines.push(`- user-agent distinti: ${group.userAgents.length}`);
    }
    lines.push('');
  }

  return `${lines.join('\n')}`;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    console.log('Usage: node scripts/report-legacy-redirect-usage.mjs [--days=30] [--from=YYYY-MM-DD] [--to=YYYY-MM-DD] [--format=json|markdown]');
    process.exit(0);
  }

  const todayIso = isoDate(new Date());
  const days = Math.max(1, parseIntArg(args.days, 30));
  const toDate = String(args.to || todayIso);
  const fromDate = String(args.from || isoDate(addDays(parseIsoDate(toDate), -(days - 1))));
  const format = String(args.format || 'markdown').trim().toLowerCase();

  const runtime = resolveRuntimeConfig({
    projectRoot: process.cwd(),
    context: process.env.NETLIFY_CONTEXT || 'production',
  });

  if (!runtime.supabaseUrl || !runtime.supabaseKey) {
    throw new Error('SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY/SUPABASE_SERVICE_KEY sono obbligatori');
  }

  const supabase = createClient(runtime.supabaseUrl, runtime.supabaseKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  });

  const { data, error } = await supabase
    .from('audit_log')
    .select('record_id,created_at,new_data,ip_address')
    .eq('action', 'legacy_redirect_hit')
    .eq('table_name', 'legacy_compatibility')
    .gte('created_at', startOfDayIso(fromDate))
    .lte('created_at', endOfDayIso(toDate))
    .order('created_at', { ascending: false })
    .limit(5000);

  if (error) throw error;

  const entries = getLegacyCompatibilityEntries().filter((entry) => entry.strategy === 'host-redirect');
  const summary = summarize(data || [], buildIndex(entries));
  const markdown = renderMarkdown({ fromDate, toDate, summary });

  const reportsDir = path.join(process.cwd(), 'reports', 'audit');
  await fs.mkdir(reportsDir, { recursive: true });
  await fs.writeFile(
    path.join(reportsDir, 'legacy-redirect-usage-latest.md'),
    `${markdown}\n`,
    'utf8',
  );

  if (format === 'json') {
    console.log(JSON.stringify({ fromDate, toDate, summary }, null, 2));
    return;
  }

  process.stdout.write(`${markdown}\n`);
}

main().catch((error) => {
  console.error(error.message || error);
  process.exit(1);
});
