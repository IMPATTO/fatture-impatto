import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { createClient } from '@supabase/supabase-js';

import { getAllPageOwners, siteRegistry } from '../sites/site-registry.mjs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, '..');

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://tysxeikqbgebpfyblgeb.supabase.co';
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;

if (!SUPABASE_SERVICE_ROLE_KEY) {
  console.error('Missing SUPABASE_SERVICE_ROLE_KEY / SUPABASE_SERVICE_KEY');
  process.exit(1);
}

const PORTALE_BASE = 'https://checkin.illupoaffitta.com';
const LEGACY_PORTALE_BASE = 'https://checkinillupoaffitta.netlify.app';
const KNOWN_SITE_URLS = {
  'fatt-docs': 'https://fatture.illupoaffitta.com',
  operativita: 'https://operativita.illupoaffitta.com',
  portale: PORTALE_BASE,
  contabilita: 'https://contabilita.illupoaffitta.com',
  calendario: 'https://calendario.illupoaffitta.com',
  'residence-pr': PORTALE_BASE,
};

const TEST_GUEST = {
  data_checkin: '2026-06-10',
  data_checkout: '2026-06-12',
  lingua: 'IT',
  tipo_cliente: 'privato',
  vuoi_fattura: false,
  paese_residenza: 'IT',
  numero_persone: 1,
  nome: 'Audit',
  cognome: 'Cliente',
  email: 'codex-audit-checkin@example.com',
  telefono: '+393331112233',
  data_nascita: '1980-01-01',
  luogo_nascita: 'Riccione',
  luogo_nascita_codice: '408099013',
  nato_in_italia: true,
  stato_nascita: '',
  sesso: 'M',
  cittadinanza: 'Italia',
  tipo_documento: "Carta d'identita",
  numero_documento: 'AZ1234567',
  luogo_rilascio_documento: 'Riccione',
  luogo_rilascio_codice: '408099013',
  tipo_alloggiato: 16,
  additional_guests: [],
  codice_fiscale: 'RSSMRA80A01H501U',
  codice_fiscale_verificato: true,
  indirizzo_residenza: 'Via Roma 1, Riccione, 47838, RN',
  ragione_sociale: '',
  indirizzo_fatturazione: '',
  piva_cliente: '',
  sdi: '',
  pec: '',
};

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { autoRefreshToken: false, persistSession: false },
});

async function mapWithConcurrency(items, worker, concurrency = 8) {
  const results = new Array(items.length);
  let index = 0;

  async function runWorker() {
    while (index < items.length) {
      const currentIndex = index;
      index += 1;
      results[currentIndex] = await worker(items[currentIndex], currentIndex);
    }
  }

  const workers = Array.from({ length: Math.min(concurrency, items.length) }, () => runWorker());
  await Promise.all(workers);
  return results;
}

function joinUrl(baseUrl, route) {
  return new URL(route.replace(/^\//, ''), `${baseUrl.replace(/\/+$/, '')}/`).toString();
}

async function fetchStatus(url, options = {}) {
  try {
    const response = await fetch(url, {
      redirect: 'follow',
      signal: AbortSignal.timeout(options.timeoutMs || 12000),
      ...options,
    });
    return {
      ok: response.ok,
      status: response.status,
      url,
      body: options.includeBody ? await response.text() : null,
      json: options.includeJson ? await response.json() : null,
    };
  } catch (error) {
    return { ok: false, status: 0, url, error: error.message };
  }
}

function isAcceptablePageStatus(status) {
  return status === 200;
}

function isAcceptableFunctionStatus(status) {
  return [200, 400, 401, 405].includes(status);
}

async function loadActiveApartments() {
  const { data, error } = await supabase
    .from('apartments')
    .select('id,nome_appartamento,public_checkin_key,attivo')
    .eq('attivo', true)
    .not('public_checkin_key', 'is', null)
    .order('nome_appartamento');
  if (error) throw error;
  return data.filter((row) => String(row.public_checkin_key || '').trim());
}

async function loadAlloggiatiMappings() {
  const { data, error } = await supabase
    .from('apartment_alloggiati')
    .select('apartment_id,alloggiati_account_id,id_appartamento_portale,invio_automatico,alloggiati_accounts(nome_account,questura,attivo,ultimo_invio_ok,ultimo_errore)');
  if (error) throw error;
  return new Map((data || []).map((row) => [row.apartment_id, row]));
}

async function cleanupCheckinRecord(id) {
  if (!id) return;
  const { error } = await supabase.from('ospiti_check_in').delete().eq('id', id);
  if (error) {
    console.warn(`cleanup failed for ${id}: ${error.message}`);
  }
}

async function auditApartmentLink(apartment) {
  const checkinUrl = `${PORTALE_BASE}/?apt=${encodeURIComponent(apartment.public_checkin_key)}&mode=checkin`;
  const portalUrl = `${PORTALE_BASE}/portale.html?apt=${encodeURIComponent(apartment.public_checkin_key)}&mode=open`;
  const apiUrl = `${PORTALE_BASE}/.netlify/functions/get-public-portal-data?apt=${encodeURIComponent(apartment.public_checkin_key)}&lang=IT`;

  const [checkinPage, portalPage, payloadResponse] = await Promise.all([
    fetchStatus(checkinUrl),
    fetchStatus(portalUrl),
    fetchStatus(apiUrl, { includeJson: true }),
  ]);

  let submitResult = { ok: false, status: 0, error: 'not-run' };
  const payload = { ...TEST_GUEST, apartment_ref: apartment.public_checkin_key };
  const submitResponse = await fetchStatus(`${PORTALE_BASE}/.netlify/functions/submit-public-checkin`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    includeJson: true,
  });
  submitResult = {
    ok: submitResponse.status === 200 && submitResponse.json?.ok === true,
    status: submitResponse.status,
    error: submitResponse.json?.error || submitResponse.error || null,
    fieldErrors: submitResponse.json?.fields || [],
  };

  if (submitResponse.json?.record?.id) {
    await cleanupCheckinRecord(submitResponse.json.record.id);
  }

  return {
    apartment_id: apartment.id,
    apartment_name: apartment.nome_appartamento,
    public_checkin_key: apartment.public_checkin_key,
    checkin_page_ok: isAcceptablePageStatus(checkinPage.status),
    checkin_page_status: checkinPage.status,
    portal_page_ok: isAcceptablePageStatus(portalPage.status),
    portal_page_status: portalPage.status,
    api_ok: payloadResponse.status === 200 && payloadResponse.json?.meta?.apartment_found === true,
    api_status: payloadResponse.status,
    api_meta: payloadResponse.json?.meta || null,
    submit_ok: submitResult.ok,
    submit_status: submitResult.status,
    submit_error: submitResult.error,
    submit_field_errors: submitResult.fieldErrors,
  };
}

function collectHtmlFiles(dirPath, acc = []) {
  for (const entry of fs.readdirSync(dirPath, { withFileTypes: true })) {
    if (entry.name === 'dist' || entry.name === 'node_modules' || entry.name === '.git') continue;
    const fullPath = path.join(dirPath, entry.name);
    if (entry.isDirectory()) collectHtmlFiles(fullPath, acc);
    else if (entry.isFile() && entry.name.endsWith('.html')) acc.push(fullPath);
  }
  return acc;
}

function extractClickableUrls(filePath) {
  const source = fs.readFileSync(filePath, 'utf8');
  const urls = new Set();
  const regexes = [
    /href="([^"]+)"/g,
    /href='([^']+)'/g,
  ];
  for (const regex of regexes) {
    for (const match of source.matchAll(regex)) {
      const raw = String(match[1] || '').trim();
      if (!raw || raw.startsWith('#') || raw.startsWith('mailto:') || raw.startsWith('tel:') || raw.startsWith('javascript:')) continue;
      if (raw.includes('${') || raw.includes("'+") || raw.includes('+\'') || raw.includes('"+') || raw.includes('+"')) continue;
      urls.add(raw);
    }
  }
  return [...urls];
}

function resolveRelativeLink(sourceFile, rawUrl) {
  if (/^https?:\/\//i.test(rawUrl)) return rawUrl;
  if (rawUrl.startsWith('/')) return joinUrl(PORTALE_BASE, rawUrl);

  const fileName = path.basename(sourceFile);
  const ownerSite = getAllPageOwners().get(fileName) || 'portale';
  const baseUrl = KNOWN_SITE_URLS[ownerSite] || PORTALE_BASE;
  return joinUrl(baseUrl, rawUrl);
}

async function auditClientClickableLinks() {
  const files = collectHtmlFiles(repoRoot).filter((filePath) => !filePath.includes(`${path.sep}extracted-repos${path.sep}`));
  const seen = new Map();

  for (const filePath of files) {
    for (const rawUrl of extractClickableUrls(filePath)) {
      const resolved = resolveRelativeLink(filePath, rawUrl);
      if (!resolved.startsWith('https://checkin.illupoaffitta.com')
        && !resolved.startsWith('https://operativita.illupoaffitta.com')
        && !resolved.startsWith('https://fatture.illupoaffitta.com')
        && !resolved.startsWith('https://contabilita.illupoaffitta.com')
        && !resolved.startsWith('https://calendario.illupoaffitta.com')
        && !resolved.startsWith('https://checkinillupoaffitta.netlify.app')) {
        continue;
      }
      if (!seen.has(resolved)) seen.set(resolved, []);
      seen.get(resolved).push(path.relative(repoRoot, filePath));
    }
  }

  const entries = [...seen.entries()];
  const results = await mapWithConcurrency(entries, async ([url, sources]) => {
    const response = await fetchStatus(url);
    return {
      url,
      status: response.status,
      ok: isAcceptablePageStatus(response.status),
      sources,
    };
  }, 12);
  return results.sort((a, b) => a.url.localeCompare(b.url));
}

async function auditCorePages() {
  const ownerMap = getAllPageOwners();
  const pages = [];
  for (const [route, ownerSiteKey] of ownerMap.entries()) {
    const baseUrl = KNOWN_SITE_URLS[ownerSiteKey] || PORTALE_BASE;
    pages.push(joinUrl(baseUrl, route));
  }
  pages.push(`${LEGACY_PORTALE_BASE}/residence-backoffice.html`);
  pages.push(`${LEGACY_PORTALE_BASE}/pr_kekko_riccione.html`);

  const uniquePages = [...new Set(pages)];
  const results = await mapWithConcurrency(uniquePages, async (url) => {
    const response = await fetchStatus(url);
    return { url, status: response.status, ok: isAcceptablePageStatus(response.status) };
  }, 10);
  return results.sort((a, b) => a.url.localeCompare(b.url));
}

async function main() {
  const [apartments, mappings, corePages, clickableLinks] = await Promise.all([
    loadActiveApartments(),
    loadAlloggiatiMappings(),
    auditCorePages(),
    auditClientClickableLinks(),
  ]);

  const apartmentResults = await mapWithConcurrency(apartments, auditApartmentLink, 6);

  const questuraReadiness = apartments.map((apartment) => {
    const link = mappings.get(apartment.id) || null;
    const account = link?.alloggiati_accounts || null;
    return {
      apartment_id: apartment.id,
      apartment_name: apartment.nome_appartamento,
      public_checkin_key: apartment.public_checkin_key,
      linked: !!link,
      account_active: account?.attivo === true,
      account_name: account?.nome_account || null,
      questura: account?.questura || null,
      invio_automatico: !!link?.invio_automatico,
      ultimo_invio_ok: account?.ultimo_invio_ok || null,
      ultimo_errore: account?.ultimo_errore || null,
    };
  });

  const report = {
    generated_at: new Date().toISOString(),
    summary: {
      active_apartments: apartments.length,
      apartment_links_ok: apartmentResults.filter((row) => row.checkin_page_ok && row.portal_page_ok && row.api_ok).length,
      apartment_submit_ok: apartmentResults.filter((row) => row.submit_ok).length,
      questura_linked_apartments: questuraReadiness.filter((row) => row.linked && row.account_active).length,
      questura_missing_apartments: questuraReadiness.filter((row) => !row.linked || !row.account_active).length,
      core_pages_ok: corePages.filter((row) => row.ok).length,
      core_pages_total: corePages.length,
      clickable_links_ok: clickableLinks.filter((row) => row.ok).length,
      clickable_links_total: clickableLinks.length,
    },
    apartment_results: apartmentResults,
    questura_readiness: questuraReadiness,
    core_pages: corePages,
    clickable_links: clickableLinks,
  };

  const reportDir = path.join(repoRoot, 'reports', 'audit');
  fs.mkdirSync(reportDir, { recursive: true });
  const reportPath = path.join(reportDir, `${new Date().toISOString().slice(0, 10)}-client-flows.json`);
  fs.writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');

  console.log(JSON.stringify({ reportPath, summary: report.summary }, null, 2));

  const failures = [];
  if (report.summary.apartment_links_ok !== apartments.length) failures.push('Some apartment links failed');
  if (report.summary.apartment_submit_ok !== apartments.length) failures.push('Some apartment submissions failed');
  if (report.summary.core_pages_ok !== report.summary.core_pages_total) failures.push('Some core pages failed');
  if (report.summary.clickable_links_ok !== report.summary.clickable_links_total) failures.push('Some clickable links failed');

  if (failures.length) {
    console.error(failures.join(' | '));
    process.exit(1);
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
