const fs = require('fs');
const path = require('path');

const EMPTY_INDEX_CACHE_TTL_MS = 60 * 1000;
const COMUNI_CSV_CANDIDATES = [
  path.resolve(__dirname, 'comuni.csv'),
  path.resolve(process.cwd(), 'comuni.csv'),
  path.resolve(__dirname, '../../comuni.csv'),
  path.resolve(__dirname, '../comuni.csv'),
];

let currentComuniIndex = null;
let currentComuniIndexPromise = null;
let currentComuniIndexExpiresAt = 0;

function parseCsvLine(line) {
  const out = [];
  let current = '';
  let insideQuotes = false;

  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    if (char === '"') {
      if (insideQuotes && line[index + 1] === '"') {
        current += '"';
        index += 1;
        continue;
      }
      insideQuotes = !insideQuotes;
      continue;
    }
    if (char === ',' && !insideQuotes) {
      out.push(current);
      current = '';
      continue;
    }
    current += char;
  }

  out.push(current);
  return out;
}

function normalizeComuneName(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[’`´'-]/g, '')
    .replace(/\s+/g, '')
    .trim()
    .toLowerCase();
}

function normalizeProvinceCode(value) {
  return String(value || '')
    .trim()
    .toUpperCase()
    .replace(/[^A-Z]/g, '')
    .slice(0, 2);
}

function buildCurrentComuniIndexFromCsv(csv) {
  const byName = new Map();
  const rows = String(csv || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  if (rows.length < 2) {
    return byName;
  }

  const header = parseCsvLine(rows[0]).map((value) => String(value || '').trim().toLowerCase());
  const codiceIndex = header.indexOf('codice');
  const nomeIndex = header.indexOf('descrizione') !== -1 ? header.indexOf('descrizione') : header.indexOf('nome');
  const provinciaIndex = header.indexOf('provincia');
  const dataFineIndex = header.indexOf('datafineval');

  rows.slice(1).forEach((line) => {
    const cols = parseCsvLine(line);
    const codice = String(cols[codiceIndex] || '').trim();
    const nome = String(cols[nomeIndex] || '').trim();
    const provincia = normalizeProvinceCode(cols[provinciaIndex]);
    const dataFineVal = String(cols[dataFineIndex] || '').trim();

    if (!codice || !nome || !provincia || dataFineVal) return;

    const normalizedName = normalizeComuneName(nome);
    if (!normalizedName) return;

    const currentRows = byName.get(normalizedName) || [];
    currentRows.push({ codice, nome, provincia });
    byName.set(normalizedName, currentRows);
  });

  return byName;
}

function loadCurrentComuniCsvFromFile() {
  for (const candidate of COMUNI_CSV_CANDIDATES) {
    try {
      if (fs.existsSync(candidate)) {
        return fs.readFileSync(candidate, 'utf8');
      }
    } catch (_error) {
      // Continue through fallback candidates.
    }
  }
  return '';
}

function resolveComuniCsvBaseUrl(options = {}) {
  const candidates = [
    options.baseUrl,
    process.env.PUBLIC_PORTAL_BASE_URL,
    process.env.SITE_URL_PORTALE,
    process.env.URL,
    process.env.DEPLOY_PRIME_URL,
  ];

  for (const candidate of candidates) {
    const normalized = String(candidate || '').trim();
    if (normalized) return normalized.replace(/\/+$/, '');
  }

  return '';
}

async function loadCurrentComuniCsvFromUrl(options = {}) {
  const baseUrl = resolveComuniCsvBaseUrl(options);
  if (!baseUrl) return '';

  try {
    const response = await fetch(`${baseUrl}/comuni.csv`);
    if (!response.ok) {
      console.warn(`current-comuni-index unable to load comuni.csv: HTTP ${response.status}`);
      return '';
    }
    return response.text();
  } catch (error) {
    console.warn('current-comuni-index unable to load comuni.csv:', error.message);
    return '';
  }
}

async function buildCurrentComuniIndex(options = {}) {
  let csv = loadCurrentComuniCsvFromFile();
  if (!csv) {
    csv = await loadCurrentComuniCsvFromUrl(options);
  }
  return buildCurrentComuniIndexFromCsv(csv);
}

async function loadCurrentComuniIndex(options = {}) {
  const now = Date.now();
  if (currentComuniIndex && (currentComuniIndex.size || now < currentComuniIndexExpiresAt)) {
    return currentComuniIndex;
  }
  if (currentComuniIndexPromise) return currentComuniIndexPromise;

  currentComuniIndexPromise = buildCurrentComuniIndex(options)
    .then((index) => {
      currentComuniIndex = index;
      currentComuniIndexExpiresAt = index.size
        ? Number.POSITIVE_INFINITY
        : Date.now() + EMPTY_INDEX_CACHE_TTL_MS;
      currentComuniIndexPromise = null;
      return currentComuniIndex;
    })
    .catch((error) => {
      currentComuniIndexPromise = null;
      throw error;
    });

  return currentComuniIndexPromise;
}

async function getCurrentComuniForName(normalizedName, options = {}) {
  const lookupKey = normalizeComuneName(normalizedName);
  if (!lookupKey) return [];
  const index = await loadCurrentComuniIndex(options);
  return index.get(lookupKey) || [];
}

async function preferCurrentComuneRows(rows, normalizedName, options = {}) {
  const normalize = typeof options.normalize === 'function'
    ? options.normalize
    : normalizeComuneName;

  const exactRows = (Array.isArray(rows) ? rows : []).filter((row) => (
    normalize(row?.nome) === normalizedName
  ));

  if (exactRows.length <= 1) return exactRows;

  const currentRows = await getCurrentComuniForName(normalizedName, options);
  if (currentRows.length !== 1) return exactRows;

  const current = currentRows[0];
  const currentCode = String(current.codice || '').trim();
  const currentProvince = normalizeProvinceCode(current.provincia);

  if (currentCode) {
    const byCode = exactRows.filter((row) => String(row?.codice || '').trim() === currentCode);
    if (byCode.length === 1) return byCode;
  }

  if (currentProvince) {
    const byProvince = exactRows.filter((row) => normalizeProvinceCode(row?.provincia) === currentProvince);
    if (byProvince.length === 1) return byProvince;
  }

  return exactRows;
}

module.exports = {
  getCurrentComuniForName,
  normalizeComuneName,
  preferCurrentComuneRows,
};
