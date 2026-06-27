const fs = require('fs');
const path = require('path');

const COMUNI_CSV_PATH = path.resolve(__dirname, '../../comuni.csv');

let currentComuniIndex = null;

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

function loadCurrentComuniIndex() {
  if (currentComuniIndex) return currentComuniIndex;

  const byName = new Map();
  let csv = '';
  try {
    csv = fs.readFileSync(COMUNI_CSV_PATH, 'utf8');
  } catch (_error) {
    currentComuniIndex = byName;
    return currentComuniIndex;
  }

  const rows = String(csv || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  if (rows.length < 2) {
    currentComuniIndex = byName;
    return currentComuniIndex;
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

  currentComuniIndex = byName;
  return currentComuniIndex;
}

function getCurrentComuniForName(normalizedName) {
  const lookupKey = normalizeComuneName(normalizedName);
  if (!lookupKey) return [];
  return loadCurrentComuniIndex().get(lookupKey) || [];
}

function preferCurrentComuneRows(rows, normalizedName, options = {}) {
  const normalize = typeof options.normalize === 'function'
    ? options.normalize
    : normalizeComuneName;

  const exactRows = (Array.isArray(rows) ? rows : []).filter((row) => (
    normalize(row?.nome) === normalizedName
  ));

  if (exactRows.length <= 1) return exactRows;

  const currentRows = getCurrentComuniForName(normalizedName);
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
