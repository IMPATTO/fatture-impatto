const fs = require('fs');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');

const CSV_PATH = process.argv[2] || '/Users/impattosrl/Downloads/kacn7gwda8.csv';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function normalizeString(value) {
  return String(value || '').trim();
}

function nullIfEmpty(value) {
  const normalized = normalizeString(value);
  return normalized ? normalized : null;
}

function normalizeToken(value) {
  return normalizeString(value)
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeBooleanSiNo(value) {
  const v = normalizeString(value).toLowerCase();
  if (!v) return null;
  if (['si', 'sì', 'yes', 'true', '1'].includes(v)) return true;
  if (['no', 'false', '0'].includes(v)) return false;
  return null;
}

function normalizeIdentifier(value) {
  const v = normalizeString(value).toUpperCase();
  if (!v || v === 'ESTERO') return null;
  return v.replace(/\s+/g, '');
}

function inferTipoCliente(row) {
  const piva = normalizeIdentifier(row['Partita IVA']);
  const cf = normalizeIdentifier(row['Codice Fiscale']);
  const privatoCsv = normalizeBooleanSiNo(row['Privato']);
  const ragioneSociale = normalizeString(row['Ragione sociale']);
  const normalizedName = normalizeToken(ragioneSociale);

  if (privatoCsv === true) return 'privato';
  if (piva && !cf) return 'azienda';
  if (piva && cf && piva !== cf) return 'professionista';

  const companyHints = ['srl', 'spa', 'sas', 'snc', 's a', 's.p.a', 's.r.l', 'ltd', 'gmbh', 'payments', 'logistics'];
  if (companyHints.some((hint) => normalizedName.includes(hint))) {
    return 'azienda';
  }

  return 'privato';
}

function buildNomeCognome(nomeVisualizzato, tipoCliente) {
  if (tipoCliente !== 'privato') {
    return { nome: null, cognome: null };
  }

  const parts = normalizeString(nomeVisualizzato).split(/\s+/).filter(Boolean);
  if (parts.length <= 1) {
    return { nome: parts[0] || null, cognome: null };
  }

  return {
    nome: parts.slice(0, -1).join(' ') || null,
    cognome: parts.slice(-1).join(' ') || null
  };
}

function detectDelimiter(headerLine) {
  const commaCount = (headerLine.match(/,/g) || []).length;
  const semicolonCount = (headerLine.match(/;/g) || []).length;
  return semicolonCount > commaCount ? ';' : ',';
}

function splitCsvLine(line, delimiter) {
  const out = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    const next = line[i + 1];

    if (ch === '"') {
      if (inQuotes && next === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (ch === delimiter && !inQuotes) {
      out.push(current);
      current = '';
      continue;
    }

    current += ch;
  }

  out.push(current);
  return out;
}

function parseCsv(raw) {
  const text = raw.replace(/^\uFEFF/, '');
  const lines = text.split(/\r?\n/).filter((line) => line.trim().length > 0);
  if (!lines.length) return [];

  const delimiter = detectDelimiter(lines[0]);
  const headers = splitCsvLine(lines.shift(), delimiter).map((h) => normalizeString(h));

  return lines.map((line) => {
    const cols = splitCsvLine(line, delimiter);
    const row = {};
    headers.forEach((header, index) => {
      row[header] = normalizeString(cols[index] || '');
    });
    return row;
  });
}

function buildClientePayload(row) {
  const nomeVisualizzato = normalizeString(row['Ragione sociale']);
  const localita = nullIfEmpty(row['Località']);
  const piva = normalizeIdentifier(row['Partita IVA']);
  const codiceFiscale = normalizeIdentifier(row['Codice Fiscale']);
  const email = nullIfEmpty(row['e-Mail']) ? normalizeString(row['e-Mail']).toLowerCase() : null;
  const privatoCsv = normalizeBooleanSiNo(row['Privato']);
  const tipoCliente = inferTipoCliente(row);
  const person = buildNomeCognome(nomeVisualizzato, tipoCliente);

  return {
    nome_visualizzato: nomeVisualizzato,
    tipo_cliente: tipoCliente,
    ragione_sociale: tipoCliente === 'azienda' ? nomeVisualizzato : null,
    nome: person.nome,
    cognome: person.cognome,
    localita,
    piva,
    codice_fiscale: codiceFiscale,
    email,
    privato_csv: privatoCsv,
    pec: null,
    codice_destinatario: null,
    indirizzo: null,
    cap: null,
    citta: null,
    provincia: null,
    paese: 'Italia',
    note: null,
    attivo: true
  };
}

function mergeCliente(existing, incoming) {
  const merged = { ...existing };

  Object.keys(incoming).forEach((field) => {
    const incomingValue = incoming[field];
    const existingValue = existing[field];

    if (incomingValue == null || incomingValue === '') return;
    if (existingValue == null || existingValue === '') {
      merged[field] = incomingValue;
      return;
    }
  });

  merged.nome_visualizzato = incoming.nome_visualizzato || existing.nome_visualizzato;
  merged.tipo_cliente = incoming.tipo_cliente || existing.tipo_cliente || 'privato';
  return merged;
}

async function findExistingCliente(payload) {
  if (payload.piva) {
    const { data, error } = await supabase
      .from('clienti')
      .select('*')
      .eq('piva', payload.piva)
      .limit(1)
      .maybeSingle();
    if (error) throw error;
    return data || null;
  }

  if (payload.codice_fiscale) {
    const { data, error } = await supabase
      .from('clienti')
      .select('*')
      .eq('codice_fiscale', payload.codice_fiscale)
      .limit(1)
      .maybeSingle();
    if (error) throw error;
    return data || null;
  }

  const normalizedName = normalizeToken(payload.nome_visualizzato);
  if (!normalizedName) return null;

  const { data, error } = await supabase
    .from('clienti')
    .select('*')
    .eq('attivo', true)
    .ilike('nome_visualizzato', payload.nome_visualizzato)
    .limit(5);

  if (error) throw error;

  return (data || []).find((item) => normalizeToken(item.nome_visualizzato) === normalizedName) || null;
}

async function saveCliente(payload) {
  const existing = await findExistingCliente(payload);

  if (existing) {
    const merged = mergeCliente(existing, payload);
    const { error } = await supabase
      .from('clienti')
      .update(merged)
      .eq('id', existing.id);
    if (error) throw error;
    return 'updated';
  }

  const { error } = await supabase
    .from('clienti')
    .insert(payload);
  if (error) throw error;
  return 'inserted';
}

async function main() {
  const raw = fs.readFileSync(path.resolve(CSV_PATH), 'utf8');
  const rows = parseCsv(raw);

  let inserted = 0;
  let updated = 0;
  let skipped = 0;

  for (const row of rows) {
    const payload = buildClientePayload(row);
    if (!payload.nome_visualizzato) {
      skipped += 1;
      continue;
    }

    const result = await saveCliente(payload);
    if (result === 'inserted') inserted += 1;
    if (result === 'updated') updated += 1;
  }

  console.log(JSON.stringify({
    csv_path: path.resolve(CSV_PATH),
    rows_total: rows.length,
    inserted,
    updated,
    skipped
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
