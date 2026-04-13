const { createClient } = require('@supabase/supabase-js');

const ALLOWED_ESITI = new Set([
  'OK',
  'PARZIALE',
  'ERRORE',
  'TEST_OK',
  'TEST_PARZIALE',
  'TEST_ERRORE',
  'SEND_OK',
  'SEND_PARZIALE',
  'SEND_ERRORE',
]);

exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: JSON.stringify({ error: 'Method not allowed' }) };
  }

  const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_ROLE_KEY
  );

  const authHeader = event.headers.authorization || event.headers.Authorization;
  if (!authHeader?.startsWith('Bearer ')) {
    return { statusCode: 401, body: JSON.stringify({ error: 'Unauthorized' }) };
  }

  const token = authHeader.replace('Bearer ', '');
  const { data: { user }, error: userErr } = await supabase.auth.getUser(token);
  if (userErr || !user) {
    return { statusCode: 401, body: JSON.stringify({ error: 'Unauthorized' }) };
  }

  let body;
  try {
    body = JSON.parse(event.body || '{}');
  } catch {
    return { statusCode: 400, body: JSON.stringify({ error: 'Invalid JSON' }) };
  }

  const {
    date_from,
    date_to,
    apartment_id = null,
    account_id = null,
    esito = null,
  } = body;

  if (!date_from || !/^\d{4}-\d{2}-\d{2}$/.test(date_from)) {
    return { statusCode: 400, body: JSON.stringify({ error: 'date_from richiesto nel formato YYYY-MM-DD' }) };
  }

  if (!date_to || !/^\d{4}-\d{2}-\d{2}$/.test(date_to)) {
    return { statusCode: 400, body: JSON.stringify({ error: 'date_to richiesto nel formato YYYY-MM-DD' }) };
  }

  if (date_to < date_from) {
    return { statusCode: 400, body: JSON.stringify({ error: 'date_to non può essere precedente a date_from' }) };
  }

  if (esito && !ALLOWED_ESITI.has(esito)) {
    return { statusCode: 400, body: JSON.stringify({ error: 'esito non valido' }) };
  }

  try {
    const rows = await fetchInvii(supabase, {
      date_from,
      date_to,
      apartment_id,
      account_id,
      esito,
    });

    const pdfBuffer = buildPdfReport(rows, {
      date_from,
      date_to,
      apartment_id,
      account_id,
      esito,
      generated_by: user.email || '',
    });

    const filename = `alloggiati-report-${date_from}_${date_to}.pdf`;

    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'application/pdf',
        'Content-Disposition': `attachment; filename="${filename}"`,
        'Cache-Control': 'no-store',
      },
      isBase64Encoded: true,
      body: pdfBuffer.toString('base64'),
    };
  } catch (err) {
    console.error('export-alloggiati-report error:', err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'Errore interno', detail: err.message }),
    };
  }
};

async function fetchInvii(supabase, filters) {
  const pageSize = 500;
  let from = 0;
  let allRows = [];

  while (true) {
    let query = supabase
      .from('alloggiati_invii')
      .select(`
        id,
        created_at,
        data_riferimento,
        apartment_id,
        alloggiati_account_id,
        num_schedine,
        esito,
        inviato_da,
        errore_dettaglio,
        apartments(nome_appartamento),
        alloggiati_accounts(nome_account)
      `)
      .gte('created_at', toRomeDayStart(filters.date_from))
      .lt('created_at', toRomeNextDayStart(filters.date_to))
      .order('created_at', { ascending: false })
      .range(from, from + pageSize - 1);

    if (filters.apartment_id) {
      query = query.eq('apartment_id', filters.apartment_id);
    }
    if (filters.account_id) {
      query = query.eq('alloggiati_account_id', filters.account_id);
    }
    if (filters.esito) {
      query = query.eq('esito', filters.esito);
    }

    const { data, error } = await query;
    if (error) throw error;

    const batch = data || [];
    allRows = allRows.concat(batch);

    if (batch.length < pageSize) break;
    from += pageSize;
  }

  return allRows;
}

function buildPdfReport(rows, filters) {
  const pageWidth = 841.89;
  const pageHeight = 595.28;
  const margin = 36;
  const lineGap = 14;

  const objects = {};
  const fontId = 1;
  const pagesId = 2;
  const catalogId = 3;
  let nextId = 4;

  objects[fontId] = '<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>';

  const pageEntries = [];
  let currentCommands = [];
  let y = pageHeight - margin;
  let pageNo = 0;

  const drawText = (text, x, posY, size) => {
    currentCommands.push('BT');
    currentCommands.push(`/F1 ${size} Tf`);
    currentCommands.push(`1 0 0 1 ${x} ${posY} Tm`);
    currentCommands.push(`(${pdfEscape(text)}) Tj`);
    currentCommands.push('ET');
  };

  const startPage = () => {
    pageNo += 1;
    currentCommands = [];
    y = pageHeight - margin;

    drawText('Storico invii AlloggiatiWeb', margin, y, 16);
    y -= 22;
    drawText(`Generato il ${formatDateTime(new Date().toISOString())} da ${filters.generated_by || 'utente'}`, margin, y, 10);
    y -= 16;
    drawText(`Periodo created_at: ${filters.date_from} -> ${filters.date_to}`, margin, y, 10);
    y -= 14;
    drawText(`Filtri: appartamento=${filters.apartment_id || 'tutti'} | account=${filters.account_id || 'tutti'} | esito=${filters.esito || 'tutti'}`, margin, y, 10);
    y -= 18;
    drawText(`Pagina ${pageNo}`, pageWidth - 100, pageHeight - margin + 2, 9);
    y -= 4;
  };

  const finishPage = () => {
    const stream = currentCommands.join('\n');
    const contentId = nextId++;
    const pageId = nextId++;

    objects[contentId] = `<< /Length ${Buffer.byteLength(stream, 'utf8')} >>\nstream\n${stream}\nendstream`;
    objects[pageId] = `<< /Type /Page /Parent ${pagesId} 0 R /MediaBox [0 0 ${pageWidth} ${pageHeight}] /Resources << /Font << /F1 ${fontId} 0 R >> >> /Contents ${contentId} 0 R >>`;
    pageEntries.push(pageId);
  };

  const ensureSpace = (neededLines) => {
    if (y - (neededLines * lineGap) < margin) {
      finishPage();
      startPage();
    }
  };

  const addWrappedLine = (text, size, maxChars, indent) => {
    const lines = wrapText(text, maxChars);
    ensureSpace(lines.length);
    lines.forEach(line => {
      drawText(line, margin + indent, y, size);
      y -= lineGap;
    });
  };

  const legacyRows = rows.filter(row =>
    row.esito === 'OK' || row.esito === 'PARZIALE' || row.esito === 'ERRORE'
  );

  const validRows = rows.filter(row => row.esito === 'SEND_OK');

  const invalidRows = rows.filter(row =>
    !legacyRows.includes(row) && row.esito !== 'SEND_OK'
  );

  const renderRowBlock = (row, idx) => {
    const created = formatDateTime(row.created_at);
    const riferimento = formatDate(row.data_riferimento);
    const appartamento = row.apartments?.nome_appartamento || row.apartment_id || '—';
    const account = row.alloggiati_accounts?.nome_account || row.alloggiati_account_id || '—';
    const inviatoDa = row.inviato_da || '—';
    const errore = compactText(row.errore_dettaglio || '');

    const tipoInvio =
      row.esito?.startsWith('TEST') ? 'TEST'
      : row.esito?.startsWith('SEND') ? 'INVIO REALE'
      : 'STORICO LEGACY';

    const statoLegale =
      row.esito === 'SEND_OK' ? 'VALIDO'
      : row.esito === 'OK' ? 'VALIDO (LEGACY)'
      : 'NON VALIDO';

    const erroreLines = errore ? Math.max(1, wrapText(`Errore: ${errore}`, 108).length) : 0;
    const blockLines = 8 + erroreLines;
    ensureSpace(blockLines + 1);

    addWrappedLine(`${idx + 1}. Invio: ${created} | Rif.: ${riferimento} | Esito: ${row.esito || '—'} | Schedine: ${row.num_schedine ?? '—'}`, 10, 108, 0);
    addWrappedLine(`ID Invio: ${row.id || '—'}`, 10, 108, 0);
    addWrappedLine(`Tipo invio: ${tipoInvio}`, 10, 108, 0);
    addWrappedLine(`Stato legale: ${statoLegale}`, 10, 108, 0);
    addWrappedLine(`Appartamento: ${appartamento}`, 10, 108, 0);
    addWrappedLine(`Account: ${account}`, 10, 108, 0);
    addWrappedLine(`Inviato da: ${inviatoDa}`, 10, 108, 0);

    if (errore) {
      addWrappedLine(`Errore: ${errore}`, 10, 108, 0);
    }

    addWrappedLine('--------------------------------------------------------------------------------', 9, 120, 0);
  };

  startPage();

  if (!rows.length) {
    addWrappedLine('Nessun invio trovato con i filtri selezionati.', 11, 90, 0);
  } else {
    addWrappedLine('=== INVII VALIDI ALLA QUESTURA ===', 11, 90, 0);

    if (!validRows.length) {
      addWrappedLine('Nessun invio valido trovato.', 10, 90, 0);
    } else {
      validRows.forEach((row, idx) => renderRowBlock(row, idx));
    }

    ensureSpace(3);
    y -= 6;
    addWrappedLine('=== LOG TECNICO / INVII NON VALIDI ===', 11, 90, 0);

    if (!invalidRows.length) {
      addWrappedLine('Nessun log tecnico da mostrare.', 10, 90, 0);
    } else {
      invalidRows.forEach((row, idx) => renderRowBlock(row, idx));
    }

    ensureSpace(4);
    y -= 6;
    addWrappedLine('=== STORICO LEGACY ===', 11, 90, 0);
    addWrappedLine('Questi record precedono la distinzione TEST/SEND e vanno interpretati con cautela.', 10, 108, 0);

    if (!legacyRows.length) {
      addWrappedLine('Nessun record legacy da mostrare.', 10, 90, 0);
    } else {
      legacyRows.forEach((row, idx) => renderRowBlock(row, idx));
    }
  }

  finishPage();

  objects[pagesId] = `<< /Type /Pages /Kids [${pageEntries.map(id => `${id} 0 R`).join(' ')}] /Count ${pageEntries.length} >>`;
  objects[catalogId] = `<< /Type /Catalog /Pages ${pagesId} 0 R >>`;

  const maxId = Math.max(...Object.keys(objects).map(Number));
  let pdf = '%PDF-1.4\n';
  const offsets = [0];

  for (let id = 1; id <= maxId; id += 1) {
    offsets[id] = Buffer.byteLength(pdf, 'utf8');
    pdf += `${id} 0 obj\n${objects[id]}\nendobj\n`;
  }

  const xrefPos = Buffer.byteLength(pdf, 'utf8');
  pdf += `xref\n0 ${maxId + 1}\n`;
  pdf += '0000000000 65535 f \n';
  for (let id = 1; id <= maxId; id += 1) {
    pdf += `${String(offsets[id]).padStart(10, '0')} 00000 n \n`;
  }

  pdf += `trailer\n<< /Size ${maxId + 1} /Root ${catalogId} 0 R >>\nstartxref\n${xrefPos}\n%%EOF`;
  return Buffer.from(pdf, 'utf8');
}

function wrapText(text, maxChars) {
  const clean = compactText(text);
  if (!clean) return [''];
  const words = clean.split(' ');
  const lines = [];
  let current = '';

  for (const word of words) {
    const next = current ? `${current} ${word}` : word;
    if (next.length <= maxChars) {
      current = next;
    } else {
      if (current) lines.push(current);
      current = word;
    }
  }
  if (current) lines.push(current);
  return lines;
}

function compactText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function pdfEscape(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^\x20-\x7E]/g, '?')
    .replace(/\\/g, '\\\\')
    .replace(/\(/g, '\\(')
    .replace(/\)/g, '\\)');
}

function formatDate(value) {
  if (!value) return '—';
  try {
    return new Intl.DateTimeFormat('it-IT', {
      timeZone: 'Europe/Rome',
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
    }).format(new Date(`${value}T12:00:00Z`));
  } catch {
    return String(value);
  }
}

function formatDateTime(value) {
  if (!value) return '—';
  try {
    return new Intl.DateTimeFormat('it-IT', {
      timeZone: 'Europe/Rome',
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    }).format(new Date(value));
  } catch {
    return String(value);
  }
}

function nextDate(dateStr) {
  const d = new Date(`${dateStr}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + 1);
  return d.toISOString().slice(0, 10);
}

function getRomeOffset(dateStr) {
  const sample = new Date(`${dateStr}T12:00:00Z`);
  const tzName = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Europe/Rome',
    timeZoneName: 'shortOffset',
  }).formatToParts(sample).find(p => p.type === 'timeZoneName')?.value || 'GMT+1';

  const match = tzName.match(/GMT([+-]\d{1,2})(?::?(\d{2}))?/);
  if (!match) return '+01:00';

  const sign = match[1][0];
  const hours = String(Math.abs(Number(match[1]))).padStart(2, '0');
  const minutes = match[2] || '00';
  return `${sign}${hours}:${minutes}`;
}

function toRomeDayStart(dateStr) {
  return `${dateStr}T00:00:00${getRomeOffset(dateStr)}`;
}

function toRomeNextDayStart(dateStr) {
  const next = nextDate(dateStr);
  return `${next}T00:00:00${getRomeOffset(next)}`;
}
