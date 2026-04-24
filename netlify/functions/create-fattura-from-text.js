const {
  authenticateRequest,
  buildClientEntity,
  buildFicPayload,
  buildInvoiceItems,
  computeInvoiceTotals,
  createDraftOnFic,
  createSupabaseAdmin,
  detectOptionalColumns,
  insertAuditLog,
  jsonResponse,
  normalizeSearchToken,
  normalizeString,
  parseEventJson,
  resolvePaymentAccountId,
  resolvePaymentAccountType,
  resolvePaymentStatus,
  resolveSezionale,
  saveFatturaStaging,
  toNumber
} = require('./_lib/fatture-fic');

const DEFAULT_DESCRIPTION = 'Prestazione';
const CLIENT_TYPES = new Set(['privato', 'azienda', 'professionista']);
const VAT_OPTIONS = new Set([0, 4, 5, 10, 22]);
const COMPANY_HINTS = ['srl', 'spa', 'sas', 'snc', 's a', 's.p.a', 's.r.l', 'ltd', 'gmbh', 'payments', 'logistics'];

exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') {
    return jsonResponse(405, { error: 'Method not allowed' });
  }

  const parsed = parseEventJson(event);
  if (parsed.errorResponse) return parsed.errorResponse;
  const body = parsed.body;

  const supabase = createSupabaseAdmin();
  const auth = await authenticateRequest(event, supabase);
  if (auth.errorResponse) return auth.errorResponse;
  const { user } = auth;

  const action = normalizeString(body.action || 'analyze').toLowerCase();
  if (!['analyze', 'create'].includes(action)) {
    return jsonResponse(400, { error: 'action non supportata', allowed: ['analyze', 'create'] });
  }

  const rawText = normalizeString(body.raw_text);
  if (!rawText) {
    return jsonResponse(400, { error: 'raw_text richiesto' });
  }

  const parseResult = parseFreeTextInvoice(rawText, body.parsed || {});
  const searchResult = await searchClienti(supabase, parseResult.cliente_nome_ricerca);
  if (searchResult.errorResponse) return searchResult.errorResponse;

  const selectedClientId = normalizeString(body.selected_cliente_id || body.cliente_id);
  const selectedClient = selectedClientId
    ? (searchResult.allCandidates.find((item) => String(item.id) === selectedClientId) || await loadClienteById(supabase, selectedClientId))
    : null;

  const matchedClient = selectedClient || searchResult.exactMatch || null;
  const clienteFormInput = body.cliente && typeof body.cliente === 'object'
    ? normalizeClienteInput(body.cliente)
    : null;
  const baselineCliente = matchedClient ? clienteRecordToForm(matchedClient) : defaultClienteForm(parseResult.cliente_nome_ricerca);
  const suggestedClienteForm = buildSuggestedClienteForm({
    parseResult,
    matchedClient,
    clienteFormInput
  });
  const preview = computeInvoiceTotals(parseResult.importo_lordo, parseResult.iva_percentuale);
  const clienteValidation = validateClientePayload(suggestedClienteForm);
  const requiresClientCompletion = !clienteValidation.ok;
  const requiresConfirmation = parseResult.warnings.length > 0 || requiresClientCompletion || !parseResult.can_auto_create;

  if (action === 'analyze') {
    return jsonResponse(200, {
      success: true,
      action,
      parsing: parseResult,
      preview: preview.ok
        ? {
            imponibile: preview.imponibile,
            bollo: preview.bollo,
            totale_documento: preview.importoTotaleDocumento,
            bollo_applicato: preview.bolloApplicato
          }
        : null,
      customer_lookup: {
        exact_match: searchResult.exactMatch ? slimCliente(searchResult.exactMatch) : null,
        selected_match: matchedClient ? slimCliente(matchedClient) : null,
        possible_matches: searchResult.matches.map(slimCliente)
      },
      cliente_form: suggestedClienteForm,
      cliente_form_baseline: baselineCliente,
      requires_client_completion: requiresClientCompletion,
      requires_confirmation: requiresConfirmation
    });
  }

  if (!preview.ok) {
    return jsonResponse(400, {
      error: preview.error,
      allowed: preview.allowed || undefined,
      parsing: parseResult
    });
  }

  if (parseResult.warnings.length > 0) {
    return jsonResponse(400, {
      error: 'Il testo e ambiguo o incompleto: correggi i campi estratti prima di creare la bozza.',
      parsing: parseResult
    });
  }

  const overwriteFields = Array.isArray(body.overwrite_fields)
    ? body.overwrite_fields.map((field) => normalizeString(field)).filter(Boolean)
    : [];
  const clientPayload = normalizeClienteInput(body.cliente || suggestedClienteForm);
  const existingClient = matchedClient || null;
  const mergedClientPayload = mergeClienteData(existingClient, clientPayload, overwriteFields);
  const clienteUpdatedFields = getClienteUpdatedFields(existingClient, mergedClientPayload);
  const finalClientValidation = validateClientePayload(mergedClientPayload);
  if (!finalClientValidation.ok) {
    return jsonResponse(400, {
      error: 'Dati cliente incompleti',
      fields: finalClientValidation.fields
    });
  }

  const upsertedClient = await upsertCliente(supabase, existingClient, mergedClientPayload);
  if (upsertedClient.errorResponse) return upsertedClient.errorResponse;
  const clienteRecord = upsertedClient.data;

  const paymentStatus = resolvePaymentStatus(body.payment_status);
  const paymentAccountType = resolvePaymentAccountType(body.payment_account_type);
  const paymentAccount = resolvePaymentAccountId(paymentAccountType);
  if (!paymentAccount.ok) {
    return jsonResponse(500, {
      error: paymentAccount.error,
      detail: paymentAccount.detail
    });
  }

  const {
    requestedSezionale,
    sezionale,
    sezionaleSource,
    sezionaleWarning
  } = resolveSezionale(body.sezionale);
  const today = new Date().toISOString().split('T')[0];

  const ficPayload = buildFicPayload({
    entity: buildClientEntity(clienteRecord, { tipoCliente: clienteRecord.tipo_cliente }),
    date: today,
    itemsList: buildInvoiceItems({
      description: parseResult.descrizione || DEFAULT_DESCRIPTION,
      itemDescription: body.item_description || null,
      importoTotale: preview.importoTotale,
      vatId: preview.vatId,
      bolloApplicato: preview.bolloApplicato
    }),
    sezionale,
    paymentStatus,
    paymentAccountId: paymentAccount.paymentAccountId,
    paymentAccountType,
    importoTotaleDocumento: preview.importoTotaleDocumento,
    imponibileDocumento: preview.imponibileDocumento
  });

  const ficResult = await createDraftOnFic(ficPayload);
  if (!ficResult.ok) {
    return jsonResponse(ficResult.statusCode, {
      error: ficResult.error,
      detail: ficResult.detail,
      raw_response: ficResult.raw_response,
      parsing: parseResult
    });
  }

  const columnSupport = await detectOptionalColumns(supabase);
  const localSyncDetails = [];
  const stagingPayload = {
    ospiti_check_in_id: null,
    numero_fattura: ficResult.ficNumero ? String(ficResult.ficNumero) : null,
    data_fattura: today,
    importo_lordo: preview.importoTotale,
    iva_percentuale: preview.ivaPercentuale,
    importo_totale_con_iva: preview.importoTotaleDocumento,
    nome_cliente: clienteRecord.nome || clienteRecord.nome_visualizzato || clienteRecord.ragione_sociale || null,
    cognome_cliente: clienteRecord.cognome || null,
    stato: 'BOZZA_CREATA',
    link_fatture_cloud: ficResult.ficDocUrl
  };
  if (columnSupport.fatture_staging_payment_status) {
    stagingPayload.payment_status = paymentStatus;
  }
  if (columnSupport.fatture_staging_fic_document_id) {
    stagingPayload.fic_document_id = ficResult.ficDocId;
  }
  if (columnSupport.fatture_staging_sezionale) {
    stagingPayload.sezionale = sezionale;
  }
  if (columnSupport.fatture_staging_cliente_id) {
    stagingPayload.cliente_id = clienteRecord.id;
  }
  if (columnSupport.fatture_staging_input_testuale_originale) {
    stagingPayload.input_testuale_originale = rawText;
  }
  if (columnSupport.fatture_staging_parsing_payload) {
    stagingPayload.parsing_payload = {
      ...parseResult,
      customer_lookup: {
        exact_match_id: searchResult.exactMatch?.id || null,
        selected_match_id: matchedClient?.id || null
      }
    };
  }

  let staging = null;
  const stagingResult = await saveFatturaStaging(supabase, stagingPayload);
  if (stagingResult.error) {
    console.error('Errore insert fatture_staging from text:', stagingResult.error);
    localSyncDetails.push({
      step: 'fatture_staging_insert',
      message: stagingResult.error.message,
      detail: {
        code: stagingResult.error.code || null,
        details: stagingResult.error.details || null,
        hint: stagingResult.error.hint || null
      }
    });
  } else {
    staging = stagingResult.data;
  }

  const { error: auditError } = await insertAuditLog(supabase, {
    userEmail: user.email,
    action: 'CREATE_FATTURA_FIC_FROM_TEXT',
    tableName: 'fatture_staging',
    recordId: staging?.id ?? String(ficResult.ficDocId || clienteRecord.id)
  });

  if (auditError) {
    console.error('Errore audit_log post FiC from text:', auditError);
    localSyncDetails.push({
      step: 'audit_log_insert',
      message: auditError.message,
      detail: {
        code: auditError.code || null,
        details: auditError.details || null,
        hint: auditError.hint || null
      }
    });
  }

  return jsonResponse(200, {
    success: true,
    action,
    created_on_fic: true,
    cliente: slimCliente(clienteRecord),
    cliente_action: existingClient ? 'updated' : 'created',
    cliente_updated_fields: clienteUpdatedFields,
    parsing: parseResult,
    fic_document_id: ficResult.ficDocId,
    fic_url: ficResult.ficDocUrl,
    numero_fattura: ficResult.ficNumero,
    fic_numeration: ficResult.ficReturnedNumeration,
    fattura_staging_id: staging?.id ?? null,
    sezionale,
    requested_sezionale: requestedSezionale,
    sezionale_source: sezionaleSource,
    sezionale_warning: sezionaleWarning,
    bollo_applicato: preview.bolloApplicato,
    payment_status: paymentStatus,
    payment_account_type: paymentAccountType,
    importo_lordo: preview.importoTotale,
    importo_totale_documento: preview.importoTotaleDocumento,
    imponibile_calcolato: preview.imponibileDocumento,
    local_sync_warning: localSyncDetails.length > 0,
    local_sync_details: localSyncDetails
  });
};

function parseFreeTextInvoice(rawText, overrides = {}) {
  const raw = normalizeString(rawText);
  const normalized = normalizeInvoiceText(raw);
  const warnings = [];

  let clienteNome = normalizeString(overrides.cliente_nome_ricerca);
  if (!clienteNome) {
    clienteNome = extractClienteName(raw, normalized);
  }
  if (!clienteNome) {
    warnings.push('Cliente non riconosciuto dal testo.');
  }

  let importoLordo = toNumber(overrides.importo_lordo);
  if (importoLordo == null) {
    importoLordo = extractImporto(raw, normalized);
  }
  if (!Number.isFinite(importoLordo) || importoLordo <= 0) {
    warnings.push('Importo non trovato o non valido.');
  }

  let ivaPercentuale = Number.isFinite(Number(overrides.iva_percentuale))
    ? Number(overrides.iva_percentuale)
    : extractIva(normalized);
  if (!VAT_OPTIONS.has(ivaPercentuale)) {
    warnings.push(`IVA ${ivaPercentuale} non supportata.`);
  }

  const descrizione = normalizeString(overrides.descrizione) || extractDescrizione(raw) || DEFAULT_DESCRIPTION;
  const confidence = computeConfidence({ clienteNome, importoLordo, ivaPercentuale, warnings });

  return {
    raw_text: raw,
    cliente_nome_ricerca: clienteNome,
    importo_lordo: Number.isFinite(importoLordo) ? Number(importoLordo.toFixed(2)) : null,
    iva_percentuale: VAT_OPTIONS.has(ivaPercentuale) ? ivaPercentuale : null,
    descrizione,
    confidence,
    warnings,
    can_auto_create: warnings.length === 0
  };
}

function normalizeInvoiceText(text) {
  return normalizeString(text)
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[€]/g, ' euro ')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function extractClienteName(rawText, normalizedText) {
  const patterns = [
    /\b(?:fattura|fai una fattura|fai fattura|crea fattura|nuova fattura)\s+(?:a|per)\s+(.+?)(?=\s+(?:di|da)\s+(?:euro\s+)?\d|\s+\d+(?:[.,]\d{1,2})?\s*euro|\s+con\s+iva|\s+iva\b|$)/i,
    /\b(?:a|per)\s+(.+?)(?=\s+(?:di|da)\s+(?:euro\s+)?\d|\s+\d+(?:[.,]\d{1,2})?\s*euro|\s+con\s+iva|\s+iva\b|$)/i
  ];

  for (const pattern of patterns) {
    const match = rawText.match(pattern) || normalizedText.match(pattern);
    if (match?.[1]) {
      return cleanupClienteName(match[1]);
    }
  }

  return '';
}

function cleanupClienteName(value) {
  return normalizeString(value)
    .replace(/[.;,:-]\s*(?:numero\s+fattura|fattura\s+n(?:umero|r)?\.?|nr\.?|n\.)\s*.*$/i, '')
    .replace(/[.;,:-]\s*sezionale\s+.*$/i, '')
    .replace(/[.;,:-]\s*importo\s+.*$/i, '')
    .replace(/[.;,:-]\s*(?:e|ed|e un|è un|e una|rimborso)\b.*$/i, '')
    .replace(/\b(?:numero\s+fattura|fattura\s+n(?:umero|r)?\.?|nr\.?|n\.)\s*.*$/i, '')
    .replace(/\bsezionale\s+.*$/i, '')
    .replace(/\bimporto\s+.*$/i, '')
    .replace(/\b(?:e un|è un|e una|rimborso)\b.*$/i, '')
    .replace(/\bcon\s+iva.*$/i, '')
    .replace(/\biva\s+\d+.*$/i, '')
    .replace(/\bdi\s+euro.*$/i, '')
    .replace(/\b(?:di|da)\s+\d.*$/i, '')
    .replace(/[.;,:-]+\s*$/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractImporto(rawText, normalizedText) {
  const patterns = [
    /\bimporto(?:\s+lordo)?(?:\s+di)?[:\s]+(?:euro\s+)?(\d+(?:[.,]\d{1,2})?)/i,
    /\b(?:di|da)\s+(?:euro\s+)?(\d+(?:[.,]\d{1,2})?)/i,
    /\b(\d+(?:[.,]\d{1,2})?)\s*euro\b/i,
    /\beuro\s+(\d+(?:[.,]\d{1,2})?)\b/i
  ];

  for (const pattern of patterns) {
    const match = rawText.match(pattern) || normalizedText.match(pattern);
    if (match?.[1]) {
      const amount = toNumber(match[1]);
      if (amount != null) return amount;
    }
  }

  return null;
}

function extractIva(normalizedText) {
  if (
    /\b(?:iva\s+esente|esente\s+iva|operazione\s+esente|esente|fuori\s+campo\s+iva|iva\s+zero)\b/i.test(normalizedText)
  ) {
    return 0;
  }
  const match = normalizedText.match(/\biva(?:\s+al)?\s+(\d{1,2})(?:\s*%|\b)/i);
  if (match?.[1]) return Number(match[1]);
  return 22;
}

function extractDescrizione(rawText) {
  const normalized = normalizeInvoiceText(rawText);
  if (/\brimborso\s+spese\s+esente\b/i.test(normalized)) return 'Rimborso spese esente';
  if (/\brimborso\s+spese\b/i.test(normalized)) return 'Rimborso spese';
  if (/\besente\b/i.test(normalized)) return 'Operazione esente';
  const patterns = [
    /\bdescrizione[:\s]+(.+)$/i,
    /\bcausale[:\s]+(.+)$/i,
    /\bservizio[:\s]+(.+)$/i
  ];

  for (const pattern of patterns) {
    const match = rawText.match(pattern);
    if (match?.[1]) return normalizeString(match[1]);
  }

  return '';
}

function computeConfidence({ clienteNome, importoLordo, ivaPercentuale, warnings }) {
  let score = 0.2;
  if (clienteNome) score += 0.3;
  if (Number.isFinite(importoLordo) && importoLordo > 0) score += 0.3;
  if (VAT_OPTIONS.has(ivaPercentuale)) score += 0.2;
  score -= warnings.length * 0.15;
  return Number(Math.max(0, Math.min(1, score)).toFixed(2));
}

async function searchClienti(supabase, clienteNomeRicerca) {
  const searchToken = normalizeSearchToken(clienteNomeRicerca);
  if (!searchToken) {
    return {
      exactMatch: null,
      matches: [],
      allCandidates: []
    };
  }

  const escapedQuery = escapeLike(searchToken).replace(/\s+/g, '%');
  const { data, error } = await supabase
    .from('clienti')
    .select('*')
    .eq('attivo', true)
    .or(`search_text.ilike.%${escapedQuery}%,nome_visualizzato.ilike.%${escapedQuery}%,ragione_sociale.ilike.%${escapedQuery}%`)
    .limit(20);

  if (error) {
    if (/clienti/i.test(String(error.message || ''))) {
      return {
        errorResponse: jsonResponse(500, {
          error: 'Tabella clienti non disponibile. Applica prima la migration Supabase della nuova feature.'
        })
      };
    }

    return {
      errorResponse: jsonResponse(500, {
        error: 'Errore ricerca clienti',
        detail: error.message
      })
    };
  }

  const ranked = (data || [])
    .map((cliente) => ({ cliente, score: rankClienteMatch(cliente, searchToken) }))
    .filter((item) => item.score < 999)
    .sort((a, b) => a.score - b.score || String(a.cliente.nome_visualizzato || '').localeCompare(String(b.cliente.nome_visualizzato || '')));

  const allCandidates = ranked.map((item) => item.cliente);
  const exactCandidates = ranked.filter((item) => item.score <= 1).map((item) => item.cliente);

  return {
    exactMatch: exactCandidates.length === 1 ? exactCandidates[0] : null,
    matches: allCandidates.slice(0, 5),
    allCandidates
  };
}

function rankClienteMatch(cliente, searchToken) {
  const fields = [
    cliente.nome_visualizzato,
    cliente.ragione_sociale,
    [cliente.nome, cliente.cognome].filter(Boolean).join(' '),
    cliente.localita,
    cliente.email,
    cliente.piva,
    cliente.codice_fiscale
  ]
    .map(normalizeSearchToken)
    .filter(Boolean);

  if (!fields.length) return 999;
  if (fields.some((field) => field === searchToken)) return 0;
  if (fields.some((field) => field.replace(/\s+/g, '') === searchToken.replace(/\s+/g, ''))) return 1;
  if (fields.some((field) => field.startsWith(searchToken))) return 2;
  if (fields.some((field) => field.includes(searchToken))) return 3;
  return 999;
}

async function loadClienteById(supabase, clienteId) {
  if (!clienteId) return null;
  const { data } = await supabase
    .from('clienti')
    .select('*')
    .eq('id', clienteId)
    .maybeSingle();
  return data || null;
}

function defaultClienteForm(nomeVisualizzato = '') {
  return {
    tipo_cliente: 'privato',
    nome_visualizzato: nomeVisualizzato || '',
    ragione_sociale: '',
    nome: '',
    cognome: '',
    localita: '',
    piva: '',
    codice_fiscale: '',
    email: '',
    privato_csv: '',
    pec: '',
    codice_destinatario: '',
    indirizzo: '',
    cap: '',
    citta: '',
    provincia: '',
    paese: 'Italia',
    note: ''
  };
}

function clienteRecordToForm(cliente) {
  return {
    tipo_cliente: cliente.tipo_cliente || 'privato',
    nome_visualizzato: cliente.nome_visualizzato || '',
    ragione_sociale: cliente.ragione_sociale || '',
    nome: cliente.nome || '',
    cognome: cliente.cognome || '',
    localita: cliente.localita || '',
    piva: cliente.piva || '',
    codice_fiscale: cliente.codice_fiscale || '',
    email: cliente.email || '',
    privato_csv: cliente.privato_csv == null ? '' : (cliente.privato_csv ? 'Si' : 'No'),
    pec: cliente.pec || '',
    codice_destinatario: cliente.codice_destinatario || '',
    indirizzo: cliente.indirizzo || '',
    cap: cliente.cap || '',
    citta: cliente.citta || '',
    provincia: cliente.provincia || '',
    paese: cliente.paese || 'Italia',
    note: cliente.note || ''
  };
}

function buildSuggestedClienteForm({ parseResult, matchedClient, clienteFormInput }) {
  const base = matchedClient
    ? clienteRecordToForm(matchedClient)
    : defaultClienteForm(parseResult.cliente_nome_ricerca);
  const merged = { ...base };

  if (clienteFormInput) {
    Object.keys(clienteFormInput).forEach((field) => {
      const incomingValue = normalizeString(clienteFormInput[field]);
      if (incomingValue) {
        merged[field] = incomingValue;
      }
    });
    if (CLIENT_TYPES.has(clienteFormInput.tipo_cliente)) {
      merged.tipo_cliente = clienteFormInput.tipo_cliente;
    }
  }

  if (!merged.nome_visualizzato && parseResult.cliente_nome_ricerca) {
    merged.nome_visualizzato = parseResult.cliente_nome_ricerca;
  }

  if (!matchedClient && parseResult.cliente_nome_ricerca) {
    const inferredTipoCliente = inferCustomerTypeFromName(parseResult.cliente_nome_ricerca);
    if (!clienteFormInput?.tipo_cliente || clienteFormInput.tipo_cliente === 'privato') {
      merged.tipo_cliente = inferredTipoCliente;
    }
    if (merged.tipo_cliente === 'azienda' && !merged.ragione_sociale) {
      merged.ragione_sociale = parseResult.cliente_nome_ricerca;
    }
  }

  if (!matchedClient && merged.tipo_cliente === 'privato' && parseResult.cliente_nome_ricerca) {
    const splitName = splitPersonName(parseResult.cliente_nome_ricerca);
    if (!merged.nome) merged.nome = splitName.nome;
    if (!merged.cognome) merged.cognome = splitName.cognome;
  }

  return normalizeClienteInput(merged);
}

function splitPersonName(fullName) {
  const parts = normalizeString(fullName).split(/\s+/).filter(Boolean);
  if (parts.length <= 1) {
    return {
      nome: parts[0] || '',
      cognome: ''
    };
  }

  return {
    nome: parts.slice(0, -1).join(' '),
    cognome: parts.slice(-1).join(' ')
  };
}

function normalizeClienteInput(input) {
  const payload = defaultClienteForm();
  Object.keys(payload).forEach((key) => {
    payload[key] = normalizeString(input[key] ?? payload[key]);
  });

  payload.tipo_cliente = CLIENT_TYPES.has(payload.tipo_cliente) ? payload.tipo_cliente : 'privato';
  payload.paese = payload.paese || 'Italia';
  payload.privato_csv = normalizePrivatoCsv(payload.privato_csv);
  return payload;
}

function validateClientePayload(cliente) {
  const fields = [];
  if (!CLIENT_TYPES.has(cliente.tipo_cliente)) fields.push('tipo_cliente');
  if (!cliente.nome_visualizzato) fields.push('nome_visualizzato');

  if (cliente.tipo_cliente === 'azienda') {
    if (!cliente.ragione_sociale && !cliente.nome_visualizzato) fields.push('ragione_sociale');
    if (!cliente.piva && !cliente.codice_fiscale) fields.push('piva_o_codice_fiscale');
  } else if (cliente.tipo_cliente === 'professionista') {
    if (!cliente.nome && !cliente.ragione_sociale && !cliente.nome_visualizzato) fields.push('nome_o_ragione_sociale');
    if (!cliente.piva && !cliente.codice_fiscale) fields.push('piva_o_codice_fiscale');
  } else if (!cliente.nome_visualizzato) {
    fields.push('nome_visualizzato');
  }

  return {
    ok: fields.length === 0,
    fields
  };
}

function mergeClienteData(existingClient, incomingClient, overwriteFields = []) {
  if (!existingClient) {
    return normalizeClienteInput(incomingClient);
  }

  const base = clienteRecordToForm(existingClient);
  const next = { ...base };

  Object.keys(base).forEach((field) => {
    const incomingValue = normalizeString(incomingClient[field]);
    if (!incomingValue) return;

    if (!base[field]) {
      next[field] = incomingValue;
      return;
    }

    if (overwriteFields.includes(field) && incomingValue !== base[field]) {
      next[field] = incomingValue;
    }
  });

  next.tipo_cliente = CLIENT_TYPES.has(incomingClient.tipo_cliente) ? incomingClient.tipo_cliente : base.tipo_cliente;
  return normalizeClienteInput(next);
}

function getClienteUpdatedFields(existingClient, mergedClientPayload) {
  if (!existingClient) {
    return Object.keys(mergedClientPayload).filter((field) => {
      const value = mergedClientPayload[field];
      return value !== null && value !== '';
    });
  }

  const base = clienteRecordToForm(existingClient);
  return Object.keys(mergedClientPayload).filter((field) => {
    const before = normalizeString(base[field]);
    const after = normalizeString(mergedClientPayload[field]);
    return before !== after;
  });
}

async function upsertCliente(supabase, existingClient, payload) {
  if (existingClient?.id) {
    const { data, error } = await supabase
      .from('clienti')
      .update(payload)
      .eq('id', existingClient.id)
      .select()
      .single();

    if (error) {
      return {
        errorResponse: jsonResponse(500, {
          error: 'Errore aggiornamento cliente',
          detail: error.message
        })
      };
    }

    return { data };
  }

  const { data, error } = await supabase
    .from('clienti')
    .insert(payload)
    .select()
    .single();

  if (error) {
    return {
      errorResponse: jsonResponse(500, {
        error: 'Errore creazione cliente',
        detail: error.message
      })
    };
  }

  return { data };
}

function slimCliente(cliente) {
  return {
    id: cliente.id,
    nome_visualizzato: cliente.nome_visualizzato || '',
    tipo_cliente: cliente.tipo_cliente || 'privato',
    ragione_sociale: cliente.ragione_sociale || '',
    nome: cliente.nome || '',
    cognome: cliente.cognome || '',
    localita: cliente.localita || '',
    piva: cliente.piva || '',
    codice_fiscale: cliente.codice_fiscale || '',
    email: cliente.email || '',
    privato_csv: cliente.privato_csv == null ? null : !!cliente.privato_csv,
    pec: cliente.pec || '',
    codice_destinatario: cliente.codice_destinatario || '',
    indirizzo: cliente.indirizzo || '',
    cap: cliente.cap || '',
    citta: cliente.citta || '',
    provincia: cliente.provincia || '',
    paese: cliente.paese || 'Italia',
    note: cliente.note || ''
  };
}

function escapeLike(value) {
  return String(value || '').replace(/[%_]/g, '\\$&');
}

function normalizePrivatoCsv(value) {
  const normalized = normalizeString(value).toLowerCase();
  if (!normalized) return null;
  if (['si', 'sì', 'yes', 'true', '1'].includes(normalized)) return true;
  if (['no', 'false', '0'].includes(normalized)) return false;
  return null;
}

function inferCustomerTypeFromName(value) {
  const normalized = normalizeSearchToken(value);
  if (COMPANY_HINTS.some((hint) => normalized.includes(hint))) {
    return 'azienda';
  }
  return 'privato';
}
