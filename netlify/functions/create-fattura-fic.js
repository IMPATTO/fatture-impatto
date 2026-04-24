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
  parseEventJson,
  resolvePaymentAccountId,
  resolvePaymentAccountType,
  resolvePaymentStatus,
  resolveSezionale,
  saveFatturaStaging
} = require('./_lib/fatture-fic');

exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') {
    return jsonResponse(405, { error: 'Method not allowed' });
  }

  const parsed = parseEventJson(event);
  if (parsed.errorResponse) return parsed.errorResponse;
  const body = parsed.body;

  const { ospiti_check_in_id } = body;
  if (!ospiti_check_in_id) {
    return jsonResponse(400, { error: 'ospiti_check_in_id richiesto' });
  }

  const supabase = createSupabaseAdmin();
  const auth = await authenticateRequest(event, supabase);
  if (auth.errorResponse) return auth.errorResponse;
  const { user } = auth;

  const columnSupport = await detectOptionalColumns(supabase);
  const selectFields = [
    'id',
    'nome',
    'cognome',
    'email',
    'telefono',
    'stato',
    'tipo_cliente',
    'piva_cliente',
    'iva_percentuale',
    'importo_lordo',
    'data_checkin',
    'data_checkout',
    'codice_fiscale',
    'codice_fiscale_verificato',
    'indirizzo_residenza',
    'apartments(nome_appartamento)'
  ];
  if (columnSupport.ragione_sociale) selectFields.push('ragione_sociale');
  if (columnSupport.indirizzo_fatturazione) selectFields.push('indirizzo_fatturazione');

  const { data: ospite, error: ospiteError } = await supabase
    .from('ospiti_check_in')
    .select(selectFields.join(','))
    .eq('id', ospiti_check_in_id)
    .single();

  if (ospiteError || !ospite) {
    return jsonResponse(404, {
      error: 'Ospite non trovato',
      detail: ospiteError?.message
    });
  }

  const existingStagingSelect = ['id', 'numero_fattura', 'link_fatture_cloud', 'stato'];
  if (columnSupport.fatture_staging_fic_document_id) existingStagingSelect.push('fic_document_id');
  if (columnSupport.fatture_staging_sezionale) existingStagingSelect.push('sezionale');
  if (columnSupport.fatture_staging_payment_status) existingStagingSelect.push('payment_status');

  const { data: existingStaging, error: existingStagingError } = await supabase
    .from('fatture_staging')
    .select(existingStagingSelect.join(', '))
    .eq('ospiti_check_in_id', ospiti_check_in_id)
    .maybeSingle();

  if (existingStagingError) {
    return jsonResponse(500, {
      error: 'Errore controllo fattura esistente',
      detail: existingStagingError.message
    });
  }

  if (existingStaging) {
    return jsonResponse(409, {
      error: 'Fattura già presente',
      fattura_staging_id: existingStaging.id,
      fic_document_id: existingStaging.fic_document_id ?? null,
      numero_fattura: existingStaging.numero_fattura ?? null,
      link_fatture_cloud: existingStaging.link_fatture_cloud ?? null,
      payment_status: existingStaging.payment_status ?? null,
      sezionale: existingStaging.sezionale ?? null,
      stato: existingStaging.stato ?? null
    });
  }

  const totals = computeInvoiceTotals(ospite.importo_lordo, ospite.iva_percentuale ?? 22);
  if (!totals.ok) {
    return jsonResponse(400, {
      error: totals.error,
      allowed: totals.allowed || undefined
    });
  }

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
  const itemsList = buildInvoiceItems({
    description: 'Soggiorno',
    itemDescription: `Check-in: ${ospite.data_checkin ?? '-'} | Check-out: ${ospite.data_checkout ?? '-'}`,
    importoTotale: totals.importoTotale,
    vatId: totals.vatId,
    bolloApplicato: totals.bolloApplicato
  });

  const ficPayload = buildFicPayload({
    entity: buildClientEntity(ospite, { tipoCliente: ospite.tipo_cliente }),
    date: today,
    itemsList,
    sezionale,
    paymentStatus,
    paymentAccountId: paymentAccount.paymentAccountId,
    paymentAccountType,
    importoTotaleDocumento: totals.importoTotaleDocumento,
    imponibileDocumento: totals.imponibileDocumento
  });

  const ficResult = await createDraftOnFic(ficPayload);
  if (!ficResult.ok) {
    return jsonResponse(ficResult.statusCode, {
      error: ficResult.error,
      detail: ficResult.detail,
      raw_response: ficResult.raw_response,
      tipoDocumento: 'invoice'
    });
  }

  const stagingPayload = {
    ospiti_check_in_id,
    numero_fattura: ficResult.ficNumero ? String(ficResult.ficNumero) : null,
    data_fattura: today,
    importo_lordo: totals.importoTotale,
    iva_percentuale: totals.ivaPercentuale,
    importo_totale_con_iva: totals.importoTotaleDocumento,
    nome_cliente: ospite.nome,
    cognome_cliente: ospite.cognome,
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

  const localSyncDetails = [];
  let staging = null;

  const stagingResult = await saveFatturaStaging(supabase, stagingPayload);
  if (stagingResult.error) {
    console.error('Errore upsert fatture_staging:', stagingResult.error);
    localSyncDetails.push({
      step: 'fatture_staging_upsert',
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

  const { error: ospiteUpdateError } = await supabase
    .from('ospiti_check_in')
    .update({ stato: 'BOZZA_CREATA', updated_at: new Date().toISOString() })
    .eq('id', ospiti_check_in_id)
    .eq('stato', 'APPROVATA');

  if (ospiteUpdateError) {
    console.error('Errore update ospiti_check_in post FiC:', ospiteUpdateError);
    localSyncDetails.push({
      step: 'ospiti_check_in_update',
      message: ospiteUpdateError.message,
      detail: {
        code: ospiteUpdateError.code || null,
        details: ospiteUpdateError.details || null,
        hint: ospiteUpdateError.hint || null
      }
    });
  }

  const { error: auditError } = await insertAuditLog(supabase, {
    userEmail: user.email,
    action: 'CREATE_FATTURA_FIC',
    tableName: 'fatture_staging',
    recordId: staging?.id ?? ospiti_check_in_id
  });

  if (auditError) {
    console.error('Errore audit_log post FiC:', auditError);
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

  const localSyncWarning = localSyncDetails.length > 0;

  return jsonResponse(200, {
    success: true,
    created_on_fic: true,
    fic_document_id: ficResult.ficDocId,
    fic_url: ficResult.ficDocUrl,
    numero_fattura: ficResult.ficNumero,
    fic_numeration: ficResult.ficReturnedNumeration,
    fattura_staging_id: staging?.id ?? null,
    sezionale,
    requested_sezionale: requestedSezionale,
    sezionale_source: sezionaleSource,
    sezionale_warning: sezionaleWarning,
    bollo_applicato: totals.bolloApplicato,
    payment_status: paymentStatus,
    payment_account_type: paymentAccountType,
    importo_lordo: totals.importoTotale,
    importo_totale_documento: totals.importoTotaleDocumento,
    imponibile_calcolato: totals.imponibileDocumento,
    local_sync_warning: localSyncWarning,
    local_sync_details: localSyncDetails,
    local_sync_context: localSyncWarning
      ? {
          fic_document_id: ficResult.ficDocId,
          fic_url: ficResult.ficDocUrl,
          numero_fattura: ficResult.ficNumero,
          fic_numeration: ficResult.ficReturnedNumeration || sezionale,
          payment_status: paymentStatus,
          recovery_hint: 'Documento creato su Fatture in Cloud ma sincronizzazione locale incompleta. Verificare fatture_staging e registrare manualmente i riferimenti FiC se necessario.'
        }
      : null
  });
};
