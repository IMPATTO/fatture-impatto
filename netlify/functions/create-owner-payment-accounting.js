const { createClient } = require('@supabase/supabase-js');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: CORS, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return respond(405, { error: 'Method not allowed' });
  }

  const supabaseUrl = process.env.SUPABASE_URL;
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
  const anonKey = process.env.SUPABASE_ANON_KEY;
  if (!supabaseUrl || !serviceRoleKey || !anonKey) {
    return respond(500, { error: 'Configurazione Supabase mancante' });
  }

  const authHeader = event.headers.authorization || event.headers.Authorization;
  if (!authHeader?.startsWith('Bearer ')) {
    return respond(401, { error: 'Autenticazione mancante' });
  }

  const token = authHeader.replace('Bearer ', '').trim();
  const authClient = createClient(supabaseUrl, anonKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  });
  const { data: authData, error: authError } = await authClient.auth.getUser(token);
  const email = String(authData?.user?.email || '').trim().toLowerCase();
  if (authError || !email) {
    return respond(401, { error: 'Sessione non valida' });
  }

  const allowedEmails = new Set([
    'fatturazione@illupoaffitta.com',
    'contabilita@illupoaffitta.com',
  ]);
  if (!allowedEmails.has(email)) {
    return respond(403, { error: 'Accesso non autorizzato' });
  }

  let body;
  try {
    body = JSON.parse(event.body || '{}');
  } catch {
    return respond(400, { error: 'JSON non valido' });
  }

  const paymentId = String(body.payment_id || '').trim();
  if (!paymentId) {
    return respond(400, { error: 'payment_id richiesto' });
  }

  const admin = createClient(supabaseUrl, serviceRoleKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  });

  try {
    const { data: payment, error: paymentError } = await admin
      .from('apartment_owner_payments')
      .select(`
        *,
        apartments(id,nome_appartamento,struttura_nome),
        owners(id,nome_visualizzato,email),
        apartment_contracts(id,titolo)
      `)
      .eq('id', paymentId)
      .maybeSingle();

    if (paymentError) {
      return respond(500, { error: 'Errore lettura pagamento', detail: paymentError.message });
    }
    if (!payment) {
      return respond(404, { error: 'Pagamento non trovato' });
    }

    if (!payment.needs_accounting) {
      return respond(400, { error: 'Pagamento segnato come solo gestionale' });
    }

    const amount = Number(payment.amount || 0);
    if (!Number.isFinite(amount) || amount === 0) {
      return respond(400, { error: 'Importo non valido per contabilita' });
    }

    const sourceRef = `owner_payment:${payment.id}`;
    const counterparty = clean(payment.owners?.nome_visualizzato) || 'Proprietario';
    const apartmentName = clean(payment.apartments?.nome_appartamento) || 'Appartamento';
    const contractTitle = clean(payment.apartment_contracts?.titolo);
    const paymentTypeLabel = paymentTypeLabelFor(payment.payment_type);
    const description = [
      paymentTypeLabel,
      apartmentName,
      contractTitle,
      clean(payment.reference),
    ].filter(Boolean).join(' · ');

    const payload = {
      source_channel: 'manual',
      source_email: email,
      source_subject: `Owner payment ${apartmentName}`,
      source: 'owner_payment',
      source_ref: sourceRef,
      supplier_name: counterparty,
      counterparty,
      document_type: 'altro',
      document_kind: 'owner_payment',
      stato: 'DA_APPROVARE',
      approval_status: 'da_revisionare',
      apartment_id: payment.apartment_id,
      amount_total: amount,
      total_amount: amount,
      currency: 'EUR',
      issue_date: payment.payment_date || null,
      document_date: payment.payment_date || null,
      competence_month: payment.competence_month || null,
      competence_date: payment.competence_month || null,
      payment_method: clean(payment.accounting_payment_method) || deriveAccountingPaymentMethod(payment.payment_type),
      fiscal_status: clean(payment.accounting_fiscal_status) || deriveAccountingFiscalStatus(payment.payment_type),
      category: 'proprietari',
      subcategory: 'pagamento_proprietario',
      description,
      notes: buildNotes(payment),
      reviewed_json: {
        apartment_owner_payment_id: payment.id,
        owner_id: payment.owner_id || null,
        contract_id: payment.contract_id || null,
        payment_type: payment.payment_type || null,
        reference: clean(payment.reference) || null,
      },
    };

    const { data: existing, error: existingError } = await admin
      .from('contabilita_documenti')
      .select('id')
      .eq('source_ref', sourceRef)
      .maybeSingle();
    if (existingError) {
      return respond(500, { error: 'Errore lookup documento contabile', detail: existingError.message });
    }

    let contabilitaId = existing?.id || payment.contabilita_documento_id || null;
    if (existing?.id) {
      const { error } = await admin.from('contabilita_documenti').update(payload).eq('id', existing.id);
      if (error) {
        await markPaymentSyncError(admin, payment.id, error.message);
        return respond(500, { error: 'Errore aggiornamento documento contabile', detail: error.message });
      }
      contabilitaId = existing.id;
    } else {
      const { data, error } = await admin.from('contabilita_documenti').insert(payload).select('id').single();
      if (error) {
        await markPaymentSyncError(admin, payment.id, error.message);
        return respond(500, { error: 'Errore creazione documento contabile', detail: error.message });
      }
      contabilitaId = data.id;
    }

    const syncStatus = payload.fiscal_status === 'fatturato' ? 'fatturato' : 'contabilizzato';
    const { error: paymentUpdateError } = await admin
      .from('apartment_owner_payments')
      .update({
        contabilita_documento_id: contabilitaId,
        accounting_sync_status: syncStatus,
        accounting_payment_method: payload.payment_method,
        accounting_fiscal_status: payload.fiscal_status,
        accounting_sync_error: null,
        updated_at: new Date().toISOString(),
      })
      .eq('id', payment.id);
    if (paymentUpdateError) {
      return respond(500, { error: 'Documento creato ma sync pagamento fallita', detail: paymentUpdateError.message, contabilita_documento_id: contabilitaId });
    }

    return respond(200, {
      success: true,
      payment_id: payment.id,
      contabilita_documento_id: contabilitaId,
      accounting_sync_status: syncStatus,
    });
  } catch (err) {
    console.error('[create-owner-payment-accounting] error:', err);
    return respond(500, { error: 'Errore interno', detail: err.message });
  }
};

async function markPaymentSyncError(admin, paymentId, message) {
  await admin
    .from('apartment_owner_payments')
    .update({
      accounting_sync_status: 'errore_sync',
      accounting_sync_error: clean(message) || 'Errore sincronizzazione',
      updated_at: new Date().toISOString(),
    })
    .eq('id', paymentId);
}

function clean(value) {
  if (value == null) return '';
  return String(value).trim();
}

function paymentTypeLabelFor(value) {
  return {
    bonifico_proprietario: 'Bonifico proprietario',
    contante: 'Contante',
    contante_da_fatturare: 'Contante da fatturare',
    rimborso_spese: 'Rimborso spese',
    canone: 'Canone',
    anticipo: 'Anticipo',
    altro: 'Altro',
  }[value] || 'Pagamento proprietario';
}

function deriveAccountingPaymentMethod(paymentType) {
  if (paymentType === 'contante' || paymentType === 'contante_da_fatturare') return 'contanti';
  if (paymentType === 'bonifico_proprietario') return 'bonifico';
  return 'altro';
}

function deriveAccountingFiscalStatus(paymentType) {
  if (paymentType === 'contante_da_fatturare') return 'da_fatturare';
  if (paymentType === 'contante') return 'non_soggetto';
  return null;
}

function buildNotes(payment) {
  const lines = [];
  if (clean(payment.note)) lines.push(clean(payment.note));
  if (clean(payment.reference)) lines.push(`Riferimento: ${clean(payment.reference)}`);
  if (clean(payment.apartments?.struttura_nome)) lines.push(`Struttura: ${clean(payment.apartments.struttura_nome)}`);
  return lines.join('\n') || null;
}
