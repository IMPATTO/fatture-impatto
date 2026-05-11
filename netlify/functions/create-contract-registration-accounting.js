const { createClient } = require('@supabase/supabase-js');
const {
  getSupabaseRuntimeConfig,
  isInternalAllowedEmail,
} = require('./_lib/shared-auth');

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

  const runtimeConfig = getSupabaseRuntimeConfig();
  const supabaseUrl = runtimeConfig.url;
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
  const anonKey = runtimeConfig.anonKey;
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
  const email = clean(authData?.user?.email).toLowerCase();
  if (authError || !email) {
    return respond(401, { error: 'Sessione non valida' });
  }

  if (!isInternalAllowedEmail(email)) {
    return respond(403, { error: 'Accesso non autorizzato' });
  }

  let body;
  try {
    body = JSON.parse(event.body || '{}');
  } catch {
    return respond(400, { error: 'JSON non valido' });
  }

  const contractId = clean(body.contract_id);
  if (!contractId) {
    return respond(400, { error: 'contract_id richiesto' });
  }

  const admin = createClient(supabaseUrl, serviceRoleKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  });

  try {
    const { data: contract, error: contractError } = await admin
      .from('apartment_contracts')
      .select(`
        *,
        apartments(id,nome_appartamento,struttura_nome),
        owners(id,nome_visualizzato,email)
      `)
      .eq('id', contractId)
      .maybeSingle();

    if (contractError) {
      return respond(500, { error: 'Errore lettura contratto', detail: contractError.message });
    }
    if (!contract) {
      return respond(404, { error: 'Contratto non trovato' });
    }

    const amount = Number(contract.registration_paid_amount || 0);
    if (!Number.isFinite(amount) || amount <= 0) {
      return respond(400, { error: 'Importo registrazione non valido' });
    }

    const sourceRef = `contract_registration:${contract.id}`;
    const apartmentName = clean(contract.apartments?.nome_appartamento) || 'Appartamento';
    const structureName = clean(contract.apartments?.struttura_nome);
    const contractTitle = clean(contract.titolo) || 'Contratto';
    const ownerName = clean(contract.owners?.nome_visualizzato);
    const counterparty = 'Agenzia Entrate';
    const description = ['Registrazione contratto', apartmentName, contractTitle].filter(Boolean).join(' · ');

    const payload = {
      source_channel: 'manual',
      source_email: email,
      source_subject: `Registrazione contratto ${apartmentName}`,
      source: 'contract_registration',
      source_ref: sourceRef,
      supplier_name: counterparty,
      counterparty,
      document_type: 'altro',
      document_kind: 'altro',
      stato: 'DA_APPROVARE',
      approval_status: 'da_revisionare',
      apartment_id: contract.apartment_id,
      amount_total: amount,
      total_amount: amount,
      currency: 'EUR',
      issue_date: contract.registration_payment_date || null,
      document_date: contract.registration_payment_date || null,
      payment_method: clean(contract.registration_payment_method) || 'altro',
      category: 'contratti',
      subcategory: 'registrazione_contratto',
      description,
      notes: buildNotes(contract, ownerName, structureName),
      reviewed_json: {
        apartment_contract_id: contract.id,
        owner_id: contract.owner_id || null,
        registration_document_url: clean(contract.registration_document_url) || null,
        registration_payment_date: contract.registration_payment_date || null,
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

    let contabilitaId = existing?.id || clean(contract.registration_contabilita_documento_id) || null;
    if (existing?.id) {
      const { error } = await admin.from('contabilita_documenti').update(payload).eq('id', existing.id);
      if (error) {
        await markContractSyncError(admin, contract.id, error.message);
        return respond(500, { error: 'Errore aggiornamento documento contabile', detail: error.message });
      }
      contabilitaId = existing.id;
    } else {
      const { data, error } = await admin.from('contabilita_documenti').insert(payload).select('id').single();
      if (error) {
        await markContractSyncError(admin, contract.id, error.message);
        return respond(500, { error: 'Errore creazione documento contabile', detail: error.message });
      }
      contabilitaId = data.id;
    }

    const { error: contractUpdateError } = await admin
      .from('apartment_contracts')
      .update({
        registration_contabilita_documento_id: contabilitaId,
        registration_accounting_sync_status: 'contabilizzato',
        registration_accounting_sync_error: null,
        updated_at: new Date().toISOString(),
      })
      .eq('id', contract.id);
    if (contractUpdateError) {
      return respond(500, { error: 'Documento creato ma sync contratto fallita', detail: contractUpdateError.message, contabilita_documento_id: contabilitaId });
    }

    return respond(200, {
      success: true,
      contract_id: contract.id,
      contabilita_documento_id: contabilitaId,
      accounting_sync_status: 'contabilizzato',
    });
  } catch (err) {
    console.error('[create-contract-registration-accounting] error:', err);
    return respond(500, { error: 'Errore interno', detail: err.message });
  }
};

async function markContractSyncError(admin, contractId, message) {
  await admin
    .from('apartment_contracts')
    .update({
      registration_accounting_sync_status: 'errore_sync',
      registration_accounting_sync_error: clean(message) || 'Errore sincronizzazione',
      updated_at: new Date().toISOString(),
    })
    .eq('id', contractId);
}

function clean(value) {
  if (value == null) return '';
  return String(value).trim();
}

function buildNotes(contract, ownerName, structureName) {
  const lines = [];
  if (clean(contract.registration_note)) lines.push(clean(contract.registration_note));
  if (clean(contract.registration_document_url)) lines.push(`Documento registrazione: ${clean(contract.registration_document_url)}`);
  if (ownerName) lines.push(`Proprietario: ${ownerName}`);
  if (structureName) lines.push(`Struttura: ${structureName}`);
  return lines.join('\n') || null;
}
