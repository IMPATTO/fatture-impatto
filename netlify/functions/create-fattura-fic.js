const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const FIC_TOKEN = process.env.FATTURE_CLOUD_TOKEN;
const FIC_COMPANY_ID = process.env.FATTURE_CLOUD_COMPANY_ID;
const FIC_BASE = 'https://api-v2.fattureincloud.it';

const VAT_IDS = {
  22: 0,
  10: 3,
  4: 4,
  5: 54,
  0: 6
};

exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: JSON.stringify({ error: 'Method not allowed' }) };
  }

  let body;
  try {
    body = JSON.parse(event.body);
  } catch {
    return { statusCode: 400, body: JSON.stringify({ error: 'Invalid JSON' }) };
  }

  const { ospiti_check_in_id } = body;
  if (!ospiti_check_in_id) {
    return { statusCode: 400, body: JSON.stringify({ error: 'ospiti_check_in_id richiesto' }) };
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
  const authHeader = event.headers.authorization || event.headers.Authorization;

  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return {
      statusCode: 401,
      body: JSON.stringify({ error: 'Unauthorized' })
    };
  }

  const token = authHeader.replace('Bearer ', '').trim();

  const {
    data: { user },
    error: userError
  } = await supabase.auth.getUser(token);

  if (userError || !user) {
    return {
      statusCode: 401,
      body: JSON.stringify({ error: 'Unauthorized' })
    };
  }

  const { data: ospite, error: ospiteError } = await supabase
    .from('ospiti_check_in')
    .select('*, apartments(nome_appartamento)')
    .eq('id', ospiti_check_in_id)
    .single();

  if (ospiteError || !ospite) {
    return {
      statusCode: 404,
      body: JSON.stringify({ error: 'Ospite non trovato', detail: ospiteError?.message })
    };
  }

  const isAzienda = ospite.tipo_cliente === 'azienda' && ospite.piva_cliente;
  const { data: existingStaging, error: existingStagingError } = await supabase
    .from('fatture_staging')
    .select('id, numero_fattura, link_fatture_cloud, stato')
    .eq('ospiti_check_in_id', ospiti_check_in_id)
    .maybeSingle();

  if (existingStagingError) {
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: 'Errore controllo fattura esistente',
        detail: existingStagingError.message
      })
    };
  }

  if (existingStaging) {
    return {
      statusCode: 409,
      body: JSON.stringify({
        error: 'Fattura già presente',
        fattura_staging_id: existingStaging.id,
        numero_fattura: existingStaging.numero_fattura ?? null,
        link_fatture_cloud: existingStaging.link_fatture_cloud ?? null,
        stato: existingStaging.stato ?? null
      })
    };
  }
  const tipoDocumento = body.document_type || 'invoice';

  const ivaPerc = Number(ospite.iva_percentuale ?? 22);
  const vatId = VAT_IDS[ivaPerc];

  if (vatId === undefined) {
    return {
      statusCode: 400,
      body: JSON.stringify({
        error: `iva_percentuale ${ivaPerc} non supportata`,
        allowed: Object.keys(VAT_IDS)
      })
    };
  }

  // importo_lordo viene trattato come LORDO IVA INCLUSA
  const importoLordo = parseFloat(ospite.importo_lordo ?? 0);

  if (!Number.isFinite(importoLordo) || importoLordo <= 0) {
    return {
      statusCode: 400,
      body: JSON.stringify({ error: 'importo_lordo non valido' })
    };
  }

  const imponibile = parseFloat((importoLordo / (1 + ivaPerc / 100)).toFixed(2));
  const importoTotale = parseFloat(importoLordo.toFixed(2));

  const today = new Date().toISOString().split('T')[0];
  const nomeApp = ospite.apartments?.nome_appartamento ?? 'Appartamento';
  const paymentStatus = body.payment_status === 'not_paid' ? 'not_paid' : 'paid';
const PAYMENT_ACCOUNT_ID = process.env.FIC_PAYMENT_ACCOUNT_ID;

  const ficPayload = {
    data: {
      type: tipoDocumento,
      date: today,
      currency: { id: 'EUR' },
      language: { code: 'it', name: 'Italiano' },
      entity: buildClient(ospite, isAzienda),
      use_gross_prices: true,
            items_list: [
        {
          name: `Soggiorno`,
          description: `Check-in: ${ospite.data_checkin ?? '-'} | Check-out: ${ospite.data_checkout ?? '-'}`,
          qty: 1,
          gross_price: importoTotale,
          vat: { id: vatId },
          discount: 0,
          order: 1
        }
  ],
         payments_list: [
        {
          amount: importoTotale,
          due_date: today,
          status: paymentStatus,
          payment_account: {
            id: Number(PAYMENT_ACCOUNT_ID)
          }
        }
      ],
      gross_worth: importoTotale,
      net_worth: imponibile,
      is_marked: false,
      e_invoice: false
    }
  };

  let ficResponse;
  try {
    const res = await fetch(`${FIC_BASE}/c/${FIC_COMPANY_ID}/issued_documents`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${FIC_TOKEN}`,
        'Content-Type': 'application/json',
        Accept: 'application/json'
      },
      body: JSON.stringify(ficPayload)
    });

    ficResponse = await res.json();

    if (!res.ok) {
      console.error('FiC API error:', JSON.stringify(ficResponse));
      return {
        statusCode: res.status,
        body: JSON.stringify({ error: 'Errore FiC API', detail: ficResponse, tipoDocumento })
      };
    }
  } catch (e) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'Errore chiamata FiC', detail: e.message })
    };
  }

  const ficDoc = ficResponse?.data;
  const ficDocId = ficDoc?.id ?? null;
  const ficDocUrl = ficDoc?.url ?? null;
  const ficNumero = ficDoc?.number ?? null;

  const { data: staging, error: stagingError } = await supabase
    .from('fatture_staging')
    .upsert(
      {
        ospiti_check_in_id,
        numero_fattura: ficNumero ? String(ficNumero) : null,
        data_fattura: today,
        importo_lordo: importoTotale,
        iva_percentuale: ivaPerc,
        importo_totale_con_iva: importoTotale,
        nome_cliente: ospite.nome,
        cognome_cliente: ospite.cognome,
        stato: 'BOZZA_CREATA',
        payment_status: paymentStatus,
        link_fatture_cloud: ficDocUrl ?? `https://secure.fattureincloud.it/issued_documents/${ficDocId}`
      },
      {
        onConflict: 'ospiti_check_in_id',
        returning: 'representation'
      }
    )
    .select()
    .single();

  if (stagingError) {
    console.error('Errore upsert fatture_staging:', stagingError);
  }

  await supabase
    .from('ospiti_check_in')
    .update({ stato: 'BOZZA_CREATA', updated_at: new Date().toISOString() })
    .eq('id', ospiti_check_in_id)
    .eq('stato', 'APPROVATA');

  await supabase.from('audit_log').insert({
    user_email: 'system@illupoaffitta.com',
    action: 'CREATE_FATTURA_FIC',
    table_name: 'fatture_staging',
    record_id: staging?.id ?? ospiti_check_in_id,
    timestamp: new Date().toISOString()
  });

  return {
    statusCode: 200,
    body: JSON.stringify({
      success: true,
      fic_document_id: ficDocId,
      fic_url: ficDocUrl,
      numero_fattura: ficNumero,
      fattura_staging_id: staging?.id ?? null,
      payment_status: paymentStatus,
      importo_lordo: importoTotale,
      imponibile_calcolato: imponibile
    })
  };
};

function buildClient(ospite, isAzienda) {
  const client = {
    name: isAzienda
      ? (ospite.piva_cliente ?? ospite.nome)
      : [ospite.nome, ospite.cognome].filter(Boolean).join(' '),
    type: isAzienda ? 'company' : 'person',
    address_street: ospite.indirizzo_residenza ?? null
  };

  if (!isAzienda && ospite.codice_fiscale && ospite.codice_fiscale_verificato) {
    client.tax_code = ospite.codice_fiscale;
  }

  if (isAzienda && ospite.piva_cliente) {
    client.vat_number = ospite.piva_cliente;
  }

  if (ospite.email) client.email = ospite.email;

  return client;
}
