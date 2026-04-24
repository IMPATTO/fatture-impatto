const { createClient } = require('@supabase/supabase-js');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Content-Type': 'application/json',
};

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: CORS, body: '' };
  }

  if (event.httpMethod !== 'GET') {
    return respond(405, { error: 'Method not allowed' });
  }

  if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
    return respond(500, { error: 'Configurazione Supabase mancante' });
  }

  const params = new URLSearchParams(event.queryStringParameters || {});
  const token = String(params.get('token') || '').trim();
  const aptKey = String(params.get('apt') || '').trim();
  const preferredLang = String(params.get('lang') || 'IT').trim().toUpperCase() || 'IT';

  if (!token && !aptKey) {
    return respond(400, { error: 'token oppure apt richiesto' });
  }

  const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_ROLE_KEY
  );

  try {
    if (token) {
      const { data: guest, error } = await supabase
        .from('ospiti_check_in')
        .select(`
          id,
          nome,
          cognome,
          lingua,
          data_checkin,
          data_checkout,
          apartment_id,
          stato,
          apartments (
            id,
            nome_appartamento,
            indirizzo_completo,
            latitudine,
            longitudine,
            maps_url_override
          )
        `)
        .eq('portale_token', token)
        .neq('stato', 'SCARTATA')
        .limit(1)
        .maybeSingle();

      if (error) {
        return respond(500, { error: 'Errore lettura ospite', detail: error.message });
      }
      if (!guest?.apartment_id) {
        return respond(404, { error: 'Record non trovato' });
      }

      const lang = String(guest.lingua || preferredLang || 'IT').toUpperCase();
      const portalData = await loadApartmentPortalData(supabase, guest.apartment_id, lang);
      return respond(200, {
        guest: {
          id: guest.id,
          nome: guest.nome,
          cognome: guest.cognome,
          lingua: guest.lingua,
          data_checkin: guest.data_checkin,
          data_checkout: guest.data_checkout,
          apartment_id: guest.apartment_id,
          stato: guest.stato,
        },
        apartment: guest.apartments || null,
        creds: portalData.creds,
        info: portalData.info,
      });
    }

    const { data: apartment, error } = await supabase
      .from('apartments')
      .select('id, nome_appartamento, indirizzo_completo, latitudine, longitudine, maps_url_override, public_checkin_key, attivo')
      .eq('public_checkin_key', aptKey)
      .eq('attivo', true)
      .limit(1)
      .maybeSingle();

    if (error) {
      return respond(500, { error: 'Errore lettura appartamento', detail: error.message });
    }
    if (!apartment?.id) {
      return respond(404, { error: 'Appartamento non trovato' });
    }

    const portalData = await loadApartmentPortalData(supabase, apartment.id, preferredLang);
    return respond(200, {
      guest: null,
      apartment,
      creds: portalData.creds,
      info: portalData.info,
    });
  } catch (error) {
    console.error('[get-public-portal-data] error:', error);
    return respond(500, { error: 'Errore interno', detail: error.message });
  }
};

async function loadApartmentPortalData(supabase, apartmentId, preferredLang) {
  const { data: creds, error: credsError } = await supabase
    .from('apartment_credentials')
    .select('codice_accesso, luogo_chiavi, istruzioni_accesso, video_url')
    .eq('apartment_id', apartmentId)
    .limit(1)
    .maybeSingle();

  if (credsError) {
    throw new Error(`Errore lettura credenziali: ${credsError.message}`);
  }

  let { data: info, error: infoError } = await supabase
    .from('apartment_info')
    .select('come_arrivare, dove_parcheggiare, come_entrare, regole_della_casa, cosa_fare_se_luce_spenta, cosa_fare_se_chiuso_fuori, numeri_utili')
    .eq('apartment_id', apartmentId)
    .eq('lingua', preferredLang)
    .maybeSingle();

  if (infoError) {
    throw new Error(`Errore lettura istruzioni: ${infoError.message}`);
  }

  if (!info && preferredLang !== 'IT') {
    const fallback = await supabase
      .from('apartment_info')
      .select('come_arrivare, dove_parcheggiare, come_entrare, regole_della_casa, cosa_fare_se_luce_spenta, cosa_fare_se_chiuso_fuori, numeri_utili')
      .eq('apartment_id', apartmentId)
      .eq('lingua', 'IT')
      .maybeSingle();

    if (fallback.error) {
      throw new Error(`Errore fallback istruzioni: ${fallback.error.message}`);
    }
    info = fallback.data || null;
  }

  return {
    creds: creds || null,
    info: info || null,
  };
}

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}
