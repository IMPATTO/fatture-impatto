const { createClient } = require('@supabase/supabase-js');
const DEFAULT_SUPABASE_URL = 'https://tysxeikqbgebpfyblgeb.supabase.co';

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Content-Type': 'application/json',
};
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const APARTMENT_INFO_COLUMNS = [
  'come_arrivare',
  'dove_parcheggiare',
  'come_entrare',
  'regole_della_casa',
  'cosa_fare_se_luce_spenta',
  'cosa_fare_se_chiuso_fuori',
  'numeri_utili',
  'consigli',
  'beach_info',
  'waste_info',
  'wifi_info',
];

let apartmentInfoColumnsCache = [...APARTMENT_INFO_COLUMNS];

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: CORS, body: '' };
  }

  if (event.httpMethod !== 'GET') {
    return respond(405, { error: 'Method not allowed' });
  }

  const supabaseUrl = process.env.SUPABASE_URL || DEFAULT_SUPABASE_URL;
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

  if (!supabaseUrl || !serviceRoleKey) {
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
    supabaseUrl,
    serviceRoleKey
  );

  try {
    if (token) {
      if (!UUID_RE.test(token)) {
        if (aptKey) {
          const apartmentPayload = await loadApartmentFallbackPayload(supabase, aptKey, preferredLang);
          if (apartmentPayload) {
            return respond(200, {
              guest: null,
              apartment: apartmentPayload.apartment,
              creds: apartmentPayload.creds,
              info: apartmentPayload.info,
              meta: {
                token_present: true,
                apt_present: !!aptKey,
                guest_found: false,
                apartment_found: true,
                used_fallback_open: true,
              },
            });
          }
        }
        return respond(404, {
          error: 'Link non valido',
          meta: {
            token_present: true,
            apt_present: !!aptKey,
            guest_found: false,
            apartment_found: false,
            used_fallback_open: false,
          },
        });
      }

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
        const apartmentPayload = aptKey
          ? await loadApartmentFallbackPayload(supabase, aptKey, preferredLang)
          : null;
        if (apartmentPayload) {
          return respond(200, {
            guest: null,
            apartment: apartmentPayload.apartment,
            creds: apartmentPayload.creds,
            info: apartmentPayload.info,
            meta: {
              token_present: true,
              apt_present: !!aptKey,
              guest_found: false,
              apartment_found: true,
              used_fallback_open: true,
            },
          });
        }
        return respond(404, {
          error: 'Link non valido',
          meta: {
            token_present: true,
            apt_present: !!aptKey,
            guest_found: false,
            apartment_found: false,
            used_fallback_open: false,
          },
        });
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
        meta: {
          token_present: true,
          apt_present: !!aptKey,
          guest_found: true,
          apartment_found: !!guest.apartments,
          used_fallback_open: false,
        },
      });
    }

    const apartmentPayload = await loadApartmentFallbackPayload(supabase, aptKey, preferredLang);
    if (!apartmentPayload) {
      return respond(404, {
        error: 'Appartamento non trovato',
        meta: {
          token_present: false,
          apt_present: !!aptKey,
          guest_found: false,
          apartment_found: false,
          used_fallback_open: false,
        },
      });
    }

    return respond(200, {
      guest: null,
      apartment: apartmentPayload.apartment,
      creds: apartmentPayload.creds,
      info: apartmentPayload.info,
      meta: {
        token_present: false,
        apt_present: !!aptKey,
        guest_found: false,
        apartment_found: true,
        used_fallback_open: true,
      },
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

  let info = await loadApartmentInfoWithFallback(supabase, apartmentId, preferredLang);
  if (!info && preferredLang !== 'IT') {
    info = await loadApartmentInfoWithFallback(supabase, apartmentId, 'IT');
  }

  return {
    creds: creds || null,
    info: info || null,
  };
}

async function loadApartmentFallbackPayload(supabase, aptKey, preferredLang) {
  const apartment = await loadApartmentByPublicKey(supabase, aptKey);
  if (!apartment?.id) return null;
  const portalData = await loadApartmentPortalData(supabase, apartment.id, preferredLang);
  return {
    apartment,
    creds: portalData.creds,
    info: portalData.info,
  };
}

async function loadApartmentByPublicKey(supabase, aptKey) {
  const { data: apartmentByKey, error: keyError } = await supabase
    .from('apartments')
    .select('id, nome_appartamento, indirizzo_completo, latitudine, longitudine, maps_url_override, public_checkin_key, attivo')
    .eq('public_checkin_key', aptKey)
    .eq('attivo', true)
    .limit(1)
    .maybeSingle();

  if (keyError) {
    throw new Error(`Errore lettura appartamento: ${keyError.message}`);
  }
  if (apartmentByKey?.id) {
    return apartmentByKey;
  }

  if (!UUID_RE.test(aptKey)) {
    return null;
  }

  const { data: apartmentById, error: idError } = await supabase
    .from('apartments')
    .select('id, nome_appartamento, indirizzo_completo, latitudine, longitudine, maps_url_override, public_checkin_key, attivo')
    .eq('id', aptKey)
    .eq('attivo', true)
    .limit(1)
    .maybeSingle();

  if (idError) {
    throw new Error(`Errore lettura appartamento: ${idError.message}`);
  }

  return apartmentById || null;
}

async function loadApartmentInfoWithFallback(supabase, apartmentId, lang) {
  let columns = Array.isArray(apartmentInfoColumnsCache) && apartmentInfoColumnsCache.length
    ? [...apartmentInfoColumnsCache]
    : [...APARTMENT_INFO_COLUMNS];

  while (columns.length) {
    const { data, error } = await supabase
      .from('apartment_info')
      .select(columns.join(', '))
      .eq('apartment_id', apartmentId)
      .eq('lingua', lang)
      .maybeSingle();

    if (!error) {
      apartmentInfoColumnsCache = [...columns];
      return data || null;
    }

    const missingColumn = getMissingColumnName(error.message);
    if (!missingColumn || !columns.includes(missingColumn)) {
      throw new Error(`Errore lettura istruzioni: ${error.message}`);
    }

    columns = columns.filter((column) => column !== missingColumn);
    apartmentInfoColumnsCache = [...columns];
  }

  return null;
}

function getMissingColumnName(message) {
  const normalized = String(message || '');
  const postgresMatch = normalized.match(/column\s+apartment_info\.([a-z_]+)\s+does not exist/i);
  if (postgresMatch) return postgresMatch[1];

  const schemaCacheMatch = normalized.match(/Could not find the '([a-z_]+)' column of 'apartment_info' in the schema cache/i);
  return schemaCacheMatch ? schemaCacheMatch[1] : '';
}

function respond(statusCode, payload) {
  return {
    statusCode,
    headers: CORS,
    body: JSON.stringify(payload),
  };
}
