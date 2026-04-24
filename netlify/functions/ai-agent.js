const ANTHROPIC_URL = 'https://api.anthropic.com/v1/messages';
const BEDS24_URL = 'https://api.beds24.com/v2';
let beds24TokenCache = null;

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

const TOOLS = [
  {
    name: 'get_today_summary',
    description: 'Restituisce il riepilogo della giornata: check-in, check-out, occupazione, segnalazioni urgenti da Supabase e prenotazioni da Beds24.',
    input_schema: { type: 'object', properties: {}, required: [] },
  },
  {
    name: 'get_bookings',
    description: 'Recupera prenotazioni da Beds24 per un periodo e/o appartamento specifico.',
    input_schema: {
      type: 'object',
      properties: {
        date_from: { type: 'string', description: 'Data inizio YYYY-MM-DD (default: oggi)' },
        date_to: { type: 'string', description: 'Data fine YYYY-MM-DD (default: +30 giorni)' },
        room_id: { type: 'string', description: 'ID appartamento Beds24 (opzionale)' },
        apartment: { type: 'string', description: 'Nome appartamento da cercare in Supabase se room_id non e disponibile' },
      },
      required: [],
    },
  },
  {
    name: 'get_availability',
    description: 'Controlla disponibilita e notti libere per un appartamento in un periodo.',
    input_schema: {
      type: 'object',
      properties: {
        room_id: { type: 'string', description: 'ID appartamento Beds24' },
        apartment: { type: 'string', description: 'Nome appartamento da cercare in Supabase se room_id non e disponibile' },
        date_from: { type: 'string', description: 'Data inizio YYYY-MM-DD' },
        date_to: { type: 'string', description: 'Data fine YYYY-MM-DD' },
      },
      required: ['date_from', 'date_to'],
    },
  },
  {
    name: 'set_price',
    description: 'Modifica il prezzo per un appartamento in un periodo specifico su Beds24.',
    input_schema: {
      type: 'object',
      properties: {
        room_id: { type: 'string', description: 'ID appartamento Beds24' },
        apartment: { type: 'string', description: 'Nome appartamento da cercare in Supabase se room_id non e disponibile' },
        date_from: { type: 'string', description: 'Data inizio YYYY-MM-DD' },
        date_to: { type: 'string', description: 'Data fine YYYY-MM-DD' },
        price: { type: 'number', description: 'Nuovo prezzo per notte in EUR' },
        percentage: { type: 'number', description: 'Oppure variazione percentuale (es. 20 = +20%, -10 = -10%)' },
      },
      required: ['date_from', 'date_to'],
    },
  },
  {
    name: 'block_dates',
    description: 'Blocca date su Beds24 per un appartamento (manutenzione, uso personale, ecc.).',
    input_schema: {
      type: 'object',
      properties: {
        room_id: { type: 'string', description: 'ID appartamento Beds24' },
        apartment: { type: 'string', description: 'Nome appartamento da cercare in Supabase se room_id non e disponibile' },
        date_from: { type: 'string', description: 'Data inizio YYYY-MM-DD' },
        date_to: { type: 'string', description: 'Data fine YYYY-MM-DD' },
        reason: { type: 'string', description: 'Motivo del blocco (opzionale)' },
      },
      required: ['room_id', 'date_from', 'date_to'],
    },
  },
  {
    name: 'get_checkins_supabase',
    description: 'Legge i check-in da Supabase: ospiti, stati, segnalazioni, schedine alloggiati.',
    input_schema: {
      type: 'object',
      properties: {
        stato: { type: 'string', description: 'Filtra per stato: CHECK_IN_COMPLETATO, DA_VERIFICARE, APPROVATA, SCARTATA' },
        date_from: { type: 'string', description: 'Data inizio check-in YYYY-MM-DD' },
        date_to: { type: 'string', description: 'Data fine check-in YYYY-MM-DD' },
        apartment: { type: 'string', description: 'Nome o ID appartamento (opzionale)' },
      },
      required: [],
    },
  },
  {
    name: 'send_alloggiati',
    description: 'Invia le schedine alloggiati alla Polizia di Stato (AlloggiatiWeb) per un ospite specifico.',
    input_schema: {
      type: 'object',
      properties: {
        checkin_id: { type: 'string', description: 'ID del record ospiti_check_in in Supabase' },
      },
      required: ['checkin_id'],
    },
  },
  {
    name: 'get_revenue',
    description: 'Calcola entrate per periodo, appartamento, o confronto mesi.',
    input_schema: {
      type: 'object',
      properties: {
        period: { type: 'string', description: 'mese corrente, mese scorso, anno, o YYYY-MM' },
        apartment: { type: 'string', description: 'Nome o ID appartamento (opzionale, ometti per totale)' },
      },
      required: ['period'],
    },
  },
  {
    name: 'get_alerts',
    description: 'Restituisce tutte le segnalazioni urgenti: schedine mancanti, fatture scadute, check-in non processati.',
    input_schema: { type: 'object', properties: {}, required: [] },
  },
  {
    name: 'create_todo',
    description: 'Crea un nuovo task nella lista todo del sistema.',
    input_schema: {
      type: 'object',
      properties: {
        task: { type: 'string', description: 'Descrizione del task' },
        priority: { type: 'string', enum: ['alta', 'media', 'bassa'], description: 'Priorita' },
        due_date: { type: 'string', description: 'Scadenza YYYY-MM-DD (opzionale)' },
        apartment: { type: 'string', description: 'Appartamento correlato (opzionale)' },
      },
      required: ['task'],
    },
  },
];

async function executeTool(name, input, env) {
  const sbUrl = env.SUPABASE_URL;
  const sbKey = env.SUPABASE_SERVICE_KEY;
  const b24Key = env.BEDS24_API_KEY;

  const sbHeaders = {
    apikey: sbKey,
    Authorization: `Bearer ${sbKey}`,
    'Content-Type': 'application/json',
  };

  const today = new Date().toISOString().slice(0, 10);
  const in30 = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

  switch (name) {
    case 'get_today_summary': {
      const [checkins, alerts] = await Promise.all([
        fetch(`${sbUrl}/rest/v1/ospiti_check_in?select=nome,cognome,data_checkin,data_checkout,stato,apartments(nome_appartamento)&or=(data_checkin.eq.${today},data_checkout.eq.${today})&limit=20`, { headers: sbHeaders }),
        fetch(`${sbUrl}/rest/v1/ospiti_check_in?select=nome,cognome,data_checkin,alloggiati_stato,apartments(nome_appartamento)&alloggiati_stato=neq.INVIATA&data_checkin=lte.${today}&limit=10`, { headers: sbHeaders }),
      ]);
      const checkinsData = await checkins.json();
      const alertsData = await alerts.json();

      const arrivals = checkinsData.filter((r) => r.data_checkin === today);
      const departures = checkinsData.filter((r) => r.data_checkout === today);
      const missingSchedine = alertsData.filter((r) => !r.alloggiati_stato || r.alloggiati_stato !== 'INVIATA');

      return {
        date: today,
        arrivals: arrivals.map((r) => ({ name: `${r.nome} ${r.cognome}`, apartment: r.apartments?.nome_appartamento, checkin: r.data_checkin })),
        departures: departures.map((r) => ({ name: `${r.nome} ${r.cognome}`, apartment: r.apartments?.nome_appartamento })),
        missing_schedine: missingSchedine.length,
        missing_schedine_detail: missingSchedine.map((r) => ({ name: `${r.nome} ${r.cognome}`, apartment: r.apartments?.nome_appartamento, checkin_date: r.data_checkin })),
      };
    }

    case 'get_bookings': {
      if (!b24Key) return { error: 'BEDS24_API_KEY non configurata' };
      const from = input.date_from || today;
      const to = input.date_to || in30;
      const params = new URLSearchParams({ dateFrom: from, dateTo: to });
      const roomId = await resolveBeds24RoomId(input, sbUrl, sbHeaders);
      if (roomId) params.append('roomId', roomId);

      const res = await beds24Fetch(`/bookings?${params}`, { method: 'GET' }, env);
      if (!res.ok) return await formatBeds24Error(res);
      const data = await res.json();
      const bookings = data?.data || data || [];
      return { bookings, count: bookings.length };
    }

    case 'get_availability': {
      if (!b24Key) return { error: 'BEDS24_API_KEY non configurata' };
      const params = new URLSearchParams({ dateFrom: input.date_from, dateTo: input.date_to });
      const roomId = await resolveBeds24RoomId(input, sbUrl, sbHeaders);
      if (!roomId) return { error: 'room_id o nome appartamento richiesto per controllare la disponibilita' };
      params.append('roomId', roomId);

      const res = await beds24Fetch(`/inventory/rooms/calendar?${params}`, { method: 'GET' }, env);
      if (!res.ok) return await formatBeds24Error(res);
      const data = await res.json();
      return { room_id: roomId, availability: data?.data || data || [] };
    }

    case 'set_price': {
      if (!b24Key) return { error: 'BEDS24_API_KEY non configurata' };
      const roomId = await resolveBeds24RoomId(input, sbUrl, sbHeaders);
      if (!roomId) return { error: 'room_id o nome appartamento obbligatorio per modificare prezzi' };

      let newPrice = input.price;
      if (input.percentage && !input.price) {
        const params = new URLSearchParams({
          roomId,
          dateFrom: input.date_from,
          dateTo: input.date_to,
        });
        const res = await beds24Fetch(`/inventory/rooms/calendar?${params}`, { method: 'GET' }, env);
        if (!res.ok) return await formatBeds24Error(res);
        const current = await res.json();
        const firstCalendarRow = current?.data?.[0]?.calendar?.[0] || current?.[0]?.calendar?.[0] || current?.data?.[0] || current?.[0];
        const currentPrice = firstCalendarRow?.price1 ?? firstCalendarRow?.price;
        if (currentPrice) newPrice = Math.round(currentPrice * (1 + input.percentage / 100));
        else return { error: 'Impossibile leggere prezzo attuale per calcolare variazione percentuale' };
      }

      const body = [
        {
          roomId,
          calendar: [
            {
              from: input.date_from,
              to: input.date_to,
              price1: newPrice,
            },
          ],
        },
      ];
      const res = await beds24Fetch('/inventory/rooms/calendar', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      }, env);
      if (!res.ok) return await formatBeds24Error(res);
      return { success: true, new_price: newPrice, from: input.date_from, to: input.date_to };
    }

    case 'block_dates': {
      if (!b24Key) return { error: 'BEDS24_API_KEY non configurata' };
      const roomId = await resolveBeds24RoomId(input, sbUrl, sbHeaders);
      if (!roomId) return { error: 'room_id o nome appartamento obbligatorio per bloccare date' };
      const body = {
        roomId,
        dateFrom: input.date_from,
        dateTo: input.date_to,
        status: 'blocked',
        description: input.reason || 'Blocco manuale',
      };
      const res = await beds24Fetch('/bookings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      }, env);
      if (!res.ok) return await formatBeds24Error(res);
      return { success: true, blocked_from: input.date_from, blocked_to: input.date_to };
    }

    case 'get_checkins_supabase': {
      let url = `${sbUrl}/rest/v1/ospiti_check_in?select=*,apartments(nome_appartamento)&order=created_at.desc&limit=50`;
      if (input.stato) url += `&stato=eq.${input.stato}`;
      if (input.date_from) url += `&data_checkin=gte.${input.date_from}`;
      if (input.date_to) url += `&data_checkin=lte.${input.date_to}`;
      const res = await fetch(url, { headers: sbHeaders });
      const data = await res.json();
      return { checkins: data, count: data.length };
    }

    case 'send_alloggiati': {
      const baseUrl = env.URL || env.DEPLOY_PRIME_URL || env.DEPLOY_URL;
      if (!baseUrl) return { error: 'URL Netlify non disponibile per chiamare send-alloggiati' };

      const headers = { 'Content-Type': 'application/json' };
      if (env.INTERNAL_KEY) headers['x-internal-key'] = env.INTERNAL_KEY;

      const res = await fetch(`${baseUrl}/.netlify/functions/send-alloggiati`, {
        method: 'POST',
        headers,
        body: JSON.stringify({ checkin_id: input.checkin_id }),
      });
      return res.json();
    }

    case 'get_revenue': {
      let dateFrom;
      let dateTo;
      const now = new Date();
      if (input.period === 'mese corrente') {
        dateFrom = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-01`;
        dateTo = today;
      } else if (input.period === 'mese scorso') {
        const prev = new Date(now.getFullYear(), now.getMonth() - 1, 1);
        dateFrom = prev.toISOString().slice(0, 10);
        const lastDay = new Date(now.getFullYear(), now.getMonth(), 0);
        dateTo = lastDay.toISOString().slice(0, 10);
      } else if (input.period === 'anno') {
        dateFrom = `${now.getFullYear()}-01-01`;
        dateTo = today;
      } else {
        dateFrom = `${input.period}-01`;
        const [y, m] = input.period.split('-');
        dateTo = new Date(parseInt(y, 10), parseInt(m, 10), 0).toISOString().slice(0, 10);
      }
      let url = `${sbUrl}/rest/v1/ospiti_check_in?select=importo_lordo,data_checkin,apartments(nome_appartamento)&data_checkin=gte.${dateFrom}&data_checkin=lte.${dateTo}&stato=eq.APPROVATA`;
      if (input.apartment) url += `&apartments.nome_appartamento=ilike.*${input.apartment}*`;
      const res = await fetch(url, { headers: sbHeaders });
      const data = await res.json();
      const total = data.reduce((sum, row) => sum + parseFloat(row.importo_lordo || 0), 0);
      return { period: input.period, total: Math.round(total * 100) / 100, count: data.length, breakdown: data };
    }

    case 'get_alerts': {
      const [missingSchedine, pendingCheckins] = await Promise.all([
        fetch(`${sbUrl}/rest/v1/ospiti_check_in?select=id,nome,cognome,data_checkin,apartments(nome_appartamento)&alloggiati_stato=neq.INVIATA&data_checkin=lte.${today}&stato=neq.SCARTATA&limit=20`, { headers: sbHeaders }),
        fetch(`${sbUrl}/rest/v1/ospiti_check_in?select=id,nome,cognome,data_checkin,stato,apartments(nome_appartamento)&stato=in.(CHECK_IN_COMPLETATO,DA_VERIFICARE)&limit=20`, { headers: sbHeaders }),
      ]);
      const schedine = await missingSchedine.json();
      const pending = await pendingCheckins.json();
      return {
        missing_schedine: schedine,
        pending_checkins: pending,
        total_alerts: schedine.length + pending.length,
      };
    }

    case 'create_todo': {
      const payload = {
        task: input.task,
        priority: input.priority || 'media',
        due_date: input.due_date || null,
        apartment: input.apartment || null,
        source: 'ai_agent',
        done: false,
        created_at: new Date().toISOString(),
      };
      const res = await fetch(`${sbUrl}/rest/v1/agent_todos`, {
        method: 'POST',
        headers: { ...sbHeaders, Prefer: 'return=representation' },
        body: JSON.stringify(payload),
      });
      if (!res.ok) return { error: `Supabase error ${res.status}: ${await res.text()}` };
      const data = await res.json();
      return { success: true, todo: data[0] };
    }

    default:
      return { error: `Tool "${name}" non riconosciuto` };
  }
}

async function resolveBeds24RoomId(input, sbUrl, sbHeaders) {
  if (input.room_id) return String(input.room_id);
  if (!input.apartment) return null;

  const apartment = String(input.apartment).trim();
  if (!apartment) return null;

  const url = `${sbUrl}/rest/v1/apartments?select=nome_appartamento,beds24_room_id&beds24_room_id=not.is.null&nome_appartamento=ilike.*${encodeURIComponent(apartment)}*&limit=1`;
  const res = await fetch(url, { headers: sbHeaders });
  if (!res.ok) return null;
  const data = await res.json();
  return data?.[0]?.beds24_room_id ? String(data[0].beds24_room_id) : null;
}

async function beds24Fetch(path, options = {}, env) {
  const directToken = env.BEDS24_API_KEY;
  const request = async (token) => fetch(`${BEDS24_URL}${path}`, {
    ...options,
    headers: {
      accept: 'application/json',
      ...(options.headers || {}),
      token,
    },
  });

  let response = await request(await getBeds24Token(directToken, false));
  if (response.status !== 401) return response;

  const refreshedToken = await getBeds24Token(directToken, true);
  if (refreshedToken && refreshedToken !== directToken) {
    response = await request(refreshedToken);
  }

  return response;
}

async function getBeds24Token(rawKey, forceRefresh) {
  if (!rawKey) return null;

  if (!forceRefresh) {
    if (beds24TokenCache && beds24TokenCache.sourceKey === rawKey && beds24TokenCache.expiresAt > Date.now()) {
      return beds24TokenCache.token;
    }
    return rawKey;
  }

  const res = await fetch(`${BEDS24_URL}/authentication/token`, {
    method: 'GET',
    headers: {
      accept: 'application/json',
      refreshToken: rawKey,
    },
  });

  if (!res.ok) {
    return rawKey;
  }

  const data = await res.json();
  const token = data?.token;
  const expiresIn = Number(data?.expiresIn || 3600);
  if (!token) return rawKey;

  beds24TokenCache = {
    sourceKey: rawKey,
    token,
    expiresAt: Date.now() + Math.max(expiresIn - 60, 60) * 1000,
  };

  return token;
}

async function formatBeds24Error(res) {
  const text = await res.text();
  let detail = text;
  try {
    const parsed = JSON.parse(text);
    detail = parsed?.error || parsed?.message || text;
  } catch (_error) {
    // Keep raw text when the response is not JSON.
  }

  if (res.status === 401) {
    return {
      error: `Beds24 autenticazione fallita (${detail}). Serve un token API V2 valido: long life token oppure refresh token attivo.`,
    };
  }

  return {
    error: `Beds24 error ${res.status}: ${detail}`,
  };
}

async function loadApartmentsList(env) {
  const res = await fetch(
    `${env.SUPABASE_URL}/rest/v1/apartments?select=id,nome_appartamento,beds24_room_id&order=nome_appartamento`,
    {
      headers: {
        apikey: env.SUPABASE_SERVICE_KEY,
        Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      },
    }
  );
  if (!res.ok) {
    throw new Error(`Errore caricamento appartamenti: ${res.status}`);
  }
  const data = await res.json();
  return (data || []).map((apartment) =>
    `- ${apartment.nome_appartamento} (id: ${apartment.id}${apartment.beds24_room_id ? `, beds24: ${apartment.beds24_room_id}` : ', NO beds24'})`
  ).join('\n');
}

function buildSystemPrompt(apartmentsList) {
  return `Sei l'assistente AI di "Il Lupo Affitta", una societa di property management che gestisce appartamenti turistici a Rimini, Pesaro, Verona e Valtournenche.

Il tuo ruolo e aiutare il gestore (che si chiama Impatto) a gestire operativamente gli appartamenti tramite conversazione in linguaggio naturale. Puoi eseguire azioni reali tramite i tool a tua disposizione.

COSA SAI FARE:
- Mostrare riepilogo della giornata (check-in, check-out, segnalazioni urgenti)
- Leggere e analizzare prenotazioni da Beds24
- Controllare disponibilita degli appartamenti
- Modificare prezzi su Beds24 (per data o percentuale)
- Bloccare date su Beds24
- Leggere check-in e ospiti da Supabase
- Inviare schedine alloggiati alla Polizia di Stato
- Calcolare entrate per periodo
- Mostrare segnalazioni urgenti
- Creare task nella lista todo

STILE:
- Rispondi sempre in italiano
- Sii conciso e diretto: l'utente e da mobile
- Dopo ogni azione conferma cosa hai fatto con un emoji (✅ successo, ⚠️ avviso, ❌ errore)
- Quando mostri liste, usa formati compatti (max 3-4 righe per item)
- Se devi fare un'azione distruttiva (blocco date, cambio prezzo), mostra un riassunto prima e chiedi conferma

DATA DI OGGI: ${new Date().toISOString().slice(0, 10)}

SISTEMA:
- Database: Supabase (tabelle: ospiti_check_in, apartments, alloggiati_invii, agent_todos)
- Channel manager: Beds24 API v2
- Compliance: AlloggiatiWeb SOAP (gia integrato)
- Fatturazione: Fatture in Cloud API (gia integrata)

APPARTAMENTI REALI NEL SISTEMA (usa SOLO questi, non inventarne altri):
${apartmentsList}

Quando un utente dice "Via Amalfi" o nome parziale, cerca il match piu vicino nella lista sopra.
Se non trovi un match certo, chiedi conferma prima di procedere.
MAI inventare nomi, MAI usare appartamenti non in questa lista.`;
}

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers: CORS, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, headers: CORS, body: JSON.stringify({ error: 'Method not allowed' }) };
  }

  const env = {
    SUPABASE_URL: process.env.SUPABASE_URL || 'https://tysxeikqbgebpfyblgeb.supabase.co',
    SUPABASE_SERVICE_KEY: process.env.SUPABASE_SERVICE_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY,
    BEDS24_API_KEY: process.env.BEDS24_API_KEY,
    INTERNAL_KEY: process.env.INTERNAL_KEY,
    ANTHROPIC_API_KEY: process.env.ANTHROPIC_API_KEY,
    URL: process.env.URL || '',
    DEPLOY_URL: process.env.DEPLOY_URL || '',
    DEPLOY_PRIME_URL: process.env.DEPLOY_PRIME_URL || '',
  };

  if (!env.ANTHROPIC_API_KEY) {
    return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: 'ANTHROPIC_API_KEY mancante' }) };
  }

  if (!env.SUPABASE_SERVICE_KEY) {
    return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: 'SUPABASE_SERVICE_KEY o SUPABASE_SERVICE_ROLE_KEY mancante' }) };
  }

  let body;
  try {
    body = JSON.parse(event.body);
  } catch (_error) {
    return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'JSON non valido' }) };
  }

  const { messages } = body;
  if (!messages?.length) {
    return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'messages obbligatorio' }) };
  }

  const apartmentsList = await loadApartmentsList(env);
  const systemPrompt = buildSystemPrompt(apartmentsList);
  let currentMessages = [...messages];
  let maxIterations = 8;

  while (maxIterations-- > 0) {
    const claudeRes = await fetch(ANTHROPIC_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: 'claude-opus-4-5-20251101',
        max_tokens: 1024,
        system: systemPrompt,
        tools: TOOLS,
        messages: currentMessages,
      }),
    });

    if (!claudeRes.ok) {
      const err = await claudeRes.text();
      return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: 'Errore Claude API', detail: err }) };
    }

    const claudeData = await claudeRes.json();
    const hasToolUse = claudeData.content?.some((block) => block.type === 'tool_use');

    if (claudeData.stop_reason === 'end_turn' || !hasToolUse) {
      const textBlock = claudeData.content.find((block) => block.type === 'text');
      return {
        statusCode: 200,
        headers: CORS,
        body: JSON.stringify({
          reply: textBlock?.text || '',
          usage: claudeData.usage,
        }),
      };
    }

    const toolUseBlocks = claudeData.content.filter((block) => block.type === 'tool_use');
    currentMessages.push({ role: 'assistant', content: claudeData.content });

    const toolResults = await Promise.all(
      toolUseBlocks.map(async (block) => {
        let result;
        try {
          result = await executeTool(block.name, block.input, env);
        } catch (error) {
          result = { error: error.message };
        }
        return {
          type: 'tool_result',
          tool_use_id: block.id,
          content: JSON.stringify(result),
        };
      })
    );

    currentMessages.push({ role: 'user', content: toolResults });
  }

  return {
    statusCode: 500,
    headers: CORS,
    body: JSON.stringify({ error: 'Loop agente superato' }),
  };
};
