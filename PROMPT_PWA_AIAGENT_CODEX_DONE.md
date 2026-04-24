# Prompt operativo: PWA + AI Agent completati

Usa questo riepilogo come prompt/brief di continuita per il progetto "Il Lupo Affitta".

## Cosa e stato fatto

- Creato [app.html](/Users/impattosrl/Desktop/fatture-impatto/app.html) nella root come PWA mobile-first con:
  - login Supabase standalone
  - dashboard Home
  - chat AI
  - calendario prenotazioni
  - todo list
- Creato [manifest.json](/Users/impattosrl/Desktop/fatture-impatto/manifest.json) nella root con `start_url: /app.html`.
- Creata la Netlify Function [netlify/functions/ai-agent.js](/Users/impattosrl/Desktop/fatture-impatto/netlify/functions/ai-agent.js) con tool use per:
  - riepilogo giornata
  - prenotazioni Beds24
  - disponibilita
  - cambio prezzi
  - blocco date
  - check-in da Supabase
  - invio schedine alloggiati
  - revenue
  - alert
  - creazione todo
- Aggiunta compatibilita env nella function:
  - legge `SUPABASE_SERVICE_KEY`
  - fallback automatico a `SUPABASE_SERVICE_ROLE_KEY`
- Aggiunti header CORS nella function.
- Verifica sintattica eseguita con `node --check netlify/functions/ai-agent.js`.
- Creata migration versionata: [supabase/migrations/202604240001_create_agent_todos_and_add_beds24_room_id.sql](/Users/impattosrl/Desktop/fatture-impatto/supabase/migrations/202604240001_create_agent_todos_and_add_beds24_room_id.sql)
  - crea tabella `agent_todos`
  - abilita RLS
  - crea policy authenticated
  - aggiunge `beds24_room_id` a `apartments`
- Eseguito commit di backup:
  - `backup pre-pwa-aiagent`
- Impostata su Netlify la env var:
  - `SUPABASE_SERVICE_KEY`
  - valorizzata con la stessa service role gia presente nel sito

## Cosa ho verificato sull'ambiente

- Progetto Netlify collegato:
  - sito `checkinillupoaffitta`
  - URL `https://checkin.illupoaffitta.com`
- Supabase collegato al progetto:
  - ref `tysxeikqbgebpfyblgeb`
- `ANTHROPIC_API_KEY` risulta presente in Netlify.
- `SUPABASE_SERVICE_ROLE_KEY` risulta presente in Netlify.

## Cosa non sono riuscito a completare automaticamente

- Non ho potuto applicare la migration direttamente su Supabase dal terminale:
  - nel sistema non e installato `supabase`
  - non era disponibile un token/PAT Supabase locale per usare la Management API
- Non ho potuto impostare `BEDS24_API_KEY` su Netlify:
  - la variabile non risulta presente
  - non ho trovato una chiave Beds24 recuperabile in locale

## Passi finali richiesti

1. Applicare la migration SQL su Supabase remoto.
2. Impostare `BEDS24_API_KEY` su Netlify.
3. Fare redeploy Netlify dopo il cambio env.
4. Testare da telefono:
   - login
   - tab Todo
   - chat AI con richieste senza Beds24
   - chat AI con richieste Beds24 dopo inserimento chiave

## Nota importante

La function `ai-agent.js` funziona gia per Supabase e Anthropic se la migration e applicata.
Le funzioni che dipendono da Beds24 risponderanno con errore finche `BEDS24_API_KEY` non viene configurata.
