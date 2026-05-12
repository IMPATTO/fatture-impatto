# Residence + PR

Snapshot autonoma estratta da `fatture-impatto`.

## Include

- Residence Margherita lato partner e backoffice
- storico legacy Luca servito con URL statici compatibili
- Netlify Functions minime collegate
- migration SQL minime per `rm_*` e `partner_booking_requests`

## Variabili ambiente

- `SUPABASE_URL`
- `SUPABASE_RUNTIME_URL` opzionale ma consigliata se Netlify maschera il valore
- `SUPABASE_ANON_KEY`
- `SUPABASE_SERVICE_ROLE_KEY`
- `APP_SESSION_SECRET`
- `RESIDENCE_KEKKO_PASSWORDS`
- `RESIDENCE_ADMIN_PASSWORDS`
- `BEDS24_API_KEY`

## Avvio locale

```bash
npm install
netlify dev
```

## Deploy

Pubblica direttamente questa repo su Netlify usando il `netlify.toml` in root.
