# Siti separati

Questo repo ora puo produrre 5 pacchetti di deploy distinti:

- `fatt-docs`: fatturazione + documenti
- `operativita`
- `portale`
- `contabilita`
- `calendario`

Ogni sito viene costruito con:

```bash
SITE_KEY=fatt-docs npm run build:site
```

L'output finisce in `dist/<site-key>`.

## Perche questa struttura

- ogni deploy pubblica solo i file e le function dichiarate per quell'area
- i link verso le altre aree continuano a funzionare grazie a pagine redirect generate in build
- la comunicazione dati resta via Supabase e Netlify Functions, ma il deploy non trascina piu tutto il repo insieme

## Variabili URL consigliate

Ogni sito dovrebbe conoscere gli URL degli altri tramite env vars:

- `SITE_URL_FATT_DOCS`
- `SITE_URL_OPERATIVITA`
- `SITE_URL_PORTALE`
- `SITE_URL_CONTABILITA`
- `SITE_URL_CALENDARIO`

Se una env var manca, il redirect usa il path relativo. Va bene solo finche i siti restano sullo stesso dominio.
