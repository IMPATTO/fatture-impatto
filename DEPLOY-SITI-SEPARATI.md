# Deploy siti separati

## Aree

1. `fatt-docs` = fatturazione + documenti
2. `operativita`
3. `portale`
4. `contabilita`
5. `calendario`

## Build locale

```bash
npm ci
SITE_KEY=fatt-docs npm run build:site
SITE_KEY=operativita npm run build:site
SITE_KEY=portale npm run build:site
SITE_KEY=contabilita npm run build:site
SITE_KEY=calendario npm run build:site
```

Ogni build crea una cartella `dist/<site-key>`.

## Setup Netlify consigliato

Crea 5 siti Netlify separati puntando tutti a questo stesso repository.

Per ciascun sito:

1. imposta come **Base directory** la cartella `sites/<site-key>`
2. lascia leggere il `netlify.toml` presente in quella cartella
3. aggiungi le env vars `SITE_URL_*` con gli URL pubblici degli altri 4 siti
4. mantieni le stesse env vars Supabase/servizi gia in uso oggi

## Perche funziona

- il sito pubblica solo le sue pagine, i suoi asset e le sue function
- i link verso gli altri moduli vengono generati come redirect statici
- se tocchi `operativita`, il deploy di `contabilita` non cambia

## Nota importante

La separazione di deploy e pronta, ma alcuni file nuovi oggi sono ancora non tracciati da git.
Finche non vengono aggiunti in commit, il controllo integrita continuera a bloccare i deploy per evitare 404 nascosti.
