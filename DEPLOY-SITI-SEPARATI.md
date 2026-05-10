# Deploy siti separati

## Aree

1. `fatt-docs` = fatturazione + documenti
2. `operativita`
3. `portale`
4. `contabilita`
5. `calendario`
6. `residence-pr` = Residence Margherita + PR Luca

## Build locale

```bash
npm ci
SITE_KEY=fatt-docs npm run build:site
SITE_KEY=operativita npm run build:site
SITE_KEY=portale npm run build:site
SITE_KEY=contabilita npm run build:site
SITE_KEY=calendario npm run build:site
SITE_KEY=residence-pr npm run build:site
```

Ogni build crea una cartella `dist/<site-key>`.

## Setup Netlify consigliato

Crea 6 siti Netlify separati puntando tutti a questo stesso repository.

Per ciascun sito:

1. imposta come **Base directory** la cartella `sites/<site-key>`
2. lascia leggere il `netlify.toml` presente in quella cartella
3. aggiungi le env vars `SITE_URL_*` con gli URL pubblici degli altri 5 siti
4. mantieni le stesse env vars Supabase/servizi gia in uso oggi

## Perche funziona

- il sito pubblica solo le sue pagine, i suoi asset e le sue function
- i link verso gli altri moduli vengono generati come redirect statici
- se tocchi `operativita`, il deploy di `contabilita` non cambia
- `residence-pr` puo vivere qui oppure essere il primo blocco da staccare in una repo autonoma

## Split repo dedicata

Se vuoi davvero staccare `Residence + PR` in una repo autonoma, da questo repo puoi generare una snapshot pronta con:

```bash
npm run extract:residence-pr
```

Di default l'output va in `extracted-repos/residence-pr` ed include:

- pagine HTML `Residence/PR`
- asset JS minimi
- Netlify Functions dedicate
- migration Supabase minime per `rm_*` e `partner_booking_requests`

## Nota importante

La separazione di deploy e pronta, ma alcuni file nuovi oggi sono ancora non tracciati da git.
Finche non vengono aggiunti in commit, il controllo integrita continuera a bloccare i deploy per evitare 404 nascosti.
