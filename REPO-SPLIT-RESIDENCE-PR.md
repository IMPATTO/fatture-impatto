# Split `residence-pr`

## Cosa ho preparato

La parte `Residence Margherita + PR Luca` ora puo essere separata in due modi:

1. come sito Netlify dedicato dentro questo stesso repo, usando `SITE_KEY=residence-pr`
2. come repo autonoma generata con `npm run extract:residence-pr`

## Perche conviene

- smetti di pubblicare pagine `Residence/PR` dentro moduli che non c'entrano
- tieni funzioni, password condivise e migration di quel progetto in un perimetro chiaro
- prepari la migrazione a una repo separata senza fare copia/incolla manuale ogni volta

## Contenuto del pacchetto

- pagine: `residence-kekko.html`, `residence-backoffice.html`, `pr_kekko_riccione.html`, `pr_luca_riccione.html`, `backoffice-pr-luca.html`
- asset: `js/supabase-client.js`, `js/shared-app-auth.js`, `js/pr-luca-shared.js`
- function: `shared-app-login`, `residence-api`, `pr-luca-data`, `approve-partner-booking`, `get-calendar`
- supporto: `netlify/functions/_lib/shared-auth.js`
- SQL: `setup-supabase.sql` e le migration minime collegate

## Limiti attuali da tenere presenti

- `backoffice-pr-luca.html` continua a usare utenti Supabase interni per le approvazioni, quindi la repo separata dipende comunque dai tuoi account staff reali
- `get-calendar` dentro questo pacchetto resta dipendente da `BEDS24_API_KEY` e dalle tabelle appartamenti gia presenti nel database principale
- `setup-supabase.sql` contiene piu setup storico del necessario: nella repo nuova puoi poi rifinirlo in uno script solo `Residence/PR`

## Passo consigliato dopo lo split

Una volta verificata la snapshot estratta, il passo giusto e:

1. creare la nuova repo da `extracted-repos/residence-pr`
2. collegare un sito Netlify dedicato
3. spostare li le env `RESIDENCE_*`, `PR_LUCA_PASSWORDS`, `APP_SESSION_SECRET`
4. togliere infine le pagine `Residence/PR` dal repo principale quando il nuovo deploy e stabile
