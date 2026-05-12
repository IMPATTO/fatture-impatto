# Roadmap Architettura Indipendente

Ultimo aggiornamento: 2026-05-12

## Obiettivo

Portare `fatture-impatto` da repo multi-area fragile a piattaforma di progetti collaboranti ma indipendenti.

Indipendente significa:

- ogni progetto ha ownership chiara di pagine, functions, asset e deploy
- la rottura di un progetto non deve rompere gli altri
- la collaborazione avviene tramite contratti espliciti: Supabase, URL canonici, API mirate
- nessun progetto ingloba pagine di un altro in build

## Confini target approvati

1. `fatt-docs`
   - Fatturazione + Documenti
2. `contabilita`
   - Contabilità indipendente
3. `portale`
   - Check-in pubblico + portale ospite + caricamento documenti
4. `operativita-beds24`
   - Richieste + Beds24 + Messaggi
5. `villa-margherita`
   - Sito indipendente
6. `pr-kekko`
   - PR Kekko + backoffice Kekko
7. `calendario`
   - Calendario indipendente
8. `pr-luca`
   - Da dismettere e rimuovere

## Diagnosi sintetica corretta

Il progetto non parte da zero:

- esiste gia un builder multi-sito in `scripts/build-site.mjs`
- esistono gia 5 configurazioni Netlify separate sotto `sites/`
- esiste gia un client Supabase condiviso in `js/supabase-client.js`
- esistono gia controlli di integrita e audit dei flussi

I problemi reali oggi sono questi:

1. ownership delle pagine non affidabile
2. alcune route e domini sono ancora hardcodati
3. esistono copie vive e snapshot parallele (`extracted-repos/residence-pr`)
4. alcuni moduli sono fuori perimetro o ibridi (`villa-margherita`)
5. `pr-luca` e ancora intrecciato in UI, functions, auth e dati

## Stato attuale da correggere prima di tutto

### 1. Registry ambiguo

`sites/site-registry.mjs` contiene oggi pagine duplicate tra piu siti e usa una logica implicita di priorita. Questo va eliminato.

Regola nuova:

- una pagina puo avere un solo owner
- una function puo avere un solo owner
- se c'e collisione, la build deve fallire

### 2. Inclusioni non compatibili con l'indipendenza

`portale` usa ancora `includeSites`, quindi oggi non e un vero progetto indipendente.

Regola nuova:

- nessun sito puo includere pagine di un altro sito in fase di build
- i link tra progetti devono essere route canoniche o redirect espliciti

### 3. Hardcode residui

Ci sono ancora URL canonici scritti a mano in punti sparsi.

Esempi da eliminare:

- `backoffice-beds24-messaggi.html`
- `elenco-link-appartamenti.html`
- `scripts/audit-client-flows.mjs`
- `docs/project-links.md`
- `config/project-links.json`

### 4. Moduli speciali fuori standard

`villa-margherita` ha una sua configurazione Netlify e il suo sito, ma oggi non e integrato nel registry dei siti principali e ha avuto link locali mancanti durante le verifiche.

`extracted-repos/residence-pr` e una copia autonoma dichiarata del blocco Residence + PR e introduce rischio di drift.

## Mappa owner target

### `fatt-docs`

Pagine:

- `backoffice.html`
- `backoffice-documenti.html`

Functions candidate owner:

- `build-istat-monthly`
- `create-fattura-fic`
- `create-fattura-from-text`
- `export-alloggiati-report`
- `export-istat-marche-xml`
- `send-alloggiati`
- `transcribe-invoice-audio`

Asset:

- `comuni.csv`
- `stati.csv`
- eventuali template documentali/fiscali

Note:

- `save-istat-config` oggi e cross-cutting e va deciso separatamente

### `contabilita`

Pagine:

- `backoffice-contabilita.html`
- `backoffice-amministrazione-appartamenti.html`

Functions candidate owner:

- `create-contract-registration-accounting`
- `create-owner-payment-accounting`
- `inbound-bollette-email`
- `inbound-bollette-telegram`
- `inbound-contabilita-email`
- `monthly-export-commercialista`
- `upload-bollette-manual`

### `portale`

Pagine:

- `index.html`
- `portale.html`
- `carica-documenti-fattura.html`
- `backoffice-portale.html`

Functions candidate owner:

- `submit-public-checkin`
- `submit-public-invoice-documents`
- `get-public-portal-data`
- `get-backoffice-portale-data`
- `apply-apartment-links`
- `audit-apartment-links`
- `translate-apartment-info`

Note:

- `backoffice-portale.html` oggi e amministrazione del dominio portale; puo restare qui se il dominio "portale" possiede davvero i link pubblici e l'anagrafica portal-facing

### `operativita-beds24`

Pagine:

- `backoffice-operativita.html`
- `backoffice-richieste.html`
- `backoffice-beds24-messaggi.html`

Functions candidate owner:

- `get-backoffice-richieste`
- `get-operativita-apartments`

Note:

- non dovrebbe possedere sync calendario o funzioni PMS di base
- deve leggere dati gia sincronizzati, non dipendere in tempo reale dal progetto calendario

### `villa-margherita`

Pagine:

- `villa-margherita/index.html`
- pagine SEO correlate in `villa-margherita/*.html`

Config:

- `villa-margherita/netlify.toml`
- `villa-margherita/robots.txt`
- `villa-margherita/sitemap.xml`

Decisione richiesta:

- o entra come progetto esplicito nel registry generale
- oppure viene considerato progetto esterno e rimosso dal perimetro dei check del repo principale

### `pr-kekko`

Pagine:

- `residence-kekko.html`
- `residence-backoffice.html`
- `pr_kekko_riccione.html`

Functions candidate owner:

- `residence-api`
- `shared-app-login`
- `approve-partner-booking`

Note:

- oggi e intrecciato con `pr-luca`; la separazione va fatta prima di dichiararlo davvero indipendente

### `calendario`

Pagine:

- `backoffice-calendario.html`

Functions candidate owner:

- `get-calendar`
- `sync-beds24-calendar`
- `beds24-sync-bookings`
- `update-calendar-inventory`
- `populate-pms-mappings-and-units`
- `beds24-inspect`
- `beds24-rate-diagnostics`
- `beds24-offers-diagnose`

Note:

- il calendario deve diventare il proprietario unico delle sync PMS/inventory
- gli altri moduli devono consumare dati persistiti, non chiamarlo come dipendenza bloccante

### `pr-luca`

Pagine da dismettere:

- `pr_luca_riccione.html`
- `backoffice-pr-luca.html`

Function da dismettere:

- `pr-luca-data`

Altri riferimenti da pulire:

- `js/pr-luca-shared.js`
- `shared-app-login` role `pr_luca`
- record e source `pr-luca` nei seed SQL e nei dati residence

## Decisioni strutturali da prendere

### Decisione A: chi possiede `save-istat-config`

Opzioni pratiche:

1. owner `fatt-docs`
   - `portale` smette di amministrare questa configurazione
2. owner `portale`
   - `fatt-docs` consuma la configurazione ma non la modifica
3. service shared dedicato
   - scelta piu pulita ma piu costosa

Raccomandazione:

- tenere `save-istat-config` in `portale` solo se il dominio portale resta owner della configurazione appartamento
- altrimenti spostarlo a `contabilita` o a un futuro modulo admin condiviso

### Decisione B: chi possiede i dati PMS/Beds24

Raccomandazione:

- proprietario unico: `calendario`
- `operativita-beds24` usa viste/tabelle Supabase gia aggiornate
- niente chiamate incrociate runtime tra progetti come dipendenza forte

### Decisione C: cosa fare di `villa-margherita`

Raccomandazione:

- trattarlo come progetto separato da subito
- o dentro un sesto registry esplicito
- o fuori dal repo principale dei siti applicativi

Lasciarlo a meta e il caso peggiore.

## Fase 0 - Congelamento e inventario

Obiettivo:

- fissare i confini senza toccare subito la produzione

Task:

1. congelare `sites/site-registry.mjs` come area critica
2. mappare ogni pagina root a un owner definitivo
3. mappare ogni function a un owner definitivo
4. elencare tutto cio che e `shared` solo per errore storico
5. separare esplicitamente il perimetro `villa-margherita`

Exit criteria:

- tabella owner pagina/function completa
- nessuna ambiguita sui 7 progetti target

## Fase 1 - Ripulire il registry e il builder

Obiettivo:

- fare in modo che il sistema multi-sito attuale sia corretto

Task:

1. rimuovere pagine duplicate dal registry
2. rimuovere `includeSites`
3. far fallire `build-site` su collisioni di pagina
4. far fallire `build-site` su function duplicate non autorizzate
5. aggiornare `_site-build.json` con owner chiaro e dipendenze dichiarate

Exit criteria:

- ogni sito builda solo le sue pagine
- nessun owner implicito

## Fase 2 - Single source of truth per i link

Obiettivo:

- eliminare i domini e path sparsi

Task:

1. creare un file centrale di route, inizialmente nel repo stesso
2. sostituire hardcode residui in HTML e JS
3. generare `docs/project-links.md` e `config/project-links.json` dal registry
4. separare i link deprecated in `config/project-links-deprecated.json`
5. aggiornare `audit-client-flows` per leggere dal registry e non da costanti sparse

Exit criteria:

- nessun dominio canonico scritto a mano fuori dalla config
- lista link generata automaticamente
- `pr-luca` non compare piu nella lista attiva dei link di progetto

## Fase 3 - Verifiche architetturali

Obiettivo:

- far fallire prima gli errori che oggi emergono solo a runtime

Task:

1. rendere `verify-site-integrity` site-aware
2. aggiungere check per collisione owner
3. aggiungere check per domini hardcodati
4. aggiungere check che vieta riferimenti a moduli dismessi (`pr-luca`)
5. escludere o integrare correttamente `villa-margherita` dal perimetro

Exit criteria:

- il check non segnala falsi positivi strutturali
- i problemi veri emergono prima del deploy
- i riferimenti legacy a `pr-luca` restano confinati nel loro perimetro

## Fase 4 - Separazione dei moduli speciali

Obiettivo:

- sbloccare i casi piu fragili

Task:

1. staccare `villa-margherita` dal perimetro ambiguo
2. dividere `pr-kekko` da `pr-luca`
3. rendere `pr-kekko` progetto chiaro e stabile
4. marcare `pr-luca` come deprecated in docs, auth e registry

Exit criteria:

- `pr-kekko` non dipende da `pr-luca`
- `villa-margherita` non rompe i check generali

## Fase 5 - Rimozione di `pr-luca`

Obiettivo:

- eliminare un modulo che oggi sporca routing, auth e dati

Task:

1. togliere pagine dal registry
2. togliere function e ruolo auth
3. rimuovere link dalle UI Residence
4. rimuovere riferimenti da docs, config e audit
5. pulire seed e riferimenti SQL se non piu necessari

Exit criteria:

- nessun file applicativo punta piu a `pr-luca`
- nessun controllo include piu `pr-luca`

## Fase 6 - Solo dopo: valutare multi-repo

Obiettivo:

- estrarre eventualmente i progetti quando i loro confini sono gia veri

Raccomandazione:

- non aprire subito 7 repo nuovi
- estrarre solo dopo che registry, routing e verifiche sono coerenti

Ordine suggerito se si decide di estrarre:

1. `villa-margherita`
2. `pr-kekko`
3. `calendario`
4. `operativita-beds24`
5. `contabilita`
6. `portale`
7. `fatt-docs`

Motivo:

- i primi tre hanno un perimetro piu naturale o gia semiautonomo
- gli ultimi due toccano piu dati e piu flussi core

## Guardrail permanenti

1. una pagina = un owner
2. una function = un owner
3. niente `includeSites`
4. niente domini hardcodati fuori dalla config
5. niente snapshot vive non governate
6. ogni nuovo progetto nasce indipendente
7. ogni collaborazione tra progetti passa da contratto esplicito

## Prossimo step operativo consigliato

Il primo intervento concreto da fare sul codice non e aprire repo nuovi.

Il primo intervento corretto e:

1. chiudere la mappa owner pagina/function
2. ripulire `sites/site-registry.mjs`
3. aggiornare `scripts/build-site.mjs` per bloccare collisioni
4. aggiornare `scripts/verify-site-integrity.mjs` per diventare site-aware

Solo dopo ha senso migrare i link sparsi e iniziare a spegnere `pr-luca`.
