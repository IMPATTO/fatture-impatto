# Weekly Architecture Review

Data: 2026-06-21

## Stato

Valutazione complessiva: **Needs attention**.

- Siti builder-managed: 7
- Pagine con owner univoco: 17
- Functions mappate: 48
- Shared functions residue: 0
- Progetti standalone fuori builder: 0
- Sezioni deprecated attive: 1

## Check

- OK `verify:integrity` — Site integrity check passed for 127 tracked HTML/JS files.
- OK `check:deprecated-refs` — No deprecated pr-luca references found outside the allowed legacy perimeter.
- FAIL `check:hardcoded-domains` — 11: [https://checkin.illupoaffitta.com] const PORTALE_BASE_URL = 'https://checkin.illupoaffitta.com'; | - supabase/migrations/202605160001_schedule_nightly_sync.sql | 22: [https://calendario.illupoaffitta.com] url := 'https://calendario.illupoaffitta.com/.netlify/functions/cron-sync-prices-nightly',
- OK `check:links` — - checkinillupoaffitta.netlify.app: 5 checked, 0 failing | - villamargheritarimini.com: 6 checked, 0 failing | All links look healthy.
- OK `check:links:deprecated` — Checked 2 links across 1 hosts. | - checkin.illupoaffitta.com: 2 checked, 0 failing | All links look healthy.
- OK `build:fatt-docs` — Built Fatturazione + Documenti into dist/fatt-docs
- OK `build:operativita` — Built Richieste + Beds24 + Messaggi into dist/operativita
- OK `build:portale` — Built Portale into dist/portale
- OK `build:contabilita` — Built Contabilita into dist/contabilita
- OK `build:calendario` — Built Calendario into dist/calendario
- OK `build:residence-pr` — Built PR Kekko + Residence into dist/residence-pr
- OK `build:villa-margherita` — Built Villa Margherita into dist/villa-margherita

## Working Tree

- Modificati: 27
- Aggiunti: 3
- Cancellati: 0
- Non tracciati: 29

## Punti critici

- Ci sono 1 check rossi: `check:hardcoded-domains`.
- Restano 2 URL legacy mantenuti per compatibilita.
- Esiste ancora una snapshot estratta `extracted-repos/residence-pr`, quindi il rischio di drift non e del tutto sparito.
- La working tree non e pulita: 27 modificati, 3 aggiunti, 0 cancellati, 29 non tracciati.

## Prossime mosse

- Governare `extracted-repos/residence-pr`: o rigenerazione automatica, o rimozione della copia viva.
- Applicare al database le migration gia presenti per allineare runtime e repo.

