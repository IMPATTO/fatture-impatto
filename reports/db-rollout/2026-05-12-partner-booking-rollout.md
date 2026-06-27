# Partner Booking Rollout

Data UTC: 2026-05-12T14:00:01Z

## Azioni eseguite sul database live

1. Precheck eseguito via `supabase db query --linked`
2. Creata snapshot di sicurezza `public.partner_booking_requests_backup_20260512`
3. Applicato `ALTER TABLE public.partner_booking_requests FORCE ROW LEVEL SECURITY`
4. Eseguita archiviazione legacy Luca
5. Postcheck finale su grant, flag RLS e stato Luca

## Stato finale verificato

- `public.partner_booking_requests` contiene `0` righe Luca
- backup table `public.partner_booking_requests_backup_20260512` contiene `0` righe
- `relrowsecurity = true`
- `relforcerowsecurity = true`
- grant utili presenti solo per `postgres` e `service_role`

## Effetti reali del rollout

- il perimetro legacy `partner_booking_requests` e ora forzato via RLS anche per il table owner
- l’archiviazione Luca e risultata una no-op operativa perche la tabella e gia vuota
- non risultano effetti collaterali su grant o dati attivi

## Debito residuo

- nessun debito runtime residuo sul perimetro `partner_booking_requests`
- la history di `supabase_migrations.schema_migrations` e stata riconciliata:
  - `202605120002` → `harden_partner_booking_requests`
  - `202605120003` → `archive_legacy_luca_requests`
- `migration list` via pooler puo ancora richiedere `SUPABASE_DB_PASSWORD`, ma la history e stata comunque sistemata con `supabase migration repair`

## File di supporto

- `reports/db-rollout/20260512_partner_booking_requests_precheck.json`
- `reports/db-rollout/20260512_partner_booking_requests_harden.json`
- `reports/db-rollout/20260512_partner_booking_requests_force_rls.json`
- `reports/db-rollout/20260512_partner_booking_requests_archive_luca.json`
- `reports/db-rollout/20260512_partner_booking_requests_postcheck.json`
