-- ============================================================
-- Patch sblocco calendario PMS Fase 2A: lettura apartments
-- ============================================================
-- Scopo: consentire SELECT sulla tabella apartments agli utenti
--   autenticati via Supabase Auth, perche senza questa policy
--   il calendario backoffice-calendario.html riceve [] e
--   non puo costruire l'UI.
--
-- Stato precedente: apartments ha RLS abilitata ma nessuna policy
--   per il role authenticated. Le query ritornano 200 [] silente.
--
-- Cosa NON fa: non disabilita RLS, non aggiunge INSERT/UPDATE/DELETE,
--   non tocca altre tabelle.
--
-- Rollback:
--   DROP POLICY "authenticated_read_apartments" ON public.apartments;
-- ============================================================

DROP POLICY IF EXISTS "authenticated_read_apartments" ON public.apartments;

CREATE POLICY "authenticated_read_apartments"
  ON public.apartments
  FOR SELECT
  TO authenticated
  USING (true);
