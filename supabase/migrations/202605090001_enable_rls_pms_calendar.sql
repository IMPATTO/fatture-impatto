-- Scopo: mettere in sicurezza il perimetro PMS calendar Fase 2A lato frontend.
-- Tabelle coperte: bookings, apartment_units, channel_property_mappings, sync_jobs, sync_state.
-- Cosa fa: abilita Row Level Security e consente solo SELECT agli utenti autenticati.
-- Cosa NON fa: non apre permessi anonimi, non aggiunge policy di INSERT/UPDATE/DELETE,
-- non tocca apartments, ospiti_check_in, alloggiati_*, rm_*, fatturazione o altre tabelle esistenti.
-- Le Netlify Functions server-side continuano a scrivere tramite service_role e non vengono modificate.
-- Rollback manuale:
--   DROP POLICY "authenticated_read_sync_state" ON public.sync_state;
--   DROP POLICY "authenticated_read_sync_jobs" ON public.sync_jobs;
--   DROP POLICY "authenticated_read_channel_property_mappings" ON public.channel_property_mappings;
--   DROP POLICY "authenticated_read_apartment_units" ON public.apartment_units;
--   DROP POLICY "authenticated_read_bookings" ON public.bookings;
--   ALTER TABLE public.sync_state DISABLE ROW LEVEL SECURITY;
--   ALTER TABLE public.sync_jobs DISABLE ROW LEVEL SECURITY;
--   ALTER TABLE public.channel_property_mappings DISABLE ROW LEVEL SECURITY;
--   ALTER TABLE public.apartment_units DISABLE ROW LEVEL SECURITY;
--   ALTER TABLE public.bookings DISABLE ROW LEVEL SECURITY;

ALTER TABLE public.bookings ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "authenticated_read_bookings" ON public.bookings;
CREATE POLICY "authenticated_read_bookings"
  ON public.bookings
  FOR SELECT
  TO authenticated
  USING (true);

ALTER TABLE public.apartment_units ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "authenticated_read_apartment_units" ON public.apartment_units;
CREATE POLICY "authenticated_read_apartment_units"
  ON public.apartment_units
  FOR SELECT
  TO authenticated
  USING (true);

ALTER TABLE public.channel_property_mappings ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "authenticated_read_channel_property_mappings" ON public.channel_property_mappings;
CREATE POLICY "authenticated_read_channel_property_mappings"
  ON public.channel_property_mappings
  FOR SELECT
  TO authenticated
  USING (true);

ALTER TABLE public.sync_jobs ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "authenticated_read_sync_jobs" ON public.sync_jobs;
CREATE POLICY "authenticated_read_sync_jobs"
  ON public.sync_jobs
  FOR SELECT
  TO authenticated
  USING (true);

ALTER TABLE public.sync_state ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "authenticated_read_sync_state" ON public.sync_state;
CREATE POLICY "authenticated_read_sync_state"
  ON public.sync_state
  FOR SELECT
  TO authenticated
  USING (true);
