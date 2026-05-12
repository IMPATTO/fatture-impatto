-- ============================================================
-- Harden legacy partner booking request queue
-- ============================================================
-- Scopo:
--   allineare il repo allo stato sicuro desiderato del runtime,
--   lasciando la tabella partner_booking_requests accessibile
--   solo dal perimetro server-side tramite service_role.
--
-- Cosa fa:
--   - abilita RLS sulla tabella
--   - forza il passaggio attraverso RLS anche per il table owner
--   - revoca i permessi diretti a anon e authenticated
--   - mantiene pieni permessi a service_role
--
-- Cosa NON fa:
--   - non cancella dati storici
--   - non modifica schema o indici
--   - non tocca le netlify functions che continuano a usare service_role
--
-- Rollback manuale, solo se strettamente necessario:
--   ALTER TABLE public.partner_booking_requests NO FORCE ROW LEVEL SECURITY;
--   ALTER TABLE public.partner_booking_requests DISABLE ROW LEVEL SECURITY;
--   GRANT SELECT, INSERT, UPDATE ON public.partner_booking_requests TO anon, authenticated;
-- ============================================================

ALTER TABLE public.partner_booking_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.partner_booking_requests FORCE ROW LEVEL SECURITY;

REVOKE ALL ON TABLE public.partner_booking_requests FROM anon, authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.partner_booking_requests TO service_role;
