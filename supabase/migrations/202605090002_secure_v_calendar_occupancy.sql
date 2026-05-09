-- ============================================================
-- Patch sicurezza v_calendar_occupancy
-- Scopo: impedire lettura anon della view, consentirla solo a authenticated.
-- Strategia: security_invoker = true per ereditare RLS delle tabelle base
--            + REVOKE esplicito su anon come cintura aggiuntiva.
--
-- Rollback manuale:
--   DROP VIEW IF EXISTS public.v_calendar_occupancy;
--   -- poi riapplicare il blocco CREATE VIEW originale da
--   -- 202605080001_create_pms_calendar_tables.sql
-- ============================================================

DROP VIEW IF EXISTS public.v_calendar_occupancy;

CREATE VIEW public.v_calendar_occupancy
WITH (security_invoker = true)
AS
SELECT
  b.apartment_unit_id,
  b.apartment_id,
  gs.occupancy_date AS date,
  b.id AS booking_id,
  b.beds24_booking_id,
  b.status,
  b.channel_normalized,
  b.guest_last_name,
  b.check_in,
  b.check_out,
  (gs.occupancy_date = b.check_in) AS is_check_in,
  (gs.occupancy_date = (b.check_out - 1)) AS is_last_night
FROM public.bookings b
CROSS JOIN LATERAL (
  SELECT generate_series(
    b.check_in::timestamp,
    (b.check_out - 1)::timestamp,
    interval '1 day'
  )::date AS occupancy_date
) gs
WHERE b.status IN ('confirmed', 'new', 'request', 'black')
  AND b.apartment_unit_id IS NOT NULL;

REVOKE ALL ON public.v_calendar_occupancy FROM anon;
GRANT SELECT ON public.v_calendar_occupancy TO authenticated;
