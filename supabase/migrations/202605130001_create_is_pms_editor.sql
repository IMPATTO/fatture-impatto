-- ============================================================
-- Function is_pms_editor() per Fase 3.1 editing prezzi/availability
-- ============================================================
-- Sottoinsieme di is_internal_staff() che ESCLUDE ramirez.
-- Usato da beds24-push-calendar e da policy UPDATE su calendar_days.
--
-- Allowlist:
--   - fatturazione@illupoaffitta.com
--   - contabilita@illupoaffitta.com
--   - info@marcovenzon.com
--   - veronica.dieta@gmail.com
--   - jessica.appartamenticaldari@gmail.com
--   - cerulliserena@gmail.com
-- ============================================================

CREATE OR REPLACE FUNCTION public.is_pms_editor()
RETURNS boolean
LANGUAGE sql
STABLE
AS $function$
  SELECT lower(coalesce(auth.jwt() ->> 'email', '')) = ANY (ARRAY[
    'fatturazione@illupoaffitta.com',
    'contabilita@illupoaffitta.com',
    'info@marcovenzon.com',
    'veronica.dieta@gmail.com',
    'jessica.appartamenticaldari@gmail.com',
    'cerulliserena@gmail.com'
  ]);
$function$;

COMMENT ON FUNCTION public.is_pms_editor() IS
  'Allowlist PMS editing (sottoinsieme di is_internal_staff). Usata da push-calendar API e policy UPDATE su calendar_days.';

DROP POLICY IF EXISTS "pms_editor_update_calendar_days" ON public.calendar_days;
CREATE POLICY "pms_editor_update_calendar_days"
  ON public.calendar_days
  FOR UPDATE
  TO authenticated
  USING (is_pms_editor())
  WITH CHECK (is_pms_editor());

DROP POLICY IF EXISTS "pms_editor_insert_calendar_days" ON public.calendar_days;
CREATE POLICY "pms_editor_insert_calendar_days"
  ON public.calendar_days
  FOR INSERT
  TO authenticated
  WITH CHECK (is_pms_editor());
