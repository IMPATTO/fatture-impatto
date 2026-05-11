-- ============================================================
-- Patch sicurezza: chiusura leak anon su apartment_istat_config_public
-- ============================================================
-- Scopo: impedire lettura anon della view, consentirla solo a authenticated.
-- Strategia: security_invoker=true (la view eredita RLS della tabella madre)
--            + REVOKE esplicito su anon.
--
-- Diagnosi pre-fix:
--   - anon legge 3 record con campi: id, apartment_id, attivo, regione,
--     sistema, portal_url, codice_struttura, auth_type, username,
--     export_format, requires_open_close, supports_file_import,
--     supports_webservice, deadline_rule, note, created_at, updated_at
--   - sensibile: username e metadati operativi/configurativi ISTAT
--   - tabella madre: apartment_istat_config
--
-- Rollback:
--   ALTER VIEW public.apartment_istat_config_public SET (security_invoker = false);
--   GRANT SELECT ON public.apartment_istat_config_public TO anon;
-- ============================================================

ALTER VIEW public.apartment_istat_config_public SET (security_invoker = true);
REVOKE ALL ON public.apartment_istat_config_public FROM anon;
GRANT SELECT ON public.apartment_istat_config_public TO authenticated;
