create or replace view public.apartment_istat_config_public as
select
  id,
  apartment_id,
  attivo,
  regione,
  sistema,
  portal_url,
  codice_struttura,
  auth_type,
  username,
  export_format,
  requires_open_close,
  supports_file_import,
  supports_webservice,
  deadline_rule,
  note,
  created_at,
  updated_at
from public.apartment_istat_config;

grant select on public.apartment_istat_config_public to authenticated;

drop policy if exists apartment_istat_config_authenticated_select on public.apartment_istat_config;
revoke all on public.apartment_istat_config from authenticated;

comment on view public.apartment_istat_config_public is
'Vista sicura per il backoffice ISTAT: espone solo campi non sensibili. password_encrypted resta leggibile solo lato service role.';
