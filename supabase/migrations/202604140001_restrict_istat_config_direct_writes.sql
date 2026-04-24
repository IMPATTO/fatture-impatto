drop policy if exists apartment_istat_config_authenticated_insert on public.apartment_istat_config;
drop policy if exists apartment_istat_config_authenticated_update on public.apartment_istat_config;

comment on table public.apartment_istat_config is
'Configurazione ISTAT per appartamento. La scrittura client-side authenticated e'' stata chiusa: update/insert passano ora da Netlify Function con service role.';
