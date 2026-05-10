create or replace function public.is_internal_staff()
returns boolean
language sql
stable
as $$
  select lower(coalesce(auth.jwt() ->> 'email', '')) = any (array[
    'fatturazione@illupoaffitta.com',
    'contabilita@illupoaffitta.com',
    'info@marcovenzon.com',
    'veronica.dieta@gmail.com',
    'jessica.appartamenticaldari@gmail.com',
    'cerulliserena@gmail.com',
    'ramirezgonzalezv44@gmail.com'
  ]);
$$;

grant execute on function public.is_internal_staff() to authenticated, anon, service_role;

alter table public.apartment_channel_mappings enable row level security;
revoke all on table public.apartment_channel_mappings from anon, authenticated;
grant select, insert, update, delete on table public.apartment_channel_mappings to authenticated;
grant select, insert, update, delete on table public.apartment_channel_mappings to service_role;
drop policy if exists apartment_channel_mappings_internal_staff_all on public.apartment_channel_mappings;
create policy apartment_channel_mappings_internal_staff_all
on public.apartment_channel_mappings
for all
to authenticated
using (public.is_internal_staff())
with check (public.is_internal_staff());

alter table public.apartment_credentials enable row level security;
revoke all on table public.apartment_credentials from anon, authenticated;
grant select, insert, update, delete on table public.apartment_credentials to authenticated;
grant select, insert, update, delete on table public.apartment_credentials to service_role;
drop policy if exists apartment_credentials_internal_staff_all on public.apartment_credentials;
create policy apartment_credentials_internal_staff_all
on public.apartment_credentials
for all
to authenticated
using (public.is_internal_staff())
with check (public.is_internal_staff());

alter table public.apartment_info enable row level security;
revoke all on table public.apartment_info from anon, authenticated;
grant select, insert, update, delete on table public.apartment_info to authenticated;
grant select, insert, update, delete on table public.apartment_info to service_role;
drop policy if exists apartment_info_internal_staff_all on public.apartment_info;
create policy apartment_info_internal_staff_all
on public.apartment_info
for all
to authenticated
using (public.is_internal_staff())
with check (public.is_internal_staff());

alter table public.export_commercialista_log enable row level security;
revoke all on table public.export_commercialista_log from anon, authenticated;
grant select, insert, update, delete on table public.export_commercialista_log to service_role;

alter table public.export_commercialista_delivery_attempts enable row level security;
revoke all on table public.export_commercialista_delivery_attempts from anon, authenticated;
grant select, insert, update, delete on table public.export_commercialista_delivery_attempts to service_role;

alter table public.ospiti_cronologia enable row level security;
revoke all on table public.ospiti_cronologia from anon, authenticated;
grant select, insert, update, delete on table public.ospiti_cronologia to service_role;
