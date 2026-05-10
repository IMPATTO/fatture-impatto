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

drop policy if exists owners_authenticated_all on public.owners;
drop policy if exists owners_internal_staff_all on public.owners;
create policy owners_internal_staff_all
on public.owners
for all
to authenticated
using (public.is_internal_staff())
with check (public.is_internal_staff());

drop policy if exists apartment_owner_links_authenticated_all on public.apartment_owner_links;
drop policy if exists apartment_owner_links_internal_staff_all on public.apartment_owner_links;
create policy apartment_owner_links_internal_staff_all
on public.apartment_owner_links
for all
to authenticated
using (public.is_internal_staff())
with check (public.is_internal_staff());

drop policy if exists apartment_contracts_authenticated_all on public.apartment_contracts;
drop policy if exists apartment_contracts_internal_staff_all on public.apartment_contracts;
create policy apartment_contracts_internal_staff_all
on public.apartment_contracts
for all
to authenticated
using (public.is_internal_staff())
with check (public.is_internal_staff());

drop policy if exists apartment_owner_payments_authenticated_all on public.apartment_owner_payments;
drop policy if exists apartment_owner_payments_internal_staff_all on public.apartment_owner_payments;
create policy apartment_owner_payments_internal_staff_all
on public.apartment_owner_payments
for all
to authenticated
using (public.is_internal_staff())
with check (public.is_internal_staff());
