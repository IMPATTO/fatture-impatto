begin;

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

comment on function public.is_internal_staff() is
'Ritorna true per gli account interni autorizzati a usare i backoffice browser.';

grant execute on function public.is_internal_staff() to authenticated;

grant select on table public.apartments to authenticated;
grant select, update, delete on table public.ospiti_check_in to authenticated;
grant insert on table public.audit_log to authenticated;

drop policy if exists apartments_backoffice_select on public.apartments;
create policy apartments_backoffice_select
  on public.apartments
  for select
  to authenticated
  using (public.is_internal_staff());

drop policy if exists ospiti_check_in_backoffice_select on public.ospiti_check_in;
create policy ospiti_check_in_backoffice_select
  on public.ospiti_check_in
  for select
  to authenticated
  using (public.is_internal_staff());

drop policy if exists ospiti_check_in_backoffice_update on public.ospiti_check_in;
create policy ospiti_check_in_backoffice_update
  on public.ospiti_check_in
  for update
  to authenticated
  using (public.is_internal_staff())
  with check (public.is_internal_staff());

drop policy if exists ospiti_check_in_backoffice_delete on public.ospiti_check_in;
create policy ospiti_check_in_backoffice_delete
  on public.ospiti_check_in
  for delete
  to authenticated
  using (public.is_internal_staff());

drop policy if exists clienti_backoffice_select on public.clienti;
create policy clienti_backoffice_select
  on public.clienti
  for select
  to authenticated
  using (public.is_internal_staff());

drop policy if exists clienti_backoffice_insert on public.clienti;
create policy clienti_backoffice_insert
  on public.clienti
  for insert
  to authenticated
  with check (public.is_internal_staff());

drop policy if exists clienti_backoffice_update on public.clienti;
create policy clienti_backoffice_update
  on public.clienti
  for update
  to authenticated
  using (public.is_internal_staff())
  with check (public.is_internal_staff());

drop policy if exists clienti_backoffice_delete on public.clienti;
create policy clienti_backoffice_delete
  on public.clienti
  for delete
  to authenticated
  using (public.is_internal_staff());

drop policy if exists contabilita_documenti_accounting_only_select on public.contabilita_documenti;
create policy contabilita_documenti_accounting_only_select
  on public.contabilita_documenti
  for select
  to authenticated
  using (public.is_internal_staff());

drop policy if exists contabilita_documenti_accounting_only_insert on public.contabilita_documenti;
create policy contabilita_documenti_accounting_only_insert
  on public.contabilita_documenti
  for insert
  to authenticated
  with check (public.is_internal_staff());

drop policy if exists contabilita_documenti_accounting_only_update on public.contabilita_documenti;
create policy contabilita_documenti_accounting_only_update
  on public.contabilita_documenti
  for update
  to authenticated
  using (public.is_internal_staff())
  with check (public.is_internal_staff());

drop policy if exists contabilita_documenti_accounting_only_delete on public.contabilita_documenti;
create policy contabilita_documenti_accounting_only_delete
  on public.contabilita_documenti
  for delete
  to authenticated
  using (public.is_internal_staff());

drop policy if exists contabilita_bollette_accounting_only_select on public.contabilita_bollette;
create policy contabilita_bollette_accounting_only_select
  on public.contabilita_bollette
  for select
  to authenticated
  using (public.is_internal_staff());

drop policy if exists contabilita_bollette_accounting_only_insert on public.contabilita_bollette;
create policy contabilita_bollette_accounting_only_insert
  on public.contabilita_bollette
  for insert
  to authenticated
  with check (public.is_internal_staff());

drop policy if exists contabilita_bollette_accounting_only_update on public.contabilita_bollette;
create policy contabilita_bollette_accounting_only_update
  on public.contabilita_bollette
  for update
  to authenticated
  using (public.is_internal_staff())
  with check (public.is_internal_staff());

drop policy if exists contabilita_bollette_accounting_only_delete on public.contabilita_bollette;
create policy contabilita_bollette_accounting_only_delete
  on public.contabilita_bollette
  for delete
  to authenticated
  using (public.is_internal_staff());

drop policy if exists contabilita_bollette_allocazioni_accounting_only_select on public.contabilita_bollette_allocazioni;
create policy contabilita_bollette_allocazioni_accounting_only_select
  on public.contabilita_bollette_allocazioni
  for select
  to authenticated
  using (public.is_internal_staff());

drop policy if exists contabilita_bollette_allocazioni_accounting_only_insert on public.contabilita_bollette_allocazioni;
create policy contabilita_bollette_allocazioni_accounting_only_insert
  on public.contabilita_bollette_allocazioni
  for insert
  to authenticated
  with check (public.is_internal_staff());

drop policy if exists contabilita_bollette_allocazioni_accounting_only_update on public.contabilita_bollette_allocazioni;
create policy contabilita_bollette_allocazioni_accounting_only_update
  on public.contabilita_bollette_allocazioni
  for update
  to authenticated
  using (public.is_internal_staff())
  with check (public.is_internal_staff());

drop policy if exists contabilita_bollette_allocazioni_accounting_only_delete on public.contabilita_bollette_allocazioni;
create policy contabilita_bollette_allocazioni_accounting_only_delete
  on public.contabilita_bollette_allocazioni
  for delete
  to authenticated
  using (public.is_internal_staff());

drop policy if exists audit_log_authenticated_insert on public.audit_log;
create policy audit_log_authenticated_insert
  on public.audit_log
  for insert
  to authenticated
  with check (public.is_internal_staff());

commit;
