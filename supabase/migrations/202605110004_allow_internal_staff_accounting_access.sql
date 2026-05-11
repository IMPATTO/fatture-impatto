drop policy if exists contabilita_documenti_accounting_only_select on public.contabilita_documenti;
drop policy if exists contabilita_documenti_accounting_only_insert on public.contabilita_documenti;
drop policy if exists contabilita_documenti_accounting_only_update on public.contabilita_documenti;
drop policy if exists contabilita_documenti_accounting_only_delete on public.contabilita_documenti;

create policy contabilita_documenti_accounting_only_select
on public.contabilita_documenti
for select
to authenticated
using (public.is_internal_staff());

create policy contabilita_documenti_accounting_only_insert
on public.contabilita_documenti
for insert
to authenticated
with check (public.is_internal_staff());

create policy contabilita_documenti_accounting_only_update
on public.contabilita_documenti
for update
to authenticated
using (public.is_internal_staff())
with check (public.is_internal_staff());

create policy contabilita_documenti_accounting_only_delete
on public.contabilita_documenti
for delete
to authenticated
using (public.is_internal_staff());

drop policy if exists contabilita_bollette_accounting_only_select on public.contabilita_bollette;
drop policy if exists contabilita_bollette_accounting_only_insert on public.contabilita_bollette;
drop policy if exists contabilita_bollette_accounting_only_update on public.contabilita_bollette;
drop policy if exists contabilita_bollette_accounting_only_delete on public.contabilita_bollette;

create policy contabilita_bollette_accounting_only_select
  on public.contabilita_bollette
  for select
  to authenticated
  using (public.is_internal_staff());

create policy contabilita_bollette_accounting_only_insert
  on public.contabilita_bollette
  for insert
  to authenticated
  with check (public.is_internal_staff());

create policy contabilita_bollette_accounting_only_update
  on public.contabilita_bollette
  for update
  to authenticated
  using (public.is_internal_staff())
  with check (public.is_internal_staff());

create policy contabilita_bollette_accounting_only_delete
  on public.contabilita_bollette
  for delete
  to authenticated
  using (public.is_internal_staff());

drop policy if exists contabilita_bollette_allocazioni_accounting_only_select on public.contabilita_bollette_allocazioni;
drop policy if exists contabilita_bollette_allocazioni_accounting_only_insert on public.contabilita_bollette_allocazioni;
drop policy if exists contabilita_bollette_allocazioni_accounting_only_update on public.contabilita_bollette_allocazioni;
drop policy if exists contabilita_bollette_allocazioni_accounting_only_delete on public.contabilita_bollette_allocazioni;

create policy contabilita_bollette_allocazioni_accounting_only_select
  on public.contabilita_bollette_allocazioni
  for select
  to authenticated
  using (public.is_internal_staff());

create policy contabilita_bollette_allocazioni_accounting_only_insert
  on public.contabilita_bollette_allocazioni
  for insert
  to authenticated
  with check (public.is_internal_staff());

create policy contabilita_bollette_allocazioni_accounting_only_update
  on public.contabilita_bollette_allocazioni
  for update
  to authenticated
  using (public.is_internal_staff())
  with check (public.is_internal_staff());

create policy contabilita_bollette_allocazioni_accounting_only_delete
  on public.contabilita_bollette_allocazioni
  for delete
  to authenticated
  using (public.is_internal_staff());
