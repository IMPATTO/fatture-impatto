alter table public.contabilita_documenti enable row level security;
alter table public.contabilita_documenti force row level security;

revoke all on table public.contabilita_documenti from public;
revoke all on table public.contabilita_documenti from anon;
revoke all on table public.contabilita_documenti from authenticated;

revoke all on table public.contabilita_documenti_ui from public;
revoke all on table public.contabilita_documenti_ui from anon;
revoke all on table public.contabilita_documenti_ui from authenticated;

grant select, insert, update, delete on table public.contabilita_documenti to authenticated;

drop policy if exists contabilita_documenti_accounting_only_select on public.contabilita_documenti;
drop policy if exists contabilita_documenti_accounting_only_insert on public.contabilita_documenti;
drop policy if exists contabilita_documenti_accounting_only_update on public.contabilita_documenti;
drop policy if exists contabilita_documenti_accounting_only_delete on public.contabilita_documenti;

create policy contabilita_documenti_accounting_only_select
on public.contabilita_documenti
for select
to authenticated
using ((auth.jwt() ->> 'email') = 'contabilita@illupoaffitta.com');

create policy contabilita_documenti_accounting_only_insert
on public.contabilita_documenti
for insert
to authenticated
with check ((auth.jwt() ->> 'email') = 'contabilita@illupoaffitta.com');

create policy contabilita_documenti_accounting_only_update
on public.contabilita_documenti
for update
to authenticated
using ((auth.jwt() ->> 'email') = 'contabilita@illupoaffitta.com')
with check ((auth.jwt() ->> 'email') = 'contabilita@illupoaffitta.com');

create policy contabilita_documenti_accounting_only_delete
on public.contabilita_documenti
for delete
to authenticated
using ((auth.jwt() ->> 'email') = 'contabilita@illupoaffitta.com');
