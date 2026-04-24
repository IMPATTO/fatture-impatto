begin;

-- Pre-deploy: verificare le policy reali prima di lanciare questa migration.
-- select
--   schemaname,
--   tablename,
--   policyname,
--   roles,
--   cmd,
--   qual,
--   with_check
-- from pg_policies
-- where schemaname = 'public'
--   and tablename in (
--     'apartments',
--     'ospiti_check_in',
--     'fatture_staging',
--     'audit_log'
--   )
-- order by tablename, policyname;
--
-- Nota: questa migration puo ancora fermarsi se nel DB reale esistono policy
-- diverse da quelle note qui sotto.
--
-- Guard rail: questa passata tocca solo 4 tabelle. Se esistono policy inattese su queste
-- tabelle, la migration si ferma invece di alterarle alla cieca.
do $$
declare
  unexpected record;
begin
  for unexpected in
    select tablename, policyname
    from pg_policies
    where schemaname = 'public'
      and (
        (tablename = 'apartments' and policyname not in (
          'apartments_backoffice_select',
          'apartments_backoffice_insert_fatturazione',
          'apartments_backoffice_update_fatturazione',
          'auth_read_apartments',
          'auth_update_apartments',
          'apartments_authenticated_select',
          'apartments_authenticated_insert',
          'apartments_authenticated_update',
          'apartments_anon_select_active'
        ))
        or
        (tablename = 'ospiti_check_in' and policyname not in (
          'ospiti_check_in_backoffice_select',
          'ospiti_check_in_backoffice_update',
          'ospiti_check_in_authenticated_select',
          'ospiti_check_in_authenticated_update'
        ))
        or
        (tablename = 'audit_log' and policyname not in (
          'audit_log_authenticated_insert'
        ))
        or
        (tablename = 'fatture_staging' and false)
      )
  loop
    raise exception
      'Policy inattesa su %.%: %',
      'public',
      unexpected.tablename,
      unexpected.policyname;
  end loop;
end
$$;

-- Ripulisce solo le policy introdotte dalla bozza precedente, se presenti.
drop policy if exists apartments_authenticated_select on public.apartments;
drop policy if exists apartments_authenticated_insert on public.apartments;
drop policy if exists apartments_authenticated_update on public.apartments;
drop policy if exists apartments_anon_select_active on public.apartments;

drop policy if exists ospiti_check_in_authenticated_select on public.ospiti_check_in;
drop policy if exists ospiti_check_in_authenticated_update on public.ospiti_check_in;
drop policy if exists ospiti_check_in_authenticated_delete on public.ospiti_check_in;
drop policy if exists ospiti_check_in_backoffice_delete on public.ospiti_check_in;

drop policy if exists audit_log_authenticated_insert on public.audit_log;

alter table public.apartments enable row level security;
alter table public.ospiti_check_in enable row level security;
alter table public.fatture_staging enable row level security;
alter table public.audit_log enable row level security;

-- Compromesso temporaneo: manca una chiave tenant/ownership affidabile nel modello dati.
-- Stringiamo quindi per principal-level sui soli account backoffice rilevati nel codice caricato.
do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'apartments'
      and policyname = 'apartments_backoffice_select'
  ) then
    create policy apartments_backoffice_select
      on public.apartments
      for select
      to authenticated
      using (
        lower(coalesce(auth.jwt() ->> 'email', '')) in (
          'fatturazione@illupoaffitta.com',
          'contabilita@illupoaffitta.com'
        )
      );
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'apartments'
      and policyname = 'apartments_backoffice_insert_fatturazione'
  ) then
    create policy apartments_backoffice_insert_fatturazione
      on public.apartments
      for insert
      to authenticated
      with check (
        lower(coalesce(auth.jwt() ->> 'email', '')) = 'fatturazione@illupoaffitta.com'
      );
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'apartments'
      and policyname = 'apartments_backoffice_update_fatturazione'
  ) then
    create policy apartments_backoffice_update_fatturazione
      on public.apartments
      for update
      to authenticated
      using (
        lower(coalesce(auth.jwt() ->> 'email', '')) = 'fatturazione@illupoaffitta.com'
      )
      with check (
        lower(coalesce(auth.jwt() ->> 'email', '')) = 'fatturazione@illupoaffitta.com'
      );
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'ospiti_check_in'
      and policyname = 'ospiti_check_in_backoffice_select'
  ) then
    create policy ospiti_check_in_backoffice_select
      on public.ospiti_check_in
      for select
      to authenticated
      using (
        lower(coalesce(auth.jwt() ->> 'email', '')) in (
          'fatturazione@illupoaffitta.com',
          'contabilita@illupoaffitta.com'
        )
      );
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'ospiti_check_in'
      and policyname = 'ospiti_check_in_backoffice_update'
  ) then
    create policy ospiti_check_in_backoffice_update
      on public.ospiti_check_in
      for update
      to authenticated
      using (
        lower(coalesce(auth.jwt() ->> 'email', '')) in (
          'fatturazione@illupoaffitta.com',
          'contabilita@illupoaffitta.com'
        )
      )
      with check (
        lower(coalesce(auth.jwt() ->> 'email', '')) in (
          'fatturazione@illupoaffitta.com',
          'contabilita@illupoaffitta.com'
        )
      );
  end if;

end
$$;

-- fatture_staging: nessuna policy browser.
-- audit_log: nessuna policy browser. Le function con service role continuano a scrivere.
--
-- Dopo il deploy verificare:
-- select tablename, rowsecurity
-- from pg_tables
-- where schemaname = 'public'
--   and tablename in ('apartments', 'ospiti_check_in', 'fatture_staging', 'audit_log');

commit;
