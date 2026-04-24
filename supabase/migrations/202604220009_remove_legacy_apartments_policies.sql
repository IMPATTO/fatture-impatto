begin;

-- Pre-check:
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
--   and tablename = 'apartments'
-- order by policyname;

-- Questa micro-migration e valida solo se lo stato reale di public.apartments
-- corrisponde a quello verificato prima del deploy:
--   auth_read_apartments
--   auth_update_apartments
--   apartments_backoffice_select
--   apartments_backoffice_insert_fatturazione
--   apartments_backoffice_update_fatturazione
do $$
declare
  unexpected record;
begin
  for unexpected in
    select policyname
    from pg_policies
    where schemaname = 'public'
      and tablename = 'apartments'
      and policyname not in (
        'auth_read_apartments',
        'auth_update_apartments',
        'apartments_backoffice_select',
        'apartments_backoffice_insert_fatturazione',
        'apartments_backoffice_update_fatturazione'
      )
  loop
    raise exception 'Stato inatteso per public.apartments: policy extra trovata: %', unexpected.policyname;
  end loop;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'apartments'
      and policyname = 'auth_read_apartments'
  ) then
    raise exception 'Stato inatteso per public.apartments: manca policy auth_read_apartments';
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'apartments'
      and policyname = 'auth_update_apartments'
  ) then
    raise exception 'Stato inatteso per public.apartments: manca policy auth_update_apartments';
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'apartments'
      and policyname = 'apartments_backoffice_select'
  ) then
    raise exception 'Stato inatteso per public.apartments: manca policy apartments_backoffice_select';
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'apartments'
      and policyname = 'apartments_backoffice_insert_fatturazione'
  ) then
    raise exception 'Stato inatteso per public.apartments: manca policy apartments_backoffice_insert_fatturazione';
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'apartments'
      and policyname = 'apartments_backoffice_update_fatturazione'
  ) then
    raise exception 'Stato inatteso per public.apartments: manca policy apartments_backoffice_update_fatturazione';
  end if;
end
$$;

drop policy if exists auth_read_apartments on public.apartments;
drop policy if exists auth_update_apartments on public.apartments;

-- Post-check policies:
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
--   and tablename = 'apartments'
-- order by policyname;
--
-- Post-check rowsecurity:
-- select
--   tablename,
--   rowsecurity
-- from pg_tables
-- where schemaname = 'public'
--   and tablename = 'apartments';

commit;
