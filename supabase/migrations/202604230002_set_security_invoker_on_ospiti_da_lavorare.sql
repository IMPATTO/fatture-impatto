begin;

-- Pre-check:
-- select
--   n.nspname as schema_name,
--   c.relname as view_name,
--   c.reloptions
-- from pg_class c
-- join pg_namespace n on n.oid = c.relnamespace
-- where n.nspname = 'public'
--   and c.relkind = 'v'
--   and c.relname = 'ospiti_da_lavorare';

do $$
begin
  if not exists (
    select 1
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'public'
      and c.relkind = 'v'
      and c.relname = 'ospiti_da_lavorare'
  ) then
    raise exception 'View attesa mancante: public.ospiti_da_lavorare';
  end if;
end
$$;

alter view public.ospiti_da_lavorare set (security_invoker = true);

-- Post-check:
-- select
--   n.nspname as schema_name,
--   c.relname as view_name,
--   c.reloptions
-- from pg_class c
-- join pg_namespace n on n.oid = c.relnamespace
-- where n.nspname = 'public'
--   and c.relkind = 'v'
--   and c.relname = 'ospiti_da_lavorare';

commit;
