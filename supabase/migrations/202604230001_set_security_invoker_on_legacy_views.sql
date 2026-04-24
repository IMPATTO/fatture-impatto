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
--   and c.relname in (
--     'fatture_da_lavorare',
--     'ospiti_da_inviare_ps',
--     'riepilogo_adempimenti'
--   )
-- order by c.relname;

-- Questa passata tocca solo le tre view candidate legacy non referenziate dal codice attivo.
do $$
declare
  missing_view text;
begin
  foreach missing_view in array array[
    'fatture_da_lavorare',
    'ospiti_da_inviare_ps',
    'riepilogo_adempimenti'
  ]
  loop
    if not exists (
      select 1
      from pg_class c
      join pg_namespace n on n.oid = c.relnamespace
      where n.nspname = 'public'
        and c.relkind = 'v'
        and c.relname = missing_view
    ) then
      raise exception 'View attesa mancante: public.%', missing_view;
    end if;
  end loop;
end
$$;

alter view public.fatture_da_lavorare set (security_invoker = true);
alter view public.ospiti_da_inviare_ps set (security_invoker = true);
alter view public.riepilogo_adempimenti set (security_invoker = true);

-- Post-check:
-- select
--   n.nspname as schema_name,
--   c.relname as view_name,
--   c.reloptions
-- from pg_class c
-- join pg_namespace n on n.oid = c.relnamespace
-- where n.nspname = 'public'
--   and c.relkind = 'v'
--   and c.relname in (
--     'fatture_da_lavorare',
--     'ospiti_da_inviare_ps',
--     'riepilogo_adempimenti'
--   )
-- order by c.relname;

commit;
