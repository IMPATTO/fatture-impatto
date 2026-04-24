alter table public.contabilita_bollette
  add column if not exists supply_address text,
  add column if not exists account_holder text,
  add column if not exists extraction_status text not null default 'pending';

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'contabilita_bollette_extraction_status_check'
  ) then
    alter table public.contabilita_bollette
      add constraint contabilita_bollette_extraction_status_check
      check (extraction_status in ('pending', 'partial', 'extracted', 'failed'));
  end if;
end $$;

create index if not exists contabilita_bollette_extraction_status_idx
  on public.contabilita_bollette (extraction_status);

comment on column public.contabilita_bollette.supply_address is
'Indirizzo di fornitura estratto dal documento bolletta.';

comment on column public.contabilita_bollette.account_holder is
'Intestatario della fornitura estratto dal documento bolletta.';

comment on column public.contabilita_bollette.extraction_status is
'Stato estrazione assistita: pending, partial, extracted, failed.';
