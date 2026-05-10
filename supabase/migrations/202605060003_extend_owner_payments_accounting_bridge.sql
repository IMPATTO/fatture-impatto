alter table public.apartment_owner_payments
  add column if not exists needs_accounting boolean not null default false,
  add column if not exists accounting_sync_status text not null default 'solo_gestionale',
  add column if not exists accounting_payment_method text null,
  add column if not exists accounting_fiscal_status text null,
  add column if not exists contabilita_documento_id uuid null references public.contabilita_documenti(id) on delete set null,
  add column if not exists accounting_sync_error text null,
  add column if not exists source_origin text not null default 'manual';

do $$
begin
  if exists (
    select 1
    from pg_constraint
    where conname = 'apartment_owner_payments_type_check'
      and conrelid = 'public.apartment_owner_payments'::regclass
  ) then
    alter table public.apartment_owner_payments
      drop constraint apartment_owner_payments_type_check;
  end if;

  alter table public.apartment_owner_payments
    add constraint apartment_owner_payments_type_check
    check (
      payment_type in (
        'bonifico_proprietario',
        'contante',
        'contante_da_fatturare',
        'rimborso_spese',
        'canone',
        'anticipo',
        'altro'
      )
    );

  if not exists (
    select 1
    from pg_constraint
    where conname = 'apartment_owner_payments_accounting_sync_status_check'
      and conrelid = 'public.apartment_owner_payments'::regclass
  ) then
    alter table public.apartment_owner_payments
      add constraint apartment_owner_payments_accounting_sync_status_check
      check (accounting_sync_status in ('solo_gestionale', 'da_contabilizzare', 'contabilizzato', 'fatturato', 'errore_sync'));
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'apartment_owner_payments_accounting_payment_method_check'
      and conrelid = 'public.apartment_owner_payments'::regclass
  ) then
    alter table public.apartment_owner_payments
      add constraint apartment_owner_payments_accounting_payment_method_check
      check (accounting_payment_method is null or accounting_payment_method in ('contanti', 'bonifico', 'carta', 'pos', 'addebito', 'altro'));
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'apartment_owner_payments_accounting_fiscal_status_check'
      and conrelid = 'public.apartment_owner_payments'::regclass
  ) then
    alter table public.apartment_owner_payments
      add constraint apartment_owner_payments_accounting_fiscal_status_check
      check (accounting_fiscal_status is null or accounting_fiscal_status in ('da_fatturare', 'fatturato', 'non_soggetto', 'annullato'));
  end if;
end
$$;

create index if not exists apartment_owner_payments_accounting_sync_status_idx
  on public.apartment_owner_payments (accounting_sync_status, needs_accounting, payment_date desc nulls last);

create index if not exists apartment_owner_payments_contabilita_documento_id_idx
  on public.apartment_owner_payments (contabilita_documento_id);

update public.apartment_owner_payments
set
  needs_accounting = case
    when payment_type = 'contante_da_fatturare' then true
    else coalesce(needs_accounting, false)
  end,
  accounting_payment_method = case
    when payment_type in ('contante', 'contante_da_fatturare') then 'contanti'
    when payment_type = 'bonifico_proprietario' then 'bonifico'
    else accounting_payment_method
  end,
  accounting_fiscal_status = case
    when payment_type = 'contante_da_fatturare' then coalesce(accounting_fiscal_status, 'da_fatturare')
    when payment_type = 'contante' then coalesce(accounting_fiscal_status, 'non_soggetto')
    else accounting_fiscal_status
  end,
  accounting_sync_status = case
    when payment_type = 'contante_da_fatturare' and coalesce(contabilita_documento_id, null) is null then 'da_contabilizzare'
    when contabilita_documento_id is not null and accounting_fiscal_status = 'fatturato' then 'fatturato'
    when contabilita_documento_id is not null then 'contabilizzato'
    else coalesce(accounting_sync_status, 'solo_gestionale')
  end
where true;

insert into public.owners (nome_visualizzato, note, attivo)
select distinct
  aa.nome_account,
  concat('Prefill automatico da alloggiati_accounts', case when aa.questura is not null and btrim(aa.questura) <> '' then ' · Questura: ' || aa.questura else '' end),
  true
from public.apartment_alloggiati al
join public.alloggiati_accounts aa on aa.id = al.alloggiati_account_id
left join public.owners o on lower(o.nome_visualizzato) = lower(aa.nome_account)
where o.id is null
  and aa.nome_account is not null
  and btrim(aa.nome_account) <> '';

insert into public.apartment_owner_links (
  apartment_id,
  owner_id,
  ruolo,
  quota_percentuale,
  is_primary,
  sort_order,
  note
)
select
  al.apartment_id,
  o.id,
  'referente',
  null,
  true,
  0,
  'Prefill automatico da collegamento Alloggiati'
from public.apartment_alloggiati al
join public.alloggiati_accounts aa on aa.id = al.alloggiati_account_id
join public.owners o on lower(o.nome_visualizzato) = lower(aa.nome_account)
left join public.apartment_owner_links existing on existing.apartment_id = al.apartment_id
where existing.id is null;
