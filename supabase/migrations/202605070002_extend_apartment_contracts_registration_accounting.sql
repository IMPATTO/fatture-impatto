alter table public.apartment_contracts
  add column if not exists registration_document_url text null,
  add column if not exists registration_payment_date date null,
  add column if not exists registration_paid_amount numeric(12,2) null,
  add column if not exists registration_payment_method text null,
  add column if not exists registration_note text null,
  add column if not exists registration_needs_accounting boolean not null default false,
  add column if not exists registration_contabilita_documento_id uuid null references public.contabilita_documenti(id) on delete set null,
  add column if not exists registration_accounting_sync_status text null default 'solo_gestionale',
  add column if not exists registration_accounting_sync_error text null;

do $$
begin
  if not exists (
    select 1 from pg_constraint where conname = 'apartment_contracts_registration_payment_method_check'
  ) then
    alter table public.apartment_contracts
      add constraint apartment_contracts_registration_payment_method_check
      check (
        registration_payment_method is null
        or registration_payment_method in ('contanti', 'bonifico', 'carta', 'pos', 'addebito', 'altro')
      );
  end if;

  if not exists (
    select 1 from pg_constraint where conname = 'apartment_contracts_registration_sync_status_check'
  ) then
    alter table public.apartment_contracts
      add constraint apartment_contracts_registration_sync_status_check
      check (
        registration_accounting_sync_status is null
        or registration_accounting_sync_status in ('solo_gestionale', 'da_contabilizzare', 'contabilizzato', 'errore_sync')
      );
  end if;
end $$;

create index if not exists apartment_contracts_registration_doc_idx
  on public.apartment_contracts (registration_contabilita_documento_id);
