create extension if not exists pgcrypto;

create table if not exists public.owners (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  nome_visualizzato text not null,
  nome text null,
  cognome text null,
  email text null,
  telefono text null,
  codice_fiscale text null,
  piva text null,
  indirizzo text null,
  iban text null,
  note text null,
  attivo boolean not null default true
);

create table if not exists public.apartment_owner_links (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  apartment_id uuid not null references public.apartments(id) on delete cascade,
  owner_id uuid not null references public.owners(id) on delete cascade,
  ruolo text not null default 'proprietario',
  quota_percentuale numeric(6,2) null,
  is_primary boolean not null default false,
  sort_order integer not null default 0,
  note text null,
  constraint apartment_owner_links_ruolo_check
    check (ruolo in ('proprietario', 'co_proprietario', 'referente', 'amministratore'))
);

create unique index if not exists apartment_owner_links_unique_apartment_owner_idx
  on public.apartment_owner_links (apartment_id, owner_id);

create index if not exists apartment_owner_links_apartment_idx
  on public.apartment_owner_links (apartment_id, sort_order);

create table if not exists public.apartment_contracts (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  apartment_id uuid not null references public.apartments(id) on delete cascade,
  owner_id uuid null references public.owners(id) on delete set null,
  titolo text not null default 'Contratto gestione',
  contract_type text not null default 'gestione',
  status text not null default 'bozza',
  decorrenza date null,
  scadenza date null,
  rinnovo_automatico boolean not null default false,
  canone_fisso numeric(12,2) null,
  percentuale_gestione numeric(6,2) null,
  deposito_cauzionale numeric(12,2) null,
  documento_url text null,
  note text null,
  constraint apartment_contracts_type_check
    check (contract_type in ('gestione', 'locazione', 'comodato', 'mandato', 'altro')),
  constraint apartment_contracts_status_check
    check (status in ('bozza', 'attivo', 'scaduto', 'cessato'))
);

create index if not exists apartment_contracts_apartment_idx
  on public.apartment_contracts (apartment_id, decorrenza desc nulls last);

create table if not exists public.apartment_owner_payments (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  apartment_id uuid not null references public.apartments(id) on delete cascade,
  owner_id uuid null references public.owners(id) on delete set null,
  contract_id uuid null references public.apartment_contracts(id) on delete set null,
  payment_date date null,
  competence_month date null,
  amount numeric(12,2) not null default 0,
  payment_type text not null default 'bonifico_proprietario',
  status text not null default 'da_pagare',
  reference text null,
  document_url text null,
  note text null,
  constraint apartment_owner_payments_type_check
    check (payment_type in ('bonifico_proprietario', 'rimborso_spese', 'canone', 'anticipo', 'altro')),
  constraint apartment_owner_payments_status_check
    check (status in ('da_pagare', 'pagato', 'annullato'))
);

create index if not exists apartment_owner_payments_apartment_idx
  on public.apartment_owner_payments (apartment_id, payment_date desc nulls last);

create or replace function public.set_updated_at_timestamp()
returns trigger
language plpgsql
as $$
begin
  new.updated_at := now();
  return new;
end;
$$;

drop trigger if exists trg_owners_updated_at on public.owners;
create trigger trg_owners_updated_at
before update on public.owners
for each row execute function public.set_updated_at_timestamp();

drop trigger if exists trg_apartment_owner_links_updated_at on public.apartment_owner_links;
create trigger trg_apartment_owner_links_updated_at
before update on public.apartment_owner_links
for each row execute function public.set_updated_at_timestamp();

drop trigger if exists trg_apartment_contracts_updated_at on public.apartment_contracts;
create trigger trg_apartment_contracts_updated_at
before update on public.apartment_contracts
for each row execute function public.set_updated_at_timestamp();

drop trigger if exists trg_apartment_owner_payments_updated_at on public.apartment_owner_payments;
create trigger trg_apartment_owner_payments_updated_at
before update on public.apartment_owner_payments
for each row execute function public.set_updated_at_timestamp();

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

grant execute on function public.is_internal_staff() to authenticated, anon, service_role;

alter table public.owners enable row level security;
alter table public.apartment_owner_links enable row level security;
alter table public.apartment_contracts enable row level security;
alter table public.apartment_owner_payments enable row level security;

grant select, insert, update, delete on public.owners to authenticated;
grant select, insert, update, delete on public.apartment_owner_links to authenticated;
grant select, insert, update, delete on public.apartment_contracts to authenticated;
grant select, insert, update, delete on public.apartment_owner_payments to authenticated;

drop policy if exists owners_authenticated_all on public.owners;
drop policy if exists owners_internal_staff_all on public.owners;
create policy owners_internal_staff_all
on public.owners
for all
to authenticated
using (public.is_internal_staff())
with check (public.is_internal_staff());

drop policy if exists apartment_owner_links_authenticated_all on public.apartment_owner_links;
drop policy if exists apartment_owner_links_internal_staff_all on public.apartment_owner_links;
create policy apartment_owner_links_internal_staff_all
on public.apartment_owner_links
for all
to authenticated
using (public.is_internal_staff())
with check (public.is_internal_staff());

drop policy if exists apartment_contracts_authenticated_all on public.apartment_contracts;
drop policy if exists apartment_contracts_internal_staff_all on public.apartment_contracts;
create policy apartment_contracts_internal_staff_all
on public.apartment_contracts
for all
to authenticated
using (public.is_internal_staff())
with check (public.is_internal_staff());

drop policy if exists apartment_owner_payments_authenticated_all on public.apartment_owner_payments;
drop policy if exists apartment_owner_payments_internal_staff_all on public.apartment_owner_payments;
create policy apartment_owner_payments_internal_staff_all
on public.apartment_owner_payments
for all
to authenticated
using (public.is_internal_staff())
with check (public.is_internal_staff());
