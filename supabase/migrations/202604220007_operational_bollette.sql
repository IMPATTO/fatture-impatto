alter table public.contabilita_bollette
  add column if not exists is_general boolean not null default false;

do $$
begin
  if exists (
    select 1
    from pg_constraint
    where conname = 'contabilita_bollette_payment_status_check'
  ) then
    alter table public.contabilita_bollette
      drop constraint contabilita_bollette_payment_status_check;
  end if;
end $$;

alter table public.contabilita_bollette
  add constraint contabilita_bollette_payment_status_check
  check (payment_status in (
    'non_pagata',
    'pagata',
    'pagata_proprietario',
    'da_rimborsare',
    'rimborsata',
    'pagata_direttamente',
    'bonifico_effettuato'
  ));

create table if not exists public.contabilita_bollette_allocazioni (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  contabilita_bolletta_id uuid not null references public.contabilita_bollette(id) on delete cascade,
  apartment_id uuid not null references public.apartments(id) on delete cascade,
  notes text null,
  constraint contabilita_bollette_allocazioni_unique unique (contabilita_bolletta_id, apartment_id)
);

create index if not exists contabilita_bollette_allocazioni_bolletta_idx
  on public.contabilita_bollette_allocazioni (contabilita_bolletta_id);

create index if not exists contabilita_bollette_allocazioni_apartment_idx
  on public.contabilita_bollette_allocazioni (apartment_id);

alter table public.contabilita_bollette_allocazioni enable row level security;
alter table public.contabilita_bollette_allocazioni force row level security;

revoke all on public.contabilita_bollette_allocazioni from public;
revoke all on public.contabilita_bollette_allocazioni from anon;
revoke all on public.contabilita_bollette_allocazioni from authenticated;

grant select, insert, update, delete on public.contabilita_bollette_allocazioni to authenticated;

create policy contabilita_bollette_allocazioni_accounting_only_select
  on public.contabilita_bollette_allocazioni
  for select
  to authenticated
  using ((auth.jwt() ->> 'email') = 'contabilita@illupoaffitta.com');

create policy contabilita_bollette_allocazioni_accounting_only_insert
  on public.contabilita_bollette_allocazioni
  for insert
  to authenticated
  with check ((auth.jwt() ->> 'email') = 'contabilita@illupoaffitta.com');

create policy contabilita_bollette_allocazioni_accounting_only_update
  on public.contabilita_bollette_allocazioni
  for update
  to authenticated
  using ((auth.jwt() ->> 'email') = 'contabilita@illupoaffitta.com')
  with check ((auth.jwt() ->> 'email') = 'contabilita@illupoaffitta.com');

create policy contabilita_bollette_allocazioni_accounting_only_delete
  on public.contabilita_bollette_allocazioni
  for delete
  to authenticated
  using ((auth.jwt() ->> 'email') = 'contabilita@illupoaffitta.com');
