create table if not exists public.contabilita_bollette (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  contabilita_documento_id uuid not null unique references public.contabilita_documenti(id) on delete cascade,
  apartment_id uuid null references public.apartments(id) on delete set null,
  utility_type text null,
  bill_number text null,
  issue_date date null,
  due_date date null,
  period_start date null,
  period_end date null,
  amount_total numeric(12,2) null,
  payment_status text not null default 'non_pagata',
  accounting_status text not null default 'da_registrare',
  reimbursement_method text null,
  reimbursed_at timestamptz null,
  linked_movement_id uuid null,
  notes text null,
  constraint contabilita_bollette_utility_type_check
    check (utility_type is null or utility_type in ('luce', 'acqua', 'gas', 'tari', 'internet', 'altro')),
  constraint contabilita_bollette_payment_status_check
    check (payment_status in ('non_pagata', 'pagata_proprietario', 'da_rimborsare', 'rimborsata', 'pagata_direttamente', 'bonifico_effettuato')),
  constraint contabilita_bollette_accounting_status_check
    check (accounting_status in ('da_registrare', 'registrata'))
);

create index if not exists contabilita_bollette_apartment_idx
  on public.contabilita_bollette (apartment_id);

create index if not exists contabilita_bollette_payment_status_idx
  on public.contabilita_bollette (payment_status);

create index if not exists contabilita_bollette_utility_type_idx
  on public.contabilita_bollette (utility_type);

create index if not exists contabilita_bollette_accounting_status_idx
  on public.contabilita_bollette (accounting_status);

alter table public.contabilita_bollette enable row level security;
alter table public.contabilita_bollette force row level security;

revoke all on public.contabilita_bollette from public;
revoke all on public.contabilita_bollette from anon;
revoke all on public.contabilita_bollette from authenticated;

grant select, insert, update, delete on public.contabilita_bollette to authenticated;

create policy contabilita_bollette_accounting_only_select
  on public.contabilita_bollette
  for select
  to authenticated
  using ((auth.jwt() ->> 'email') = 'contabilita@illupoaffitta.com');

create policy contabilita_bollette_accounting_only_insert
  on public.contabilita_bollette
  for insert
  to authenticated
  with check ((auth.jwt() ->> 'email') = 'contabilita@illupoaffitta.com');

create policy contabilita_bollette_accounting_only_update
  on public.contabilita_bollette
  for update
  to authenticated
  using ((auth.jwt() ->> 'email') = 'contabilita@illupoaffitta.com')
  with check ((auth.jwt() ->> 'email') = 'contabilita@illupoaffitta.com');

create policy contabilita_bollette_accounting_only_delete
  on public.contabilita_bollette
  for delete
  to authenticated
  using ((auth.jwt() ->> 'email') = 'contabilita@illupoaffitta.com');
