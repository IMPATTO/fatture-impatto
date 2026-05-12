-- ============================================================
-- Calendar days: cache prezzi/min-stay/availability da Beds24
-- ============================================================
-- Una riga per (apartment_unit_id, date). Aggiornata da
-- sync-beds24-calendar-background Netlify function al click di "Aggiorna".
-- ============================================================

create table if not exists public.calendar_days (
  id uuid primary key default gen_random_uuid(),
  apartment_unit_id uuid not null references public.apartment_units(id) on delete cascade,
  date date not null,
  price numeric(10,2),
  min_stay integer,
  available boolean,
  closed boolean default false,
  source_updated_at timestamptz default now(),
  raw_payload jsonb,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create unique index if not exists calendar_days_unit_date_idx
  on public.calendar_days (apartment_unit_id, date);

create index if not exists calendar_days_date_idx
  on public.calendar_days (date);

alter table public.calendar_days enable row level security;

drop policy if exists "authenticated_read_calendar_days" on public.calendar_days;
create policy "authenticated_read_calendar_days"
  on public.calendar_days
  for select
  to authenticated
  using (true);

grant select on public.calendar_days to authenticated;

comment on table public.calendar_days is
  'Cache prezzi/min-stay/availability sync da Beds24 /offers endpoint. Aggiornata da sync-beds24-calendar-background.';
