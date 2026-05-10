-- README
-- Scopo: schema minimo Fase 2 per calendario PMS read-only basato su prenotazioni Beds24.
-- Crea: apartment_units, channel_property_mappings, bookings, sync_jobs, sync_state.
-- Crea anche la vista v_calendar_occupancy per esplodere le notti occupate da bookings.
-- Non crea: calendar_days, write_intents, webhook tables, mutation flows o colonne su tabelle esistenti.
-- Non modifica: apartments, apartment_channel_mappings, ospiti_check_in, alloggiati_*, rm_*, fatturazione, portale o frontend.
-- Rollback manuale: DROP VIEW public.v_calendar_occupancy; poi DROP TABLE public.sync_state;
-- DROP TABLE public.sync_jobs; DROP TABLE public.bookings;
-- DROP TABLE public.channel_property_mappings; DROP TABLE public.apartment_units;

create extension if not exists pgcrypto;

create table if not exists public.apartment_units (
  id uuid primary key default gen_random_uuid(),
  apartment_id uuid not null references public.apartments(id) on delete restrict,
  unit_label text not null,
  room_type_label text,
  beds24_room_id text,
  beds24_unit_index integer,
  max_guests integer,
  active boolean not null default true,
  notes text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (apartment_id, unit_label)
);

create index if not exists idx_apartment_units_apartment_id
  on public.apartment_units (apartment_id);

create index if not exists idx_apartment_units_beds24_room_unit
  on public.apartment_units (beds24_room_id, beds24_unit_index)
  where beds24_room_id is not null;

create index if not exists idx_apartment_units_active
  on public.apartment_units (active)
  where active = true;

create table if not exists public.channel_property_mappings (
  id uuid primary key default gen_random_uuid(),
  apartment_id uuid not null references public.apartments(id) on delete restrict,
  channel text not null,
  external_property_id text not null,
  external_room_id text not null,
  external_room_qty integer,
  external_room_name text,
  is_primary boolean not null default false,
  active boolean not null default true,
  notes text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (channel, external_property_id, external_room_id)
);

create index if not exists idx_channel_property_mappings_apartment_id
  on public.channel_property_mappings (apartment_id);

create table if not exists public.bookings (
  id uuid primary key default gen_random_uuid(),
  beds24_booking_id text not null unique,
  beds24_property_id text not null,
  beds24_room_id text not null,
  beds24_unit_id integer not null default 1,
  apartment_id uuid references public.apartments(id) on delete set null,
  apartment_unit_id uuid references public.apartment_units(id) on delete set null,
  guest_first_name text,
  guest_last_name text,
  guest_email text,
  guest_phone text,
  num_adults integer,
  num_children integer,
  check_in date not null,
  check_out date not null,
  status text not null,
  channel text,
  channel_normalized text,
  total_price numeric(10,2),
  currency text default 'EUR',
  source text not null default 'beds24',
  source_updated_at timestamptz not null,
  source_created_at timestamptz,
  synced_at timestamptz not null default now(),
  raw_payload jsonb not null,
  created_at timestamptz not null default now(),
  check (check_out > check_in)
);

create index if not exists idx_bookings_apartment_unit_dates
  on public.bookings (apartment_unit_id, check_in, check_out);

create index if not exists idx_bookings_apartment_dates
  on public.bookings (apartment_id, check_in, check_out);

create index if not exists idx_bookings_dates
  on public.bookings (check_in, check_out);

create index if not exists idx_bookings_status
  on public.bookings (status);

create index if not exists idx_bookings_beds24_room_unit
  on public.bookings (beds24_property_id, beds24_room_id, beds24_unit_id);

create index if not exists idx_bookings_source_updated_at
  on public.bookings (source_updated_at);

create table if not exists public.sync_jobs (
  id uuid primary key default gen_random_uuid(),
  scope text not null,
  trigger text not null,
  target_apartment_id uuid references public.apartments(id),
  target_external_property_id text,
  date_range_start date,
  date_range_end date,
  status text not null,
  rows_read integer default 0,
  rows_upserted integer default 0,
  rows_skipped_stale integer default 0,
  rows_orphaned integer default 0,
  error_message text,
  started_at timestamptz not null default now(),
  finished_at timestamptz
);

create index if not exists idx_sync_jobs_status_started_at
  on public.sync_jobs (status, started_at desc);

create index if not exists idx_sync_jobs_scope_started_at
  on public.sync_jobs (scope, started_at desc);

create table if not exists public.sync_state (
  apartment_id uuid not null references public.apartments(id) on delete restrict,
  scope text not null,
  last_full_sync_at timestamptz,
  last_incremental_sync_at timestamptz,
  last_webhook_at timestamptz,
  last_success_at timestamptz,
  last_error text,
  last_error_at timestamptz,
  consecutive_errors integer not null default 0,
  updated_at timestamptz not null default now(),
  primary key (apartment_id, scope)
);

create or replace view public.v_calendar_occupancy as
select
  b.apartment_unit_id,
  b.apartment_id,
  gs.occupancy_date as date,
  b.id as booking_id,
  b.beds24_booking_id,
  b.status,
  b.channel_normalized,
  b.guest_last_name,
  b.check_in,
  b.check_out,
  (gs.occupancy_date = b.check_in) as is_check_in,
  (gs.occupancy_date = (b.check_out - 1)) as is_last_night
from public.bookings b
cross join lateral (
  select generate_series(
    b.check_in::timestamp,
    (b.check_out - 1)::timestamp,
    interval '1 day'
  )::date as occupancy_date
) gs
where b.status in ('confirmed', 'new', 'request', 'black')
  and b.apartment_unit_id is not null;
