create table if not exists public.partner_booking_requests (
  id uuid primary key default gen_random_uuid(),
  partner text not null check (partner in ('luca')),
  scope text not null check (scope in ('residence', 'beds24')),
  apartment_ref text not null,
  apartment_label text not null,
  beds24_room_id text,
  customer_name text not null,
  checkin date not null,
  checkout date not null,
  pax integer not null check (pax > 0),
  notes text,
  status text not null default 'pending'
    check (status in ('pending', 'confirmed', 'rejected', 'cancelled')),
  sync_status text not null default 'pending'
    check (sync_status in ('pending', 'applied', 'error', 'not_required')),
  approved_at timestamptz,
  approved_by text,
  synced_at timestamptz,
  sync_error text,
  residence_booking_id uuid,
  beds24_booking_id text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (checkout > checkin)
);

create index if not exists idx_partner_booking_requests_partner
  on public.partner_booking_requests (partner);

create index if not exists idx_partner_booking_requests_status
  on public.partner_booking_requests (status);

create index if not exists idx_partner_booking_requests_scope
  on public.partner_booking_requests (scope);

create index if not exists idx_partner_booking_requests_dates
  on public.partner_booking_requests (checkin, checkout);

create or replace function public.set_partner_booking_requests_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists trg_partner_booking_requests_updated_at on public.partner_booking_requests;
create trigger trg_partner_booking_requests_updated_at
before update on public.partner_booking_requests
for each row execute function public.set_partner_booking_requests_updated_at();

alter table public.partner_booking_requests enable row level security;

revoke all on public.partner_booking_requests from anon, authenticated;
grant select, insert, update, delete on public.partner_booking_requests to service_role;
