create table if not exists public.beds24_inventory_changes (
  id uuid primary key default gen_random_uuid(),
  apartment_id uuid,
  property_id text,
  room_id text,
  date_from date,
  date_to date,
  before_snapshot jsonb,
  write_payload jsonb,
  after_snapshot jsonb,
  rollback_payload jsonb,
  status text,
  created_by text,
  created_at timestamptz not null default now()
);

alter table public.beds24_inventory_changes enable row level security;

drop policy if exists "service role only" on public.beds24_inventory_changes;

create policy "service role only"
on public.beds24_inventory_changes
for all
to authenticated
using (false)
with check (false);
