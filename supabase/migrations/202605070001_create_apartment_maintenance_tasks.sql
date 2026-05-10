create table if not exists public.apartment_maintenance_tasks (
  id uuid primary key default gen_random_uuid(),
  apartment_id uuid not null references public.apartments(id) on delete cascade,
  title text not null,
  description text null,
  status text not null default 'da_fare',
  priority text null,
  assigned_to text null,
  due_date date null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  created_by uuid null default auth.uid(),
  completed_at timestamptz null,
  constraint apartment_maintenance_tasks_status_check
    check (status in ('da_fare', 'in_corso', 'fatto', 'annullato')),
  constraint apartment_maintenance_tasks_priority_check
    check (priority is null or priority in ('bassa', 'media', 'alta', 'urgente'))
);

create index if not exists apartment_maintenance_tasks_apartment_idx
  on public.apartment_maintenance_tasks (apartment_id, status, due_date);

create index if not exists apartment_maintenance_tasks_status_idx
  on public.apartment_maintenance_tasks (status, priority, created_at desc);

revoke all on table public.apartment_maintenance_tasks from public, anon;
grant select, insert, update, delete on table public.apartment_maintenance_tasks to authenticated;

alter table public.apartment_maintenance_tasks enable row level security;

drop policy if exists "apartment_maintenance_tasks_authenticated_select" on public.apartment_maintenance_tasks;
create policy "apartment_maintenance_tasks_authenticated_select"
on public.apartment_maintenance_tasks
for select
to authenticated
using (true);

drop policy if exists "apartment_maintenance_tasks_authenticated_insert" on public.apartment_maintenance_tasks;
create policy "apartment_maintenance_tasks_authenticated_insert"
on public.apartment_maintenance_tasks
for insert
to authenticated
with check (true);

drop policy if exists "apartment_maintenance_tasks_authenticated_update" on public.apartment_maintenance_tasks;
create policy "apartment_maintenance_tasks_authenticated_update"
on public.apartment_maintenance_tasks
for update
to authenticated
using (true)
with check (true);

drop policy if exists "apartment_maintenance_tasks_authenticated_delete" on public.apartment_maintenance_tasks;
create policy "apartment_maintenance_tasks_authenticated_delete"
on public.apartment_maintenance_tasks
for delete
to authenticated
using (true);
