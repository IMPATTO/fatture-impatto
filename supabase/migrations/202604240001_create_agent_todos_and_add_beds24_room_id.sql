create table if not exists public.agent_todos (
  id uuid primary key default gen_random_uuid(),
  task text not null,
  priority text default 'media' check (priority in ('alta', 'media', 'bassa')),
  done boolean default false,
  due_date date,
  apartment text,
  source text default 'manual',
  created_at timestamptz default now()
);

alter table public.agent_todos enable row level security;

drop policy if exists "authenticated users can manage agent_todos" on public.agent_todos;
create policy "authenticated users can manage agent_todos"
on public.agent_todos
for all
to authenticated
using (true)
with check (true);

alter table public.apartments
add column if not exists beds24_room_id text;
