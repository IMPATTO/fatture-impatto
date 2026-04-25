alter table public.agent_todos
add column if not exists created_by uuid,
add column if not exists assigned_to text[] default '{}'::text[],
add column if not exists is_private boolean default false,
add column if not exists visible_to text[] default '{}'::text[];

create or replace function public.agent_todo_actor_labels()
returns text[]
language sql
stable
as $$
  with src as (
    select lower(
      concat_ws(
        ' ',
        coalesce(auth.jwt() ->> 'email', ''),
        coalesce(auth.jwt() -> 'user_metadata' ->> 'full_name', ''),
        coalesce(auth.jwt() -> 'user_metadata' ->> 'name', ''),
        coalesce(auth.jwt() -> 'app_metadata' ->> 'full_name', '')
      )
    ) as raw
  )
  select array_remove(array[
    case when raw like '%marco%' then 'Marco' end,
    case when raw like '%veronica%' then 'Veronica' end,
    case when raw like '%jessica%' then 'Jessica' end,
    case when raw like '%serena%' then 'Serena' end,
    case when raw like '%susanna%' then 'Susanna' end,
    case when raw like '%victor%' or raw like '%vanessa%' then 'Victor&Vanessa' end
  ], null)
  from src;
$$;

create or replace function public.agent_todo_is_privileged()
returns boolean
language sql
stable
as $$
  select public.agent_todo_actor_labels() && array['Marco', 'Veronica']::text[];
$$;

create or replace function public.agent_todo_is_victor_vanessa()
returns boolean
language sql
stable
as $$
  select public.agent_todo_actor_labels() @> array['Victor&Vanessa']::text[];
$$;

create or replace function public.agent_todo_can_view(todo_assigned_to text[], todo_visible_to text[], todo_is_private boolean)
returns boolean
language sql
stable
as $$
  with actor as (
    select public.agent_todo_actor_labels() as labels
  )
  select case
    when auth.uid() is null then false
    when public.agent_todo_is_victor_vanessa() then coalesce(todo_assigned_to, '{}'::text[]) @> array['Victor&Vanessa']::text[]
    when public.agent_todo_is_privileged() then true
    when todo_is_private then coalesce(todo_assigned_to, '{}'::text[]) && (select labels from actor)
    when coalesce(array_length(todo_visible_to, 1), 0) = 0 then true
    else coalesce(todo_visible_to, '{}'::text[]) && (select labels from actor)
      or coalesce(todo_assigned_to, '{}'::text[]) && (select labels from actor)
  end;
$$;

create or replace function public.agent_todo_can_write(todo_assigned_to text[], todo_visible_to text[], todo_is_private boolean, todo_created_by uuid)
returns boolean
language sql
stable
as $$
  select case
    when auth.uid() is null then false
    when public.agent_todo_is_victor_vanessa() then
      coalesce(todo_assigned_to, '{}'::text[]) <@ array['Victor&Vanessa']::text[]
      and coalesce(todo_visible_to, '{}'::text[]) <@ array['Victor&Vanessa']::text[]
      and (todo_created_by is null or todo_created_by = auth.uid())
    else true
  end;
$$;

alter table public.agent_todos enable row level security;

drop policy if exists "authenticated users can manage agent_todos" on public.agent_todos;
drop policy if exists "agent_todos_select_visible" on public.agent_todos;
drop policy if exists "agent_todos_insert_guarded" on public.agent_todos;
drop policy if exists "agent_todos_update_guarded" on public.agent_todos;
drop policy if exists "agent_todos_delete_guarded" on public.agent_todos;

create policy "agent_todos_select_visible"
on public.agent_todos
for select
to authenticated
using (
  public.agent_todo_can_view(assigned_to, visible_to, is_private)
);

create policy "agent_todos_insert_guarded"
on public.agent_todos
for insert
to authenticated
with check (
  public.agent_todo_can_write(assigned_to, visible_to, is_private, created_by)
);

create policy "agent_todos_update_guarded"
on public.agent_todos
for update
to authenticated
using (
  public.agent_todo_can_view(assigned_to, visible_to, is_private)
)
with check (
  public.agent_todo_can_write(assigned_to, visible_to, is_private, created_by)
);

create policy "agent_todos_delete_guarded"
on public.agent_todos
for delete
to authenticated
using (
  public.agent_todo_can_view(assigned_to, visible_to, is_private)
);
