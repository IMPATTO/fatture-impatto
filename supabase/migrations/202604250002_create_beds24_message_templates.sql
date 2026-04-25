create extension if not exists pgcrypto;

create table if not exists public.beds24_message_templates (
  id uuid primary key default gen_random_uuid(),
  apartment_id uuid not null references public.apartments(id) on delete cascade,
  channel text not null check (channel in ('booking', 'airbnb', 'all')),
  message_type text not null,
  subject text,
  body_it text not null default '',
  body_en text,
  timing_note text,
  enabled boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists idx_beds24_message_templates_unique
  on public.beds24_message_templates (apartment_id, channel, message_type);

create index if not exists idx_beds24_message_templates_apartment
  on public.beds24_message_templates (apartment_id);

create index if not exists idx_beds24_message_templates_channel
  on public.beds24_message_templates (channel);

create or replace function public.set_beds24_message_templates_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists trg_beds24_message_templates_updated_at on public.beds24_message_templates;
create trigger trg_beds24_message_templates_updated_at
before update on public.beds24_message_templates
for each row
execute function public.set_beds24_message_templates_updated_at();

alter table public.beds24_message_templates enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'beds24_message_templates'
      and policyname = 'beds24_message_templates_backoffice_select'
  ) then
    create policy beds24_message_templates_backoffice_select
      on public.beds24_message_templates
      for select
      to authenticated
      using (
        lower(coalesce(auth.jwt() ->> 'email', '')) in (
          'fatturazione@illupoaffitta.com',
          'contabilita@illupoaffitta.com'
        )
      );
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'beds24_message_templates'
      and policyname = 'beds24_message_templates_backoffice_insert'
  ) then
    create policy beds24_message_templates_backoffice_insert
      on public.beds24_message_templates
      for insert
      to authenticated
      with check (
        lower(coalesce(auth.jwt() ->> 'email', '')) in (
          'fatturazione@illupoaffitta.com',
          'contabilita@illupoaffitta.com'
        )
      );
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'beds24_message_templates'
      and policyname = 'beds24_message_templates_backoffice_update'
  ) then
    create policy beds24_message_templates_backoffice_update
      on public.beds24_message_templates
      for update
      to authenticated
      using (
        lower(coalesce(auth.jwt() ->> 'email', '')) in (
          'fatturazione@illupoaffitta.com',
          'contabilita@illupoaffitta.com'
        )
      )
      with check (
        lower(coalesce(auth.jwt() ->> 'email', '')) in (
          'fatturazione@illupoaffitta.com',
          'contabilita@illupoaffitta.com'
        )
      );
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'beds24_message_templates'
      and policyname = 'beds24_message_templates_backoffice_delete'
  ) then
    create policy beds24_message_templates_backoffice_delete
      on public.beds24_message_templates
      for delete
      to authenticated
      using (
        lower(coalesce(auth.jwt() ->> 'email', '')) in (
          'fatturazione@illupoaffitta.com',
          'contabilita@illupoaffitta.com'
        )
      );
  end if;
end
$$;
