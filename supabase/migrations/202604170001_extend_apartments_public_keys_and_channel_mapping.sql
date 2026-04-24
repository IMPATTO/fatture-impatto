create extension if not exists pgcrypto;

alter table public.apartments
  add column if not exists codice_interno text,
  add column if not exists public_checkin_key text,
  add column if not exists attivo boolean not null default true,
  add column if not exists struttura_nome text;

create unique index if not exists idx_apartments_public_checkin_key_unique
  on public.apartments (lower(public_checkin_key))
  where public_checkin_key is not null;

create table if not exists public.apartment_channel_mappings (
  id uuid primary key default gen_random_uuid(),
  apartment_id uuid not null references public.apartments(id) on delete cascade,
  channel text not null,
  external_name text,
  external_unit_id text,
  direct_link_override text,
  active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists idx_apartment_channel_mappings_unique
  on public.apartment_channel_mappings (
    apartment_id,
    lower(channel),
    coalesce(lower(external_unit_id), ''),
    coalesce(lower(external_name), '')
  );

create index if not exists idx_apartment_channel_mappings_apartment_id
  on public.apartment_channel_mappings (apartment_id);

create index if not exists idx_apartment_channel_mappings_channel
  on public.apartment_channel_mappings (lower(channel));

create or replace function public.set_apartment_channel_mappings_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists trg_apartment_channel_mappings_updated_at on public.apartment_channel_mappings;
create trigger trg_apartment_channel_mappings_updated_at
before update on public.apartment_channel_mappings
for each row
execute function public.set_apartment_channel_mappings_updated_at();

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'apartment_alloggiati_apartment_id_key'
      and conrelid = 'public.apartment_alloggiati'::regclass
  ) then
    alter table public.apartment_alloggiati
      add constraint apartment_alloggiati_apartment_id_key unique (apartment_id);
  end if;
end $$;
