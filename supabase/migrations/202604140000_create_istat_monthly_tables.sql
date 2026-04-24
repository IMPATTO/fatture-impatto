create extension if not exists pgcrypto;

create table if not exists public.apartment_istat_config (
  id uuid primary key default gen_random_uuid(),
  apartment_id uuid not null references public.apartments(id) on delete cascade,
  attivo boolean not null default true,
  regione text not null,
  sistema text not null,
  portal_url text null,
  codice_struttura text null,
  auth_type text null,
  username text null,
  password_encrypted text null,
  export_format text null,
  requires_open_close boolean not null default false,
  supports_file_import boolean not null default false,
  supports_webservice boolean not null default false,
  deadline_rule text null,
  note text null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (apartment_id),
  constraint apartment_istat_config_regione_check check (
    regione in ('emilia-romagna', 'marche', 'veneto', 'valle-daosta')
  ),
  constraint apartment_istat_config_sistema_check check (
    sistema in ('ross1000', 'istrice_ross1000', 'vit_albergatori')
  )
);

create table if not exists public.istat_invii (
  id uuid primary key default gen_random_uuid(),
  apartment_id uuid not null references public.apartments(id) on delete cascade,
  config_id uuid null references public.apartment_istat_config(id) on delete set null,
  mese_riferimento date not null,
  regione text not null,
  sistema text not null,
  modalita text not null,
  esito text not null,
  payload_json jsonb null,
  file_generato_path text null,
  risposta_portale text null,
  errore_dettaglio text null,
  inviato_da text null,
  created_at timestamptz not null default now(),
  constraint istat_invii_modalita_check check (
    modalita in ('export', 'send', 'manual_confirmed')
  ),
  constraint istat_invii_esito_check check (
    esito in ('EXPORT_OK', 'SEND_OK', 'PARZIALE', 'ERRORE', 'DRAFT')
  )
);

alter table public.apartment_istat_config
  add column if not exists attivo boolean not null default true,
  add column if not exists regione text,
  add column if not exists sistema text,
  add column if not exists portal_url text,
  add column if not exists codice_struttura text,
  add column if not exists auth_type text,
  add column if not exists username text,
  add column if not exists password_encrypted text,
  add column if not exists export_format text,
  add column if not exists requires_open_close boolean not null default false,
  add column if not exists supports_file_import boolean not null default false,
  add column if not exists supports_webservice boolean not null default false,
  add column if not exists deadline_rule text,
  add column if not exists note text,
  add column if not exists created_at timestamptz not null default now(),
  add column if not exists updated_at timestamptz not null default now();

alter table public.istat_invii
  add column if not exists config_id uuid,
  add column if not exists mese_riferimento date,
  add column if not exists regione text,
  add column if not exists sistema text,
  add column if not exists modalita text,
  add column if not exists payload_json jsonb,
  add column if not exists file_generato_path text,
  add column if not exists risposta_portale text;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'apartment_istat_config_apartment_id_key'
      and conrelid = 'public.apartment_istat_config'::regclass
  ) then
    alter table public.apartment_istat_config
      add constraint apartment_istat_config_apartment_id_key unique (apartment_id);
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'istat_invii_config_id_fkey'
      and conrelid = 'public.istat_invii'::regclass
  ) then
    alter table public.istat_invii
      add constraint istat_invii_config_id_fkey
      foreign key (config_id) references public.apartment_istat_config(id) on delete set null;
  end if;
end $$;

create index if not exists idx_apartment_istat_config_apartment_id
  on public.apartment_istat_config(apartment_id);

create index if not exists idx_istat_invii_apartment_month
  on public.istat_invii(apartment_id, mese_riferimento desc);

create index if not exists idx_istat_invii_esito
  on public.istat_invii(esito);

create index if not exists idx_istat_invii_config_id
  on public.istat_invii(config_id);

create or replace function public.set_apartment_istat_config_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists trg_apartment_istat_config_updated_at on public.apartment_istat_config;
create trigger trg_apartment_istat_config_updated_at
before update on public.apartment_istat_config
for each row
execute function public.set_apartment_istat_config_updated_at();

alter table public.apartment_istat_config enable row level security;
alter table public.istat_invii enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'apartment_istat_config'
      and policyname = 'apartment_istat_config_authenticated_select'
  ) then
    create policy apartment_istat_config_authenticated_select
      on public.apartment_istat_config
      for select
      to authenticated
      using (true);
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'apartment_istat_config'
      and policyname = 'apartment_istat_config_authenticated_insert'
  ) then
    create policy apartment_istat_config_authenticated_insert
      on public.apartment_istat_config
      for insert
      to authenticated
      with check (true);
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'apartment_istat_config'
      and policyname = 'apartment_istat_config_authenticated_update'
  ) then
    create policy apartment_istat_config_authenticated_update
      on public.apartment_istat_config
      for update
      to authenticated
      using (true)
      with check (true);
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'istat_invii'
      and policyname = 'istat_invii_authenticated_select'
  ) then
    create policy istat_invii_authenticated_select
      on public.istat_invii
      for select
      to authenticated
      using (true);
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'istat_invii'
      and policyname = 'istat_invii_authenticated_insert'
  ) then
    create policy istat_invii_authenticated_insert
      on public.istat_invii
      for insert
      to authenticated
      with check (true);
  end if;
end $$;

comment on table public.apartment_istat_config is
'Configurazione ISTAT per appartamento. Lato function si usa service role; le policy authenticated servono al backoffice esistente.';

comment on table public.istat_invii is
'Log dedicato dei preview/export/send ISTAT mensili per appartamento.';
