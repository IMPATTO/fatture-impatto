begin;

create extension if not exists pgcrypto;

create table if not exists public.clienti (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  nome_visualizzato text not null,
  tipo_cliente text not null check (tipo_cliente in ('privato', 'azienda', 'professionista')),
  ragione_sociale text null,
  nome text null,
  cognome text null,
  localita text null,
  piva text null,
  codice_fiscale text null,
  email text null,
  privato_csv boolean null,
  pec text null,
  codice_destinatario text null,
  indirizzo text null,
  cap text null,
  citta text null,
  provincia text null,
  paese text null default 'Italia',
  note text null,
  attivo boolean not null default true,
  search_text text null
);

alter table public.clienti
  add column if not exists created_at timestamptz not null default now(),
  add column if not exists updated_at timestamptz not null default now(),
  add column if not exists nome_visualizzato text,
  add column if not exists tipo_cliente text,
  add column if not exists ragione_sociale text,
  add column if not exists nome text,
  add column if not exists cognome text,
  add column if not exists localita text,
  add column if not exists piva text,
  add column if not exists codice_fiscale text,
  add column if not exists email text,
  add column if not exists privato_csv boolean,
  add column if not exists pec text,
  add column if not exists codice_destinatario text,
  add column if not exists indirizzo text,
  add column if not exists cap text,
  add column if not exists citta text,
  add column if not exists provincia text,
  add column if not exists paese text default 'Italia',
  add column if not exists note text,
  add column if not exists attivo boolean not null default true,
  add column if not exists search_text text;

update public.clienti
set
  nome_visualizzato = coalesce(nullif(nome_visualizzato, ''), coalesce(ragione_sociale, concat_ws(' ', nome, cognome), 'Cliente')),
  tipo_cliente = coalesce(nullif(tipo_cliente, ''), 'privato'),
  paese = coalesce(nullif(paese, ''), 'Italia')
where nome_visualizzato is null
   or tipo_cliente is null
   or paese is null;

alter table public.clienti
  alter column nome_visualizzato set not null,
  alter column tipo_cliente set not null;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'clienti_tipo_cliente_check'
      and conrelid = 'public.clienti'::regclass
  ) then
    alter table public.clienti
      add constraint clienti_tipo_cliente_check
      check (tipo_cliente in ('privato', 'azienda', 'professionista'));
  end if;
end
$$;

create index if not exists clienti_nome_visualizzato_lower_idx
  on public.clienti (lower(nome_visualizzato));

create index if not exists clienti_ragione_sociale_lower_idx
  on public.clienti (lower(ragione_sociale));

create index if not exists clienti_piva_idx
  on public.clienti (piva);

create index if not exists clienti_codice_fiscale_idx
  on public.clienti (codice_fiscale);

comment on column public.clienti.localita is
'Campo raw dall''anagrafica clienti legacy CSV (colonna Località), conservato per matching e import futuri.';

comment on column public.clienti.privato_csv is
'Flag raw dall''anagrafica clienti legacy CSV (colonna Privato: Si/No), utile per import futuri e riconciliazione.';

create unique index if not exists clienti_piva_unique_not_null_idx
  on public.clienti (piva)
  where piva is not null and btrim(piva) <> '';

create unique index if not exists clienti_codice_fiscale_unique_not_null_idx
  on public.clienti (codice_fiscale)
  where codice_fiscale is not null and btrim(codice_fiscale) <> '';

create or replace function public.set_clienti_derived_fields()
returns trigger
language plpgsql
as $$
begin
  new.updated_at := now();
  new.paese := coalesce(nullif(new.paese, ''), 'Italia');
  new.search_text := trim(
    regexp_replace(
      lower(
        concat_ws(
          ' ',
          coalesce(new.nome_visualizzato, ''),
          coalesce(new.ragione_sociale, ''),
          coalesce(new.nome, ''),
          coalesce(new.cognome, ''),
          coalesce(new.localita, ''),
          coalesce(new.piva, ''),
          coalesce(new.codice_fiscale, ''),
          coalesce(new.email, '')
        )
      ),
      '\s+',
      ' ',
      'g'
    )
  );
  return new;
end;
$$;

drop trigger if exists trg_clienti_derived_fields on public.clienti;

create trigger trg_clienti_derived_fields
before insert or update on public.clienti
for each row
execute function public.set_clienti_derived_fields();

update public.clienti
set updated_at = now();

alter table public.clienti enable row level security;
alter table public.clienti force row level security;

revoke all on table public.clienti from public;
revoke all on table public.clienti from anon;
revoke all on table public.clienti from authenticated;

grant select, insert, update, delete on table public.clienti to authenticated;

drop policy if exists clienti_backoffice_select on public.clienti;
drop policy if exists clienti_backoffice_insert on public.clienti;
drop policy if exists clienti_backoffice_update on public.clienti;
drop policy if exists clienti_backoffice_delete on public.clienti;

create policy clienti_backoffice_select
on public.clienti
for select
to authenticated
using (
  lower(coalesce(auth.jwt() ->> 'email', '')) in (
    'fatturazione@illupoaffitta.com',
    'contabilita@illupoaffitta.com'
  )
);

create policy clienti_backoffice_insert
on public.clienti
for insert
to authenticated
with check (
  lower(coalesce(auth.jwt() ->> 'email', '')) in (
    'fatturazione@illupoaffitta.com',
    'contabilita@illupoaffitta.com'
  )
);

create policy clienti_backoffice_update
on public.clienti
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

create policy clienti_backoffice_delete
on public.clienti
for delete
to authenticated
using (
  lower(coalesce(auth.jwt() ->> 'email', '')) in (
    'fatturazione@illupoaffitta.com',
    'contabilita@illupoaffitta.com'
  )
);

alter table public.fatture_staging
  alter column ospiti_check_in_id drop not null;

alter table public.fatture_staging
  add column if not exists cliente_id uuid null references public.clienti(id),
  add column if not exists input_testuale_originale text null,
  add column if not exists parsing_payload jsonb null;

create index if not exists fatture_staging_cliente_id_idx
  on public.fatture_staging (cliente_id);

comment on column public.fatture_staging.cliente_id is
'Cliente collegato alla bozza fattura, valorizzato sia per flussi ospite sia per nuove fatture da testo libero.';

comment on column public.fatture_staging.input_testuale_originale is
'Prompt testuale originale usato per generare la bozza fattura da backoffice.';

comment on column public.fatture_staging.parsing_payload is
'Payload di parsing deterministicamente estratto dal testo libero prima della creazione bozza FiC.';

commit;
