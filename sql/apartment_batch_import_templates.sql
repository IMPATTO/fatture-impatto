-- CSV consigliato: apartments
-- nome_appartamento,codice_interno,public_checkin_key,attivo,struttura_nome,indirizzo_completo,provincia,cap,latitudine,longitudine,maps_url_override,numero_emergenza

create temporary table staging_apartments (
  nome_appartamento text,
  codice_interno text,
  public_checkin_key text,
  attivo text,
  struttura_nome text,
  indirizzo_completo text,
  provincia text,
  cap text,
  latitudine text,
  longitudine text,
  maps_url_override text,
  numero_emergenza text
);

-- \copy staging_apartments from '/absolute/path/apartments.csv' with (format csv, header true);

-- STOP 1: righe senza chiave pubblica
select *
from staging_apartments
where nullif(trim(public_checkin_key), '') is null;

do $$
begin
  if exists (
    select 1
    from staging_apartments
    where nullif(trim(public_checkin_key), '') is null
  ) then
    raise exception 'Import apartments bloccato: esistono righe senza public_checkin_key.';
  end if;
end $$;

-- STOP 2: duplicati chiave pubblica nel CSV
select
  lower(trim(public_checkin_key)) as public_checkin_key_norm,
  count(*) as rows_count
from staging_apartments
where nullif(trim(public_checkin_key), '') is not null
group by lower(trim(public_checkin_key))
having count(*) > 1;

do $$
begin
  if exists (
    select 1
    from staging_apartments
    where nullif(trim(public_checkin_key), '') is not null
    group by lower(trim(public_checkin_key))
    having count(*) > 1
  ) then
    raise exception 'Import apartments bloccato: duplicati public_checkin_key nello staging.';
  end if;
end $$;

-- STOP 3: duplicati codice interno nel CSV
select
  upper(trim(codice_interno)) as codice_interno_norm,
  count(*) as rows_count
from staging_apartments
where nullif(trim(codice_interno), '') is not null
group by upper(trim(codice_interno))
having count(*) > 1;

do $$
begin
  if exists (
    select 1
    from staging_apartments
    where nullif(trim(codice_interno), '') is not null
    group by upper(trim(codice_interno))
    having count(*) > 1
  ) then
    raise exception 'Import apartments bloccato: duplicati codice_interno nello staging.';
  end if;
end $$;

with normalized as (
  select
    trim(nome_appartamento) as nome_appartamento,
    nullif(trim(codice_interno), '') as codice_interno,
    lower(nullif(trim(public_checkin_key), '')) as public_checkin_key,
    coalesce(nullif(lower(trim(attivo)), '') in ('true', 't', '1', 'yes', 'y'), true) as attivo,
    nullif(trim(struttura_nome), '') as struttura_nome,
    nullif(trim(indirizzo_completo), '') as indirizzo_completo,
    nullif(trim(provincia), '') as provincia,
    nullif(trim(cap), '') as cap,
    case
      when nullif(trim(latitudine), '') is null then null
      when trim(latitudine) ~ '^-?[0-9]+(\.[0-9]+)?$' then trim(latitudine)::numeric
      else null
    end as latitudine,
    case
      when nullif(trim(longitudine), '') is null then null
      when trim(longitudine) ~ '^-?[0-9]+(\.[0-9]+)?$' then trim(longitudine)::numeric
      else null
    end as longitudine,
    nullif(trim(maps_url_override), '') as maps_url_override,
    nullif(trim(numero_emergenza), '') as numero_emergenza
  from staging_apartments
  where nullif(trim(nome_appartamento), '') is not null
    and nullif(trim(public_checkin_key), '') is not null
)
update public.apartments a
set
  nome_appartamento = n.nome_appartamento,
  codice_interno = n.codice_interno,
  public_checkin_key = n.public_checkin_key,
  attivo = n.attivo,
  struttura_nome = n.struttura_nome,
  indirizzo_completo = n.indirizzo_completo,
  provincia = n.provincia,
  cap = n.cap,
  latitudine = n.latitudine,
  longitudine = n.longitudine,
  maps_url_override = n.maps_url_override,
  numero_emergenza = n.numero_emergenza
from normalized n
where lower(a.public_checkin_key) = n.public_checkin_key;

with normalized as (
  select
    trim(nome_appartamento) as nome_appartamento,
    nullif(trim(codice_interno), '') as codice_interno,
    lower(nullif(trim(public_checkin_key), '')) as public_checkin_key,
    coalesce(nullif(lower(trim(attivo)), '') in ('true', 't', '1', 'yes', 'y'), true) as attivo,
    nullif(trim(struttura_nome), '') as struttura_nome,
    nullif(trim(indirizzo_completo), '') as indirizzo_completo,
    nullif(trim(provincia), '') as provincia,
    nullif(trim(cap), '') as cap,
    case
      when nullif(trim(latitudine), '') is null then null
      when trim(latitudine) ~ '^-?[0-9]+(\.[0-9]+)?$' then trim(latitudine)::numeric
      else null
    end as latitudine,
    case
      when nullif(trim(longitudine), '') is null then null
      when trim(longitudine) ~ '^-?[0-9]+(\.[0-9]+)?$' then trim(longitudine)::numeric
      else null
    end as longitudine,
    nullif(trim(maps_url_override), '') as maps_url_override,
    nullif(trim(numero_emergenza), '') as numero_emergenza
  from staging_apartments
  where nullif(trim(nome_appartamento), '') is not null
    and nullif(trim(public_checkin_key), '') is not null
)
insert into public.apartments (
  nome_appartamento,
  codice_interno,
  public_checkin_key,
  attivo,
  struttura_nome,
  indirizzo_completo,
  provincia,
  cap,
  latitudine,
  longitudine,
  maps_url_override,
  numero_emergenza
)
select
  n.nome_appartamento,
  n.codice_interno,
  n.public_checkin_key,
  n.attivo,
  n.struttura_nome,
  n.indirizzo_completo,
  n.provincia,
  n.cap,
  n.latitudine,
  n.longitudine,
  n.maps_url_override,
  n.numero_emergenza
from normalized n
where not exists (
  select 1
  from public.apartments a
  where lower(a.public_checkin_key) = n.public_checkin_key
);

-- CSV consigliato: apartment_alloggiati
-- apartment_ref,alloggiati_account_id,id_appartamento_portale,invio_automatico,orario_invio,istat_codice_struttura_override

create temporary table staging_apartment_alloggiati (
  apartment_ref text,
  alloggiati_account_id uuid,
  id_appartamento_portale text,
  invio_automatico text,
  orario_invio text,
  istat_codice_struttura_override text
);

-- \copy staging_apartment_alloggiati from '/absolute/path/apartment_alloggiati.csv' with (format csv, header true);

with staged as (
  select
    trim(apartment_ref) as apartment_ref,
    alloggiati_account_id,
    nullif(trim(id_appartamento_portale), '') as id_appartamento_portale,
    coalesce(nullif(lower(trim(invio_automatico)), '') in ('true', 't', '1', 'yes', 'y'), false) as invio_automatico,
    case
      when nullif(trim(orario_invio), '') is null then '22:00:00'::time
      when trim(orario_invio) ~ '^[0-9]{2}:[0-9]{2}(:[0-9]{2})?$' then trim(orario_invio)::time
      else '22:00:00'::time
    end as orario_invio,
    nullif(trim(istat_codice_struttura_override), '') as istat_codice_struttura_override
  from staging_apartment_alloggiati
  where alloggiati_account_id is not null
)
select s.*
from staging_apartment_alloggiati s
left join public.alloggiati_accounts aa on aa.id = s.alloggiati_account_id
where s.alloggiati_account_id is not null
  and aa.id is null;

do $$
begin
  if exists (
    select 1
    from staging_apartment_alloggiati s
    left join public.alloggiati_accounts aa on aa.id = s.alloggiati_account_id
    where s.alloggiati_account_id is not null
      and aa.id is null
  ) then
    raise exception 'Import apartment_alloggiati bloccato: esistono alloggiati_account_id inesistenti.';
  end if;
end $$;

with staged as (
  select
    trim(apartment_ref) as apartment_ref,
    alloggiati_account_id,
    nullif(trim(id_appartamento_portale), '') as id_appartamento_portale,
    coalesce(nullif(lower(trim(invio_automatico)), '') in ('true', 't', '1', 'yes', 'y'), false) as invio_automatico,
    case
      when nullif(trim(orario_invio), '') is null then '22:00:00'::time
      when trim(orario_invio) ~ '^[0-9]{2}:[0-9]{2}(:[0-9]{2})?$' then trim(orario_invio)::time
      else '22:00:00'::time
    end as orario_invio,
    nullif(trim(istat_codice_struttura_override), '') as istat_codice_struttura_override
  from staging_apartment_alloggiati
  where alloggiati_account_id is not null
),
resolved as (
  select
    coalesce(a_uuid.id, a_key.id) as apartment_id,
    s.apartment_ref
  from staged s
  left join public.apartments a_uuid
    on a_uuid.id::text = s.apartment_ref
  left join public.apartments a_key
    on lower(a_key.public_checkin_key) = lower(s.apartment_ref)
)
select *
from resolved
where apartment_id is null;

do $$
begin
  if exists (
    with staged as (
      select
        trim(apartment_ref) as apartment_ref,
        alloggiati_account_id,
        nullif(trim(id_appartamento_portale), '') as id_appartamento_portale,
        coalesce(nullif(lower(trim(invio_automatico)), '') in ('true', 't', '1', 'yes', 'y'), false) as invio_automatico,
        case
          when nullif(trim(orario_invio), '') is null then '22:00:00'::time
          when trim(orario_invio) ~ '^[0-9]{2}:[0-9]{2}(:[0-9]{2})?$' then trim(orario_invio)::time
          else '22:00:00'::time
        end as orario_invio,
        nullif(trim(istat_codice_struttura_override), '') as istat_codice_struttura_override
      from staging_apartment_alloggiati
      where alloggiati_account_id is not null
    ),
    resolved as (
      select
        coalesce(a_uuid.id, a_key.id) as apartment_id,
        s.apartment_ref
      from staged s
      left join public.apartments a_uuid
        on a_uuid.id::text = s.apartment_ref
      left join public.apartments a_key
        on lower(a_key.public_checkin_key) = lower(s.apartment_ref)
    )
    select 1
    from resolved
    where apartment_id is null
  ) then
    raise exception 'Import apartment_alloggiati bloccato: esistono apartment_ref non risolti.';
  end if;
end $$;

with staged as (
  select
    trim(apartment_ref) as apartment_ref,
    alloggiati_account_id,
    nullif(trim(id_appartamento_portale), '') as id_appartamento_portale,
    coalesce(nullif(lower(trim(invio_automatico)), '') in ('true', 't', '1', 'yes', 'y'), false) as invio_automatico,
    case
      when nullif(trim(orario_invio), '') is null then '22:00:00'::time
      when trim(orario_invio) ~ '^[0-9]{2}:[0-9]{2}(:[0-9]{2})?$' then trim(orario_invio)::time
      else '22:00:00'::time
    end as orario_invio,
    nullif(trim(istat_codice_struttura_override), '') as istat_codice_struttura_override
  from staging_apartment_alloggiati
  where alloggiati_account_id is not null
),
resolved as (
  select
    coalesce(a_uuid.id, a_key.id) as apartment_id,
    s.apartment_ref
  from staged s
  left join public.apartments a_uuid
    on a_uuid.id::text = s.apartment_ref
  left join public.apartments a_key
    on lower(a_key.public_checkin_key) = lower(s.apartment_ref)
)
select *
from resolved
where apartment_id is null;

do $$
begin
  if exists (
    with staged as (
      select
        trim(apartment_ref) as apartment_ref,
        alloggiati_account_id,
        nullif(trim(id_appartamento_portale), '') as id_appartamento_portale,
        coalesce(nullif(lower(trim(invio_automatico)), '') in ('true', 't', '1', 'yes', 'y'), false) as invio_automatico,
        case
          when nullif(trim(orario_invio), '') is null then '22:00:00'::time
          when trim(orario_invio) ~ '^[0-9]{2}:[0-9]{2}(:[0-9]{2})?$' then trim(orario_invio)::time
          else '22:00:00'::time
        end as orario_invio,
        nullif(trim(istat_codice_struttura_override), '') as istat_codice_struttura_override
      from staging_apartment_alloggiati
      where alloggiati_account_id is not null
    ),
    resolved as (
      select
        coalesce(a_uuid.id, a_key.id) as apartment_id,
        s.apartment_ref
      from staged s
      left join public.apartments a_uuid
        on a_uuid.id::text = s.apartment_ref
      left join public.apartments a_key
        on lower(a_key.public_checkin_key) = lower(s.apartment_ref)
    )
    select 1
    from resolved
    where apartment_id is null
  ) then
    raise exception 'Import apartment_alloggiati bloccato: esistono apartment_ref non risolti.';
  end if;
end $$;

with staged as (
  select
    trim(apartment_ref) as apartment_ref,
    alloggiati_account_id,
    nullif(trim(id_appartamento_portale), '') as id_appartamento_portale,
    coalesce(nullif(lower(trim(invio_automatico)), '') in ('true', 't', '1', 'yes', 'y'), false) as invio_automatico,
    case
      when nullif(trim(orario_invio), '') is null then '22:00:00'::time
      when trim(orario_invio) ~ '^[0-9]{2}:[0-9]{2}(:[0-9]{2})?$' then trim(orario_invio)::time
      else '22:00:00'::time
    end as orario_invio,
    nullif(trim(istat_codice_struttura_override), '') as istat_codice_struttura_override
  from staging_apartment_alloggiati
  where alloggiati_account_id is not null
),
resolved as (
  select
    coalesce(a_uuid.id, a_key.id) as apartment_id,
    s.alloggiati_account_id,
    s.id_appartamento_portale,
    s.invio_automatico,
    s.orario_invio,
    s.istat_codice_struttura_override,
    s.apartment_ref
  from staged s
  left join public.apartments a_uuid
    on a_uuid.id::text = s.apartment_ref
  left join public.apartments a_key
    on lower(a_key.public_checkin_key) = lower(s.apartment_ref)
)
select apartment_ref, count(*) as rows_count
from resolved
where apartment_id is not null
group by apartment_ref
having count(*) > 1;

do $$
begin
  if exists (
    with staged as (
      select
        trim(apartment_ref) as apartment_ref,
        alloggiati_account_id,
        nullif(trim(id_appartamento_portale), '') as id_appartamento_portale,
        coalesce(nullif(lower(trim(invio_automatico)), '') in ('true', 't', '1', 'yes', 'y'), false) as invio_automatico,
        case
          when nullif(trim(orario_invio), '') is null then '22:00:00'::time
          when trim(orario_invio) ~ '^[0-9]{2}:[0-9]{2}(:[0-9]{2})?$' then trim(orario_invio)::time
          else '22:00:00'::time
        end as orario_invio,
        nullif(trim(istat_codice_struttura_override), '') as istat_codice_struttura_override
      from staging_apartment_alloggiati
      where alloggiati_account_id is not null
    ),
    resolved as (
      select
        coalesce(a_uuid.id, a_key.id) as apartment_id,
        s.alloggiati_account_id,
        s.id_appartamento_portale,
        s.invio_automatico,
        s.orario_invio,
        s.istat_codice_struttura_override,
        s.apartment_ref
      from staged s
      left join public.apartments a_uuid
        on a_uuid.id::text = s.apartment_ref
      left join public.apartments a_key
        on lower(a_key.public_checkin_key) = lower(s.apartment_ref)
    )
    select 1
    from resolved
    where apartment_id is not null
    group by apartment_ref
    having count(*) > 1
  ) then
    raise exception 'Import apartment_alloggiati bloccato: apartment_ref ambiguo nella risoluzione.';
  end if;
end $$;

with staged as (
  select
    trim(apartment_ref) as apartment_ref,
    alloggiati_account_id,
    nullif(trim(id_appartamento_portale), '') as id_appartamento_portale,
    coalesce(nullif(lower(trim(invio_automatico)), '') in ('true', 't', '1', 'yes', 'y'), false) as invio_automatico,
    case
      when nullif(trim(orario_invio), '') is null then '22:00:00'::time
      when trim(orario_invio) ~ '^[0-9]{2}:[0-9]{2}(:[0-9]{2})?$' then trim(orario_invio)::time
      else '22:00:00'::time
    end as orario_invio,
    nullif(trim(istat_codice_struttura_override), '') as istat_codice_struttura_override
  from staging_apartment_alloggiati
  where alloggiati_account_id is not null
),
resolved as (
  select
    coalesce(a_uuid.id, a_key.id) as apartment_id,
    s.alloggiati_account_id,
    s.id_appartamento_portale,
    s.invio_automatico,
    s.orario_invio,
    s.istat_codice_struttura_override,
    s.apartment_ref
  from staged s
  left join public.apartments a_uuid
    on a_uuid.id::text = s.apartment_ref
  left join public.apartments a_key
    on lower(a_key.public_checkin_key) = lower(s.apartment_ref)
)
insert into public.apartment_alloggiati (
  apartment_id,
  alloggiati_account_id,
  id_appartamento_portale,
  invio_automatico,
  orario_invio,
  istat_codice_struttura_override
)
select
  apartment_id,
  alloggiati_account_id,
  id_appartamento_portale,
  invio_automatico,
  orario_invio,
  istat_codice_struttura_override
from resolved
where apartment_id is not null
on conflict (apartment_id) do update
set
  alloggiati_account_id = excluded.alloggiati_account_id,
  id_appartamento_portale = excluded.id_appartamento_portale,
  invio_automatico = excluded.invio_automatico,
  orario_invio = excluded.orario_invio,
  istat_codice_struttura_override = excluded.istat_codice_struttura_override;
