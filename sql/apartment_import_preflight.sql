-- A. CONTROLLI DB REMOTO

-- 1. Duplicati attuali su apartment_alloggiati che romperebbero il vincolo unique(apartment_id)
select
  apartment_id,
  count(*) as rows_count,
  array_agg(id order by id) as link_ids
from public.apartment_alloggiati
group by apartment_id
having count(*) > 1;

-- 2. Duplicati attuali su public_checkin_key, nel caso la colonna esista gia in ambienti sporchi
select
  lower(trim(public_checkin_key)) as public_checkin_key_norm,
  count(*) as rows_count,
  array_agg(id order by id) as apartment_ids
from public.apartments
where public_checkin_key is not null
group by lower(trim(public_checkin_key))
having count(*) > 1;

-- 3. Appartamenti senza chiave pubblica
select id, nome_appartamento, codice_interno, struttura_nome
from public.apartments
where coalesce(public_checkin_key, '') = ''
order by nome_appartamento;

-- 4. Chiavi pubbliche non normalizzate bene
select id, nome_appartamento, public_checkin_key
from public.apartments
where public_checkin_key is not null
  and public_checkin_key !~ '^[a-z0-9]+(?:-[a-z0-9]+)*$'
order by nome_appartamento;

-- 5. Mapping Alloggiati che puntano a appartamenti inesistenti o disattivi
select
  l.id,
  l.apartment_id,
  a.nome_appartamento,
  a.attivo,
  l.alloggiati_account_id
from public.apartment_alloggiati l
left join public.apartments a on a.id = l.apartment_id
where a.id is null
   or a.attivo = false
order by l.id;

-- 6. Appartamenti attivi senza mapping Alloggiati
select
  a.id,
  a.nome_appartamento,
  a.codice_interno,
  a.public_checkin_key,
  a.struttura_nome
from public.apartments a
left join public.apartment_alloggiati l on l.apartment_id = a.id
where a.attivo = true
  and l.id is null
order by a.nome_appartamento;

-- B. CONTROLLI DOPO LOAD STAGING_APARTMENTS

-- 7. Duplicati chiave pubblica nel CSV di staging apartments
select
  lower(trim(public_checkin_key)) as public_checkin_key_norm,
  count(*) as rows_count
from staging_apartments
where nullif(trim(public_checkin_key), '') is not null
group by lower(trim(public_checkin_key))
having count(*) > 1;

-- 8. Duplicati codice interno nel CSV di staging apartments
select
  upper(trim(codice_interno)) as codice_interno_norm,
  count(*) as rows_count
from staging_apartments
where nullif(trim(codice_interno), '') is not null
group by upper(trim(codice_interno))
having count(*) > 1;
