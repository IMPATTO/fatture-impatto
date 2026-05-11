alter table public.apartment_credentials
  add column if not exists codice_accesso_lavanderia text,
  add column if not exists istruzioni_accesso_lavanderia text;

comment on column public.apartment_credentials.codice_accesso_lavanderia is
  'Codici dedicati all accesso lavanderia per strutture con ingressi separati, come Residence Montefeltro.';

comment on column public.apartment_credentials.istruzioni_accesso_lavanderia is
  'Dettagli e istruzioni dedicate all accesso lavanderia per strutture con ingressi separati.';

update public.apartment_credentials as creds
set
  istruzioni_accesso_lavanderia = coalesce(nullif(creds.istruzioni_accesso_lavanderia, ''), creds.luogo_chiavi),
  updated_at = now()
from public.apartments as apt
where apt.id = creds.apartment_id
  and lower(coalesce(apt.nome_appartamento, '') || ' ' || coalesce(apt.struttura_nome, '')) like '%montefeltro%'
  and coalesce(nullif(creds.luogo_chiavi, ''), '') <> ''
  and coalesce(nullif(creds.istruzioni_accesso_lavanderia, ''), '') = '';
