alter table public.ospiti_check_in
  add column if not exists numero_persone integer not null default 1,
  add column if not exists additional_guests jsonb not null default '[]'::jsonb;

comment on column public.ospiti_check_in.numero_persone is
'Numero persone dichiarate nel form pubblico di check-in.';

comment on column public.ospiti_check_in.additional_guests is
'Ospiti aggiuntivi raccolti dal frontend pubblico in formato JSONB.';
