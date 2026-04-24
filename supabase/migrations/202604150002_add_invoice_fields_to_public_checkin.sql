alter table public.ospiti_check_in
  add column if not exists ragione_sociale text,
  add column if not exists indirizzo_fatturazione text;

comment on column public.ospiti_check_in.ragione_sociale is
'Ragione sociale usata per la fattura quando il check-in richiede intestazione aziendale.';

comment on column public.ospiti_check_in.indirizzo_fatturazione is
'Indirizzo di fatturazione separato dall indirizzo di residenza dell ospite.';
