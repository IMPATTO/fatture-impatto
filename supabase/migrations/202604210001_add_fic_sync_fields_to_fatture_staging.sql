alter table public.fatture_staging
  add column if not exists fic_document_id bigint,
  add column if not exists sezionale text,
  add column if not exists payment_status text;

comment on column public.fatture_staging.fic_document_id is
'ID del documento creato su Fatture in Cloud, utile per riconciliazione quando il sync locale e incompleto.';

comment on column public.fatture_staging.sezionale is
'Sezionale/numeration usato o restituito da Fatture in Cloud per il documento emesso.';

comment on column public.fatture_staging.payment_status is
'Stato pagamento usato in creazione documento su Fatture in Cloud.';
