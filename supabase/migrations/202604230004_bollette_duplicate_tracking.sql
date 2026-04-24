alter table public.contabilita_documenti
  add column if not exists original_source_message_id text,
  add column if not exists attachment_hash text,
  add column if not exists duplicate_status text not null default 'normal',
  add column if not exists duplicate_of_document_id uuid null references public.contabilita_documenti(id) on delete set null,
  add column if not exists duplicate_reason text null,
  add column if not exists processed_at timestamptz not null default now();

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'contabilita_documenti_duplicate_status_check'
  ) then
    alter table public.contabilita_documenti
      add constraint contabilita_documenti_duplicate_status_check
      check (duplicate_status in ('normal', 'duplicate_exact', 'possible_duplicate'));
  end if;
end $$;

update public.contabilita_documenti
set
  original_source_message_id = coalesce(
    original_source_message_id,
    nullif(extracted_json->>'source_message_id_original', ''),
    source_message_id
  ),
  processed_at = coalesce(processed_at, updated_at, created_at, now())
where
  original_source_message_id is null
  or processed_at is null;

create index if not exists contabilita_documenti_original_source_message_id_idx
  on public.contabilita_documenti (original_source_message_id)
  where original_source_message_id is not null;

create index if not exists contabilita_documenti_attachment_hash_idx
  on public.contabilita_documenti (attachment_hash)
  where attachment_hash is not null;

create index if not exists contabilita_documenti_duplicate_status_idx
  on public.contabilita_documenti (duplicate_status);

create index if not exists contabilita_documenti_duplicate_of_document_id_idx
  on public.contabilita_documenti (duplicate_of_document_id)
  where duplicate_of_document_id is not null;

comment on column public.contabilita_documenti.original_source_message_id is
'Message-ID originale del provider inbound email/telegram, distinto dal source_message_id univoco per allegato.';

comment on column public.contabilita_documenti.attachment_hash is
'Hash SHA-256 dell allegato usato per rilevare duplicati tecnici certi.';

comment on column public.contabilita_documenti.duplicate_status is
'Stato anti-doppione: normal, duplicate_exact, possible_duplicate.';

comment on column public.contabilita_documenti.duplicate_of_document_id is
'Riferimento al documento gia presente che ha fatto scattare il flag di possibile doppione o doppione tecnico.';

comment on column public.contabilita_documenti.duplicate_reason is
'Motivo sintetico del flag anti-doppione.';

comment on column public.contabilita_documenti.processed_at is
'Timestamp ultimo processamento della pipeline inbound bollette.';
