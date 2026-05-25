alter table public.ospiti_check_in
  add column if not exists documenti_caricati jsonb not null default '[]'::jsonb;

comment on column public.ospiti_check_in.documenti_caricati is
'Metadati dei documenti caricati dal portale check-in: guest_scope, guest_index, file_name, mime_type, size_bytes, storage_bucket, storage_path, uploaded_at.';
