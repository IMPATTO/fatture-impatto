create table if not exists public.contabilita_documenti (
  id uuid primary key default gen_random_uuid(),
  source_channel text not null default 'email',
  source_email text,
  source_subject text,
  source_message_id text,
  supplier_name text,
  document_type text not null default 'bolletta',
  stato text not null default 'DA_APPROVARE',
  apartment_id uuid references public.apartments(id) on delete set null,
  attachment_count integer not null default 0,
  attachments jsonb not null default '[]'::jsonb,
  extracted_text text,
  ocr_payload jsonb,
  raw_payload jsonb,
  amount_total numeric(12,2),
  currency text not null default 'EUR',
  issue_date date,
  due_date date,
  competence_month date,
  approval_notes text,
  tags text[] not null default '{}'::text[],
  approved_by text,
  approved_at timestamptz,
  received_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint contabilita_documenti_stato_check check (
    stato in ('DA_APPROVARE', 'DA_CORREGGERE', 'APPROVATO', 'SCARTATO')
  ),
  constraint contabilita_documenti_document_type_check check (
    document_type in ('bolletta', 'fattura_fornitore', 'ricevuta', 'contratto', 'altro')
  ),
  constraint contabilita_documenti_attachment_count_check check (attachment_count >= 0),
  constraint contabilita_documenti_currency_check check (char_length(currency) between 3 and 8)
);

create unique index if not exists contabilita_documenti_source_message_id_idx
  on public.contabilita_documenti (source_message_id)
  where source_message_id is not null;

create index if not exists contabilita_documenti_stato_idx
  on public.contabilita_documenti (stato, received_at desc);

create index if not exists contabilita_documenti_apartment_idx
  on public.contabilita_documenti (apartment_id, received_at desc);

create index if not exists contabilita_documenti_supplier_idx
  on public.contabilita_documenti (supplier_name);

comment on table public.contabilita_documenti is
'Coda documenti contabili ricevuti via email o caricamento manuale e assegnati agli appartamenti.';

comment on column public.contabilita_documenti.attachments is
'Array JSON con metadati allegati: filename, url/storage_path, content_type, size_bytes, preview_url.';

comment on column public.contabilita_documenti.ocr_payload is
'Dati strutturati prodotti da OCR/AI dal documento contabile.';

comment on column public.contabilita_documenti.raw_payload is
'Payload sorgente completo della mail o del provider di ingestione, per debug e audit.';
