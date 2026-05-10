create table if not exists public.export_commercialista_delivery_attempts (
  id uuid primary key default gen_random_uuid(),
  settimana_riferimento date not null,
  mese_riferimento date not null,
  periodo_label text not null,
  resend_email_id text not null unique,
  resend_last_event text,
  email_destinatario text not null,
  email_mittente text not null,
  email_subject text not null,
  attachment_name text,
  document_count integer not null default 0,
  chunk_index integer not null default 1,
  chunk_total integer not null default 1,
  checked_at timestamptz,
  created_at timestamptz not null default now()
);

create index if not exists export_commercialista_delivery_attempts_week_idx
  on public.export_commercialista_delivery_attempts (settimana_riferimento desc, created_at desc);
