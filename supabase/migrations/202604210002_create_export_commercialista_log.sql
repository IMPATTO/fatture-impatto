create table if not exists public.export_commercialista_log (
  id uuid primary key default gen_random_uuid(),
  mese_riferimento date not null,
  eseguito_at timestamptz not null default now(),
  esito text not null,
  numero_documenti integer not null default 0,
  tipo_export text not null,
  nome_file text,
  errore_dettaglio text
);

create index if not exists export_commercialista_log_month_idx
  on public.export_commercialista_log (mese_riferimento desc);

create unique index if not exists export_commercialista_log_success_month_uidx
  on public.export_commercialista_log (mese_riferimento)
  where esito = 'SUCCESSO';

comment on table public.export_commercialista_log is
'Log mensile export XML verso commercialista con protezione anti doppio invio sul mese con esito SUCCESSO.';

comment on column public.export_commercialista_log.mese_riferimento is
'Primo giorno del mese di riferimento esportato.';

comment on column public.export_commercialista_log.tipo_export is
'Valori attesi: ORIGINAL_XML, CUSTOM_FALLBACK_XML, MIXED, NO_DOCUMENTS.';
