alter table public.export_commercialista_log
  add column if not exists xml_source text,
  add column if not exists email_destinatario text,
  add column if not exists dry_run boolean not null default false;

comment on column public.export_commercialista_log.xml_source is
'Origine reale dell export: original, mixed, fallback_only, none.';

comment on column public.export_commercialista_log.email_destinatario is
'Destinatario email previsto per l export mensile.';

comment on column public.export_commercialista_log.dry_run is
'True quando il run e stato eseguito senza invio email reale.';
