alter table public.export_commercialista_log
  add column if not exists settimana_riferimento date;

drop index if exists export_commercialista_log_success_month_uidx;

create index if not exists export_commercialista_log_week_idx
  on public.export_commercialista_log (settimana_riferimento desc);

create unique index if not exists export_commercialista_log_success_week_uidx
  on public.export_commercialista_log (settimana_riferimento)
  where esito = 'SUCCESSO'
    and dry_run = false
    and settimana_riferimento is not null;

comment on column public.export_commercialista_log.settimana_riferimento is
'Lunedi della settimana di invio per l export automatico settimanale.';
