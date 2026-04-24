alter table public.contabilita_documenti
  add column if not exists source text,
  add column if not exists source_ref text,
  add column if not exists file_name text,
  add column if not exists file_url text,
  add column if not exists mime_type text,
  add column if not exists document_kind text,
  add column if not exists document_date date,
  add column if not exists competence_date date,
  add column if not exists counterparty text,
  add column if not exists description text,
  add column if not exists total_amount numeric(12,2),
  add column if not exists payment_method text,
  add column if not exists fiscal_status text,
  add column if not exists approval_status text not null default 'da_revisionare',
  add column if not exists property_id uuid,
  add column if not exists residence_id uuid,
  add column if not exists category text,
  add column if not exists subcategory text,
  add column if not exists notes text,
  add column if not exists extracted_json jsonb not null default '{}'::jsonb,
  add column if not exists reviewed_json jsonb not null default '{}'::jsonb,
  add column if not exists assigned_by text,
  add column if not exists reviewed_by text,
  add column if not exists discarded_reason text null;

update public.contabilita_documenti
set
  source = coalesce(source, source_channel, 'email'),
  source_ref = coalesce(source_ref, source_message_id),
  file_name = coalesce(file_name, attachments->0->>'filename'),
  file_url = coalesce(file_url, attachments->0->>'url', attachments->0->>'storage_path'),
  mime_type = coalesce(mime_type, attachments->0->>'content_type'),
  document_kind = coalesce(document_kind, document_type),
  document_date = coalesce(document_date, issue_date),
  competence_date = coalesce(competence_date, competence_month),
  counterparty = coalesce(counterparty, supplier_name),
  description = coalesce(description, source_subject),
  total_amount = coalesce(total_amount, amount_total),
  approval_status = coalesce(
    approval_status,
    case stato
      when 'DA_APPROVARE' then 'da_revisionare'
      when 'DA_CORREGGERE' then 'corretto'
      when 'APPROVATO' then 'approvato'
      when 'SCARTATO' then 'scartato'
      else 'da_revisionare'
    end
  ),
  notes = coalesce(notes, approval_notes),
  extracted_json = case
    when coalesce(extracted_json, '{}'::jsonb) <> '{}'::jsonb then extracted_json
    else jsonb_strip_nulls(jsonb_build_object(
      'source_email', source_email,
      'source_subject', source_subject,
      'supplier_name', supplier_name,
      'document_type', document_type,
      'amount_total', amount_total,
      'currency', currency,
      'issue_date', issue_date,
      'due_date', due_date,
      'competence_month', competence_month,
      'extracted_text', extracted_text,
      'attachments', attachments,
      'ocr_payload', ocr_payload
    ))
  end,
  reviewed_by = coalesce(reviewed_by, approved_by)
where true;

do $$
begin
  if not exists (
    select 1 from pg_constraint where conname = 'contabilita_documenti_payment_method_check'
  ) then
    alter table public.contabilita_documenti
      add constraint contabilita_documenti_payment_method_check
      check (payment_method is null or payment_method in ('contanti', 'bonifico', 'carta', 'pos', 'addebito', 'altro'));
  end if;

  if not exists (
    select 1 from pg_constraint where conname = 'contabilita_documenti_fiscal_status_check'
  ) then
    alter table public.contabilita_documenti
      add constraint contabilita_documenti_fiscal_status_check
      check (fiscal_status is null or fiscal_status in ('da_fatturare', 'fatturato', 'non_soggetto', 'annullato'));
  end if;

  if not exists (
    select 1 from pg_constraint where conname = 'contabilita_documenti_approval_status_check'
  ) then
    alter table public.contabilita_documenti
      add constraint contabilita_documenti_approval_status_check
      check (approval_status in ('da_revisionare', 'approvato', 'corretto', 'scartato'));
  end if;
end $$;

create unique index if not exists contabilita_documenti_source_ref_uidx
  on public.contabilita_documenti (source_ref)
  where source_ref is not null;

create index if not exists contabilita_documenti_approval_status_idx
  on public.contabilita_documenti (approval_status, created_at desc);

create index if not exists contabilita_documenti_fiscal_status_idx
  on public.contabilita_documenti (fiscal_status, payment_method, created_at desc);

create index if not exists contabilita_documenti_category_idx
  on public.contabilita_documenti (category, subcategory);

create or replace view public.contabilita_documenti_ui as
select
  d.*,
  case
    when d.payment_method = 'contanti' and d.fiscal_status = 'fatturato' then 'green'
    when d.payment_method = 'contanti' and d.fiscal_status = 'da_fatturare' then 'amber'
    else null
  end as cash_badge_color
from public.contabilita_documenti d;

comment on view public.contabilita_documenti_ui is
'Vista helper per la UI contabilità con badge operativo contanti.';
