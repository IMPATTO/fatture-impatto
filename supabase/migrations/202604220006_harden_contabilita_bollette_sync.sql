comment on column public.contabilita_bollette.linked_movement_id is
  'Riferimento operativo riusato: oggi contiene public.contabilita_documenti.id del documento base collegato, non un movimento bancario separato.';

create or replace function public.sync_bolletta_from_documento()
returns trigger
language plpgsql
as $$
begin
  update public.contabilita_bollette
  set
    apartment_id = new.apartment_id,
    amount_total = new.total_amount,
    updated_at = now()
  where contabilita_documento_id = new.id
    and (
      apartment_id is distinct from new.apartment_id
      or amount_total is distinct from new.total_amount
    );

  return new;
end;
$$;

drop trigger if exists trg_sync_bolletta_from_documento on public.contabilita_documenti;

create trigger trg_sync_bolletta_from_documento
after update of apartment_id, total_amount
on public.contabilita_documenti
for each row
when (new.document_kind = 'bolletta')
execute function public.sync_bolletta_from_documento();
