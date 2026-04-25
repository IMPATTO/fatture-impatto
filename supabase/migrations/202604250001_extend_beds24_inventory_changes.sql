alter table public.beds24_inventory_changes
  add column if not exists error_message text;

alter table public.beds24_inventory_changes
  alter column status set default 'pending';
