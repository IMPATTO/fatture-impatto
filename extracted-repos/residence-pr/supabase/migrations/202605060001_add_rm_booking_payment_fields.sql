alter table public.rm_bookings
  add column if not exists deposit_amount numeric(10,2),
  add column if not exists balance_due_at_checkin numeric(10,2);

comment on column public.rm_bookings.deposit_amount is 'Caparra gia versata dal cliente';
comment on column public.rm_bookings.balance_due_at_checkin is 'Saldo previsto all arrivo';

select column_name, data_type
from information_schema.columns
where table_schema = 'public'
  and table_name = 'rm_bookings'
  and column_name in ('deposit_amount', 'balance_due_at_checkin')
order by column_name;
