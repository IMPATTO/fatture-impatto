alter table public.rm_apartments enable row level security;
alter table public.rm_unavailability enable row level security;
alter table public.rm_bookings enable row level security;
alter table public.rm_audit enable row level security;

revoke all on table public.rm_apartments from anon, authenticated;
revoke all on table public.rm_unavailability from anon, authenticated;
revoke all on table public.rm_bookings from anon, authenticated;
revoke all on table public.rm_audit from anon, authenticated;

grant select, insert, update, delete on table public.rm_apartments to service_role;
grant select, insert, update, delete on table public.rm_unavailability to service_role;
grant select, insert, update, delete on table public.rm_bookings to service_role;
grant select, insert, update, delete on table public.rm_audit to service_role;

alter table public.partner_booking_requests enable row level security;

revoke all on table public.partner_booking_requests from anon, authenticated;
grant select, insert, update, delete on table public.partner_booking_requests to service_role;
