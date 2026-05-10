revoke all on table public.owners from public, anon;
revoke all on table public.apartment_owner_links from public, anon;
revoke all on table public.apartment_contracts from public, anon;
revoke all on table public.apartment_owner_payments from public, anon;

grant select, insert, update, delete on table public.owners to authenticated;
grant select, insert, update, delete on table public.apartment_owner_links to authenticated;
grant select, insert, update, delete on table public.apartment_contracts to authenticated;
grant select, insert, update, delete on table public.apartment_owner_payments to authenticated;
