begin;

create table if not exists public.pms_calendar_access (
  id uuid primary key default gen_random_uuid(),
  login_email text not null,
  apartment_id uuid not null references public.apartments(id) on delete cascade,
  can_edit boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint pms_calendar_access_login_email_lower_check
    check (login_email = lower(login_email))
);

create unique index if not exists pms_calendar_access_login_email_apartment_idx
  on public.pms_calendar_access (login_email, apartment_id);

create index if not exists pms_calendar_access_apartment_idx
  on public.pms_calendar_access (apartment_id);

drop trigger if exists trg_pms_calendar_access_updated_at on public.pms_calendar_access;
create trigger trg_pms_calendar_access_updated_at
before update on public.pms_calendar_access
for each row execute function public.set_updated_at_timestamp();

alter table public.pms_calendar_access enable row level security;

grant select on public.pms_calendar_access to authenticated;

drop policy if exists pms_calendar_access_select_own_or_internal on public.pms_calendar_access;
create policy pms_calendar_access_select_own_or_internal
  on public.pms_calendar_access
  for select
  to authenticated
  using (
    public.is_internal_staff()
    or lower(login_email) = lower(coalesce(auth.jwt() ->> 'email', ''))
  );

create or replace function public.current_auth_email()
returns text
language sql
stable
as $$
  select lower(coalesce(auth.jwt() ->> 'email', ''));
$$;

grant execute on function public.current_auth_email() to authenticated;

create or replace function public.has_pms_calendar_read_access(target_apartment_id uuid)
returns boolean
language sql
stable
as $$
  select
    public.is_internal_staff()
    or exists (
      select 1
      from public.pms_calendar_access access_row
      where access_row.apartment_id = target_apartment_id
        and access_row.login_email = public.current_auth_email()
    );
$$;

grant execute on function public.has_pms_calendar_read_access(uuid) to authenticated;

create or replace function public.has_pms_calendar_edit_access(target_apartment_id uuid)
returns boolean
language sql
stable
as $$
  select
    public.is_pms_editor()
    or exists (
      select 1
      from public.pms_calendar_access access_row
      where access_row.apartment_id = target_apartment_id
        and access_row.login_email = public.current_auth_email()
        and access_row.can_edit = true
    );
$$;

grant execute on function public.has_pms_calendar_edit_access(uuid) to authenticated;

drop policy if exists apartments_pms_calendar_scoped_select on public.apartments;
create policy apartments_pms_calendar_scoped_select
  on public.apartments
  for select
  to authenticated
  using (public.has_pms_calendar_read_access(id));

drop policy if exists authenticated_read_apartments on public.apartments;

drop policy if exists "authenticated_read_apartment_units" on public.apartment_units;
create policy "authenticated_read_apartment_units"
  on public.apartment_units
  for select
  to authenticated
  using (public.has_pms_calendar_read_access(apartment_id));

drop policy if exists "authenticated_read_bookings" on public.bookings;
create policy "authenticated_read_bookings"
  on public.bookings
  for select
  to authenticated
  using (
    (
      apartment_id is not null
      and public.has_pms_calendar_read_access(apartment_id)
    )
    or exists (
      select 1
      from public.apartment_units unit_row
      where unit_row.id = public.bookings.apartment_unit_id
        and public.has_pms_calendar_read_access(unit_row.apartment_id)
    )
  );

drop policy if exists "authenticated_read_calendar_days" on public.calendar_days;
create policy "authenticated_read_calendar_days"
  on public.calendar_days
  for select
  to authenticated
  using (
    exists (
      select 1
      from public.apartment_units unit_row
      where unit_row.id = public.calendar_days.apartment_unit_id
        and public.has_pms_calendar_read_access(unit_row.apartment_id)
    )
  );

insert into public.pms_calendar_access (login_email, apartment_id, can_edit)
values
  ('susybrossa@outlook.it', '34a29327-4313-4761-ac31-49224b88e3b2', true),
  ('susybrossa@outlook.it', 'a9ad3351-4e32-4555-b07b-31c4420df230', true),
  ('teresacolfitbit@gmail.com', 'a9ad3351-4e32-4555-b07b-31c4420df230', false),
  ('mariafasoli71@yahoo.com', '34a29327-4313-4761-ac31-49224b88e3b2', false)
on conflict (login_email, apartment_id) do update
set can_edit = excluded.can_edit,
    updated_at = now();

commit;
