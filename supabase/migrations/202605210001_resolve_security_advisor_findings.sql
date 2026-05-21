begin;

-- Fix all current Security Advisor findings while preserving the
-- intended browser access model:
-- - backoffice browser tables -> authenticated internal staff only
-- - operational/service tables -> explicit client deny, service_role only
-- - legacy backup table -> no API exposure

do $$
declare
  sig text;
begin
  foreach sig in array array[
    'agent_todo_actor_labels()',
    'agent_todo_can_view(text[],text[],boolean)',
    'agent_todo_can_write(text[],text[],boolean,uuid)',
    'agent_todo_is_privileged()',
    'agent_todo_is_victor_vanessa()',
    'is_internal_staff()',
    'is_pms_editor()',
    'rm_update_updated_at()',
    'set_apartment_channel_mappings_updated_at()',
    'set_apartment_istat_config_updated_at()',
    'set_beds24_bookings_cache_updated_at()',
    'set_beds24_inventory_cache_updated_at()',
    'set_beds24_message_templates_updated_at()',
    'set_clienti_derived_fields()',
    'set_contabilita_updated_at()',
    'set_operativita_access_profiles_updated_at()',
    'set_operativita_tasks_updated_at()',
    'set_partner_booking_requests_updated_at()',
    'set_richieste_appartamenti_updated_at()',
    'set_updated_at_timestamp()',
    'sync_bolletta_from_documento()',
    'update_updated_at()'
  ]
  loop
    if to_regprocedure(format('public.%s', sig)) is not null then
      execute format(
        'alter function public.%s set search_path = public, auth',
        sig
      );
    end if;
  end loop;
end
$$;

do $$
declare
  table_name text;
  policy_name text;
begin
  foreach table_name in array array[
    'apartment_credentials',
    'apartment_info',
    'apartment_maintenance_tasks',
    'owners',
    'apartment_owner_links',
    'apartment_contracts',
    'apartment_owner_payments'
  ]
  loop
    if to_regclass(format('public.%I', table_name)) is not null then
      policy_name := format('%s_internal_staff_all', table_name);

      execute format(
        'alter table public.%I enable row level security',
        table_name
      );
      execute format(
        'revoke all on table public.%I from public, anon',
        table_name
      );
      execute format(
        'grant select, insert, update, delete on table public.%I to authenticated',
        table_name
      );
      execute format(
        'grant select, insert, update, delete on table public.%I to service_role',
        table_name
      );

      case table_name
        when 'apartment_credentials' then
          execute 'drop policy if exists auth_read_apartment_credentials on public.apartment_credentials';
          execute 'drop policy if exists auth_write_apartment_credentials on public.apartment_credentials';
        when 'apartment_info' then
          execute 'drop policy if exists auth_read_apartment_info on public.apartment_info';
          execute 'drop policy if exists auth_write_apartment_info on public.apartment_info';
        when 'apartment_maintenance_tasks' then
          execute 'drop policy if exists "apartment_maintenance_tasks_authenticated_select" on public.apartment_maintenance_tasks';
          execute 'drop policy if exists "apartment_maintenance_tasks_authenticated_insert" on public.apartment_maintenance_tasks';
          execute 'drop policy if exists "apartment_maintenance_tasks_authenticated_update" on public.apartment_maintenance_tasks';
          execute 'drop policy if exists "apartment_maintenance_tasks_authenticated_delete" on public.apartment_maintenance_tasks';
        else
          execute format(
            'drop policy if exists %I on public.%I',
            table_name || '_authenticated_all',
            table_name
          );
      end case;

      execute format(
        'drop policy if exists %I on public.%I',
        policy_name,
        table_name
      );
      execute format(
        'create policy %I on public.%I for all to authenticated using (public.is_internal_staff()) with check (public.is_internal_staff())',
        policy_name,
        table_name
      );
    end if;
  end loop;
end
$$;

do $$
begin
  if to_regclass('public.apartment_istat_config') is not null then
    execute 'alter table public.apartment_istat_config enable row level security';
    execute 'revoke all on table public.apartment_istat_config from public, anon, authenticated';
    execute 'grant select on table public.apartment_istat_config to authenticated';
    execute 'grant select, insert, update, delete on table public.apartment_istat_config to service_role';

    execute 'drop policy if exists apartment_istat_config_authenticated_select on public.apartment_istat_config';
    execute 'drop policy if exists apartment_istat_config_authenticated_insert on public.apartment_istat_config';
    execute 'drop policy if exists apartment_istat_config_authenticated_update on public.apartment_istat_config';
    execute 'drop policy if exists authenticated_read_apartment_istat_config on public.apartment_istat_config';
    execute 'drop policy if exists apartment_istat_config_internal_staff_select on public.apartment_istat_config';

    execute '
      create policy apartment_istat_config_internal_staff_select
      on public.apartment_istat_config
      for select
      to authenticated
      using (public.is_internal_staff())
    ';
  end if;

  if to_regclass('public.istat_invii') is not null then
    execute 'alter table public.istat_invii enable row level security';
    execute 'revoke all on table public.istat_invii from public, anon, authenticated';
    execute 'grant select on table public.istat_invii to authenticated';
    execute 'grant select, insert, update, delete on table public.istat_invii to service_role';

    execute 'drop policy if exists "Log ISTAT solo autenticati" on public.istat_invii';
    execute 'drop policy if exists istat_invii_authenticated_select on public.istat_invii';
    execute 'drop policy if exists istat_invii_authenticated_insert on public.istat_invii';
    execute 'drop policy if exists istat_invii_internal_staff_select on public.istat_invii';

    execute '
      create policy istat_invii_internal_staff_select
      on public.istat_invii
      for select
      to authenticated
      using (public.is_internal_staff())
    ';
  end if;
end
$$;

do $$
declare
  table_name text;
begin
  if to_regclass('public.audit_log') is not null then
    execute 'alter table public.audit_log enable row level security';
    execute 'revoke all on table public.audit_log from public, anon, authenticated';
    execute 'grant insert on table public.audit_log to authenticated';
    execute 'grant select, insert, update, delete on table public.audit_log to service_role';

    execute 'drop policy if exists audit_log_authenticated_insert on public.audit_log';
    execute 'drop policy if exists audit_log_internal_staff_insert on public.audit_log';

    execute '
      create policy audit_log_internal_staff_insert
      on public.audit_log
      for insert
      to authenticated
      with check (public.is_internal_staff())
    ';
  end if;

  foreach table_name in array array[
    'beds24_sync_log',
    'export_commercialista_delivery_attempts',
    'export_commercialista_log',
    'fatture_staging',
    'ospiti_cronologia',
    'partner_booking_requests',
    'rm_apartments',
    'rm_audit',
    'rm_bookings',
    'rm_unavailability'
  ]
  loop
    if to_regclass(format('public.%I', table_name)) is not null then
      execute format(
        'alter table public.%I enable row level security',
        table_name
      );

      if table_name = 'partner_booking_requests' then
        execute 'alter table public.partner_booking_requests force row level security';
      end if;

      execute format(
        'revoke all on table public.%I from public, anon, authenticated',
        table_name
      );
      execute format(
        'grant select, insert, update, delete on table public.%I to service_role',
        table_name
      );
      execute format(
        'drop policy if exists %I on public.%I',
        table_name || '_no_client_access',
        table_name
      );
      execute format(
        'create policy %I on public.%I for all to authenticated using (false) with check (false)',
        table_name || '_no_client_access',
        table_name
      );
    end if;
  end loop;

  if to_regclass('public.partner_booking_requests_backup_20260512') is not null then
    execute 'alter table public.partner_booking_requests_backup_20260512 enable row level security';
    execute 'alter table public.partner_booking_requests_backup_20260512 force row level security';
    execute 'revoke all on table public.partner_booking_requests_backup_20260512 from public, anon, authenticated, service_role';
    execute 'drop policy if exists partner_booking_requests_backup_20260512_no_client_access on public.partner_booking_requests_backup_20260512';

    execute '
      create policy partner_booking_requests_backup_20260512_no_client_access
      on public.partner_booking_requests_backup_20260512
      for all
      to authenticated
      using (false)
      with check (false)
    ';
  end if;
end
$$;

do $$
begin
  if to_regprocedure('public.contabilita_is_authorized()') is not null then
    execute 'revoke all on function public.contabilita_is_authorized() from public, anon, authenticated';
    execute 'grant execute on function public.contabilita_is_authorized() to service_role';
  end if;

  if to_regprocedure('public.log_contabilita_movimento_history()') is not null then
    execute 'revoke all on function public.log_contabilita_movimento_history() from public, anon, authenticated';
    execute 'grant execute on function public.log_contabilita_movimento_history() to service_role';
  end if;
end
$$;

do $$
begin
  execute 'create schema if not exists extensions';

  if exists (
    select 1
    from pg_extension e
    join pg_namespace n
      on n.oid = e.extnamespace
    where e.extname = 'pg_trgm'
      and n.nspname = 'public'
  ) then
    execute 'alter extension pg_trgm set schema extensions';
  end if;

  if exists (
    select 1
    from pg_extension e
    join pg_namespace n
      on n.oid = e.extnamespace
    where e.extname = 'pg_net'
      and n.nspname = 'public'
  ) then
    if exists (select 1 from net.http_request_queue limit 1) then
      raise exception
        'Cannot reinstall pg_net while net.http_request_queue is not empty';
    end if;

    execute 'drop extension if exists pg_net';
    execute 'create extension if not exists pg_net schema extensions';
  end if;
end
$$;

commit;
