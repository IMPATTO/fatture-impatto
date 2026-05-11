do $$
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'apartments'
      and column_name = 'numero_emergenza'
  ) then
    update public.apartments
    set numero_emergenza = '+39 349 243 4501'
    where lower(trim(coalesce(nome_appartamento, ''))) in (
      'valtournenche - frazione chaloz di sotto snc - condominio grolla a',
      'valtournenche - frazione evette snc - condominio lisa'
    );
  end if;

  if exists (
    select 1
    from information_schema.tables
    where table_schema = 'public'
      and table_name = 'beds24_message_templates'
  ) then
    update public.beds24_message_templates as t
    set
      subject = case
        when t.subject is null then null
        else replace(replace(t.subject, '+39 351 3239927', '+39 349 243 4501'), '+39 350 1258030', '+39 349 243 4501')
      end,
      body_it = replace(replace(t.body_it, '+39 351 3239927', '+39 349 243 4501'), '+39 350 1258030', '+39 349 243 4501'),
      body_en = case
        when t.body_en is null then null
        else replace(replace(t.body_en, '+39 351 3239927', '+39 349 243 4501'), '+39 350 1258030', '+39 349 243 4501')
      end
    where exists (
      select 1
      from public.apartments as a
      where a.id = t.apartment_id
        and lower(trim(coalesce(a.nome_appartamento, ''))) in (
          'valtournenche - frazione chaloz di sotto snc - condominio grolla a',
          'valtournenche - frazione evette snc - condominio lisa'
        )
    );
  end if;
end
$$;
