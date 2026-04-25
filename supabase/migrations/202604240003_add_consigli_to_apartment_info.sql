alter table public.apartment_info
add column if not exists consigli jsonb not null default '{"ristoranti":"","bar_colazione":"","aperitivo_vita_serale":"","attivita_bambini":""}'::jsonb;
