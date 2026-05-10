alter table public.apartment_info
add column if not exists beach_info jsonb not null default '[]'::jsonb;

alter table public.apartment_info
add column if not exists waste_info jsonb not null default '{}'::jsonb;

alter table public.apartment_info
add column if not exists wifi_info jsonb not null default '{}'::jsonb;
