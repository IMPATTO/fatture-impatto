-- ============================================================
-- Schedula sync notturno prezzi via pg_cron + pg_net
-- ============================================================
-- Job che fa HTTP POST a Netlify function ogni notte alle 02:00 UTC
-- (= 03:00 IT inverno, 04:00 IT estate)
-- ============================================================

-- Rimuovi job esistente se presente (idempotente)
SELECT cron.unschedule('nightly_sync_prices_beds24')
WHERE EXISTS (
  SELECT 1
  FROM cron.job
  WHERE jobname = 'nightly_sync_prices_beds24'
);

-- Schedula nuovo job
SELECT cron.schedule(
  'nightly_sync_prices_beds24',
  '0 2 * * *',
  $$
  SELECT net.http_post(
    url := 'https://calendario.illupoaffitta.com/.netlify/functions/cron-sync-prices-nightly',
    headers := jsonb_build_object(
      'Content-Type', 'application/json',
      'X-Cron-Source', 'supabase-pg-cron'
    ),
    body := jsonb_build_object('source', 'pg_cron_nightly')
  ) AS request_id;
  $$
);
