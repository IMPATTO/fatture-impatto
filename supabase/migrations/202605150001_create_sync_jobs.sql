CREATE TABLE IF NOT EXISTS public.sync_jobs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  job_type text NOT NULL,
  status text NOT NULL CHECK (status IN ('running', 'success', 'failed')),
  started_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  records_processed integer DEFAULT 0,
  error_message text,
  raw_payload jsonb
);

ALTER TABLE public.sync_jobs
  ADD COLUMN IF NOT EXISTS job_type text,
  ADD COLUMN IF NOT EXISTS completed_at timestamptz,
  ADD COLUMN IF NOT EXISTS records_processed integer DEFAULT 0,
  ADD COLUMN IF NOT EXISTS raw_payload jsonb;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'sync_jobs'
      AND column_name = 'scope'
  ) THEN
    EXECUTE $sql$
      UPDATE public.sync_jobs
      SET job_type = coalesce(job_type, scope)
      WHERE job_type IS NULL
        AND scope IS NOT NULL
    $sql$;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'sync_jobs'
      AND column_name = 'finished_at'
  ) THEN
    EXECUTE $sql$
      UPDATE public.sync_jobs
      SET completed_at = coalesce(completed_at, finished_at)
      WHERE completed_at IS NULL
        AND finished_at IS NOT NULL
    $sql$;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'sync_jobs'
      AND column_name = 'rows_upserted'
  ) THEN
    EXECUTE $sql$
      UPDATE public.sync_jobs
      SET records_processed = coalesce(records_processed, rows_upserted, 0)
      WHERE records_processed IS NULL
    $sql$;
  END IF;
END $$;

UPDATE public.sync_jobs
SET job_type = coalesce(job_type, 'legacy_sync'),
    records_processed = coalesce(records_processed, 0)
WHERE job_type IS NULL
   OR records_processed IS NULL;

CREATE INDEX IF NOT EXISTS idx_sync_jobs_type_started
  ON public.sync_jobs (job_type, started_at DESC);

ALTER TABLE public.sync_jobs ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename = 'sync_jobs'
      AND policyname = 'authenticated_read_sync_jobs'
  ) THEN
    CREATE POLICY "authenticated_read_sync_jobs"
      ON public.sync_jobs
      FOR SELECT
      TO authenticated
      USING (true);
  END IF;
END $$;
