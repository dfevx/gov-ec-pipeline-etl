-- Auditoría de corridas ETL (resumen y detalle)

CREATE TABLE IF NOT EXISTS public.etl_runs (
  run_id text PRIMARY KEY,
  started_at timestamptz NOT NULL DEFAULT now(),
  ended_at timestamptz,
  status text CHECK (status IN ('ok','error')) NOT NULL,
  resources jsonb,
  rows_in_total bigint,
  rows_out_total bigint,
  schema_missing_total int,
  schema_extra_total int,
  error_message text
);

CREATE TABLE IF NOT EXISTS public.etl_run_resources (
  run_id text REFERENCES public.etl_runs(run_id) ON DELETE CASCADE,
  rid text,
  rows_in bigint,
  rows_out bigint,
  schema_missing jsonb,
  schema_extra jsonb,
  duplicates_business_key int,
  dedup_rows_dropped int,
  PRIMARY KEY (run_id, rid)
);

-- Índices útiles para inspección
CREATE INDEX IF NOT EXISTS idx_etl_runs_status     ON public.etl_runs (status);
CREATE INDEX IF NOT EXISTS idx_etl_runs_started_at ON public.etl_runs (started_at);
