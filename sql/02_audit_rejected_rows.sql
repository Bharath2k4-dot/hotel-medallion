CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.rejected_rows (
  rejected_id BIGSERIAL PRIMARY KEY,
  run_id TEXT NOT NULL,
  layer TEXT NOT NULL,
  source_table TEXT NOT NULL,
  target_table TEXT NOT NULL,
  reason TEXT NOT NULL,
  row_data JSONB,
  rejected_at TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_rejected_rows_run ON audit.rejected_rows(run_id);
