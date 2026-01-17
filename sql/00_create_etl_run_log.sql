CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.etl_run_log (
  run_id TEXT,
  step TEXT,
  dataset TEXT,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  status TEXT,
  row_count BIGINT,
  checksum TEXT,
  message TEXT
);
