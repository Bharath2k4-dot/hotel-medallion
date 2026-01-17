import os
import sys
import subprocess
from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv("config/.env")

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise RuntimeError("DB_URL missing in config/.env")

ENGINE = create_engine(DB_URL)

PSQL_URL = "postgresql://hotel_user:hotel_pass@localhost:5432/hotel_db"

def utc_run_id():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def log_run(run_id, step, status, message=""):
    with ENGINE.begin() as conn:
        conn.execute(text("""
          INSERT INTO audit.etl_run_log
          (run_id, step, dataset, started_at, ended_at, status, row_count, checksum, message)
          VALUES (:run_id, :step, :dataset, :started_at, :ended_at, :status, :row_count, :checksum, :message)
        """), {
            "run_id": run_id,
            "step": step,
            "dataset": "hotel_medallion",
            "started_at": datetime.utcnow(),
            "ended_at": datetime.utcnow(),
            "status": status,
            "row_count": None,
            "checksum": None,
            "message": message[:1000]
        })

def run_psql_file(sql_path, run_id):
    cmd = [
        "psql",
        PSQL_URL,
        "-v", f"run_id={run_id}",
        "-f", sql_path
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "psql failed")
    return result.stdout

def build_silver():
    run_id = utc_run_id()
    try:
        out = run_psql_file("sql/03_build_silver.sql", run_id)
        # Save output for audit/debug
        os.makedirs("logs", exist_ok=True)
        with open(f"logs/day2_build_silver_{run_id}.log", "w") as f:
            f.write(out)
        log_run(run_id, "build_silver", "success", "silver built successfully")
        print("✅ build_silver completed:", run_id)
    except Exception as e:
        log_run(run_id, "build_silver", "failed", str(e))
        raise

def main():
    if len(sys.argv) < 2:
        print("Usage: python etl.py build_silver")
        sys.exit(1)

    cmd = sys.argv[1].lower()
    if cmd == "build_silver":
        build_silver()
    else:
        raise SystemExit(f"Unknown command: {cmd}")

if __name__ == "__main__":
    main()
