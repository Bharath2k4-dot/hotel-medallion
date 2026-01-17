import os
import hashlib
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime

load_dotenv("config/.env")
engine = create_engine(os.getenv("DB_URL"))

files = [
  "hotels_raw",
  "room_types_raw",
  "bookings_raw",
  "payments_raw",
  "room_inventory_raw"
]

def checksum(path):
    h = hashlib.md5()
    with open(path, 'rb') as f:
        h.update(f.read())
    return h.hexdigest()

for f in files:
    path = f"bronze_inputs/{f}.csv"
    df = pd.read_csv(path)
    df.to_sql(f, engine, schema="bronze", if_exists="replace", index=False)

    print(f"Loaded {f}: {len(df)} rows | checksum={checksum(path)[:10]}")
