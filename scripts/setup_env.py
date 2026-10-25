#!/usr/bin/env python3
"""Create `.env` from `.env.example` with freshly generated passwords and keys.

Usage:  python3 scripts/setup_env.py [--low-memory]
Refuses to overwrite an existing .env. Uses only the Python standard library.
"""
import base64
import os
import re
import secrets
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
src, dst = root / ".env.example", root / ".env"
if dst.exists():
    sys.exit(".env already exists - delete it first if you really want a new one.")

def pw() -> str:                       # letters+digits only: safe in URLs/shell/YAML
    return secrets.token_hex(12)

values = {
    "POSTGRES_ADMIN_PASSWORD": pw(), "ETL_DB_PASSWORD": pw(), "ML_DB_PASSWORD": pw(),
    "MINIO_ROOT_PASSWORD": pw(), "MINIO_PIPELINE_SECRET_KEY": pw(),
    "AIRFLOW_DB_PASSWORD": pw(), "AIRFLOW_ADMIN_PASSWORD": pw(),
    "AIRFLOW_FERNET_KEY": base64.urlsafe_b64encode(os.urandom(32)).decode(),  # valid Fernet key format
    "AIRFLOW_SECRET_KEY": secrets.token_hex(32),
}
if "--low-memory" in sys.argv:
    values.update({"SPARK_MASTER": "local[2]", "SPARK_DRIVER_MEMORY": "2g", "SPARK_SHUFFLE_PARTITIONS": "8"})

text = src.read_text()
for key, val in values.items():
    text, n = re.subn(rf"^{key}=.*$", f"{key}={val}", text, flags=re.M)
    assert n == 1, f"{key} not found in .env.example"
dst.write_text(text)
print(f"Created {dst}")
print("Airflow login ->  user: admin   password:", values["AIRFLOW_ADMIN_PASSWORD"])
print("MinIO console ->  user: minio_admin   password:", values["MINIO_ROOT_PASSWORD"])
print("(you can read these again any time by opening the .env file)")
