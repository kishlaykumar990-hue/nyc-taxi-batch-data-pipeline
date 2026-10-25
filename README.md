# NYC Taxi batch data backend (IU DLMDSEDE02, Task 1)

A reproducible, local, Docker-based **batch** data architecture that ingests official NYC TLC
yellow-taxi trip records, validates and cleans them, aggregates by pickup hour and taxi zone, and
delivers forecasting-ready tables to PostgreSQL for a quarterly ML application (not part of this project).

![architecture](docs/architecture.png)

| Layer | Service | Role |
|---|---|---|
| Ingestion | Airflow DAG `taxi_ingest_monthly` + `src/ingestion` | verified, idempotent download to `taxi-raw` |
| Storage | MinIO (`taxi-raw`, `-cleaned`, `-rejected`, `-curated`) | Parquet data lake layers |
| Processing | Spark master + worker; `src/spark` | validate, flag, dedupe, aggregate, features |
| Orchestration | Airflow (LocalExecutor) `taxi_process_quarterly` | schedule, retries, verification |
| Serving | PostgreSQL 16 (`serving`, `staging`, `ops`) | query-ready tables, read-only ML role |

Details: [data quality rules](docs/data_quality_rules.md) · [security & governance](docs/security_governance.md) ·
[compliance checklist](docs/compliance_checklist.md) · [benchmarks](benchmarks/RESULTS.md)

## Prerequisites
Docker + Compose v2, ~8 GB RAM free (4 GB works for one month), ~10 GB disk, internet access to
`d37ci6vzurychx.cloudfront.net` (TLC files) and Maven Central / Docker Hub (first start).

## Quick start
```bash
cp .env.example .env
# edit .env: replace every change_me_*; generate the two Airflow keys as described inside
docker compose up -d --build
docker compose ps                      # wait until services are healthy / init jobs exited 0
```
Airflow UI: http://localhost:8081 · Spark UI: http://localhost:8080 · MinIO console: http://localhost:9001 · Postgres: localhost:5433 (all bound to localhost only)

## Run the pipeline (demo: Jan 2024 yellow taxi ≈ 3M rows)
```bash
docker compose exec airflow-scheduler airflow dags unpause taxi_ingest_monthly
docker compose exec airflow-scheduler airflow dags unpause taxi_process_quarterly
docker compose exec airflow-scheduler airflow dags trigger taxi_ingest_monthly  --conf '{"months":"2024-01"}'
# when it has succeeded:
docker compose exec airflow-scheduler airflow dags trigger taxi_process_quarterly --conf '{"months":"2024-01"}'
```
Scheduled operation: ingestion on the 15th of every month (ingests month-2), processing on the 15th of
Feb/May/Aug/Nov over the quarter's last three published months. Sample/dev run: `--conf '{"months":"2024-01","sample_fraction":0.05}'`
(then the 1,000,000-row check will, correctly, fail).

## Verify
```bash
set -a; source .env; set +a
python scripts/verify_pipeline.py --months 2024-01 --minio-endpoint http://localhost:9000 --pg-host localhost --pg-port 5433
docker compose exec -T postgres psql -U postgres -d taxi -f /sql/queries/example_queries.sql
```
Tests: `pip install -r requirements-dev.txt && pytest tests/unit` (needs Java 17+). Integration tests
(`pytest tests/integration`) need a Postgres initialised from `sql/`; set `PG_HOST/PG_PORT/PG_PASSWORD(/ML_DB_PASSWORD)`.

## Configuration
Everything is in `.env` (months, sample fraction, quality limits, Spark memory/partitions, image tags).
Scale processing: `docker compose up -d --scale spark-worker=2`.

## Reliability, scalability, maintainability
Retries with exponential backoff; health checks and `depends_on` conditions; restart policies; idempotent
ingestion (manifest), idempotent object writes (dynamic partition overwrite) and atomic Postgres publish;
reconciliation guards fail the run instead of publishing wrong data. Scaling: partitioned Parquet, more Spark
workers / partitions. Maintainability: config via env, pure transform functions with unit tests, IaC for everything.

## Cleanup
`docker compose down` (keep data) · `docker compose down -v` (delete all volumes).

## Troubleshooting
- *Compose says a variable is missing*: fill it in `.env`. 
- *Spark job cannot reach driver*: workers must resolve `airflow-scheduler` (same compose network); `SPARK_MASTER=local[2]` bypasses the cluster.
- *First run slow*: Spark downloads S3A/JDBC jars via Ivy (cached in the `ivycache` volume).
- *Out of memory*: lower `SPARK_SHUFFLE_PARTITIONS`, process one month, or raise `SPARK_WORKER_MEMORY`.
- *Month not found*: TLC publishes with ~2 month delay.

## Status and limitations (read before submitting)
Verified in the authoring sandbox: unit tests (44 passed, local Spark), integration tests against PostgreSQL 16 (7 passed),
the Spark job end-to-end on **synthetic** 1.2M-row data with local storage, and idempotent re-runs.
**Not verified there** (no Docker daemon and no access to the TLC/Maven hosts): `docker compose up`, MinIO/S3A I/O,
Spark cluster mode, JDBC staging, Airflow DAG execution, the real TLC download. Treat the first `docker compose up`
as the real integration test and fix issues it surfaces. Security limitations: see `docs/security_governance.md`.
