# Compliance checklist (IU DLMDSEDE02 Task 1)

Status key: **Verified** = executed here with evidence · **Implemented** = written, not yet run in the full stack · **Open** = needs you.

| # | Requirement (source: assignment) | Implementation | Status / evidence |
|---|---|---|---|
| 1 | Batch system: ingest, store, pre-process, aggregate, deliver for ML | Airflow + download_tlc + MinIO + Spark + Postgres | Implemented; Spark core Verified on synthetic data |
| 2 | Dataset >= 1,000,000 time-referenced points | TLC yellow Jan 2024 (verify actual count) ; check in `verify_pipeline.py` | **Open**: confirm count on real file. Synthetic 1.206M verified for logic only |
| 3 | Microservices, Docker, images from Docker Hub | `docker-compose.yml` (9 services) | Implemented; not started here (no daemon) |
| 4 | IaC, reproducible, version control | compose, SQL, init scripts, `.env.example`, Git repo | Implemented; local git repo created, push is yours |
| 5 | Reliability | retries/backoff, healthchecks, restart, atomic publish | Verified (publish tx tests); rest Implemented |
| 6 | Scalability | partitioned Parquet, scalable workers, shuffle partitions | Implemented; benchmark on real data **Open** |
| 7 | Maintainability | env config, pure functions, tests, docs | Verified (44 unit tests) |
| 8 | Security/governance/protection | roles, scoped MinIO key, localhost ports, lineage, docs | Verified for Postgres roles; docs written |
| 9 | Ingestion/pre-processing/aggregation incl. missing/dup/invalid data | `transform.py`, DQ doc | Verified (unit + e2e synthetic) |
| 10 | Frequency stated (monthly ingest, quarterly deliver) | two DAG schedules | Implemented |
| 11 | Architecture flow chart (PNG) + pros/cons | `docs/architecture.png`, phase 1 text | Done |
| 12 | Phase 1/2/3 deliverables, naming pattern | `portfolio/` | Drafts; need your name/matriculation no. |
| 13 | Abstract PDF: 2 pages A4, Arial 11, 1.5 spacing, 2 cm margins | — | **Open**: write after you run the real stack |
| 14 | Streaming-pipeline strategy discussion (finalization) | `portfolio/phase3_notes.md` | Draft |
| 15 | Citation guideline | — | **Open**: not in supplied files; please provide |
