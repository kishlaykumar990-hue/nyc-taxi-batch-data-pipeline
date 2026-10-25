# Security, governance and data protection

## Implemented
- **Secrets**: only in git-ignored `.env`; `.env.example` has placeholders; compose uses `${VAR:?}` so startup fails if one is missing.
- **Least privilege (Postgres)**: `etl_writer` (staging/serving/ops only, cannot create or drop in `serving`), `ml_reader` (SELECT on `serving` only). Verified by integration tests against a real PostgreSQL 16.
- **Object store access**: MinIO's public images (and the `mc` admin client) are no longer available, so the pipeline uses the object store's single root account instead of a scoped service account. This is a *reduced* least-privilege posture (documented limitation); with a client that supports admin policies, create a service account limited to the four `taxi-*` buckets.
- **Network exposure**: one private bridge network; all published ports bound to `127.0.0.1`.
- **Isolation**: one container per component, restart policies, health checks, source mounted read-only.
- **Input validation**: month format/contiguity, size and Parquet check on download, schema contract, plausibility rules.
- **Lineage/audit**: source URL + sha256 manifests, `batch_id` on every served row, `ops.batch_runs` with DQ counts, rejected rows retained with reasons.
- **Integrity**: atomic publish transaction, partial-staging guard, reconciliation checks.

## Data protection assessment
Yellow-taxi records contain no driver/passenger identifiers, but contain precise
pickup/dropoff times and zone-level locations. Individual trips can be re-identifiable
in combination with outside knowledge (e.g. a known person's trip time/place), so the
data should not be called "fully anonymous". This project publishes only **hourly
zone-level aggregates**, which greatly reduces that risk, but small zone-hours (counts of 1-2)
remain; a production system serving external parties should apply a minimum-count threshold.
Whether processing is GDPR-compliant is a legal assessment not made here; the dataset is a public
NYC release, and usage terms should be checked on the TLC site before any redistribution.

## Not implemented (local-dev limitations)
No TLS between containers, no encryption at rest, MinIO/Postgres run with a single node and default
auditing, no secrets manager (Vault/Docker secrets), Airflow web UI uses one admin account, failure
alerts are log-only, no retention/deletion automation. Recommended for production: TLS everywhere,
a secrets manager, SSO/RBAC, backups, retention policy, alert routing, minimum-count suppression.
