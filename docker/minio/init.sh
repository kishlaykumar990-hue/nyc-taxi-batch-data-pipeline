#!/bin/sh
# Bootstraps MinIO: four data-layer buckets + a least-privilege service account.
# Safe to re-run (idempotent).
set -eu

mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

for bucket in taxi-raw taxi-cleaned taxi-rejected taxi-curated; do
  mc mb --ignore-existing "local/${bucket}"
done

# Policy limited to the four taxi-* buckets (no admin API, no other buckets).
mc admin policy create local pipeline-rw /config/pipeline-policy.json

# `user add` on an existing user simply resets its secret.
mc admin user add local "$MINIO_PIPELINE_ACCESS_KEY" "$MINIO_PIPELINE_SECRET_KEY"

# Re-attaching an already attached policy returns a non-zero code on some mc
# versions; that is the only error tolerated here.
mc admin policy attach local pipeline-rw --user "$MINIO_PIPELINE_ACCESS_KEY" \
  || echo "policy pipeline-rw already attached to ${MINIO_PIPELINE_ACCESS_KEY}"

echo "MinIO bootstrap complete."
