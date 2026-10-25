#!/usr/bin/env bash
# Benchmark plan: run the same batch with different input sizes / shuffle
# partitions and print the measured rows from ops.batch_runs.
# Usage (host, stack up, months already ingested):  bash scripts/benchmark.sh
set -euo pipefail
cd "$(dirname "$0")/.."

run() {  # months, partitions, batch_id
  docker compose exec -T airflow-scheduler bash /opt/project/scripts/spark_submit.sh \
    --months "$1" --shuffle-partitions "$2" --batch-id "$3" | tail -1 >/dev/null
}

run "2024-01"                       8  bench_1m_p8
run "2024-01"                      32  bench_1m_p32
run "2024-01,2024-02,2024-03"       8  bench_3m_p8
run "2024-01,2024-02,2024-03"      32  bench_3m_p32

docker compose exec -T postgres psql -U "${POSTGRES_ADMIN_USER:-postgres}" -d "${POSTGRES_DB:-taxi}" -c \
 "SELECT batch_id, source_rows, accepted_rows, rejected_rows, input_bytes, output_bytes,
         round(processing_s::numeric,1) AS processing_s, round(publish_s::numeric,1) AS publish_s,
         round(throughput_rows_per_s::numeric) AS rows_per_s, config->>'shuffle_partitions' AS shuffle_parts
  FROM ops.batch_runs WHERE batch_id LIKE 'bench_%' ORDER BY batch_id;"
