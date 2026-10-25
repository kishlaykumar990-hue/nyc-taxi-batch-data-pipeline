#!/usr/bin/env bash
# Submit the batch job to the Spark standalone cluster (driver = this container,
# "client" mode). Secrets are NOT passed on the command line; the job reads them
# from the environment. Set SPARK_MASTER=local[2] to bypass the cluster.
set -euo pipefail

: "${SPARK_MASTER:?SPARK_MASTER is not set}"
PACKAGES="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.4"

exec spark-submit \
  --master "$SPARK_MASTER" \
  --name nyc-taxi-batch \
  --packages "$PACKAGES" \
  --conf "spark.jars.ivy=${SPARK_IVY_DIR:-/tmp/.ivy2}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY:-2g}" \
  --conf "spark.driver.host=${SPARK_DRIVER_HOST:-$(hostname)}" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.port=7078 \
  --conf spark.blockManager.port=7079 \
  --conf spark.driver.extraJavaOptions=-Duser.timezone=UTC \
  --conf spark.executor.extraJavaOptions=-Duser.timezone=UTC \
  --conf spark.ui.showConsoleProgress=false \
  /opt/project/src/spark/batch_job.py "$@"
