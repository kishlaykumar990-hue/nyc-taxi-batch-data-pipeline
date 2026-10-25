# Airflow image extended with a JRE + PySpark so the scheduler container can act
# as the Spark *driver* (client mode) against the standalone cluster.
FROM apache/airflow:2.10.5

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
    && rm -rf /var/lib/apt/lists/* \
    # architecture-independent JAVA_HOME (amd64 and arm64 hosts)
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java \
    && mkdir -p /opt/ivy && chown airflow /opt/ivy
ENV JAVA_HOME=/opt/java

USER airflow
COPY docker/airflow/requirements.txt /tmp/requirements.txt
# Re-pinning apache-airflow keeps pip from upgrading Airflow's own dependencies.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt
