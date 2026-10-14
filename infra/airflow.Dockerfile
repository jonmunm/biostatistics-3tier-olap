FROM apache/airflow:3.0.0

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

RUN mkdir -p /opt/airflow/drivers

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.1.3 \
    pyspark==3.4.0 \
    duckdb \
    duckdb-engine \
    great_expectations