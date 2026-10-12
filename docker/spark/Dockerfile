FROM apache/spark-py:latest

USER root

# Instalamos herramientas básicas y dependencias de python necesarias para GE
RUN apt-get update && apt-get install -y \
    curl \
    procps \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Instalamos Great Expectations
RUN pip3 install --no-cache-dir great_expectations

ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

WORKDIR /opt/spark