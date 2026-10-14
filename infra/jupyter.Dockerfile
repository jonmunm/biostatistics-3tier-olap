FROM apache/spark-py:latest

USER root

RUN sed -i 's/http:\/\/archive.ubuntu.com/http:\/\/azure.archive.ubuntu.com/g' /etc/apt/sources.list && \
    apt-get update --fix-missing && \
    apt-get install -y --no-install-recommends \
    curl \
    procps \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir \
    jupyterlab \
    ipywidgets \
    great_expectations \
    pandas \
    pyarrow

ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

ENV PYSPARK_DRIVER_PYTHON="jupyter"
ENV PYSPARK_DRIVER_PYTHON_OPTS="lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root"

WORKDIR /opt/spark/work-dir

EXPOSE 8888 4040