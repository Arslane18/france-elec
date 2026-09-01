FROM eclipse-temurin:17-jre-jammy

ARG SPARK_VERSION=4.2.0
ARG HADOOP_VERSION=3
ARG PYTHON_VERSION=3.13.15

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates procps \
    && rm -rf /var/lib/apt/lists/*

# Installs the exact same python-build-standalone binary that `uv` uses on the
# host venv (see .python-version), rather than apt's own Ubuntu-patched build
# — driver and executors end up byte-for-byte the same interpreter.
ENV PATH="/root/.local/bin:${PATH}"
RUN curl -LsSf https://astral.sh/uv/install.sh | sh \
    && uv python install ${PYTHON_VERSION}

ENV SPARK_HOME=/opt/spark
RUN mkdir -p ${SPARK_HOME} \
    && curl -fsSL "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C ${SPARK_HOME} --strip-components=1 \
    && rm /tmp/spark.tgz

ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
# Must match the Python minor version of the driver (the project's venv, 3.13)
# — Spark refuses to run when driver and executors are on different minors.
ENV PYSPARK_PYTHON=python3.13
WORKDIR ${SPARK_HOME}
