# Reproducible dev/test/pipeline image.
#  - Debian 12 (bookworm) is pinned on purpose: floating "slim" tags move to newer Debian
#    releases that no longer ship OpenJDK 17.
#  - Python 3.11: PySpark 3.5 does not support the newest Python releases.
#  - Java 17:     required by Spark 3.5 (Java 8 on the host is too old, which is why Spark
#                 only runs in this container).
FROM python:3.11-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/src:/app

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt requirements-api.txt requirements-dev.txt requirements-spark.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt -r requirements-spark.txt

COPY docker/install_delta_jars.py /tmp/install_delta_jars.py
RUN python /tmp/install_delta_jars.py

COPY . .
