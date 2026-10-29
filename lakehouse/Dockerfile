FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_LOCAL_IP=127.0.0.1 \
    PYTHONPATH=/app
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY lakehouse/requirements.txt /app/lakehouse/requirements.txt
RUN pip install --no-cache-dir -r /app/lakehouse/requirements.txt
# Resolve pinned Delta JVM artifacts at build time; offline runs need no Maven access.
RUN mkdir -p /opt/delta && \
    curl --fail --retry 3 -o /opt/delta/delta-spark.jar \
      https://repo.maven.apache.org/maven2/io/delta/delta-spark_2.12/3.3.2/delta-spark_2.12-3.3.2.jar && \
    curl --fail --retry 3 -o /opt/delta/delta-storage.jar \
      https://repo.maven.apache.org/maven2/io/delta/delta-storage/3.3.2/delta-storage-3.3.2.jar
COPY lakehouse /app/lakehouse
ENTRYPOINT ["python", "-m", "lakehouse.job"]
CMD ["--help"]
