FROM python:3.9-slim-bookworm

RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

RUN pip install pyspark psycopg2-binary


COPY src/etl_job.py /app/etl_job.py

CMD ["python", "etl_job.py"]