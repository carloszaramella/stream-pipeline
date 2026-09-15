FROM apache/spark:3.5.3-python3

USER root

WORKDIR /opt/stream-pipeline

ENV POSTGRES_JDBC_VERSION=42.7.8

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

RUN wget -q \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -O /opt/spark/jars/hadoop-aws-3.3.4.jar

RUN wget -q \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -O /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

RUN wget -q \
    https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    -O /opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar

COPY src ./src
COPY producer ./producer
COPY tests ./tests
COPY data ./data

ENV PYTHONPATH=/opt/stream-pipeline
ENV PIPELINE_BASE_DIR=/opt/stream-pipeline

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

CMD ["/opt/spark/bin/spark-submit", "--master", "local[2]", "/opt/stream-pipeline/src/medallion_pipeline.py"]