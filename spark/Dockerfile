# spark/Dockerfile
FROM apache/spark:3.5.1
USER root

COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt

# Base image passwd lists /home/spark but does not create the directory.
RUN mkdir -p /home/spark /opt/spark-jars /tmp/ivy2/cache /tmp/ivy2/jars && \
    chown -R spark:spark /home/spark /opt/spark-jars /tmp/ivy2

ENV SPARK_JARS_DIR=/opt/spark-jars
ENV SPARK_IVY_DIR=/tmp/ivy2
ENV HOME=/home/spark

# Pre-download Kafka + PostgreSQL jars at build time (avoids Ivy at runtime).
RUN su spark -c "/opt/spark/bin/spark-submit \
    --master 'local[1]' \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
    --conf spark.jars.ivy=/tmp/ivy2 \
    /opt/spark/examples/src/main/python/pi.py 1" && \
    cp /tmp/ivy2/jars/*.jar /opt/spark-jars/ && \
    chown spark:spark /opt/spark-jars/*.jar

USER spark
WORKDIR /workspace
