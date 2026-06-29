#!/usr/bin/env python3
#pipeline\base_job.py
import os
from pyspark.sql import SparkSession

class SparkStreamingJob:
    def __init__(self, job_name):
        self.job_name = job_name
        self.spark = self._init_spark()

    def _init_spark(self):
        """Khởi tạo SparkSession với cấu hình tối ưu cho Streaming."""
        return SparkSession.builder \
            .appName(self.job_name) \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
            .getOrCreate()

    def read_kafka(self, topic):
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092")) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .load()

    def write_kafka(self, df, topic):
        return df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092")) \
            .option("topic", topic) \
            .option("checkpointLocation", f"/tmp/checkpoints/{topic}") \
            .start()