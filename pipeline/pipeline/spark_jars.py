"""Spark jar resolution — pre-baked in image or Ivy fallback at runtime."""

from __future__ import annotations

import glob
import os

SPARK_JARS_DIR = os.getenv("SPARK_JARS_DIR", "/opt/spark-jars")
SPARK_IVY_DIR = os.getenv("SPARK_IVY_DIR", "/tmp/ivy2")

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
KAFKA_PG_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
    "org.postgresql:postgresql:42.7.3"
)


def ensure_ivy_dirs(ivy_dir: str = SPARK_IVY_DIR) -> None:
    os.makedirs(os.path.join(ivy_dir, "cache"), exist_ok=True)
    os.makedirs(os.path.join(ivy_dir, "jars"), exist_ok=True)


def prebuilt_jars() -> list[str]:
    return sorted(glob.glob(os.path.join(SPARK_JARS_DIR, "*.jar")))


def configure_spark_jars(builder, packages: str):
    """Prefer image-baked jars; fall back to Ivy package download."""
    jars = prebuilt_jars()
    if jars:
        return builder.config("spark.jars", ",".join(jars))
    ensure_ivy_dirs()
    return (
        builder
        .config("spark.jars.packages", packages)
        .config("spark.jars.ivy", SPARK_IVY_DIR)
    )
