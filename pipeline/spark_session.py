"""SparkSession factory (local mode by default, free, no cluster needed)."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_spark(app_name: str = "cfpb-complaints") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.ui.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        # Only overwrite the partitions we actually write (incremental silver).
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Spark 4 turns ANSI mode on by default, which makes to_date()/cast()
        # THROW on bad values. For cleaning messy data we want NULL instead,
        # so bad values can be flagged / quarantined rather than crash the job.
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.parquet.compression.codec", "snappy")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "ERROR"))
    return spark
