"""SparkSession builder configured for Kafka and streaming checkpoints."""

from __future__ import annotations

import os
from typing import Optional
from qoe.common.config import get_config


def get_spark_session(
    app_name: str = "ott-qoe-engine",
    master: Optional[str] = None,
    extra_packages: Optional[list[str]] = None,
):
    """Create or retrieve configured SparkSession."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        raise RuntimeError("pyspark is not installed in current environment")

    cfg = get_config()
    shuffle_parts = str(cfg.get("streaming", {}).get("shuffle_partitions", 8))
    checkpoint_dir = str(cfg.get("streaming", {}).get("checkpoint_root", "data/checkpoints"))

    packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"]
    if extra_packages:
        packages.extend(extra_packages)

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", shuffle_parts)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
    )

    if master:
        builder = builder.master(master)
    elif "SPARK_MASTER" in os.environ:
        builder = builder.master(os.environ["SPARK_MASTER"])

    return builder.getOrCreate()
