"""Shared SparkSession builder + lake paths for all notebooks/jobs.

Everything in one place so you only tune memory/shuffle in one spot.

Remote (Spark Connect) vs classic:
    spark = get_spark("demo")                     # classic driver, connects to spark://spark-master:7077
    spark = get_spark("demo", remote="sc://spark-connect:15002")   # thin client
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

LAKE_ROOT = "/data"
BRONZE = f"{LAKE_ROOT}/bronze"
SILVER = f"{LAKE_ROOT}/silver"
GOLD = f"{LAKE_ROOT}/gold"
CHECKPOINTS = f"{LAKE_ROOT}/checkpoints"

MASTER_URL = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")
CONNECT_URL = os.environ.get("SPARK_CONNECT_URL", "sc://spark-connect:15002")


def get_spark(app: str = "study", remote: str | None = None,
              shuffle_partitions: int = 8, extra_conf: dict | None = None):
    """Create/get the study SparkSession with Delta Lake pre-configured."""
    builder = (
        SparkSession.builder.appName(app)
        .master(remote or MASTER_URL)
        .config("spark.sql.extensions", DELTA_EXTENSION)
        .config("spark.sql.catalog.spark_catalog", DELTA_CATALOG)
        .config("spark.sql.warehouse.dir", f"{LAKE_ROOT}/warehouse")
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
    )
    for k, v in (extra_conf or {}).items():
        builder = builder.config(k, v)
    driver_mem = os.environ.get("SPARK_DRIVER_MEMORY")
    if driver_mem:
        builder = builder.config("spark.driver.memory", driver_mem)
    return builder.getOrCreate()


def reset_lake(spark) -> None:
    """Drop the medallion databases (handy when replaying notebooks)."""
    for db in ("gold", "silver", "bronze"):
        spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")


def register_table(spark, db: str, name: str) -> bool:
    """Register an existing on-disk Delta table so this session can query it by name.

    Table metadata may not survive a process restart, but the data always does:
    this re-registers `<warehouse>/<db>.db/<name>` under its qualified name.
    Returns False when the table has not been written yet.
    """
    path = f"{LAKE_ROOT}/warehouse/{db}.db/{name}"
    if not os.path.isdir(path):
        return False
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {db}.{name} USING DELTA LOCATION '{path}'")
    return True


def sql(spark, query: str, n: int = 20):
    """Run SQL and show the result (shorthand used across notebooks)."""
    return spark.sql(query).show(n, truncate=False)
