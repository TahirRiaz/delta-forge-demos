"""
Shared PySpark session factory and variable resolution for Delta demo verification.

Usage in verify scripts:
    from verify_lib.spark_session import get_spark, resolve_data_root

    data_root = resolve_data_root()
    spark = get_spark()

Variables are resolved in this order:
    1. CLI argument (positional `data_root`)
    2. Environment variable `DEMO_DATA_PATH`
    3. Fail with a clear error message

This module is the single source of truth for Spark configuration
across all Delta demo verification scripts.
"""

import argparse
import os
import sys


def get_spark():
    """Create a SparkSession configured for Delta Lake reads."""
    from pyspark.sql import SparkSession

    return SparkSession.builder \
        .appName("delta-verify") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "2g") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.log.level", "WARN") \
        .getOrCreate()


def resolve_data_root(description="Verify Delta data for demo"):
    """Parse CLI args / env vars and return the absolute data_root path.

    Resolution order:
        1. Positional CLI arg ``data_root``
        2. Environment variable ``DEMO_DATA_PATH``
        3. Exit with error
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "data_root",
        nargs="?",
        default=os.environ.get("DEMO_DATA_PATH"),
        help="Root path containing table directories (or set DEMO_DATA_PATH env var)",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    if args.data_root is None:
        print("Error: data_root not provided and DEMO_DATA_PATH not set")
        sys.exit(1)

    return os.path.abspath(args.data_root), args.verbose
