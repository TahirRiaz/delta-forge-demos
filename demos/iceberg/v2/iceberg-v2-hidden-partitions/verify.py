#!/usr/bin/env python3
"""
Iceberg V2 Hidden Partitions — Data Verification
==================================================
Reads the trips table through the Iceberg metadata chain and verifies
300 ride-share trip records with hidden months(pickup_date) partitioning.

Usage:
    python verify.py <data_root_path> [--verbose]

Requirements:
    pip install pyiceberg[pyarrow] fastavro
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (
    read_iceberg_table, ok, fail, info,
    assert_row_count, assert_sum, assert_avg, assert_min, assert_max,
    assert_distinct_count, assert_count_where, assert_format_version,
    reset_counters,
)
from verify_lib import print_header, print_summary, exit_with_status


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v2-hidden-partitions demo"
    )
    parser.add_argument("data_root", help="Parent folder containing trips/")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show sample data")
    args = parser.parse_args()

    import pyarrow.compute as pc

    data_root = os.path.abspath(args.data_root)
    table_path = os.path.join(data_root, "trips")

    print_header("Hidden Partitions (Trips) — Data Verification")
    print(f"  Data root: {data_root}")

    if not os.path.isdir(os.path.join(table_path, "metadata")):
        print(f"\nError: {table_path}/metadata not found")
        sys.exit(1)

    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg metadata chain")

    if args.verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 300)

    # Distinct counts
    assert_distinct_count(table, "trip_id", 300)
    assert_distinct_count(table, "city", 5)
    assert_distinct_count(table, "payment_type", 3)
    assert_distinct_count(table, "driver_id", 50)

    # City breakdown (60 each)
    assert_count_where(table, "city", "Austin", 60)
    assert_count_where(table, "city", "Chicago", 60)
    assert_count_where(table, "city", "New York", 60)
    assert_count_where(table, "city", "San Francisco", 60)
    assert_count_where(table, "city", "Seattle", 60)

    # Payment type breakdown
    assert_count_where(table, "payment_type", "Cash", 104)
    assert_count_where(table, "payment_type", "Credit Card", 108)
    assert_count_where(table, "payment_type", "Digital Wallet", 88)

    # Fare totals
    assert_sum(table, "fare_amount", 10890.01, label="total fare")
    assert_avg(table, "fare_amount", 36.30, label="avg fare")

    # Distance totals
    total_dist = round(pc.sum(table.column("distance_miles")).as_py(), 1)
    if total_dist == 3747.1:
        ok(f"SUM(distance_miles) = 3747.1")
    else:
        fail(f"SUM(distance_miles) = {total_dist}, expected 3747.1")

    # Null tip count (Cash rides with null tip)
    null_tips = pc.sum(pc.is_null(table.column("tip_amount"))).as_py()
    if null_tips == 14:
        ok(f"NULL tip_amount count = 14")
    else:
        fail(f"NULL tip_amount count = {null_tips}, expected 14")

    # Fare/distance ranges
    assert_min(table, "fare_amount", 3.70)
    assert_max(table, "fare_amount", 85.14)
    assert_min(table, "distance_miles", 0.50)
    assert_max(table, "distance_miles", 25.00)

    # Per-city avg distance
    for city, exp_avg in [("San Francisco", 13.99), ("New York", 13.26),
                           ("Seattle", 12.22), ("Austin", 12.11), ("Chicago", 10.87)]:
        mask = pc.equal(table.column("city"), city)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("distance_miles")).as_py(), 2)
        if actual == exp_avg:
            ok(f"AVG(distance_miles) WHERE city={city!r} = {exp_avg}")
        else:
            fail(f"AVG(distance_miles) WHERE city={city!r} = {actual}, expected {exp_avg}")

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
