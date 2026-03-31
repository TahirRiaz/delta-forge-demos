#!/usr/bin/env python3
"""
Iceberg Native Partition Transforms — Data Verification
=========================================================
Reads the network_traffic table through the Iceberg metadata chain and
verifies 480 packet records with bucket(8, source_ip) + days(capture_time)
partitioning match expected values.

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
    assert_row_count, assert_min, assert_max,
    assert_distinct_count, assert_count_where, assert_format_version,
    reset_counters,
)
from verify_lib import print_header, print_summary, exit_with_status


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-native-partition-transforms demo"
    )
    parser.add_argument("data_root", help="Parent folder containing network_traffic/")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show sample data")
    args = parser.parse_args()

    import pyarrow.compute as pc

    data_root = os.path.abspath(args.data_root)
    table_path = os.path.join(data_root, "network_traffic")

    print_header("Partition Transforms — Data Verification")
    print(f"  Data root: {data_root}")

    if not os.path.isdir(os.path.join(table_path, "metadata")):
        print(f"\nError: {table_path}/metadata not found")
        sys.exit(1)

    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg metadata chain")

    if args.verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 480)

    # Per-region breakdown (160 each)
    assert_count_where(table, "region", "asia-pacific", 160)
    assert_count_where(table, "region", "europe", 160)
    assert_count_where(table, "region", "north-america", 160)

    # Per-protocol breakdown (120 each)
    assert_count_where(table, "protocol", "DNS", 120)
    assert_count_where(table, "protocol", "ICMP", 120)
    assert_count_where(table, "protocol", "TCP", 120)
    assert_count_where(table, "protocol", "UDP", 120)

    # Threat level distribution
    assert_count_where(table, "threat_level", "critical", 22)
    assert_count_where(table, "threat_level", "high", 51)
    assert_count_where(table, "threat_level", "low", 248)
    assert_count_where(table, "threat_level", "medium", 159)

    # Bytes transferred aggregations
    total_bytes = pc.sum(table.column("bytes_transferred")).as_py()
    if total_bytes == 251040311:
        ok(f"SUM(bytes_transferred) = 251040311")
    else:
        fail(f"SUM(bytes_transferred) = {total_bytes}, expected 251040311")

    avg_bytes = round(pc.mean(table.column("bytes_transferred")).as_py(), 2)
    if avg_bytes == 523000.65:
        ok(f"AVG(bytes_transferred) = 523000.65")
    else:
        fail(f"AVG(bytes_transferred) = {avg_bytes}, expected 523000.65")

    assert_min(table, "bytes_transferred", 1837)
    assert_max(table, "bytes_transferred", 1047983)

    # Per-region bytes
    for region, exp_bytes in [("asia-pacific", 85974312), ("europe", 80023887), ("north-america", 85042112)]:
        mask = pc.equal(table.column("region"), region)
        filtered = table.filter(mask)
        actual = pc.sum(filtered.column("bytes_transferred")).as_py()
        if actual == exp_bytes:
            ok(f"SUM(bytes_transferred) WHERE region={region!r} = {exp_bytes}")
        else:
            fail(f"SUM(bytes_transferred) WHERE region={region!r} = {actual}, expected {exp_bytes}")

    # Distinct counts
    assert_distinct_count(table, "region", 3)
    assert_distinct_count(table, "protocol", 4)
    assert_distinct_count(table, "threat_level", 4)

    # High port traffic (port > 8000)
    high_port = table.filter(pc.greater(table.column("port"), 8000))
    assert_row_count(high_port, 122, label="port > 8000")

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
