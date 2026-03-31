#!/usr/bin/env python3
"""
Iceberg Native Large Manifests (Web Analytics) — Data Verification
===================================================================
Reads the web_analytics table through the Iceberg metadata chain and
verifies 600 session records across 10 manifest entries match expected values.

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
    assert_row_count, assert_sum, assert_min, assert_max,
    assert_distinct_count, assert_count_where, assert_format_version,
    reset_counters,
)
from verify_lib import print_header, print_summary, exit_with_status


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-native-large-manifests demo"
    )
    parser.add_argument("data_root", help="Parent folder containing web_analytics/")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show sample data")
    args = parser.parse_args()

    import pyarrow.compute as pc

    data_root = os.path.abspath(args.data_root)
    table_path = os.path.join(data_root, "web_analytics")

    print_header("Large Manifests (Web Analytics) — Data Verification")
    print(f"  Data root: {data_root}")

    if not os.path.isdir(os.path.join(table_path, "metadata")):
        print(f"\nError: {table_path}/metadata not found")
        sys.exit(1)

    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg metadata chain")

    if args.verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 600)

    # Per-country breakdown
    for country, cnt in [("AU", 48), ("BR", 66), ("CA", 76), ("DE", 48), ("FR", 71),
                          ("IN", 65), ("JP", 58), ("MX", 57), ("UK", 64), ("US", 47)]:
        assert_count_where(table, "country", country, cnt)

    # Per-device breakdown
    assert_count_where(table, "device_type", "desktop", 194)
    assert_count_where(table, "device_type", "mobile", 204)
    assert_count_where(table, "device_type", "tablet", 202)

    # Bounce count
    bounce_mask = pc.equal(table.column("is_bounce"), True)
    bounce_count = pc.sum(bounce_mask).as_py()
    if bounce_count == 165:
        ok(f"Bounce count = 165")
    else:
        fail(f"Bounce count = {bounce_count}, expected 165")

    # Distinct counts
    assert_distinct_count(table, "country", 10)
    assert_distinct_count(table, "device_type", 3)
    assert_distinct_count(table, "referrer", 10)

    # Total events
    assert_sum(table, "event_count", 6023.0, label="total events")

    # Total time on page
    assert_sum(table, "time_on_page", 173924.0, label="total time_on_page")

    # Min/max event count
    assert_min(table, "event_count", 1)
    assert_max(table, "event_count", 25)

    # Min/max time_on_page
    assert_min(table, "time_on_page", 3)
    assert_max(table, "time_on_page", 599)

    # Per-referrer counts
    for ref, cnt in [("bing.com", 75), ("facebook.com", 75), ("github.com", 74),
                      ("reddit.com", 63), ("google.com", 60), ("twitter.com", 60),
                      ("linkedin.com", 56), ("direct", 48), ("youtube.com", 46),
                      ("email-campaign", 43)]:
        assert_count_where(table, "referrer", ref, cnt)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
