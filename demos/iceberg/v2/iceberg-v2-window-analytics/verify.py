#!/usr/bin/env python3
"""
Iceberg V2 Airline Loyalty Window Analytics — Data Verification
================================================================
Reads the loyalty_members table through the Iceberg metadata chain and
verifies 60 frequent flyer members across 4 tiers and 5 airports.

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
    assert_row_count, assert_sum, assert_avg,
    assert_distinct_count, assert_count_where, assert_format_version,
    reset_counters,
)
from verify_lib import print_header, print_summary, exit_with_status


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v2-window-analytics demo"
    )
    parser.add_argument("data_root", help="Parent folder containing loyalty_members/")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show sample data")
    args = parser.parse_args()

    import pyarrow.compute as pc

    data_root = os.path.abspath(args.data_root)
    table_path = os.path.join(data_root, "loyalty_members")

    print_header("Window Analytics (Loyalty) — Data Verification")
    print(f"  Data root: {data_root}")

    if not os.path.isdir(os.path.join(table_path, "metadata")):
        print(f"\nError: {table_path}/metadata not found")
        sys.exit(1)

    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg metadata chain")

    if args.verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 60)

    # Tier breakdown
    assert_count_where(table, "tier", "Bronze", 20)
    assert_count_where(table, "tier", "Silver", 18)
    assert_count_where(table, "tier", "Gold", 14)
    assert_count_where(table, "tier", "Platinum", 8)

    # Per-airport breakdown (12 each)
    assert_count_where(table, "home_airport", "ATL", 12)
    assert_count_where(table, "home_airport", "DFW", 12)
    assert_count_where(table, "home_airport", "JFK", 12)
    assert_count_where(table, "home_airport", "LAX", 12)
    assert_count_where(table, "home_airport", "ORD", 12)

    # Grand totals
    assert_sum(table, "miles_ytd", 2685394.0, label="total miles")
    assert_sum(table, "flights_ytd", 1682.0, label="total flights")
    assert_sum(table, "spend_ytd", 615209.16, label="total spend")
    assert_avg(table, "spend_ytd", 10253.49, label="avg spend")

    # Distinct counts
    assert_distinct_count(table, "tier", 4)
    assert_distinct_count(table, "home_airport", 5)

    # Platinum total spend
    plat = table.filter(pc.equal(table.column("tier"), "Platinum"))
    plat_spend = round(pc.sum(plat.column("spend_ytd")).as_py(), 2)
    if plat_spend == 294131.32:
        ok(f"Platinum total spend = 294131.32")
    else:
        fail(f"Platinum total spend = {plat_spend}, expected 294131.32")

    # Top member by miles (member_id=55, Leo Park, 198664 miles)
    sorted_miles = pc.sort_indices(table.column("miles_ytd"), sort_keys=[("miles_ytd", "descending")])
    top_idx = sorted_miles[0].as_py()
    top_miles = table.column("miles_ytd")[top_idx].as_py()
    top_member_id = table.column("member_id")[top_idx].as_py()
    if top_miles == 198664:
        ok(f"Top member miles_ytd = 198664")
    else:
        fail(f"Top member miles_ytd = {top_miles}, expected 198664")
    if top_member_id == 55:
        ok(f"Top member member_id = 55")
    else:
        fail(f"Top member member_id = {top_member_id}, expected 55")

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
