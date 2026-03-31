#!/usr/bin/env python3
"""
Iceberg V2 Retail Multi-Dimensional Aggregation — Data Verification
=====================================================================
Reads the retail_sales table through the Iceberg metadata chain and
verifies 120 retail transactions with aggregation analytics.

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
    assert_row_count, assert_sum,
    assert_distinct_count, assert_count_where, assert_format_version,
    reset_counters,
)
from verify_lib import print_header, print_summary, exit_with_status


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v2-aggregate-analytics demo"
    )
    parser.add_argument("data_root", help="Parent folder containing retail_sales/")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show sample data")
    args = parser.parse_args()

    import pyarrow.compute as pc

    data_root = os.path.abspath(args.data_root)
    table_path = os.path.join(data_root, "retail_sales")

    print_header("Aggregate Analytics (Retail) — Data Verification")
    print(f"  Data root: {data_root}")

    if not os.path.isdir(os.path.join(table_path, "metadata")):
        print(f"\nError: {table_path}/metadata not found")
        sys.exit(1)

    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg metadata chain")

    if args.verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 120)

    # Compute gross revenue = quantity * unit_price
    gross_values = [
        round(table.column("quantity")[i].as_py() * table.column("unit_price")[i].as_py(), 2)
        for i in range(table.num_rows)
    ]
    gross_revenue = round(sum(gross_values), 2)
    if gross_revenue == 25506.46:
        ok(f"Gross revenue = 25506.46")
    else:
        fail(f"Gross revenue = {gross_revenue}, expected 25506.46")

    # Net revenue = gross * (1 - discount/100)
    net_values = [
        round(
            table.column("quantity")[i].as_py()
            * table.column("unit_price")[i].as_py()
            * (1 - table.column("discount_pct")[i].as_py() / 100),
            2
        )
        for i in range(table.num_rows)
    ]
    net_revenue = round(sum(net_values), 2)
    if net_revenue == 23220.27:
        ok(f"Net revenue = 23220.27")
    else:
        fail(f"Net revenue = {net_revenue}, expected 23220.27")

    # Total units
    assert_sum(table, "quantity", 529.0, label="total units")

    # Return count
    assert_sum(table, "is_return", 9.0, label="return count")

    # Distinct counts
    assert_distinct_count(table, "store_name", 4)
    assert_distinct_count(table, "region", 3)
    assert_distinct_count(table, "category", 5)

    # Per-region counts
    assert_count_where(table, "region", "Central", 39)
    assert_count_where(table, "region", "East", 50)
    assert_count_where(table, "region", "West", 31)

    # Per-category counts
    assert_count_where(table, "category", "Clothing", 28)
    assert_count_where(table, "category", "Electronics", 20)

    # Per-store counts
    assert_count_where(table, "store_name", "Downtown Flagship", 23)
    assert_count_where(table, "store_name", "Lakefront Center", 39)
    assert_count_where(table, "store_name", "Midtown Express", 27)
    assert_count_where(table, "store_name", "Westside Mall", 31)

    # Discounted sales count (discount_pct > 0)
    disc_count = pc.sum(pc.greater(table.column("discount_pct"), 0)).as_py()
    if disc_count == 54:
        ok(f"Discounted sales = 54")
    else:
        fail(f"Discounted sales = {disc_count}, expected 54")

    # Per-region gross revenue
    for region, exp_gross in [("Central", 8617.99), ("East", 9098.57), ("West", 7789.90)]:
        mask = pc.equal(table.column("region"), region)
        filtered = table.filter(mask)
        gross = round(sum(
            filtered.column("quantity")[i].as_py() * filtered.column("unit_price")[i].as_py()
            for i in range(filtered.num_rows)
        ), 2)
        if gross == exp_gross:
            ok(f"Gross revenue WHERE region={region!r} = {exp_gross}")
        else:
            fail(f"Gross revenue WHERE region={region!r} = {gross}, expected {exp_gross}")

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
