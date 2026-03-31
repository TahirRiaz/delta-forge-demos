#!/usr/bin/env python3
"""
Iceberg V2 Complex Types — Data Verification
==============================================
Reads the orders table through the Iceberg metadata chain and verifies
100 e-commerce orders with nested STRUCT and ARRAY<STRUCT> columns.

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
    assert_count_where, assert_format_version,
    reset_counters,
)
from verify_lib import print_header, print_summary, exit_with_status


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v2-complex-types demo"
    )
    parser.add_argument("data_root", help="Parent folder containing orders/")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show sample data")
    args = parser.parse_args()

    import pyarrow.compute as pc
    import pyarrow as pa

    data_root = os.path.abspath(args.data_root)
    table_path = os.path.join(data_root, "orders")

    print_header("Complex Types — Data Verification")
    print(f"  Data root: {data_root}")

    if not os.path.isdir(os.path.join(table_path, "metadata")):
        print(f"\nError: {table_path}/metadata not found")
        sys.exit(1)

    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg metadata chain")

    if args.verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 100)

    # Status breakdown
    assert_count_where(table, "status", "Cancelled", 6)
    assert_count_where(table, "status", "Delivered", 43)
    assert_count_where(table, "status", "Processing", 15)
    assert_count_where(table, "status", "Shipped", 36)

    # Total order_total
    assert_sum(table, "order_total", 111547.70, label="sum order_total")

    # Count total line items via items array
    total_items = 0
    items_col = table.column("items")
    for i in range(table.num_rows):
        arr = items_col[i].as_py()
        if arr is not None:
            total_items += len(arr)
    if total_items == 311:
        ok(f"Total line items = 311")
    else:
        fail(f"Total line items = {total_items}, expected 311")

    # Distinct cities via shipping_address struct
    cities = set()
    addr_col = table.column("shipping_address")
    for i in range(table.num_rows):
        addr = addr_col[i].as_py()
        if addr is not None and "city" in addr:
            cities.add(addr["city"])
    if len(cities) == 15:
        ok(f"Distinct cities = 15")
    else:
        fail(f"Distinct cities = {len(cities)}, expected 15")

    # Status avg_total
    for status, exp_avg in [("Cancelled", 1758.66), ("Delivered", 1039.51),
                             ("Processing", 1329.27), ("Shipped", 1009.94)]:
        mask = pc.equal(table.column("status"), status)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("order_total")).as_py(), 2)
        if actual == exp_avg:
            ok(f"AVG(order_total) WHERE status={status!r} = {exp_avg}")
        else:
            fail(f"AVG(order_total) WHERE status={status!r} = {actual}, expected {exp_avg}")

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
