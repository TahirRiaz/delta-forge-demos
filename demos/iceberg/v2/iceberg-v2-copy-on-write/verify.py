#!/usr/bin/env python3
"""
Iceberg V2 Copy-on-Write — Data Verification
==============================================
Reads the shipments table through the Iceberg metadata chain and verifies
the final 110-row state after copy-on-write UPDATE and DELETE operations.

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
        description="Verify Iceberg data for iceberg-v2-copy-on-write demo"
    )
    parser.add_argument("data_root", help="Parent folder containing shipments/")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show sample data")
    args = parser.parse_args()

    import pyarrow.compute as pc

    data_root = os.path.abspath(args.data_root)
    table_path = os.path.join(data_root, "shipments")

    print_header("Copy-on-Write (Shipments) — Data Verification")
    print(f"  Data root: {data_root}")

    if not os.path.isdir(os.path.join(table_path, "metadata")):
        print(f"\nError: {table_path}/metadata not found")
        sys.exit(1)

    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg metadata chain")

    if args.verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 110)

    # Status breakdown
    assert_count_where(table, "status", "Delivered", 70)
    assert_count_where(table, "status", "In Transit", 15)
    assert_count_where(table, "status", "Processing", 14)
    assert_count_where(table, "status", "Returned", 11)

    # Carrier analysis
    for carrier, cnt, avg_cost in [("DHL", 30, 230.84), ("FedEx", 32, 265.77),
                                    ("UPS", 24, 295.16), ("USPS", 24, 259.04)]:
        assert_count_where(table, "carrier", carrier, cnt)
        mask = pc.equal(table.column("carrier"), carrier)
        filtered = table.filter(mask)
        actual_avg = round(pc.mean(filtered.column("shipping_cost")).as_py(), 2)
        if actual_avg == avg_cost:
            ok(f"AVG(shipping_cost) WHERE carrier={carrier!r} = {avg_cost}")
        else:
            fail(f"AVG(shipping_cost) WHERE carrier={carrier!r} = {actual_avg}, expected {avg_cost}")

    # Deleted shipments absent
    deleted_ids = ['SHP-0009', 'SHP-0012', 'SHP-0015', 'SHP-0017', 'SHP-0020',
                   'SHP-0022', 'SHP-0029', 'SHP-0031', 'SHP-0032', 'SHP-0036']
    import pyarrow as pa
    del_mask = pc.is_in(table.column("shipment_id"), value_set=pa.array(deleted_ids))
    del_count = pc.sum(del_mask).as_py()
    if del_count == 0:
        ok(f"Deleted shipments not present (0 of 10)")
    else:
        fail(f"Found {del_count} deleted shipments, expected 0")

    # Delivered with actual_delivery
    delivered_with_date = pc.sum(pc.is_valid(table.column("actual_delivery"))).as_py()
    if delivered_with_date == 70:
        ok(f"Delivered with actual_delivery = 70")
    else:
        fail(f"Delivered with actual_delivery = {delivered_with_date}, expected 70")

    # Priority breakdown
    assert_count_where(table, "priority", "Express", 43)
    assert_count_where(table, "priority", "Overnight", 33)
    assert_count_where(table, "priority", "Standard", 34)

    # Distinct counts
    assert_distinct_count(table, "carrier", 4)
    assert_distinct_count(table, "status", 4)
    assert_distinct_count(table, "priority", 3)

    # Financial totals
    assert_sum(table, "shipping_cost", 28730.35, label="total shipping cost")
    assert_avg(table, "weight_kg", 67.44, label="avg weight")

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
