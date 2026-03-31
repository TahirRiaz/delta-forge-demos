#!/usr/bin/env python3
"""
Iceberg V3 Deletion Vectors (Puffin) -- Data Verification
===========================================================
Reads the shipment_manifests table through native Iceberg V3 metadata.
36 faulty SCAN-ERR rows were deleted via Puffin DV, leaving 504 of 540.

Usage:
    python verify.py <data_root_path> [--verbose]

Requirements:
    pip install pyiceberg[pyarrow] fastavro
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (read_iceberg_table, ok, fail, info,
    assert_row_count, assert_count_where, assert_distinct_count,
    assert_format_version)
from verify_lib import print_header, print_section, print_summary, exit_with_status


def verify_shipment_manifests(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("shipment_manifests -- Puffin DV Post-Delete")

    table_path = os.path.join(data_root, "shipment_manifests")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 3)

    # 540 - 36 SCAN-ERR = 504
    assert_row_count(table, 504)

    # Faulty scanner gone
    assert_count_where(table, "scanner_id", "SCAN-ERR", 0)

    # Per-region counts
    assert_count_where(table, "region", "Americas", 144)
    assert_count_where(table, "region", "EMEA", 180)
    assert_count_where(table, "region", "APAC", 180)

    # Distinct scanners: 15 (16 - SCAN-ERR)
    assert_distinct_count(table, "scanner_id", 15)

    # Per-category distribution
    assert_count_where(table, "product_category", "Automotive-Parts", 86)
    assert_count_where(table, "product_category", "Electronics", 107)
    assert_count_where(table, "product_category", "Heavy-Machinery", 73)
    assert_count_where(table, "product_category", "Perishable-Foods", 82)
    assert_count_where(table, "product_category", "Pharmaceuticals", 77)
    assert_count_where(table, "product_category", "Textiles", 79)

    # Hazardous counts by region
    for region, expected_cnt in [("Americas", 17), ("EMEA", 21), ("APAC", 21)]:
        mask_region = pc.equal(table.column("region"), region)
        mask_hazardous = pc.equal(table.column("is_hazardous"), True)
        combined = pc.and_(mask_region, mask_hazardous)
        actual = pc.sum(combined).as_py()
        if actual == expected_cnt:
            ok(f"Hazardous count WHERE region={region!r} = {expected_cnt}")
        else:
            fail(f"Hazardous count WHERE region={region!r} = {actual}, expected {expected_cnt}")

    # Total hazardous = 59
    total_hazardous = pc.sum(pc.equal(table.column("is_hazardous"), True)).as_py()
    if total_hazardous == 59:
        ok(f"Total hazardous = 59")
    else:
        fail(f"Total hazardous = {total_hazardous}, expected 59")

    # Weight stats
    avg_weight = round(pc.mean(table.column("weight_kg")).as_py(), 2)
    min_weight = round(pc.min(table.column("weight_kg")).as_py(), 2)
    max_weight = round(pc.max(table.column("weight_kg")).as_py(), 2)
    if avg_weight == 1279.27:
        ok(f"AVG(weight_kg) = 1279.27")
    else:
        fail(f"AVG(weight_kg) = {avg_weight}, expected 1279.27")
    if min_weight == 1.29:
        ok(f"MIN(weight_kg) = 1.29")
    else:
        fail(f"MIN(weight_kg) = {min_weight}, expected 1.29")
    if max_weight == 2499.03:
        ok(f"MAX(weight_kg) = 2499.03")
    else:
        fail(f"MAX(weight_kg) = {max_weight}, expected 2499.03")

    # Distinct entity counts
    assert_distinct_count(table, "shipment_id", 504)
    assert_distinct_count(table, "destination_country", 18)
    assert_distinct_count(table, "carrier", 12)
    assert_distinct_count(table, "product_category", 6)

    # Low-value shipments (<$500)
    low_value = pc.sum(pc.less(table.column("declared_value"), 500)).as_py()
    if low_value == 5:
        ok(f"Low-value shipments (<$500) = 5")
    else:
        fail(f"Low-value shipments (<$500) = {low_value}, expected 5")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v3-deletion-vectors demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing shipment_manifests/"
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg V3 Deletion Vectors -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "shipment_manifests")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_shipment_manifests(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
