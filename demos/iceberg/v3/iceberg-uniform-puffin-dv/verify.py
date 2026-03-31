#!/usr/bin/env python3
"""
Iceberg UniForm Puffin Deletion Vector Write & Read -- Data Verification
=========================================================================
Reads the products table through Iceberg metadata after 3 products were
deleted (IDs 2, 5, 8), leaving 7 of 10 products. Verifies per-row values,
category distribution, and total price.

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
    assert_row_count, assert_sum, assert_count_where, assert_value_where,
    assert_format_version)
from verify_lib import print_header, print_section, print_summary, exit_with_status


def assert_count_where_in(table, filter_col, filter_vals, expected, label=""):
    import pyarrow.compute as pc
    mask = pc.is_in(table.column(filter_col), value_set=filter_vals)
    actual = pc.sum(mask).as_py()
    ctx = f" ({label})" if label else ""
    if actual == expected:
        ok(f"COUNT WHERE {filter_col} IN ... = {expected}{ctx}")
    else:
        fail(f"COUNT WHERE {filter_col} IN ... = {actual}, expected {expected}{ctx}")


def verify_products(data_root, verbose=False):
    import pyarrow as pa
    import pyarrow.compute as pc

    print_section("puffin_dv_products -- Puffin DV Post-Delete State")

    table_path = os.path.join(data_root, "puffin_dv_products")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    # 10 - 3 deleted = 7
    assert_row_count(table, 7)

    # Deleted IDs absent
    deleted_ids = pa.array([2, 5, 8])
    assert_count_where_in(table, "id", deleted_ids, 0, "deleted products absent")

    # Total price of remaining: 2813.48
    assert_sum(table, "price", 2813.48)

    # Per-row spot checks
    assert_value_where(table, "name", "Quantum Widget", "id", 1)
    assert_value_where(table, "price", 299.99, "id", 1)
    assert_value_where(table, "name", "Bio Reactor Kit", "id", 3)
    assert_value_where(table, "price", 599.0, "id", 3)
    assert_value_where(table, "name", "Solar Panel Mini", "id", 4)
    assert_value_where(table, "price", 425.0, "id", 4)
    assert_value_where(table, "name", "LED Matrix Board", "id", 6)
    assert_value_where(table, "price", 175.0, "id", 6)
    assert_value_where(table, "name", "Thermal Coupler", "id", 7)
    assert_value_where(table, "price", 64.5, "id", 7)
    assert_value_where(table, "name", "Wind Turbine Blade", "id", 9)
    assert_value_where(table, "price", 850.0, "id", 9)
    assert_value_where(table, "name", "Plasma Cutter Pro", "id", 10)
    assert_value_where(table, "price", 399.99, "id", 10)

    # Category distribution
    assert_count_where(table, "category", "Electronics", 2)
    assert_count_where(table, "category", "Energy", 2)
    assert_count_where(table, "category", "Industrial", 2)
    assert_count_where(table, "category", "Science", 1)


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-uniform-puffin-dv demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing puffin_dv_products/"
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg UniForm Puffin DV -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "puffin_dv_products")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_products(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
