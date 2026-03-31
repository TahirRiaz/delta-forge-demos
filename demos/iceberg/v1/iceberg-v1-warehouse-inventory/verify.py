#!/usr/bin/env python3
"""
Iceberg V1 Warehouse Inventory — Data Verification
=====================================================
Reads the warehouse_inventory table through the native Iceberg v1 metadata
chain and verifies the data matches expected values. This is a read-only
dataset (no DML mutations) — 489 SKUs across 3 warehouses and 5 categories.

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
    assert_row_count, assert_sum, assert_avg, assert_distinct_count,
    assert_count_where, assert_format_version,
)
from verify_lib import print_header, print_section, print_summary, exit_with_status
from verify_lib.assertions import CYAN, RESET


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
def verify_warehouse_inventory(data_root, verbose=False):
    print_section("warehouse_inventory — Iceberg V1 Read-Only")

    table_path = os.path.join(data_root, "warehouse_inventory")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")
        info("First 3 rows:")
        sample = table.slice(0, min(3, table.num_rows)).to_pydict()
        for i in range(min(3, table.num_rows)):
            row = {c: sample[c][i] for c in table.column_names}
            info(f"    {row}")

    # This is a native Iceberg v1 table
    assert_format_version(metadata, 1)

    # Total rows
    assert_row_count(table, 489)

    # Distinct counts
    print(f"\n  {CYAN}Distinct counts:{RESET}")
    assert_distinct_count(table, "warehouse", 3)
    assert_distinct_count(table, "category", 5)
    assert_distinct_count(table, "supplier", 5)

    # Per-warehouse counts
    print(f"\n  {CYAN}Per-warehouse counts:{RESET}")
    assert_count_where(table, "warehouse", "Charlotte-NC", 159)
    assert_count_where(table, "warehouse", "Dallas-TX", 166)
    assert_count_where(table, "warehouse", "Portland-OR", 164)

    # Per-category counts
    print(f"\n  {CYAN}Per-category counts:{RESET}")
    assert_count_where(table, "category", "Apparel", 100)
    assert_count_where(table, "category", "Electronics", 99)
    assert_count_where(table, "category", "Food-Bev", 97)
    assert_count_where(table, "category", "Furniture", 94)
    assert_count_where(table, "category", "Industrial", 99)

    # Total inventory value = SUM(quantity_on_hand * unit_cost)
    print(f"\n  {CYAN}Inventory value:{RESET}")
    import pyarrow.compute as pc
    qty = table.column("quantity_on_hand")
    cost = table.column("unit_cost")
    product = pc.multiply(pc.cast(qty, "float64"), cost)
    total_value = round(pc.sum(product).as_py(), 2)
    if total_value == 17554271.58:
        ok(f"Total inventory value = 17554271.58")
    else:
        fail(f"Total inventory value = {total_value}, expected 17554271.58")

    # Per-warehouse inventory values
    print(f"\n  {CYAN}Per-warehouse inventory values:{RESET}")
    for wh, expected_value in [("Charlotte-NC", 5047746.44), ("Dallas-TX", 6234098.71),
                                ("Portland-OR", 6272426.43)]:
        mask = pc.equal(table.column("warehouse"), wh)
        filtered = table.filter(mask)
        q = filtered.column("quantity_on_hand")
        c = filtered.column("unit_cost")
        p = pc.multiply(pc.cast(q, "float64"), c)
        actual = round(pc.sum(p).as_py(), 2)
        if actual == expected_value:
            ok(f"Inventory value WHERE warehouse={wh!r} = {expected_value}")
        else:
            fail(f"Inventory value WHERE warehouse={wh!r} = {actual}, expected {expected_value}")

    # Items below reorder point
    print(f"\n  {CYAN}Reorder analysis:{RESET}")
    below_reorder = pc.less(table.column("quantity_on_hand"), table.column("reorder_point"))
    below_count = pc.sum(below_reorder).as_py()
    if below_count == 56:
        ok(f"Items below reorder point = 56")
    else:
        fail(f"Items below reorder point = {below_count}, expected 56")

    # Per-warehouse below reorder
    for wh, expected in [("Charlotte-NC", 21), ("Dallas-TX", 15), ("Portland-OR", 20)]:
        mask = pc.and_(pc.equal(table.column("warehouse"), wh), below_reorder)
        actual = pc.sum(mask).as_py()
        if actual == expected:
            ok(f"Below reorder WHERE warehouse={wh!r} = {expected}")
        else:
            fail(f"Below reorder WHERE warehouse={wh!r} = {actual}, expected {expected}")

    # Average unit cost by category
    print(f"\n  {CYAN}Average unit cost by category:{RESET}")
    for cat, expected_avg in [("Apparel", 137.18), ("Electronics", 148.05),
                               ("Food-Bev", 144.74), ("Furniture", 137.43),
                               ("Industrial", 150.28)]:
        mask = pc.equal(table.column("category"), cat)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("unit_cost")).as_py(), 2)
        if actual == expected_avg:
            ok(f"AVG(unit_cost) WHERE category={cat!r} = {expected_avg}")
        else:
            fail(f"AVG(unit_cost) WHERE category={cat!r} = {actual}, expected {expected_avg}")

    # Supplier distribution
    print(f"\n  {CYAN}Supplier distribution:{RESET}")
    assert_count_where(table, "supplier", "Acme Corp", 88)
    assert_count_where(table, "supplier", "EcoSupply", 101)
    assert_count_where(table, "supplier", "GlobalTrade", 82)
    assert_count_where(table, "supplier", "PrimeParts", 108)
    assert_count_where(table, "supplier", "QuickShip", 110)

    # High-value items (qty * cost > 10000)
    print(f"\n  {CYAN}High-value items:{RESET}")
    high_value = pc.greater(product, 10000)
    hv_count = pc.sum(high_value).as_py()
    if hv_count == 372:
        ok(f"Items with line_value > 10000 = 372")
    else:
        fail(f"Items with line_value > 10000 = {hv_count}, expected 372")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v1-warehouse-inventory demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing warehouse_inventory/"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show sample data and column names"
    )
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg V1 Warehouse Inventory — Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "warehouse_inventory")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_warehouse_inventory(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
