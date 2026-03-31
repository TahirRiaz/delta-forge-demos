#!/usr/bin/env python3
"""
Iceberg V3 Position Delete Files -- Data Verification
=======================================================
Reads the equity_trades table through Iceberg metadata (V2 upgraded to V3).
24 erroneous ALGO-X99 trades were position-deleted, leaving 456 of 480.

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
    assert_row_count, assert_sum, assert_avg, assert_count_where,
    assert_distinct_count, assert_format_version)
from verify_lib import print_header, print_section, print_summary, exit_with_status


def verify_equity_trades(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("equity_trades -- Position Delete Post-ALGO-X99")

    table_path = os.path.join(data_root, "equity_trades")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 3)

    # 480 - 24 ALGO-X99 = 456
    assert_row_count(table, 456)

    # Erroneous trader removed
    assert_count_where(table, "trader", "ALGO-X99", 0)

    # Per-exchange counts
    assert_count_where(table, "exchange", "LSE", 120)
    assert_count_where(table, "exchange", "NASDAQ", 120)
    assert_count_where(table, "exchange", "NYSE", 96)
    assert_count_where(table, "exchange", "TSE", 120)

    # Distinct traders: 10 (11 - ALGO-X99)
    assert_distinct_count(table, "trader", 10)

    # Per-exchange notional
    for exchange, expected_notional in [
        ("LSE", 72660043.35), ("NASDAQ", 77910888.46),
        ("NYSE", 61730838.44), ("TSE", 82249678.63)
    ]:
        mask = pc.equal(table.column("exchange"), exchange)
        filtered = table.filter(mask)
        actual = round(pc.sum(filtered.column("notional")).as_py(), 2)
        if actual == expected_notional:
            ok(f"SUM(notional) WHERE exchange={exchange!r} = {expected_notional}")
        else:
            fail(f"SUM(notional) WHERE exchange={exchange!r} = {actual}, expected {expected_notional}")

    # Buy vs sell
    assert_count_where(table, "side", "BUY", 237)
    assert_count_where(table, "side", "SELL", 219)

    # Average price by exchange
    for exchange, expected_avg in [("LSE", 251.21), ("NASDAQ", 233.7),
                                    ("NYSE", 249.17), ("TSE", 256.13)]:
        mask = pc.equal(table.column("exchange"), exchange)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("price")).as_py(), 2)
        if actual == expected_avg:
            ok(f"AVG(price) WHERE exchange={exchange!r} = {expected_avg}")
        else:
            fail(f"AVG(price) WHERE exchange={exchange!r} = {actual}, expected {expected_avg}")

    # Overall price stats
    assert_avg(table, "price", 247.47)
    min_price = round(pc.min(table.column("price")).as_py(), 2)
    max_price = round(pc.max(table.column("price")).as_py(), 2)
    if min_price == 10.3:
        ok(f"MIN(price) = 10.3")
    else:
        fail(f"MIN(price) = {min_price}, expected 10.3")
    if max_price == 499.03:
        ok(f"MAX(price) = 499.03")
    else:
        fail(f"MAX(price) = {max_price}, expected 499.03")

    # High-value trades (>100K notional)
    high_value = pc.sum(pc.greater(table.column("notional"), 100000)).as_py()
    if high_value == 385:
        ok(f"High-value trades (>$100K) = 385")
    else:
        fail(f"High-value trades (>$100K) = {high_value}, expected 385")

    # Distinct entity counts
    assert_distinct_count(table, "trade_id", 456)
    assert_distinct_count(table, "exchange", 4)
    assert_distinct_count(table, "symbol", 20)
    assert_distinct_count(table, "trader", 10)


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v3-position-deletes demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing equity_trades/"
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg V3 Position Deletes -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "equity_trades")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_equity_trades(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
