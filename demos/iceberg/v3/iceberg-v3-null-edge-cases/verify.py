#!/usr/bin/env python3
"""
Iceberg V3 Clinical Lab NULL Edge Cases -- Data Verification
==============================================================
Reads the lab_results table through native Iceberg V3 metadata.
50 rows with intentional NULLs across 7 columns. Verifies NULL counts,
aggregate behavior, and per-test statistics.

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
    assert_row_count, assert_distinct_count, assert_format_version)
from verify_lib import print_header, print_section, print_summary, exit_with_status


def verify_lab_results(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("lab_results -- NULL Edge Cases")

    table_path = os.path.join(data_root, "lab_results")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 3)
    assert_row_count(table, 50)

    # NULL counts per column
    null_checks = [
        ("result_value", 5),
        ("unit", 2),
        ("reference_low", 3),
        ("is_critical", 12),
        ("lab_technician", 7),
        ("notes", 37),
    ]
    for col, expected_nulls in null_checks:
        null_count = pc.sum(pc.is_null(table.column(col))).as_py()
        if null_count == expected_nulls:
            ok(f"NULL count for {col} = {expected_nulls}")
        else:
            fail(f"NULL count for {col} = {null_count}, expected {expected_nulls}")

    # COUNT(column) = non-null count
    count_result = pc.count(table.column("result_value")).as_py()
    if count_result == 45:
        ok(f"COUNT(result_value) = 45 (excludes NULLs)")
    else:
        fail(f"COUNT(result_value) = {count_result}, expected 45")

    count_unit = pc.count(table.column("unit")).as_py()
    if count_unit == 48:
        ok(f"COUNT(unit) = 48")
    else:
        fail(f"COUNT(unit) = {count_unit}, expected 48")

    count_notes = pc.count(table.column("notes")).as_py()
    if count_notes == 13:
        ok(f"COUNT(notes) = 13")
    else:
        fail(f"COUNT(notes) = {count_notes}, expected 13")

    # Aggregates on result_value (NULLs skipped)
    avg_result = round(pc.mean(table.column("result_value")).as_py(), 2)
    min_result = round(pc.min(table.column("result_value"), skip_nulls=True).as_py(), 2)
    max_result = round(pc.max(table.column("result_value"), skip_nulls=True).as_py(), 2)
    sum_result = round(pc.sum(table.column("result_value")).as_py(), 2)

    if avg_result == 100.73:
        ok(f"AVG(result_value) = 100.73")
    else:
        fail(f"AVG(result_value) = {avg_result}, expected 100.73")
    if min_result == 0.1:
        ok(f"MIN(result_value) = 0.1")
    else:
        fail(f"MIN(result_value) = {min_result}, expected 0.1")
    if max_result == 567.17:
        ok(f"MAX(result_value) = 567.17")
    else:
        fail(f"MAX(result_value) = {max_result}, expected 567.17")
    if sum_result == 4532.72:
        ok(f"SUM(result_value) = 4532.72")
    else:
        fail(f"SUM(result_value) = {sum_result}, expected 4532.72")

    # is_critical classification: 1=critical(24), 0=normal(14), NULL=unknown(12)
    # is_critical may be stored as int or bool
    col_critical = table.column("is_critical")
    # Count NULLs
    null_critical = pc.sum(pc.is_null(col_critical)).as_py()
    non_null = table.filter(pc.is_valid(col_critical))
    critical_col = non_null.column("is_critical")
    # Cast to int for comparison
    import pyarrow as pa
    try:
        critical_int = pc.cast(critical_col, pa.int64())
        critical_count = pc.sum(pc.equal(critical_int, 1)).as_py()
        normal_count = pc.sum(pc.equal(critical_int, 0)).as_py()
    except Exception:
        # Boolean type
        critical_count = pc.sum(critical_col).as_py()
        normal_count = non_null.num_rows - critical_count

    if critical_count == 24:
        ok(f"Critical count (is_critical=1) = 24")
    else:
        fail(f"Critical count = {critical_count}, expected 24")
    if normal_count == 14:
        ok(f"Normal count (is_critical=0) = 14")
    else:
        fail(f"Normal count = {normal_count}, expected 14")
    if null_critical == 12:
        ok(f"Unknown count (is_critical=NULL) = 12")
    else:
        fail(f"Unknown count = {null_critical}, expected 12")

    # NULL lab_technician = 7 (automated runs)
    null_tech = pc.sum(pc.is_null(table.column("lab_technician"))).as_py()
    if null_tech == 7:
        ok(f"Automated runs (NULL lab_technician) = 7")
    else:
        fail(f"Automated runs = {null_tech}, expected 7")

    # Distinct counts
    assert_distinct_count(table, "test_name", 10)
    assert_distinct_count(table, "patient_name", 15)

    # Per-test has_result counts
    for test, expected_has in [("Hemoglobin", 5), ("Glucose", 4), ("Platelet Count", 3)]:
        mask = pc.equal(table.column("test_name"), test)
        filtered = table.filter(mask)
        has_result = pc.count(filtered.column("result_value")).as_py()
        if has_result == expected_has:
            ok(f"COUNT(result_value) WHERE test_name={test!r} = {expected_has}")
        else:
            fail(f"COUNT(result_value) WHERE test_name={test!r} = {has_result}, expected {expected_has}")

    # Per-test averages (on non-null results)
    for test, expected_avg in [("Hemoglobin", 11.87), ("Glucose", 168.75)]:
        mask = pc.equal(table.column("test_name"), test)
        filtered = table.filter(mask)
        avg_val = round(pc.mean(filtered.column("result_value")).as_py(), 2)
        if avg_val == expected_avg:
            ok(f"AVG(result_value) WHERE test_name={test!r} = {expected_avg}")
        else:
            fail(f"AVG(result_value) WHERE test_name={test!r} = {avg_val}, expected {expected_avg}")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v3-null-edge-cases demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing lab_results/"
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg V3 NULL Edge Cases -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "lab_results")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_lab_results(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
