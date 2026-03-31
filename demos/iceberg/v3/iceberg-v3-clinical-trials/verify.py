#!/usr/bin/env python3
"""
Iceberg V3 Clinical Trial Lab Results -- Data Verification
============================================================
Reads 480 clinical trial lab results through native Iceberg V3 metadata.
Read-only table: verifies row count, per-site/per-test counts, abnormal
result detection, analyst workload, and distinct entity counts.

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


def verify_lab_results(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("lab_results -- Clinical Trial V3 Read-Only")

    table_path = os.path.join(data_root, "iceberg_warehouse", "trials", "lab_results")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 3)
    assert_row_count(table, 480)

    # Per-site counts: 160 each
    assert_count_where(table, "site", "Boston-MGH", 160)
    assert_count_where(table, "site", "Houston-MD", 160)
    assert_count_where(table, "site", "Seattle-UW", 160)

    # Per-test counts: 120 each
    assert_count_where(table, "test_name", "ALT", 120)
    assert_count_where(table, "test_name", "Creatinine", 120)
    assert_count_where(table, "test_name", "Hemoglobin", 120)
    assert_count_where(table, "test_name", "Platelet-Count", 120)

    # Average result by test
    for test, expected_avg in [("ALT", 29.23), ("Creatinine", 0.89),
                                ("Hemoglobin", 14.71), ("Platelet-Count", 243.14)]:
        mask = pc.equal(table.column("test_name"), test)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("result_value")).as_py(), 2)
        if actual == expected_avg:
            ok(f"AVG(result_value) WHERE test_name={test!r} = {expected_avg}")
        else:
            fail(f"AVG(result_value) WHERE test_name={test!r} = {actual}, expected {expected_avg}")

    # Abnormal counts by test (is_abnormal = True)
    for test, expected_cnt in [("ALT", 15), ("Creatinine", 38),
                                ("Hemoglobin", 12), ("Platelet-Count", 19)]:
        mask_test = pc.equal(table.column("test_name"), test)
        mask_abnormal = pc.equal(table.column("is_abnormal"), True)
        combined = pc.and_(mask_test, mask_abnormal)
        actual = pc.sum(combined).as_py()
        if actual == expected_cnt:
            ok(f"Abnormal count WHERE test_name={test!r} = {expected_cnt}")
        else:
            fail(f"Abnormal count WHERE test_name={test!r} = {actual}, expected {expected_cnt}")

    # Abnormal counts by site
    for site, expected_cnt in [("Boston-MGH", 28), ("Houston-MD", 29), ("Seattle-UW", 27)]:
        mask_site = pc.equal(table.column("site"), site)
        mask_abnormal = pc.equal(table.column("is_abnormal"), True)
        combined = pc.and_(mask_site, mask_abnormal)
        actual = pc.sum(combined).as_py()
        if actual == expected_cnt:
            ok(f"Abnormal count WHERE site={site!r} = {expected_cnt}")
        else:
            fail(f"Abnormal count WHERE site={site!r} = {actual}, expected {expected_cnt}")

    # Total abnormal = 84
    total_abnormal = pc.sum(pc.equal(table.column("is_abnormal"), True)).as_py()
    if total_abnormal == 84:
        ok(f"Total abnormal = 84")
    else:
        fail(f"Total abnormal = {total_abnormal}, expected 84")

    # Above/below reference range
    above = pc.sum(pc.greater(table.column("result_value"), table.column("reference_high"))).as_py()
    below = pc.sum(pc.less(table.column("result_value"), table.column("reference_low"))).as_py()
    if above == 31:
        ok(f"Above reference range = 31")
    else:
        fail(f"Above reference range = {above}, expected 31")
    if below == 53:
        ok(f"Below reference range = 53")
    else:
        fail(f"Below reference range = {below}, expected 53")

    # Analyst workload
    for analyst, expected_cnt in [("Dr. Chen", 90), ("Dr. Kovacs", 100),
                                   ("Dr. Okafor", 99), ("Dr. Patel", 100), ("Dr. Reyes", 91)]:
        assert_count_where(table, "analyst", analyst, expected_cnt)

    # Distinct entity counts
    assert_distinct_count(table, "sample_id", 480)
    assert_distinct_count(table, "patient_id", 173)
    assert_distinct_count(table, "analyst", 5)


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v3-clinical-trials demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing iceberg_warehouse/trials/lab_results/"
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg V3 Clinical Trials -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "iceberg_warehouse", "trials", "lab_results")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_lab_results(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
