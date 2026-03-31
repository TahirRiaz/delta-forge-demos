#!/usr/bin/env python3
"""
Iceberg V3 Equality Delete Files -- Data Verification
=======================================================
Reads the patient_visits table through native Iceberg V3 metadata.
4 GDPR patients were equality-deleted (55 rows removed), leaving 445 of 500.

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


def assert_count_where_in(table, filter_col, filter_vals, expected, label=""):
    import pyarrow.compute as pc
    mask = pc.is_in(table.column(filter_col), value_set=filter_vals)
    actual = pc.sum(mask).as_py()
    ctx = f" ({label})" if label else ""
    if actual == expected:
        ok(f"COUNT WHERE {filter_col} IN ... = {expected}{ctx}")
    else:
        fail(f"COUNT WHERE {filter_col} IN ... = {actual}, expected {expected}{ctx}")


def verify_patient_visits(data_root, verbose=False):
    import pyarrow as pa
    import pyarrow.compute as pc

    print_section("patient_visits -- Equality Delete Post-GDPR")

    table_path = os.path.join(data_root, "patient_visits")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 3)

    # 500 - 55 GDPR = 445
    assert_row_count(table, 445)

    # GDPR patients fully removed
    gdpr_ids = pa.array(["P-0012", "P-0025", "P-0041", "P-0067"])
    assert_count_where_in(table, "patient_id", gdpr_ids, 0, "GDPR patients absent")

    # Per-hospital visit counts
    assert_count_where(table, "hospital", "Cleveland-Clinic-OH", 96)
    assert_count_where(table, "hospital", "Johns-Hopkins-Baltimore", 80)
    assert_count_where(table, "hospital", "Mass-General-Boston", 93)
    assert_count_where(table, "hospital", "Mayo-Clinic-Rochester", 84)
    assert_count_where(table, "hospital", "Mount-Sinai-NYC", 92)

    # Distinct patients
    assert_distinct_count(table, "patient_id", 75)

    # Per-department distribution
    assert_count_where(table, "department", "Cardiology", 52)
    assert_count_where(table, "department", "Dermatology", 58)
    assert_count_where(table, "department", "Emergency", 50)
    assert_count_where(table, "department", "Neurology", 63)
    assert_count_where(table, "department", "Oncology", 59)
    assert_count_where(table, "department", "Orthopedics", 63)
    assert_count_where(table, "department", "Pediatrics", 55)
    assert_count_where(table, "department", "Radiology", 45)

    # Emergency visits by hospital
    for hospital, expected_cnt in [
        ("Cleveland-Clinic-OH", 10), ("Johns-Hopkins-Baltimore", 9),
        ("Mass-General-Boston", 16), ("Mayo-Clinic-Rochester", 5),
        ("Mount-Sinai-NYC", 10)
    ]:
        mask_hosp = pc.equal(table.column("hospital"), hospital)
        mask_emerg = pc.equal(table.column("is_emergency"), True)
        combined = pc.and_(mask_hosp, mask_emerg)
        actual = pc.sum(combined).as_py()
        if actual == expected_cnt:
            ok(f"Emergency count WHERE hospital={hospital!r} = {expected_cnt}")
        else:
            fail(f"Emergency count WHERE hospital={hospital!r} = {actual}, expected {expected_cnt}")

    # Total emergency = 50
    total_emerg = pc.sum(pc.equal(table.column("is_emergency"), True)).as_py()
    if total_emerg == 50:
        ok(f"Total emergency visits = 50")
    else:
        fail(f"Total emergency visits = {total_emerg}, expected 50")

    # Cost aggregates
    assert_sum(table, "treatment_cost", 5859194.13)
    assert_avg(table, "treatment_cost", 13166.73)

    # Distinct entity counts
    assert_distinct_count(table, "visit_id", 445)
    assert_distinct_count(table, "hospital", 5)
    assert_distinct_count(table, "department", 8)
    assert_distinct_count(table, "attending_physician", 10)


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v3-equality-deletes demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing patient_visits/"
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg V3 Equality Deletes -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "patient_visits")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_patient_visits(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
