#!/usr/bin/env python3
"""
Iceberg Native Schema Evolution — Data Verification
=====================================================
Reads the employee_directory table through the Iceberg metadata chain and
verifies 360 employees with schema evolution (ADD COLUMN, RENAME COLUMN).

Usage:
    python verify.py <data_root_path> [--verbose]

Requirements:
    pip install pyiceberg[pyarrow] fastavro
"""

import argparse
import glob
import gzip
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (
    ok, fail, info,
    assert_row_count, assert_sum, assert_distinct_count,
    assert_count_where, assert_format_version,
    reset_counters,
)
from verify_lib import print_header, print_summary, exit_with_status


# ---------------------------------------------------------------------------
# Custom reader: uses promote_options="default" for schema evolution
# ---------------------------------------------------------------------------
def read_iceberg_table_with_promotion(table_path):
    """Read a table through Iceberg metadata chain with promote_options for
    schema evolution (ADD/RENAME COLUMN produces heterogeneous Parquet files)."""
    import fastavro
    import pyarrow as pa
    import pyarrow.parquet as pq

    meta_dir = os.path.join(table_path, "metadata")
    meta_files = sorted(glob.glob(os.path.join(meta_dir, "v*.metadata.json")))
    gz_files = sorted(glob.glob(os.path.join(meta_dir, "v*.metadata.json.gz")))
    all_meta = meta_files + gz_files
    if not all_meta:
        raise FileNotFoundError(f"No metadata files in {meta_dir}")

    latest_meta = all_meta[-1]
    if latest_meta.endswith(".gz"):
        with gzip.open(latest_meta, "rt") as f:
            metadata = json.load(f)
    else:
        with open(latest_meta) as f:
            metadata = json.load(f)

    fmt_version = metadata.get("format-version", 2)

    schema_fields = []
    if fmt_version == 1:
        schema = metadata.get("schema")
        schemas = metadata.get("schemas", [])
        if schema:
            schema_fields = schema.get("fields", [])
        elif schemas:
            schema_fields = schemas[-1].get("fields", [])
    else:
        schemas = metadata.get("schemas", [])
        if schemas:
            schema_fields = schemas[-1].get("fields", [])

    field_id_to_name = {f["id"]: f["name"] for f in schema_fields}

    snapshots = metadata.get("snapshots", [])
    if not snapshots:
        raise ValueError("No snapshots in metadata")

    latest_snap = snapshots[-1]
    ml_path_raw = latest_snap.get("manifest-list", "")

    def from_uri(u):
        return u.replace("file:///", "").replace("file://", "")

    ml_path = from_uri(ml_path_raw)
    if not os.path.isfile(ml_path):
        ml_path = os.path.join(table_path, "metadata", os.path.basename(ml_path))

    with open(ml_path, "rb") as f:
        ml_records = list(fastavro.reader(f))

    data_files = []
    for ml_rec in ml_records:
        m_path = from_uri(ml_rec.get("manifest_path", ""))
        if not os.path.isfile(m_path):
            m_path = os.path.join(table_path, "metadata", os.path.basename(m_path))
        with open(m_path, "rb") as f:
            for entry in fastavro.reader(f):
                df_entry = entry.get("data_file", entry)
                status = entry.get("status", 1)
                if status != 2:
                    fp = from_uri(df_entry.get("file_path", ""))
                    if not os.path.isfile(fp):
                        fp = os.path.join(table_path, os.path.basename(fp))
                    data_files.append(fp)

    tables = []
    for df_path in data_files:
        pf = pq.read_table(df_path)
        arrow_schema = pf.schema
        rename_map = {}
        for arrow_field in arrow_schema:
            md = arrow_field.metadata or {}
            fid = md.get(b"PARQUET:field_id")
            if fid is not None:
                fid_int = int(fid)
                if fid_int in field_id_to_name:
                    rename_map[arrow_field.name] = field_id_to_name[fid_int]
        if rename_map:
            new_names = [rename_map.get(c, c) for c in pf.column_names]
            pf = pf.rename_columns(new_names)
        tables.append(pf)

    return pa.concat_tables(tables, promote_options="default"), metadata


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-native-schema-evolution demo"
    )
    parser.add_argument("data_root", help="Parent folder containing employee_directory/")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show sample data")
    args = parser.parse_args()

    import pyarrow.compute as pc

    data_root = os.path.abspath(args.data_root)
    table_path = os.path.join(data_root, "employee_directory")

    print_header("Schema Evolution — Data Verification")
    print(f"  Data root: {data_root}")

    if not os.path.isdir(os.path.join(table_path, "metadata")):
        print(f"\nError: {table_path}/metadata not found")
        sys.exit(1)

    table, metadata = read_iceberg_table_with_promotion(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg metadata chain")

    if args.verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 360)

    # Per-department counts (72 each, renamed from dept -> department)
    assert_count_where(table, "department", "Engineering", 72)
    assert_count_where(table, "department", "Finance", 72)
    assert_count_where(table, "department", "HR", 72)
    assert_count_where(table, "department", "Marketing", 72)
    assert_count_where(table, "department", "Sales", 72)
    assert_distinct_count(table, "department", 5)

    # NULL title count: original 300 employees have no title
    null_title = pc.sum(pc.is_null(table.column("title"))).as_py()
    if null_title == 300:
        ok(f"NULL title count = 300")
    else:
        fail(f"NULL title count = {null_title}, expected 300")

    has_title = pc.sum(pc.is_valid(table.column("title"))).as_py()
    if has_title == 60:
        ok(f"Non-NULL title count = 60")
    else:
        fail(f"Non-NULL title count = {has_title}, expected 60")

    # NULL location count
    null_loc = pc.sum(pc.is_null(table.column("location"))).as_py()
    if null_loc == 300:
        ok(f"NULL location count = 300")
    else:
        fail(f"NULL location count = {null_loc}, expected 300")

    has_loc = pc.sum(pc.is_valid(table.column("location"))).as_py()
    if has_loc == 60:
        ok(f"Non-NULL location count = 60")
    else:
        fail(f"Non-NULL location count = {has_loc}, expected 60")

    # Salary aggregations
    assert_sum(table, "salary", 35044993.03, label="total salary")

    avg_sal = round(pc.mean(table.column("salary")).as_py(), 2)
    if avg_sal == 97347.20:
        ok(f"AVG(salary) = 97347.20")
    else:
        fail(f"AVG(salary) = {avg_sal}, expected 97347.20")

    min_sal = round(pc.min(table.column("salary")).as_py(), 2)
    max_sal = round(pc.max(table.column("salary")).as_py(), 2)
    if min_sal == 50115.82:
        ok(f"MIN(salary) = 50115.82")
    else:
        fail(f"MIN(salary) = {min_sal}, expected 50115.82")
    if max_sal == 149715.19:
        ok(f"MAX(salary) = 149715.19")
    else:
        fail(f"MAX(salary) = {max_sal}, expected 149715.19")

    # Per-department salary averages
    for dept, exp_avg in [("Engineering", 96315.90), ("Finance", 94828.12),
                           ("HR", 97538.32), ("Marketing", 97049.79), ("Sales", 101003.89)]:
        mask = pc.equal(table.column("department"), dept)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("salary")).as_py(), 2)
        if actual == exp_avg:
            ok(f"AVG(salary) WHERE department={dept!r} = {exp_avg}")
        else:
            fail(f"AVG(salary) WHERE department={dept!r} = {actual}, expected {exp_avg}")

    # Spot checks
    emp1 = table.filter(pc.equal(table.column("emp_id"), 1))
    if emp1.num_rows > 0:
        name = emp1.column("full_name")[0].as_py()
        if name == "Alice Smith":
            ok("emp_id=1 full_name = 'Alice Smith'")
        else:
            fail(f"emp_id=1 full_name = {name!r}, expected 'Alice Smith'")

    emp360 = table.filter(pc.equal(table.column("emp_id"), 360))
    if emp360.num_rows > 0:
        name = emp360.column("full_name")[0].as_py()
        title = emp360.column("title")[0].as_py()
        loc = emp360.column("location")[0].as_py()
        if name == "Eve Miller":
            ok("emp_id=360 full_name = 'Eve Miller'")
        else:
            fail(f"emp_id=360 full_name = {name!r}, expected 'Eve Miller'")
        if title == "Data Scientist":
            ok("emp_id=360 title = 'Data Scientist'")
        else:
            fail(f"emp_id=360 title = {title!r}, expected 'Data Scientist'")
        if loc == "New York":
            ok("emp_id=360 location = 'New York'")
        else:
            fail(f"emp_id=360 location = {loc!r}, expected 'New York'")

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
