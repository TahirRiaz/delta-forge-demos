#!/usr/bin/env python3
"""
Iceberg UniForm Equality Deletes — Data Verification
======================================================
Reads the eq_del_products table through the Iceberg metadata chain and
verifies that 3 of 10 products were correctly deleted via equality deletes.

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
    assert_row_count, assert_sum,
    assert_count_where, assert_format_version,
    reset_counters,
)
from verify_lib import print_header, print_summary, exit_with_status


# ---------------------------------------------------------------------------
# Custom reader: handles equality delete files
# ---------------------------------------------------------------------------
def read_iceberg_table_with_equality_deletes(table_path):
    """Read a table through Iceberg metadata chain with equality delete support."""
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

    current_snap_id = metadata.get("current-snapshot-id")
    latest_snap = None
    for snap in snapshots:
        if snap.get("snapshot-id") == current_snap_id:
            latest_snap = snap
            break
    if latest_snap is None:
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
    eq_delete_files = []
    pos_delete_files = []
    for ml_rec in ml_records:
        m_path = from_uri(ml_rec.get("manifest_path", ""))
        if not os.path.isfile(m_path):
            m_path = os.path.join(table_path, "metadata", os.path.basename(m_path))
        content_type = ml_rec.get("content", 0)
        with open(m_path, "rb") as f:
            for entry in fastavro.reader(f):
                df_entry = entry.get("data_file", entry)
                status = entry.get("status", 1)
                if status == 2:
                    continue
                fp = from_uri(df_entry.get("file_path", ""))
                if not os.path.isfile(fp):
                    fp = os.path.join(table_path, os.path.basename(fp))
                file_content = df_entry.get("content", content_type)
                if file_content == 1:  # equality deletes
                    eq_delete_files.append(fp)
                elif file_content == 2:  # position deletes
                    pos_delete_files.append(fp)
                else:
                    data_files.append(fp)

    # Read data files
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

    result = pa.concat_tables(tables)

    # Apply equality deletes
    if eq_delete_files:
        import pyarrow.compute as pc
        deleted_ids = set()
        for del_path in eq_delete_files:
            del_table = pq.read_table(del_path)
            # Rename columns by field ID
            arrow_schema = del_table.schema
            rename_map = {}
            for arrow_field in arrow_schema:
                md = arrow_field.metadata or {}
                fid = md.get(b"PARQUET:field_id")
                if fid is not None:
                    fid_int = int(fid)
                    if fid_int in field_id_to_name:
                        rename_map[arrow_field.name] = field_id_to_name[fid_int]
            if rename_map:
                new_names = [rename_map.get(c, c) for c in del_table.column_names]
                del_table = del_table.rename_columns(new_names)
            if "id" in del_table.column_names:
                for i in range(del_table.num_rows):
                    deleted_ids.add(del_table.column("id")[i].as_py())

        if deleted_ids:
            mask = pc.invert(pc.is_in(result.column("id"), value_set=pa.array(list(deleted_ids))))
            result = result.filter(mask)

    return result, metadata


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-uniform-equality-deletes demo"
    )
    parser.add_argument("data_root", help="Parent folder containing eq_del_products/")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show sample data")
    args = parser.parse_args()

    import pyarrow.compute as pc

    data_root = os.path.abspath(args.data_root)
    table_path = os.path.join(data_root, "eq_del_products")

    print_header("Equality Deletes — Data Verification")
    print(f"  Data root: {data_root}")

    if not os.path.isdir(os.path.join(table_path, "metadata")):
        print(f"\nError: {table_path}/metadata not found")
        sys.exit(1)

    table, metadata = read_iceberg_table_with_equality_deletes(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg metadata chain")

    if args.verbose:
        info(f"Columns: {table.column_names}")

    # Row count: 10 - 3 deleted = 7
    assert_row_count(table, 7)

    # Deleted products absent
    for del_id in [2, 5, 8]:
        cnt = pc.sum(pc.equal(table.column("id"), del_id)).as_py()
        if cnt == 0:
            ok(f"Deleted product id={del_id} not present")
        else:
            fail(f"Deleted product id={del_id} found ({cnt} rows), expected 0")

    # Total price of remaining products
    assert_sum(table, "price", 2813.48, label="total price")

    # Category distribution
    assert_count_where(table, "category", "Electronics", 2)
    assert_count_where(table, "category", "Energy", 2)
    assert_count_where(table, "category", "Industrial", 2)
    assert_count_where(table, "category", "Science", 1)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
