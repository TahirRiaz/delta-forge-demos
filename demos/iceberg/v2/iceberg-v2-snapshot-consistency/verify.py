#!/usr/bin/env python3
"""
Iceberg V2 Snapshot Consistency — Data Verification
=====================================================
Reads the inventory table through the Iceberg metadata chain with position
delete support and verifies the final 90-row state after INSERT, UPDATE,
and DELETE mutations.

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
    assert_row_count, assert_sum, assert_avg,
    assert_distinct_count, assert_count_where, assert_format_version,
    reset_counters,
)
from verify_lib import print_header, print_summary, exit_with_status


# ---------------------------------------------------------------------------
# Custom reader: handles position delete files
# ---------------------------------------------------------------------------
def read_iceberg_table_with_deletes(table_path):
    """Read a table through Iceberg metadata chain with position delete support."""
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
    delete_files = []
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
                if file_content == 2:
                    delete_files.append(fp)
                else:
                    data_files.append(fp)

    # Read delete positions
    delete_positions = {}
    for del_path in delete_files:
        del_table = pq.read_table(del_path)
        if "file_path" in del_table.column_names and "pos" in del_table.column_names:
            for i in range(del_table.num_rows):
                fp = del_table.column("file_path")[i].as_py()
                pos = del_table.column("pos")[i].as_py()
                fp_resolved = from_uri(fp)
                if not os.path.isfile(fp_resolved):
                    fp_resolved = os.path.join(table_path, os.path.basename(fp_resolved))
                delete_positions.setdefault(fp_resolved, set()).add(pos)

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

        if df_path in delete_positions:
            positions = delete_positions[df_path]
            indices = [i for i in range(pf.num_rows) if i not in positions]
            pf = pf.take(indices)

        tables.append(pf)

    return pa.concat_tables(tables), metadata


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v2-snapshot-consistency demo"
    )
    parser.add_argument("data_root", help="Parent folder containing inventory/")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show sample data")
    args = parser.parse_args()

    import pyarrow.compute as pc
    import pyarrow as pa

    data_root = os.path.abspath(args.data_root)
    table_path = os.path.join(data_root, "inventory")

    print_header("Snapshot Consistency (Inventory) — Data Verification")
    print(f"  Data root: {data_root}")

    if not os.path.isdir(os.path.join(table_path, "metadata")):
        print(f"\nError: {table_path}/metadata not found")
        sys.exit(1)

    table, metadata = read_iceberg_table_with_deletes(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg metadata chain")

    if args.verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 90)

    # Category breakdown
    assert_count_where(table, "category", "Clothing", 22)
    assert_count_where(table, "category", "Electronics", 23)
    assert_count_where(table, "category", "Home & Garden", 22)
    assert_count_where(table, "category", "Sports", 23)
    assert_distinct_count(table, "category", 4)

    # Category quantities
    for cat, exp_qty in [("Clothing", 2685), ("Electronics", 2104),
                          ("Home & Garden", 2514), ("Sports", 2729)]:
        mask = pc.equal(table.column("category"), cat)
        filtered = table.filter(mask)
        actual = pc.sum(filtered.column("quantity_on_hand")).as_py()
        if actual == exp_qty:
            ok(f"SUM(quantity_on_hand) WHERE category={cat!r} = {exp_qty}")
        else:
            fail(f"SUM(quantity_on_hand) WHERE category={cat!r} = {actual}, expected {exp_qty}")

    # Electronics avg price (after +8% update)
    elec = table.filter(pc.equal(table.column("category"), "Electronics"))
    avg_elec_price = round(pc.mean(elec.column("unit_price")).as_py(), 2)
    if avg_elec_price == 50.05:
        ok(f"AVG(unit_price) Electronics = 50.05")
    else:
        fail(f"AVG(unit_price) Electronics = {avg_elec_price}, expected 50.05")

    # New products (SKU-%-N%)
    new_count = 0
    for i in range(table.num_rows):
        sku = table.column("sku")[i].as_py()
        if sku and "-N" in sku:
            new_count += 1
    if new_count == 20:
        ok(f"New product count (SKU-%-N%) = 20")
    else:
        fail(f"New product count (SKU-%-N%) = {new_count}, expected 20")

    # Discontinued products absent
    discontinued = ['SKU-E007', 'SKU-E013', 'SKU-H008', 'SKU-H018', 'SKU-H019',
                    'SKU-S006', 'SKU-S008', 'SKU-C004', 'SKU-C017', 'SKU-C018']
    disc_mask = pc.is_in(table.column("sku"), value_set=pa.array(discontinued))
    disc_count = pc.sum(disc_mask).as_py()
    if disc_count == 0:
        ok(f"Discontinued SKUs not present (0 of 10)")
    else:
        fail(f"Found {disc_count} discontinued SKUs, expected 0")

    # Total inventory value
    # Compute unit_price * quantity_on_hand
    prices = table.column("unit_price")
    quantities = table.column("quantity_on_hand")
    inv_value = sum(
        round(prices[i].as_py() * quantities[i].as_py(), 2)
        for i in range(table.num_rows)
    )
    inv_value = round(inv_value, 2)
    if inv_value == 300102.64:
        ok(f"Total inventory value = 300102.64")
    else:
        fail(f"Total inventory value = {inv_value}, expected 300102.64")

    # Per-category inventory value
    for cat, exp_val in [("Clothing", 97463.15), ("Electronics", 82611.92),
                          ("Home & Garden", 61844.86), ("Sports", 58182.71)]:
        mask = pc.equal(table.column("category"), cat)
        filtered = table.filter(mask)
        val = sum(
            round(filtered.column("unit_price")[i].as_py() * filtered.column("quantity_on_hand")[i].as_py(), 2)
            for i in range(filtered.num_rows)
        )
        val = round(val, 2)
        if val == exp_val:
            ok(f"Inventory value WHERE category={cat!r} = {exp_val}")
        else:
            fail(f"Inventory value WHERE category={cat!r} = {val}, expected {exp_val}")

    # Overall aggregates
    assert_avg(table, "unit_price", 33.07, label="avg unit_price")
    assert_sum(table, "quantity_on_hand", 10032.0, label="total quantity")

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
