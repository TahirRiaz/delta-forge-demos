#!/usr/bin/env python3
"""
Iceberg V2 Position Deletes — Data Verification
=================================================
Reads the cold_chain_readings table through the Iceberg metadata chain,
applies position delete files, and verifies 570 valid readings remain
after removing 30 faulty sensor readings.

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
    assert_row_count, assert_min, assert_max,
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
                if file_content == 2:  # position deletes
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
        description="Verify Iceberg data for iceberg-v2-position-deletes demo"
    )
    parser.add_argument("data_root", help="Parent folder containing cold_chain_readings/")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show sample data")
    args = parser.parse_args()

    import pyarrow.compute as pc

    data_root = os.path.abspath(args.data_root)
    table_path = os.path.join(data_root, "cold_chain_readings")

    print_header("Position Deletes (Cold Chain) — Data Verification")
    print(f"  Data root: {data_root}")

    if not os.path.isdir(os.path.join(table_path, "metadata")):
        print(f"\nError: {table_path}/metadata not found")
        sys.exit(1)

    table, metadata = read_iceberg_table_with_deletes(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg metadata chain")

    if args.verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 570)

    # Faulty sensor completely removed
    faulty = table.filter(pc.equal(table.column("sensor_id"), "SENSOR-F01"))
    assert_row_count(faulty, 0, label="SENSOR-F01 (faulty)")

    # Per-route counts
    assert_count_where(table, "route", "ROUTE-A", 120)
    assert_count_where(table, "route", "ROUTE-B", 150)
    assert_count_where(table, "route", "ROUTE-C", 150)
    assert_count_where(table, "route", "ROUTE-D", 150)

    # Distinct sensors (20, not 21)
    assert_distinct_count(table, "sensor_id", 20)

    # Vaccine type distribution
    assert_count_where(table, "vaccine_type", "HPV-9v", 150)
    assert_count_where(table, "vaccine_type", "Influenza-Quad", 150)
    assert_count_where(table, "vaccine_type", "Tdap", 150)
    assert_count_where(table, "vaccine_type", "mRNA-COVID", 120)

    # Temperature excursions by route
    for route, exp_exc in [("ROUTE-A", 45), ("ROUTE-B", 45), ("ROUTE-C", 57), ("ROUTE-D", 63)]:
        mask = pc.equal(table.column("route"), route)
        filtered = table.filter(mask)
        exc_count = pc.sum(pc.equal(filtered.column("temp_excursion"), True)).as_py()
        if exc_count == exp_exc:
            ok(f"Excursion count WHERE route={route!r} = {exp_exc}")
        else:
            fail(f"Excursion count WHERE route={route!r} = {exc_count}, expected {exp_exc}")

    # Total excursions
    total_exc = pc.sum(pc.equal(table.column("temp_excursion"), True)).as_py()
    if total_exc == 210:
        ok(f"Total excursions = 210")
    else:
        fail(f"Total excursions = {total_exc}, expected 210")

    # Average temperature by route
    for route, exp_avg in [("ROUTE-A", -0.24), ("ROUTE-B", 0.81), ("ROUTE-C", 0.21), ("ROUTE-D", -0.29)]:
        mask = pc.equal(table.column("route"), route)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("temperature_c")).as_py(), 2)
        if actual == exp_avg:
            ok(f"AVG(temperature_c) WHERE route={route!r} = {exp_avg}")
        else:
            fail(f"AVG(temperature_c) WHERE route={route!r} = {actual}, expected {exp_avg}")

    # Low battery alerts
    low_batt = table.filter(pc.less_equal(table.column("battery_pct"), 25))
    assert_row_count(low_batt, 70, label="battery_pct <= 25")

    # Temperature range (no faulty readings)
    assert_min(table, "temperature_c", -7.95)
    assert_max(table, "temperature_c", 7.98)

    avg_temp = round(pc.mean(table.column("temperature_c")).as_py(), 2)
    if avg_temp == 0.14:
        ok(f"AVG(temperature_c) = 0.14")
    else:
        fail(f"AVG(temperature_c) = {avg_temp}, expected 0.14")

    # Distinct counts
    assert_distinct_count(table, "route", 4)
    assert_distinct_count(table, "vaccine_type", 4)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
