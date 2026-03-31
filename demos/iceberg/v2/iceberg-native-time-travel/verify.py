#!/usr/bin/env python3
"""
Iceberg Native Time Travel (Stock Prices) — Data Verification
===============================================================
Reads the stock_prices table through the Iceberg metadata chain and
verifies the final 138-row state after UPDATE, INSERT, and DELETE mutations.

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
    assert_row_count, assert_distinct_count,
    assert_count_where, assert_format_version,
    reset_counters,
)
from verify_lib import print_header, print_summary, exit_with_status


# ---------------------------------------------------------------------------
# Custom reader: handles position delete files for merge-on-read
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

    # Use current-snapshot-id to find the right snapshot
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

    # Separate data files and delete files
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
                if status == 2:  # DELETED
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
    delete_positions = {}  # data_file_path -> set of positions
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

    # Read data files applying position deletes
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

        # Apply position deletes
        if df_path in delete_positions:
            positions = delete_positions[df_path]
            import pyarrow.compute as pc
            indices = [i for i in range(pf.num_rows) if i not in positions]
            pf = pf.take(indices)

        tables.append(pf)

    return pa.concat_tables(tables), metadata


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-native-time-travel demo"
    )
    parser.add_argument("data_root", help="Parent folder containing stock_prices/")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show sample data")
    args = parser.parse_args()

    import pyarrow.compute as pc

    data_root = os.path.abspath(args.data_root)
    table_path = os.path.join(data_root, "stock_prices")

    print_header("Time Travel (Stock Prices) — Data Verification")
    print(f"  Data root: {data_root}")

    if not os.path.isdir(os.path.join(table_path, "metadata")):
        print(f"\nError: {table_path}/metadata not found")
        sys.exit(1)

    table, metadata = read_iceberg_table_with_deletes(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg metadata chain")

    if args.verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 138)

    # Per-sector breakdown
    assert_count_where(table, "sector", "Technology", 60)
    assert_count_where(table, "sector", "Healthcare", 30)
    assert_count_where(table, "sector", "Finance", 30)
    assert_count_where(table, "sector", "Energy", 18)

    # Distinct tickers
    assert_distinct_count(table, "ticker", 23)
    assert_distinct_count(table, "sector", 4)

    # Delisted tickers absent
    cop = table.filter(pc.equal(table.column("ticker"), "COP"))
    assert_row_count(cop, 0, label="COP (delisted)")
    slb = table.filter(pc.equal(table.column("ticker"), "SLB"))
    assert_row_count(slb, 0, label="SLB (delisted)")

    # IPO tickers present (5 tickers x 6 days = 30)
    ipo_tickers = ["BIOT", "FINX", "GRNH", "NWAI", "QCMP"]
    ipo_count = 0
    for t in ipo_tickers:
        mask = pc.equal(table.column("ticker"), t)
        cnt = pc.sum(mask).as_py()
        ipo_count += cnt
    if ipo_count == 30:
        ok(f"IPO tickers total rows = 30")
    else:
        fail(f"IPO tickers total rows = {ipo_count}, expected 30")

    # Per-ticker avg prices (select key ones)
    for ticker, exp_avg in [("AAPL", 196.40), ("MSFT", 438.53), ("NVDA", 925.06),
                             ("JPM", 195.18), ("XOM", 104.69), ("BIOT", 78.86),
                             ("NWAI", 45.31), ("QCMP", 119.67)]:
        mask = pc.equal(table.column("ticker"), ticker)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("price")).as_py(), 2)
        if actual == exp_avg:
            ok(f"AVG(price) WHERE ticker={ticker!r} = {exp_avg}")
        else:
            fail(f"AVG(price) WHERE ticker={ticker!r} = {actual}, expected {exp_avg}")

    # Grand averages
    grand_avg = round(pc.mean(table.column("price")).as_py(), 2)
    if grand_avg == 239.37:
        ok(f"AVG(price) overall = 239.37")
    else:
        fail(f"AVG(price) overall = {grand_avg}, expected 239.37")

    grand_vol = pc.sum(table.column("volume")).as_py()
    if grand_vol == 5255593877:
        ok(f"SUM(volume) = 5255593877")
    else:
        fail(f"SUM(volume) = {grand_vol}, expected 5255593877")

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
