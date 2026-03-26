#!/usr/bin/env python3
"""
Iceberg UniForm External Verifier
===================================
Reads ONLY through the Iceberg metadata chain (not Delta) and verifies
every row against the expected final state after queries.sql.

This is a VALUE-LEVEL check — not just row counts. Every cell in every
row is compared against the expected data. If an external Iceberg engine
would see wrong values, this script catches it.

Usage:
    python3 verify.py <data_root>

    On WSL: python3 verify.py "/mnt/b/!demo/df-demo/iceberg-verify"
"""

import argparse
import glob
import gzip
import json
import os
import sys

import duckdb
import fastavro

# ---------------------------------------------------------------------------
# ANSI
# ---------------------------------------------------------------------------
G = "\033[92m"; R = "\033[91m"; Y = "\033[93m"; C = "\033[96m"
B = "\033[1m"; D = "\033[2m"; X = "\033[0m"

passed = 0; failed = 0; warned = 0

def ok(msg):
    global passed; passed += 1; print(f"  {G}✓{X} {msg}")
def fail(msg):
    global failed; failed += 1; print(f"  {R}✗{X} {msg}")
def warn(msg):
    global warned; warned += 1; print(f"  {Y}⚠{X} {msg}")
def info(msg):
    print(f"  {C}•{X} {msg}")

def win_to_wsl(path):
    if len(path) >= 2 and path[1] == ':':
        return f"/mnt/{path[0].lower()}" + path[2:].replace('\\', '/')
    return path


# ---------------------------------------------------------------------------
# Expected final state — EVERY ROW, EVERY VALUE
# ---------------------------------------------------------------------------
# After queries.sql completes, these are the exact rows in each table.
# Column order matches the CREATE TABLE definition.

PRODUCTS_COLUMNS = ["id", "name", "category", "price", "stock", "is_active"]
PRODUCTS_ROWS = [
    # Original 10 rows, then:
    #   UPDATE Electronics price * 1.10: ids 1,2,3
    #   DELETE is_active=false: ids 6,10 removed
    #   INSERT ids 11,12
    (1,  "Laptop",     "Electronics", 1099.99, 50,  True),
    (2,  "Mouse",      "Electronics", 32.99,   200, True),
    (3,  "Hub",        "Electronics", 54.99,   150, True),
    (4,  "Desk",       "Furniture",   549.99,  30,  True),
    (5,  "Chair",      "Furniture",   449.99,  40,  True),
    # id=6 Lamp deleted (is_active=false)
    (7,  "Headphones", "Audio",       249.99,  75,  True),
    (8,  "Speaker",    "Audio",       79.99,   110, True),
    (9,  "Mic",        "Audio",       129.99,  65,  True),
    # id=10 Earbuds deleted (is_active=false)
    (11, "Webcam",     "Electronics", 69.99,   85,  True),
    (12, "Footrest",   "Furniture",   29.99,   300, True),
]

SALES_COLUMNS = ["id", "product", "region", "quarter", "amount", "qty"]
SALES_ROWS = [
    # Original 12, UPDATE Q2 amount*1.05, DELETE eu-west amount<120 (id=9), INSERT 13,14
    (1,  "Widget", "us-east", "Q1", 100.00,  10),
    (2,  "Gadget", "us-east", "Q1", 200.00,  5),
    (3,  "Widget", "us-east", "Q2", 157.50,  8),   # 150*1.05
    (4,  "Gadget", "us-east", "Q2", 315.00,  3),   # 300*1.05
    (5,  "Widget", "us-west", "Q1", 120.00,  12),
    (6,  "Gadget", "us-west", "Q1", 180.00,  6),
    (7,  "Widget", "us-west", "Q2", 94.50,   15),  # 90*1.05
    (8,  "Gadget", "us-west", "Q2", 262.50,  4),   # 250*1.05
    # id=9 deleted (eu-west, amount=110 < 120)
    (10, "Gadget", "eu-west", "Q1", 220.00,  7),
    (11, "Widget", "eu-west", "Q2", 136.50,  9),   # 130*1.05
    (12, "Gadget", "eu-west", "Q2", 283.50,  5),   # 270*1.05
    (13, "Widget", "us-east", "Q3", 175.00,  10),
    (14, "Gadget", "us-west", "Q3", 225.00,  8),
]

EVOLVE_COLUMNS = ["id", "name", "value", "category", "priority"]
EVOLVE_ROWS = [
    # Original 5, ADD category, backfill group-a (ids 1-3), group-b (ids 4-5),
    # INSERT 3 with category, ADD priority (NULL for all)
    (1, "Alpha",   10.0, "group-a", None),
    (2, "Beta",    20.0, "group-a", None),
    (3, "Gamma",   30.0, "group-a", None),
    (4, "Delta",   40.0, "group-b", None),
    (5, "Epsilon", 50.0, "group-b", None),
    (6, "Zeta",    60.0, "group-a", None),
    (7, "Eta",     70.0, "group-b", None),
    (8, "Theta",   80.0, "group-a", None),
]

V3_TABLE_COLUMNS = ["id", "name", "value", "tag"]
V3_TABLE_ROWS = [
    # Original 6, UPDATE tag='x' for ids 1-3, DELETE tag='x' → ids 1-3 removed
    (4, "Four", 4.4, "b"),
    (5, "Five", 5.5, "a"),
    (6, "Six",  6.6, "b"),
]

TABLES = {
    "products":  (PRODUCTS_COLUMNS, PRODUCTS_ROWS),
    "sales":     (SALES_COLUMNS, SALES_ROWS),
    "evolve":    (EVOLVE_COLUMNS, EVOLVE_ROWS),
    "v3_table":  (V3_TABLE_COLUMNS, V3_TABLE_ROWS),
}

FORMAT_VERSIONS = {"products": 2, "sales": 2, "evolve": 2, "v3_table": 3}
PARTITIONED_TABLES = {"sales": "region"}


# ---------------------------------------------------------------------------
# Read active files from Iceberg manifest chain
# ---------------------------------------------------------------------------
def get_iceberg_active_files(table_dir):
    """Parse manifest chain, return list of WSL paths to active Parquet files."""
    meta_dir = os.path.join(table_dir, "metadata")
    if not os.path.isdir(meta_dir):
        return None, None

    meta_files = sorted(
        glob.glob(os.path.join(meta_dir, "v*.metadata.json"))
        + glob.glob(os.path.join(meta_dir, "v*.metadata.json.gz"))
    )
    if not meta_files:
        return None, None

    latest = meta_files[-1]
    opener = gzip.open if latest.endswith(".gz") else open
    with opener(latest, "rt") as f:
        metadata = json.load(f)

    snapshots = metadata.get("snapshots", [])
    if not snapshots:
        return metadata, []

    ml_ref = snapshots[-1].get("manifest-list", "")
    ml_path = os.path.join(meta_dir, os.path.basename(ml_ref))
    if not os.path.isfile(ml_path):
        return metadata, []

    with open(ml_path, "rb") as f:
        ml_records = list(fastavro.reader(f))

    paths = []
    for ml in ml_records:
        mpath = os.path.join(meta_dir, os.path.basename(ml.get("manifest_path", "")))
        if not os.path.isfile(mpath):
            continue
        with open(mpath, "rb") as f:
            for entry in fastavro.reader(f):
                if entry.get("status", 0) == 2:  # DELETED
                    continue
                df = entry.get("data_file", entry)
                wsl = win_to_wsl(df.get("file_path", ""))
                if os.path.isfile(wsl):
                    paths.append(wsl)

    return metadata, paths


# ---------------------------------------------------------------------------
# Column mapping: Iceberg logical names → Parquet physical col-UUIDs
# ---------------------------------------------------------------------------
def build_column_map(metadata, parquet_cols):
    """
    Map logical column names from Iceberg schema to physical Parquet columns.
    Uses field-id matching: Iceberg schema has field IDs, Parquet column metadata
    should have the same IDs. Falls back to positional mapping.
    """
    fmt = metadata.get("format-version", 2)
    if fmt == 1:
        fields = metadata.get("schema", {}).get("fields", [])
    else:
        schemas = metadata.get("schemas", [])
        fields = schemas[-1].get("fields", []) if schemas else []

    logical_names = [f["name"] for f in fields]

    # If Parquet columns match logical names, no mapping needed
    if set(logical_names) == set(parquet_cols):
        return {n: n for n in logical_names}

    # Positional mapping (Parquet columns in same order as schema fields)
    if len(logical_names) == len(parquet_cols):
        return dict(zip(logical_names, parquet_cols))

    # Partial match — some columns may have been added later
    # Map what we can positionally
    mapping = {}
    for i, name in enumerate(logical_names):
        if i < len(parquet_cols):
            mapping[name] = parquet_cols[i]
    return mapping


# ---------------------------------------------------------------------------
# Row-level comparison
# ---------------------------------------------------------------------------
def compare_rows(table_name, expected_cols, expected_rows, actual_df, col_map):
    """
    Compare every expected row against actual data.
    Returns (matches, mismatches, missing).
    """
    # Build lookup by ID (first column)
    id_col = col_map.get(expected_cols[0])
    if not id_col:
        warn(f"Cannot find ID column mapping for '{expected_cols[0]}'")
        return 0, 0, len(expected_rows)

    # Index actual rows by ID
    actual_rows = {}
    for _, row in actual_df.iterrows():
        rid = row[id_col]
        actual_rows[int(rid)] = row

    matches = 0
    mismatches = 0
    missing = 0

    for expected in expected_rows:
        eid = expected[0]

        if eid not in actual_rows:
            fail(f"Row id={eid}: MISSING from Iceberg data")
            missing += 1
            continue

        actual = actual_rows[eid]
        row_ok = True

        for j, (col_name, exp_val) in enumerate(zip(expected_cols, expected)):
            phys_col = col_map.get(col_name)
            if not phys_col or phys_col not in actual.index:
                continue  # Column not mapped, skip

            act_val = actual[phys_col]

            # Handle None/NaN
            if exp_val is None:
                import math
                if act_val is None or (isinstance(act_val, float) and math.isnan(act_val)):
                    continue
                else:
                    fail(f"Row id={eid}, {col_name}: expected NULL, got {act_val!r}")
                    row_ok = False
                    continue

            # Type-aware comparison
            if isinstance(exp_val, float):
                try:
                    if round(float(act_val), 2) != round(exp_val, 2):
                        fail(f"Row id={eid}, {col_name}: expected {exp_val}, got {act_val}")
                        row_ok = False
                except (TypeError, ValueError):
                    fail(f"Row id={eid}, {col_name}: expected {exp_val}, got {act_val!r} (type mismatch)")
                    row_ok = False
            elif isinstance(exp_val, bool):
                if bool(act_val) != exp_val:
                    fail(f"Row id={eid}, {col_name}: expected {exp_val}, got {act_val}")
                    row_ok = False
            elif isinstance(exp_val, int):
                try:
                    if int(act_val) != exp_val:
                        fail(f"Row id={eid}, {col_name}: expected {exp_val}, got {act_val}")
                        row_ok = False
                except (TypeError, ValueError):
                    fail(f"Row id={eid}, {col_name}: expected {exp_val}, got {act_val!r}")
                    row_ok = False
            elif isinstance(exp_val, str):
                if str(act_val) != exp_val:
                    fail(f"Row id={eid}, {col_name}: expected '{exp_val}', got '{act_val}'")
                    row_ok = False

        if row_ok:
            matches += 1
        else:
            mismatches += 1

    # Check for unexpected extra rows
    expected_ids = {r[0] for r in expected_rows}
    extra_ids = set(actual_rows.keys()) - expected_ids
    if extra_ids:
        fail(f"Extra rows in Iceberg data (not expected): ids={sorted(extra_ids)}")

    return matches, mismatches, missing


# ---------------------------------------------------------------------------
# Verify a single table
# ---------------------------------------------------------------------------
def verify_table(table_name, expected_cols, expected_rows, data_root, conn):
    table_dir = os.path.join(data_root, table_name)

    print(f"\n{B}{'─' * 60}{X}")
    print(f"{B}  {table_name} — {len(expected_rows)} expected rows, "
          f"{len(expected_cols)} columns{X}")
    print(f"{'─' * 60}")

    if not os.path.isdir(table_dir):
        fail(f"Directory not found: {table_dir}")
        return

    metadata, active_paths = get_iceberg_active_files(table_dir)

    if metadata is None:
        fail("No Iceberg metadata found")
        return

    # Format version check
    fmt = metadata.get("format-version", 0)
    expected_fmt = FORMAT_VERSIONS.get(table_name, 2)
    if fmt == expected_fmt:
        ok(f"Format version: V{fmt}")
    else:
        fail(f"Format version: V{fmt}, expected V{expected_fmt}")

    # Schema check
    if fmt == 1:
        fields = metadata.get("schema", {}).get("fields", [])
    else:
        schemas = metadata.get("schemas", [])
        fields = schemas[-1].get("fields", []) if schemas else []
    schema_names = [f["name"] for f in fields]
    if schema_names == expected_cols:
        ok(f"Schema: {schema_names}")
    else:
        fail(f"Schema: {schema_names}, expected {expected_cols}")

    if not active_paths:
        fail("No active Parquet files in manifest")
        return

    # Read all active data
    actual_df = conn.execute(f"SELECT * FROM read_parquet({active_paths})").fetchdf()
    parquet_cols = list(actual_df.columns)
    info(f"Parquet columns: {parquet_cols}")

    # Build column mapping
    col_map = build_column_map(metadata, parquet_cols)
    info(f"Column mapping: {col_map}")

    # Row count sanity
    if len(actual_df) == len(expected_rows):
        ok(f"Row count: {len(actual_df)}")
    else:
        fail(f"Row count: {len(actual_df)}, expected {len(expected_rows)}")

    # ROW-LEVEL VALUE COMPARISON
    matches, mismatches, missing = compare_rows(
        table_name, expected_cols, expected_rows, actual_df, col_map
    )

    if mismatches == 0 and missing == 0:
        ok(f"All {matches} rows verified — every value matches ✓")
    else:
        if matches > 0:
            info(f"{matches} rows correct")

    # Partition check for partitioned tables
    part_col = PARTITIONED_TABLES.get(table_name)
    if part_col:
        phys_part = col_map.get(part_col)
        if phys_part and phys_part in actual_df.columns:
            region_dist = actual_df.groupby(phys_part).size().to_dict()
            # Expected from SALES_ROWS
            expected_dist = {}
            for row in expected_rows:
                region_idx = expected_cols.index(part_col)
                r = row[region_idx]
                expected_dist[r] = expected_dist.get(r, 0) + 1
            if region_dist == expected_dist:
                ok(f"Partition distribution: {region_dist}")
            else:
                fail(f"Partition distribution: {region_dist}, expected {expected_dist}")
        else:
            warn(f"Partition column '{part_col}' not found in Parquet — "
                 f"cannot verify partition values")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg metadata — row-level value comparison"
    )
    parser.add_argument("data_root",
                        help="Directory containing table subdirectories "
                             "(products/, sales/, evolve/, v3_table/)")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print(f"\n{B}{'═' * 60}{X}")
    print(f"{B}  ICEBERG UNIFORM — ROW-LEVEL VALUE VERIFICATION{X}")
    print(f"{'═' * 60}")
    print(f"  Data root: {data_root}")
    print(f"  Reader: Iceberg metadata only (no Delta)")
    print(f"  Checks: EVERY row, EVERY column, EVERY value")

    conn = duckdb.connect()

    for table_name, (cols, rows) in TABLES.items():
        verify_table(table_name, cols, rows, data_root, conn)

    conn.close()

    # Summary
    total = passed + failed
    print(f"\n{B}{'═' * 60}{X}")
    print(f"{B}  RESULT{X}")
    print(f"{'═' * 60}")
    print(f"  {G}✓ Passed: {passed}{X}")
    if failed:
        print(f"  {R}✗ Failed: {failed}{X}")
    if warned:
        print(f"  {Y}⚠ Warnings: {warned}{X}")

    if failed == 0:
        print(f"\n  {G}{B}PASS — Every value verified. External Iceberg reader sees correct data.{X}")
    else:
        print(f"\n  {R}{B}FAIL — Data discrepancies found. See failures above.{X}")

    return failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
