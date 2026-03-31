"""
Table readers for Iceberg and Delta formats.

Each reader walks the format's metadata chain to discover Parquet data files,
reads them with PyArrow, applies column mapping, and returns
(pa.Table, metadata_dict).
"""

import glob
import gzip
import json
import os


# ---------------------------------------------------------------------------
# Iceberg reader
# ---------------------------------------------------------------------------
def read_iceberg_table(table_path):
    """Read a table purely through Iceberg metadata, returning a PyArrow table
    with columns renamed by field ID -> Iceberg schema name mapping.

    Walks: metadata.json -> manifest list (Avro) -> manifest (Avro) -> Parquet files.
    Handles col-<uuid> physical names via PARQUET:field_id metadata.
    """
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

    # Build field ID -> name map
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

    # Walk manifest chain
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
                if status != 2:  # 2 = DELETED
                    fp = from_uri(df_entry.get("file_path", ""))
                    if not os.path.isfile(fp):
                        fp = os.path.join(table_path, os.path.basename(fp))
                    data_files.append(fp)

    # Read Parquet and rename columns via field IDs
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

    return pa.concat_tables(tables), metadata


# ---------------------------------------------------------------------------
# Delta reader
# ---------------------------------------------------------------------------
def read_delta_table(table_path):
    """Read a Delta table through its transaction log -> Arrow table.

    Walks the _delta_log/ directory, parses JSON commit files to find
    active 'add' actions (minus 'remove' actions), reads the referenced
    Parquet files, and applies Delta column mapping if present.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    delta_log = os.path.join(table_path, "_delta_log")
    if not os.path.isdir(delta_log):
        raise FileNotFoundError(f"No _delta_log/ directory in {table_path}")

    # Find all commit JSON files (00000000000000000000.json, etc.)
    commit_files = sorted(glob.glob(os.path.join(delta_log, "*.json")))
    if not commit_files:
        raise FileNotFoundError(f"No commit files in {delta_log}")

    # Parse commits to build set of active files
    active_files = {}  # path -> add_action
    metadata = {}
    protocol = {}

    for commit_file in commit_files:
        with open(commit_file) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                action = json.loads(line)
                if "add" in action:
                    add = action["add"]
                    path = add["path"]
                    active_files[path] = add
                elif "remove" in action:
                    rem = action["remove"]
                    path = rem["path"]
                    active_files.pop(path, None)
                elif "metaData" in action:
                    metadata = action["metaData"]
                elif "protocol" in action:
                    protocol = action["protocol"]

    if not active_files:
        raise ValueError("No active data files in Delta log")

    # Extract column mapping if present
    config = metadata.get("configuration", {})
    mapping_mode = config.get("delta.columnMapping.mode", "none")

    # Build field ID -> name map from schema
    field_id_to_name = {}
    if mapping_mode in ("id", "name"):
        schema_str = metadata.get("schemaString", "{}")
        schema_obj = json.loads(schema_str)
        for field in schema_obj.get("fields", []):
            field_md = field.get("metadata", {})
            fid = field_md.get("delta.columnMapping.id")
            fname = field_md.get("delta.columnMapping.physicalName")
            logical_name = field.get("name")
            if fid is not None:
                field_id_to_name[int(fid)] = logical_name
            if fname is not None:
                # physical name -> logical name mapping
                field_id_to_name[fname] = logical_name

    # Read Parquet files
    tables = []
    for rel_path, add_action in active_files.items():
        # URL-decode the path (Delta encodes special chars)
        from urllib.parse import unquote
        decoded_path = unquote(rel_path)
        abs_path = os.path.join(table_path, decoded_path)

        if not os.path.isfile(abs_path):
            continue

        pf = pq.read_table(abs_path)

        # Apply column mapping
        if mapping_mode == "id":
            # Map via Parquet field IDs
            rename_map = {}
            for arrow_field in pf.schema:
                md = arrow_field.metadata or {}
                fid = md.get(b"PARQUET:field_id")
                if fid is not None:
                    fid_int = int(fid)
                    if fid_int in field_id_to_name:
                        rename_map[arrow_field.name] = field_id_to_name[fid_int]
            if rename_map:
                new_names = [rename_map.get(c, c) for c in pf.column_names]
                pf = pf.rename_columns(new_names)
        elif mapping_mode == "name":
            # Map via physical column names
            rename_map = {}
            for col_name in pf.column_names:
                if col_name in field_id_to_name:
                    rename_map[col_name] = field_id_to_name[col_name]
            if rename_map:
                new_names = [rename_map.get(c, c) for c in pf.column_names]
                pf = pf.rename_columns(new_names)

        tables.append(pf)

    result_table = pa.concat_tables(tables, promote_options="default")

    delta_metadata = {
        "format": "delta",
        "version": len(commit_files) - 1,
        "num_files": len(active_files),
        "metadata": metadata,
        "protocol": protocol,
    }

    return result_table, delta_metadata
