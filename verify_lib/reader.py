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
import sys

import pyarrow as pa


# ---------------------------------------------------------------------------
# Schema unification helpers
# ---------------------------------------------------------------------------
# Type promotion priority: wider numeric types win; dictionary types are
# decoded to their value type so that tables from different partitions or
# schema-evolution snapshots can be concatenated without Arrow type errors.

_NUMERIC_RANK = {
    pa.int8(): 1, pa.int16(): 2, pa.int32(): 3, pa.int64(): 4,
    pa.uint8(): 1, pa.uint16(): 2, pa.uint32(): 3, pa.uint64(): 4,
    pa.float16(): 5, pa.float32(): 6, pa.float64(): 7,
}


def _wider_type(a, b):
    """Return the wider of two Arrow types, or *b* if they are unrelated."""
    if a == b:
        return a

    # Unwrap dictionary types to their value type
    if pa.types.is_dictionary(a):
        a = a.value_type
    if pa.types.is_dictionary(b):
        b = b.value_type
    if a == b:
        return a

    ra, rb = _NUMERIC_RANK.get(a), _NUMERIC_RANK.get(b)
    if ra is not None and rb is not None:
        return a if ra >= rb else b

    # Fallback: prefer the second (later-seen) type
    return b


def _unified_schema(tables):
    """Build a unified schema that is the widest superset of all tables."""
    if not tables:
        return None
    field_map = {}  # name -> pa.Field (widest type seen so far)
    ordered_names = []
    for tbl in tables:
        for field in tbl.schema:
            if field.name not in field_map:
                field_map[field.name] = field
                ordered_names.append(field.name)
            else:
                existing = field_map[field.name]
                wide = _wider_type(existing.type, field.type)
                if wide != existing.type:
                    field_map[field.name] = pa.field(field.name, wide,
                                                     nullable=existing.nullable or field.nullable)
    return pa.schema([field_map[n] for n in ordered_names])


def _cast_table(table, target_schema):
    """Cast *table* columns to match *target_schema* types.

    Handles missing columns (schema evolution) by filling with nulls,
    and dictionary-encoded columns by decoding first.
    """
    columns = []
    for field in target_schema:
        if field.name in table.column_names:
            col = table.column(field.name)
            if col.type != field.type:
                # Dictionary -> plain type: use dictionary_decode first
                if pa.types.is_dictionary(col.type):
                    col = col.dictionary_decode()
                if col.type != field.type:
                    col = col.cast(field.type)
        else:
            # Column missing (schema evolution) -- fill with nulls
            col = pa.nulls(table.num_rows, type=field.type)
        columns.append(col)
    return pa.table({f.name: c for f, c in zip(target_schema, columns)},
                    schema=target_schema)


def _concat_tables_safe(tables):
    """Concatenate tables with automatic schema widening and type promotion."""
    if not tables:
        return pa.table({})
    if len(tables) == 1:
        return tables[0]
    schema = _unified_schema(tables)
    casted = [_cast_table(t, schema) for t in tables]
    return pa.concat_tables(casted)


# ---------------------------------------------------------------------------
# Iceberg file-path resolution
# ---------------------------------------------------------------------------
def _resolve_iceberg_file(fp, table_path):
    """Resolve an Iceberg file path using multiple fallback strategies.

    Tries in order:
    1. The absolute path as-is
    2. Relative to the table's metadata/ directory (by basename)
    3. Relative to table_path using the table directory name as anchor
    4. Relative to table_path's data/ directory (by basename)
    5. Relative to table_path directly (by basename)

    Returns the resolved path, or the original path if no strategy finds the
    file (caller should check existence and handle accordingly).
    """
    if os.path.isfile(fp):
        return fp

    # Strategy 2: metadata/<basename>
    candidate = os.path.join(table_path, "metadata", os.path.basename(fp))
    if os.path.isfile(candidate):
        return candidate

    # Strategy 3: find table dir name in path, resolve relative suffix
    table_dir_name = os.path.basename(table_path)
    parts = fp.replace("\\", "/").split("/")
    try:
        idx = parts.index(table_dir_name)
        if idx + 1 < len(parts):
            rel_suffix = os.path.join(*parts[idx + 1:])
            candidate = os.path.join(table_path, rel_suffix)
            if os.path.isfile(candidate):
                return candidate
    except (ValueError, TypeError):
        pass

    # Strategy 4: data/<basename>
    candidate = os.path.join(table_path, "data", os.path.basename(fp))
    if os.path.isfile(candidate):
        return candidate

    # Strategy 5: <table_path>/<basename>
    candidate = os.path.join(table_path, os.path.basename(fp))
    if os.path.isfile(candidate):
        return candidate

    return fp  # Return original; caller must handle missing file


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
    import pyarrow.parquet as pq

    meta_dir = os.path.join(table_path, "metadata")
    # Match both naming conventions:
    #   Spark-style:  v1.metadata.json, v2.metadata.json, ...
    #   Numeric-UUID: 00000-<uuid>.metadata.json, 00001-<uuid>.metadata.json, ...
    meta_files = sorted(glob.glob(os.path.join(meta_dir, "v*.metadata.json")))
    meta_files += sorted(glob.glob(os.path.join(meta_dir, "[0-9]*.metadata.json")))
    gz_files = sorted(glob.glob(os.path.join(meta_dir, "v*.metadata.json.gz")))
    gz_files += sorted(glob.glob(os.path.join(meta_dir, "[0-9]*.metadata.json.gz")))
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

    # Build field ID -> name map from the *current* schema.
    # Iceberg v1 uses a top-level "schema" object; v2 uses "schemas" array
    # indexed by "current-schema-id".  Some writers (e.g. UniForm) may use
    # the v2 layout even for format-version 1, so we check both.
    schema_fields = []
    current_schema_id = metadata.get("current-schema-id")

    def _find_schema_by_id(schemas_list, schema_id):
        """Return the schema dict matching *schema_id*, or None."""
        for s in schemas_list:
            if s.get("schema-id") == schema_id:
                return s
        return None

    def _collect_fields_recursive(fields):
        """Flatten nested struct fields into a single id->name mapping."""
        result = {}
        for f in fields:
            result[f["id"]] = f["name"]
            # Recurse into struct types which have nested "fields"
            ftype = f.get("type")
            if isinstance(ftype, dict) and ftype.get("type") == "struct":
                nested = ftype.get("fields", [])
                result.update(_collect_fields_recursive(nested))
        return result

    if fmt_version == 1:
        schema = metadata.get("schema")
        schemas = metadata.get("schemas", [])
        if schemas and current_schema_id is not None:
            matched = _find_schema_by_id(schemas, current_schema_id)
            if matched:
                schema_fields = matched.get("fields", [])
        if not schema_fields and schema:
            schema_fields = schema.get("fields", [])
        if not schema_fields and schemas:
            schema_fields = schemas[-1].get("fields", [])
    else:
        schemas = metadata.get("schemas", [])
        if schemas and current_schema_id is not None:
            matched = _find_schema_by_id(schemas, current_schema_id)
            if matched:
                schema_fields = matched.get("fields", [])
        if not schema_fields and schemas:
            schema_fields = schemas[-1].get("fields", [])

    field_id_to_name = _collect_fields_recursive(schema_fields)
    # Keep a flat ordered list of top-level logical column names for the
    # positional fallback (used when Parquet files lack PARQUET:field_id).
    logical_column_names = [f["name"] for f in schema_fields]

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

    # Collect data files and delete files from the manifest chain.
    # The UniForm writer may produce both a DELETED entry and position deletes
    # for the same file (hybrid copy-on-write + merge-on-read).  When position
    # deletes target a file, the file is kept and only specific rows removed.
    added_files = set()      # file paths with status ADDED or EXISTING
    deleted_files = set()    # file paths with status DELETED
    pos_delete_files = []    # resolved paths to position-delete Parquet files
    eq_delete_files = []     # resolved paths to equality-delete Parquet files

    for ml_rec in ml_records:
        m_path = from_uri(ml_rec.get("manifest_path", ""))
        content_type = ml_rec.get("content", 0)  # 0=DATA, 1=DELETES
        if not os.path.isfile(m_path):
            m_path = os.path.join(table_path, "metadata", os.path.basename(m_path))
        with open(m_path, "rb") as f:
            for entry in fastavro.reader(f):
                df_entry = entry.get("data_file", entry)
                status = entry.get("status", 1)
                fp = from_uri(df_entry.get("file_path", ""))
                entry_content = df_entry.get("content", content_type)

                if entry_content == 1:
                    # Position delete file
                    if status != 2:
                        resolved = _resolve_iceberg_file(fp, table_path)
                        pos_delete_files.append(resolved)
                    continue
                if entry_content == 2:
                    # Equality delete file
                    if status != 2:
                        resolved = _resolve_iceberg_file(fp, table_path)
                        eq_delete_files.append(resolved)
                    continue
                if entry_content != 0:
                    continue  # Skip unknown content types

                if status == 2:
                    deleted_files.add(fp)
                else:
                    added_files.add(fp)

    # Build position delete index: {file_path -> set of row positions}
    # Normalize file paths (strip file:// prefix) to match manifest entries.
    pos_deletes = {}
    for pd_path in pos_delete_files:
        if not os.path.isfile(pd_path):
            print(f"  WARNING: position delete file not found, skipping: "
                  f"{pd_path}", file=sys.stderr)
            continue
        pd_table = pq.read_table(pd_path)
        if "file_path" in pd_table.column_names and "pos" in pd_table.column_names:
            for fp_val, pos_val in zip(
                pd_table.column("file_path").to_pylist(),
                pd_table.column("pos").to_pylist(),
            ):
                normalized_fp = from_uri(fp_val)
                pos_deletes.setdefault(normalized_fp, set()).add(pos_val)

    # Build equality delete index: collect column values that mark rows for
    # removal.  Each equality delete Parquet file contains rows whose column
    # values identify the data rows that should be removed.
    eq_delete_values = {}  # {column_name: set of values}
    for ed_path in eq_delete_files:
        if not os.path.isfile(ed_path):
            print(f"  WARNING: equality delete file not found, skipping: "
                  f"{ed_path}", file=sys.stderr)
            continue
        ed_table = pq.read_table(ed_path)
        # Rename columns via field IDs (delete files use the same physical names)
        rename_map = {}
        for arrow_field in ed_table.schema:
            md = arrow_field.metadata or {}
            fid = md.get(b"PARQUET:field_id")
            if fid is not None:
                fid_int = int(fid)
                if fid_int in field_id_to_name:
                    rename_map[arrow_field.name] = field_id_to_name[fid_int]
        if rename_map:
            new_names = [rename_map.get(c, c) for c in ed_table.column_names]
            ed_table = ed_table.rename_columns(new_names)
        for col_name in ed_table.column_names:
            vals = ed_table.column(col_name).to_pylist()
            eq_delete_values.setdefault(col_name, set()).update(vals)

    # A file targeted by position deletes is LIVE even if also marked DELETED
    # (the UniForm writer emits both; the delete entry is for the old snapshot).
    live_files = set()
    for fp in added_files:
        if fp in deleted_files and fp not in pos_deletes:
            continue  # Truly deleted (copy-on-write, no position deletes)
        live_files.add(fp)

    data_files = []
    for fp in live_files:
        resolved = _resolve_iceberg_file(fp, table_path)
        data_files.append((resolved, fp))

    # Read Parquet, apply position deletes, and rename columns via field IDs
    tables = []
    for df_path, original_fp in data_files:
        try:
            pf = pq.read_table(df_path)
        except pa.lib.ArrowTypeError:
            # Mixed dictionary / plain string encoding across row-groups or
            # partition directories.  Retry with dictionary columns decoded.
            pf = pq.read_table(df_path, read_dictionary=[])

        # Apply position deletes for this file
        if original_fp in pos_deletes:
            keep = [i for i in range(pf.num_rows) if i not in pos_deletes[original_fp]]
            if not keep:
                continue  # All rows deleted
            pf = pf.take(keep)

        arrow_schema = pf.schema
        rename_map = {}
        has_field_ids = False
        for arrow_field in arrow_schema:
            md = arrow_field.metadata or {}
            fid = md.get(b"PARQUET:field_id")
            if fid is not None:
                has_field_ids = True
                fid_int = int(fid)
                if fid_int in field_id_to_name:
                    rename_map[arrow_field.name] = field_id_to_name[fid_int]
        if rename_map:
            new_names = [rename_map.get(c, c) for c in pf.column_names]
            pf = pf.rename_columns(new_names)
        elif not has_field_ids and logical_column_names:
            # Fallback: Parquet files lack PARQUET:field_id metadata.
            # If the column count matches the Iceberg schema, rename
            # positionally.  This handles writers that use logical names
            # directly (e.g. pre-built test fixtures without field-id
            # metadata).
            if len(pf.column_names) == len(logical_column_names):
                pf = pf.rename_columns(logical_column_names)

        # Apply equality deletes: remove rows where ALL delete columns match
        if eq_delete_values:
            import pyarrow.compute as pc
            mask = None
            for col_name, del_vals in eq_delete_values.items():
                if col_name in pf.column_names:
                    col_mask = pc.is_in(
                        pf.column(col_name),
                        value_set=pa.array(list(del_vals)),
                    )
                    if mask is None:
                        mask = col_mask
                    else:
                        mask = pc.and_(mask, col_mask)
            if mask is not None:
                keep_mask = pc.invert(mask)
                pf = pf.filter(keep_mask)
                if pf.num_rows == 0:
                    continue

        tables.append(pf)

    return _concat_tables_safe(tables), metadata


# ---------------------------------------------------------------------------
# Delta reader
# ---------------------------------------------------------------------------
def read_delta_table(table_path):
    """Read a Delta table through its transaction log -> Arrow table.

    Walks the _delta_log/ directory, parses JSON commit files to find
    active 'add' actions (minus 'remove' actions), reads the referenced
    Parquet files, and applies Delta column mapping if present.
    """
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

    result_table = _concat_tables_safe(tables)

    delta_metadata = {
        "format": "delta",
        "version": len(commit_files) - 1,
        "num_files": len(active_files),
        "metadata": metadata,
        "protocol": protocol,
    }

    return result_table, delta_metadata
