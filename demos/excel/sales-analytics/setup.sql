-- ============================================================================
-- Excel Sales Analytics — Setup Script
-- ============================================================================
-- Creates five external tables from 4 Superstore sales XLSX files (2014–2017):
--   1. all_orders      — All 4 files unified (16,676 rows)
--   2. orders_2017     — Single file only (9,994 rows)
--   3. orders_range    — Cell range A1:K500 (limited columns + rows)
--   4. orders_trimmed  — Whitespace trimming + custom null values
--   5. orders_no_header — No header row (auto-generated column names)
--
-- Demonstrates:
--   - sheet_name: select "Orders" sheet by name
--   - has_header: true (default) and false (auto column names)
--   - skip_rows: skip rows after header
--   - max_rows: limit rows read
--   - range: read specific cell range (A1:K500)
--   - trim_whitespace: trim string whitespace
--   - null_values: custom NULL marker strings
--   - empty_cell_handling: AsNull mode
--   - infer_schema_rows: control type inference sample size
--   - Multi-file reading: 4 XLSX files from one directory
--   - file_metadata: df_file_name + df_row_number system columns
--   - Type inference: dates, numbers, strings auto-detected
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.excel
    COMMENT 'Excel-backed external tables';

-- ============================================================================
-- TABLE 1: all_orders — All 4 files, full data (16,676 rows)
-- ============================================================================
-- Reads all 4 XLSX files from the directory. Selects the "Orders" sheet by
-- name, enables file metadata for traceability, and samples 1000 rows for
-- schema inference.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.excel.all_orders
USING EXCEL
LOCATION '{{data_path}}'
OPTIONS (
    sheet_name = 'Orders',
    has_header = 'true',
    infer_schema_rows = '1000',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
DETECT SCHEMA FOR TABLE {{zone_name}}.excel.all_orders;
GRANT ADMIN ON TABLE {{zone_name}}.excel.all_orders TO USER {{current_user}};


-- ============================================================================
-- TABLE 2: orders_2017 — Single file only (9,994 rows)
-- ============================================================================
-- Uses file_filter to read only the 2017 file from the same directory.
-- Demonstrates single-file extraction from a multi-file location.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.excel.orders_2017
USING EXCEL
LOCATION '{{data_path}}'
OPTIONS (
    sheet_name = 'Orders',
    has_header = 'true',
    file_filter = 'sales-data-2017*',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
DETECT SCHEMA FOR TABLE {{zone_name}}.excel.orders_2017;
GRANT ADMIN ON TABLE {{zone_name}}.excel.orders_2017 TO USER {{current_user}};


-- ============================================================================
-- TABLE 3: orders_range — Cell range A1:K500 (limited columns + rows)
-- ============================================================================
-- Reads only columns A through K (Row ID to State) and the first 500 rows
-- (including header). Demonstrates the range option for targeted extraction.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.excel.orders_range
USING EXCEL
LOCATION '{{data_path}}'
OPTIONS (
    sheet_name = 'Orders',
    has_header = 'true',
    range = 'A1:K500',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
DETECT SCHEMA FOR TABLE {{zone_name}}.excel.orders_range;
GRANT ADMIN ON TABLE {{zone_name}}.excel.orders_range TO USER {{current_user}};


-- ============================================================================
-- TABLE 4: orders_trimmed — Whitespace trimming + custom null values
-- ============================================================================
-- Enables trim_whitespace and defines custom null marker strings. Empty cells
-- are handled as NULL (AsNull mode). Demonstrates data cleansing options.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.excel.orders_trimmed
USING EXCEL
LOCATION '{{data_path}}'
OPTIONS (
    sheet_name = 'Orders',
    has_header = 'true',
    trim_whitespace = 'true',
    null_values = '["", "NULL", "null", "N/A", "n/a", "#N/A", "#NA", "-"]',
    empty_cell_handling = 'AsNull',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
DETECT SCHEMA FOR TABLE {{zone_name}}.excel.orders_trimmed;
GRANT ADMIN ON TABLE {{zone_name}}.excel.orders_trimmed TO USER {{current_user}};


-- ============================================================================
-- TABLE 5: orders_no_header — No header row (auto-generated column names)
-- ============================================================================
-- Reads with has_header=false so columns get auto-generated names (column_0,
-- column_1, ...). skip_rows=1 skips the header row which would otherwise
-- appear as the first data row. Demonstrates headerless reading.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.excel.orders_no_header
USING EXCEL
LOCATION '{{data_path}}'
OPTIONS (
    sheet_name = 'Orders',
    has_header = 'false',
    skip_rows = '1',
    max_rows = '100',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
DETECT SCHEMA FOR TABLE {{zone_name}}.excel.orders_no_header;
GRANT ADMIN ON TABLE {{zone_name}}.excel.orders_no_header TO USER {{current_user}};
