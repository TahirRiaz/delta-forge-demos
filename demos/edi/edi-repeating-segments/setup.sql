-- ============================================================================
-- EDI Repeating Segments — Setup Script
-- ============================================================================
-- Demonstrates repeating_segment_mode for X12 transactions containing multiple
-- occurrences of the same segment type (N1, PO1, REF, etc.).
--
-- The default EDI parser returns only the first occurrence of each segment.
-- This demo creates three tables with different repeating_segment_mode values
-- on the same 14-file X12 feed to compare approaches:
--
--   1. repeating_first    — Default (First) mode: only first occurrence kept
--                           n1_1 = first entity code, n1_2 = first party name
--   2. repeating_concat   — Concatenate mode: pipe-delimited values
--                           n1_1 = "ST|BY|SO", n1_2 = "Name1|Name2|Name3"
--   3. repeating_json     — ToJson mode: JSON arrays
--                           n1_1 = ["ST","BY","SO"], n1_2 = ["Name1","Name2"]
--
-- Materialized paths: N1 (party names/codes) and PO1 (line items)
--
-- Variables (auto-injected by Delta Forge):
--   data_path     — Local or cloud path where demo data files were downloaded
--   current_user  — Username of the current logged-in user
--   zone_name     — Target zone name (defaults to 'external')
--
-- Naming convention: zone_name.format.table
--   zone   = {{zone_name}}  (defaults to 'external')
--   schema = 'edi'          (the file format)
--   table  = object name
-- ============================================================================


-- ============================================================================
-- STEP 1: Zone & Schema
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}}
    TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.edi
    COMMENT 'EDI transaction-backed external tables';


-- ============================================================================
-- TABLE 1: repeating_first — Default (First) mode
-- ============================================================================
-- Only the first occurrence of each repeating segment is kept. Files with
-- multiple N1 segments lose all but the first. This is the baseline behavior
-- when no repeating_segment_mode is specified.
--
-- Column names are the standard materialized path names:
--   n1_1 = entity code (first N1 only)
--   n1_2 = party name  (first N1 only)
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi.repeating_first
USING EDI
LOCATION '{{data_path}}/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "x12",
        "materialized_paths": ["n1_1", "n1_2", "po1_1", "po1_2", "po1_3", "po1_4"]
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

DETECT SCHEMA FOR TABLE {{zone_name}}.edi.repeating_first;

GRANT ADMIN ON TABLE {{zone_name}}.edi.repeating_first TO USER {{current_user}};


-- ============================================================================
-- TABLE 2: repeating_concat — Pipe-delimited concatenation (Concatenate mode)
-- ============================================================================
-- Each materialized path becomes a single column with all occurrences joined
-- by a pipe (|) separator:
--   n1_1 = "ST|BY|SO"
--   n1_2 = "Ship To Name|Buyer Name|Seller Name"
--   po1_2 = "2500|2000|1000"
--
-- This is the most compact representation — useful when the number of
-- occurrences varies and you want a single column to scan or search.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi.repeating_concat
USING EDI
LOCATION '{{data_path}}/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "x12",
        "materialized_paths": ["n1_1", "n1_2", "po1_1", "po1_2", "po1_3", "po1_4"],
        "repeating_segment_mode": "Concatenate"
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

DETECT SCHEMA FOR TABLE {{zone_name}}.edi.repeating_concat;

GRANT ADMIN ON TABLE {{zone_name}}.edi.repeating_concat TO USER {{current_user}};


-- ============================================================================
-- TABLE 3: repeating_json — JSON array output (ToJson mode)
-- ============================================================================
-- Each materialized path becomes a JSON array containing all occurrences:
--   n1_1 = ["ST", "BY", "SO"]
--   n1_2 = ["Ship To Name", "Buyer Name", "Seller Name"]
--   po1_2 = ["2500", "2000", "1000"]
--
-- Best for programmatic access — parse with JSON functions, integrate with
-- downstream JSON-native pipelines, or feed into APIs that expect arrays.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi.repeating_json
USING EDI
LOCATION '{{data_path}}/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "x12",
        "materialized_paths": ["n1_1", "n1_2", "po1_1", "po1_2", "po1_3", "po1_4"],
        "repeating_segment_mode": "ToJson"
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

DETECT SCHEMA FOR TABLE {{zone_name}}.edi.repeating_json;

GRANT ADMIN ON TABLE {{zone_name}}.edi.repeating_json TO USER {{current_user}};
