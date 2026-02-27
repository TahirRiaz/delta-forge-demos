-- ============================================================================
-- ORC Server Logs — Setup Script
-- ============================================================================
-- Creates three external tables from 5 server access log ORC files:
--   1. all_requests   — All 5 files with schema evolution (2,500 rows)
--   2. api01_only     — Single server via file_filter (500 rows)
--   3. requests_sample — Sampled subset via max_rows (50 per file)
--
-- Demonstrates:
--   - Multi-file reading: 5 ORC files in one table
--   - Schema evolution: v1 (11 fields) → v2 (13 fields, adds
--     request_body_bytes, cache_hit); NULL filling for web servers
--   - Self-describing schema: ORC file footers provide types automatically
--   - file_filter: glob pattern to select files by server name
--   - max_rows: limit rows per file for data profiling
--   - file_metadata: df_file_name + df_row_number system columns
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.orc
    COMMENT 'ORC-backed external tables';

-- ============================================================================
-- TABLE 1: all_requests — All 5 files with schema evolution
-- ============================================================================
-- Reads all ORC files from the directory. Files use two schema versions:
--   v1 (web-01, web-02, web-03): 11 fields (basic access log)
--   v2 (api-01, api-02): 13 fields (adds request_body_bytes, cache_hit)
-- The union schema merges both versions; v1 rows get NULL for new columns.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.orc.all_requests
USING ORC
LOCATION '{{data_path}}'
OPTIONS (
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
DETECT SCHEMA FOR TABLE {{zone_name}}.orc.all_requests;
GRANT READ ON TABLE {{zone_name}}.orc.all_requests TO USER {{current_user}};


-- ============================================================================
-- TABLE 2: api01_only — Single server via file_filter (500 rows)
-- ============================================================================
-- Uses file_filter to read only api-01_access.orc, which uses schema v2
-- (includes request_body_bytes and cache_hit).
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.orc.api01_only
USING ORC
LOCATION '{{data_path}}'
OPTIONS (
    file_filter = 'api-01*',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
DETECT SCHEMA FOR TABLE {{zone_name}}.orc.api01_only;
GRANT READ ON TABLE {{zone_name}}.orc.api01_only TO USER {{current_user}};


-- ============================================================================
-- TABLE 3: requests_sample — Data profiling via max_rows (50 per file)
-- ============================================================================
-- Limits to 50 rows per file for quick data profiling. With 5 files,
-- produces approximately 250 rows — enough to inspect data quality
-- without reading the full 2,500-row dataset.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.orc.requests_sample
USING ORC
LOCATION '{{data_path}}'
OPTIONS (
    max_rows = '50',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
DETECT SCHEMA FOR TABLE {{zone_name}}.orc.requests_sample;
GRANT READ ON TABLE {{zone_name}}.orc.requests_sample TO USER {{current_user}};
