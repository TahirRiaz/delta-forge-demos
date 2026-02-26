-- ============================================================================
-- CSV Advanced Options Testbench — Cleanup Script
-- ============================================================================
-- DROP TABLE commands automatically clean up catalog metadata (columns, etc.).
-- ============================================================================

-- Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.opt_delimiter;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.opt_null_value;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.opt_comment;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.opt_skip_rows;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.opt_max_rows;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.opt_trim;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.opt_quoted;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.opt_combined;

-- Shared resources (safe — won't fail if other demos use them)
DROP SCHEMA IF EXISTS {{zone_name}}.csv;
DROP ZONE IF EXISTS {{zone_name}};
