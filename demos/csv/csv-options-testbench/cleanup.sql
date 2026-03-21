-- ============================================================================
-- CSV Advanced Options Testbench — Cleanup Script
-- ============================================================================
-- DROP TABLE commands automatically clean up catalog metadata (columns, etc.).
-- ============================================================================

-- Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.opt_delimiter WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.opt_null_value WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.opt_comment WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.opt_skip_rows WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.opt_max_rows WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.opt_trim WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.opt_quoted WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.opt_combined WITH FILES;

-- Shared resources (safe — won't fail if other demos use them)
DROP SCHEMA IF EXISTS {{zone_name}}.csv;
DROP ZONE IF EXISTS {{zone_name}};
