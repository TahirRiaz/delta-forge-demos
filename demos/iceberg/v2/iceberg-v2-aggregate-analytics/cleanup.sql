-- ============================================================================
-- Iceberg V2 — Retail Multi-Dimensional Aggregation — Cleanup
-- ============================================================================

-- STEP 1: Drop tables (native Iceberg, files live under LOCATION)
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.retail_sales WITH FILES;

-- STEP 2: Shared resources (used by other iceberg demos if present)
DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
