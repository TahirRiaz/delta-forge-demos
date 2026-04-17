-- ============================================================================
-- Iceberg Cross-Format Join — Retail Store Analytics — Cleanup
-- ============================================================================

-- STEP 1: Drop Iceberg read-back verification table
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.sales_iceberg WITH FILES;

-- STEP 2: Drop tables (Delta sales + external CSV stores)
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.sales WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.stores WITH FILES;

-- STEP 3: Shared resources (used by other iceberg demos if present)
DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
