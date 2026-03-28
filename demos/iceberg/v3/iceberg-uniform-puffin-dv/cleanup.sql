-- ============================================================================
-- Iceberg UniForm Puffin Deletion Vectors — Cleanup
-- ============================================================================

-- STEP 1: Drop tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.puffin_dv_demo.products_iceberg WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.puffin_dv_demo.products WITH FILES;

-- STEP 2: Drop schema
DROP SCHEMA IF EXISTS {{zone_name}}.puffin_dv_demo;
