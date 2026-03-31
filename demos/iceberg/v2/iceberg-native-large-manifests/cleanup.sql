-- ============================================================================
-- Iceberg Native Large Manifests (Web Analytics) — Cleanup
-- ============================================================================

-- STEP 1: Drop external table and its files
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.web_analytics WITH FILES;

-- STEP 2: Shared resources
DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
