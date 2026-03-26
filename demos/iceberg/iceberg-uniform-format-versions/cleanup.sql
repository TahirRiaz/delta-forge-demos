-- ============================================================================
-- Iceberg UniForm Format Versions — Cleanup
-- ============================================================================

-- STEP 1: Drop all three tables
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.sensors_v1 WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.sensors_v2 WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.sensors_v3 WITH FILES;

-- STEP 2: Shared resources (used by other iceberg demos if present)
DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
