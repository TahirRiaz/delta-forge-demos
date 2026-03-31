-- ============================================================================
-- Iceberg Energy Grid Monitoring — Cleanup
-- ============================================================================

-- STEP 1: Drop Iceberg read-back verification tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.grid_readings_iceberg_readback WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.grid_readings_delta WITH FILES;

-- STEP 2: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.grid_readings WITH FILES;

-- STEP 3: Shared resources (used by other iceberg demos if present)
DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
