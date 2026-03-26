-- ============================================================================
-- Iceberg UniForm Schema Evolution — Cleanup
-- ============================================================================

-- STEP 1: Drop tables (includes Delta log + Iceberg metadata/ directory)
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.customer_orders WITH FILES;

-- STEP 2: Shared resources (used by other iceberg demos if present)
DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
