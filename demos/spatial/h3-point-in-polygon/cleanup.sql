-- ============================================================================
-- H3 Point-in-Polygon — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- Views must be dropped before the tables they depend on.
-- ============================================================================

-- STEP 1: Drop views (depend on tables, so drop first)
DROP VIEW IF EXISTS {{zone_name}}.spatial.zone_cells;
DROP VIEW IF EXISTS {{zone_name}}.spatial.driver_cells;

-- STEP 2: Drop Delta tables (WITH FILES removes physical data too)
DROP DELTA TABLE IF EXISTS {{zone_name}}.spatial.driver_positions WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.spatial.zones WITH FILES;

-- STEP 3: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.spatial;
DROP ZONE IF EXISTS {{zone_name}};
