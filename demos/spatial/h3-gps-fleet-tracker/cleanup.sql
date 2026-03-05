-- ============================================================================
-- H3 GPS Fleet Tracker — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- Views must be dropped before tables they depend on.
-- DROP TABLE commands automatically clean up catalog metadata (columns, etc.).
-- ============================================================================

-- STEP 1: Drop views (depend on tables, so drop first)
DROP VIEW IF EXISTS {{zone_name}}.spatial.region_cells;
DROP VIEW IF EXISTS {{zone_name}}.spatial.points_h3;

-- STEP 2: Drop Delta tables (WITH FILES removes physical data too)
DROP DELTA TABLE IF EXISTS {{zone_name}}.spatial.gps_points WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.spatial.regions WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.spatial.landmarks WITH FILES;

-- STEP 3: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.spatial;
DROP ZONE IF EXISTS {{zone_name}};
