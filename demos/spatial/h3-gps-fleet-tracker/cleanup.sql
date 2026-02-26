-- ============================================================================
-- H3 GPS Fleet Tracker — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- Views must be dropped before tables they depend on.
-- ============================================================================

-- STEP 1: Revoke permissions
REVOKE READ ON TABLE {{zone_name}}.spatial.landmarks FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.spatial.regions FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.spatial.gps_points FROM USER {{current_user}};

-- STEP 2: Drop schema columns metadata
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.spatial.landmarks;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.spatial.regions;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.spatial.gps_points;

-- STEP 3: Drop views (depend on tables, so drop first)
DROP VIEW IF EXISTS {{zone_name}}.spatial.region_cells;
DROP VIEW IF EXISTS {{zone_name}}.spatial.points_h3;

-- STEP 4: Drop Delta tables
DROP DELTA TABLE IF EXISTS {{zone_name}}.spatial.gps_points;
DROP DELTA TABLE IF EXISTS {{zone_name}}.spatial.regions;
DROP DELTA TABLE IF EXISTS {{zone_name}}.spatial.landmarks;

-- STEP 5: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.spatial;
DROP ZONE IF EXISTS {{zone_name}};
