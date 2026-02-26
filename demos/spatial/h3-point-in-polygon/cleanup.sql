-- ============================================================================
-- H3 Point-in-Polygon — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- Views must be dropped before the tables they depend on.
-- ============================================================================

-- STEP 1: Revoke permissions
REVOKE READ ON TABLE {{zone_name}}.spatial.zones FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.spatial.driver_positions FROM USER {{current_user}};

-- STEP 2: Drop schema columns metadata
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.spatial.zones;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.spatial.driver_positions;

-- STEP 3: Drop views (depend on tables, so drop first)
DROP VIEW IF EXISTS {{zone_name}}.spatial.zone_cells;
DROP VIEW IF EXISTS {{zone_name}}.spatial.driver_cells;

-- STEP 4: Drop Delta tables
DROP TABLE IF EXISTS {{zone_name}}.spatial.driver_positions;
DROP TABLE IF EXISTS {{zone_name}}.spatial.zones;

-- STEP 5: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.spatial;
DROP ZONE IF EXISTS {{zone_name}};
