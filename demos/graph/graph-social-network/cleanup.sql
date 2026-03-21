-- ============================================================================
-- Graph Social Network — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- Views must be dropped before tables they depend on.
-- ============================================================================

-- STEP 1: Drop graph definition (also cascade-deletes table mappings)
DROP GRAPH IF EXISTS {{zone_name}}.graph.social_network;

-- STEP 2: Drop views (depend on tables, so drop first)
DROP VIEW IF EXISTS {{zone_name}}.graph.dept_connections;
DROP VIEW IF EXISTS {{zone_name}}.graph.employee_stats;

-- STEP 3: Drop Delta tables (WITH FILES removes physical data too)
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph.connections WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph.employees WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph.departments WITH FILES;

-- STEP 4: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.graph;
DROP ZONE IF EXISTS {{zone_name}};
