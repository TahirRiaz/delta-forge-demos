-- ============================================================================
-- Graph Social Network — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- Views must be dropped before tables they depend on.
-- ============================================================================

-- STEP 1: Drop graph definition (references vertex/edge tables via FK)
DROP GRAPH IF EXISTS social_network;

-- STEP 2: Drop graph configuration
DROP GRAPH CONFIG {{zone_name}}.graph.connections;
DROP GRAPH CONFIG {{zone_name}}.graph.employees;

-- STEP 3: Drop views (depend on tables, so drop first)
DROP VIEW IF EXISTS {{zone_name}}.graph.dept_connections;
DROP VIEW IF EXISTS {{zone_name}}.graph.employee_stats;

-- STEP 4: Drop Delta tables
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph.connections;
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph.employees;
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph.departments;

-- STEP 5: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.graph;
DROP ZONE IF EXISTS {{zone_name}};
