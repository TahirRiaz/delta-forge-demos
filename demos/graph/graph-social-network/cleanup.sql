-- ============================================================================
-- Graph Social Network — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- Views must be dropped before tables they depend on.
-- ============================================================================

-- STEP 1: Clear graph configuration
CLEAR GRAPH CONFIG {{zone_name}}.graph.connections;
CLEAR GRAPH CONFIG {{zone_name}}.graph.employees;

-- STEP 2: Drop views (depend on tables, so drop first)
DROP VIEW IF EXISTS {{zone_name}}.graph.dept_connections;
DROP VIEW IF EXISTS {{zone_name}}.graph.employee_stats;

-- STEP 3: Drop Delta tables
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph.connections;
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph.employees;
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph.departments;

-- STEP 4: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.graph;
DROP ZONE IF EXISTS {{zone_name}};
