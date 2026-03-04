-- ============================================================================
-- Graph Stress Test — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- Views must be dropped before tables they depend on.
-- ============================================================================

-- STEP 1: Drop graph definition (references vertex/edge tables)
DROP GRAPH IF EXISTS stress_test_network;

-- STEP 2: Drop graph configuration
DROP GRAPH CONFIG {{zone_name}}.graph.st_edges;
DROP GRAPH CONFIG {{zone_name}}.graph.st_people;

-- STEP 3: Drop views (depend on tables, so drop first)
DROP VIEW IF EXISTS {{zone_name}}.graph.st_dept_matrix;
DROP VIEW IF EXISTS {{zone_name}}.graph.st_people_stats;

-- STEP 4: Drop Delta tables
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph.st_edges;
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph.st_people;
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph.st_departments;

-- STEP 5: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.graph;
DROP ZONE IF EXISTS {{zone_name}};
