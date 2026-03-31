-- ============================================================================
-- Graph Storage Modes — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql (3 graph definitions, 6 tables).
-- ============================================================================

-- STEP 1: Drop graph definitions
DROP GRAPH IF EXISTS {{zone_name}}.graph_demos.storage_flat;
DROP GRAPH IF EXISTS {{zone_name}}.graph_demos.storage_hybrid;
DROP GRAPH IF EXISTS {{zone_name}}.graph_demos.storage_json;

-- STEP 2: Drop flattened tables
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph_demos.edges_flat WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph_demos.persons_flat WITH FILES;

-- STEP 3: Drop hybrid tables
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph_demos.edges_hybrid WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph_demos.persons_hybrid WITH FILES;

-- STEP 4: Drop JSON tables
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph_demos.edges_json WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph_demos.persons_json WITH FILES;

-- STEP 5: Shared resources
DROP SCHEMA IF EXISTS {{zone_name}}.graph_demos;
DROP ZONE IF EXISTS {{zone_name}};
