-- ============================================================================
-- Graph Flattened Mode — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Drop graph definition (references vertex/edge tables via FK)
DROP GRAPH IF EXISTS flattened_demo;

-- STEP 2: Drop graph configuration
DROP GRAPH CONFIG {{zone_name}}.graph.friendships_flattened;
DROP GRAPH CONFIG {{zone_name}}.graph.persons_flattened;

-- STEP 3: Drop Delta tables (WITH FILES removes physical data too)
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph.friendships_flattened WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph.persons_flattened WITH FILES;

-- STEP 4: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.graph;
DROP ZONE IF EXISTS {{zone_name}};
