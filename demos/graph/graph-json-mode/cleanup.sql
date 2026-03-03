-- ============================================================================
-- Graph JSON Mode — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Clear graph configuration
CLEAR GRAPH CONFIG {{zone_name}}.graph.friendships_json;
CLEAR GRAPH CONFIG {{zone_name}}.graph.persons_json;

-- STEP 2: Drop Delta tables
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph.friendships_json;
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph.persons_json;

-- STEP 3: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.graph;
DROP ZONE IF EXISTS {{zone_name}};
