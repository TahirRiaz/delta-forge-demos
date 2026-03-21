-- ============================================================================
-- Dolphins Social Network — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- Graph definition must be dropped before the tables it references.
-- ============================================================================

-- STEP 1: Drop graph definition (also cascade-deletes table mappings)
DROP GRAPH IF EXISTS {{zone_name}}.dolphins.dolphins_social;

-- STEP 2: Drop Delta tables
DROP DELTA TABLE IF EXISTS {{zone_name}}.dolphins.vertices WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.dolphins.edges WITH FILES;

-- STEP 3: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.dolphins_edges WITH FILES;

-- STEP 4: Drop schemas and zone
DROP SCHEMA IF EXISTS {{zone_name}}.dolphins;
DROP SCHEMA IF EXISTS {{zone_name}}.raw;
DROP ZONE IF EXISTS {{zone_name}};
