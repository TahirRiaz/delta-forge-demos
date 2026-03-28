-- ============================================================================
-- Graph Mutations — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql (including mutated data).
-- ============================================================================

-- STEP 1: Drop graph definition
DROP GRAPH IF EXISTS {{zone_name}}.graph.hospital_referrals;

-- STEP 2: Drop Delta tables (WITH FILES removes physical data too)
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph.referrals WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph.physicians WITH FILES;

-- STEP 3: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.graph;
DROP ZONE IF EXISTS {{zone_name}};
