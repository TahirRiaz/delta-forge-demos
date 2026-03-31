-- ============================================================================
-- Sales Territory Optimization — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- Graph definition must be dropped before the tables it references.
-- ============================================================================

-- STEP 1: Drop graph definition
DROP GRAPH IF EXISTS {{zone_name}}.graph_demos.customer_network;

-- STEP 2: Drop working tables (populated by Cypher in queries.sql)
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph_demos.community_assignments WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph_demos.influence_scores WITH FILES;

-- STEP 3: Drop data tables
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph_demos.sales_reps WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph_demos.orders WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph_demos.referrals WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.graph_demos.customers WITH FILES;

-- STEP 4: Drop schema and zone
DROP SCHEMA IF EXISTS {{zone_name}}.graph_demos;
DROP ZONE IF EXISTS {{zone_name}};
