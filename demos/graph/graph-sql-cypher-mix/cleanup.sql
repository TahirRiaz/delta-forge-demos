-- ============================================================================
-- Sales Territory Optimization — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- Graph definition must be dropped before the tables it references.
-- ============================================================================

-- STEP 1: Drop graph definition
DROP GRAPH IF EXISTS {{zone_name}}.sales.customer_network;

-- STEP 2: Drop working tables (populated by Cypher in queries.sql)
DROP DELTA TABLE IF EXISTS {{zone_name}}.sales.community_assignments WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.sales.influence_scores WITH FILES;

-- STEP 3: Drop data tables
DROP DELTA TABLE IF EXISTS {{zone_name}}.sales.sales_reps WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.sales.orders WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.sales.referrals WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.sales.customers WITH FILES;

-- STEP 4: Drop schema and zone
DROP SCHEMA IF EXISTS {{zone_name}}.sales;
DROP ZONE IF EXISTS {{zone_name}};
