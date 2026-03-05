-- ============================================================================
-- Excel Sales Analytics — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.excel.all_orders WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.excel.orders_2017 WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.excel.orders_range WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.excel.orders_trimmed WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.excel.orders_no_header WITH FILES;

-- STEP 2: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.excel;
DROP ZONE IF EXISTS {{zone_name}};
