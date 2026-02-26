-- ============================================================================
-- Excel Sales Analytics — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.excel.all_orders;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.excel.orders_2017;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.excel.orders_range;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.excel.orders_trimmed;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.excel.orders_no_header;

-- STEP 2: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.excel;
DROP ZONE IF EXISTS {{zone_name}};
