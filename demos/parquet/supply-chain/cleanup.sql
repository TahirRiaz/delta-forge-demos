-- ============================================================================
-- Parquet Supply Chain — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.parquet.all_orders WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.parquet.orders_2015 WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.parquet.orders_sample WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.parquet.orders_q1_2014 WITH FILES;

-- STEP 2: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.parquet;
DROP ZONE IF EXISTS {{zone_name}};
