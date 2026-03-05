-- ============================================================================
-- Avro IoT Sensors — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.avro.all_readings WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.avro.floor4_only WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.avro.readings_sample WITH FILES;

-- STEP 2: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.avro;
DROP ZONE IF EXISTS {{zone_name}};
