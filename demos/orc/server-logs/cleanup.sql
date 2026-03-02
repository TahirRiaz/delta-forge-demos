-- ============================================================================
-- ORC Server Logs — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.orc.all_requests;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.orc.api01_only;

-- STEP 2: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.orc;
DROP ZONE IF EXISTS {{zone_name}};
