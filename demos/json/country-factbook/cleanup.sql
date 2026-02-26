-- ============================================================================
-- JSON Country Factbook — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.json.countries;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.json.country_economy;

-- STEP 2: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.json;
DROP ZONE IF EXISTS {{zone_name}};
