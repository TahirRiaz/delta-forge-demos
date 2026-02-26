-- ============================================================================
-- JSON Country Factbook — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Revoke permissions
REVOKE READ ON TABLE {{zone_name}}.json.countries FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.json.country_economy FROM USER {{current_user}};

-- STEP 2: Drop schema columns metadata
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.json.countries;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.json.country_economy;

-- STEP 3: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.json.countries;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.json.country_economy;

-- STEP 4: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.json;
DROP ZONE IF EXISTS {{zone_name}};
