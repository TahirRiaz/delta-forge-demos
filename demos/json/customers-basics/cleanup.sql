-- ============================================================================
-- JSON Customers Basics — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Revoke permissions
REVOKE READ ON TABLE {{zone_name}}.json.customers FROM USER {{current_user}};

-- STEP 2: Drop schema columns metadata
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.json.customers;

-- STEP 3: Drop external table
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.json.customers;

-- STEP 4: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.json;
DROP ZONE IF EXISTS {{zone_name}};
