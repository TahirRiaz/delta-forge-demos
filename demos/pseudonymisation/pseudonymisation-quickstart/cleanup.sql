-- ============================================================================
-- Pseudonymisation Quickstart — Banking KYC — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql: pseudonymisation rules, table,
-- schema, and zone.
-- ============================================================================

-- STEP 1: Drop Pseudonymisation Rules
DROP PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.bank_customers;

-- STEP 2: Drop Table
DROP TABLE IF EXISTS {{zone_name}}.pseudonymisation_demos.bank_customers;

-- STEP 3: Drop Schema
DROP SCHEMA IF EXISTS {{zone_name}}.pseudonymisation_demos;

-- STEP 4: Drop Zone
DROP ZONE IF EXISTS {{zone_name}};
