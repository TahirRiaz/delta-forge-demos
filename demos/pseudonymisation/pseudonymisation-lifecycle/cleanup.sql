-- ============================================================================
-- Pseudonymisation Lifecycle — Insurance Claims — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql: pseudonymisation rules, table,
-- schema, and zone.
--
-- Pseudonymisation rules are dropped per-table (omitting column pattern drops
-- all rules for that table). Table is dropped after rules are removed.
-- ============================================================================

-- STEP 1: Drop Pseudonymisation Rules
DROP PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.insurance_claims;

-- STEP 2: Drop Table
DROP TABLE IF EXISTS {{zone_name}}.pseudonymisation.insurance_claims;

-- STEP 3: Drop Schema
DROP SCHEMA IF EXISTS {{zone_name}}.pseudonymisation;

-- STEP 4: Drop Zone
DROP ZONE IF EXISTS {{zone_name}};
