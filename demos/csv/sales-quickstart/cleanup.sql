-- ============================================================================
-- Sales Schema Evolution Demo — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
--
-- The schema and zone are shared across demos. DROP SCHEMA / DROP ZONE will
-- succeed silently if they are empty, or produce a warning (not an error) if
-- other tables / schemas still exist — so it is always safe to leave them in.
-- ============================================================================


-- ============================================================================
-- STEP 1: Revoke Table Permission
-- ============================================================================

REVOKE READ ON TABLE external.csv.sales FROM USER {{current_user}};


-- ============================================================================
-- STEP 2: Drop Schema Columns
-- ============================================================================

DROP SCHEMA COLUMNS FOR TABLE external.csv.sales;
clear

-- ============================================================================
-- STEP 3: Drop External Table
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS external.csv.sales;


-- ============================================================================
-- STEP 4: Drop Schema
-- ============================================================================

DROP SCHEMA IF EXISTS external.csv;


-- ============================================================================
-- STEP 5: Drop Zone
-- ============================================================================

DROP ZONE IF EXISTS external;
