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
-- STEP 1: Drop External Table
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS external.csv.sales;


-- ============================================================================
-- STEP 2: Drop Schema (no-op warning if other tables remain)
-- ============================================================================

DROP SCHEMA IF EXISTS external.csv;


-- ============================================================================
-- STEP 3: Drop Zone (no-op warning if other schemas remain)
-- ============================================================================

DROP ZONE IF EXISTS external;
