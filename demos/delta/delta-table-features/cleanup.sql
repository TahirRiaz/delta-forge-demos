-- ============================================================================
-- Delta Protocol & Table Features — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.feature_demo WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.audit_trail WITH FILES;

-- Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
