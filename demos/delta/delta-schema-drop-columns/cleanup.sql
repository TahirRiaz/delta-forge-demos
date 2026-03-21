-- ============================================================================
-- Delta Schema Evolution — Drop Columns & GDPR Cleanup — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

DROP VIEW IF EXISTS {{zone_name}}.delta_demos.user_profiles_clean;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.user_profiles WITH FILES;

-- Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
