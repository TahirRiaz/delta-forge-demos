-- ============================================================================
-- Delta MERGE Composite Keys — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.telemetry_batch WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.fleet_daily_summary WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
