-- ============================================================================
-- Delta Time Travel — Point-in-Time Joins — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.fx_trades WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.fx_rates WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
