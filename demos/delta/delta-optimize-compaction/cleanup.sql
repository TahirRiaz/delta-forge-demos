-- ============================================================================
-- Delta OPTIMIZE — Manual File Compaction & TARGET SIZE — Cleanup
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.daily_orders WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
