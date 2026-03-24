-- ============================================================================
-- Delta MERGE — Soft Delete with BY SOURCE — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.vendor_feed WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.vendors WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
