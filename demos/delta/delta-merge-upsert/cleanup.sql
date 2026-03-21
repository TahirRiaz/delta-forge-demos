-- ============================================================================
-- Delta MERGE — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.customer_updates WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.customers WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
