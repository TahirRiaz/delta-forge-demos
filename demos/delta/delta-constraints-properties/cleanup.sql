-- ============================================================================
-- Delta Constraints & Table Properties — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.event_log WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.invoices WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
