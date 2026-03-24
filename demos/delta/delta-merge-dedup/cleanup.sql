-- ============================================================================
-- Delta MERGE — Deduplication (Keep Latest) — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.events_deduped WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.events WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
