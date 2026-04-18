-- ============================================================================
-- Cleanup: Fleet Dispatch Network — AUTO REFRESH CSR demo
-- ============================================================================
-- Drop order: paired graphs → delta tables (WITH FILES) → schema.
-- The zone is shared with other demos and is not dropped here.

DROP GRAPH IF EXISTS {{zone_name}}.fleet_dispatch.dispatch_batch;
DROP GRAPH IF EXISTS {{zone_name}}.fleet_dispatch.dispatch_live;

DROP DELTA TABLE IF EXISTS {{zone_name}}.fleet_dispatch.routes WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.fleet_dispatch.hubs   WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.fleet_dispatch;
