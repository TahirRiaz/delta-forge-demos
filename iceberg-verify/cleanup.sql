-- ============================================================================
-- Iceberg UniForm Verification — Cleanup
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_verify.products WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_verify.sales WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_verify.evolve WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_verify.v3_table WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_verify;
DROP ZONE IF EXISTS {{zone_name}};
