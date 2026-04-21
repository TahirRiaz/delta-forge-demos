-- ============================================================================
-- FHIR Multi-Vendor Patients (XML) — Cleanup Script
-- ============================================================================
-- Drops the silver Delta table, then the bronze external table, then the
-- schema, then the zone. Order is important: tables -> schema -> zone.
-- WITH FILES removes the underlying parquet (silver) + XML files (bronze).
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.hie.patients_silver WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.hie.patients WITH FILES;
DROP SCHEMA IF EXISTS {{zone_name}}.hie;
DROP ZONE IF EXISTS {{zone_name}};
