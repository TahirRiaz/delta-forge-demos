-- ============================================================================
-- Cleanup: European Travel — Incremental Wave-Based Enrichment
-- ============================================================================
-- Reverse order of creation: bronze external table → silver delta table →
-- API ingest → connection → credential → schema. WITH FILES on both
-- tables removes their on-disk artefacts. The zone is left in place so
-- the sibling reference-catalog demo (if installed) keeps working.
-- ============================================================================

-- 1. Bronze external table (removes the JSON pages INVOKE wrote)
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.travel_waves.alpha_batch_bronze WITH FILES;

-- 2. Silver Delta table (removes its Delta log + parquet)
DROP DELTA TABLE IF EXISTS {{zone_name}}.travel_waves.country_catalog WITH FILES;

-- 3. API ingest definition (cascades its run history)
DROP API INGEST IF EXISTS {{zone_name}}.rest_countries_waves.alpha_batch;

-- 4. REST API connection
DROP CONNECTION IF EXISTS rest_countries_waves;

-- 5. Credential vault entry (OS keychain backend is never dropped)
DROP CREDENTIAL IF EXISTS travel_incremental_token;

-- 6. Schema (zone left in place — many demos share `bronze`)
DROP SCHEMA IF EXISTS {{zone_name}}.travel_waves;
