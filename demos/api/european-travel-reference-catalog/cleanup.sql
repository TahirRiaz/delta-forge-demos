-- ============================================================================
-- Cleanup: European Travel Reference Catalog
-- ============================================================================
-- Reverse order of creation: external table → API ingest → connection →
-- vault entry → credential storage backend → schema → zone. WITH FILES
-- on the external table also removes the bronze landing directory the
-- ingest wrote to.
-- ============================================================================

-- 1. External table (also removes the on-disk JSON files INVOKE wrote)
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.travel_geo.european_countries WITH FILES;

-- 2. API ingest definition (cascades its run history)
DROP API INGEST IF EXISTS {{zone_name}}.rest_countries.europe;

-- 3. REST API connection (data source)
DROP CONNECTION IF EXISTS rest_countries;

-- 4. Vault credential entry
DROP CREDENTIAL IF EXISTS travel_api_token;

-- 5. Credential storage backend
DROP CREDENTIAL STORAGE IF EXISTS local_keychain;

-- 6. Schema then zone (zone last — schemas live under it)
DROP SCHEMA IF EXISTS {{zone_name}}.travel_geo;
-- Zone left in place by default — many demos may share `bronze`. Uncomment
-- if this demo runs in an isolated environment where the zone should go too.
-- DROP ZONE IF EXISTS {{zone_name}};
