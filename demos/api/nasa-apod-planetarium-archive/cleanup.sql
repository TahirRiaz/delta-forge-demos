-- ============================================================================
-- Cleanup: Planetarium APOD Archive
-- ============================================================================
-- Reverse order: silver → bronze → endpoint → connection → credential →
-- schema. The zone is left in place for sibling API demos.
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.space_imagery.apod_silver WITH FILES;

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.space_imagery.apod_bronze WITH FILES;

DROP API ENDPOINT IF EXISTS {{zone_name}}.nasa_api.apod_archive;

DROP CONNECTION IF EXISTS nasa_api;

DROP CREDENTIAL IF EXISTS nasa_apod_key;

DROP SCHEMA IF EXISTS {{zone_name}}.space_imagery;
