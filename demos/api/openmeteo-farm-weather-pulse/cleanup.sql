-- ============================================================================
-- Cleanup: Farm Weather Pulse
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.agri_telemetry.weather_silver WITH FILES;

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.agri_telemetry.weather_bronze WITH FILES;

DROP API ENDPOINT IF EXISTS {{zone_name}}.openmeteo_api.current_observation;

DROP CONNECTION IF EXISTS openmeteo_api;

DROP SCHEMA IF EXISTS {{zone_name}}.agri_telemetry;
