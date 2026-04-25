-- ============================================================================
-- Cleanup: Farm Weather Pulse
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.openmeteo_api.weather_silver WITH FILES;

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.openmeteo_api.weather_bronze WITH FILES;

DROP API ENDPOINT IF EXISTS {{zone_name}}.openmeteo_api.observation_oslo;
DROP API ENDPOINT IF EXISTS {{zone_name}}.openmeteo_api.observation_hamburg;
DROP API ENDPOINT IF EXISTS {{zone_name}}.openmeteo_api.observation_dublin;

DROP CONNECTION IF EXISTS openmeteo_api;

DROP SCHEMA IF EXISTS {{zone_name}}.openmeteo_api;
