-- Cleanup: GIS Emergency Response Network

DROP DELTA TABLE IF EXISTS {{zone_name}}.emergency.response_zones WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.emergency.incidents WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.emergency.hospitals WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.emergency;
