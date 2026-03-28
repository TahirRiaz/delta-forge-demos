-- Cleanup: GIS Maritime Shipping

DROP DELTA TABLE IF EXISTS {{zone_name}}.maritime.positions WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.maritime.vessels WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.maritime.ports WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.maritime;
