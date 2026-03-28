-- Cleanup: H3+GIS Delivery Optimization

DROP DELTA TABLE IF EXISTS {{zone_name}}.logistics.stores WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.logistics.warehouses WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.logistics;
