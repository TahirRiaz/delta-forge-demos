-- Cleanup: E-Commerce Order Lifecycle — Change Data Feed with UniForm

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_cdf.orders_iceberg WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_cdf.orders WITH FILES;
DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_cdf;
