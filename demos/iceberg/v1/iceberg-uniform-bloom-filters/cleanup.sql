-- Cleanup: Customer Loyalty Program — Bloom Filters with UniForm

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_bloom.members_iceberg WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_bloom.members WITH FILES;
DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_bloom;
