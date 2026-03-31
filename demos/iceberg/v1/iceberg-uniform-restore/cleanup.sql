-- Cleanup: Regulatory Compliance Recovery — RESTORE with UniForm

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.compliance_iceberg WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.compliance_records WITH FILES;
DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
