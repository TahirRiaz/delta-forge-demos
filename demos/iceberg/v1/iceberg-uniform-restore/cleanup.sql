-- Cleanup: Regulatory Compliance Recovery — RESTORE with UniForm

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_restore.compliance_iceberg WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_restore.compliance_records WITH FILES;
DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_restore;
