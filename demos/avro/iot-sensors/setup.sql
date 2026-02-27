-- ============================================================================
-- Avro IoT Sensors — Setup Script
-- ============================================================================
-- Creates three external tables from 5 building-floor Avro files:
--   1. all_readings   — All 5 files with schema evolution (2,500 rows)
--   2. floor4_only    — Single floor via file_filter (500 rows)
--   3. readings_sample — Sampled subset via max_rows (50 per file)
--
-- Demonstrates:
--   - Multi-file reading: 5 Avro files in one table
--   - Schema evolution: v1 (8 fields) → v2 (10 fields, adds battery_pct,
--     firmware_version); NULL filling for floors 1–3
--   - Self-describing schema: Avro file headers provide types automatically
--   - Mixed compression codecs: null (floors 1,3,5) and deflate (floors 2,4)
--   - file_filter: glob pattern to select files by floor
--   - max_rows: limit rows per file for data profiling
--   - file_metadata: df_file_name + df_row_number system columns
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.avro
    COMMENT 'Avro-backed external tables';

-- ============================================================================
-- TABLE 1: all_readings — All 5 files with schema evolution
-- ============================================================================
-- Reads all Avro files from the directory. Files use two schema versions:
--   v1 (floors 1–3): sensor_id, floor, zone, timestamp, temperature_c,
--                     humidity_pct, co2_ppm, occupancy
--   v2 (floors 4–5): same + battery_pct, firmware_version
-- The union schema merges both versions; v1 rows get NULL for new columns.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.avro.all_readings
USING AVRO
LOCATION '{{data_path}}'
OPTIONS (
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
DETECT SCHEMA FOR TABLE {{zone_name}}.avro.all_readings;
GRANT READ ON TABLE {{zone_name}}.avro.all_readings TO USER {{current_user}};


-- ============================================================================
-- TABLE 2: floor4_only — Single floor via file_filter (500 rows)
-- ============================================================================
-- Uses file_filter to read only floor4_sensors.avro, which uses schema v2
-- (includes battery_pct and firmware_version) with deflate compression.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.avro.floor4_only
USING AVRO
LOCATION '{{data_path}}'
OPTIONS (
    file_filter = 'floor4*',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
DETECT SCHEMA FOR TABLE {{zone_name}}.avro.floor4_only;
GRANT READ ON TABLE {{zone_name}}.avro.floor4_only TO USER {{current_user}};


-- ============================================================================
-- TABLE 3: readings_sample — Data profiling via max_rows (50 per file)
-- ============================================================================
-- Limits to 50 rows per file for quick data profiling. With 5 files,
-- produces approximately 250 rows — enough to inspect data quality
-- without reading the full 2,500-row dataset.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.avro.readings_sample
USING AVRO
LOCATION '{{data_path}}'
OPTIONS (
    max_rows = '50',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
DETECT SCHEMA FOR TABLE {{zone_name}}.avro.readings_sample;
GRANT READ ON TABLE {{zone_name}}.avro.readings_sample TO USER {{current_user}};
