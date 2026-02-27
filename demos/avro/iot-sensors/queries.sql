-- ============================================================================
-- Avro IoT Sensors — Verification Queries
-- ============================================================================
-- Each query verifies a specific Avro feature: multi-file reading, schema
-- evolution, file_filter, max_rows, file_metadata, self-describing schema,
-- and mixed compression codec support.
-- ============================================================================


-- ============================================================================
-- 1. TOTAL ROW COUNT — 2,500 readings across 5 floor files
-- ============================================================================

SELECT 'total_row_count' AS check_name,
       COUNT(*) AS actual,
       2500 AS expected,
       CASE WHEN COUNT(*) = 2500 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.avro.all_readings;


-- ============================================================================
-- 2. BROWSE READINGS — See column types (self-describing Avro schema)
-- ============================================================================

SELECT sensor_id, floor, zone, timestamp, temperature_c, humidity_pct,
       co2_ppm, occupancy, battery_pct, firmware_version
FROM {{zone_name}}.avro.all_readings
LIMIT 20;


-- ============================================================================
-- 3. ROWS PER FILE — 500 rows each across 5 files
-- ============================================================================

SELECT df_file_name, COUNT(*) AS row_count
FROM {{zone_name}}.avro.all_readings
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 4. SCHEMA EVOLUTION — battery_pct is NULL for v1 floors (1–3),
--    non-NULL for v2 floors (4–5)
-- ============================================================================

SELECT floor,
       COUNT(*) AS total_rows,
       COUNT(battery_pct) AS battery_non_null,
       COUNT(firmware_version) AS firmware_non_null
FROM {{zone_name}}.avro.all_readings
GROUP BY floor
ORDER BY floor;


-- ============================================================================
-- 5. FILE FILTER — floor4_only has exactly 500 rows
-- ============================================================================

SELECT 'file_filter_floor4' AS check_name,
       COUNT(*) AS actual,
       500 AS expected,
       CASE WHEN COUNT(*) = 500 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.avro.floor4_only;


-- ============================================================================
-- 6. FILE FILTER — floor4_only has all v2 columns populated
-- ============================================================================

SELECT 'floor4_v2_columns' AS check_name,
       COUNT(battery_pct) AS battery_non_null,
       COUNT(firmware_version) AS firmware_non_null,
       CASE WHEN COUNT(battery_pct) = 500 AND COUNT(firmware_version) = 500
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.avro.floor4_only;


-- ============================================================================
-- 7. MAX ROWS — readings_sample is limited (50 rows per file × 5 files)
-- ============================================================================

SELECT 'max_rows_sample' AS check_name,
       COUNT(*) AS actual,
       250 AS expected,
       CASE WHEN COUNT(*) = 250 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.avro.readings_sample;


-- ============================================================================
-- 8. FILE METADATA — All rows have non-NULL df_file_name
-- ============================================================================

SELECT 'file_metadata_populated' AS check_name,
       CASE WHEN COUNT(*) = (SELECT COUNT(*) FROM {{zone_name}}.avro.all_readings)
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.avro.all_readings
WHERE df_file_name IS NOT NULL;


-- ============================================================================
-- 9. ANALYTICS — Average temperature and humidity by floor/zone
-- ============================================================================

SELECT floor, zone,
       ROUND(AVG(temperature_c), 1) AS avg_temp_c,
       ROUND(AVG(humidity_pct), 1) AS avg_humidity,
       ROUND(AVG(co2_ppm), 0) AS avg_co2
FROM {{zone_name}}.avro.all_readings
GROUP BY floor, zone
ORDER BY floor, zone;


-- ============================================================================
-- 10. ANALYTICS — Occupancy rate by floor
-- ============================================================================

SELECT floor,
       COUNT(*) AS total_readings,
       SUM(CASE WHEN occupancy THEN 1 ELSE 0 END) AS occupied_readings,
       ROUND(SUM(CASE WHEN occupancy THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS occupancy_pct
FROM {{zone_name}}.avro.all_readings
GROUP BY floor
ORDER BY floor;


-- ============================================================================
-- 11. SUMMARY — All checks in one query
-- ============================================================================

SELECT check_name, result FROM (

    -- Check 1: Total row count = 2,500
    SELECT 'total_count_2500' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.avro.all_readings) = 2500
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: 5 distinct source files
    SELECT 'five_source_files' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.avro.all_readings) = 5
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: Schema evolution — battery_pct NULL for floors 1–3 (1,500 rows)
    SELECT 'schema_v1_null_battery' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.avro.all_readings
               WHERE floor IN (1, 2, 3) AND battery_pct IS NULL
           ) = 1500 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: Schema evolution — battery_pct non-NULL for floors 4–5 (1,000 rows)
    SELECT 'schema_v2_has_battery' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.avro.all_readings
               WHERE floor IN (4, 5) AND battery_pct IS NOT NULL
           ) = 1000 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: File filter — floor4_only has 500 rows
    SELECT 'filter_floor4_count' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.avro.floor4_only) = 500
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: Max rows — sample has 250 rows (50 × 5)
    SELECT 'max_rows_250' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.avro.readings_sample) = 250
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 7: File metadata populated for all rows
    SELECT 'file_metadata_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.avro.all_readings WHERE df_file_name IS NOT NULL) = 2500
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 8: Self-describing schema — sensor_id column exists
    SELECT 'schema_sensor_id_exists' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM information_schema.columns
               WHERE table_schema = 'avro' AND table_name = 'all_readings'
               AND column_name = 'sensor_id'
           ) = 1 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 9: Column count — 10 data columns + 2 metadata = 12
    SELECT 'column_count_12' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM information_schema.columns
               WHERE table_schema = 'avro' AND table_name = 'all_readings'
           ) = 12 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 10: Sample covers all 5 files
    SELECT 'sample_all_files' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.avro.readings_sample) = 5
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
