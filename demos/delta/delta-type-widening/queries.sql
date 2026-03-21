-- ============================================================================
-- Delta Type Widening -- Educational Queries
-- ============================================================================
-- WHAT: Type widening allows promoting a column's type to a wider one (INT to
--       BIGINT, FLOAT to DOUBLE) without rewriting existing Parquet data files.
-- WHY:  As data grows, values may exceed the original type's range. Type widening
--       avoids costly full-table rewrites when schema needs to evolve.
-- HOW:  Delta records the widened type in the schema metadata of the transaction
--       log. Readers automatically upcast values from older Parquet files that
--       still use the narrower type -- no physical rewrite needed.
-- ============================================================================


-- ============================================================================
-- EXPLORE: What numeric types are in use?
-- ============================================================================
-- This table has three numeric columns with different ranges:
--   small_reading (INT)    -- fits values up to ~2.1 billion
--   large_reading (BIGINT) -- fits values up to ~9.2 quintillion
--   precise_value (DOUBLE) -- 64-bit floating point for decimal precision
-- Notice how the same "measurement" concept needs different types at scale.

ASSERT ROW_COUNT = 10
SELECT id, sensor_id, category,
       small_reading, large_reading, precise_value,
       unit
FROM {{zone_name}}.delta_demos.measurements
ORDER BY id
LIMIT 10;


-- ============================================================================
-- EXPLORE: The INT vs BIGINT boundary
-- ============================================================================
-- INT tops out at 2,147,483,647. Look at the large_reading column for
-- categories like 'bytes' and 'ticks' -- these values far exceed INT range.
-- Without BIGINT (or type widening from INT to BIGINT), these values would
-- overflow or require a full table rewrite to change the column type.

-- Verify all 9 rows from bytes/ticks/nanoseconds categories are returned
ASSERT ROW_COUNT = 9
SELECT id, sensor_id, category,
       small_reading, large_reading,
       CASE WHEN large_reading > 2147483647 THEN 'exceeds INT range'
            ELSE 'fits in INT' END AS int_compatibility
FROM {{zone_name}}.delta_demos.measurements
WHERE category IN ('bytes', 'ticks', 'nanoseconds')
ORDER BY large_reading DESC;


-- ============================================================================
-- LEARN: Precision preservation after arithmetic operations
-- ============================================================================
-- The pressure readings were scaled by 1000x (hPa to Pa conversion) via UPDATE.
-- Delta wrote new Parquet files with the updated values. Because precise_value
-- is DOUBLE, the multiplication preserves decimal places (with ROUND).
-- This is a common real-world pattern: unit conversions that change magnitude
-- but must preserve precision.

ASSERT ROW_COUNT = 5
SELECT id, sensor_id,
       small_reading AS scaled_int_reading,
       precise_value AS scaled_precise_value,
       unit
FROM {{zone_name}}.delta_demos.measurements
WHERE category = 'pressure'
ORDER BY id;


-- ============================================================================
-- LEARN: Comparing value ranges across categories
-- ============================================================================
-- Type widening matters most when a single table holds data with vastly
-- different magnitudes. Here we see categories ranging from single-digit
-- temperatures to trillion-scale byte counts -- all in the same table.
-- The Delta schema tracks that small_reading is INT and large_reading is
-- BIGINT, allowing both to coexist in the same Parquet column metadata.

ASSERT ROW_COUNT = 10
ASSERT VALUE max_large = 9223372036854 WHERE category = 'ticks'
ASSERT VALUE min_small = -5 WHERE category = 'temperature'
SELECT category,
       MIN(small_reading) AS min_small,
       MAX(small_reading) AS max_small,
       MIN(large_reading) AS min_large,
       MAX(large_reading) AS max_large,
       COUNT(*) AS readings
FROM {{zone_name}}.delta_demos.measurements
GROUP BY category
ORDER BY max_large DESC;


-- ============================================================================
-- LEARN: DOUBLE precision and rounding behavior
-- ============================================================================
-- DOUBLE (64-bit IEEE 754) provides ~15-17 significant digits. For most sensor
-- and financial calculations this is sufficient, but ROUND() is essential to
-- control output precision. Delta stores the exact DOUBLE bits in Parquet;
-- rounding is a query-time operation, not a storage-time one.

ASSERT ROW_COUNT = 10
-- Non-deterministic: floating-point AVG over DOUBLE column may vary slightly across platforms
ASSERT WARNING VALUE avg_amount BETWEEN 45.74 AND 45.76 WHERE category = 'pressure'
-- Non-deterministic: floating-point AVG over DOUBLE column may vary slightly across platforms
ASSERT WARNING VALUE avg_amount BETWEEN 20.43 AND 20.47 WHERE category = 'temperature'
SELECT category,
       ROUND(AVG(precise_value), 3) AS avg_precise,
       ROUND(AVG(amount), 2) AS avg_amount,
       COUNT(*) AS readings
FROM {{zone_name}}.delta_demos.measurements
GROUP BY category
ORDER BY avg_precise DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count is 40
ASSERT ROW_COUNT = 40
SELECT * FROM {{zone_name}}.delta_demos.measurements;

-- Verify 10 distinct categories
ASSERT VALUE category_count = 10
SELECT COUNT(DISTINCT category) AS category_count FROM {{zone_name}}.delta_demos.measurements;

-- Verify pressure readings have updated unit 'pa'
ASSERT VALUE pressure_pa_count = 5
SELECT COUNT(*) AS pressure_pa_count FROM {{zone_name}}.delta_demos.measurements WHERE category = 'pressure' AND unit = 'pa';

-- Verify pressure scaled reading (id=6, 1013 * 1000 = 1013000)
ASSERT VALUE small_reading = 1013000
SELECT small_reading FROM {{zone_name}}.delta_demos.measurements WHERE id = 6;

-- Verify pressure precise value (id=6)
ASSERT VALUE precise_value = 1013250.0
SELECT precise_value FROM {{zone_name}}.delta_demos.measurements WHERE id = 6;

-- Verify BIGINT large value (id=31)
ASSERT VALUE large_reading = 2199023255552
SELECT large_reading FROM {{zone_name}}.delta_demos.measurements WHERE id = 31;

-- Verify max large_reading across all rows
ASSERT VALUE max_large_reading = 9223372036854
SELECT MAX(large_reading) AS max_large_reading FROM {{zone_name}}.delta_demos.measurements;

-- Verify temperature reading unchanged (id=1)
ASSERT VALUE small_reading = 22
SELECT small_reading FROM {{zone_name}}.delta_demos.measurements WHERE id = 1;
