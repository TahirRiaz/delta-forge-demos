-- ============================================================================
-- Delta Statistics — Min/Max & Data Skipping — Educational Queries
-- ============================================================================
-- WHAT: Delta stores per-file column statistics (min, max, null count, row
--       count) in the transaction log for every data file.
-- WHY:  When a query has a WHERE clause, the engine checks file-level stats
--       BEFORE reading any Parquet data. If a file's min/max range doesn't
--       overlap with the filter, the entire file is skipped. This can turn
--       a full-table scan into reading just a fraction of the files.
-- HOW:  Statistics are written into each Add action in the _delta_log JSON.
--       For strings, only the first 32 characters are tracked (truncation).
--       NULL counts let the engine skip files with no NULLs when filtering
--       for IS NULL, or skip files that are all NULL.
-- ============================================================================


-- ============================================================================
-- EXPLORE: The Three Batches with Non-Overlapping Value Ranges
-- ============================================================================
-- The setup inserted sensor readings in 3 separate batches, each with a
-- distinct value range. Since each batch creates its own Parquet file(s),
-- the per-file statistics will have non-overlapping min/max ranges:
--
--   Batch 1 (ids 1-20):  values [10.1 - 100.0]
--   Batch 2 (ids 21-40): values [200.0 - 500.0]
--   Batch 3 (ids 41-60): values [1000.0 - 5000.0]

-- Verify non-overlapping value ranges per batch
ASSERT VALUE min_value = 10.1 WHERE batch = 'Batch 1 (ids 1-20)'
ASSERT VALUE max_value = 100.0 WHERE batch = 'Batch 1 (ids 1-20)'
ASSERT VALUE min_value = 200.0 WHERE batch = 'Batch 2 (ids 21-40)'
ASSERT VALUE max_value = 500.0 WHERE batch = 'Batch 2 (ids 21-40)'
ASSERT VALUE min_value = 1000.0 WHERE batch = 'Batch 3 (ids 41-60)'
ASSERT VALUE max_value = 5000.0 WHERE batch = 'Batch 3 (ids 41-60)'
ASSERT VALUE row_count = 20 WHERE batch = 'Batch 1 (ids 1-20)'
ASSERT VALUE row_count = 20 WHERE batch = 'Batch 2 (ids 21-40)'
ASSERT VALUE row_count = 20 WHERE batch = 'Batch 3 (ids 41-60)'
ASSERT ROW_COUNT = 3
SELECT
    CASE
        WHEN id BETWEEN 1 AND 20 THEN 'Batch 1 (ids 1-20)'
        WHEN id BETWEEN 21 AND 40 THEN 'Batch 2 (ids 21-40)'
        ELSE 'Batch 3 (ids 41-60)'
    END AS batch,
    MIN(value) AS min_value,
    MAX(value) AS max_value,
    COUNT(*) AS row_count,
    COUNT(DISTINCT category) AS categories
FROM {{zone_name}}.delta_demos.sensor_readings
GROUP BY CASE
    WHEN id BETWEEN 1 AND 20 THEN 'Batch 1 (ids 1-20)'
    WHEN id BETWEEN 21 AND 40 THEN 'Batch 2 (ids 21-40)'
    ELSE 'Batch 3 (ids 41-60)'
END
ORDER BY batch;


-- ============================================================================
-- LEARN: Data Skipping in Action
-- ============================================================================
-- When you query WHERE value >= 1000.0, the engine checks per-file stats:
--   - Batch 1 file: max = 100.0   -> SKIP (100 < 1000)
--   - Batch 2 file: max = 500.0   -> SKIP (500 < 1000)
--   - Batch 3 file: min = 1000.0  -> READ (overlaps the filter range)
--
-- Result: only 1 out of 3 files is read. On a table with thousands of files,
-- this optimization can skip 90%+ of the data.

-- Verify 20 readings with value >= 1000.0 (entire Batch 3)
ASSERT VALUE high_value_readings = 20
ASSERT VALUE min_high = 1000.0
ASSERT VALUE max_high = 5000.0
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS high_value_readings,
       MIN(value) AS min_high,
       MAX(value) AS max_high
FROM {{zone_name}}.delta_demos.sensor_readings
WHERE value >= 1000.0;


-- ============================================================================
-- LEARN: No Value Overlap Between Batches
-- ============================================================================
-- The non-overlapping ranges are key to effective data skipping. If values
-- from different batches overlapped, the engine would need to read multiple
-- files even for narrow range filters. This is why data layout matters:
-- sorting or partitioning data by frequently-filtered columns maximizes
-- the effectiveness of min/max statistics.

-- Verify zero overlap: no rows with id > 20 have value < 200
ASSERT VALUE cross_batch_overlap = 0
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS cross_batch_overlap
FROM {{zone_name}}.delta_demos.sensor_readings
WHERE id > 20 AND value < 200.0;


-- ============================================================================
-- LEARN: NULL Count Statistics
-- ============================================================================
-- Delta also tracks null counts per column per file. This enables:
--   - Skipping files with zero NULLs when filtering IS NULL
--   - Skipping all-NULL files when filtering IS NOT NULL
--
-- 15 rows had quality_score set to NULL (5 from each batch), creating a
-- mixed NULL pattern across all files. The engine uses null counts to
-- estimate selectivity and plan optimal file reads.

-- Verify 5 NULLs per batch and 20 rows per batch
ASSERT VALUE null_quality = 5 WHERE batch = 'Batch 1'
ASSERT VALUE null_quality = 5 WHERE batch = 'Batch 2'
ASSERT VALUE null_quality = 5 WHERE batch = 'Batch 3'
ASSERT VALUE total_rows = 20 WHERE batch = 'Batch 1'
ASSERT VALUE total_rows = 20 WHERE batch = 'Batch 2'
ASSERT VALUE total_rows = 20 WHERE batch = 'Batch 3'
ASSERT ROW_COUNT = 3
SELECT
    CASE
        WHEN id BETWEEN 1 AND 20 THEN 'Batch 1'
        WHEN id BETWEEN 21 AND 40 THEN 'Batch 2'
        ELSE 'Batch 3'
    END AS batch,
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE quality_score IS NULL) AS null_quality,
    COUNT(*) FILTER (WHERE quality_score IS NOT NULL) AS non_null_quality
FROM {{zone_name}}.delta_demos.sensor_readings
GROUP BY CASE
    WHEN id BETWEEN 1 AND 20 THEN 'Batch 1'
    WHEN id BETWEEN 21 AND 40 THEN 'Batch 2'
    ELSE 'Batch 3'
END
ORDER BY batch;


-- ============================================================================
-- LEARN: String Statistics and Truncation
-- ============================================================================
-- Delta stores min/max statistics for string columns too, but only the first
-- 32 characters. Batch 3 has long descriptions (70+ chars) whose statistics
-- are truncated. This means string-based data skipping works well for short,
-- distinct prefixes but becomes less effective for long strings with common
-- prefixes.

ASSERT VALUE min_desc_length = 11 WHERE batch = 'Batch 1 (short)'
ASSERT VALUE max_desc_length = 22 WHERE batch = 'Batch 1 (short)'
ASSERT VALUE min_desc_length = 32 WHERE batch = 'Batch 2 (medium)'
ASSERT VALUE max_desc_length = 47 WHERE batch = 'Batch 2 (medium)'
ASSERT VALUE min_desc_length = 61 WHERE batch = 'Batch 3 (long, >32 chars)'
ASSERT VALUE max_desc_length = 80 WHERE batch = 'Batch 3 (long, >32 chars)'
ASSERT ROW_COUNT = 3
SELECT
    CASE
        WHEN id BETWEEN 1 AND 20 THEN 'Batch 1 (short)'
        WHEN id BETWEEN 21 AND 40 THEN 'Batch 2 (medium)'
        ELSE 'Batch 3 (long, >32 chars)'
    END AS batch,
    MIN(LENGTH(description)) AS min_desc_length,
    MAX(LENGTH(description)) AS max_desc_length
FROM {{zone_name}}.delta_demos.sensor_readings
GROUP BY CASE
    WHEN id BETWEEN 1 AND 20 THEN 'Batch 1 (short)'
    WHEN id BETWEEN 21 AND 40 THEN 'Batch 2 (medium)'
    ELSE 'Batch 3 (long, >32 chars)'
END
ORDER BY batch;


-- ============================================================================
-- EXPLORE: Range-Filtered Aggregation
-- ============================================================================
-- This query benefits from data skipping: only Batch 1 files need to be read.
-- The engine can skip Batch 2 and 3 entirely based on min/max statistics.

-- Non-deterministic: ROUND(AVG(double)) may vary by platform; expected ≈ 24.05
ASSERT WARNING VALUE avg_value BETWEEN 24.0 AND 24.1 WHERE device = 'DEVICE-A'
ASSERT VALUE readings = 4 WHERE device = 'DEVICE-A'
ASSERT VALUE readings = 4 WHERE device = 'DEVICE-E'
ASSERT ROW_COUNT = 5
SELECT device, category,
       ROUND(AVG(value), 2) AS avg_value,
       COUNT(*) AS readings
FROM {{zone_name}}.delta_demos.sensor_readings
WHERE value <= 100.0
GROUP BY device, category
ORDER BY device;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count is 60
ASSERT ROW_COUNT = 60
SELECT * FROM {{zone_name}}.delta_demos.sensor_readings;

-- Verify batch 1 min value is 10.1
ASSERT VALUE batch1_min = 10.1
SELECT MIN(value) AS batch1_min FROM {{zone_name}}.delta_demos.sensor_readings WHERE id BETWEEN 1 AND 20;

-- Verify batch 2 max value is 500.0
ASSERT VALUE batch2_max = 500.0
SELECT MAX(value) AS batch2_max FROM {{zone_name}}.delta_demos.sensor_readings WHERE id BETWEEN 21 AND 40;

-- Verify batch 3 max value is 5000.0
ASSERT VALUE batch3_max = 5000.0
SELECT MAX(value) AS batch3_max FROM {{zone_name}}.delta_demos.sensor_readings WHERE id BETWEEN 41 AND 60;

-- Verify 15 rows have NULL quality_score
ASSERT VALUE null_quality_count = 15
SELECT COUNT(*) AS null_quality_count FROM {{zone_name}}.delta_demos.sensor_readings WHERE quality_score IS NULL;

-- Verify 20 high-value readings (>= 1000.0)
ASSERT VALUE high_value_count = 20
SELECT COUNT(*) AS high_value_count FROM {{zone_name}}.delta_demos.sensor_readings WHERE value >= 1000.0;

-- Verify no range overlap between batches (no rows with id > 20 and value < 200)
ASSERT VALUE overlap_count = 0
SELECT COUNT(*) AS overlap_count FROM {{zone_name}}.delta_demos.sensor_readings WHERE id > 20 AND value < 200.0;

-- Verify all batch 3 descriptions are longer than 32 characters
ASSERT VALUE long_desc_count = 20
SELECT COUNT(*) AS long_desc_count FROM {{zone_name}}.delta_demos.sensor_readings WHERE id BETWEEN 41 AND 60 AND LENGTH(description) > 32;
