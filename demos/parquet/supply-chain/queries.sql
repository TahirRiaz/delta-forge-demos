-- ============================================================================
-- Parquet Supply Chain — Verification Queries
-- ============================================================================
-- Each query verifies a specific Parquet feature: recursive scanning,
-- file_filter, max_rows, row_group_filter, file_metadata, and self-describing
-- schema with automatic type detection.
-- ============================================================================


-- ============================================================================
-- 1. TOTAL ROW COUNT — 73,089 orders across 14 quarterly files
-- ============================================================================

SELECT 'total_row_count' AS check_name,
       COUNT(*) AS actual,
       73089 AS expected,
       CASE WHEN COUNT(*) = 73089 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.parquet.all_orders;


-- ============================================================================
-- 2. BROWSE ORDERS — See column types (self-describing Parquet schema)
-- ============================================================================

SELECT OrderID, CustomerID, SalespersonPersonID, OrderDate,
       ExpectedDeliveryDate, IsUndersupplyBackordered
FROM {{zone_name}}.parquet.all_orders
LIMIT 20;


-- ============================================================================
-- 3. ROWS PER FILE — Breakdown by source file (14 files across 5 directories)
-- ============================================================================

SELECT df_file_name, COUNT(*) AS row_count
FROM {{zone_name}}.parquet.all_orders
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 4. RECURSIVE SCANNING — 14 distinct files from year-based subdirectories
-- ============================================================================

SELECT 'recursive_file_count' AS check_name,
       COUNT(DISTINCT df_file_name) AS actual,
       14 AS expected,
       CASE WHEN COUNT(DISTINCT df_file_name) = 14 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.parquet.all_orders;


-- ============================================================================
-- 5. FILE FILTER — orders_2015 has exactly 23,636 rows (4 quarters)
-- ============================================================================

SELECT 'file_filter_2015' AS check_name,
       COUNT(*) AS actual,
       23636 AS expected,
       CASE WHEN COUNT(*) = 23636 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.parquet.orders_2015;


-- ============================================================================
-- 6. FILE FILTER — orders_2015 has exactly 4 source files
-- ============================================================================

SELECT df_file_name, COUNT(*) AS row_count
FROM {{zone_name}}.parquet.orders_2015
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 7. MAX ROWS — orders_sample is limited (100 rows per file × 14 files)
-- ============================================================================

SELECT 'max_rows_sample' AS check_name,
       COUNT(*) AS actual,
       1400 AS expected,
       CASE WHEN COUNT(*) = 1400 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.parquet.orders_sample;


-- ============================================================================
-- 8. SINGLE QUARTER — orders_q1_2014 has 5,210 rows
-- ============================================================================

SELECT 'single_quarter_q1_2014' AS check_name,
       COUNT(*) AS actual,
       5210 AS expected,
       CASE WHEN COUNT(*) = 5210 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.parquet.orders_q1_2014;


-- ============================================================================
-- 9. FILE METADATA — All rows have non-NULL df_file_name
-- ============================================================================

SELECT 'file_metadata_populated' AS check_name,
       CASE WHEN COUNT(*) = (SELECT COUNT(*) FROM {{zone_name}}.parquet.all_orders)
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.parquet.all_orders
WHERE df_file_name IS NOT NULL;


-- ============================================================================
-- 10. ORDERS PER SALESPERSON — Analytics query
-- ============================================================================

SELECT SalespersonPersonID,
       COUNT(*) AS total_orders,
       COUNT(DISTINCT CustomerID) AS unique_customers
FROM {{zone_name}}.parquet.all_orders
GROUP BY SalespersonPersonID
ORDER BY total_orders DESC;


-- ============================================================================
-- 11. BACKORDER ANALYSIS — IsUndersupplyBackordered breakdown
-- ============================================================================

SELECT IsUndersupplyBackordered,
       COUNT(*) AS order_count,
       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {{zone_name}}.parquet.all_orders), 1) AS pct
FROM {{zone_name}}.parquet.all_orders
GROUP BY IsUndersupplyBackordered;


-- ============================================================================
-- 12. SUMMARY — All checks in one query
-- ============================================================================

SELECT check_name, result FROM (

    -- Check 1: Total row count = 73,089
    SELECT 'total_count_73089' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.parquet.all_orders) = 73089
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: 14 distinct source files (recursive scanning)
    SELECT 'recursive_14_files' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.parquet.all_orders) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: File filter — 2015 has 23,636 rows
    SELECT 'filter_2015_count' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.parquet.orders_2015) = 23636
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: File filter — 2015 has 4 files
    SELECT 'filter_2015_files' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.parquet.orders_2015) = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: Max rows — sample has 1,400 rows (100 × 14)
    SELECT 'max_rows_1400' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.parquet.orders_sample) = 1400
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: Single quarter — Q1 2014 has 5,210 rows
    SELECT 'quarter_q1_2014' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.parquet.orders_q1_2014) = 5210
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 7: File metadata populated for all rows
    SELECT 'file_metadata_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.parquet.all_orders WHERE df_file_name IS NOT NULL) = 73089
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 8: Self-describing schema — OrderID column exists
    SELECT 'schema_orderid_exists' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM information_schema.columns
               WHERE table_schema = 'parquet' AND table_name = 'all_orders'
               AND column_name = 'OrderID'
           ) = 1 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 9: Column count — 18 data columns + 2 metadata = 20
    SELECT 'column_count_20' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM information_schema.columns
               WHERE table_schema = 'parquet' AND table_name = 'all_orders'
           ) = 20 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 10: Sampled data covers all 14 files
    SELECT 'sample_all_files' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.parquet.orders_sample) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
