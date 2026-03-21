-- ============================================================================
-- Excel Sales Analytics — Verification Queries
-- ============================================================================
-- Each query verifies a specific Excel feature: sheet selection, multi-file
-- reading, range extraction, header handling, type inference, and file metadata.
-- ============================================================================


-- ============================================================================
-- 1. TOTAL ROW COUNT — 9,994 orders across 4 files
-- ============================================================================

ASSERT ROW_COUNT = 9994
SELECT *
FROM {{zone_name}}.excel.all_orders;


-- ============================================================================
-- 2. BROWSE ORDERS — See column types (dates, numbers, strings)
-- ============================================================================

ASSERT ROW_COUNT = 20
SELECT order_id, order_date, ship_date, customer_name,
       category, sales, quantity, profit
FROM {{zone_name}}.excel.all_orders
LIMIT 20;


-- ============================================================================
-- 3. ROWS PER FILE — Breakdown by source file
-- ============================================================================
-- 2014: 1,993 | 2015: 2,102 | 2016: 2,587 | 2017: 3,312

ASSERT ROW_COUNT = 4
ASSERT VALUE row_count = 3312 WHERE df_file_name LIKE '%2017%'
SELECT df_file_name, COUNT(*) AS row_count
FROM {{zone_name}}.excel.all_orders
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 4. YEAR-OVER-YEAR — Sales trend by file (proxy for year)
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE orders = 3312 WHERE source_file LIKE '%2017%'
-- Non-deterministic: floating-point SUM may vary slightly across platforms
ASSERT WARNING VALUE total_sales BETWEEN 733000.00 AND 734000.00 WHERE source_file LIKE '%2017%'
-- Non-deterministic: floating-point SUM may vary slightly across platforms
ASSERT WARNING VALUE total_profit BETWEEN 93000.00 AND 94000.00 WHERE source_file LIKE '%2017%'
SELECT df_file_name AS source_file,
       COUNT(*) AS orders,
       ROUND(SUM(CAST(sales AS DOUBLE)), 2) AS total_sales,
       ROUND(SUM(CAST(profit AS DOUBLE)), 2) AS total_profit
FROM {{zone_name}}.excel.all_orders
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 5. SINGLE FILE — orders_2017 has exactly 3,312 rows
-- ============================================================================

ASSERT ROW_COUNT = 3312
SELECT *
FROM {{zone_name}}.excel.orders_2017;


-- ============================================================================
-- 6. RANGE TABLE — Limited columns and rows
-- ============================================================================
-- Range A1:K500 should produce 11 columns (A through K) and up to 499 data
-- rows per file. Columns beyond K (Region, Product ID, ..., Profit) are absent.

ASSERT VALUE column_count = 11
SELECT COUNT(*) AS column_count
FROM information_schema.columns
WHERE table_schema = 'excel'
  AND table_name = 'orders_range'
  AND column_name NOT LIKE 'df_%';


-- ============================================================================
-- 7. RANGE TABLE — Browse the subset of columns
-- ============================================================================

ASSERT ROW_COUNT = 10
SELECT *
FROM {{zone_name}}.excel.orders_range
LIMIT 10;


-- ============================================================================
-- 8. TRIMMED TABLE — Verify row count matches (same data, with trimming)
-- ============================================================================

ASSERT ROW_COUNT = 9994
SELECT *
FROM {{zone_name}}.excel.orders_trimmed;


-- ============================================================================
-- 9. NO-HEADER TABLE — Auto-generated column names (column_0, column_1, ...)
-- ============================================================================
-- With has_header=false and skip_rows=1, the header row is skipped and columns
-- get auto-generated names. max_rows=100 limits to 100 rows per file (4 files = 400).

ASSERT ROW_COUNT = 5
SELECT column_0, column_1, column_2, column_3, column_4
FROM {{zone_name}}.excel.orders_no_header
LIMIT 5;


-- ============================================================================
-- 9b. NO-HEADER TABLE — Row count = 400 (100 rows x 4 files)
-- ============================================================================

ASSERT ROW_COUNT = 400
SELECT *
FROM {{zone_name}}.excel.orders_no_header;


-- ============================================================================
-- 9c. DISTINCT REGIONS — 4 regions (Central, East, South, West)
-- ============================================================================

ASSERT VALUE region_count = 4
SELECT COUNT(DISTINCT region) AS region_count
FROM {{zone_name}}.excel.all_orders;


-- ============================================================================
-- 10. FILE METADATA — df_file_name populated for all rows
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE df_file_name LIKE '%sales-data-2014%' WHERE rows = 1993
SELECT df_file_name, COUNT(*) AS rows
FROM {{zone_name}}.excel.all_orders
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 11. SALES BY REGION — Analytics query
-- ============================================================================

ASSERT ROW_COUNT = 4
-- Non-deterministic: floating-point SUM may vary slightly across platforms
ASSERT WARNING VALUE total_sales BETWEEN 725000.00 AND 726000.00 WHERE region = 'West'
-- Non-deterministic: floating-point SUM may vary slightly across platforms
ASSERT WARNING VALUE total_profit BETWEEN 108000.00 AND 109000.00 WHERE region = 'West'
SELECT region,
       COUNT(*) AS orders,
       ROUND(SUM(CAST(sales AS DOUBLE)), 2) AS total_sales,
       ROUND(SUM(CAST(profit AS DOUBLE)), 2) AS total_profit,
       ROUND(AVG(CAST(discount AS DOUBLE)), 3) AS avg_discount
FROM {{zone_name}}.excel.all_orders
GROUP BY region
ORDER BY total_sales DESC;


-- ============================================================================
-- 12. TOP PRODUCTS — By profit margin
-- ============================================================================

ASSERT ROW_COUNT = 10
ASSERT VALUE orders = 68 WHERE sub_category = 'Copiers'
-- Non-deterministic: floating-point SUM may vary slightly across platforms
ASSERT WARNING VALUE total_profit BETWEEN 55000.00 AND 56500.00 WHERE sub_category = 'Copiers'
SELECT category, sub_category,
       COUNT(*) AS orders,
       ROUND(SUM(CAST(sales AS DOUBLE)), 2) AS total_sales,
       ROUND(SUM(CAST(profit AS DOUBLE)), 2) AS total_profit
FROM {{zone_name}}.excel.all_orders
GROUP BY category, sub_category
ORDER BY total_profit DESC
LIMIT 10;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- End-to-end sanity check: row counts, file options, metadata, and type inference.

ASSERT ROW_COUNT = 10
ASSERT VALUE result = 'PASS' WHERE check_name = 'total_count_9994'
ASSERT VALUE result = 'PASS' WHERE check_name = 'four_source_files'
ASSERT VALUE result = 'PASS' WHERE check_name = 'file_2017_count'
ASSERT VALUE result = 'PASS' WHERE check_name = 'trimmed_same_count'
ASSERT VALUE result = 'PASS' WHERE check_name = 'file_metadata_populated'
ASSERT VALUE result = 'PASS' WHERE check_name = 'no_header_max_rows'
ASSERT VALUE result = 'PASS' WHERE check_name = 'range_limited_columns'
ASSERT VALUE result = 'PASS' WHERE check_name = 'no_header_auto_columns'
ASSERT VALUE result = 'PASS' WHERE check_name = 'type_inference_numeric'
ASSERT VALUE result = 'PASS' WHERE check_name = 'four_regions'
SELECT check_name, result FROM (

    -- Check 1: Total row count = 9,994
    SELECT 'total_count_9994' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.excel.all_orders) = 9994
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: 4 distinct source files
    SELECT 'four_source_files' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.excel.all_orders) = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: 2017 file has 3,312 rows
    SELECT 'file_2017_count' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.excel.orders_2017) = 3312
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: Range table has limited columns (11 data columns + 2 metadata)
    SELECT 'range_limited_columns' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM information_schema.columns
               WHERE table_schema = 'excel' AND table_name = 'orders_range'
               AND column_name NOT LIKE 'df_%'
           ) = 11 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: Trimmed table has same count as all_orders
    SELECT 'trimmed_same_count' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.excel.orders_trimmed) = 9994
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: No-header table has auto-generated column names
    SELECT 'no_header_auto_columns' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM information_schema.columns
               WHERE table_schema = 'excel' AND table_name = 'orders_no_header'
               AND column_name LIKE 'column_%'
           ) > 0 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 7: File metadata populated (all rows have df_file_name)
    SELECT 'file_metadata_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.excel.all_orders WHERE df_file_name IS NOT NULL) = 9994
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 8: Type inference — sales column is numeric
    SELECT 'type_inference_numeric' AS check_name,
           CASE WHEN (SELECT data_type FROM information_schema.columns
                       WHERE table_schema = 'excel' AND table_name = 'all_orders'
                       AND column_name = 'sales') IN ('DOUBLE', 'FLOAT', 'DECIMAL', 'REAL', 'BIGINT', 'INTEGER')
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 9: 4 regions present (Central, East, South, West)
    SELECT 'four_regions' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT region) FROM {{zone_name}}.excel.all_orders) = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 10: No-header table limited by max_rows (100 rows x 4 files = 400)
    SELECT 'no_header_max_rows' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.excel.orders_no_header) = 400
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
