-- ============================================================================
-- Excel Sales Analytics — Verification Queries
-- ============================================================================
-- Each query verifies a specific Excel feature: sheet selection, multi-file
-- reading, range extraction, header handling, type inference, and file metadata.
-- ============================================================================


-- ============================================================================
-- 1. TOTAL ROW COUNT — 16,676 orders across 4 files
-- ============================================================================

SELECT 'total_row_count' AS check_name,
       COUNT(*) AS actual,
       16676 AS expected,
       CASE WHEN COUNT(*) = 16676 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.excel.all_orders;


-- ============================================================================
-- 2. BROWSE ORDERS — See column types (dates, numbers, strings)
-- ============================================================================

SELECT "Order ID", "Order Date", "Ship Date", "Customer Name",
       "Category", "Sales", "Quantity", "Profit"
FROM {{zone_name}}.excel.all_orders
LIMIT 20;


-- ============================================================================
-- 3. ROWS PER FILE — Breakdown by source file
-- ============================================================================
-- 2014: 1,993 | 2015: 2,102 | 2016: 2,587 | 2017: 9,994

SELECT df_file_name, COUNT(*) AS row_count
FROM {{zone_name}}.excel.all_orders
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 4. YEAR-OVER-YEAR — Sales trend by file (proxy for year)
-- ============================================================================

SELECT df_file_name AS source_file,
       COUNT(*) AS orders,
       ROUND(SUM("Sales"), 2) AS total_sales,
       ROUND(SUM("Profit"), 2) AS total_profit
FROM {{zone_name}}.excel.all_orders
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 5. SINGLE FILE — orders_2017 has exactly 9,994 rows
-- ============================================================================

SELECT 'single_file_2017' AS check_name,
       COUNT(*) AS actual,
       9994 AS expected,
       CASE WHEN COUNT(*) = 9994 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.excel.orders_2017;


-- ============================================================================
-- 6. RANGE TABLE — Limited columns and rows
-- ============================================================================
-- Range A1:K500 should produce 11 columns (A through K) and up to 499 data
-- rows per file. Columns beyond K (Region, Product ID, ..., Profit) are absent.

SELECT 'range_column_count' AS check_name,
       COUNT(*) AS column_count
FROM information_schema.columns
WHERE table_schema = 'excel'
  AND table_name = 'orders_range'
  AND column_name NOT LIKE 'df_%';


-- ============================================================================
-- 7. RANGE TABLE — Browse the subset of columns
-- ============================================================================

SELECT *
FROM {{zone_name}}.excel.orders_range
LIMIT 10;


-- ============================================================================
-- 8. TRIMMED TABLE — Verify row count matches (same data, with trimming)
-- ============================================================================

SELECT 'trimmed_count' AS check_name,
       COUNT(*) AS actual,
       16676 AS expected,
       CASE WHEN COUNT(*) = 16676 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.excel.orders_trimmed;


-- ============================================================================
-- 9. NO-HEADER TABLE — Auto-generated column names (column_0, column_1, ...)
-- ============================================================================
-- With has_header=false and skip_rows=1, the header row is skipped and columns
-- get auto-generated names. max_rows=100 limits to 100 rows per file.

SELECT column_0, column_1, column_2, column_3, column_4
FROM {{zone_name}}.excel.orders_no_header
LIMIT 5;


-- ============================================================================
-- 10. FILE METADATA — df_file_name populated for all rows
-- ============================================================================

SELECT df_file_name, COUNT(*) AS rows
FROM {{zone_name}}.excel.all_orders
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 11. SALES BY REGION — Analytics query
-- ============================================================================

SELECT "Region",
       COUNT(*) AS orders,
       ROUND(SUM("Sales"), 2) AS total_sales,
       ROUND(SUM("Profit"), 2) AS total_profit,
       ROUND(AVG("Discount"), 3) AS avg_discount
FROM {{zone_name}}.excel.all_orders
GROUP BY "Region"
ORDER BY total_sales DESC;


-- ============================================================================
-- 12. TOP PRODUCTS — By profit margin
-- ============================================================================

SELECT "Category", "Sub-Category",
       COUNT(*) AS orders,
       ROUND(SUM("Sales"), 2) AS total_sales,
       ROUND(SUM("Profit"), 2) AS total_profit
FROM {{zone_name}}.excel.all_orders
GROUP BY "Category", "Sub-Category"
ORDER BY total_profit DESC
LIMIT 10;


-- ============================================================================
-- 13. SUMMARY — All checks in one query
-- ============================================================================

SELECT check_name, result FROM (

    -- Check 1: Total row count = 16,676
    SELECT 'total_count_16676' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.excel.all_orders) = 16676
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: 4 distinct source files
    SELECT 'four_source_files' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.excel.all_orders) = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: 2017 file has 9,994 rows
    SELECT 'file_2017_count' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.excel.orders_2017) = 9994
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
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.excel.orders_trimmed) = 16676
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
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.excel.all_orders WHERE df_file_name IS NOT NULL) = 16676
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 8: Type inference — Sales column is numeric
    SELECT 'type_inference_numeric' AS check_name,
           CASE WHEN (SELECT data_type FROM information_schema.columns
                       WHERE table_schema = 'excel' AND table_name = 'all_orders'
                       AND column_name = 'Sales') IN ('DOUBLE', 'FLOAT', 'DECIMAL', 'REAL', 'BIGINT', 'INTEGER')
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 9: 4 regions present (Central, East, South, West)
    SELECT 'four_regions' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT "Region") FROM {{zone_name}}.excel.all_orders) = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 10: No-header table limited by max_rows (100 rows × 4 files = 400)
    SELECT 'no_header_max_rows' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.excel.orders_no_header) = 400
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
