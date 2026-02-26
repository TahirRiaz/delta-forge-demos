-- ============================================================================
-- XML E-Commerce Order Line Explosion — Verification Queries
-- ============================================================================
-- Each query verifies a specific XML feature: deep nesting, explode_paths,
-- CDATA, exclude_paths, column_mappings, preserve_original, xml_paths.
-- ============================================================================


-- ============================================================================
-- 1. EXPLODED ROW COUNT — 7 items (file 1) + 4 items (file 2) = 11 rows
-- ============================================================================

SELECT 'exploded_rows' AS check_name,
       COUNT(*) AS actual,
       11 AS expected,
       CASE WHEN COUNT(*) = 11 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.order_lines;


-- ============================================================================
-- 2. BROWSE ORDER LINES — See the exploded data with friendly column names
-- ============================================================================

SELECT order_id, order_status, customer_name, sku, product,
       quantity, unit_price, item_size, item_color
FROM {{zone_name}}.xml.order_lines
ORDER BY order_id, sku;


-- ============================================================================
-- 3. DEEP NESTING — variant/size and variant/color flattened to columns
-- ============================================================================
-- 3 levels deep: order → items/item → variant/color
-- All 11 items have variant info, so no NULLs expected.

SELECT 'deep_nesting_color' AS check_name,
       COUNT(*) FILTER (WHERE item_color IS NOT NULL) AS actual,
       11 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE item_color IS NOT NULL) = 11
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.order_lines;


-- ============================================================================
-- 4. COLUMN MAPPINGS — verify friendly names exist
-- ============================================================================
-- Deep paths mapped: customer/name → customer_name,
-- items/item/variant/color → item_color, etc.

SELECT 'column_mapping' AS check_name,
       COUNT(*) FILTER (WHERE customer_name IS NOT NULL AND item_color IS NOT NULL) AS actual,
       11 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE customer_name IS NOT NULL AND item_color IS NOT NULL) = 11
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.order_lines;


-- ============================================================================
-- 5. CDATA EXTRACTION — description should contain raw HTML tags
-- ============================================================================
-- Some items have CDATA-wrapped HTML: <![CDATA[<b>Premium</b> ...]]>
-- The CDATA wrapper should be removed but HTML preserved as text.

SELECT 'cdata_extraction' AS check_name,
       COUNT(*) FILTER (WHERE description LIKE '%<b>%' OR description LIKE '%<em>%') AS actual,
       CASE WHEN COUNT(*) FILTER (WHERE description LIKE '%<b>%' OR description LIKE '%<em>%') > 0
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.order_lines;


-- ============================================================================
-- 6. CDATA SPOT CHECK — verify specific CDATA content
-- ============================================================================

SELECT sku, description
FROM {{zone_name}}.xml.order_lines
WHERE sku IN ('WDG-100', 'GDG-200')
GROUP BY sku, description
ORDER BY sku;


-- ============================================================================
-- 7. EXCLUDE PATHS — internal_audit columns should NOT exist
-- ============================================================================
-- The /orders/order/internal_audit block (cost_center, margin_pct) is
-- excluded via exclude_paths. No columns with those names should appear.

SELECT 'exclude_audit' AS check_name,
       COUNT(*) AS audit_columns,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM information_schema.columns
WHERE table_name = 'order_lines'
  AND (column_name LIKE '%cost_center%' OR column_name LIKE '%margin_pct%');


-- ============================================================================
-- 8. PRESERVE ORIGINAL — _xml_source column exists and is non-NULL
-- ============================================================================

SELECT 'preserve_original' AS check_name,
       COUNT(*) FILTER (WHERE _xml_source IS NOT NULL) AS actual,
       11 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE _xml_source IS NOT NULL) = 11
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.order_lines;


-- ============================================================================
-- 9. ORDER SUMMARY — 5 rows, one per order
-- ============================================================================

SELECT 'summary_rows' AS check_name,
       COUNT(*) AS actual,
       5 AS expected,
       CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.order_summary;


-- ============================================================================
-- 10. BROWSE ORDER SUMMARY — see per-order view
-- ============================================================================

SELECT order_id, order_status, customer, order_date, item, shipping_total
FROM {{zone_name}}.xml.order_summary
ORDER BY order_id;


-- ============================================================================
-- 11. XML_PATHS — customer column contains JSON string (not flattened)
-- ============================================================================
-- The customer subtree is preserved as a JSON blob via xml_paths +
-- nested_output_format: json.

SELECT 'customer_as_json' AS check_name,
       COUNT(*) FILTER (WHERE customer LIKE '{%' OR customer LIKE '[%') AS actual,
       CASE WHEN COUNT(*) FILTER (WHERE customer LIKE '{%' OR customer LIKE '[%') > 0
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.order_summary;


-- ============================================================================
-- 12. LINE ITEM ANALYTICS — total quantity ordered by product
-- ============================================================================

SELECT product,
       SUM(CAST(quantity AS INT)) AS total_qty,
       COUNT(*) AS order_count
FROM {{zone_name}}.xml.order_lines
GROUP BY product
ORDER BY total_qty DESC;


-- ============================================================================
-- 13. REVENUE BY ORDER — join quantity and price
-- ============================================================================

SELECT order_id, customer_name,
       SUM(CAST(quantity AS INT) * CAST(unit_price AS DOUBLE)) AS order_total,
       MIN(shipping_total) AS shipping
FROM {{zone_name}}.xml.order_lines
GROUP BY order_id, customer_name
ORDER BY order_total DESC;


-- ============================================================================
-- 14. SUMMARY — All checks
-- ============================================================================

SELECT 'exploded_rows' AS check_name,
       CASE WHEN COUNT(*) = 11 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.order_lines
UNION ALL
SELECT 'deep_nesting_color',
       CASE WHEN COUNT(*) FILTER (WHERE item_color IS NOT NULL) = 11
            THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml.order_lines
UNION ALL
SELECT 'column_mapping',
       CASE WHEN COUNT(*) FILTER (WHERE customer_name IS NOT NULL AND item_color IS NOT NULL) = 11
            THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml.order_lines
UNION ALL
SELECT 'cdata_extraction',
       CASE WHEN COUNT(*) FILTER (WHERE description LIKE '%<b>%' OR description LIKE '%<em>%') > 0
            THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml.order_lines
UNION ALL
SELECT 'summary_rows',
       CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml.order_summary
ORDER BY check_name;
