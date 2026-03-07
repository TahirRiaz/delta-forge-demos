-- ============================================================================
-- XML E-Commerce Order Line Explosion — Verification Queries
-- ============================================================================
-- Each query verifies a specific XML feature: deep nesting, explode_paths,
-- CDATA, exclude_paths, column_mappings, default_repeat_handling.
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

SELECT orders_order_attr_id, orders_order_attr_status, orders_order_customer_name, orders_order_items_item_attr_sku, orders_order_items_item_product,
       orders_order_items_item_quantity, orders_order_items_item_unit_price, orders_order_items_item_variant_size, orders_order_items_item_variant_color
FROM {{zone_name}}.xml.order_lines
ORDER BY orders_order_attr_id, orders_order_items_item_attr_sku;


-- ============================================================================
-- 3. DEEP NESTING — variant/size and variant/color flattened to columns
-- ============================================================================
-- 3 levels deep: order → items/item → variant/color
-- All 11 items have variant info, so no NULLs expected.

SELECT 'deep_nesting_color' AS check_name,
       COUNT(*) FILTER (WHERE orders_order_items_item_variant_color IS NOT NULL) AS actual,
       11 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE orders_order_items_item_variant_color IS NOT NULL) = 11
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.order_lines;


-- ============================================================================
-- 4. COLUMN MAPPINGS — verify friendly names exist
-- ============================================================================
-- Deep paths mapped: customer/name → customer_name,
-- items/item/variant/color → item_color, etc.

SELECT 'column_mapping' AS check_name,
       COUNT(*) FILTER (WHERE orders_order_customer_name IS NOT NULL AND orders_order_items_item_variant_color IS NOT NULL) AS actual,
       11 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE orders_order_customer_name IS NOT NULL AND orders_order_items_item_variant_color IS NOT NULL) = 11
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.order_lines;


-- ============================================================================
-- 5. CDATA EXTRACTION — description should contain raw HTML tags
-- ============================================================================
-- Some items have CDATA-wrapped HTML: <![CDATA[<b>Premium</b> ...]]>
-- The CDATA wrapper should be removed but HTML preserved as text.

SELECT 'cdata_extraction' AS check_name,
       COUNT(*) FILTER (WHERE orders_order_items_item_description LIKE '%<b>%' OR orders_order_items_item_description LIKE '%<em>%') AS actual,
       CASE WHEN COUNT(*) FILTER (WHERE orders_order_items_item_description LIKE '%<b>%' OR orders_order_items_item_description LIKE '%<em>%') > 0
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.order_lines;


-- ============================================================================
-- 6. CDATA SPOT CHECK — verify specific CDATA content
-- ============================================================================

SELECT orders_order_items_item_attr_sku, orders_order_items_item_description
FROM {{zone_name}}.xml.order_lines
WHERE orders_order_items_item_attr_sku IN ('WDG-100', 'GDG-200')
GROUP BY orders_order_items_item_attr_sku, orders_order_items_item_description
ORDER BY orders_order_items_item_attr_sku;


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
-- 8. ORDER SUMMARY — 5 rows, one per order
-- ============================================================================

SELECT 'summary_rows' AS check_name,
       COUNT(*) AS actual,
       5 AS expected,
       CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.order_summary;


-- ============================================================================
-- 9. BROWSE ORDER SUMMARY — see per-order view with flattened customer
-- ============================================================================

SELECT orders_order_attr_id, orders_order_attr_status, orders_order_customer_name, orders_order_customer_email, orders_order_customer_tier,
       orders_order_order_date, orders_order_items_item, orders_order_shipping_total
FROM {{zone_name}}.xml.order_summary
ORDER BY orders_order_attr_id;


-- ============================================================================
-- 10. LINE ITEM ANALYTICS — total quantity ordered by product
-- ============================================================================

SELECT orders_order_items_item_product,
       SUM(CAST(orders_order_items_item_quantity AS INT)) AS total_qty,
       COUNT(*) AS order_count
FROM {{zone_name}}.xml.order_lines
GROUP BY orders_order_items_item_product
ORDER BY total_qty DESC;


-- ============================================================================
-- 11. REVENUE BY ORDER — join quantity and price
-- ============================================================================

SELECT orders_order_attr_id, orders_order_customer_name,
       SUM(CAST(orders_order_items_item_quantity AS INT) * CAST(orders_order_items_item_unit_price AS DOUBLE)) AS order_total,
       MIN(orders_order_shipping_total) AS shipping
FROM {{zone_name}}.xml.order_lines
GROUP BY orders_order_attr_id, orders_order_customer_name
ORDER BY order_total DESC;


-- ============================================================================
-- 12. SUMMARY — All checks
-- ============================================================================

SELECT 'exploded_rows' AS check_name,
       CASE WHEN COUNT(*) = 11 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.order_lines
UNION ALL
SELECT 'deep_nesting_color',
       CASE WHEN COUNT(*) FILTER (WHERE orders_order_items_item_variant_color IS NOT NULL) = 11
            THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml.order_lines
UNION ALL
SELECT 'column_mapping',
       CASE WHEN COUNT(*) FILTER (WHERE orders_order_customer_name IS NOT NULL AND orders_order_items_item_variant_color IS NOT NULL) = 11
            THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml.order_lines
UNION ALL
SELECT 'cdata_extraction',
       CASE WHEN COUNT(*) FILTER (WHERE orders_order_items_item_description LIKE '%<b>%' OR orders_order_items_item_description LIKE '%<em>%') > 0
            THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml.order_lines
UNION ALL
SELECT 'summary_rows',
       CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml.order_summary
ORDER BY check_name;
