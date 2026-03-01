-- ============================================================================
-- XML Subtree Capture — Verification Queries
-- ============================================================================
-- Each query verifies a specific aspect of xml_paths and nested_output_format.
-- ============================================================================


-- ============================================================================
-- 1. JSON TABLE ROW COUNT — 3 products (file 1) + 2 products (file 2) = 5
-- ============================================================================

SELECT 'json_row_count' AS check_name,
       COUNT(*) AS actual,
       5 AS expected,
       CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.products_json;


-- ============================================================================
-- 2. XML TABLE ROW COUNT — same 5 products
-- ============================================================================

SELECT 'xml_row_count' AS check_name,
       COUNT(*) AS actual,
       5 AS expected,
       CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.products_xml;


-- ============================================================================
-- 3. BROWSE JSON TABLE — top-level fields + captured subtrees
-- ============================================================================

SELECT product_id, status, product_name, category, price, currency,
       specs_json, supplier_json, tags
FROM {{zone_name}}.xml.products_json
ORDER BY product_id;


-- ============================================================================
-- 4. BROWSE XML TABLE — same fields but subtrees as XML fragments
-- ============================================================================

SELECT product_id, status, product_name, category, price, currency,
       specs_xml, supplier_xml, tags
FROM {{zone_name}}.xml.products_xml
ORDER BY product_id;


-- ============================================================================
-- 5. JSON SUBTREE FORMAT — specs_json should start with { (JSON object)
-- ============================================================================
-- When nested_output_format is "json", captured subtrees are serialized
-- as JSON objects with underscore-joined keys.

SELECT 'specs_is_json' AS check_name,
       COUNT(*) FILTER (WHERE specs_json LIKE '{%') AS actual,
       5 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE specs_json LIKE '{%') = 5
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.products_json;


-- ============================================================================
-- 6. XML SUBTREE FORMAT — specs_xml should start with < (XML fragment)
-- ============================================================================
-- When nested_output_format is "xml", captured subtrees are serialized
-- as raw XML fragment strings preserving original element structure.

SELECT 'specs_is_xml' AS check_name,
       COUNT(*) FILTER (WHERE specs_xml LIKE '<%') AS actual,
       5 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE specs_xml LIKE '<%') = 5
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.products_xml;


-- ============================================================================
-- 7. JSON SUPPLIER CAPTURE — supplier_json contains company name
-- ============================================================================
-- The supplier subtree includes company, contact, and address. When
-- captured as JSON, the company name should appear in the string.

SELECT 'supplier_json_has_company' AS check_name,
       COUNT(*) FILTER (WHERE supplier_json LIKE '%TechParts%'
                           OR supplier_json LIKE '%NetCore%'
                           OR supplier_json LIKE '%EuroPower%') AS actual,
       5 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE supplier_json LIKE '%TechParts%'
                                    OR supplier_json LIKE '%NetCore%'
                                    OR supplier_json LIKE '%EuroPower%') = 5
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.products_json;


-- ============================================================================
-- 8. XML SUPPLIER CAPTURE — supplier_xml contains company element
-- ============================================================================

SELECT 'supplier_xml_has_company' AS check_name,
       COUNT(*) FILTER (WHERE supplier_xml LIKE '%TechParts%'
                           OR supplier_xml LIKE '%NetCore%'
                           OR supplier_xml LIKE '%EuroPower%') AS actual,
       5 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE supplier_xml LIKE '%TechParts%'
                                    OR supplier_xml LIKE '%NetCore%'
                                    OR supplier_xml LIKE '%EuroPower%') = 5
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.products_xml;


-- ============================================================================
-- 9. TOP-LEVEL FIELDS STILL FLATTENED — product_name is not NULL
-- ============================================================================
-- xml_paths only affects the specified subtrees. Other paths should
-- still be flattened normally into their own columns.

SELECT 'top_level_flattened' AS check_name,
       COUNT(*) FILTER (WHERE product_name IS NOT NULL AND category IS NOT NULL) AS actual,
       5 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE product_name IS NOT NULL AND category IS NOT NULL) = 5
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.products_json;


-- ============================================================================
-- 10. SPOT CHECK PRD-001 JSON — verify specs contain expected keys
-- ============================================================================

SELECT product_id, product_name, specs_json
FROM {{zone_name}}.xml.products_json
WHERE product_id = 'PRD-001';


-- ============================================================================
-- 11. SPOT CHECK PRD-001 XML — verify specs contain original XML elements
-- ============================================================================

SELECT product_id, product_name, specs_xml
FROM {{zone_name}}.xml.products_xml
WHERE product_id = 'PRD-001';


-- ============================================================================
-- 12. COMPARE FORMATS — side-by-side JSON vs XML for same product
-- ============================================================================

SELECT j.product_id,
       j.product_name,
       j.specs_json,
       x.specs_xml
FROM {{zone_name}}.xml.products_json j
JOIN {{zone_name}}.xml.products_xml x ON j.product_id = x.product_id
WHERE j.product_id = 'PRD-002';


-- ============================================================================
-- 13. SUMMARY — All checks
-- ============================================================================

SELECT 'json_row_count' AS check_name,
       CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.products_json
UNION ALL
SELECT 'xml_row_count',
       CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml.products_xml
UNION ALL
SELECT 'specs_is_json',
       CASE WHEN COUNT(*) FILTER (WHERE specs_json LIKE '{%') = 5
            THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml.products_json
UNION ALL
SELECT 'specs_is_xml',
       CASE WHEN COUNT(*) FILTER (WHERE specs_xml LIKE '<%') = 5
            THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml.products_xml
UNION ALL
SELECT 'supplier_json_has_company',
       CASE WHEN COUNT(*) FILTER (WHERE supplier_json LIKE '%TechParts%'
                                    OR supplier_json LIKE '%NetCore%'
                                    OR supplier_json LIKE '%EuroPower%') = 5
            THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml.products_json
UNION ALL
SELECT 'supplier_xml_has_company',
       CASE WHEN COUNT(*) FILTER (WHERE supplier_xml LIKE '%TechParts%'
                                    OR supplier_xml LIKE '%NetCore%'
                                    OR supplier_xml LIKE '%EuroPower%') = 5
            THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml.products_xml
UNION ALL
SELECT 'top_level_flattened',
       CASE WHEN COUNT(*) FILTER (WHERE product_name IS NOT NULL AND category IS NOT NULL) = 5
            THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml.products_json
ORDER BY check_name;
