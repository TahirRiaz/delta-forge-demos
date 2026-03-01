-- ============================================================================
-- JSON Subtree Capture — Verification Queries
-- ============================================================================
-- Each query verifies a specific aspect of json_paths subtree capture,
-- contrasting captured (JSON blob) vs flattened approaches.
-- ============================================================================


-- ============================================================================
-- 1. CAPTURED TABLE ROW COUNT — 3 residential + 2 commercial = 5
-- ============================================================================

SELECT 'captured_row_count' AS check_name,
       COUNT(*) AS actual,
       5 AS expected,
       CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.json.listings_captured;


-- ============================================================================
-- 2. FLATTENED TABLE ROW COUNT — same 5 listings
-- ============================================================================

SELECT 'flattened_row_count' AS check_name,
       COUNT(*) AS actual,
       5 AS expected,
       CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.json.listings_flattened;


-- ============================================================================
-- 3. BROWSE CAPTURED — top-level fields + JSON blobs
-- ============================================================================

SELECT listing_id, title, property_type, bedrooms, sqft, status,
       location_json, pricing_json
FROM {{zone_name}}.json.listings_captured
ORDER BY listing_id;


-- ============================================================================
-- 4. BROWSE FLATTENED — same data with individual columns
-- ============================================================================

SELECT listing_id, title, property_type, bedrooms, sqft, status,
       street, city, state, neighborhood,
       list_price, tax_annual, monthly_payment
FROM {{zone_name}}.json.listings_flattened
ORDER BY listing_id;


-- ============================================================================
-- 5. LOCATION IS JSON — location_json should be a JSON object
-- ============================================================================
-- When json_paths captures a subtree, the result is a JSON string.

SELECT 'location_is_json' AS check_name,
       COUNT(*) FILTER (WHERE location_json LIKE '{%') AS actual,
       5 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE location_json LIKE '{%') = 5
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.json.listings_captured;


-- ============================================================================
-- 6. PRICING IS JSON — pricing_json should be a JSON object
-- ============================================================================

SELECT 'pricing_is_json' AS check_name,
       COUNT(*) FILTER (WHERE pricing_json LIKE '{%') AS actual,
       5 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE pricing_json LIKE '{%') = 5
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.json.listings_captured;


-- ============================================================================
-- 7. LOCATION CONTENT — location_json contains city names
-- ============================================================================

SELECT 'location_has_city' AS check_name,
       COUNT(*) FILTER (WHERE location_json LIKE '%San Francisco%'
                           OR location_json LIKE '%Portland%'
                           OR location_json LIKE '%Seattle%'
                           OR location_json LIKE '%Austin%'
                           OR location_json LIKE '%Boston%') AS actual,
       5 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE location_json LIKE '%San Francisco%'
                                    OR location_json LIKE '%Portland%'
                                    OR location_json LIKE '%Seattle%'
                                    OR location_json LIKE '%Austin%'
                                    OR location_json LIKE '%Boston%') = 5
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.json.listings_captured;


-- ============================================================================
-- 8. PRICING CONTENT — pricing_json contains list_price values
-- ============================================================================

SELECT 'pricing_has_list_price' AS check_name,
       COUNT(*) FILTER (WHERE pricing_json LIKE '%list_price%') AS actual,
       5 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE pricing_json LIKE '%list_price%') = 5
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.json.listings_captured;


-- ============================================================================
-- 9. TOP-LEVEL FIELDS STILL WORK — title and bedrooms populated
-- ============================================================================
-- json_paths only affects the specified subtrees. Other fields flatten normally.

SELECT 'top_level_flattened' AS check_name,
       COUNT(*) FILTER (WHERE title IS NOT NULL AND bedrooms IS NOT NULL) AS actual,
       5 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE title IS NOT NULL AND bedrooms IS NOT NULL) = 5
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.json.listings_captured;


-- ============================================================================
-- 10. FLATTENED HAS CITY — flattened table has direct city column
-- ============================================================================

SELECT 'flattened_has_city' AS check_name,
       COUNT(*) FILTER (WHERE city IS NOT NULL) AS actual,
       5 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE city IS NOT NULL) = 5
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.json.listings_flattened;


-- ============================================================================
-- 11. SPOT CHECK LST-001 — verify captured JSON contains expected data
-- ============================================================================

SELECT listing_id, title, location_json, pricing_json
FROM {{zone_name}}.json.listings_captured
WHERE listing_id = 'LST-001';


-- ============================================================================
-- 12. COMPARE APPROACHES — side-by-side captured vs flattened
-- ============================================================================

SELECT c.listing_id,
       c.title,
       c.location_json,
       f.city,
       f.state,
       f.neighborhood,
       f.list_price,
       f.monthly_payment
FROM {{zone_name}}.json.listings_captured c
JOIN {{zone_name}}.json.listings_flattened f ON c.listing_id = f.listing_id
WHERE c.listing_id = 'LST-005';


-- ============================================================================
-- 13. SUMMARY — All checks
-- ============================================================================

SELECT check_name, result FROM (

    SELECT 'captured_row_count' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.listings_captured) = 5
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'flattened_row_count',
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.listings_flattened) = 5
                THEN 'PASS' ELSE 'FAIL' END

    UNION ALL

    SELECT 'location_is_json',
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.listings_captured WHERE location_json LIKE '{%') = 5
                THEN 'PASS' ELSE 'FAIL' END

    UNION ALL

    SELECT 'pricing_is_json',
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.listings_captured WHERE pricing_json LIKE '{%') = 5
                THEN 'PASS' ELSE 'FAIL' END

    UNION ALL

    SELECT 'location_has_city',
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.listings_captured
                      WHERE location_json LIKE '%San Francisco%'
                         OR location_json LIKE '%Portland%'
                         OR location_json LIKE '%Seattle%'
                         OR location_json LIKE '%Austin%'
                         OR location_json LIKE '%Boston%') = 5
                THEN 'PASS' ELSE 'FAIL' END

    UNION ALL

    SELECT 'pricing_has_list_price',
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.listings_captured WHERE pricing_json LIKE '%list_price%') = 5
                THEN 'PASS' ELSE 'FAIL' END

    UNION ALL

    SELECT 'top_level_flattened',
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.listings_captured WHERE title IS NOT NULL AND bedrooms IS NOT NULL) = 5
                THEN 'PASS' ELSE 'FAIL' END

    UNION ALL

    SELECT 'flattened_has_city',
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.listings_flattened WHERE city IS NOT NULL) = 5
                THEN 'PASS' ELSE 'FAIL' END

) checks
ORDER BY check_name;
