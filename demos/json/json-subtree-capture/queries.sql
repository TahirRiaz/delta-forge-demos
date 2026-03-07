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

SELECT id, title, type, bedrooms, sqft, status,
       location, pricing
FROM {{zone_name}}.json.listings_captured
ORDER BY id;


-- ============================================================================
-- 4. BROWSE FLATTENED — same data with individual columns
-- ============================================================================

SELECT id, title, type, bedrooms, sqft, status,
       location_address_street, location_address_city, location_address_state, location_neighborhood,
       pricing_list_price, pricing_tax_annual, pricing_mortgage_estimate_monthly_payment
FROM {{zone_name}}.json.listings_flattened
ORDER BY id;


-- ============================================================================
-- 5. LOCATION IS JSON — location_json should be a JSON object
-- ============================================================================
-- When json_paths captures a subtree, the result is a JSON string.

SELECT 'location_is_json' AS check_name,
       COUNT(*) FILTER (WHERE location LIKE '{%') AS actual,
       5 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE location LIKE '{%') = 5
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.json.listings_captured;


-- ============================================================================
-- 6. PRICING IS JSON — pricing_json should be a JSON object
-- ============================================================================

SELECT 'pricing_is_json' AS check_name,
       COUNT(*) FILTER (WHERE pricing LIKE '{%') AS actual,
       5 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE pricing LIKE '{%') = 5
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.json.listings_captured;


-- ============================================================================
-- 7. LOCATION CONTENT — location_json contains city names
-- ============================================================================

SELECT 'location_has_city' AS check_name,
       COUNT(*) FILTER (WHERE location LIKE '%San Francisco%'
                           OR location LIKE '%Portland%'
                           OR location LIKE '%Seattle%'
                           OR location LIKE '%Austin%'
                           OR location LIKE '%Boston%') AS actual,
       5 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE location LIKE '%San Francisco%'
                                    OR location LIKE '%Portland%'
                                    OR location LIKE '%Seattle%'
                                    OR location LIKE '%Austin%'
                                    OR location LIKE '%Boston%') = 5
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.json.listings_captured;


-- ============================================================================
-- 8. PRICING CONTENT — pricing_json contains list_price values
-- ============================================================================

SELECT 'pricing_has_list_price' AS check_name,
       COUNT(*) FILTER (WHERE pricing LIKE '%list_price%') AS actual,
       5 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE pricing LIKE '%list_price%') = 5
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
       COUNT(*) FILTER (WHERE location_address_city IS NOT NULL) AS actual,
       5 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE location_address_city IS NOT NULL) = 5
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.json.listings_flattened;


-- ============================================================================
-- 11. SPOT CHECK LST-001 — verify captured JSON contains expected data
-- ============================================================================

SELECT id, title, location, pricing
FROM {{zone_name}}.json.listings_captured
WHERE id = 'LST-001';


-- ============================================================================
-- 12. COMPARE APPROACHES — side-by-side captured vs flattened
-- ============================================================================

SELECT c.id,
       c.title,
       c.location,
       f.location_address_city,
       f.location_address_state,
       f.location_neighborhood,
       f.pricing_list_price,
       f.pricing_mortgage_estimate_monthly_payment
FROM {{zone_name}}.json.listings_captured c
JOIN {{zone_name}}.json.listings_flattened f ON c.id = f.id
WHERE c.id = 'LST-005';


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
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.listings_captured WHERE location LIKE '{%') = 5
                THEN 'PASS' ELSE 'FAIL' END

    UNION ALL

    SELECT 'pricing_is_json',
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.listings_captured WHERE pricing LIKE '{%') = 5
                THEN 'PASS' ELSE 'FAIL' END

    UNION ALL

    SELECT 'location_has_city',
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.listings_captured
                      WHERE location LIKE '%San Francisco%'
                         OR location LIKE '%Portland%'
                         OR location LIKE '%Seattle%'
                         OR location LIKE '%Austin%'
                         OR location LIKE '%Boston%') = 5
                THEN 'PASS' ELSE 'FAIL' END

    UNION ALL

    SELECT 'pricing_has_list_price',
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.listings_captured WHERE pricing LIKE '%list_price%') = 5
                THEN 'PASS' ELSE 'FAIL' END

    UNION ALL

    SELECT 'top_level_flattened',
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.listings_captured WHERE title IS NOT NULL AND bedrooms IS NOT NULL) = 5
                THEN 'PASS' ELSE 'FAIL' END

    UNION ALL

    SELECT 'flattened_has_city',
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.listings_flattened WHERE location_address_city IS NOT NULL) = 5
                THEN 'PASS' ELSE 'FAIL' END

) checks
ORDER BY check_name;
