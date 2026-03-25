-- ============================================================================
-- EDI Repeating Segments — Demo Queries
-- ============================================================================
-- Queries showcasing the three repeating-segment strategies: Indexed,
-- Concatenate, and ToJson. Each strategy handles multi-occurrence X12
-- segments (N1 party loops, PO1 line items) differently.
--
-- Three tables are available:
--   repeating_indexed  — Each occurrence in its own columns (n1_1_1, n1_1_2, ...)
--   repeating_concat   — All occurrences pipe-delimited (n1_2 = 'A|B|C')
--   repeating_json     — All occurrences as JSON arrays (n1_2 = '["A","B","C"]')
--
-- Indexed column naming: {segment}_{occurrence}_{element} (all 1-based)
--   n1_1_1 = N1 segment, 1st occurrence, element 1 (entity code)
--   n1_1_2 = N1 segment, 1st occurrence, element 2 (party name)
--   n1_2_1 = N1 segment, 2nd occurrence, element 1
--   po1_1_1 = PO1 segment, 1st occurrence, element 1 (line number)
--   po1_1_2 = PO1 segment, 1st occurrence, element 2 (quantity)
--   po1_1_3 = PO1 segment, 1st occurrence, element 3 (unit of measure)
--   po1_1_4 = PO1 segment, 1st occurrence, element 4 (unit price)
-- ============================================================================


-- ============================================================================
-- 1. Indexed Mode — Multi-Address Overview
-- ============================================================================
-- Shows up to 6 N1 entity codes and party names from the indexed table.
-- Each occurrence is a separate column: n1_1_1/n1_1_2 through n1_6_1/n1_6_2.
-- Files with fewer N1 segments will have NULLs in the higher-numbered columns.
--
-- What you'll see:
--   - df_file_name:  Source .edi file
--   - n1_1_1..n1_6_1: Entity identifier codes for up to 6 N1 occurrences
--   - n1_1_2..n1_6_2: Party names for up to 6 N1 occurrences
--
-- Examples:
--   x12_810_invoice_a.edi: 6 N1s — SO/Aaron Copeland, RI/XYZ Bank, SF/Philadelphia, ...
--   x12_850_purchase_order.edi: 1 N1 — ST/John Doe
--   x12_850_purchase_order_a.edi: 5 N1s — ST/Transplace Laredo, Z7/Penjamo Cutting, ...

ASSERT ROW_COUNT = 14
ASSERT VALUE n1_1_2 = 'Transplace Laredo' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE n1_2_2 = 'Penjamo Cutting' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE n1_1_2 = 'John Doe' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE n1_1_2 = 'Aaron Copeland' WHERE df_file_name = 'x12_810_invoice_a.edi'
SELECT
    df_file_name,
    n1_1_1, n1_1_2,
    n1_2_1, n1_2_2,
    n1_3_1, n1_3_2,
    n1_4_1, n1_4_2,
    n1_5_1, n1_5_2,
    n1_6_1, n1_6_2
FROM {{zone_name}}.edi.repeating_indexed
ORDER BY df_file_name;


-- ============================================================================
-- 2. Indexed Mode — PO1 Line Items
-- ============================================================================
-- Shows PO1 line-item columns for files that contain purchase order data
-- (850 and 855 transactions). Each PO1 occurrence gets 4 elements:
-- line number, quantity, unit of measure, and unit price.
--
-- What you'll see:
--   - df_file_name:    Source file
--   - po1_1_1..po1_3_1: Line numbers for up to 3 PO1 occurrences
--   - po1_1_2..po1_3_2: Quantities ordered
--   - po1_1_3..po1_3_3: Units of measure (EA, YD)
--   - po1_1_4..po1_3_4: Unit prices
--
-- Examples:
--   x12_850_purchase_order.edi: 1 PO1 — line 1, qty 1, EA, $19.95
--   x12_850_purchase_order_a.edi: 3 PO1s — 2500 YD @ $2.53, 2000 YD @ $3.41, 1000 YD @ $3.41

ASSERT ROW_COUNT = 3
ASSERT VALUE po1_1_1 = '1' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE po1_1_4 = '19.95' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE po1_1_2 = '2500' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE po1_2_2 = '2000' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
SELECT
    df_file_name,
    po1_1_1, po1_1_2, po1_1_3, po1_1_4,
    po1_2_1, po1_2_2, po1_2_3, po1_2_4,
    po1_3_1, po1_3_2, po1_3_3, po1_3_4
FROM {{zone_name}}.edi.repeating_indexed
WHERE po1_1_1 IS NOT NULL
  AND st_1 IN ('850', '855')
ORDER BY df_file_name;


-- ============================================================================
-- 3. Concatenate Mode — Party Names
-- ============================================================================
-- Shows how Concatenate mode pipe-delimits all N1 party names into a single
-- column. Files with multiple N1 segments will have pipe-separated values;
-- files with one N1 will have a plain string; files with zero N1s will be NULL.
--
-- What you'll see:
--   - df_file_name:  Source file
--   - entity_codes:  All N1 entity identifier codes, pipe-delimited
--   - party_names:   All N1 party names, pipe-delimited
--
-- Examples:
--   x12_850_purchase_order.edi: 'John Doe' (single N1)
--   x12_810_invoice_a.edi: 'Aaron Copeland|XYZ Bank|Philadelphia|...' (6 N1s)

ASSERT ROW_COUNT = 14
ASSERT VALUE party_names = 'John Doe' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE party_names = 'ABC AEROSPACE' WHERE df_file_name = 'x12_850_purchase_order_edifabric.edi'
SELECT
    df_file_name,
    n1_1 AS entity_codes,
    n1_2 AS party_names
FROM {{zone_name}}.edi.repeating_concat
ORDER BY df_file_name;


-- ============================================================================
-- 4. ToJson Mode — Party Names
-- ============================================================================
-- Shows how ToJson mode encodes all N1 party names as JSON arrays. Files with
-- multiple N1 segments produce arrays with multiple elements; single-N1 files
-- produce single-element arrays.
--
-- What you'll see:
--   - df_file_name:  Source file
--   - entity_codes:  N1 entity codes as JSON array
--   - party_names:   N1 party names as JSON array
--
-- Examples:
--   x12_850_purchase_order.edi: '["John Doe"]'
--   x12_810_invoice_a.edi: '["Aaron Copeland","XYZ Bank","Philadelphia",...]'

ASSERT ROW_COUNT = 14
SELECT
    df_file_name,
    n1_1 AS entity_codes,
    n1_2 AS party_names
FROM {{zone_name}}.edi.repeating_json
ORDER BY df_file_name;


-- ============================================================================
-- 5. Compare All Three Modes — Side-by-Side
-- ============================================================================
-- For x12_850_purchase_order_a.edi (5 N1 parties, 3 PO1 line items), shows
-- how each mode represents the same repeating data. Uses UNION ALL with a
-- mode label to compare the three approaches in a single result set.
--
-- What you'll see:
--   - mode:       'indexed', 'concatenate', or 'to_json'
--   - party_info: The N1 party name data in each mode's format
--   - po_info:    The PO1 quantity data in each mode's format

ASSERT ROW_COUNT = 3
SELECT
    'indexed' AS mode,
    n1_1_2 || ' | ' || COALESCE(n1_2_2, '') || ' | ' || COALESCE(n1_3_2, '')
        || ' | ' || COALESCE(n1_4_2, '') || ' | ' || COALESCE(n1_5_2, '') AS party_info,
    po1_1_2 || ' | ' || COALESCE(po1_2_2, '') || ' | ' || COALESCE(po1_3_2, '') AS po_info
FROM {{zone_name}}.edi.repeating_indexed
WHERE df_file_name = 'x12_850_purchase_order_a.edi'

UNION ALL

SELECT
    'concatenate' AS mode,
    n1_2 AS party_info,
    po1_2 AS po_info
FROM {{zone_name}}.edi.repeating_concat
WHERE df_file_name = 'x12_850_purchase_order_a.edi'

UNION ALL

SELECT
    'to_json' AS mode,
    n1_2 AS party_info,
    po1_2 AS po_info
FROM {{zone_name}}.edi.repeating_json
WHERE df_file_name = 'x12_850_purchase_order_a.edi';


-- ============================================================================
-- 6. Indexed PO1 Price Analysis — Line Totals
-- ============================================================================
-- Computes line totals (quantity * price) from the indexed PO1 columns for
-- files that have PO1 data. Only 850 files have both quantity and price;
-- 855 files have quantity but no price.
--
-- What you'll see:
--   - df_file_name:     Source file
--   - line_1_total:     qty * price for PO1 occurrence 1
--   - line_2_total:     qty * price for PO1 occurrence 2 (NULL if < 2 PO1s)
--   - line_3_total:     qty * price for PO1 occurrence 3 (NULL if < 3 PO1s)
--   - order_total:      Sum of all line totals
--
-- Examples:
--   x12_850_purchase_order.edi: 1 * 19.95 = 19.95
--   x12_850_purchase_order_a.edi: 2500*2.53 + 2000*3.41 + 1000*3.41 = 16555.00

ASSERT ROW_COUNT = 3
SELECT
    df_file_name,
    CAST(po1_1_2 AS DOUBLE) * CAST(po1_1_4 AS DOUBLE) AS line_1_total,
    CASE WHEN po1_2_2 IS NOT NULL AND po1_2_4 IS NOT NULL
         THEN CAST(po1_2_2 AS DOUBLE) * CAST(po1_2_4 AS DOUBLE)
         ELSE NULL END AS line_2_total,
    CASE WHEN po1_3_2 IS NOT NULL AND po1_3_4 IS NOT NULL
         THEN CAST(po1_3_2 AS DOUBLE) * CAST(po1_3_4 AS DOUBLE)
         ELSE NULL END AS line_3_total,
    CAST(po1_1_2 AS DOUBLE) * CAST(po1_1_4 AS DOUBLE)
        + COALESCE(CAST(po1_2_2 AS DOUBLE) * CAST(po1_2_4 AS DOUBLE), 0)
        + COALESCE(CAST(po1_3_2 AS DOUBLE) * CAST(po1_3_4 AS DOUBLE), 0) AS order_total
FROM {{zone_name}}.edi.repeating_indexed
WHERE po1_1_4 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 7. Multi-Party Detection via Concatenate
-- ============================================================================
-- Uses the Concatenate table to quickly find files that have multiple N1
-- parties by checking for pipe characters in the n1_2 column. This is a
-- practical pattern: use Concatenate mode for filtering/detection, then
-- switch to Indexed mode for structured access to individual occurrences.
--
-- What you'll see:
--   - df_file_name:    Source file
--   - party_count:     Approximate number of parties (pipe count + 1)
--   - party_names:     Pipe-delimited party names

ASSERT ROW_COUNT >= 5
SELECT
    df_file_name,
    LENGTH(n1_2) - LENGTH(REPLACE(n1_2, '|', '')) + 1 AS party_count,
    n1_2 AS party_names
FROM {{zone_name}}.edi.repeating_concat
WHERE n1_2 LIKE '%|%'
ORDER BY LENGTH(n1_2) - LENGTH(REPLACE(n1_2, '|', '')) DESC, df_file_name;


-- ============================================================================
-- 8. VERIFY — All Checks
-- ============================================================================
-- Automated pass/fail verification that all three tables loaded correctly
-- and each repeating-segment mode produces the expected output format.

ASSERT ROW_COUNT = 6
ASSERT VALUE result = 'PASS' WHERE check_name = 'concat_count_14'
ASSERT VALUE result = 'PASS' WHERE check_name = 'concat_has_pipes'
ASSERT VALUE result = 'PASS' WHERE check_name = 'indexed_count_14'
ASSERT VALUE result = 'PASS' WHERE check_name = 'indexed_multi_n1'
ASSERT VALUE result = 'PASS' WHERE check_name = 'json_count_14'
ASSERT VALUE result = 'PASS' WHERE check_name = 'json_has_arrays'
SELECT check_name, result FROM (

    -- Check 1: Indexed table has 14 rows (one per .edi file)
    SELECT 'indexed_count_14' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_indexed) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: Concatenate table has 14 rows
    SELECT 'concat_count_14' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_concat) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: ToJson table has 14 rows
    SELECT 'json_count_14' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_json) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: Indexed table has multi-occurrence N1 data (n1_2_2 populated)
    SELECT 'indexed_multi_n1' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_indexed
                       WHERE n1_2_2 IS NOT NULL) > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: Concatenate table has pipe-delimited values
    SELECT 'concat_has_pipes' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_concat
                       WHERE n1_2 LIKE '%|%') > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: ToJson table has JSON arrays
    SELECT 'json_has_arrays' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_json
                       WHERE n1_2 LIKE '[%') > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
