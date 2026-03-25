-- ============================================================================
-- EDI Repeating Segments — Demo Queries
-- ============================================================================
-- Queries showcasing three repeating_segment_mode options for handling
-- multi-occurrence X12 segments: First (default), Concatenate, and ToJson.
--
-- Three tables are available (all read the same 14 X12 files):
--   repeating_first   — Default mode: only first occurrence of each segment
--   repeating_concat  — Pipe-delimited: n1_2 = "Name1|Name2|Name3"
--   repeating_json    — JSON arrays:    n1_2 = ["Name1","Name2","Name3"]
--
-- Key data points (N1 party names per file):
--   850 purchase_order:   1 N1  — John Doe
--   850 purchase_order_a: 5 N1s — Transplace Laredo, Penjamo Cutting, Test Inc., '', Supplier Name
--   850 edifabric:        1 N1  — ABC AEROSPACE
--   810 invoice_a:        6 N1s — Aaron Copeland, XYZ Bank, Philadelphia, Music Insurance..., ...
--   810 edifabric:        1 N1  — ABC AEROSPACE CORPORATION
--   855 po_ack:           2 N1s — XYZ MANUFACTURING CO, KOHLS DEPARTMENT STORES
--   997 functional_ack:   0 N1s
-- ============================================================================


-- ============================================================================
-- 1. Default Mode — First Occurrence Only
-- ============================================================================
-- Shows n1_1 (entity code) and n1_2 (party name) from the First-mode table.
-- For files with multiple N1 segments, only the first occurrence appears.
-- This is the baseline — demonstrates what data is LOST when repeating
-- segments are not explicitly handled.
--
-- What you'll see:
--   - x12_850_purchase_order.edi:     n1_2 = 'John Doe' (only 1 N1, nothing lost)
--   - x12_850_purchase_order_a.edi:   n1_2 = 'Transplace Laredo' (4 more N1s hidden)
--   - x12_810_invoice_a.edi:          n1_2 = 'Aaron Copeland' (5 more N1s hidden)
--   - x12_855_purchase_order_ack.edi: n1_2 = 'XYZ MANUFACTURING CO' (1 more N1 hidden)

ASSERT ROW_COUNT = 14
ASSERT VALUE n1_2 = 'John Doe' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE n1_2 = 'Transplace Laredo' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE n1_2 = 'Aaron Copeland' WHERE df_file_name = 'x12_810_invoice_a.edi'
SELECT
    df_file_name,
    st_1 AS txn_type,
    n1_1 AS entity_code,
    n1_2 AS party_name
FROM {{zone_name}}.edi.repeating_first
ORDER BY df_file_name;


-- ============================================================================
-- 2. Concatenate Mode — All Party Names
-- ============================================================================
-- Shows n1_2 from the Concatenate table — all party names from every N1
-- segment occurrence joined with a pipe (|) separator.
--
-- Files with a single N1 show just the name (no pipes). Files with no N1
-- segments return NULL.
--
-- What you'll see:
--   - x12_850_purchase_order.edi:     "John Doe" (single — no pipes)
--   - x12_850_purchase_order_a.edi:   "Transplace Laredo|Penjamo Cutting|Test Inc.||Supplier Name"
--   - x12_855_purchase_order_ack.edi: "XYZ MANUFACTURING CO|KOHLS DEPARTMENT STORES"
--   - x12_810_invoice_a.edi:          "Aaron Copeland|XYZ Bank|Philadelphia|Music Insurance Co. - San Fran|..."

ASSERT ROW_COUNT = 14
ASSERT VALUE party_names = 'John Doe' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE party_names = 'ABC AEROSPACE' WHERE df_file_name = 'x12_850_purchase_order_edifabric.edi'
ASSERT VALUE party_names = 'ABC AEROSPACE CORPORATION' WHERE df_file_name = 'x12_810_invoice_edifabric.edi'
SELECT
    df_file_name,
    st_1 AS txn_type,
    n1_1 AS entity_codes,
    n1_2 AS party_names
FROM {{zone_name}}.edi.repeating_concat
ORDER BY df_file_name;


-- ============================================================================
-- 3. ToJson Mode — Party Names as Array
-- ============================================================================
-- Shows n1_2 from the ToJson table — all party names as a JSON array.
-- Each occurrence becomes an element in the array.
--
-- Single-occurrence files produce a single-element array.
-- Files with no N1 segments return NULL.
--
-- What you'll see:
--   - x12_850_purchase_order.edi:     ["John Doe"]
--   - x12_855_purchase_order_ack.edi: ["XYZ MANUFACTURING CO","KOHLS DEPARTMENT STORES"]
--   - x12_810_invoice_edifabric.edi:  ["ABC AEROSPACE CORPORATION"]

ASSERT ROW_COUNT = 14
SELECT
    df_file_name,
    st_1 AS txn_type,
    n1_1 AS entity_codes_json,
    n1_2 AS party_names_json
FROM {{zone_name}}.edi.repeating_json
ORDER BY df_file_name;


-- ============================================================================
-- 4. Compare Default vs Concatenate — Data Loss Demonstration
-- ============================================================================
-- For x12_850_purchase_order_a.edi (5 N1 segments), shows how default First
-- mode loses data while Concatenate mode preserves all occurrences.
--
-- What you'll see:
--   - First mode:  n1_2 = 'Transplace Laredo' (only 1 of 5 names)
--   - Concat mode: n1_2 = 'Transplace Laredo|Penjamo Cutting|Test Inc.||Supplier Name'

ASSERT ROW_COUNT = 2
SELECT
    'First (default)' AS mode,
    f.n1_2 AS party_names,
    f.po1_1 AS first_po1_line
FROM {{zone_name}}.edi.repeating_first f
WHERE f.df_file_name = 'x12_850_purchase_order_a.edi'

UNION ALL

SELECT
    'Concatenate' AS mode,
    c.n1_2 AS party_names,
    c.po1_1 AS first_po1_line
FROM {{zone_name}}.edi.repeating_concat c
WHERE c.df_file_name = 'x12_850_purchase_order_a.edi'

ORDER BY mode;


-- ============================================================================
-- 5. Concatenate PO1 Line Items — Purchase Order Details
-- ============================================================================
-- Shows po1_1 (line numbers), po1_2 (quantities), po1_4 (prices) from the
-- Concatenate table for 850 Purchase Order transactions.
--
-- Files with multiple PO1 line items have pipe-delimited values.
--
-- What you'll see:
--   - x12_850_purchase_order.edi:          po1_1='1', po1_2='1', po1_4='19.95'
--   - x12_850_purchase_order_a.edi:        po1_1='000100001|000200001|000200002',
--                                           po1_2='2500|2000|1000', po1_4='2.53|3.41|3.41'
--   - x12_850_purchase_order_edifabric.edi: po1_1='1', po1_2='25', po1_4='36'

ASSERT ROW_COUNT = 3
SELECT
    df_file_name,
    po1_1 AS line_numbers,
    po1_2 AS quantities,
    po1_3 AS units_of_measure,
    po1_4 AS unit_prices
FROM {{zone_name}}.edi.repeating_concat
WHERE st_1 = '850'
ORDER BY df_file_name;


-- ============================================================================
-- 6. Count Occurrences from Concatenate Mode
-- ============================================================================
-- Uses the pipe-delimited n1_2 from Concatenate mode to count how many N1
-- segments each file contains. The count is derived by counting pipe
-- separators: occurrences = LENGTH(n1_2) - LENGTH(REPLACE(n1_2, '|', '')) + 1.
--
-- Files with NULL n1_2 (no N1 segments, like the 997) get count = 0.
--
-- What you'll see:
--   - x12_850_purchase_order.edi:   1  (single name, no pipes)
--   - x12_850_purchase_order_a.edi: 5  (4 pipes = 5 occurrences)
--   - x12_810_invoice_a.edi:        6  (5 pipes = 6 occurrences)
--   - x12_997_functional_acknowledgment.edi: 0 (NULL — no N1 segments)

ASSERT ROW_COUNT = 14
SELECT
    df_file_name,
    st_1 AS txn_type,
    n1_2 AS party_names_concat,
    CASE
        WHEN n1_2 IS NULL THEN 0
        ELSE LENGTH(n1_2) - LENGTH(REPLACE(n1_2, '|', '')) + 1
    END AS n1_count
FROM {{zone_name}}.edi.repeating_concat
ORDER BY n1_count DESC, df_file_name;


-- ============================================================================
-- 7. ToJson Array Length — Party Names Count via JSON
-- ============================================================================
-- Uses json_array_length on the JSON mode's n1_2 to count party names per
-- file. This is the JSON equivalent of the pipe-counting approach in query 6.
--
-- Files with NULL n1_2 (no N1 segments) get count = 0.
--
-- What you'll see:
--   - x12_810_invoice_a.edi:        6 (array with 6 elements)
--   - x12_850_purchase_order_a.edi: 5 (array with 5 elements)
--   - x12_997_functional_acknowledgment.edi: 0 (NULL — no array)

ASSERT ROW_COUNT = 14
SELECT
    df_file_name,
    st_1 AS txn_type,
    n1_2 AS party_names_json,
    COALESCE(json_array_length(n1_2), 0) AS n1_count
FROM {{zone_name}}.edi.repeating_json
ORDER BY n1_count DESC, df_file_name;


-- ============================================================================
-- 8. VERIFY — All Checks
-- ============================================================================
-- Automated pass/fail verification that the demo loaded correctly and all
-- three repeating_segment_mode tables are producing expected results.
-- All checks should return PASS.

ASSERT ROW_COUNT = 8
ASSERT VALUE result = 'PASS' WHERE check_name = 'concat_has_pipes'
ASSERT VALUE result = 'PASS' WHERE check_name = 'first_count_14'
ASSERT VALUE result = 'PASS' WHERE check_name = 'first_is_first_only'
ASSERT VALUE result = 'PASS' WHERE check_name = 'first_po1_populated'
ASSERT VALUE result = 'PASS' WHERE check_name = 'json_count_14'
ASSERT VALUE result = 'PASS' WHERE check_name = 'concat_count_14'
ASSERT VALUE result = 'PASS' WHERE check_name = 'json_has_arrays'
ASSERT VALUE result = 'PASS' WHERE check_name = 'three_tables_same_count'
SELECT check_name, result FROM (

    -- Check 1: First table has 14 rows (one per .edi file)
    SELECT 'first_count_14' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_first) = 14
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

    -- Check 4: All three tables have the same row count
    SELECT 'three_tables_same_count' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_first)
                   = (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_concat)
                AND (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_concat)
                   = (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_json)
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: First mode returns only first N1 (Transplace Laredo, not the full pipe list)
    SELECT 'first_is_first_only' AS check_name,
           CASE WHEN (SELECT n1_2 FROM {{zone_name}}.edi.repeating_first
                       WHERE df_file_name = 'x12_850_purchase_order_a.edi') = 'Transplace Laredo'
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: First table has PO1 columns populated for 850 transactions
    SELECT 'first_po1_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_first
                       WHERE po1_1 IS NOT NULL AND st_1 = '850') = 3
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 7: Concatenate table has pipe-delimited values (contains '|')
    SELECT 'concat_has_pipes' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_concat
                       WHERE n1_2 LIKE '%|%') > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 8: ToJson table has JSON arrays (contains '[')
    SELECT 'json_has_arrays' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_json
                       WHERE n1_2 LIKE '[%') > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
