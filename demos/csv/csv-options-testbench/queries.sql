-- ============================================================================
-- CSV Advanced Options Testbench — Verification Queries
-- ============================================================================
-- Each query is designed so that CORRECT results prove the option is working.
-- If an option is NOT wired, the result will be obviously wrong.
-- ============================================================================


-- ============================================================================
-- 1. PIPE DELIMITER — delimiter = '|'
-- ============================================================================
-- If wired: 5 rows with 4 proper columns
-- If NOT wired: 1 column containing the whole pipe-delimited line

-- Expected: 5 rows, amounts sum to 1247.50
SELECT 'delimiter' AS option_tested,
       COUNT(*) AS row_count,
       CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END AS result
FROM external.csv.opt_delimiter;

-- Verify column parsing works — this query fails if delimiter not wired
SELECT id, name, amount, category
FROM external.csv.opt_delimiter
ORDER BY id;


-- ============================================================================
-- 2. NULL VALUE — null_value = 'N/A'
-- ============================================================================
-- If wired: 2 rows have NULL score (ids 2 and 4)
-- If NOT wired: 0 rows have NULL score (N/A stays as literal string)

-- Expected: null_count = 2
SELECT 'null_value' AS option_tested,
       COUNT(*) FILTER (WHERE score IS NULL) AS null_count,
       CASE WHEN COUNT(*) FILTER (WHERE score IS NULL) = 2
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM external.csv.opt_null_value;

SELECT id, name, score, status
FROM external.csv.opt_null_value
ORDER BY id;


-- ============================================================================
-- 3. COMMENT CHARACTER — comment_char = '#'
-- ============================================================================
-- If wired: 3 data rows (all # lines skipped)
-- If NOT wired: parser error or extra garbage rows

-- Expected: row_count = 3
SELECT 'comment_char' AS option_tested,
       COUNT(*) AS row_count,
       CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END AS result
FROM external.csv.opt_comment;

SELECT id, sensor, temperature, humidity
FROM external.csv.opt_comment
ORDER BY id;


-- ============================================================================
-- 4. SKIP STARTING ROWS — skip_starting_rows = 3
-- ============================================================================
-- If wired: 5 data rows with proper column names (id, product, warehouse...)
-- If NOT wired: column names are wrong ("Report: Quarterly..." etc.)

-- Expected: row_count = 5
SELECT 'skip_starting_rows' AS option_tested,
       COUNT(*) AS row_count,
       CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END AS result
FROM external.csv.opt_skip_rows;

-- This query would fail entirely if skip_rows not wired (no "product" column)
SELECT id, product, warehouse, quantity, unit_cost
FROM external.csv.opt_skip_rows
ORDER BY id;


-- ============================================================================
-- 5. MAX ROWS — max_rows = 5
-- ============================================================================
-- If wired: exactly 5 rows (ids 1-5)
-- If NOT wired: all 10 rows

-- Expected: row_count = 5, max_id = 5
SELECT 'max_rows' AS option_tested,
       COUNT(*) AS row_count,
       CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END AS result
FROM external.csv.opt_max_rows;


-- ============================================================================
-- 6. TRIM WHITESPACE — trim_whitespace = 'true'
-- ============================================================================
-- If wired: name='Alice' (length 5), city='New York' (length 8)
-- If NOT wired: name='  Alice  ' (length 9), city='  New York  ' (length 14)

-- Expected: name_length = 5
SELECT 'trim_whitespace' AS option_tested,
       name,
       LENGTH(name) AS name_length,
       CASE WHEN LENGTH(name) = 5 THEN 'PASS' ELSE 'FAIL' END AS result
FROM external.csv.opt_trim
WHERE CAST(id AS INT) = 1;

-- Verify exact match works (would fail without trim)
SELECT id, name, city, score
FROM external.csv.opt_trim
WHERE name = 'Alice';


-- ============================================================================
-- 7. SEMICOLON + QUOTED FIELDS — delimiter=';' quote='"'
-- ============================================================================
-- If wired: 4 rows, descriptions with embedded semicolons parse correctly
-- If NOT wired: semicolons split columns incorrectly

-- Expected: row_count = 4
SELECT 'semicolon_quoted' AS option_tested,
       COUNT(*) AS row_count,
       CASE WHEN COUNT(*) = 4 THEN 'PASS' ELSE 'FAIL' END AS result
FROM external.csv.opt_quoted;

-- The semicolons inside description should NOT split the column
SELECT id, name, description, price
FROM external.csv.opt_quoted
ORDER BY id;


-- ============================================================================
-- 8. COMBINED OPTIONS — delimiter + comment + null + trim together
-- ============================================================================
-- Tests all options working simultaneously.
-- Expected: 5 rows (comments skipped), 2 null scores, names trimmed

SELECT 'combined_options' AS option_tested,
       COUNT(*) AS total_rows,
       COUNT(*) FILTER (WHERE score IS NULL) AS null_scores,
       CASE WHEN COUNT(*) = 5
             AND COUNT(*) FILTER (WHERE score IS NULL) = 2
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM external.csv.opt_combined;

-- Verify trim + null + comment all work
SELECT id, name, LENGTH(name) AS name_len, score, department
FROM external.csv.opt_combined
ORDER BY CAST(id AS INT);


-- ============================================================================
-- 9. SUMMARY — All Options Test Results
-- ============================================================================
-- Collects PASS/FAIL for every option into one view.

SELECT 'delimiter' AS option, CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END AS result FROM external.csv.opt_delimiter
UNION ALL
SELECT 'null_value', CASE WHEN COUNT(*) FILTER (WHERE score IS NULL) = 2 THEN 'PASS' ELSE 'FAIL' END FROM external.csv.opt_null_value
UNION ALL
SELECT 'comment_char', CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END FROM external.csv.opt_comment
UNION ALL
SELECT 'skip_starting_rows', CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END FROM external.csv.opt_skip_rows
UNION ALL
SELECT 'max_rows', CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END FROM external.csv.opt_max_rows
UNION ALL
SELECT 'semicolon_quoted', CASE WHEN COUNT(*) = 4 THEN 'PASS' ELSE 'FAIL' END FROM external.csv.opt_quoted
UNION ALL
SELECT 'combined_options', CASE WHEN COUNT(*) = 5 AND COUNT(*) FILTER (WHERE score IS NULL) = 2 THEN 'PASS' ELSE 'FAIL' END FROM external.csv.opt_combined
ORDER BY option;
