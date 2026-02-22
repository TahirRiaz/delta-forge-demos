-- ============================================================================
-- XML Books Schema Evolution — Verification Queries
-- ============================================================================
-- Each query verifies that schema evolution works correctly:
--   - All 15 books from 5 files are read
--   - New columns added in later files appear as NULL in earlier files
--   - Dropped columns appear as NULL in files that lack them
--   - Attributes (@id, @format) are extracted as attr_id, attr_format
-- ============================================================================


-- ============================================================================
-- 1. TOTAL ROW COUNT — All 5 files should produce 15 rows
-- ============================================================================
-- If the table reads all files: 15 rows (3 per file)
-- If only some files are read: fewer rows

SELECT 'total_rows' AS check_name,
       COUNT(*) AS actual,
       15 AS expected,
       CASE WHEN COUNT(*) = 15 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.books_evolved;


-- ============================================================================
-- 2. BROWSE ALL DATA — See the full union schema
-- ============================================================================

SELECT *
FROM {{zone_name}}.xml.books_evolved
ORDER BY attr_id;


-- ============================================================================
-- 3. ISBN COLUMN (added in file 2) — 3 NULLs from file 1
-- ============================================================================
-- File 1 (bk101-bk103) lacks isbn → NULL
-- Files 2-5 (bk104-bk115) have isbn → NOT NULL

SELECT 'isbn_nulls' AS check_name,
       COUNT(*) FILTER (WHERE isbn IS NULL) AS actual,
       3 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE isbn IS NULL) = 3
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.books_evolved;


-- ============================================================================
-- 4. LANGUAGE COLUMN (added in file 2) — 3 NULLs from file 1
-- ============================================================================

SELECT 'language_nulls' AS check_name,
       COUNT(*) FILTER (WHERE language IS NULL) AS actual,
       3 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE language IS NULL) = 3
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.books_evolved;


-- ============================================================================
-- 5. PUBLISHER COLUMN (added in file 3) — 6 NULLs from files 1-2
-- ============================================================================

SELECT 'publisher_nulls' AS check_name,
       COUNT(*) FILTER (WHERE publisher IS NULL) AS actual,
       6 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE publisher IS NULL) = 6
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.books_evolved;


-- ============================================================================
-- 6. RATING COLUMN (added in file 3) — 6 NULLs from files 1-2
-- ============================================================================

SELECT 'rating_nulls' AS check_name,
       COUNT(*) FILTER (WHERE rating IS NULL) AS actual,
       6 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE rating IS NULL) = 6
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.books_evolved;


-- ============================================================================
-- 7. DESCRIPTION COLUMN (dropped in file 4) — 6 NULLs from files 4-5
-- ============================================================================
-- Files 1-3 (bk101-bk109) have description
-- Files 4-5 (bk110-bk115) dropped description → NULL

SELECT 'description_nulls' AS check_name,
       COUNT(*) FILTER (WHERE description IS NULL) AS actual,
       6 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE description IS NULL) = 6
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.books_evolved;


-- ============================================================================
-- 8. EDITION COLUMN (added in file 4) — 9 NULLs from files 1-3
-- ============================================================================

SELECT 'edition_nulls' AS check_name,
       COUNT(*) FILTER (WHERE edition IS NULL) AS actual,
       9 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE edition IS NULL) = 9
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.books_evolved;


-- ============================================================================
-- 9. PAGES COLUMN (added in file 4) — 9 NULLs from files 1-3
-- ============================================================================

SELECT 'pages_nulls' AS check_name,
       COUNT(*) FILTER (WHERE pages IS NULL) AS actual,
       9 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE pages IS NULL) = 9
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.books_evolved;


-- ============================================================================
-- 10. SERIES COLUMN (added in file 5) — 12 NULLs from files 1-4
-- ============================================================================

SELECT 'series_nulls' AS check_name,
       COUNT(*) FILTER (WHERE series IS NULL) AS actual,
       12 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE series IS NULL) = 12
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.books_evolved;


-- ============================================================================
-- 11. FORMAT ATTRIBUTE (added in file 5) — 12 NULLs from files 1-4
-- ============================================================================
-- The @format attribute on <book> is extracted as column attr_format.

SELECT 'attr_format_nulls' AS check_name,
       COUNT(*) FILTER (WHERE attr_format IS NULL) AS actual,
       12 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE attr_format IS NULL) = 12
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.books_evolved;


-- ============================================================================
-- 12. VALUE SPOT-CHECK — Verify specific books have correct data
-- ============================================================================

SELECT attr_id, author, title, genre, price
FROM {{zone_name}}.xml.books_evolved
WHERE attr_id IN ('bk101', 'bk108', 'bk113')
ORDER BY attr_id;


-- ============================================================================
-- 13. GENRE DISTRIBUTION — All 15 books should have a genre
-- ============================================================================

SELECT genre, COUNT(*) AS book_count
FROM {{zone_name}}.xml.books_evolved
GROUP BY genre
ORDER BY book_count DESC;


-- ============================================================================
-- 14. SUMMARY — All Schema Evolution Checks
-- ============================================================================

SELECT 'total_rows' AS check_name, CASE WHEN COUNT(*) = 15 THEN 'PASS' ELSE 'FAIL' END AS result FROM {{zone_name}}.xml.books_evolved
UNION ALL
SELECT 'isbn_nulls', CASE WHEN COUNT(*) FILTER (WHERE isbn IS NULL) = 3 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.xml.books_evolved
UNION ALL
SELECT 'language_nulls', CASE WHEN COUNT(*) FILTER (WHERE language IS NULL) = 3 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.xml.books_evolved
UNION ALL
SELECT 'publisher_nulls', CASE WHEN COUNT(*) FILTER (WHERE publisher IS NULL) = 6 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.xml.books_evolved
UNION ALL
SELECT 'rating_nulls', CASE WHEN COUNT(*) FILTER (WHERE rating IS NULL) = 6 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.xml.books_evolved
UNION ALL
SELECT 'description_nulls', CASE WHEN COUNT(*) FILTER (WHERE description IS NULL) = 6 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.xml.books_evolved
UNION ALL
SELECT 'edition_nulls', CASE WHEN COUNT(*) FILTER (WHERE edition IS NULL) = 9 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.xml.books_evolved
UNION ALL
SELECT 'pages_nulls', CASE WHEN COUNT(*) FILTER (WHERE pages IS NULL) = 9 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.xml.books_evolved
UNION ALL
SELECT 'series_nulls', CASE WHEN COUNT(*) FILTER (WHERE series IS NULL) = 12 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.xml.books_evolved
UNION ALL
SELECT 'attr_format_nulls', CASE WHEN COUNT(*) FILTER (WHERE attr_format IS NULL) = 12 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.xml.books_evolved
ORDER BY check_name;
