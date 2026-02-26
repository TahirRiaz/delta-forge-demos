-- ============================================================================
-- XML NYT News RSS Feed Analysis — Verification Queries
-- ============================================================================
-- Each query verifies that namespace handling, repeating element modes,
-- column mappings, and multi-file reading work correctly.
-- ============================================================================


-- ============================================================================
-- 1. TOTAL ARTICLE COUNT — 7 files should produce 231 rows
-- ============================================================================
-- Africa(20) + Americas(31) + AsiaPacific(20) + Europe(20) +
-- MiddleEast(26) + World(57) + news(57) = 231

SELECT 'total_articles' AS check_name,
       COUNT(*) AS actual,
       231 AS expected,
       CASE WHEN COUNT(*) = 231 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.news_articles;


-- ============================================================================
-- 2. BROWSE ARTICLES — See the full schema with friendly column names
-- ============================================================================

SELECT title, author, pubDate, category, thumbnail_url, media_credit
FROM {{zone_name}}.xml.news_articles
ORDER BY pubDate DESC
LIMIT 10;


-- ============================================================================
-- 3. NAMESPACE STRIPPING — dc:creator becomes "author" via column_mappings
-- ============================================================================
-- If namespaces are NOT stripped, the column would be "dc_creator" or similar.
-- The column_mapping renames it to "author".

SELECT 'namespace_author' AS check_name,
       COUNT(*) FILTER (WHERE author IS NOT NULL) AS actual,
       CASE WHEN COUNT(*) FILTER (WHERE author IS NOT NULL) > 200
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.news_articles;


-- ============================================================================
-- 4. REPEATING CATEGORIES (JoinComma) — comma-separated string
-- ============================================================================
-- Most items have multiple <category> elements. With JoinComma they become
-- a single string like "War and Armed Conflicts,Russia,Ukraine".

SELECT 'categories_joined' AS check_name,
       COUNT(*) FILTER (WHERE category LIKE '%,%') AS actual,
       CASE WHEN COUNT(*) FILTER (WHERE category LIKE '%,%') > 150
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.news_articles;


-- ============================================================================
-- 5. MEDIA ATTRIBUTES — thumbnail_url extracted from self-closing element
-- ============================================================================
-- <media:content height="1800" url="https://..." width="1800"/>
-- The @url attribute is extracted and mapped to "thumbnail_url".

SELECT 'thumbnail_extracted' AS check_name,
       COUNT(*) FILTER (WHERE thumbnail_url LIKE 'https://%') AS actual,
       CASE WHEN COUNT(*) FILTER (WHERE thumbnail_url LIKE 'https://%') > 200
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.news_articles;


-- ============================================================================
-- 6. ARTICLES PER REGION — using df_file_name metadata column
-- ============================================================================

SELECT df_file_name AS region_file,
       COUNT(*) AS article_count
FROM {{zone_name}}.xml.news_articles
GROUP BY df_file_name
ORDER BY article_count DESC;


-- ============================================================================
-- 7. EXPLODED CATEGORY COUNT — should be ~2023 rows (one per category)
-- ============================================================================

SELECT 'exploded_categories' AS check_name,
       COUNT(*) AS actual,
       2023 AS expected,
       CASE WHEN COUNT(*) = 2023 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.news_categories;


-- ============================================================================
-- 8. CATEGORY DOMAIN ATTRIBUTE — distinguishes keyword types
-- ============================================================================
-- The @domain attribute on <category> contains URIs like:
--   .../keywords/des     → topic descriptors
--   .../keywords/nyt_per → people
--   .../keywords/nyt_geo → places
--   .../keywords/nyt_org → organizations

SELECT category_type,
       COUNT(*) AS keyword_count
FROM {{zone_name}}.xml.news_categories
WHERE category_type IS NOT NULL
GROUP BY category_type
ORDER BY keyword_count DESC;


-- ============================================================================
-- 9. TOP MENTIONED PEOPLE — from nyt_per category domain
-- ============================================================================

SELECT category AS person,
       COUNT(*) AS mention_count
FROM {{zone_name}}.xml.news_categories
WHERE category_type LIKE '%nyt_per'
GROUP BY category
ORDER BY mention_count DESC
LIMIT 10;


-- ============================================================================
-- 10. TOP GEOGRAPHIC KEYWORDS — from nyt_geo category domain
-- ============================================================================

SELECT category AS location,
       COUNT(*) AS mention_count
FROM {{zone_name}}.xml.news_categories
WHERE category_type LIKE '%nyt_geo'
GROUP BY category
ORDER BY mention_count DESC
LIMIT 10;


-- ============================================================================
-- 11. ARTICLES WITH MOST CATEGORIES — spot-check the join
-- ============================================================================

SELECT title, author,
       LENGTH(category) - LENGTH(REPLACE(category, ',', '')) + 1 AS category_count
FROM {{zone_name}}.xml.news_articles
WHERE category IS NOT NULL
ORDER BY category_count DESC
LIMIT 5;


-- ============================================================================
-- 12. SUMMARY — All checks
-- ============================================================================

SELECT 'total_articles' AS check_name,
       CASE WHEN COUNT(*) = 231 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml.news_articles
UNION ALL
SELECT 'namespace_author',
       CASE WHEN COUNT(*) FILTER (WHERE author IS NOT NULL) > 200
            THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml.news_articles
UNION ALL
SELECT 'categories_joined',
       CASE WHEN COUNT(*) FILTER (WHERE category LIKE '%,%') > 150
            THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml.news_articles
UNION ALL
SELECT 'thumbnail_extracted',
       CASE WHEN COUNT(*) FILTER (WHERE thumbnail_url LIKE 'https://%') > 200
            THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml.news_articles
UNION ALL
SELECT 'exploded_categories',
       CASE WHEN COUNT(*) = 2023 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml.news_categories
ORDER BY check_name;
