-- ============================================================================
-- Demo: Planetarium APOD Archive — Queries
-- ============================================================================
-- Validates the api_key_query + USING (header.*, query_param.*) path
-- end to end:
--   • 8 rows — one per day in the requested window (2024-12-20..27).
--   • All 8 dates distinct, exactly matching the USING window.
--   • Christmas Day 2024 present (specific date assertion).
--   • Every row has a non-empty title, explanation, and media_url —
--     NASA's APOD API guarantees these fields on every entry.
--   • media_type is always 'image' or 'video' (the only two values
--     NASA's APOD service emits).
--   • Every media_url starts with http — a crude SSRF sanity check.
--   • Bronze ↔ silver promotion preserved every row.
--
-- Stability: the APOD archive is historical (the images were chosen and
-- the explanations written on the specific days in 2024), so the exact
-- 8-row count, the exact date range, and Christmas-Day presence are
-- stable invariants forever. Specific titles/explanations/urls are not
-- asserted since the demo could someday point at a different window.
-- ============================================================================

-- ============================================================================
-- Query 1: Window Row Count — 8 days in USING (..start_date, ..end_date)
-- ============================================================================
-- start_date=2024-12-20 + end_date=2024-12-27 is an inclusive 8-day
-- window. NASA returns one row per calendar day. Any count other than 8
-- means either the query_param.start_date / end_date USING overrides
-- didn't make it to the URL, or the flatten dropped an entry.

ASSERT ROW_COUNT = 1
ASSERT VALUE apod_count = 8
SELECT COUNT(*) AS apod_count
FROM {{zone_name}}.space_imagery.apod_bronze;

-- ============================================================================
-- Query 2: Date Range — bounded correctly by USING
-- ============================================================================
-- All 8 dates distinct. MIN must be 2024-12-20 (start_date inclusive);
-- MAX must be 2024-12-27 (end_date inclusive). NASA returns the window
-- literally — no off-by-one.

ASSERT ROW_COUNT = 1
ASSERT VALUE distinct_dates = 8
ASSERT VALUE min_date = '2024-12-20'
ASSERT VALUE max_date = '2024-12-27'
SELECT
    COUNT(DISTINCT apod_date) AS distinct_dates,
    MIN(apod_date)            AS min_date,
    MAX(apod_date)            AS max_date
FROM {{zone_name}}.space_imagery.apod_bronze;

-- ============================================================================
-- Query 3: Christmas Day 2024 Present — fixed anchor date
-- ============================================================================
-- APOD publishes every single day without gaps (15+ year streak).
-- Asserting the Christmas-Day row exists in silver confirms the
-- DATE cast round-tripped cleanly from bronze's YYYY-MM-DD string.

ASSERT ROW_COUNT = 1
ASSERT VALUE has_christmas = 1
SELECT
    SUM(CASE WHEN apod_date = DATE '2024-12-25' THEN 1 ELSE 0 END) AS has_christmas
FROM {{zone_name}}.space_imagery.apod_silver;

-- ============================================================================
-- Query 4: Required Fields Non-Empty
-- ============================================================================
-- NASA APOD guarantees title, explanation, url on every entry. A count
-- other than 8 here means the JSON flatten lost a field binding.

ASSERT ROW_COUNT = 1
ASSERT VALUE non_null_titles = 8
ASSERT VALUE non_null_explanations = 8
ASSERT VALUE non_null_media_urls = 8
SELECT
    SUM(CASE WHEN title IS NOT NULL AND LENGTH(title) > 0             THEN 1 ELSE 0 END) AS non_null_titles,
    SUM(CASE WHEN explanation IS NOT NULL AND LENGTH(explanation) > 0 THEN 1 ELSE 0 END) AS non_null_explanations,
    SUM(CASE WHEN media_url IS NOT NULL AND LENGTH(media_url) > 0     THEN 1 ELSE 0 END) AS non_null_media_urls
FROM {{zone_name}}.space_imagery.apod_bronze;

-- ============================================================================
-- Query 5: Media Type Enum — only 'image' or 'video'
-- ============================================================================
-- APOD's media_type is a closed enum. The planetarium's exhibit player
-- switches rendering logic based on this flag; an unknown value would
-- break the display pipeline downstream.

ASSERT ROW_COUNT = 1
ASSERT VALUE valid_media_types = 8
SELECT
    SUM(CASE WHEN media_type IN ('image', 'video') THEN 1 ELSE 0 END) AS valid_media_types
FROM {{zone_name}}.space_imagery.apod_silver;

-- ============================================================================
-- Query 6: URL Scheme Sanity — every media_url is an http(s) URL
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE non_http_urls = 0
SELECT
    SUM(CASE WHEN media_url NOT LIKE 'http%' THEN 1 ELSE 0 END) AS non_http_urls
FROM {{zone_name}}.space_imagery.apod_silver;

-- ============================================================================
-- Query 7: Silver Delta History — v0 schema + v1 INSERT
-- ============================================================================

ASSERT ROW_COUNT >= 2
DESCRIBE HISTORY {{zone_name}}.space_imagery.apod_silver;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_apods = 8
ASSERT VALUE distinct_dates = 8
ASSERT VALUE christmas_present = 1
ASSERT VALUE all_media_types_ok = 1
ASSERT VALUE all_urls_http = 1
ASSERT VALUE bronze_silver_parity = 1
SELECT
    COUNT(*)                                                                                     AS total_apods,
    COUNT(DISTINCT apod_date)                                                                    AS distinct_dates,
    SUM(CASE WHEN apod_date = DATE '2024-12-25' THEN 1 ELSE 0 END)                               AS christmas_present,
    CASE WHEN SUM(CASE WHEN media_type NOT IN ('image','video') THEN 1 ELSE 0 END) = 0
         THEN 1 ELSE 0 END                                                                       AS all_media_types_ok,
    CASE WHEN SUM(CASE WHEN media_url NOT LIKE 'http%' THEN 1 ELSE 0 END) = 0
         THEN 1 ELSE 0 END                                                                       AS all_urls_http,
    CASE WHEN COUNT(*) = (SELECT COUNT(*) FROM {{zone_name}}.space_imagery.apod_bronze)
         THEN 1 ELSE 0 END                                                                       AS bronze_silver_parity
FROM {{zone_name}}.space_imagery.apod_silver;
