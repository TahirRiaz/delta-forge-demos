-- ============================================================================
-- Demo: Blog Moderation Corpus, Queries
-- ============================================================================
-- This file is where the API endpoint is actually exercised. The whole
-- API surface lives here, registry inspection, the INVOKE that drives
-- the HTTPS fetch, the per-run audit row, schema detection on the
-- landed JSON, and the bronze->silver promotion, followed by the
-- assertions that prove the page-pagination loop walked all 5 pages
-- correctly.
--
-- Validates the full page-pagination path:
--   - Exactly 100 rows (5 pages x 20 per-page) land in bronze.
--   - Post IDs cover the full 1..100 contiguous range (no skip, no dup).
--   - Exactly 10 distinct authors (JSONPlaceholder's fixed 10 users).
--   - Every author has exactly 10 posts (100/10 = 10 per author).
--   - Deterministic post-1 title check, if this changes, upstream
--     JSONPlaceholder itself shipped new content.
--   - Bronze <-> silver parity (the promotion did not lose rows).
--   - DESCRIBE HISTORY shows the two expected silver versions.
-- ============================================================================

-- ============================================================================
-- API surface, calling the endpoint from SQL
-- ============================================================================

-- Registry smoke test: lists every endpoint under the connection. Narrow
-- scoping via IN CONNECTION keeps the result small in shared envs.
SHOW API ENDPOINTS IN CONNECTION {{zone_name}}.blog_moderation;

-- DESCRIBE prints url, http_method, response_format, option count, and
-- the last run status. Useful before an INVOKE to confirm the paginate
-- knobs are what you expect.
DESCRIBE API ENDPOINT {{zone_name}}.blog_moderation.blog_posts;

-- INVOKE is the actual HTTPS call. The engine walks _page=1.._page=5
-- with &_limit=20 each, producing 5 JSON files under the per-run
-- timestamped folder. Each file is an array of 20 post objects.
INVOKE API ENDPOINT {{zone_name}}.blog_moderation.blog_posts;

-- Each INVOKE writes one row into api_endpoint_runs with status,
-- pages_fetched, files_written, bytes_written, watermark deltas. LIMIT 5
-- caps history so repeat demo runs stay readable.
SHOW API ENDPOINT RUNS {{zone_name}}.blog_moderation.blog_posts LIMIT 5;

-- After data lands, detect the bronze schema from the freshly written
-- JSON files so DESCRIBE TABLE returns the resolved columns.
DETECT SCHEMA FOR TABLE {{zone_name}}.content_moderation.posts_bronze;

-- Bronze -> silver promotion: typed columns + computed char_len. This is
-- the medallion lift the moderation team's models query directly.
INSERT INTO {{zone_name}}.content_moderation.posts_silver
SELECT
    CAST(post_id AS BIGINT)      AS post_id,
    CAST(author_id AS BIGINT)    AS author_id,
    title,
    body,
    CAST(LENGTH(body) AS BIGINT) AS char_len
FROM {{zone_name}}.content_moderation.posts_bronze;

-- ============================================================================
-- Query 1: Full-Corpus Row Count, 5 pages x 20 per page = 100
-- ============================================================================
-- If this count is anything other than 100, the pagination loop either
-- stopped early (max_pages too low, or engine halted on empty response)
-- or over-fetched (duplicate pages written).

ASSERT ROW_COUNT = 1
ASSERT VALUE post_count = 100
SELECT COUNT(*) AS post_count
FROM {{zone_name}}.content_moderation.posts_bronze;

-- ============================================================================
-- Query 2: Post-ID Contiguity, 1..100 with no gaps or duplicates
-- ============================================================================
-- JSONPlaceholder's /posts is a 100-row contiguous sequence. If the
-- flatten dropped rows (max gap), or the pagination double-wrote a
-- page (min distinct < 100), this catches it.

ASSERT ROW_COUNT = 1
ASSERT VALUE distinct_post_ids = 100
ASSERT VALUE min_post_id = 1
ASSERT VALUE max_post_id = 100
SELECT
    COUNT(DISTINCT CAST(post_id AS BIGINT)) AS distinct_post_ids,
    MIN(CAST(post_id AS BIGINT))            AS min_post_id,
    MAX(CAST(post_id AS BIGINT))            AS max_post_id
FROM {{zone_name}}.content_moderation.posts_bronze;

-- ============================================================================
-- Query 3: Author Coverage, JSONPlaceholder's fixed 10 users
-- ============================================================================
-- Every post is authored by user 1..10. Asserting COUNT(DISTINCT) = 10
-- proves the flatten preserved the userId field (mapped to author_id)
-- across every landed page.

ASSERT ROW_COUNT = 1
ASSERT VALUE distinct_authors = 10
SELECT COUNT(DISTINCT author_id) AS distinct_authors
FROM {{zone_name}}.content_moderation.posts_bronze;

-- ============================================================================
-- Query 4: Even Author Distribution, 10 posts per user
-- ============================================================================
-- Each of the 10 fake users authored exactly 10 posts (total 100). Spot-
-- checking authors 1, 5, 10 is enough to catch any GROUP BY drift.

ASSERT ROW_COUNT = 10
ASSERT VALUE posts_per_author = 10 WHERE author_id = 1
ASSERT VALUE posts_per_author = 10 WHERE author_id = 5
ASSERT VALUE posts_per_author = 10 WHERE author_id = 10
SELECT author_id, COUNT(*) AS posts_per_author
FROM {{zone_name}}.content_moderation.posts_bronze
GROUP BY author_id
ORDER BY author_id;

-- ============================================================================
-- Query 5: Post-1 Title Fingerprint, deterministic upstream content
-- ============================================================================
-- JSONPlaceholder's post id=1 has been serving the same Latin-filler
-- title for years. Asserting the exact string proves the whole chain:
-- HTTPS fetch -> per-page file write -> json_flatten_config mapping ->
-- DETECT SCHEMA column resolution. If the string changes, either
-- upstream rotated its fixtures or the flatten mis-mapped the $.title
-- path.

ASSERT ROW_COUNT = 1
ASSERT VALUE post1_title = 'sunt aut facere repellat provident occaecati excepturi optio reprehenderit'
SELECT title AS post1_title
FROM {{zone_name}}.content_moderation.posts_bronze
WHERE post_id = 1;

-- ============================================================================
-- Query 6: Bronze <-> Silver Parity, promotion preserved every row
-- ============================================================================
-- The INSERT INTO ... SELECT above copies bronze into silver with a
-- computed char_len column. Row counts must match exactly; any drift
-- means the promotion lost rows (fewer silver) or double-ran (more
-- silver).

ASSERT ROW_COUNT = 1
ASSERT VALUE silver_count = 100
ASSERT VALUE bronze_silver_delta = 0
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.content_moderation.posts_silver) AS silver_count,
    (SELECT COUNT(*) FROM {{zone_name}}.content_moderation.posts_bronze)
        - (SELECT COUNT(*) FROM {{zone_name}}.content_moderation.posts_silver) AS bronze_silver_delta;

-- ============================================================================
-- Query 7: Silver Delta History, v0 schema + v1 INSERT
-- ============================================================================
-- CREATE DELTA TABLE (v0) + INSERT INTO ... SELECT (v1) means DESCRIBE
-- HISTORY must return at least 2 rows, the foundation for VERSION AS OF
-- rollback if a re-ingest ships bad data.

ASSERT ROW_COUNT >= 2
DESCRIBE HISTORY {{zone_name}}.content_moderation.posts_silver;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- One cross-cutting sanity query covering every invariant the whole
-- pipeline must uphold:
--   - Row count exactly 100 (page pagination reached max_pages)
--   - 10 distinct authors (flatten preserved userId)
--   - ID range 1..100 (no skips / duplicates)
--   - Every body non-empty (no null-on-wire rows)
--   - Author 1 has exactly 10 posts (even distribution spot-check)

ASSERT ROW_COUNT = 1
ASSERT VALUE total_posts = 100
ASSERT VALUE distinct_authors = 10
ASSERT VALUE posts_min_id = 1
ASSERT VALUE posts_max_id = 100
ASSERT VALUE nonempty_bodies = 100
ASSERT VALUE author_one_count = 10
SELECT
    COUNT(*)                                          AS total_posts,
    COUNT(DISTINCT author_id)                         AS distinct_authors,
    MIN(post_id)                                      AS posts_min_id,
    MAX(post_id)                                      AS posts_max_id,
    SUM(CASE WHEN LENGTH(body) > 0 THEN 1 ELSE 0 END) AS nonempty_bodies,
    SUM(CASE WHEN author_id = 1 THEN 1 ELSE 0 END)    AS author_one_count
FROM {{zone_name}}.content_moderation.posts_silver;
