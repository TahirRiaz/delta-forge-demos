-- ============================================================================
-- Demo: Blog Moderation Corpus, Queries
-- ============================================================================
-- API demo assertion policy: only ASSERT ROW_COUNT > 0 is used.
--
-- Block ordering note: INVOKE is isolated in its own block with no
-- bronze references because the planner pre-registers external tables
-- and JSON registration fails on empty directories.
-- ============================================================================

-- ============================================================================
-- Block 1: registry inspection
-- ============================================================================

SHOW API ENDPOINTS IN CONNECTION {{zone_name}}.blog_moderation;

-- ============================================================================
-- Block 2: describe the endpoint
-- ============================================================================

DESCRIBE API ENDPOINT {{zone_name}}.blog_moderation.blog_posts;

-- ============================================================================
-- Block 3: INVOKE the endpoint (isolated)
-- ============================================================================
-- Walks _page=1.._page=5 with &_limit=20 each, producing 5 JSON files
-- under the per-run timestamped folder.

INVOKE API ENDPOINT {{zone_name}}.blog_moderation.blog_posts;

-- ============================================================================
-- Block 4: per-run audit
-- ============================================================================

SHOW API ENDPOINT RUNS {{zone_name}}.blog_moderation.blog_posts LIMIT 5;

-- ============================================================================
-- Block 5: detect bronze schema
-- ============================================================================

DETECT SCHEMA FOR TABLE {{zone_name}}.blog_moderation.posts_bronze;

-- ============================================================================
-- Block 6: bronze feed landed
-- ============================================================================

ASSERT ROW_COUNT > 0
SELECT
    post_id,
    author_id,
    title
FROM {{zone_name}}.blog_moderation.posts_bronze
LIMIT 10;

-- ============================================================================
-- Block 7: bronze -> silver promotion
-- ============================================================================

INSERT INTO {{zone_name}}.blog_moderation.posts_silver
SELECT
    CAST(post_id   AS BIGINT)      AS post_id,
    CAST(author_id AS BIGINT)      AS author_id,
    title,
    body,
    CAST(LENGTH(body) AS BIGINT)   AS char_len
FROM {{zone_name}}.blog_moderation.posts_bronze;

-- ============================================================================
-- Block 8: silver curated feed
-- ============================================================================

ASSERT ROW_COUNT > 0
SELECT
    post_id,
    author_id,
    title,
    char_len
FROM {{zone_name}}.blog_moderation.posts_silver
ORDER BY char_len DESC
LIMIT 10;

-- ============================================================================
-- Block 9: author distribution
-- ============================================================================

SELECT
    author_id,
    COUNT(*) AS post_count
FROM {{zone_name}}.blog_moderation.posts_silver
GROUP BY author_id
ORDER BY author_id;

-- ============================================================================
-- Block 10: silver Delta history
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.blog_moderation.posts_silver;
