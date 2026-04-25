-- ============================================================================
-- Demo: GitHub Topic Repo Discovery, Queries
-- ============================================================================
-- This file exercises the paginated API endpoint end to end. Inspection,
-- the INVOKE FULL REFRESH that drives the link-header crawl, the per-run
-- audit, schema detection, and the bronze->silver promotion all live
-- here so the user sees the link-header pagination flow from a single file.
--
-- API demo assertion policy: only ASSERT ROW_COUNT > 0 is used. The
-- GitHub search results for any topic change daily as repos gain stars
-- or new repos are created, so exact counts or values are never asserted.
-- ============================================================================

-- ============================================================================
-- API surface, calling the endpoint from SQL
-- ============================================================================

-- Inspect the endpoint catalog row before invoking.
DESCRIBE API ENDPOINT {{zone_name}}.github_search_api.delta_lake_topic;

-- INVOKE ... FULL REFRESH drives the link-header crawl. FULL REFRESH
-- reseeds the watermark state; for this endpoint it signals a clean start
-- so downstream consumers can decide to truncate-and-insert silver.
INVOKE API ENDPOINT {{zone_name}}.github_search_api.delta_lake_topic FULL REFRESH;

-- Per-run audit row.
SHOW API ENDPOINT RUNS {{zone_name}}.github_search_api.delta_lake_topic LIMIT 5;

-- Resolve the bronze schema from the freshly written JSON pages.
DETECT SCHEMA FOR TABLE {{zone_name}}.github_search_api.delta_lake_repos_bronze;

-- Bronze -> silver promotion with typed BIGINT and BOOLEAN columns.
INSERT INTO {{zone_name}}.github_search_api.delta_lake_repos_silver
SELECT
    CAST(repo_id AS BIGINT)      AS repo_id,
    full_name,
    owner_login,
    CAST(stars AS BIGINT)        AS stars,
    CAST(forks AS BIGINT)        AS forks,
    language,
    CAST(is_archived AS BOOLEAN) AS is_archived,
    CAST(is_fork AS BOOLEAN)     AS is_fork,
    html_url
FROM {{zone_name}}.github_search_api.delta_lake_repos_bronze;

-- ============================================================================
-- Query 1: Bronze feed landed
-- ============================================================================
-- ROW_COUNT > 0 confirms the link-header crawl fetched at least one page
-- and the JSON flatten produced rows.

ASSERT ROW_COUNT > 0
SELECT
    repo_id,
    full_name,
    owner_login,
    stars,
    html_url
FROM {{zone_name}}.github_search_api.delta_lake_repos_bronze
LIMIT 10;

-- ============================================================================
-- Query 2: Silver curated repos
-- ============================================================================
-- Show the top repos by star count. Stars is a BIGINT in silver, which
-- enables ORDER BY natively.

ASSERT ROW_COUNT > 0
SELECT
    full_name,
    owner_login,
    stars,
    forks,
    language,
    is_fork
FROM {{zone_name}}.github_search_api.delta_lake_repos_silver
ORDER BY stars DESC
LIMIT 10;

-- ============================================================================
-- Query 3: URL shape check
-- ============================================================================
-- Every html_url should be on github.com. Showing a sample for visual
-- confirmation.

SELECT
    full_name,
    html_url
FROM {{zone_name}}.github_search_api.delta_lake_repos_silver
LIMIT 5;

-- ============================================================================
-- Query 4: Language distribution
-- ============================================================================
-- Show which languages appear in the Delta Lake topic repos.

SELECT
    language,
    COUNT(*) AS repo_count
FROM {{zone_name}}.github_search_api.delta_lake_repos_silver
GROUP BY language
ORDER BY repo_count DESC
LIMIT 10;

-- ============================================================================
-- Query 5: Silver Delta history
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.github_search_api.delta_lake_repos_silver;
