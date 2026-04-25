-- ============================================================================
-- Demo: Rust Release Catalog, Queries
-- ============================================================================
-- API demo assertion policy: only ASSERT ROW_COUNT > 0 is used.
--
-- Block ordering note: INVOKE is isolated in its own block. The planner
-- pre-registers external tables across the whole script and JSON
-- registration fails on empty directories, so any block referencing
-- rust_releases_bronze must run after the INVOKE has written files.
-- ============================================================================

-- ============================================================================
-- Block 1: describe the endpoint
-- ============================================================================

DESCRIBE API ENDPOINT {{zone_name}}.github_releases.rust_releases;

-- ============================================================================
-- Block 2: INVOKE the endpoint (isolated)
-- ============================================================================
-- Single-page response (per_page=30) writes one page_0001.json under
-- a timestamped per-run folder.

INVOKE API ENDPOINT {{zone_name}}.github_releases.rust_releases;

-- ============================================================================
-- Block 3: per-run audit
-- ============================================================================

SHOW API ENDPOINT RUNS {{zone_name}}.github_releases.rust_releases LIMIT 5;

-- ============================================================================
-- Block 4: detect bronze schema
-- ============================================================================

DETECT SCHEMA FOR TABLE {{zone_name}}.github_releases.rust_releases_bronze;

-- ============================================================================
-- Block 5: bronze feed landed
-- ============================================================================

ASSERT ROW_COUNT > 0
SELECT
    tag_name,
    release_name,
    author_login,
    published_at
FROM {{zone_name}}.github_releases.rust_releases_bronze
LIMIT 10;

-- ============================================================================
-- Block 6: bronze -> silver promotion
-- ============================================================================

INSERT INTO {{zone_name}}.github_releases.rust_releases_silver
SELECT
    release_id,
    tag_name,
    release_name,
    is_draft,
    is_prerelease,
    created_at,
    published_at,
    html_url,
    author_login
FROM {{zone_name}}.github_releases.rust_releases_bronze;

-- ============================================================================
-- Block 7: silver typed releases
-- ============================================================================

ASSERT ROW_COUNT > 0
SELECT
    tag_name,
    release_name,
    is_draft,
    is_prerelease,
    published_at
FROM {{zone_name}}.github_releases.rust_releases_silver
WHERE is_draft = false
  AND is_prerelease = false
ORDER BY published_at DESC
LIMIT 10;

-- ============================================================================
-- Block 8: release overview
-- ============================================================================

SELECT
    COUNT(*)         AS total_releases,
    MIN(published_at) AS oldest_release,
    MAX(published_at) AS newest_release
FROM {{zone_name}}.github_releases.rust_releases_silver;

-- ============================================================================
-- Block 9: silver Delta history
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.github_releases.rust_releases_silver;
