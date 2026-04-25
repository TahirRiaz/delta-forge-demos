-- ============================================================================
-- Demo: Planetarium APOD Archive, Queries
-- ============================================================================
-- API demo assertion policy: only ASSERT ROW_COUNT > 0 is used.
--
-- Block ordering note: INVOKE is isolated in its own block. The planner
-- pre-registers external tables across the whole script and JSON
-- registration fails on empty directories, so any block referencing
-- apod_bronze must run after the INVOKE has written files.
-- ============================================================================

-- ============================================================================
-- Block 1: describe the endpoint
-- ============================================================================

DESCRIBE API ENDPOINT {{zone_name}}.nasa_api.apod_archive;

-- ============================================================================
-- Block 2: INVOKE the endpoint (isolated)
-- ============================================================================
-- Single HTTPS GET against api.nasa.gov; the URL's start_date/end_date
-- window returns a JSON array in one response.

INVOKE API ENDPOINT {{zone_name}}.nasa_api.apod_archive;

-- ============================================================================
-- Block 3: per-run audit
-- ============================================================================

SHOW API ENDPOINT RUNS {{zone_name}}.nasa_api.apod_archive LIMIT 5;

-- ============================================================================
-- Block 4: detect bronze schema
-- ============================================================================

DETECT SCHEMA FOR TABLE {{zone_name}}.nasa_api.apod_bronze;

-- ============================================================================
-- Block 5: bronze feed landed
-- ============================================================================

ASSERT ROW_COUNT > 0
SELECT
    apod_date,
    title,
    media_type,
    media_url
FROM {{zone_name}}.nasa_api.apod_bronze
ORDER BY apod_date;

-- ============================================================================
-- Block 6: bronze -> silver promotion
-- ============================================================================

INSERT INTO {{zone_name}}.nasa_api.apod_silver
SELECT
    CAST(apod_date AS DATE) AS apod_date,
    title,
    explanation,
    media_type,
    media_url,
    hd_url,
    service_version,
    copyright_holder
FROM {{zone_name}}.nasa_api.apod_bronze;

-- ============================================================================
-- Block 7: silver curated records
-- ============================================================================

ASSERT ROW_COUNT > 0
SELECT
    apod_date,
    title,
    media_type,
    media_url
FROM {{zone_name}}.nasa_api.apod_silver
ORDER BY apod_date;

-- ============================================================================
-- Block 8: media type distribution
-- ============================================================================

SELECT
    media_type,
    COUNT(*) AS entry_count
FROM {{zone_name}}.nasa_api.apod_silver
GROUP BY media_type
ORDER BY media_type;

-- ============================================================================
-- Block 9: silver Delta history
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.nasa_api.apod_silver;
