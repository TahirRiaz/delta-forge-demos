-- ============================================================================
-- Demo: arXiv AI Research Feed, Queries
-- ============================================================================
-- This file exercises the API endpoint end to end. Registry inspection,
-- INVOKE, run-history audit, schema detection, and the bronze->silver
-- promotion all live here so the user sees in one place how an XML REST
-- endpoint is driven from SQL.
--
-- API demo assertion policy: only ASSERT ROW_COUNT > 0 is used. Live
-- feeds change constantly so exact counts or values are never asserted.
-- The only meaningful check is that the API returned data at all.
-- ============================================================================

-- ============================================================================
-- API surface: inspect, invoke, audit
-- ============================================================================

-- Inspect the endpoint catalog row before invoking.
DESCRIBE API ENDPOINT {{zone_name}}.arxiv_api.cs_ai_latest;

-- Issue the actual HTTPS GET. One request writes one .xml file under a
-- per-run timestamped folder inside the connection storage path.
INVOKE API ENDPOINT {{zone_name}}.arxiv_api.cs_ai_latest;

-- Per-run audit: status, files_written, bytes_written, duration_ms.
SHOW API ENDPOINT RUNS {{zone_name}}.arxiv_api.cs_ai_latest LIMIT 5;

-- Resolve the bronze schema from the freshly written XML file.
DETECT SCHEMA FOR TABLE {{zone_name}}.arxiv_api.arxiv_bronze;

-- ============================================================================
-- Query 1: Bronze raw feed
-- ============================================================================
-- Show the first 10 rows from the landed XML. Each row is one Atom entry
-- with the six mapped columns: paper_url, title, published_at, updated_at,
-- summary, author_names. ROW_COUNT > 0 confirms the API returned data.

ASSERT ROW_COUNT > 0
SELECT
    paper_url,
    title,
    published_at,
    author_names
FROM {{zone_name}}.arxiv_api.arxiv_bronze
LIMIT 10;

-- ============================================================================
-- Query 2: Bronze -> silver promotion
-- ============================================================================
-- Copy the bronze feed into the curated silver Delta table.
-- Silver is the layer downstream digest tools point at.

INSERT INTO {{zone_name}}.arxiv_api.arxiv_silver
SELECT
    paper_url,
    title,
    published_at,
    updated_at,
    summary,
    author_names
FROM {{zone_name}}.arxiv_api.arxiv_bronze;

-- ============================================================================
-- Query 3: Silver curated feed
-- ============================================================================
-- Confirm the promotion landed. Shows the most-recently-published papers.
-- ROW_COUNT > 0 confirms at least one row made it through the INSERT.

ASSERT ROW_COUNT > 0
SELECT
    paper_url,
    title,
    published_at,
    author_names
FROM {{zone_name}}.arxiv_api.arxiv_silver
ORDER BY published_at DESC
LIMIT 10;

-- ============================================================================
-- Query 4: Multi-author papers (join_comma repeat handling)
-- ============================================================================
-- Papers with more than one author have a comma in author_names, proving
-- the xml_flatten_config default_repeat_handling = "join_comma" fired.
-- (No row count assertion: single-author batches are valid.)

SELECT
    title,
    author_names
FROM {{zone_name}}.arxiv_api.arxiv_silver
WHERE author_names LIKE '%,%'
LIMIT 5;

-- ============================================================================
-- Query 5: Timestamp shape check
-- ============================================================================
-- Atom <published> serializes as YYYY-MM-DDTHH:MM:SSZ. Show a few rows
-- to confirm the shape is intact after the XML flatten.

ASSERT ROW_COUNT > 0
SELECT
    title,
    published_at,
    updated_at
FROM {{zone_name}}.arxiv_api.arxiv_silver
ORDER BY published_at DESC
LIMIT 5;

-- ============================================================================
-- Query 6: Silver Delta history
-- ============================================================================
-- The silver table should have at least two versions: v0 (schema creation)
-- and v1 (the INSERT above).

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.arxiv_api.arxiv_silver;
