-- ============================================================================
-- Demo: arXiv AI Research Feed — Queries
-- ============================================================================
-- Validates the XML response path end to end:
--   • Exactly 50 rows (max_results cap).
--   • All paper_urls are canonical http://arxiv.org/abs/... links.
--   • Every title, summary, and author_names field is non-empty — the
--     XML flatten did not drop required elements.
--   • published_at matches Atom's RFC-3339 shape (`YYYY-MM-DDTHH:MM:SSZ`).
--   • At least one paper has multiple authors (the "," separator from
--     join_comma is visible in author_names) — proving the repeat
--     handling fired and wasn't dropped.
--   • Bronze ↔ silver promotion preserved every row.
--
-- Upstream stability: arXiv's Atom API has been stable for 15+ years and
-- the cs.AI category has hundreds of new submissions per week, so
-- max_results=50 is always saturated. Row count is exact-asserted.
-- Specific titles / authors drift daily and are never asserted.
-- ============================================================================

-- ============================================================================
-- Query 1: Full-Corpus Row Count — 50 entries returned
-- ============================================================================
-- max_results=50 in the URL pins the response. If bronze shows anything
-- else, either the XML flatten dropped an entry (row_xpath mis-matched
-- the Atom namespace), or arXiv returned fewer than max_results (which
-- it won't for a busy category).

ASSERT ROW_COUNT = 1
ASSERT VALUE paper_count = 50
SELECT COUNT(*) AS paper_count
FROM {{zone_name}}.research_intel.arxiv_bronze;

-- ============================================================================
-- Query 2: Paper-URL Distinctness — 50 unique IDs
-- ============================================================================
-- Every arXiv paper has a unique `http://arxiv.org/abs/YYMM.NNNNN[vN]`
-- identifier. COUNT(DISTINCT) = 50 proves no duplicate entries landed.

ASSERT ROW_COUNT = 1
ASSERT VALUE distinct_papers = 50
SELECT COUNT(DISTINCT paper_url) AS distinct_papers
FROM {{zone_name}}.research_intel.arxiv_bronze;

-- ============================================================================
-- Query 3: XML Flatten Field Coverage — every required field present
-- ============================================================================
-- The arXiv API guarantees title / summary / author / id on every entry.
-- If the flatten mis-mapped a path (e.g., lost the namespace binding),
-- one of these counts would drop.

ASSERT ROW_COUNT = 1
ASSERT VALUE arxiv_urls = 50
ASSERT VALUE non_null_titles = 50
ASSERT VALUE non_null_summaries = 50
ASSERT VALUE non_null_authors = 50
SELECT
    SUM(CASE WHEN paper_url LIKE 'http://arxiv.org/abs/%' THEN 1 ELSE 0 END) AS arxiv_urls,
    SUM(CASE WHEN title IS NOT NULL AND LENGTH(title) > 0 THEN 1 ELSE 0 END) AS non_null_titles,
    SUM(CASE WHEN summary IS NOT NULL AND LENGTH(summary) > 0 THEN 1 ELSE 0 END) AS non_null_summaries,
    SUM(CASE WHEN author_names IS NOT NULL AND LENGTH(author_names) > 0 THEN 1 ELSE 0 END) AS non_null_authors
FROM {{zone_name}}.research_intel.arxiv_silver;

-- ============================================================================
-- Query 4: Timestamp Shape — Atom RFC-3339
-- ============================================================================
-- Atom `<published>` and `<updated>` serialize as `YYYY-MM-DDTHH:MM:SSZ`.
-- LIKE with `_` placeholders asserts the shape without assuming any
-- specific year/month. If arXiv ever switched to a different timezone
-- format this would flip — a real regression worth catching.

ASSERT ROW_COUNT = 1
ASSERT VALUE iso_published = 50
SELECT
    SUM(CASE WHEN published_at LIKE '20__-__-__T__:__:__Z' THEN 1 ELSE 0 END) AS iso_published
FROM {{zone_name}}.research_intel.arxiv_bronze;

-- ============================================================================
-- Query 5: join_comma repeat-handling fired at least once
-- ============================================================================
-- cs.AI papers routinely have 2-8 co-authors. Asserting that at least
-- one row's author_names contains a comma proves the XML flatten's
-- `default_repeat_handling = "join_comma"` activated — a regression
-- here would surface as every author_names being a single name.

ASSERT ROW_COUNT = 1
ASSERT VALUE multi_author_papers_any = 1
SELECT
    CASE WHEN SUM(CASE WHEN author_names LIKE '%,%' THEN 1 ELSE 0 END) > 0
         THEN 1 ELSE 0 END AS multi_author_papers_any
FROM {{zone_name}}.research_intel.arxiv_silver;

-- ============================================================================
-- Query 6: Bronze ↔ Silver Parity
-- ============================================================================
-- The bronze→silver promotion in setup copies bronze verbatim. Row
-- counts must match; the delta must be zero.

ASSERT ROW_COUNT = 1
ASSERT VALUE silver_count = 50
ASSERT VALUE bronze_silver_delta = 0
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.research_intel.arxiv_silver) AS silver_count,
    (SELECT COUNT(*) FROM {{zone_name}}.research_intel.arxiv_bronze)
        - (SELECT COUNT(*) FROM {{zone_name}}.research_intel.arxiv_silver) AS bronze_silver_delta;

-- ============================================================================
-- Query 7: Silver Delta History — v0 schema + v1 INSERT
-- ============================================================================

ASSERT ROW_COUNT >= 2
DESCRIBE HISTORY {{zone_name}}.research_intel.arxiv_silver;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting: row count, distinct URLs, every URL on arxiv.org, at
-- least one multi-author row, every title non-empty, every published_at
-- is RFC-3339 shaped.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_papers = 50
ASSERT VALUE distinct_urls = 50
ASSERT VALUE arxiv_url_pct = 1
ASSERT VALUE has_multi_author_row = 1
ASSERT VALUE every_title_nonempty = 1
ASSERT VALUE iso_published_pct = 1
SELECT
    COUNT(*)                                                                                   AS total_papers,
    COUNT(DISTINCT paper_url)                                                                  AS distinct_urls,
    CASE WHEN SUM(CASE WHEN paper_url NOT LIKE 'http://arxiv.org/abs/%' THEN 1 ELSE 0 END) = 0
         THEN 1 ELSE 0 END                                                                     AS arxiv_url_pct,
    CASE WHEN SUM(CASE WHEN author_names LIKE '%,%' THEN 1 ELSE 0 END) > 0
         THEN 1 ELSE 0 END                                                                     AS has_multi_author_row,
    CASE WHEN SUM(CASE WHEN LENGTH(title) = 0 OR title IS NULL THEN 1 ELSE 0 END) = 0
         THEN 1 ELSE 0 END                                                                     AS every_title_nonempty,
    CASE WHEN SUM(CASE WHEN published_at NOT LIKE '20__-__-__T__:__:__Z' THEN 1 ELSE 0 END) = 0
         THEN 1 ELSE 0 END                                                                     AS iso_published_pct
FROM {{zone_name}}.research_intel.arxiv_silver;
