-- ============================================================================
-- Delta Z-ORDER & Transaction Management -- Educational Queries
-- ============================================================================
-- WHAT: OPTIMIZE ZORDER is an atomic transaction that rewrites data files for
--       multi-dimensional co-location while maintaining read consistency.
-- WHY:  When batch inserts create many small files, queries must scan them all.
--       OPTIMIZE compacts and Z-orders the data. The atomic transaction ensures
--       readers never see a half-optimized state during the rewrite.
-- HOW:  OPTIMIZE reads all current files, sorts data using a Z-curve on the
--       specified columns, writes new optimally-ordered files, and atomically
--       commits: add new files + remove old files in ONE transaction log entry.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Pre-ZORDER state — fragmented across 3 batch files
-- ============================================================================
-- The data was loaded in 3 separate INSERTs: 40 pageviews, 30 clicks, and
-- 30 conversions. Each INSERT created its own data file(s). Queries filtering
-- by event_type or country must scan ALL files because there is no co-location.
-- This SELECT shows the current distribution before optimization.

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 3
SELECT event_type, COUNT(*) AS events,
       COUNT(DISTINCT country) AS countries,
       ROUND(AVG(duration_ms), 0) AS avg_duration_ms
FROM {{zone_name}}.delta_demos.web_analytics
GROUP BY event_type
ORDER BY event_type;


-- ============================================================================
-- ACTION: OPTIMIZE with Z-ORDER BY (event_type, country)
-- ============================================================================
-- This is the key operation. OPTIMIZE rewrites all data files, sorting rows
-- using a Z-curve on (event_type, country). The result is that rows with
-- the same event_type and country are physically co-located on disk.
--
-- This is an ATOMIC transaction: the Delta log records the simultaneous
-- addition of new Z-ordered files and removal of old batch files in a single
-- commit. Any reader querying during the OPTIMIZE sees EITHER the old files
-- OR the new files, never a mix (snapshot isolation).

OPTIMIZE {{zone_name}}.delta_demos.web_analytics
ZORDER BY (event_type, country);


-- ============================================================================
-- EXPLORE: Post-ZORDER state — same data, optimized layout
-- ============================================================================
-- The data content is identical before and after OPTIMIZE — only the physical
-- file layout has changed. This query confirms all 100 rows, 40 sessions, and
-- 40 users are intact. Z-ORDER is a metadata/physical operation, not a logical
-- one.

ASSERT VALUE total_events = 100
ASSERT VALUE unique_sessions = 40
ASSERT VALUE unique_users = 40
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS total_events,
       COUNT(DISTINCT session_id) AS unique_sessions,
       COUNT(DISTINCT user_id) AS unique_users
FROM {{zone_name}}.delta_demos.web_analytics;


-- ============================================================================
-- EXPLORE: Country-level analysis — a Z-ORDER dimension
-- ============================================================================
-- Country is the second Z-ORDER column. Queries filtering by country benefit
-- from data skipping because the Z-curve groups similar country values together
-- across files. US has the most events, reflecting real-world traffic patterns.

ASSERT VALUE total_events = 40 WHERE country = 'US'
ASSERT ROW_COUNT = 10
SELECT country, COUNT(*) AS total_events,
       COUNT(*) FILTER (WHERE event_type = 'pageview') AS pageviews,
       COUNT(*) FILTER (WHERE event_type = 'click') AS clicks,
       COUNT(*) FILTER (WHERE event_type = 'conversion') AS conversions
FROM {{zone_name}}.delta_demos.web_analytics
GROUP BY country
ORDER BY total_events DESC
LIMIT 10;


-- ============================================================================
-- ACTION: UPDATE — mark short pageview sessions as "bounced"
-- ============================================================================
-- Sessions under 1000ms on pageview events are considered bounces. This UPDATE
-- is a SEPARATE transaction from the OPTIMIZE — it creates new data files via
-- copy-on-write. Importantly, these new files are NOT Z-ordered (they follow
-- the update pattern), illustrating why periodic re-optimization is needed as
-- DML operations degrade the Z-ORDER layout over time.

ASSERT ROW_COUNT = 4
UPDATE {{zone_name}}.delta_demos.web_analytics
SET page_url = '/bounced' || page_url
WHERE event_type = 'pageview' AND duration_ms < 1000;


-- ============================================================================
-- EXPLORE: Bounced sessions after the UPDATE
-- ============================================================================
-- The UPDATE marked 4 short pageview sessions as bounced by prefixing their
-- page_url with '/bounced'. This shows post-optimize DML behavior: the update
-- is its own atomic transaction layered on top of the Z-ordered files.

ASSERT ROW_COUNT = 4
SELECT id, session_id, page_url, duration_ms, country
FROM {{zone_name}}.delta_demos.web_analytics
WHERE page_url LIKE '/bounced%'
ORDER BY duration_ms;


-- ============================================================================
-- LEARN: The user journey — pageview to click to conversion
-- ============================================================================
-- Z-ORDER by event_type co-locates all pageviews together, all clicks together,
-- etc. This makes funnel analysis queries efficient because scanning just the
-- conversion events requires reading fewer files. Here we trace complete user
-- journeys where users went from pageview through click to conversion.

ASSERT VALUE country = 'US' WHERE user_id = 'U001'
ASSERT ROW_COUNT = 10
SELECT w1.user_id,
       w1.page_url AS viewed_page,
       w2.page_url AS clicked_item,
       w3.page_url AS converted_at,
       w1.country
FROM {{zone_name}}.delta_demos.web_analytics w1
JOIN {{zone_name}}.delta_demos.web_analytics w2 ON w1.user_id = w2.user_id AND w2.event_type = 'click'
JOIN {{zone_name}}.delta_demos.web_analytics w3 ON w1.user_id = w3.user_id AND w3.event_type = 'conversion'
WHERE w1.event_type = 'pageview'
  AND w3.page_url LIKE '/checkout/complete'
  AND w1.id < w2.id AND w2.id < w3.id
ORDER BY w1.user_id
LIMIT 10;


-- ============================================================================
-- LEARN: Date-based analysis across the Z-ordered layout
-- ============================================================================
-- Although event_date is NOT a Z-ORDER column here, the data still supports
-- date queries. The difference is that date filters cannot leverage Z-ORDER
-- data skipping. This is a design choice — you Z-ORDER on the columns that
-- appear most frequently in WHERE clauses (event_type and country here).

ASSERT ROW_COUNT = 24
SELECT event_date, event_type, COUNT(*) AS events,
       ROUND(AVG(duration_ms), 0) AS avg_duration
FROM {{zone_name}}.delta_demos.web_analytics
GROUP BY event_date, event_type
ORDER BY event_date, event_type;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count is 100
ASSERT ROW_COUNT = 100
SELECT * FROM {{zone_name}}.delta_demos.web_analytics;

-- Verify 40 pageview events
ASSERT VALUE pageview_count = 40
SELECT COUNT(*) AS pageview_count FROM {{zone_name}}.delta_demos.web_analytics WHERE event_type = 'pageview';

-- Verify 30 click events
ASSERT VALUE click_count = 30
SELECT COUNT(*) AS click_count FROM {{zone_name}}.delta_demos.web_analytics WHERE event_type = 'click';

-- Verify 30 conversion events
ASSERT VALUE conversion_count = 30
SELECT COUNT(*) AS conversion_count FROM {{zone_name}}.delta_demos.web_analytics WHERE event_type = 'conversion';

-- Verify 40 US events
ASSERT VALUE us_event_count = 40
SELECT COUNT(*) AS us_event_count FROM {{zone_name}}.delta_demos.web_analytics WHERE country = 'US';

-- Verify 4 bounced sessions
ASSERT VALUE bounced_count = 4
SELECT COUNT(*) AS bounced_count FROM {{zone_name}}.delta_demos.web_analytics WHERE page_url LIKE '/bounced%';

-- Verify bounced URL prefix for id=31
ASSERT VALUE page_url = '/bounced/home'
SELECT page_url FROM {{zone_name}}.delta_demos.web_analytics WHERE id = 31;

-- Verify 15 distinct countries
ASSERT VALUE distinct_countries = 15
SELECT COUNT(DISTINCT country) AS distinct_countries FROM {{zone_name}}.delta_demos.web_analytics;
