-- ============================================================================
-- Delta Dynamic Partition Pruning — Educational Queries
-- ============================================================================
-- WHAT: Dynamic partition pruning skips entire partition directories during
--       query execution when the query engine can determine from a JOIN or
--       subquery that certain partition values will produce no matching rows.
-- WHY:  In star-schema analytics, fact tables are large and partitioned, while
--       dimension tables are small. Without pruning, a JOIN scans ALL fact
--       partitions even when the dimension filter selects only a subset.
-- HOW:  The engine first evaluates the dimension-side filter to determine
--       which partition values are needed, then only opens data files in
--       those partition directories — skipping the rest entirely.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Inspect the Partitioned Fact Table
-- ============================================================================
-- The sales_facts table is PARTITIONED BY (region). On disk, Delta creates
-- separate directories for each partition value:
--   region=us-east/
--   region=us-west/
--   region=eu-west/
--   region=ap-south/
-- Each directory contains only the Parquet files for that region's rows.

ASSERT VALUE total_sales = 18415.5 WHERE region = 'us-east'
ASSERT VALUE total_sales = 21380.0 WHERE region = 'us-west'
ASSERT VALUE total_sales = 18405.0 WHERE region = 'eu-west'
ASSERT VALUE total_sales = 13293.0 WHERE region = 'ap-south'
ASSERT ROW_COUNT = 4
SELECT region, COUNT(*) AS row_count,
       ROUND(SUM(amount), 2) AS total_sales,
       SUM(qty) AS total_qty
FROM {{zone_name}}.delta_demos.sales_facts
GROUP BY region
ORDER BY region;


-- ============================================================================
-- EXPLORE: Inspect the Dimension Table
-- ============================================================================
-- The region_targets table acts as a dimension/lookup table with one row per
-- region, containing target amounts and quantities. It is small and not
-- partitioned — this is the table that drives dynamic partition pruning
-- when joined with the fact table and filtered.

ASSERT ROW_COUNT = 4
SELECT region, target_amount, target_qty
FROM {{zone_name}}.delta_demos.region_targets
ORDER BY target_amount DESC;


-- ============================================================================
-- LEARN: Dynamic Partition Pruning in Action
-- ============================================================================
-- When we JOIN sales_facts with region_targets and filter on
-- target_amount > 50000, only us-east (75000) and us-west (60000) match.
--
-- With dynamic partition pruning, the engine:
--   1. Evaluates the dimension filter: target_amount > 50000
--   2. Determines matching regions: us-east, us-west
--   3. Opens ONLY the region=us-east/ and region=us-west/ partition dirs
--   4. Skips region=eu-west/ and region=ap-south/ entirely
--
-- This means only 28 rows are scanned instead of all 55 — a 49% reduction.

-- Verify only us-east and us-west match target_amount > 50000
ASSERT VALUE target_amount = 75000 WHERE region = 'us-east'
ASSERT VALUE target_amount = 60000 WHERE region = 'us-west'
ASSERT VALUE fact_rows = 14 WHERE region = 'us-east'
ASSERT VALUE actual_sales = 18415.5 WHERE region = 'us-east'
ASSERT VALUE fact_rows = 14 WHERE region = 'us-west'
ASSERT VALUE actual_sales = 21380.0 WHERE region = 'us-west'
ASSERT ROW_COUNT = 2
SELECT s.region, t.target_amount,
       COUNT(*) AS fact_rows,
       ROUND(SUM(s.amount), 2) AS actual_sales,
       ROUND(SUM(s.amount) / t.target_amount * 100, 1) AS pct_of_target
FROM {{zone_name}}.delta_demos.sales_facts s
JOIN {{zone_name}}.delta_demos.region_targets t
    ON s.region = t.region
WHERE t.target_amount > 50000
GROUP BY s.region, t.target_amount
ORDER BY s.region;


-- ============================================================================
-- LEARN: Partition-Scoped DML Operations
-- ============================================================================
-- The UPDATE that discounted ap-south amounts by 10% only rewrote files in
-- the region=ap-south/ partition directory. Files in other partitions were
-- untouched. This is a major benefit of partitioning — DML operations
-- scoped to a single partition value avoid rewriting the entire table.
--
-- Let's verify the ap-south discount was applied and other regions are
-- unaffected by checking a known value from each region.

-- Verify ap-south discount was applied (id=46 should have discounted price)
ASSERT VALUE price_status = 'Discounted (x0.90)' WHERE region = 'ap-south'
ASSERT VALUE amount = 468.0 WHERE id = 46
ASSERT ROW_COUNT = 4
SELECT id, region, amount,
       CASE
           WHEN region = 'ap-south' THEN 'Discounted (x0.90)'
           ELSE 'Original price'
       END AS price_status
FROM {{zone_name}}.delta_demos.sales_facts
WHERE id IN (1, 16, 31, 46)
ORDER BY id;


-- ============================================================================
-- EXPLORE: Quarterly Performance by Channel
-- ============================================================================
-- Partitioning by region optimizes region-based queries, but queries on
-- other dimensions (quarter, channel) still scan all partitions. Choosing
-- the right partition column depends on your most common query patterns.

ASSERT VALUE total_amount = 11060.0 WHERE quarter = 'Q1-2024' AND channel = 'wholesale'
ASSERT VALUE total_amount = 5913.0 WHERE quarter = 'Q1-2024' AND channel = 'online'
ASSERT ROW_COUNT = 12
SELECT quarter,
       channel,
       COUNT(*) AS sale_count,
       ROUND(SUM(amount), 2) AS total_amount,
       SUM(qty) AS total_qty
FROM {{zone_name}}.delta_demos.sales_facts
GROUP BY quarter, channel
ORDER BY quarter, channel;


-- ============================================================================
-- LEARN: Target Achievement by Region
-- ============================================================================
-- This star-schema query demonstrates the full power of partitioned fact +
-- dimension tables. Each region's actual sales are compared against targets.
-- Dynamic partition pruning means adding a WHERE clause on the dimension
-- (e.g., WHERE t.target_qty > 300) would skip partition directories for
-- regions that do not meet the filter — without scanning their data files.

ASSERT VALUE actual_amount = 18415.5 WHERE region = 'us-east'
ASSERT VALUE actual_amount = 21380.0 WHERE region = 'us-west'
ASSERT VALUE actual_amount = 18405.0 WHERE region = 'eu-west'
ASSERT VALUE actual_amount = 13293.0 WHERE region = 'ap-south'
ASSERT ROW_COUNT = 4
SELECT s.region,
       t.target_amount,
       ROUND(SUM(s.amount), 2) AS actual_amount,
       ROUND(SUM(s.amount) / t.target_amount * 100, 1) AS amount_pct,
       t.target_qty,
       SUM(s.qty) AS actual_qty,
       ROUND(SUM(s.qty) * 100.0 / t.target_qty, 1) AS qty_pct
FROM {{zone_name}}.delta_demos.sales_facts s
JOIN {{zone_name}}.delta_demos.region_targets t
    ON s.region = t.region
GROUP BY s.region, t.target_amount, t.target_qty
ORDER BY s.region;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_row_count: 55 rows in sales_facts
ASSERT ROW_COUNT = 55
SELECT * FROM {{zone_name}}.delta_demos.sales_facts;

-- Verify region_count: 4 distinct regions
ASSERT VALUE cnt = 4
SELECT COUNT(DISTINCT region) AS cnt FROM {{zone_name}}.delta_demos.sales_facts;

-- Verify us_east_count: 14 rows in us-east partition
ASSERT VALUE cnt = 14
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.sales_facts WHERE region = 'us-east';

-- Verify ap_south_discounted: id=46 amount discounted to 468.0
ASSERT VALUE amount = 468.0
SELECT amount FROM {{zone_name}}.delta_demos.sales_facts WHERE id = 46;

-- Verify cancelled_gone: no rows with qty = 0
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.sales_facts WHERE qty = 0;

-- Verify channel_count: 3 distinct channels
ASSERT VALUE cnt = 3
SELECT COUNT(DISTINCT channel) AS cnt FROM {{zone_name}}.delta_demos.sales_facts;

-- Verify quarterly_distribution: 16 rows in Q1-2024
ASSERT VALUE cnt = 16
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.sales_facts WHERE quarter = 'Q1-2024';

-- Verify joined_result: 28 rows when joining with high-target regions
ASSERT VALUE cnt = 28
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.sales_facts s JOIN {{zone_name}}.delta_demos.region_targets t ON s.region = t.region WHERE t.target_amount > 50000;
