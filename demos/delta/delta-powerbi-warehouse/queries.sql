-- ============================================================================
-- Demo: Pacific Retail Group: Power BI Star Warehouse Benchmark
-- ============================================================================
-- Every assertion is closed-form: derived from the deterministic generation
-- rule in setup.sql, never from an engine round trip. Two runs are
-- bit-identical and any drift is a real regression.
--
-- Queries follow the Power BI usage pattern:
--   Q1-Q2   dim_date integrity + month-end / quarter-end / year-end counts
--   Q3-Q4   dim_store integrity + region slicer breakdown
--   Q5-Q6   dim_product integrity + category_l1 matrix slicer
--   Q7-Q8   dim_customer integrity + loyalty_tier breakdown
--   Q9-Q11  fact_sales integrity + channel + country slicer aggregations
--   Q12     fact_sales x dim_product JOIN (top brands)
--   Q13-Q14 fact_inventory_snapshot integrity + stock_status breakdown
--   Q15-Q16 fact_web_events integrity + event_type funnel breakdown
--   VERIFY  cross-cutting (count, key sum) per table
-- ============================================================================

-- ============================================================================
-- Query 1: dim_date integrity
-- ============================================================================
-- 7,305 rows for 2010-01-01 .. 2029-12-31. SUM(year) is computable but
-- noisy, so we pin a row count and a simple year-count check.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 7305
ASSERT VALUE n_years = 20
ASSERT VALUE min_date_key = 20100101
ASSERT VALUE max_date_key = 20291231
SELECT
    COUNT(*)                    AS n_rows,
    COUNT(DISTINCT year)        AS n_years,
    MIN(date_key)               AS min_date_key,
    MAX(date_key)               AS max_date_key
FROM {{zone_name}}.retail.dim_date;

-- ============================================================================
-- Query 2: dim_date weekend / month-end / quarter-end distribution
-- ============================================================================
-- Closed-form: 20 years of dates. Weekends = 2 days per week. Year-ends = 20.
-- Month-ends = 12 per year * 20 years = 240. Quarter-ends = 4 per year * 20 = 80.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_year_ends = 20
ASSERT VALUE n_month_ends = 240
ASSERT VALUE n_quarter_ends = 80
SELECT
    SUM(CASE WHEN is_year_end    THEN 1 ELSE 0 END) AS n_year_ends,
    SUM(CASE WHEN is_month_end   THEN 1 ELSE 0 END) AS n_month_ends,
    SUM(CASE WHEN is_quarter_end THEN 1 ELSE 0 END) AS n_quarter_ends
FROM {{zone_name}}.retail.dim_date;

-- ============================================================================
-- Query 3: dim_store integrity and slicer-cardinality checks
-- ============================================================================
-- 25,000 rows. SUM(store_id) = 25000*25001/2 = 312_512_500. Region cycles
-- across 5 values so each region holds exactly 5,000 stores.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 25000
ASSERT VALUE sum_store_id = 312512500
ASSERT VALUE n_regions = 5
ASSERT VALUE n_store_types = 5
ASSERT VALUE n_banners = 5
SELECT
    COUNT(*)                    AS n_rows,
    SUM(store_id)               AS sum_store_id,
    COUNT(DISTINCT region)      AS n_regions,
    COUNT(DISTINCT store_type)  AS n_store_types,
    COUNT(DISTINCT banner)      AS n_banners
FROM {{zone_name}}.retail.dim_store;

-- ============================================================================
-- Query 4: dim_store region COUNT (Power BI region slicer view)
-- ============================================================================
-- Each of 5 regions holds exactly 25_000 / 5 = 5_000 stores.

ASSERT ROW_COUNT = 5
ASSERT RESULT SET INCLUDES
    ('APAC',  5000),
    ('EU',    5000),
    ('LATAM', 5000),
    ('MEA',   5000),
    ('NA',    5000)
SELECT region, COUNT(*) AS n_stores
FROM {{zone_name}}.retail.dim_store
GROUP BY region
ORDER BY region;

-- ============================================================================
-- Query 5: dim_product integrity
-- ============================================================================
-- 1,000,000 rows. SUM(product_id) = 1M * 1_000_001 / 2 = 500_000_500_000.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 1000000
ASSERT VALUE sum_product_id = 500000500000
ASSERT VALUE n_brands = 50
ASSERT VALUE n_l1 = 10
ASSERT VALUE n_l2 = 20
ASSERT VALUE n_l3 = 50
ASSERT VALUE n_abc = 4
SELECT
    COUNT(*)                        AS n_rows,
    SUM(product_id)                 AS sum_product_id,
    COUNT(DISTINCT brand)           AS n_brands,
    COUNT(DISTINCT category_l1)     AS n_l1,
    COUNT(DISTINCT category_l2)     AS n_l2,
    COUNT(DISTINCT category_l3)     AS n_l3,
    COUNT(DISTINCT abc_class)       AS n_abc
FROM {{zone_name}}.retail.dim_product;

-- ============================================================================
-- Query 6: dim_product category_l1 distribution (PBI matrix slicer)
-- ============================================================================
-- 10 categories cycle over 1M rows so each holds exactly 100,000 products.

ASSERT ROW_COUNT = 10
ASSERT RESULT SET INCLUDES
    ('Apparel',     100000),
    ('Beauty',      100000),
    ('Books',       100000),
    ('Electronics', 100000),
    ('Grocery',     100000),
    ('Home',        100000),
    ('Office',      100000),
    ('Pet',         100000),
    ('Sports',      100000),
    ('Toys',        100000)
SELECT category_l1, COUNT(*) AS n_products
FROM {{zone_name}}.retail.dim_product
GROUP BY category_l1
ORDER BY category_l1;

-- ============================================================================
-- Query 7: dim_customer integrity
-- ============================================================================
-- 5,000,000 rows. SUM(customer_id) = 5M * 5_000_001 / 2 = 12_500_002_500_000.
-- Loyalty tier cycles over 5 values so each tier holds 1,000,000 customers.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 5000000
ASSERT VALUE sum_customer_id = 12500002500000
ASSERT VALUE n_tiers = 5
ASSERT VALUE n_segments = 5
ASSERT VALUE n_signup_channels = 8
ASSERT VALUE n_countries = 10
SELECT
    COUNT(*)                        AS n_rows,
    SUM(customer_id)                AS sum_customer_id,
    COUNT(DISTINCT loyalty_tier)    AS n_tiers,
    COUNT(DISTINCT segment)         AS n_segments,
    COUNT(DISTINCT signup_channel)  AS n_signup_channels,
    COUNT(DISTINCT country_code)    AS n_countries
FROM {{zone_name}}.retail.dim_customer;

-- ============================================================================
-- Query 8: dim_customer loyalty_tier distribution
-- ============================================================================
-- 5 loyalty tiers x 1,000,000 each.

ASSERT ROW_COUNT = 5
ASSERT RESULT SET INCLUDES
    ('Bronze',   1000000),
    ('Diamond',  1000000),
    ('Gold',     1000000),
    ('Platinum', 1000000),
    ('Silver',   1000000)
SELECT loyalty_tier, COUNT(*) AS n_customers
FROM {{zone_name}}.retail.dim_customer
GROUP BY loyalty_tier
ORDER BY loyalty_tier;

-- ============================================================================
-- Query 9: fact_sales integrity (the headline ODBC scan)
-- ============================================================================
-- 200,000,000 rows.
-- SUM(sale_id)    = 200M * 200_000_001 / 2 = 20_000_000_100_000_000
-- SUM(quantity)   = N + N*9/2 = 11N/2 = 1_100_000_000  (q = 1 + rn%10)
-- COUNT returns   = N / 20 = 10_000_000               (rn % 20 = 0)
-- COUNT giftcard  = N / 20 = 10_000_000               (rn % 20 = 0)
-- Distinct fiscal = 6 (FY2020 .. FY2025; orders span 2020-01-01..2024-12-30
--                      and FY breaks April 1 so FY2020 contains Jan-Mar 2020,
--                      FY2025 contains Apr-Dec 2024.)

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 200000000
ASSERT VALUE sum_sale_id = 20000000100000000
ASSERT VALUE sum_quantity = 1100000000
ASSERT VALUE n_returns = 10000000
ASSERT VALUE n_giftcard = 10000000
ASSERT VALUE n_channels = 5
ASSERT VALUE n_payment_methods = 8
SELECT
    COUNT(*)                                            AS n_rows,
    SUM(sale_id)                                        AS sum_sale_id,
    SUM(quantity)                                       AS sum_quantity,
    SUM(CASE WHEN return_flag THEN 1 ELSE 0 END)        AS n_returns,
    SUM(CASE WHEN gift_card_amount_usd > 0 THEN 1 ELSE 0 END) AS n_giftcard,
    COUNT(DISTINCT sales_channel)                       AS n_channels,
    COUNT(DISTINCT payment_method)                      AS n_payment_methods
FROM {{zone_name}}.retail.fact_sales;

-- ============================================================================
-- Query 10: fact_sales by sales_channel (PBI channel slicer)
-- ============================================================================
-- 5 channels cycle (rn-1)%5 so each channel holds exactly 40,000,000 rows.

ASSERT ROW_COUNT = 5
ASSERT RESULT SET INCLUDES
    ('In-Store',     40000000),
    ('Marketplace',  40000000),
    ('Mobile App',   40000000),
    ('Online',       40000000),
    ('Phone',        40000000)
SELECT sales_channel, COUNT(*) AS n_sales
FROM {{zone_name}}.retail.fact_sales
GROUP BY sales_channel
ORDER BY sales_channel;

-- ============================================================================
-- Query 11: fact_sales by customer_country_code (PBI geography slicer)
-- ============================================================================
-- 10 countries cycle so each country holds exactly 20,000,000 rows.

ASSERT ROW_COUNT = 10
ASSERT RESULT SET INCLUDES
    ('AU', 20000000),
    ('BR', 20000000),
    ('CA', 20000000),
    ('DE', 20000000),
    ('FR', 20000000),
    ('IN', 20000000),
    ('JP', 20000000),
    ('MX', 20000000),
    ('UK', 20000000),
    ('US', 20000000)
SELECT customer_country_code, COUNT(*) AS n_sales
FROM {{zone_name}}.retail.fact_sales
GROUP BY customer_country_code
ORDER BY customer_country_code;

-- ============================================================================
-- Query 12: fact_sales x dim_product JOIN (top product brands by line count)
-- ============================================================================
-- 50 brands cycle over fact_sales so each brand holds exactly 4,000,000
-- denormalized rows. Joining to dim_product where each brand holds
-- 20,000 products: 50 brands x 4M = 200M, distinct join key validates.

ASSERT ROW_COUNT = 50
ASSERT VALUE n_lines = 4000000  WHERE brand = 'Acme'
ASSERT VALUE n_lines = 4000000  WHERE brand = 'Wonka'
ASSERT VALUE n_lines = 4000000  WHERE brand = 'Hooli'
SELECT p.brand, COUNT(*) AS n_lines
FROM {{zone_name}}.retail.fact_sales f
JOIN {{zone_name}}.retail.dim_product p
  ON f.product_key = p.product_id
GROUP BY p.brand
ORDER BY n_lines DESC, p.brand
LIMIT 50;

-- ============================================================================
-- Query 13: fact_inventory_snapshot integrity
-- ============================================================================
-- 100,000,000 rows. SUM(inventory_snapshot_id) = 100M * 100_000_001 / 2
-- = 5_000_000_050_000_000. ABC class cycles 4 -> 25M each. Stock status
-- cycles 5 -> 20M each.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 100000000
ASSERT VALUE sum_id = 5000000050000000
ASSERT VALUE n_abc = 4
ASSERT VALUE n_status = 5
ASSERT VALUE n_regions = 5
ASSERT VALUE n_categories = 10
SELECT
    COUNT(*)                                    AS n_rows,
    SUM(inventory_snapshot_id)                  AS sum_id,
    COUNT(DISTINCT abc_classification)          AS n_abc,
    COUNT(DISTINCT stock_status)                AS n_status,
    COUNT(DISTINCT store_region)                AS n_regions,
    COUNT(DISTINCT product_category_l1)         AS n_categories
FROM {{zone_name}}.retail.fact_inventory_snapshot;

-- ============================================================================
-- Query 14: fact_inventory_snapshot stock_status distribution (PBI report)
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT RESULT SET INCLUDES
    ('Discontinued',  20000000),
    ('In Stock',      20000000),
    ('Low Stock',     20000000),
    ('Out of Stock',  20000000),
    ('Overstock',     20000000)
SELECT stock_status, COUNT(*) AS n_snapshots
FROM {{zone_name}}.retail.fact_inventory_snapshot
GROUP BY stock_status
ORDER BY stock_status;

-- ============================================================================
-- Query 15: fact_web_events integrity
-- ============================================================================
-- 200,000,000 rows. Event type cycles 10 -> 20M each. Bounce flag
-- (rn % 10 = 0) -> 20,000,000 bounces.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 200000000
ASSERT VALUE sum_id = 20000000100000000
ASSERT VALUE n_event_types = 10
ASSERT VALUE n_devices = 5
ASSERT VALUE n_browsers = 8
ASSERT VALUE n_bounces = 20000000
ASSERT VALUE n_with_search = 20000000
SELECT
    COUNT(*)                                            AS n_rows,
    SUM(event_id)                                       AS sum_id,
    COUNT(DISTINCT event_type)                          AS n_event_types,
    COUNT(DISTINCT device_type)                         AS n_devices,
    COUNT(DISTINCT browser)                             AS n_browsers,
    SUM(CASE WHEN is_bounce THEN 1 ELSE 0 END)          AS n_bounces,
    SUM(CASE WHEN search_query IS NOT NULL THEN 1 ELSE 0 END) AS n_with_search
FROM {{zone_name}}.retail.fact_web_events;

-- ============================================================================
-- Query 16: fact_web_events event_type distribution (PBI funnel input)
-- ============================================================================

ASSERT ROW_COUNT = 10
ASSERT RESULT SET INCLUDES
    ('add_to_cart',          20000000),
    ('checkout_complete',    20000000),
    ('checkout_start',       20000000),
    ('click_recommendation', 20000000),
    ('page_view',            20000000),
    ('product_view',         20000000),
    ('remove_from_cart',     20000000),
    ('search',               20000000),
    ('share',                20000000),
    ('wishlist_add',         20000000)
SELECT event_type, COUNT(*) AS n_events
FROM {{zone_name}}.retail.fact_web_events
GROUP BY event_type
ORDER BY event_type;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- One row per table pinned to its closed-form (count, key sum). If any
-- value drifts the table name in that row points to the regressing path.

ASSERT ROW_COUNT = 7
ASSERT RESULT SET INCLUDES
    ('dim_date',                     7305,                      0),
    ('dim_store',                   25000,              312512500),
    ('dim_product',               1000000,           500000500000),
    ('dim_customer',              5000000,         12500002500000),
    ('fact_sales',              200000000,      20000000100000000),
    ('fact_inventory_snapshot', 100000000,       5000000050000000),
    ('fact_web_events',         200000000,      20000000100000000)
SELECT 'dim_date'                   AS tbl, COUNT(*) AS n, CAST(0 AS BIGINT)        AS s FROM {{zone_name}}.retail.dim_date
UNION ALL SELECT 'dim_store',                  COUNT(*), SUM(store_id)               FROM {{zone_name}}.retail.dim_store
UNION ALL SELECT 'dim_product',                COUNT(*), SUM(product_id)             FROM {{zone_name}}.retail.dim_product
UNION ALL SELECT 'dim_customer',               COUNT(*), SUM(customer_id)            FROM {{zone_name}}.retail.dim_customer
UNION ALL SELECT 'fact_sales',                 COUNT(*), SUM(sale_id)                FROM {{zone_name}}.retail.fact_sales
UNION ALL SELECT 'fact_inventory_snapshot',    COUNT(*), SUM(inventory_snapshot_id)  FROM {{zone_name}}.retail.fact_inventory_snapshot
UNION ALL SELECT 'fact_web_events',            COUNT(*), SUM(event_id)               FROM {{zone_name}}.retail.fact_web_events;
