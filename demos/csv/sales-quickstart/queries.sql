-- ============================================================================
-- Sales Schema Evolution Demo — Demo Queries
-- ============================================================================
-- Queries showcasing how Delta Forge unifies CSV files with evolving schemas.
-- Missing columns from older files surface as NULL when queried together.
--
-- Evolution timeline:
--   Q1 2024  base schema   id, product_name, quantity, unit_price, sale_date, region
--   Q2 2024  + sales_rep
--   Q3 2024  + discount_pct
--   Q4 2024  - region (retired), + territory
--   Q1 2025  - discount_pct (retired), + channel
-- ============================================================================


-- ============================================================================
-- 1. All Sales — Unified View
-- ============================================================================
-- Shows all 15 records across 5 quarterly files.
-- Missing columns from older files appear as NULL.
--
-- Expected: 15 rows, ids 1-15
-- Records 1-3 have NULL sales_rep, territory, channel, discount_pct
-- Records 4-6 have NULL territory, channel, discount_pct
-- Records 10-12 have NULL region, channel
-- Records 13-15 have NULL region, discount_pct

SELECT *
FROM external.csv.sales
ORDER BY id;


-- ============================================================================
-- 2. Revenue by Product
-- ============================================================================
-- Aggregates quantity * unit_price across all files.
--
-- Expected results (6 products):
--   Widget A  | 58 units | 1,805.42
--   Widget B  | 23 units | 1,204.77
--   Widget C  |  9 units |   809.91
--   Gadget X  | 35 units |   542.50
--   Gadget Z  | 39 units |   514.75
--   Gadget Y  | 21 units |   462.00
-- Total across all products: 5,339.35

SELECT
    product_name,
    SUM(quantity) AS total_quantity,
    ROUND(SUM(quantity * unit_price), 2) AS total_revenue
FROM external.csv.sales
GROUP BY product_name
ORDER BY total_revenue DESC;


-- ============================================================================
-- 3. Sales Rep Performance
-- ============================================================================
-- Only Q2 2024+ have a sales_rep column; Q1 2024 rows show NULL.
--
-- Expected results:
--   Alice   | 5 sales | 1,973.47
--   Charlie | 3 sales | 1,498.10
--   Bob     | 4 sales | 1,007.93
--   NULL    | 3 sales |   859.85  (Q1 2024 — no sales_rep column)

SELECT
    sales_rep,
    COUNT(*) AS sale_count,
    ROUND(SUM(quantity * unit_price), 2) AS total_revenue
FROM external.csv.sales
GROUP BY sales_rep
ORDER BY total_revenue DESC;


-- ============================================================================
-- 4. Quarterly Revenue Trends
-- ============================================================================
-- Tracks revenue per quarter across the schema-evolution timeline.
--
-- Expected results:
--   2024-Q1 | 3 sales |   859.85
--   2024-Q2 | 3 sales |   773.89
--   2024-Q3 | 3 sales |   901.18
--   2024-Q4 | 3 sales | 1,277.76
--   2025-Q1 | 3 sales | 1,526.67

SELECT
    EXTRACT(YEAR FROM sale_date) AS year,
    EXTRACT(QUARTER FROM sale_date) AS quarter,
    COUNT(*) AS sale_count,
    ROUND(SUM(quantity * unit_price), 2) AS total_revenue
FROM external.csv.sales
GROUP BY year, quarter
ORDER BY year, quarter;


-- ============================================================================
-- 5. Region vs Territory — Schema Evolution in Action
-- ============================================================================
-- region existed in Q1-Q3 2024, then was retired in Q4 2024.
-- territory was introduced in Q4 2024, replacing region.
-- This query shows the transition: early records have region but NULL
-- territory; later records have territory but NULL region.
--
-- Expected results:
--   id  1 | region: North | territory: NULL
--   id  2 | region: South | territory: NULL
--   id  3 | region: East  | territory: NULL
--   id  4 | region: West  | territory: NULL
--   ...
--   id 10 | region: NULL  | territory: Northeast
--   id 11 | region: NULL  | territory: Southeast
--   ...

SELECT
    id,
    sale_date,
    region,
    territory
FROM external.csv.sales
ORDER BY id;


-- ============================================================================
-- 6. File Metadata — Which File Each Record Came From
-- ============================================================================
-- Delta Forge injects file metadata columns (df_file_name, df_row_number)
-- so you can trace each row back to its source file.
--
-- Expected: 15 rows, 3 per file
--   sales_2024_q1.csv → ids 1-3
--   sales_2024_q2.csv → ids 4-6
--   sales_2024_q3.csv → ids 7-9
--   sales_2024_q4.csv → ids 10-12
--   sales_2025_q1.csv → ids 13-15

SELECT
    id,
    product_name,
    df_file_name,
    df_row_number
FROM external.csv.sales
ORDER BY id;
