-- ============================================================================
-- Sales Data Quickstart — Demo Setup Script
-- ============================================================================
-- A minimal demo to get started with Delta Forge in under 2 minutes.
-- Creates a zone, schema, and two small sales tables.
--
-- Variables (auto-injected by Delta Forge):
--   data_path     — Local path where demo data files were downloaded
--   current_user  — Username of the current logged-in user
--
-- What this script does:
--   1. Creates the 'external' zone (shared across all demos)
--   2. Creates the 'external.csv' schema (named after the file format)
--   3. Creates 2 external tables from CSV files
--
-- Naming convention: external.format.table
--   zone   = 'external'  (all external/demo tables live here)
--   schema = 'csv'       (the file format)
--   table  = object name
--
-- After running, try these queries:
--   SELECT * FROM external.csv.sales;
--   SELECT region, SUM(quantity * unit_price) AS revenue
--   FROM external.csv.sales GROUP BY region ORDER BY revenue DESC;
-- ============================================================================


-- ============================================================================
-- STEP 1: Zone
-- ============================================================================

CREATE ZONE IF NOT EXISTS external
    TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';


-- ============================================================================
-- STEP 2: Schema
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS external.csv
    COMMENT 'CSV-backed external tables';


-- ============================================================================
-- STEP 3: External Tables
-- ============================================================================

-- SALES — 10 sales transactions across 4 regions
-- Columns: id, product_name, quantity, unit_price, sale_date, region
CREATE EXTERNAL TABLE IF NOT EXISTS external.csv.sales
USING CSV
LOCATION '{{data_path}}/sales.csv'
OPTIONS (
    header = 'true'
);

-- SALES_EXTENDED — Extended sales record with additional demo flag column
-- Columns: id, product_name, quantity, unit_price, sale_date, region, demo
CREATE EXTERNAL TABLE IF NOT EXISTS external.csv.sales_extended
USING CSV
LOCATION '{{data_path}}/sales_extended.csv'
OPTIONS (
    header = 'true'
);
