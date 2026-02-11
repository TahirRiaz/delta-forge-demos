-- ============================================================================
-- Sales Data Quickstart — Demo Setup Script
-- ============================================================================
-- A minimal demo to get started with Delta Forge in under 2 minutes.
-- Creates a zone, schema, role, and two small sales tables.
--
-- Variables (auto-injected by Delta Forge):
--   {{data_path}}     — Local path where demo data files were downloaded
--   {{current_user}}  — Username of the current logged-in user
--
-- What this script does:
--   1. Creates the 'sales' zone
--   2. Creates the 'sales.demo' schema
--   3. Creates a 'sales_reader' role with SELECT access
--   4. Creates 2 external tables from CSV files
--   5. Grants the sales_reader role to the current user
--
-- All objects use fully qualified 3-part names: zone.schema.table
--
-- After running, try these queries:
--   SELECT * FROM sales.demo.sales;
--   SELECT region, SUM(quantity * unit_price) AS revenue
--   FROM sales.demo.sales GROUP BY region ORDER BY revenue DESC;
-- ============================================================================


-- ============================================================================
-- STEP 1: Zone
-- ============================================================================

CREATE ZONE IF NOT EXISTS sales
    COMMENT 'Sales Data Quickstart — minimal demo for learning basic queries';


-- ============================================================================
-- STEP 2: Schema
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS sales.demo
    COMMENT 'Sales transaction data for quickstart demo';


-- ============================================================================
-- STEP 3: Role & Permissions Setup
-- ============================================================================

CREATE ROLE IF NOT EXISTS sales_reader
    COMMENT 'Read-only access to sales quickstart data';

GRANT USAGE ON SCHEMA sales.demo TO ROLE sales_reader;


-- ============================================================================
-- STEP 4: External Tables
-- ============================================================================

-- SALES — 10 sales transactions across 4 regions
-- Columns: id, product_name, quantity, unit_price, sale_date, region
CREATE EXTERNAL TABLE IF NOT EXISTS sales.demo.sales
USING CSV
LOCATION '{{data_path}}/sales.csv'
OPTIONS (
    header = 'true'
);

-- SALES_EXTENDED — Extended sales record with additional demo flag column
-- Columns: id, product_name, quantity, unit_price, sale_date, region, demo
CREATE EXTERNAL TABLE IF NOT EXISTS sales.demo.sales_extended
USING CSV
LOCATION '{{data_path}}/sales_extended.csv'
OPTIONS (
    header = 'true'
);


-- ============================================================================
-- STEP 5: Table Permissions
-- ============================================================================

GRANT SELECT ON TABLE sales.demo.sales TO ROLE sales_reader;
GRANT SELECT ON TABLE sales.demo.sales_extended TO ROLE sales_reader;


-- ============================================================================
-- STEP 6: Assign Role to Current User
-- ============================================================================

GRANT ROLE sales_reader TO USER {{current_user}};
