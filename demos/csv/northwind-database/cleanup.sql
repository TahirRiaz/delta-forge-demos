-- ============================================================================
-- Northwind Trading Company — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- DROP TABLE commands automatically clean up catalog metadata (columns, etc.).
--
-- The schema and zone are shared across demos. DROP SCHEMA / DROP ZONE will
-- succeed silently if they are empty, or produce a warning (not an error) if
-- other tables / schemas still exist — so it is always safe to leave them in.
-- ============================================================================

-- STEP 1: Drop External Tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.nw_employee_territories;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.nw_territories;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.nw_regions;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.nw_shippers;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.nw_suppliers;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.nw_categories;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.nw_products;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.nw_order_details;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.nw_orders;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.nw_employees;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.nw_customers;

-- STEP 2: Drop Schema
DROP SCHEMA IF EXISTS {{zone_name}}.csv;

-- STEP 3: Drop Zone
DROP ZONE IF EXISTS {{zone_name}};
