-- ============================================================================
-- Northwind Trading Company — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
--
-- The schema and zone are shared across demos. DROP SCHEMA / DROP ZONE will
-- succeed silently if they are empty, or produce a warning (not an error) if
-- other tables / schemas still exist — so it is always safe to leave them in.
-- ============================================================================


-- ============================================================================
-- STEP 1: Revoke Table Permissions
-- ============================================================================

REVOKE READ ON TABLE {{zone_name}}.csv.nw_customers FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.csv.nw_employees FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.csv.nw_orders FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.csv.nw_order_details FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.csv.nw_products FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.csv.nw_categories FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.csv.nw_suppliers FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.csv.nw_shippers FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.csv.nw_regions FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.csv.nw_territories FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.csv.nw_employee_territories FROM USER {{current_user}};


-- ============================================================================
-- STEP 2: Drop Schema Columns
-- ============================================================================

DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.csv.nw_customers;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.csv.nw_employees;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.csv.nw_orders;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.csv.nw_order_details;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.csv.nw_products;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.csv.nw_categories;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.csv.nw_suppliers;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.csv.nw_shippers;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.csv.nw_regions;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.csv.nw_territories;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.csv.nw_employee_territories;


-- ============================================================================
-- STEP 3: Drop External Tables
-- ============================================================================

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


-- ============================================================================
-- STEP 4: Drop Schema
-- ============================================================================

DROP SCHEMA IF EXISTS {{zone_name}}.csv;


-- ============================================================================
-- STEP 5: Drop Zone
-- ============================================================================

DROP ZONE IF EXISTS {{zone_name}};
