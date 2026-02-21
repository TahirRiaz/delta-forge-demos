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

REVOKE READ ON TABLE external.csv.customers FROM USER {{current_user}};
REVOKE READ ON TABLE external.csv.employees FROM USER {{current_user}};
REVOKE READ ON TABLE external.csv.orders FROM USER {{current_user}};
REVOKE READ ON TABLE external.csv.order_details FROM USER {{current_user}};
REVOKE READ ON TABLE external.csv.products FROM USER {{current_user}};
REVOKE READ ON TABLE external.csv.categories FROM USER {{current_user}};
REVOKE READ ON TABLE external.csv.suppliers FROM USER {{current_user}};
REVOKE READ ON TABLE external.csv.shippers FROM USER {{current_user}};
REVOKE READ ON TABLE external.csv.regions FROM USER {{current_user}};
REVOKE READ ON TABLE external.csv.territories FROM USER {{current_user}};
REVOKE READ ON TABLE external.csv.employee_territories FROM USER {{current_user}};


-- ============================================================================
-- STEP 2: Drop Schema Columns
-- ============================================================================

DROP SCHEMA COLUMNS FOR TABLE external.csv.customers;
DROP SCHEMA COLUMNS FOR TABLE external.csv.employees;
DROP SCHEMA COLUMNS FOR TABLE external.csv.orders;
DROP SCHEMA COLUMNS FOR TABLE external.csv.order_details;
DROP SCHEMA COLUMNS FOR TABLE external.csv.products;
DROP SCHEMA COLUMNS FOR TABLE external.csv.categories;
DROP SCHEMA COLUMNS FOR TABLE external.csv.suppliers;
DROP SCHEMA COLUMNS FOR TABLE external.csv.shippers;
DROP SCHEMA COLUMNS FOR TABLE external.csv.regions;
DROP SCHEMA COLUMNS FOR TABLE external.csv.territories;
DROP SCHEMA COLUMNS FOR TABLE external.csv.employee_territories;


-- ============================================================================
-- STEP 3: Drop External Tables
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS external.csv.employee_territories;
DROP EXTERNAL TABLE IF EXISTS external.csv.territories;
DROP EXTERNAL TABLE IF EXISTS external.csv.regions;
DROP EXTERNAL TABLE IF EXISTS external.csv.shippers;
DROP EXTERNAL TABLE IF EXISTS external.csv.suppliers;
DROP EXTERNAL TABLE IF EXISTS external.csv.categories;
DROP EXTERNAL TABLE IF EXISTS external.csv.products;
DROP EXTERNAL TABLE IF EXISTS external.csv.order_details;
DROP EXTERNAL TABLE IF EXISTS external.csv.orders;
DROP EXTERNAL TABLE IF EXISTS external.csv.employees;
DROP EXTERNAL TABLE IF EXISTS external.csv.customers;


-- ============================================================================
-- STEP 4: Drop Schema
-- ============================================================================

DROP SCHEMA IF EXISTS external.csv;


-- ============================================================================
-- STEP 5: Drop Zone
-- ============================================================================

DROP ZONE IF EXISTS external;
