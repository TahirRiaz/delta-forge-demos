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
-- STEP 1: Revoke Role from User
-- ============================================================================

REVOKE ROLE northwind_reader FROM USER {{current_user}};


-- ============================================================================
-- STEP 2: Revoke Table Permissions
-- ============================================================================

REVOKE SELECT ON TABLE external.csv.customers FROM ROLE northwind_reader;
REVOKE SELECT ON TABLE external.csv.employees FROM ROLE northwind_reader;
REVOKE SELECT ON TABLE external.csv.orders FROM ROLE northwind_reader;
REVOKE SELECT ON TABLE external.csv.order_details FROM ROLE northwind_reader;
REVOKE SELECT ON TABLE external.csv.products FROM ROLE northwind_reader;
REVOKE SELECT ON TABLE external.csv.categories FROM ROLE northwind_reader;
REVOKE SELECT ON TABLE external.csv.suppliers FROM ROLE northwind_reader;
REVOKE SELECT ON TABLE external.csv.shippers FROM ROLE northwind_reader;
REVOKE SELECT ON TABLE external.csv.regions FROM ROLE northwind_reader;
REVOKE SELECT ON TABLE external.csv.territories FROM ROLE northwind_reader;
REVOKE SELECT ON TABLE external.csv.employee_territories FROM ROLE northwind_reader;


-- ============================================================================
-- STEP 3: Revoke Schema Permission
-- ============================================================================

REVOKE USAGE ON SCHEMA external.csv FROM ROLE northwind_reader;


-- ============================================================================
-- STEP 4: Drop Schema Columns
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
-- STEP 5: Drop External Tables
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
-- STEP 6: Drop Role
-- ============================================================================

DROP ROLE IF EXISTS northwind_reader;


-- ============================================================================
-- STEP 7: Drop Schema
-- ============================================================================

DROP SCHEMA IF EXISTS external.csv;


-- ============================================================================
-- STEP 8: Drop Zone
-- ============================================================================

DROP ZONE IF EXISTS external;
