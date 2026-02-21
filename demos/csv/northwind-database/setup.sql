-- ============================================================================
-- Northwind Trading Company — Setup Script
-- ============================================================================
-- Provisions the Northwind sample database as 11 external tables for
-- cross-table queries: joins, aggregations, and business analytics.
--
-- Variables (auto-injected by Delta Forge):
--   {{data_path}}     — Local path where demo data files were downloaded
--   {{current_user}}  — Username of the current logged-in user
--
-- What this script does:
--   1. Creates the 'external' zone (shared across all demos)
--   2. Creates the 'external.csv' schema (named after the file format)
--   3. Creates 11 external tables from semicolon-delimited CSV files
--   4. Detects schema for all tables
--   5. Grants read access on each table to the current user
--
-- See queries.sql for cross-table demo queries.
--
-- Naming convention: external.format.table
--   zone   = 'external'  (all external/demo tables live here)
--   schema = 'csv'       (the file format)
--   table  = object name (e.g. customers, orders)
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
-- Each table reads from a semicolon-delimited CSV file. All names are fully
-- qualified: external.csv.<table_name>
-- ============================================================================

-- CUSTOMERS — 91 customer companies with contact and address details
CREATE EXTERNAL TABLE IF NOT EXISTS external.csv.customers
USING CSV
LOCATION '{{data_path}}/customers.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- EMPLOYEES — 9 sales employees with hire dates and reporting hierarchy
CREATE EXTERNAL TABLE IF NOT EXISTS external.csv.employees
USING CSV
LOCATION '{{data_path}}/employees.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- ORDERS — 830 customer orders with dates, shipping info, and freight costs
CREATE EXTERNAL TABLE IF NOT EXISTS external.csv.orders
USING CSV
LOCATION '{{data_path}}/orders.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- ORDER_DETAILS — 2,155 line items linking orders to products with pricing
CREATE EXTERNAL TABLE IF NOT EXISTS external.csv.order_details
USING CSV
LOCATION '{{data_path}}/order_details.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- PRODUCTS — 77 products with pricing, stock levels, and reorder thresholds
CREATE EXTERNAL TABLE IF NOT EXISTS external.csv.products
USING CSV
LOCATION '{{data_path}}/products.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- CATEGORIES — 8 product categories (Beverages, Condiments, Seafood, etc.)
CREATE EXTERNAL TABLE IF NOT EXISTS external.csv.categories
USING CSV
LOCATION '{{data_path}}/categories.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- SUPPLIERS — 29 product suppliers with contact and location details
CREATE EXTERNAL TABLE IF NOT EXISTS external.csv.suppliers
USING CSV
LOCATION '{{data_path}}/suppliers.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- SHIPPERS — 3 shipping companies used for order delivery
CREATE EXTERNAL TABLE IF NOT EXISTS external.csv.shippers
USING CSV
LOCATION '{{data_path}}/shippers.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- REGIONS — 4 geographic sales regions (Eastern, Western, Northern, Southern)
CREATE EXTERNAL TABLE IF NOT EXISTS external.csv.regions
USING CSV
LOCATION '{{data_path}}/regions.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- TERRITORIES — 53 sales territories linked to regions
CREATE EXTERNAL TABLE IF NOT EXISTS external.csv.territories
USING CSV
LOCATION '{{data_path}}/territories.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- EMPLOYEE_TERRITORIES — Maps employees to the territories they cover
CREATE EXTERNAL TABLE IF NOT EXISTS external.csv.employee_territories
USING CSV
LOCATION '{{data_path}}/employee_territories.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);


-- ============================================================================
-- STEP 4: Detect Schema
-- ============================================================================
-- Discovers column metadata from the CSV files and saves it to the catalog.

DETECT SCHEMA FOR TABLE external.csv.customers;
DETECT SCHEMA FOR TABLE external.csv.employees;
DETECT SCHEMA FOR TABLE external.csv.orders;
DETECT SCHEMA FOR TABLE external.csv.order_details;
DETECT SCHEMA FOR TABLE external.csv.products;
DETECT SCHEMA FOR TABLE external.csv.categories;
DETECT SCHEMA FOR TABLE external.csv.suppliers;
DETECT SCHEMA FOR TABLE external.csv.shippers;
DETECT SCHEMA FOR TABLE external.csv.regions;
DETECT SCHEMA FOR TABLE external.csv.territories;
DETECT SCHEMA FOR TABLE external.csv.employee_territories;


-- ============================================================================
-- STEP 5: Table Permissions
-- ============================================================================

GRANT READ ON TABLE external.csv.customers TO USER {{current_user}};
GRANT READ ON TABLE external.csv.employees TO USER {{current_user}};
GRANT READ ON TABLE external.csv.orders TO USER {{current_user}};
GRANT READ ON TABLE external.csv.order_details TO USER {{current_user}};
GRANT READ ON TABLE external.csv.products TO USER {{current_user}};
GRANT READ ON TABLE external.csv.categories TO USER {{current_user}};
GRANT READ ON TABLE external.csv.suppliers TO USER {{current_user}};
GRANT READ ON TABLE external.csv.shippers TO USER {{current_user}};
GRANT READ ON TABLE external.csv.regions TO USER {{current_user}};
GRANT READ ON TABLE external.csv.territories TO USER {{current_user}};
GRANT READ ON TABLE external.csv.employee_territories TO USER {{current_user}};
