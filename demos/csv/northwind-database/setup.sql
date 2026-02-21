-- ============================================================================
-- Northwind Trading Company — Demo Setup Script
-- ============================================================================
-- This script provisions the Northwind sample database as external tables.
-- It creates zone, schema, role, external tables, and permissions.
--
-- Variables (auto-injected by Delta Forge):
--   {{data_path}}     — Local path where demo data files were downloaded
--   {{current_user}}  — Username of the current logged-in user
--
-- What this script does:
--   1. Creates the 'external' zone (shared across all demos)
--   2. Creates the 'external.csv' schema (named after the file format)
--   3. Creates a 'northwind_reader' role with SELECT access
--   4. Creates 11 external tables from semicolon-delimited CSV files
--   5. Grants the northwind_reader role to the current user
--
-- Naming convention: external.format.table
--   zone   = 'external'  (all external/demo tables live here)
--   schema = 'csv'       (the file format)
--   table  = object name (e.g. customers, orders)
--
-- After running, query the data with standard SQL:
--   SELECT * FROM external.csv.customers LIMIT 10;
-- ============================================================================


-- ============================================================================
-- STEP 1: Zone
-- ============================================================================
-- The 'external' zone is a shared namespace for all external/demo tables.
-- Using IF NOT EXISTS so multiple demos can safely create it.
-- ============================================================================

CREATE ZONE IF NOT EXISTS external
    TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';


-- ============================================================================
-- STEP 2: Schema
-- ============================================================================
-- The schema is named after the file format. All CSV-backed tables
-- from any demo live under external.csv.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS external.csv
    COMMENT 'CSV-backed external tables';


-- ============================================================================
-- STEP 3: Role & Permissions Setup
-- ============================================================================

CREATE ROLE IF NOT EXISTS northwind_reader
    COMMENT 'Read-only access to Northwind trading data';

GRANT USAGE ON SCHEMA external.csv TO ROLE northwind_reader;


-- ============================================================================
-- STEP 4: External Tables
-- ============================================================================
-- Each table reads from a semicolon-delimited CSV file. Schema is inferred
-- automatically from the CSV headers. All names are fully qualified:
-- external.csv.<table_name>
-- ============================================================================

-- CUSTOMERS — 91 customer companies with contact and address details
-- Columns: customerID, companyName, contactName, contactTitle, address,
--          city, region, postalCode, country, phone, fax
CREATE EXTERNAL TABLE IF NOT EXISTS external.csv.customers
USING CSV
LOCATION '{{data_path}}/customers.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- EMPLOYEES — 9 sales employees with hire dates and reporting hierarchy
-- Columns: employeeID, lastName, firstName, title, titleOfCourtesy,
--          birthDate, hireDate, address, city, region, postalCode, country,
--          homePhone, extension, photo, notes, reportsTo, photoPath
CREATE EXTERNAL TABLE IF NOT EXISTS external.csv.employees
USING CSV
LOCATION '{{data_path}}/employees.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- ORDERS — 830 customer orders with dates, shipping info, and freight costs
-- Columns: orderID, customerID, employeeID, orderDate, requiredDate,
--          shippedDate, shipVia, freight, shipName, shipAddress, shipCity,
--          shipRegion, shipPostalCode, shipCountry
CREATE EXTERNAL TABLE IF NOT EXISTS external.csv.orders
USING CSV
LOCATION '{{data_path}}/orders.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- ORDER_DETAILS — 2,155 line items linking orders to products with pricing
-- Columns: orderID, productID, unitPrice, quantity, discount
CREATE EXTERNAL TABLE IF NOT EXISTS external.csv.order_details
USING CSV
LOCATION '{{data_path}}/order_details.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- PRODUCTS — 77 products with pricing, stock levels, and reorder thresholds
-- Columns: productID, productName, supplierID, categoryID, quantityPerUnit,
--          unitPrice, unitsInStock, unitsOnOrder, reorderLevel, discontinued
CREATE EXTERNAL TABLE IF NOT EXISTS external.csv.products
USING CSV
LOCATION '{{data_path}}/products.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- CATEGORIES — 8 product categories (Beverages, Condiments, Seafood, etc.)
-- Columns: categoryID, categoryName, description, picture
CREATE EXTERNAL TABLE IF NOT EXISTS external.csv.categories
USING CSV
LOCATION '{{data_path}}/categories.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- SUPPLIERS — 29 product suppliers with contact and location details
-- Columns: supplierID, companyName, contactName, contactTitle, address,
--          city, region, postalCode, country, phone, fax, homePage
CREATE EXTERNAL TABLE IF NOT EXISTS external.csv.suppliers
USING CSV
LOCATION '{{data_path}}/suppliers.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- SHIPPERS — 3 shipping companies used for order delivery
-- Columns: shipperID, companyName, phone
CREATE EXTERNAL TABLE IF NOT EXISTS external.csv.shippers
USING CSV
LOCATION '{{data_path}}/shippers.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- REGIONS — 4 geographic sales regions (Eastern, Western, Northern, Southern)
-- Columns: regionID, regionDescription
CREATE EXTERNAL TABLE IF NOT EXISTS external.csv.regions
USING CSV
LOCATION '{{data_path}}/regions.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- TERRITORIES — 53 sales territories linked to regions
-- Columns: territoryID, territoryDescription, regionID
CREATE EXTERNAL TABLE IF NOT EXISTS external.csv.territories
USING CSV
LOCATION '{{data_path}}/territories.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- EMPLOYEE_TERRITORIES — Maps employees to the territories they cover
-- Columns: employeeID, territoryID
CREATE EXTERNAL TABLE IF NOT EXISTS external.csv.employee_territories
USING CSV
LOCATION '{{data_path}}/employee_territories.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);


-- ============================================================================
-- STEP 5: Table Permissions
-- ============================================================================

GRANT SELECT ON TABLE external.csv.customers TO ROLE northwind_reader;
GRANT SELECT ON TABLE external.csv.employees TO ROLE northwind_reader;
GRANT SELECT ON TABLE external.csv.orders TO ROLE northwind_reader;
GRANT SELECT ON TABLE external.csv.order_details TO ROLE northwind_reader;
GRANT SELECT ON TABLE external.csv.products TO ROLE northwind_reader;
GRANT SELECT ON TABLE external.csv.categories TO ROLE northwind_reader;
GRANT SELECT ON TABLE external.csv.suppliers TO ROLE northwind_reader;
GRANT SELECT ON TABLE external.csv.shippers TO ROLE northwind_reader;
GRANT SELECT ON TABLE external.csv.regions TO ROLE northwind_reader;
GRANT SELECT ON TABLE external.csv.territories TO ROLE northwind_reader;
GRANT SELECT ON TABLE external.csv.employee_territories TO ROLE northwind_reader;


-- ============================================================================
-- STEP 6: Assign Role to Current User
-- ============================================================================

GRANT ROLE northwind_reader TO USER {{current_user}};
