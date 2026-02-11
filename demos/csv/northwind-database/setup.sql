-- ============================================================================
-- Northwind Trading Company — Demo Setup Script
-- ============================================================================
-- This script creates a complete workspace with the Northwind sample database.
-- It provisions workspace, schema, role, external tables, and permissions.
--
-- Variables (auto-injected by Delta Forge):
--   {{data_path}}     — Local path where demo data files were downloaded
--   {{current_user}}  — Username of the current logged-in user
--
-- What this script does:
--   1. Creates the 'northwind' workspace
--   2. Creates the 'trading' schema for all business data
--   3. Creates a 'northwind_reader' role with SELECT access
--   4. Creates 11 external tables from semicolon-delimited CSV files
--   5. Grants the northwind_reader role to the current user
--
-- After running, query the data with standard SQL:
--   SELECT * FROM trading.customers LIMIT 10;
-- ============================================================================


-- ============================================================================
-- STEP 1: Workspace
-- ============================================================================
-- A workspace provides an isolated namespace for related data assets.
-- Each demo gets its own workspace to avoid naming conflicts.
-- ============================================================================

CREATE WORKSPACE IF NOT EXISTS northwind
    COMMENT 'Northwind Trading Company — classic sample dataset with 11 tables';

USE WORKSPACE northwind;


-- ============================================================================
-- STEP 2: Schema
-- ============================================================================
-- Schemas organize tables within a workspace. The 'trading' schema holds
-- all Northwind business entities: customers, orders, products, etc.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS trading
    COMMENT 'Business data — customers, orders, products, employees, suppliers';


-- ============================================================================
-- STEP 3: Role & Permissions Setup
-- ============================================================================
-- Create a dedicated role for Northwind data access. This follows the
-- principle of least privilege — users get only the access they need.
-- ============================================================================

CREATE ROLE IF NOT EXISTS northwind_reader
    COMMENT 'Read-only access to all Northwind trading data';

GRANT USAGE ON WORKSPACE northwind TO ROLE northwind_reader;
GRANT USAGE ON SCHEMA trading TO ROLE northwind_reader;


-- ============================================================================
-- STEP 4: External Tables
-- ============================================================================
-- Each table reads from a semicolon-delimited CSV file. Schema is inferred
-- automatically from the CSV headers.
-- ============================================================================

-- CUSTOMERS — 91 customer companies with contact and address details
-- Columns: customerID, companyName, contactName, contactTitle, address,
--          city, region, postalCode, country, phone, fax
CREATE EXTERNAL TABLE IF NOT EXISTS trading.customers
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
CREATE EXTERNAL TABLE IF NOT EXISTS trading.employees
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
CREATE EXTERNAL TABLE IF NOT EXISTS trading.orders
USING CSV
LOCATION '{{data_path}}/orders.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- ORDER_DETAILS — 2,155 line items linking orders to products with pricing
-- Columns: orderID, productID, unitPrice, quantity, discount
CREATE EXTERNAL TABLE IF NOT EXISTS trading.order_details
USING CSV
LOCATION '{{data_path}}/order_details.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- PRODUCTS — 77 products with pricing, stock levels, and reorder thresholds
-- Columns: productID, productName, supplierID, categoryID, quantityPerUnit,
--          unitPrice, unitsInStock, unitsOnOrder, reorderLevel, discontinued
CREATE EXTERNAL TABLE IF NOT EXISTS trading.products
USING CSV
LOCATION '{{data_path}}/products.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- CATEGORIES — 8 product categories (Beverages, Condiments, Seafood, etc.)
-- Columns: categoryID, categoryName, description, picture
CREATE EXTERNAL TABLE IF NOT EXISTS trading.categories
USING CSV
LOCATION '{{data_path}}/categories.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- SUPPLIERS — 29 product suppliers with contact and location details
-- Columns: supplierID, companyName, contactName, contactTitle, address,
--          city, region, postalCode, country, phone, fax, homePage
CREATE EXTERNAL TABLE IF NOT EXISTS trading.suppliers
USING CSV
LOCATION '{{data_path}}/suppliers.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- SHIPPERS — 3 shipping companies used for order delivery
-- Columns: shipperID, companyName, phone
CREATE EXTERNAL TABLE IF NOT EXISTS trading.shippers
USING CSV
LOCATION '{{data_path}}/shippers.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- REGIONS — 4 geographic sales regions (Eastern, Western, Northern, Southern)
-- Columns: regionID, regionDescription
CREATE EXTERNAL TABLE IF NOT EXISTS trading.regions
USING CSV
LOCATION '{{data_path}}/regions.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- TERRITORIES — 53 sales territories linked to regions
-- Columns: territoryID, territoryDescription, regionID
CREATE EXTERNAL TABLE IF NOT EXISTS trading.territories
USING CSV
LOCATION '{{data_path}}/territories.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- EMPLOYEE_TERRITORIES — Maps employees to the territories they cover
-- Columns: employeeID, territoryID
CREATE EXTERNAL TABLE IF NOT EXISTS trading.employee_territories
USING CSV
LOCATION '{{data_path}}/employee_territories.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);


-- ============================================================================
-- STEP 5: Table Permissions
-- ============================================================================
-- Grant SELECT on all tables to the northwind_reader role.
-- ============================================================================

GRANT SELECT ON TABLE trading.customers TO ROLE northwind_reader;
GRANT SELECT ON TABLE trading.employees TO ROLE northwind_reader;
GRANT SELECT ON TABLE trading.orders TO ROLE northwind_reader;
GRANT SELECT ON TABLE trading.order_details TO ROLE northwind_reader;
GRANT SELECT ON TABLE trading.products TO ROLE northwind_reader;
GRANT SELECT ON TABLE trading.categories TO ROLE northwind_reader;
GRANT SELECT ON TABLE trading.suppliers TO ROLE northwind_reader;
GRANT SELECT ON TABLE trading.shippers TO ROLE northwind_reader;
GRANT SELECT ON TABLE trading.regions TO ROLE northwind_reader;
GRANT SELECT ON TABLE trading.territories TO ROLE northwind_reader;
GRANT SELECT ON TABLE trading.employee_territories TO ROLE northwind_reader;


-- ============================================================================
-- STEP 6: Assign Role to Current User
-- ============================================================================
-- Grant the northwind_reader role to the user who installed this demo.
-- ============================================================================

GRANT ROLE northwind_reader TO USER {{current_user}};
