-- ============================================================================
-- Northwind Trading Company — Demo Queries
-- ============================================================================
-- Cross-table queries showcasing joins, aggregations, and business analytics
-- across the 11 Northwind tables.
--
-- Relationships:
--   nw_customers ──< nw_orders ──< nw_order_details >── nw_products >── nw_categories
--                       │                                    │
--                       └── nw_employees                     └── nw_suppliers
--                             │
--                             └──< nw_employee_territories >── nw_territories >── nw_regions
-- ============================================================================


-- ============================================================================
-- 1. Top 10 Customers by Total Order Value
-- ============================================================================
-- Joins: nw_customers → nw_orders → nw_order_details
--
-- Expected results (top 5):
--   QUICK-Stop            | 28 orders | 110,277.31
--   Ernst Handel          | 30 orders | 104,874.98
--   Save-a-lot Markets    | 31 orders | 104,361.95
--   Rattlesnake Canyon Grocery | 18 orders | 51,097.80
--   Hungry Owl All-Night Grocers | 19 orders | 49,979.91

SELECT
    c."companyName",
    COUNT(DISTINCT o."orderID") AS order_count,
    ROUND(SUM(od."unitPrice" * od.quantity * (1 - od.discount)), 2) AS total_value
FROM external.csv.nw_customers c
JOIN external.csv.nw_orders o ON c."customerID" = o."customerID"
JOIN external.csv.nw_order_details od ON o."orderID" = od."orderID"
GROUP BY c."companyName"
ORDER BY total_value DESC
LIMIT 10;


-- ============================================================================
-- 2. Revenue by Product Category
-- ============================================================================
-- Joins: nw_order_details → nw_products → nw_categories
--
-- Expected results (all 8 categories):
--   Beverages      | 12 products | 267,868.18
--   Dairy Products | 10 products | 234,507.29
--   Confections    | 13 products | 167,357.22
--   Meat/Poultry   |  6 products | 163,022.36
--   Seafood        | 12 products | 131,261.74
--   Condiments     | 12 products | 106,047.09
--   Produce        |  5 products |  99,984.58
--   Grains/Cereals |  7 products |  95,744.59

SELECT
    cat."categoryName",
    COUNT(DISTINCT p."productID") AS product_count,
    ROUND(SUM(od."unitPrice" * od.quantity * (1 - od.discount)), 2) AS total_revenue
FROM external.csv.nw_order_details od
JOIN external.csv.nw_products p ON od."productID" = p."productID"
JOIN external.csv.nw_categories cat ON p."categoryID" = cat."categoryID"
GROUP BY cat."categoryName"
ORDER BY total_revenue DESC;


-- ============================================================================
-- 3. Employee Sales Performance
-- ============================================================================
-- Joins: nw_employees → nw_orders → nw_order_details
--
-- Expected results (all 9 employees):
--   Margaret Peacock | Sales Representative        | 156 orders | 232,890.85
--   Janet Leverling  | Sales Representative        | 127 orders | 202,812.84
--   Nancy Davolio    | Sales Representative        | 123 orders | 192,107.60
--   Andrew Fuller    | Vice President, Sales       |  96 orders | 166,537.76
--   Laura Callahan   | Inside Sales Coordinator    | 104 orders | 126,862.28
--   Robert King      | Sales Representative        |  72 orders | 124,568.24
--   Anne Dodsworth   | Sales Representative        |  43 orders |  77,308.07
--   Michael Suyama   | Sales Representative        |  67 orders |  73,913.13
--   Steven Buchanan  | Sales Manager               |  42 orders |  68,792.28

SELECT
    e."firstName" || ' ' || e."lastName" AS employee_name,
    e.title,
    COUNT(DISTINCT o."orderID") AS orders_handled,
    ROUND(SUM(od."unitPrice" * od.quantity * (1 - od.discount)), 2) AS total_sales
FROM external.csv.nw_employees e
JOIN external.csv.nw_orders o ON e."employeeID" = o."employeeID"
JOIN external.csv.nw_order_details od ON o."orderID" = od."orderID"
GROUP BY e."firstName", e."lastName", e.title
ORDER BY total_sales DESC;


-- ============================================================================
-- 4. Monthly Order Trends
-- ============================================================================
-- Single table: nw_orders
-- 23 months from July 1996 to May 1998
--
-- Expected results (first 3 and last 3 months):
--   1996-07 | 22 orders |  1,288.18 freight
--   1996-08 | 25 orders |  1,397.17 freight
--   1996-09 | 23 orders |  1,123.48 freight
--   ...
--   1998-03 | 73 orders |  5,379.02 freight
--   1998-04 | 74 orders |  6,393.57 freight
--   1998-05 | 14 orders |    685.08 freight

SELECT
    EXTRACT(YEAR FROM o."orderDate") AS year,
    EXTRACT(MONTH FROM o."orderDate") AS month,
    COUNT(*) AS order_count,
    ROUND(SUM(o.freight), 2) AS total_freight
FROM external.csv.nw_orders o
GROUP BY year, month
ORDER BY year, month;


-- ============================================================================
-- 5. Products Below Reorder Level (Need Restocking)
-- ============================================================================
-- Joins: nw_products → nw_categories + nw_products → nw_suppliers
--
-- Expected results: 18 products below reorder level (not discontinued)
-- Top 3 by restock urgency:
--   Gorgonzola Telino    | Dairy Products | stock: 0  | reorder: 20 | on order: 70
--   Mascarpone Fabioli   | Dairy Products | stock: 9  | reorder: 25 | on order: 40
--   Louisiana Hot Spiced Okra | Condiments | stock: 4  | reorder: 20 | on order: 100

SELECT
    p."productName",
    cat."categoryName",
    s."companyName" AS supplier,
    p."unitsInStock",
    p."reorderLevel",
    p."unitsOnOrder"
FROM external.csv.nw_products p
JOIN external.csv.nw_categories cat ON p."categoryID" = cat."categoryID"
JOIN external.csv.nw_suppliers s ON p."supplierID" = s."supplierID"
WHERE p."unitsInStock" < p."reorderLevel"
  AND p.discontinued = 0
ORDER BY (p."reorderLevel" - p."unitsInStock") DESC;


-- ============================================================================
-- 6. Shipping Analysis by Carrier
-- ============================================================================
-- Joins: nw_orders → nw_shippers, nw_orders → nw_order_details
--
-- Expected results (all 3 carriers):
--   United Package   | 326 shipments | avg freight: 86.64  | value: 533,547.63
--   Federal Shipping | 255 shipments | avg freight: 80.44  | value: 383,405.47
--   Speedy Express   | 249 shipments | avg freight: 65.00  | value: 348,839.94

SELECT
    sh."companyName" AS shipper,
    COUNT(DISTINCT o."orderID") AS shipments,
    ROUND(AVG(o.freight), 2) AS avg_freight,
    ROUND(SUM(od."unitPrice" * od.quantity * (1 - od.discount)), 2) AS total_order_value
FROM external.csv.nw_orders o
JOIN external.csv.nw_shippers sh ON o."shipVia" = sh."shipperID"
JOIN external.csv.nw_order_details od ON o."orderID" = od."orderID"
GROUP BY sh."companyName"
ORDER BY shipments DESC;


-- ============================================================================
-- 7. Customer Orders by Country
-- ============================================================================
-- Joins: nw_customers → nw_orders
--
-- Expected results (top 5 by order count):
--   Germany   | 11 customers | 122 orders | avg freight: 92.49
--   USA       | 13 customers | 122 orders | avg freight: 112.88
--   UK        |  7 customers |  56 orders | avg freight: 52.75
--   Venezuela |  4 customers |  46 orders | avg freight: 59.46
--   Austria   |  2 customers |  40 orders | avg freight: 184.79

SELECT
    c.country,
    COUNT(DISTINCT c."customerID") AS customer_count,
    COUNT(DISTINCT o."orderID") AS order_count,
    ROUND(AVG(o.freight), 2) AS avg_freight
FROM external.csv.nw_customers c
JOIN external.csv.nw_orders o ON c."customerID" = o."customerID"
GROUP BY c.country
ORDER BY order_count DESC;


-- ============================================================================
-- 8. Employee Territory Coverage
-- ============================================================================
-- Joins: nw_employees → nw_employee_territories → nw_territories → nw_regions
--
-- Expected results (9 employees across 4 regions):
--   Andrew Fuller    | Eastern  | 7 territories
--   Anne Dodsworth   | Northern | 7 territories
--   Janet Leverling  | Southern | 4 territories
--   Laura Callahan   | Northern | 4 territories
--   Margaret Peacock | Eastern  | 3 territories
--   Michael Suyama   | Western  | 5 territories
--   Nancy Davolio    | Eastern  | 2 territories
--   Robert King      | Western  | 10 territories
--   Steven Buchanan  | Eastern  | 7 territories

SELECT
    e."firstName" || ' ' || e."lastName" AS employee_name,
    r."regionDescription" AS region,
    COUNT(t."territoryID") AS territory_count
FROM external.csv.nw_employees e
JOIN external.csv.nw_employee_territories et ON e."employeeID" = et."employeeID"
JOIN external.csv.nw_territories t ON et."territoryID" = t."territoryID"
JOIN external.csv.nw_regions r ON t."regionID" = r."regionID"
GROUP BY e."firstName", e."lastName", r."regionDescription"
ORDER BY employee_name, region;


-- ============================================================================
-- 9. Top Suppliers by Revenue
-- ============================================================================
-- Joins: nw_suppliers → nw_products → nw_order_details
--
-- Expected results (top 5):
--   Aux joyeux ecclesiastiques           | France  | 2 products | 153,691.28
--   Plutzer Lebensmittelgrossmarkte AG   | Germany | 5 products | 145,372.40
--   Gai paturage                         | France  | 2 products | 117,981.18
--   Pavlova, Ltd.                        | Australia | 5 products | 106,459.78
--   G'day, Mate                          | Australia | 3 products | 65,626.77

SELECT
    s."companyName" AS supplier,
    s.country,
    COUNT(DISTINCT p."productID") AS products_supplied,
    ROUND(SUM(od."unitPrice" * od.quantity * (1 - od.discount)), 2) AS total_revenue
FROM external.csv.nw_suppliers s
JOIN external.csv.nw_products p ON s."supplierID" = p."supplierID"
JOIN external.csv.nw_order_details od ON p."productID" = od."productID"
GROUP BY s."companyName", s.country
ORDER BY total_revenue DESC
LIMIT 10;


-- ============================================================================
-- 10. Late Shipments — Orders Shipped After Required Date
-- ============================================================================
-- Joins: nw_orders → nw_customers
--
-- Expected results: 37 late orders
-- Most recent 3 late shipments:
--   Order 10970 | Bolido Comidas preparadas  | required 1998-04-07 | shipped 1998-04-24
--   Order 10924 | Berglunds snabbkop         | required 1998-04-01 | shipped 1998-04-08
--   Order 10927 | La corne d'abondance       | required 1998-04-02 | shipped 1998-04-08

SELECT
    o."orderID",
    c."companyName",
    o."orderDate",
    o."requiredDate",
    o."shippedDate"
FROM external.csv.nw_orders o
JOIN external.csv.nw_customers c ON o."customerID" = c."customerID"
WHERE o."shippedDate" > o."requiredDate"
ORDER BY o."shippedDate" DESC;
