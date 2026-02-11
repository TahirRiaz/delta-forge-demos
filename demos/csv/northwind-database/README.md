# Northwind Trading Company Demo

## Overview

The Northwind database is a classic sample dataset representing a fictional specialty
food trading company. It contains 11 interconnected CSV files covering the full
business domain: customers, orders, products, employees, suppliers, and shipping.

This demo provisions external tables with schema, role-based access control, and
demonstrates how Delta Forge handles relational joins, aggregations, and business
analytics across multiple files.

## What This Demo Sets Up

### Infrastructure

| Resource | Name | Description |
| -------- | ---- | ----------- |
| Zone | `external` | Shared namespace for all external/demo tables |
| Schema | `csv` | CSV-backed external tables |
| Role | `northwind_reader` | Read-only access to Northwind data |

### Naming Convention

All objects use 3-part fully qualified names: `external.format.table`

- **Zone** = `external` (shared across all demos)
- **Schema** = `csv` (the file format)
- **Table** = object name (e.g. `customers`, `orders`)

### Tables Created (in `external.csv` schema)

| Table | Records | Description |
| ----- | ------- | ----------- |
| `external.csv.customers` | 91 | Customer companies with contact details |
| `external.csv.employees` | 9 | Sales employees with reporting hierarchy |
| `external.csv.orders` | 830 | Customer orders with shipping details |
| `external.csv.order_details` | 2,155 | Line items linking orders to products |
| `external.csv.products` | 77 | Product catalog with pricing and stock |
| `external.csv.categories` | 8 | Product categories |
| `external.csv.suppliers` | 29 | Product suppliers with contact info |
| `external.csv.shippers` | 3 | Shipping companies |
| `external.csv.regions` | 4 | Geographic sales regions |
| `external.csv.territories` | 53 | Sales territories |
| `external.csv.employee_territories` | 49 | Employee-to-territory assignments |

### Permissions Granted

- `northwind_reader` role gets `USAGE` on the `external.csv` schema
- `northwind_reader` role gets `SELECT` on all 11 tables
- The role is automatically assigned to the user who installs the demo

### Relationships

```text
customers ──< orders ──< order_details >── products >── categories
                │                              │
                └── employees                  └── suppliers
                      │
                      └──< employee_territories >── territories >── regions
```

### Data Format

- **Format**: CSV with semicolon (`;`) delimiter
- **Headers**: First row contains column names
- **Encoding**: UTF-8

## Sample Queries

After running setup, try these queries:

```sql
-- Top 10 customers by total order value
SELECT c.companyName, COUNT(o.orderID) AS order_count,
       ROUND(SUM(od.unitPrice * od.quantity), 2) AS total_value
FROM external.csv.customers c
JOIN external.csv.orders o ON c.customerID = o.customerID
JOIN external.csv.order_details od ON o.orderID = od.orderID
GROUP BY c.companyName
ORDER BY total_value DESC
LIMIT 10;

-- Monthly order trends
SELECT EXTRACT(YEAR FROM o.orderDate) AS year,
       EXTRACT(MONTH FROM o.orderDate) AS month,
       COUNT(*) AS order_count
FROM external.csv.orders o
GROUP BY year, month
ORDER BY year, month;

-- Products below reorder level (need restocking)
SELECT p.productName, p.unitsInStock, p.reorderLevel,
       c.categoryName, s.companyName AS supplier
FROM external.csv.products p
JOIN external.csv.categories c ON p.categoryID = c.categoryID
JOIN external.csv.suppliers s ON p.supplierID = s.supplierID
WHERE p.unitsInStock < p.reorderLevel AND p.discontinued = 0
ORDER BY p.unitsInStock;
```

## Data Source

Based on the classic Microsoft Northwind sample database, adapted to CSV format.
