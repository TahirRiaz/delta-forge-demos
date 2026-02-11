# Northwind Trading Company Demo

## Overview

The Northwind database is a classic sample dataset representing a fictional specialty
food trading company. It contains 11 interconnected CSV files covering the full
business domain: customers, orders, products, employees, suppliers, and shipping.

This demo provisions a complete workspace with schema, role-based access control,
and external tables, then demonstrates how Delta Forge handles relational joins,
aggregations, and business analytics across multiple files.

## What This Demo Sets Up

### Infrastructure
| Resource | Name | Description |
|----------|------|-------------|
| Workspace | `northwind` | Isolated namespace for all Northwind data |
| Schema | `trading` | Contains all 11 business entity tables |
| Role | `northwind_reader` | Read-only access to all trading data |

### Tables Created (in `trading` schema)
| Table | Records | Description |
|-------|---------|-------------|
| `trading.customers` | 91 | Customer companies with contact details |
| `trading.employees` | 9 | Sales employees with reporting hierarchy |
| `trading.orders` | 830 | Customer orders with shipping details |
| `trading.order_details` | 2,155 | Line items linking orders to products |
| `trading.products` | 77 | Product catalog with pricing and stock |
| `trading.categories` | 8 | Product categories |
| `trading.suppliers` | 29 | Product suppliers with contact info |
| `trading.shippers` | 3 | Shipping companies |
| `trading.regions` | 4 | Geographic sales regions |
| `trading.territories` | 53 | Sales territories |
| `trading.employee_territories` | 49 | Employee-to-territory assignments |

### Permissions Granted
- `northwind_reader` role gets `USAGE` on workspace and schema
- `northwind_reader` role gets `SELECT` on all 11 tables
- The role is automatically assigned to the user who installs the demo

### Relationships
```
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
FROM trading.customers c
JOIN trading.orders o ON c.customerID = o.customerID
JOIN trading.order_details od ON o.orderID = od.orderID
GROUP BY c.companyName
ORDER BY total_value DESC
LIMIT 10;

-- Monthly order trends
SELECT EXTRACT(YEAR FROM o.orderDate) AS year,
       EXTRACT(MONTH FROM o.orderDate) AS month,
       COUNT(*) AS order_count
FROM trading.orders o
GROUP BY year, month
ORDER BY year, month;

-- Products below reorder level (need restocking)
SELECT p.productName, p.unitsInStock, p.reorderLevel,
       c.categoryName, s.companyName AS supplier
FROM trading.products p
JOIN trading.categories c ON p.categoryID = c.categoryID
JOIN trading.suppliers s ON p.supplierID = s.supplierID
WHERE p.unitsInStock < p.reorderLevel AND p.discontinued = 0
ORDER BY p.unitsInStock;
```

## Data Source

Based on the classic Microsoft Northwind sample database, adapted to CSV format.
