# Sales Data Quickstart

## Overview

A minimal demo to get started with Delta Forge in under 2 minutes. Creates a
workspace, schema, and role with two small CSV tables — perfect for learning
basic queries, filtering, aggregations, and role-based access control.

## What This Demo Sets Up

### Infrastructure

| Resource | Name | Description |
|----------|------|-------------|
| Workspace | `sales_quickstart` | Isolated namespace for sales demo data |
| Schema | `transactions` | Contains sales transaction tables |
| Role | `sales_reader` | Read-only access to all sales data |

### Tables Created (in `transactions` schema)

| Table | Records | Description |
|-------|---------|-------------|
| `sales_quickstart.transactions.sales` | 10 | Sales transactions with product, quantity, price, date, and region |
| `sales_quickstart.transactions.sales_extended` | 1 | Extended sales record with additional demo flag column |

### Permissions Granted

- `sales_reader` role gets `USAGE` on workspace and schema
- `sales_reader` role gets `SELECT` on both tables
- The role is automatically assigned to the user who installs the demo

## Sample Queries

```sql
-- View all sales
SELECT * FROM sales_quickstart.transactions.sales;

-- Total revenue by region
SELECT region, SUM(quantity * unit_price) AS revenue
FROM sales_quickstart.transactions.sales
GROUP BY region
ORDER BY revenue DESC;

-- Average order value by product
SELECT product_name, AVG(unit_price * quantity) AS avg_value
FROM sales_quickstart.transactions.sales
GROUP BY product_name
ORDER BY avg_value DESC;
```

## Data Format

- **Format**: CSV with comma delimiter
- **Headers**: First row contains column names
