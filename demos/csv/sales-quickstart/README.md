# Sales Data Quickstart

## Overview

A minimal demo to get started with Delta Forge in under 2 minutes. Creates
external tables with a role — perfect for learning basic queries, filtering,
aggregations, and role-based access control.

## What This Demo Sets Up

### Infrastructure

| Resource | Name | Description |
| -------- | ---- | ----------- |
| Zone | `external` | Shared namespace for all external/demo tables |
| Schema | `csv` | CSV-backed external tables |
| Role | `sales_reader` | Read-only access to sales data |

### Naming Convention

All objects use 3-part fully qualified names: `external.format.table`

- **Zone** = `external` (shared across all demos)
- **Schema** = `csv` (the file format)
- **Table** = object name (e.g. `sales`, `sales_extended`)

### Tables Created (in `external.csv` schema)

| Table | Records | Description |
| ----- | ------- | ----------- |
| `external.csv.sales` | 10 | Sales transactions with product, quantity, price, date, and region |
| `external.csv.sales_extended` | 1 | Extended sales record with additional demo flag column |

### Permissions Granted

- `sales_reader` role gets `USAGE` on the `external.csv` schema
- `sales_reader` role gets `SELECT` on both tables
- The role is automatically assigned to the user who installs the demo

## Sample Queries

```sql
-- View all sales
SELECT * FROM external.csv.sales;

-- Total revenue by region
SELECT region, SUM(quantity * unit_price) AS revenue
FROM external.csv.sales
GROUP BY region
ORDER BY revenue DESC;

-- Average order value by product
SELECT product_name, AVG(unit_price * quantity) AS avg_value
FROM external.csv.sales
GROUP BY product_name
ORDER BY avg_value DESC;
```

## Data Format

- **Format**: CSV with comma delimiter
- **Headers**: First row contains column names
