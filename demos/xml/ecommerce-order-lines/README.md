# XML E-Commerce — Order Line Explosion

Demonstrates how Delta Forge handles deeply nested XML with explode_paths, CDATA sections, exclude_paths, column_mappings, and xml_paths. Two daily order batch exports are read into an exploded line-item table and a per-order summary table.

## Data Story

An online retailer exports order batches as XML twice daily. Each order contains a customer header, repeating line items with nested variant details (size/color), and an internal audit block used by finance (excluded from analytics). Some orders have self-closing flag elements (`<gift_wrap/>`, `<express/>`), and product descriptions use CDATA to embed HTML formatting.

| File | Batch | Orders | Line Items | Special Elements |
|------|-------|--------|------------|-----------------|
| `01_orders_morning.xml` | B2025-001 | 3 | 7 | gift_wrap (ORD-1001), express (ORD-1002) |
| `02_orders_afternoon.xml` | B2025-002 | 2 | 4 | gift_wrap + express (ORD-1004) |
| **Total** | | **5** | **11** | |

## XML Structure (3+ levels deep)

```
orders (@batch_id, @export_date)
└── order (@id, @status)
    ├── customer
    │   ├── name
    │   ├── email
    │   └── tier
    ├── order_date
    ├── items
    │   └── item (@sku)          ← explode_paths target
    │       ├── product
    │       ├── description      ← CDATA with HTML
    │       ├── quantity
    │       ├── unit_price
    │       └── variant          ← deep nesting (level 3)
    │           ├── size
    │           └── color
    ├── gift_wrap                ← self-closing flag
    ├── express                  ← self-closing flag
    ├── shipping_total
    └── internal_audit           ← exclude_paths target
        ├── cost_center
        └── margin_pct
```

## Tables

### `order_lines` — One row per line item (11 rows)

Exploded via `explode_paths`. Order-level fields duplicated per item.

| Column | Source | Notes |
|--------|--------|-------|
| `order_id` | `@id` | Column mapping from `attr_id` |
| `order_status` | `@status` | Column mapping |
| `customer_name` | `customer/name` | Deep nesting (level 2) |
| `customer_tier` | `customer/tier` | Deep nesting |
| `order_date` | `order_date` | Order header field |
| `sku` | `item/@sku` | Column mapping from attribute |
| `product` | `item/product` | Line item detail |
| `description` | `item/description` | CDATA with HTML preserved |
| `quantity` | `item/quantity` | Line item quantity |
| `unit_price` | `item/unit_price` | Per-unit price |
| `item_size` | `item/variant/size` | Deep nesting (level 3), column mapping |
| `item_color` | `item/variant/color` | Deep nesting (level 3), column mapping |

### `order_summary` — One row per order (5 rows)

Non-exploded. Repeating items counted. Customer kept as JSON blob.

| Column | Source | Notes |
|--------|--------|-------|
| `order_id` | `@id` | Column mapping |
| `order_status` | `@status` | Column mapping |
| `customer` | `customer/*` | Kept as JSON string via `xml_paths` |
| `order_date` | `order_date` | Order header |
| `item` | `items/item` (count) | `default_repeat_handling: count` |
| `shipping_total` | `shipping_total` | Order total |

## How to Verify

Run the **Summary** query (#13) to see PASS/FAIL for each check:

```sql
SELECT 'exploded_rows' AS check_name,
       CASE WHEN COUNT(*) = 11 THEN 'PASS' ELSE 'FAIL' END AS result
FROM external.xml.order_lines
UNION ALL ...
ORDER BY check_name;
```

## What This Tests

1. **Deep nesting (3+ levels)** — `order/items/item/variant/color` flattened to `item_color`
2. **explode_paths** — One row per `<item>` element, order fields duplicated
3. **CDATA sections** — `<![CDATA[<b>HTML</b>]]>` extracted as raw text with HTML preserved
4. **exclude_paths** — `internal_audit` block (cost_center, margin_pct) hidden from both tables
5. **column_mappings** — Deep XPaths renamed to friendly names (item_size, item_color, order_id)
6. **xml_paths** — Customer subtree kept as JSON blob in summary table (not flattened)
7. **nested_output_format** — JSON format for preserved subtrees
8. **default_repeat_handling: count** — Line items counted per order in summary view
9. **Self-closing elements** — `<gift_wrap/>` and `<express/>` extracted as columns
