# Delta Generated & Identity Columns

Demonstrates computed columns using CTEs and sequential ID generation
patterns in Delta tables.

## Data Story

An online store tracks order line items where subtotal, tax, and total are
computed at insertion time via CTEs. Batch 1 (30 items) uses an 8% tax rate
for domestic orders, while Batch 2 (10 items) uses a 10% rate for a different
jurisdiction. A separate event sequence table tracks user activity with
gap-free sequential IDs.

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `order_items` | Delta Table | 40 | Order lines with computed subtotal/tax/total |
| `event_sequence` | Delta Table | 25 | Sequential event tracking with gap-free IDs |

## Schema

**order_items:** `id INT, product VARCHAR, qty INT, unit_price DOUBLE, subtotal DOUBLE, tax DOUBLE, total DOUBLE, order_date VARCHAR`

**event_sequence:** `seq_id BIGINT, event_type VARCHAR, payload VARCHAR, created_at VARCHAR`

## Patterns

- **CTE-computed columns:** `subtotal = qty * unit_price`, `tax = subtotal * rate`, `total = subtotal + tax`
- **Multi-rate tax:** Batch 1 at 8%, Batch 2 at 10%
- **Sequential IDs:** Gap-free 1–25 sequence for event tracking

## Verification

8 automated PASS/FAIL checks verify: row counts, subtotal formula, 8% tax
computation, 10% tax computation, total formula, event count, sequential ID
integrity, and event type distribution.
