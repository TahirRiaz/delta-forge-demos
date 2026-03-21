# Delta MERGE — Upsert, Conditional Update & Delete

Demonstrates the MERGE INTO statement for complex upsert operations with
all three merge clauses.

## Data Story

A loyalty program merges daily customer updates into the master table.
Existing customers get updated spending totals and tier recalculations,
new customers are inserted, and stale bronze-tier customers not in the
update batch are removed.

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `customers` | Delta Table | 21 (final) | Master customer table (target) |
| `customer_updates` | Delta Table | 15 | Staged updates (source) |

## MERGE Clauses

| Clause | Action | Count |
|--------|--------|-------|
| WHEN MATCHED | Update spending + recalculate tier | 10 |
| WHEN NOT MATCHED | Insert new customer | 5 |
| WHEN NOT MATCHED BY SOURCE AND bronze | Delete stale record | 4 |

## Verification

10 automated PASS/FAIL checks verify matched updates, new inserts,
deleted records, tier promotions, and unchanged customers.
