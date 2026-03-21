# Delta MERGE Comprehensive — All Clause Patterns

Demonstrates all three MERGE clause types in a single operation against a
CRM customer database.

## Data Story

A CRM system performs a comprehensive customer data merge. The updates table
contains three categories of records: active customers with refreshed data
(UPDATE), closed accounts to be removed (DELETE), and brand new customers
to be onboarded (INSERT). A single MERGE statement handles all three
patterns simultaneously.

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `customer_master` | Delta Table | 47 (final) | Master customer table (target) |
| `customer_updates` | Delta Table | 25 | Staged changes (source) |

## MERGE Clauses

| Clause | Condition | Action | Count |
|--------|-----------|--------|-------|
| WHEN MATCHED AND status='active' | Source has active status | UPDATE all fields | 12 |
| WHEN MATCHED AND status='closed' | Source has closed status | DELETE row | 3 |
| WHEN NOT MATCHED | New customer ID | INSERT full row | 10 |

## Row Accounting

| Category | IDs | Count |
|----------|-----|-------|
| Original rows | 1-40 | 40 |
| Updated (active match) | 2, 5, 8, 11, 14, 17, 20, 23, 26, 29, 32, 35 | 12 |
| Deleted (closed match) | 7, 19, 37 | 3 |
| Inserted (not matched) | 41-50 | 10 |
| Unchanged | remaining 25 IDs | 25 |
| **Final count** | | **47** |

## Verification

8 automated PASS/FAIL checks verify total row count, deleted accounts,
updated emails, new customer inserts, tier changes, balance updates,
unchanged records, and status distribution.
