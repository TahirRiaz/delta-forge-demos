# Delta Protocol & Table Features

Demonstrates Delta table properties and feature configuration using
TBLPROPERTIES to enable CDC, deletion vectors, and append-only mode.

## Data Story

A SaaS product catalog tracks items with Change Data Feed enabled,
allowing downstream systems to see exactly what changed. Trial items
are converted to active (with a 20% discount), and low-value items
are deleted. A separate append-only audit trail ensures compliance
events can never be modified or deleted.

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `feature_demo` | Delta Table | 27 (final) | Product catalog with CDC enabled |
| `audit_trail` | Delta Table | 25 | Append-only compliance audit log |

## Schema

**feature_demo:** `id INT, name VARCHAR, category VARCHAR, value DOUBLE, status VARCHAR, created_date VARCHAR`
**audit_trail:** `id BIGINT, event_type VARCHAR, actor VARCHAR, resource VARCHAR, action VARCHAR, timestamp_utc VARCHAR`

## Features Demonstrated

1. **Change Data Feed** — `delta.enableChangeDataFeed = 'true'`
2. **Append-Only** — `delta.appendOnly = 'true'` for audit trail
3. **CDC + Updates** — status and value changes tracked
4. **Deletion vectors** — efficient deletion of low-value items
5. **Feature combinations** — multiple properties on single table

## Verification

8 automated PASS/FAIL checks verify row counts, status transitions,
discount calculations, deleted items, category distribution, audit
event counts, and failed login tracking.
