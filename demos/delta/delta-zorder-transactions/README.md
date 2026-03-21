# Delta Z-ORDER & Transaction Management

Demonstrates OPTIMIZE with Z-ORDER BY for multi-dimensional data co-location
and Delta transaction management across multiple batch inserts.

## Data Story

A web analytics platform collects events in 3 waves: page views (40 events),
clicks (30 events), and conversions (30 events). After loading, OPTIMIZE
with Z-ORDER BY reorganizes the data files so queries filtering by event_type
and/or country read fewer files. Short pageview sessions are then marked
as bounced to demonstrate post-optimize updates.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `web_analytics` | Delta Table | 100 (final) | Web events with Z-ORDER optimization |

## Schema

**web_analytics:** `id INT, session_id VARCHAR, user_id VARCHAR, page_url VARCHAR, event_type VARCHAR, duration_ms INT, browser VARCHAR, country VARCHAR, event_date VARCHAR`

## Patterns Demonstrated

1. **Multi-batch inserts** — 3 separate INSERT operations creating multiple files
2. **OPTIMIZE ZORDER BY** — co-locate data by (event_type, country)
3. **Post-optimize integrity** — row count preserved after optimization
4. **Multi-dimensional queries** — filter by event_type AND country
5. **Transaction-aware updates** — modify data after optimization

## Verification

8 automated PASS/FAIL checks verify total row count, event type distribution,
US event count, bounced session detection, URL prefix updates, and
country distribution.
