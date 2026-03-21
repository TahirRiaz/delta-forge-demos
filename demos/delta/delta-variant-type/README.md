# Delta Variant Data Type — Semi-Structured Data

Demonstrates semi-structured data patterns in Delta tables using VARCHAR
columns to store JSON-like strings, simulating VARIANT-type behavior.

## Data Story

An API gateway logs events with varying payload structures. Login events
carry user/ip/method fields, purchase events have item/amount/currency,
error events contain code/message/endpoint, and system events track
action/component/duration. Each payload is stored as a JSON string in a
VARCHAR column, demonstrating flexible schema patterns within a fixed
Delta table schema.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `api_events` | Delta Table | 50 (final) | API gateway events with JSON payloads |

## Schema

**api_events:** `id INT, event_type VARCHAR, source VARCHAR, payload VARCHAR, metadata VARCHAR, severity VARCHAR, created_at VARCHAR`

## Patterns Demonstrated

1. **JSON-like payloads** — different structures per event type in VARCHAR
2. **Event type filtering** — query by login, purchase, error, system
3. **Payload pattern matching** — LIKE queries on JSON strings
4. **Severity classification** — UPDATE to reclassify error severity
5. **Source aggregations** — distinct source tracking across events

## Verification

8 automated PASS/FAIL checks verify total row count, event type
distribution (login=20, purchase=15, error=10, system=5), critical
severity count (6), payload completeness, and distinct source count (4).
