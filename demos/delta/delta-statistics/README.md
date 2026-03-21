# Delta Statistics — Min/Max & Data Skipping

Demonstrates how Delta table statistics (per-file min/max, null counts)
enable efficient data skipping during query execution.

## Data Story

An industrial monitoring system collects sensor readings in 3 waves:
low-range environment sensors (batch 1), mid-range vibration/acoustic/thermal
sensors (batch 2), and high-range power/torque/RPM sensors (batch 3).
Each batch has non-overlapping value ranges, allowing Delta to skip entire
files when filtering by value. Some quality scores are NULLed out to
demonstrate NULL statistics.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `sensor_readings` | Delta Table | 60 (final) | Readings with distinct per-batch value ranges |

## Schema

**sensor_readings:** `id INT, device VARCHAR, category VARCHAR, value DOUBLE, quality_score INT, description VARCHAR, recorded_at VARCHAR`

## Patterns Demonstrated

1. **Per-batch min/max** — non-overlapping value ranges across inserts
2. **NULL statistics** — 15 rows with NULL quality_score
3. **Data skipping** — range filters touch only relevant batches
4. **String truncation** — batch 3 descriptions exceed 32 chars
5. **Range verification** — MIN/MAX aggregations per batch

## Verification

8 automated PASS/FAIL checks verify row counts, per-batch value ranges,
NULL counts, range-filtered queries, no-overlap guarantees, and
description length validation.
