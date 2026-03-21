# Delta Time Travel — Version History Deep Dive

Demonstrates deep Delta time travel capabilities using an IoT sensor
monitoring system with multiple versioned operations.

## Data Story

An IoT monitoring system tracks sensor readings across 4 locations
(lab-a, lab-b, warehouse, field). Each operation creates a new table
version. Time travel lets engineers query any historical snapshot —
comparing readings before and after calibration, recovering accidentally
deleted data, and auditing change history.

## Table

| Object | Type | Rows | Versions | Purpose |
|--------|------|------|----------|---------|
| `sensor_readings` | Delta Table | 50 (final) | 4 (V0-V3) | Versioned IoT readings |

## Version History

| Version | Operation | Rows | Key Changes |
|---------|-----------|------|-------------|
| 0 | CREATE + INSERT | 40 | Initial 40 readings (10 per location) |
| 1 | UPDATE | 40 | Lab-a sensors calibrated (reading += 2.5) |
| 2 | DELETE | 35 | 5 faulty field sensors removed |
| 3 | INSERT | 50 | 15 new sensor readings added |

## Schema

**sensor_readings:** `id INT, sensor_id VARCHAR, reading DOUBLE, unit VARCHAR, location VARCHAR, recorded_at VARCHAR`

## Verification

8 automated PASS/FAIL checks verify current state and historical snapshots
using `VERSION AS OF` time travel queries. Use the GUI version browser to
explore each historical version.
