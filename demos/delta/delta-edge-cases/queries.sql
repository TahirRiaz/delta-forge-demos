-- ============================================================================
-- Delta Edge Cases — Educational Queries
-- ============================================================================
-- WHAT: Tests Delta table behavior with boundary conditions: single-row
--       tables, wide schemas (30 columns), and empty tables.
-- WHY:  Real-world pipelines encounter these patterns regularly — config
--       singletons, denormalized wide tables, and pre-created staging tables
--       that start empty. Understanding how Delta handles them prevents
--       surprises in production.
-- HOW:  Each edge case creates a separate Delta table with its own
--       transaction log. Even a single-row table gets full ACID semantics,
--       and an empty table still has a valid schema in its metadata actions.
-- ============================================================================


-- ============================================================================
-- EXPLORE: The Singleton Pattern — One Row, Many Versions
-- ============================================================================
-- A "singleton" table holds exactly one row that gets updated in place.
-- This is common for application config, feature flags, or system state.
-- In Delta, each UPDATE creates a new version in the transaction log,
-- so you get full version history even for a single row.
--
-- After setup, the config_singleton table has version 1 (the baseline).
-- Let's see it before any updates:

ASSERT ROW_COUNT = 1
SELECT config_key, config_value, version, updated_by, updated_at
FROM {{zone_name}}.delta_demos.config_singleton;


-- ============================================================================
-- EVOLVE: Update config to version 2 — increase timeout
-- ============================================================================
-- The ops team doubles the timeout from 5000ms to 10000ms.
-- This UPDATE replaces the single Parquet file and creates a new Delta
-- transaction log entry — version 2.

ASSERT ROW_COUNT = 1
UPDATE {{zone_name}}.delta_demos.config_singleton
SET config_value = '{"max_connections":100,"timeout_ms":10000,"debug":false}',
    version = 2,
    updated_by = 'ops-team',
    updated_at = '2025-01-15 10:00:00';

-- Observe the config after the timeout increase:
ASSERT ROW_COUNT = 1
ASSERT VALUE version = 2
ASSERT VALUE updated_by = 'ops-team'
SELECT config_key, config_value, version, updated_by, updated_at
FROM {{zone_name}}.delta_demos.config_singleton;


-- ============================================================================
-- EVOLVE: Update config to version 3 — enable debug mode
-- ============================================================================
-- The dev lead enables debug mode for troubleshooting.
-- Another new version in the transaction log — version 3.

ASSERT ROW_COUNT = 1
UPDATE {{zone_name}}.delta_demos.config_singleton
SET config_value = '{"max_connections":100,"timeout_ms":10000,"debug":true}',
    version = 3,
    updated_by = 'dev-lead',
    updated_at = '2025-02-01 14:30:00';

-- Observe the config after enabling debug:
ASSERT ROW_COUNT = 1
ASSERT VALUE version = 3
ASSERT VALUE updated_by = 'dev-lead'
SELECT config_key, config_value, version, updated_by, updated_at
FROM {{zone_name}}.delta_demos.config_singleton;


-- ============================================================================
-- EVOLVE: Update config to version 4 — increase max connections
-- ============================================================================
-- The SRE team scales up max_connections from 100 to 200 for higher load.
-- This is the final update — version 4.

ASSERT ROW_COUNT = 1
UPDATE {{zone_name}}.delta_demos.config_singleton
SET config_value = '{"max_connections":200,"timeout_ms":10000,"debug":true}',
    version = 4,
    updated_by = 'sre-team',
    updated_at = '2025-03-01 09:00:00';

-- Observe the final config state:
ASSERT ROW_COUNT = 1
ASSERT VALUE version = 4
ASSERT VALUE updated_by = 'sre-team'
SELECT config_key, config_value, version, updated_by, updated_at
FROM {{zone_name}}.delta_demos.config_singleton;


-- ============================================================================
-- LEARN: How Delta Handles In-Place Updates on Single Rows
-- ============================================================================
-- Even though this table has only 1 row, Delta created multiple versions
-- in the transaction log (one per UPDATE). Each version wrote a new Parquet
-- file and marked the old one as removed. This means you could time-travel
-- back to any previous configuration state.
--
-- Let's verify the final state reflects the 4th update:

ASSERT ROW_COUNT = 1
ASSERT VALUE config_version = 4
ASSERT VALUE last_updater = 'sre-team'
SELECT version AS config_version,
       updated_by AS last_updater,
       updated_at AS last_update_time
FROM {{zone_name}}.delta_demos.config_singleton;


-- ============================================================================
-- EXPLORE: Wide Tables — 30 Columns of KPI Data
-- ============================================================================
-- Delta tables handle wide schemas without issue. The column metadata is
-- stored in the transaction log's schema action, and Parquet's columnar
-- format means queries that only access a few columns skip reading the rest.
-- Let's look at a subset of the 30 columns:

ASSERT ROW_COUNT = 20
SELECT id, name, m01_revenue, m03_profit, m04_margin_pct,
       m12_satisfaction, m16_uptime_pct
FROM {{zone_name}}.delta_demos.wide_metrics
ORDER BY id;


-- ============================================================================
-- LEARN: Columnar Advantage with Wide Tables
-- ============================================================================
-- With 30 columns in Parquet format, queries that filter or project a
-- subset of columns benefit from columnar pruning — only the needed
-- column chunks are read from disk. Let's compute a cross-column
-- metric to show all columns are accessible:

ASSERT ROW_COUNT = 12
SELECT name,
       m01_revenue,
       m02_cost,
       m03_profit,
       ROUND(m03_profit / m01_revenue * 100, 1) AS computed_margin_pct,
       m04_margin_pct AS stored_margin_pct
FROM {{zone_name}}.delta_demos.wide_metrics
WHERE m01_revenue > 150000
ORDER BY m03_profit DESC;


-- ============================================================================
-- EXPLORE: Empty Tables — Schema Without Data
-- ============================================================================
-- An empty Delta table has a valid transaction log with schema metadata
-- but zero data files. This is common for staging tables created before
-- data arrives. Queries against empty tables return zero rows (not errors),
-- and aggregates return NULL:

ASSERT ROW_COUNT = 1
ASSERT VALUE row_count = 0
ASSERT VALUE max_id IS NULL
SELECT COUNT(*) AS row_count,
       MAX(id) AS max_id,
       MIN(source_system) AS first_source
FROM {{zone_name}}.delta_demos.empty_staging;


-- ============================================================================
-- LEARN: Why Empty Tables Matter in Delta
-- ============================================================================
-- An empty Delta table is not the same as a nonexistent table. The
-- transaction log records the CREATE TABLE action with full schema
-- information. This means:
--   1. Schema enforcement is already active (inserts must match the schema)
--   2. The table can participate in MERGE, JOIN, or UNION queries
--   3. Downstream tools can discover the schema without waiting for data
--
-- Let's confirm the schema exists by selecting with a false predicate:

ASSERT ROW_COUNT = 0
SELECT id, source_system, raw_data, status, received_at
FROM {{zone_name}}.delta_demos.empty_staging
WHERE 1 = 0;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify singleton_row_count: exactly 1 row in singleton table
ASSERT ROW_COUNT = 1
SELECT * FROM {{zone_name}}.delta_demos.config_singleton;

-- Verify singleton_version: config updated to version 4
ASSERT VALUE version = 4
SELECT version FROM {{zone_name}}.delta_demos.config_singleton;

-- Verify singleton_updater: last update by sre-team
ASSERT VALUE updated_by = 'sre-team'
SELECT updated_by FROM {{zone_name}}.delta_demos.config_singleton;

-- Verify singleton_config_value: final config JSON is correct
ASSERT VALUE config_value = '{"max_connections":200,"timeout_ms":10000,"debug":true}'
SELECT config_value FROM {{zone_name}}.delta_demos.config_singleton;

-- Verify wide_row_count: 20 rows in wide_metrics
ASSERT VALUE cnt = 20
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.wide_metrics;

-- Verify wide_revenue_dec: id=12 revenue is 180000.0
ASSERT VALUE m01_revenue = 180000.0
SELECT m01_revenue FROM {{zone_name}}.delta_demos.wide_metrics WHERE id = 12;

-- Verify wide_max_revenue: maximum revenue is 190000.0
ASSERT VALUE max_rev = 190000.0
SELECT MAX(m01_revenue) AS max_rev FROM {{zone_name}}.delta_demos.wide_metrics;

-- Verify wide_total_profit: sum of all profits is 1168000.0
ASSERT VALUE total = 1168000.0
SELECT SUM(m03_profit) AS total FROM {{zone_name}}.delta_demos.wide_metrics;

-- Verify empty_row_count: empty_staging has 0 rows
ASSERT ROW_COUNT = 0
SELECT * FROM {{zone_name}}.delta_demos.empty_staging;

-- Verify empty_max_is_null: MAX(id) on empty table returns NULL
ASSERT VALUE max_id IS NULL
SELECT MAX(id) AS max_id FROM {{zone_name}}.delta_demos.empty_staging;
