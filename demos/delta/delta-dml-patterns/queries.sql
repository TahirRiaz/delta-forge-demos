-- ============================================================================
-- Delta DML Patterns — Complex DELETE & UPDATE — Queries
-- ============================================================================
-- WHAT: Delta Lake enables INSERT, UPDATE, and DELETE on Parquet-based tables
--       by recording file-level add/remove actions in the transaction log.
-- WHY:  Traditional data lakes (raw Parquet, ORC) are append-only — you cannot
--       update a price, delete a cancelled order, or fix a typo without
--       rewriting entire files manually. Delta makes these operations atomic.
-- HOW:  UPDATE and DELETE read affected data files, apply changes, write new
--       files, and atomically commit "remove old file + add new file" actions
--       to the Delta log. Readers see a consistent snapshot at every version.
-- ============================================================================
--
-- This script performs 4 DML operations on the 60-row order_history table,
-- with SELECT queries between each to observe the effects:
--   1. DELETE — purge cancelled orders older than 2024-06-01 (8 removed)
--   2. UPDATE — bulk fulfillment: pending us-east orders -> shipped (6 updated)
--   3. UPDATE — price discount: 10% off electronics products (10 updated)
--   4. DELETE — archive old completed orders before 2024-01-01 (5 removed)
--
-- Final row count: 60 - 8 - 5 = 47
-- ============================================================================


-- ============================================================================
-- BASELINE: Inspect order_history before any DML
-- ============================================================================
-- 60 rows across 4 regions, 4 statuses. Let's see the starting distribution.

ASSERT ROW_COUNT = 4
ASSERT VALUE total_value = 4488.96 WHERE status = 'completed'
SELECT status, COUNT(*) AS order_count,
       ROUND(SUM(price * qty), 2) AS total_value
FROM {{zone_name}}.delta_demos.order_history
GROUP BY status
ORDER BY total_value DESC;


-- ============================================================================
-- DML 1: DELETE — Purge cancelled orders older than 2024-06-01
-- ============================================================================
-- Multi-predicate DELETE: status='cancelled' AND order_date < '2024-06-01'.
-- This is more surgical than deleting all cancelled orders — recent
-- cancellations might still be useful for analytics.
--
-- In the Delta log, this DELETE scans data files for matching rows, rewrites
-- only the affected files (omitting the deleted rows), and records remove/add
-- actions atomically.
--
-- Removes 8 rows: ids 1-8
-- Remaining: 52 rows

ASSERT ROW_COUNT = 8
DELETE FROM {{zone_name}}.delta_demos.order_history
WHERE status = 'cancelled' AND order_date < '2024-06-01';

-- Confirm the old cancelled orders are gone while recent ones remain.
-- 12 original cancelled - 8 old = 4 recent cancelled remain
ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 4
SELECT id, customer, product, status, order_date
FROM {{zone_name}}.delta_demos.order_history
WHERE status = 'cancelled'
ORDER BY order_date;


-- ============================================================================
-- DML 2: UPDATE — Bulk fulfillment for pending us-east orders
-- ============================================================================
-- All pending orders in us-east are bulk-updated to 'shipped'. This pattern
-- simulates a warehouse fulfillment event where an entire region's backlog
-- is shipped at once.
--
-- Because Delta commits are atomic, either all 6 orders are updated or none
-- are — there is no risk of a partial update leaving some orders in an
-- inconsistent state.
--
-- Updates 6 rows: ids 14, 15, 16, 17, 18, 19

ASSERT ROW_COUNT = 6
UPDATE {{zone_name}}.delta_demos.order_history
SET status = 'shipped'
WHERE status = 'pending' AND region = 'us-east';

-- Confirm all us-east orders that were pending are now shipped.
-- Verify no pending orders remain in us-east
ASSERT VALUE pending_us_east = 0
SELECT COUNT(*) AS pending_us_east FROM {{zone_name}}.delta_demos.order_history WHERE status = 'pending' AND region = 'us-east';

ASSERT ROW_COUNT = 6
SELECT id, customer, product, status, region
FROM {{zone_name}}.delta_demos.order_history
WHERE region = 'us-east' AND status = 'shipped'
ORDER BY id;


-- ============================================================================
-- DML 3: UPDATE — 10% price discount for electronics products
-- ============================================================================
-- Electronics products: Laptop, Monitor, Tablet, Headphones, Smartwatch.
-- This UPDATE touches rows across multiple regions and statuses, showing
-- that Delta DML predicates can span the entire table. The WHERE clause
-- operates on column values, not partition boundaries.
--
-- Updates 10 rows: ids 14, 15, 19, 20, 21, 22, 23, 24, 25, 26
-- Laptop:     999.99 * 0.90 = 899.99
-- Monitor:    349.99 * 0.90 = 314.99
-- Tablet:     499.99 * 0.90 = 449.99
-- Headphones: 199.99 * 0.90 = 179.99
-- Smartwatch: 249.99 * 0.90 = 224.99

ASSERT ROW_COUNT = 10
UPDATE {{zone_name}}.delta_demos.order_history
SET price = ROUND(price * 0.90, 2)
WHERE product IN ('Laptop', 'Monitor', 'Tablet', 'Headphones', 'Smartwatch');

-- Verify discounted prices: Laptop 999.99->899.99, Monitor 349.99->314.99,
-- Tablet 499.99->449.99, Headphones 199.99->179.99, Smartwatch 249.99->224.99
ASSERT VALUE price = 899.99
SELECT price FROM {{zone_name}}.delta_demos.order_history WHERE id = 20;

ASSERT ROW_COUNT = 10
SELECT id, customer, product, price, status, region,
       ROUND(price / 0.90, 2) AS original_price
FROM {{zone_name}}.delta_demos.order_history
WHERE product IN ('Laptop', 'Monitor', 'Tablet', 'Headphones', 'Smartwatch')
ORDER BY product, id;


-- ============================================================================
-- DML 4: DELETE — Archive old completed orders before 2024-01-01
-- ============================================================================
-- Removes completed orders with order_date < '2024-01-01'. In a production
-- system these rows might be moved to a cold-storage table first; here we
-- simply delete them to demonstrate a second DELETE pattern.
--
-- Removes 5 rows: ids 9, 10, 11, 12, 13
-- Final: 52 - 5 = 47 rows

ASSERT ROW_COUNT = 5
DELETE FROM {{zone_name}}.delta_demos.order_history
WHERE status = 'completed' AND order_date < '2024-01-01';


-- ============================================================================
-- EXPLORE: Regional Order Summary After All DML Operations
-- ============================================================================
-- After all 4 DML operations, let's see how orders are distributed across
-- the 4 regions. All regions should still be represented, even though
-- DELETEs removed rows from each. Final: 60 - 8 - 5 = 47 rows.

-- Verify final row count is 47
ASSERT VALUE total_remaining = 47
SELECT COUNT(*) AS total_remaining FROM {{zone_name}}.delta_demos.order_history;

ASSERT ROW_COUNT = 4
ASSERT VALUE order_count = 13 WHERE region = 'eu-west'
SELECT region,
       COUNT(*) AS order_count,
       SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending,
       SUM(CASE WHEN status = 'shipped' THEN 1 ELSE 0 END) AS shipped,
       SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
       SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled
FROM {{zone_name}}.delta_demos.order_history
GROUP BY region
ORDER BY region;


-- ============================================================================
-- EXPLORE: Non-Electronics Products Are Unaffected
-- ============================================================================
-- The price discount UPDATE only targeted electronics. Non-electronics
-- products should retain their original prices, demonstrating that Delta
-- UPDATE predicates are precise — only matching rows are modified.

ASSERT ROW_COUNT = 10
ASSERT VALUE min_price = 150.00 WHERE product = 'Desk'
SELECT product, COUNT(*) AS orders,
       MIN(price) AS min_price, MAX(price) AS max_price
FROM {{zone_name}}.delta_demos.order_history
WHERE product NOT IN ('Laptop', 'Monitor', 'Tablet', 'Headphones', 'Smartwatch')
GROUP BY product
ORDER BY product;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_row_count: 60 - 8 - 5 = 47 rows remain
ASSERT ROW_COUNT = 47
SELECT * FROM {{zone_name}}.delta_demos.order_history;

-- Verify cancelled_old_gone: cancelled orders before 2024-06-01 were purged
ASSERT VALUE cnt = 0
SELECT COUNT(*) FILTER (WHERE status = 'cancelled' AND order_date < '2024-06-01') AS cnt FROM {{zone_name}}.delta_demos.order_history;

-- Verify us_east_shipped: 6 pending us-east orders were bulk-shipped
ASSERT VALUE cnt = 6
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.order_history WHERE status = 'shipped' AND region = 'us-east';

-- Verify discounted_price: id=20 Laptop discounted 10% to 899.99
ASSERT VALUE price = 899.99
SELECT price FROM {{zone_name}}.delta_demos.order_history WHERE id = 20;

-- Verify old_completed_gone: completed orders before 2024-01-01 were archived
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.order_history WHERE status = 'completed' AND order_date < '2024-01-01';

-- Verify region_count: all 4 regions still represented
ASSERT VALUE cnt = 4
SELECT COUNT(DISTINCT region) AS cnt FROM {{zone_name}}.delta_demos.order_history;

-- Verify remaining_pending: 8 pending orders remain after bulk shipment
ASSERT VALUE cnt = 8
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.order_history WHERE status = 'pending';

-- Verify us_west_unchanged: non-electronics price unaffected
ASSERT VALUE price = 120.00
SELECT price FROM {{zone_name}}.delta_demos.order_history WHERE id = 28;
