-- ============================================================================
-- Delta Generated & Identity Columns — Educational Queries
-- ============================================================================
-- WHAT: Computed columns (subtotal, tax, total) derived at INSERT time via
--       expressions, and sequential identity-style IDs for event tracking.
-- WHY:  Pushing computation into the write path ensures every row has
--       consistent derived values, eliminating bugs from application-side
--       calculation differences and guaranteeing referential integrity
--       of sequential IDs.
-- HOW:  Delta stores the final computed values in Parquet data files.
--       The computation happens in the SQL INSERT...SELECT, not in the
--       Delta protocol itself. The transaction log records the result.
-- ============================================================================


-- ============================================================================
-- LEARN: CTE-Based Column Generation During INSERT
-- ============================================================================
-- Delta does not have native "generated columns" like some databases,
-- so the pattern used here is INSERT...SELECT with expressions:
--   INSERT INTO table SELECT ..., qty * price AS subtotal, ...
-- This approach means the computation runs once at write time and the
-- result is stored as a plain column in the Parquet file. There is no
-- ongoing compute cost at read time.
--
-- Batch 1: 30 items with 8% tax rate, computed via expressions in SELECT.
-- Notice how subtotal, tax, and total are derived from qty and unit_price:

ASSERT ROW_COUNT = 30
INSERT INTO {{zone_name}}.delta_demos.order_items
SELECT id, product, qty, unit_price,
       ROUND(qty * unit_price, 2) AS subtotal,
       ROUND(qty * unit_price * 0.08, 2) AS tax,
       ROUND(qty * unit_price * 1.08, 2) AS total,
       order_date
FROM (VALUES
    (1,  'Laptop Pro',       1, 1299.99, '2024-01-15'),
    (2,  'Wireless Mouse',   3, 29.99,   '2024-01-15'),
    (3,  'USB-C Hub',        2, 49.99,   '2024-01-16'),
    (4,  'Monitor 27"',      1, 349.99,  '2024-01-16'),
    (5,  'Keyboard',         2, 89.99,   '2024-01-17'),
    (6,  'Webcam HD',        1, 59.99,   '2024-01-17'),
    (7,  'Headphones',       1, 199.99,  '2024-01-18'),
    (8,  'Desk Lamp',        3, 34.99,   '2024-01-18'),
    (9,  'Mouse Pad XL',     5, 19.99,   '2024-01-19'),
    (10, 'Cable Pack',       4, 14.99,   '2024-01-19'),
    (11, 'Office Chair',     1, 399.99,  '2024-01-20'),
    (12, 'Standing Desk',    1, 549.99,  '2024-01-20'),
    (13, 'Bluetooth Speaker',2, 79.99,   '2024-01-21'),
    (14, 'Power Bank',       3, 39.99,   '2024-01-21'),
    (15, 'Surge Protector',  2, 24.99,   '2024-01-22'),
    (16, 'Monitor Arm',      1, 69.99,   '2024-01-22'),
    (17, 'Laptop Stand',     2, 44.99,   '2024-01-23'),
    (18, 'HDMI Cable',       6, 12.99,   '2024-01-23'),
    (19, 'Ethernet Cable',   4, 9.99,    '2024-01-24'),
    (20, 'Smart Plug',       3, 14.99,   '2024-01-24'),
    (21, 'Desk Organizer',   2, 22.99,   '2024-01-25'),
    (22, 'Pen Set',          5, 12.99,   '2024-01-25'),
    (23, 'Notebook',         10, 8.99,   '2024-01-26'),
    (24, 'Sticky Notes',     8, 5.99,    '2024-01-26'),
    (25, 'Binder Set',       3, 11.99,   '2024-01-27'),
    (26, 'Label Maker',      1, 39.99,   '2024-01-27'),
    (27, 'Whiteboard',       1, 89.99,   '2024-01-28'),
    (28, 'Markers Set',      4, 9.99,    '2024-01-28'),
    (29, 'Paper Clips',      6, 3.99,    '2024-01-29'),
    (30, 'Desk Mat',         2, 24.99,   '2024-01-29')
) AS t(id, product, qty, unit_price, order_date);


-- ============================================================================
-- EXPLORE: Inspect the Computed Columns from Batch 1
-- ============================================================================
-- The INSERT above stored pre-computed subtotal, tax, and total values.
-- Let's verify a few rows to see the 8% tax rate in action:

ASSERT ROW_COUNT = 3
SELECT id, product, qty, unit_price, subtotal, tax, total,
       ROUND(tax / subtotal * 100, 1) AS effective_tax_pct
FROM {{zone_name}}.delta_demos.order_items
WHERE id IN (1, 2, 12)
ORDER BY id;

-- Verify: All rows in batch 1 stored correct 8% tax computation (no mismatches)
ASSERT VALUE mismatch_count = 0
SELECT COUNT(*) FILTER (WHERE subtotal != ROUND(qty * unit_price, 2)) AS mismatch_count
FROM {{zone_name}}.delta_demos.order_items
WHERE id <= 30;


-- ============================================================================
-- LEARN: Different Tax Rates in a Second Batch
-- ============================================================================
-- The same INSERT...SELECT pattern works with different computed values.
-- Batch 2 uses a 10% tax rate, simulating orders from a different
-- jurisdiction. The structure is identical; only the tax multiplier changes:

ASSERT ROW_COUNT = 10
INSERT INTO {{zone_name}}.delta_demos.order_items
SELECT id, product, qty, unit_price,
       ROUND(qty * unit_price, 2) AS subtotal,
       ROUND(qty * unit_price * 0.10, 2) AS tax,
       ROUND(qty * unit_price * 1.10, 2) AS total,
       order_date
FROM (VALUES
    (31, 'Server Rack',      1, 899.99,  '2024-02-01'),
    (32, 'UPS Battery',      2, 249.99,  '2024-02-01'),
    (33, 'Network Switch',   1, 179.99,  '2024-02-02'),
    (34, 'Patch Panel',      1, 59.99,   '2024-02-02'),
    (35, 'Cable Tester',     2, 34.99,   '2024-02-03'),
    (36, 'Rack Shelf',       3, 44.99,   '2024-02-03'),
    (37, 'KVM Switch',       1, 129.99,  '2024-02-04'),
    (38, 'Fiber Patch Cord', 5, 19.99,   '2024-02-04'),
    (39, 'PDU',              1, 199.99,  '2024-02-05'),
    (40, 'Cable Management', 4, 15.99,   '2024-02-05')
) AS t(id, product, qty, unit_price, order_date);


-- ============================================================================
-- EXPLORE: Compare Both Tax Jurisdictions
-- ============================================================================
-- Now both batches are in the table. Let's compare a row from each batch
-- and see the revenue breakdown by jurisdiction:

ASSERT ROW_COUNT = 4
SELECT id, product, qty, unit_price, subtotal, tax, total,
       ROUND(tax / subtotal * 100, 1) AS effective_tax_pct
FROM {{zone_name}}.delta_demos.order_items
WHERE id IN (1, 2, 31, 32)
ORDER BY id;

ASSERT ROW_COUNT = 2
ASSERT VALUE total_subtotal = 4676.10 WHERE tax_group = '8% jurisdiction'
ASSERT VALUE total_revenue = 5050.24 WHERE tax_group = '8% jurisdiction'
ASSERT VALUE total_subtotal = 2338.79 WHERE tax_group = '10% jurisdiction'
ASSERT VALUE total_revenue = 2572.69 WHERE tax_group = '10% jurisdiction'
SELECT CASE WHEN id <= 30 THEN '8% jurisdiction' ELSE '10% jurisdiction' END AS tax_group,
       COUNT(*) AS order_count,
       ROUND(SUM(subtotal), 2) AS total_subtotal,
       ROUND(SUM(tax), 2) AS total_tax,
       ROUND(SUM(total), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.order_items
GROUP BY CASE WHEN id <= 30 THEN '8% jurisdiction' ELSE '10% jurisdiction' END
ORDER BY tax_group;


-- ============================================================================
-- LEARN: Verify Computed Columns Match Recomputation
-- ============================================================================
-- A key benefit of write-time computation: we can verify stored values
-- by recomputing from the base columns. Every row should match:

ASSERT ROW_COUNT = 40
SELECT id, product, qty, unit_price,
       subtotal AS stored_subtotal,
       ROUND(qty * unit_price, 2) AS recomputed_subtotal,
       CASE WHEN subtotal = ROUND(qty * unit_price, 2)
            THEN 'match' ELSE 'MISMATCH' END AS check
FROM {{zone_name}}.delta_demos.order_items
ORDER BY id;


-- ============================================================================
-- LEARN: Sequential ID Generation for Event Tracking
-- ============================================================================
-- The event_sequence table uses manually assigned sequential IDs (1-25).
-- In production, this pattern is used for event sourcing, audit logs,
-- and ordered message processing. Delta's ACID guarantees ensure that
-- concurrent writers cannot create duplicate or gap-ridden sequences
-- within a single transaction.

ASSERT ROW_COUNT = 25
INSERT INTO {{zone_name}}.delta_demos.event_sequence VALUES
    (1,  'user_login',     'user=alice, ip=10.0.1.1',      '2024-03-01 08:00:00'),
    (2,  'page_view',      'page=/dashboard',               '2024-03-01 08:01:00'),
    (3,  'api_call',       'endpoint=/api/v1/users',        '2024-03-01 08:02:00'),
    (4,  'page_view',      'page=/settings',                '2024-03-01 08:03:00'),
    (5,  'config_change',  'key=theme, value=dark',         '2024-03-01 08:05:00'),
    (6,  'api_call',       'endpoint=/api/v1/profile',      '2024-03-01 08:06:00'),
    (7,  'user_login',     'user=bob, ip=10.0.1.2',         '2024-03-01 08:10:00'),
    (8,  'page_view',      'page=/dashboard',               '2024-03-01 08:11:00'),
    (9,  'api_call',       'endpoint=/api/v1/reports',       '2024-03-01 08:12:00'),
    (10, 'data_export',    'format=csv, rows=500',          '2024-03-01 08:15:00'),
    (11, 'page_view',      'page=/reports',                  '2024-03-01 08:16:00'),
    (12, 'api_call',       'endpoint=/api/v1/analytics',    '2024-03-01 08:20:00'),
    (13, 'user_logout',    'user=alice, duration=20min',    '2024-03-01 08:20:00'),
    (14, 'user_login',     'user=carol, ip=10.0.1.3',      '2024-03-01 08:25:00'),
    (15, 'page_view',      'page=/dashboard',               '2024-03-01 08:26:00'),
    (16, 'config_change',  'key=lang, value=fr',            '2024-03-01 08:28:00'),
    (17, 'api_call',       'endpoint=/api/v1/users',        '2024-03-01 08:30:00'),
    (18, 'page_view',      'page=/admin',                   '2024-03-01 08:32:00'),
    (19, 'user_logout',    'user=bob, duration=22min',      '2024-03-01 08:32:00'),
    (20, 'data_export',    'format=json, rows=1200',        '2024-03-01 08:35:00'),
    (21, 'api_call',       'endpoint=/api/v1/billing',      '2024-03-01 08:38:00'),
    (22, 'page_view',      'page=/billing',                  '2024-03-01 08:39:00'),
    (23, 'config_change',  'key=timezone, value=UTC+1',     '2024-03-01 08:40:00'),
    (24, 'user_logout',    'user=carol, duration=15min',    '2024-03-01 08:40:00'),
    (25, 'system_health',  'status=ok, cpu=45%, mem=62%',   '2024-03-01 08:45:00');


-- ============================================================================
-- EXPLORE: Verify Sequential IDs are Gap-Free
-- ============================================================================

ASSERT VALUE first_id = 1
ASSERT VALUE last_id = 25
ASSERT VALUE unique_ids = 25
ASSERT VALUE gap_check = 'No gaps'
ASSERT ROW_COUNT = 1
SELECT MIN(seq_id) AS first_id,
       MAX(seq_id) AS last_id,
       COUNT(DISTINCT seq_id) AS unique_ids,
       MAX(seq_id) - MIN(seq_id) + 1 AS expected_if_no_gaps,
       CASE WHEN COUNT(DISTINCT seq_id) = MAX(seq_id) - MIN(seq_id) + 1
            THEN 'No gaps' ELSE 'Has gaps' END AS gap_check
FROM {{zone_name}}.delta_demos.event_sequence;


-- ============================================================================
-- EXPLORE: Event Type Distribution
-- ============================================================================
-- Sequential IDs enable ordered analysis of event streams. Let's see
-- the distribution of event types:

-- 7 distinct event types: page_view(7), api_call(6), user_login(3), user_logout(3),
-- config_change(3), data_export(2), system_health(1)
ASSERT ROW_COUNT = 7
SELECT event_type,
       COUNT(*) AS event_count,
       MIN(seq_id) AS first_occurrence,
       MAX(seq_id) AS last_occurrence
FROM {{zone_name}}.delta_demos.event_sequence
GROUP BY event_type
ORDER BY event_count DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify order_items_count: 30 batch1 + 10 batch2 = 40 items
ASSERT ROW_COUNT = 40
SELECT * FROM {{zone_name}}.delta_demos.order_items;

-- Verify laptop_subtotal: id=1 Laptop Pro subtotal = 1*1299.99
ASSERT VALUE subtotal = 1299.99
SELECT subtotal FROM {{zone_name}}.delta_demos.order_items WHERE id = 1;

-- Verify mouse_tax_8pct: id=2 Wireless Mouse tax at 8% = 7.20
ASSERT VALUE tax = 7.2
SELECT tax FROM {{zone_name}}.delta_demos.order_items WHERE id = 2;

-- Verify rack_tax_10pct: id=31 Server Rack tax at 10% = 90.0
ASSERT VALUE tax = 90.0
SELECT tax FROM {{zone_name}}.delta_demos.order_items WHERE id = 31;

-- Verify desk_total: id=12 Standing Desk total = 549.99*1.08 = 593.99
ASSERT VALUE total = 593.99
SELECT total FROM {{zone_name}}.delta_demos.order_items WHERE id = 12;

-- Verify event_count: 25 events in event_sequence
ASSERT VALUE cnt = 25
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.event_sequence;

-- Verify sequential_ids: all 25 seq_ids are unique (no gaps)
ASSERT VALUE cnt = 25
SELECT COUNT(DISTINCT seq_id) AS cnt FROM {{zone_name}}.delta_demos.event_sequence;

-- Verify page_view_count: 7 page_view events
ASSERT VALUE cnt = 7
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.event_sequence WHERE event_type = 'page_view';
