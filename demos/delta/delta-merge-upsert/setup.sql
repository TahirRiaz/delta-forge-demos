-- ============================================================================
-- Delta MERGE — Upsert, Conditional Update & Delete — Setup Script
-- ============================================================================
-- Creates the target and source tables for the MERGE upsert demo.
--
-- Tables:
--   1. customers        — 20 existing customers (target)
--   2. customer_updates — 15 staged changes (source): 10 updates + 5 new
--
-- The MERGE in queries.sql will:
--   - Update 10 customers (ids 1-10) with new spending totals and tier logic
--   - Insert 5 new customers (ids 21-25)
--   - Delete 4 stale bronze customers not in source (ids 14, 17, 18, 20)
--   - Final count: 20 - 4 + 5 = 21
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE 1: customers — 20 existing customers (target)
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.customers (
    id          INT,
    name        VARCHAR,
    email       VARCHAR,
    city        VARCHAR,
    tier        VARCHAR,
    total_spent DOUBLE
) LOCATION '{{data_path}}/customers';

INSERT INTO {{zone_name}}.delta_demos.customers VALUES
    (1,  'Alice Johnson',   'alice@example.com',    'New York',     'gold',     2500.00),
    (2,  'Bob Smith',       'bob@example.com',      'Los Angeles',  'silver',   1200.00),
    (3,  'Carol Williams',  'carol@example.com',    'Chicago',      'gold',     3100.00),
    (4,  'David Brown',     'david@example.com',    'Houston',      'bronze',   450.00),
    (5,  'Eve Davis',       'eve@example.com',      'Phoenix',      'silver',   800.00),
    (6,  'Frank Miller',    'frank@example.com',    'Philadelphia', 'gold',     5200.00),
    (7,  'Grace Wilson',    'grace@example.com',    'San Antonio',  'bronze',   300.00),
    (8,  'Henry Moore',     'henry@example.com',    'San Diego',    'silver',   1500.00),
    (9,  'Irene Taylor',    'irene@example.com',    'Dallas',       'gold',     4000.00),
    (10, 'Jack Anderson',   'jack@example.com',     'San Jose',     'bronze',   200.00),
    (11, 'Karen Thomas',    'karen@example.com',    'Austin',       'silver',   900.00),
    (12, 'Leo Jackson',     'leo@example.com',      'Jacksonville', 'gold',     2800.00),
    (13, 'Maria White',     'maria@example.com',    'San Francisco','silver',   1100.00),
    (14, 'Nathan Harris',   'nathan@example.com',   'Columbus',     'bronze',   350.00),
    (15, 'Olivia Martin',   'olivia@example.com',   'Charlotte',    'gold',     3500.00),
    (16, 'Paul Garcia',     'paul@example.com',     'Indianapolis', 'silver',   750.00),
    (17, 'Quinn Martinez',  'quinn@example.com',    'Seattle',      'bronze',   400.00),
    (18, 'Rachel Robinson', 'rachel@example.com',   'Denver',       'bronze',   180.00),
    (19, 'Sam Clark',       'sam@example.com',      'Nashville',    'silver',   1300.00),
    (20, 'Tina Lewis',      'tina@example.com',     'Portland',     'bronze',   220.00);

DETECT SCHEMA FOR TABLE {{zone_name}}.delta_demos.customers;
GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.customers TO USER {{current_user}};


-- ============================================================================
-- TABLE 2: customer_updates — 15 staged changes (source)
-- ============================================================================
-- IDs 1-10: updates with increased spending
-- IDs 21-25: brand new customers
-- IDs 11-20: NOT in source → triggers NOT MATCHED BY SOURCE
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.customer_updates (
    id          INT,
    name        VARCHAR,
    email       VARCHAR,
    city        VARCHAR,
    tier        VARCHAR,
    total_spent DOUBLE
) LOCATION '{{data_path}}/customer_updates';

INSERT INTO {{zone_name}}.delta_demos.customer_updates VALUES
    -- Updates for existing customers (increased spending)
    (1,  'Alice Johnson',   'alice@example.com',    'New York',     'gold',     3200.00),
    (2,  'Bob Smith',       'bob@example.com',      'Los Angeles',  'silver',   1800.00),
    (3,  'Carol Williams',  'carol@example.com',    'Chicago',      'gold',     3500.00),
    (4,  'David Brown',     'david@example.com',    'Houston',      'bronze',   700.00),
    (5,  'Eve Davis',       'eve@example.com',      'Phoenix',      'silver',   1300.00),
    (6,  'Frank Miller',    'frank@example.com',    'Philadelphia', 'gold',     5800.00),
    (7,  'Grace Wilson',    'grace@example.com',    'San Antonio',  'bronze',   550.00),
    (8,  'Henry Moore',     'henry@example.com',    'San Diego',    'silver',   2200.00),
    (9,  'Irene Taylor',    'irene@example.com',    'Dallas',       'gold',     4500.00),
    (10, 'Jack Anderson',   'jack@example.com',     'San Jose',     'bronze',   600.00),
    -- New customers
    (21, 'Uma Lee',         'uma@example.com',      'Miami',        'bronze',   150.00),
    (22, 'Victor Walker',   'victor@example.com',   'Atlanta',      'silver',   950.00),
    (23, 'Wendy Hall',      'wendy@example.com',    'Boston',       'gold',     2600.00),
    (24, 'Xander Allen',    'xander@example.com',   'Detroit',      'bronze',   275.00),
    (25, 'Yolanda Young',   'yolanda@example.com',  'Memphis',      'silver',   1050.00);

DETECT SCHEMA FOR TABLE {{zone_name}}.delta_demos.customer_updates;
GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.customer_updates TO USER {{current_user}};
