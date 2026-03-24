-- ============================================================================
-- Delta MERGE Comprehensive — All Clause Patterns — Setup Script
-- ============================================================================
-- Creates the target (customer_master) and source (customer_updates) tables
-- with baseline data, ready for the MERGE operation in queries.sql.
--
-- Tables:
--   1. customer_master  — 40 existing customers (target)
--   2. customer_updates — 25 staged changes (source):
--        12 matching IDs with status='active' (updates)
--         3 matching IDs with status='closed' (deletes)
--        10 new IDs 41-50 (inserts)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE 1: customer_master — 40 existing customers (target)
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.customer_master (
    id            INT,
    name          VARCHAR,
    email         VARCHAR,
    tier          VARCHAR,
    balance       DOUBLE,
    status        VARCHAR,
    last_contact  VARCHAR
) LOCATION '{{data_path}}/customer_master';

GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.customer_master TO USER {{current_user}};

INSERT INTO {{zone_name}}.delta_demos.customer_master VALUES
    (1,  'Alice Johnson',    'alice.johnson@crm.com',    'gold',     4500.00,  'active', '2025-01-15'),
    (2,  'Bob Smith',        'bob.smith@crm.com',        'silver',   1200.00,  'active', '2025-02-10'),
    (3,  'Carol Williams',   'carol.williams@crm.com',   'gold',     5200.00,  'active', '2025-01-20'),
    (4,  'David Brown',      'david.brown@crm.com',      'bronze',    350.00,  'active', '2025-03-05'),
    (5,  'Eve Davis',        'eve.davis@crm.com',        'silver',   1800.00,  'active', '2025-02-28'),
    (6,  'Frank Miller',     'frank.miller@crm.com',     'gold',     6100.00,  'active', '2025-01-10'),
    (7,  'Grace Wilson',     'grace.wilson@crm.com',     'bronze',    275.00,  'active', '2025-03-12'),
    (8,  'Henry Moore',      'henry.moore@crm.com',      'silver',   1200.00,  'active', '2025-02-18'),
    (9,  'Irene Taylor',     'irene.taylor@crm.com',     'gold',     3900.00,  'active', '2025-01-25'),
    (10, 'Jack Anderson',    'jack.anderson@crm.com',    'bronze',    420.00,  'active', '2025-03-01'),
    (11, 'Karen Thomas',     'karen.thomas@crm.com',     'silver',   1500.00,  'active', '2025-02-14'),
    (12, 'Leo Jackson',      'leo.jackson@crm.com',      'gold',     4800.00,  'active', '2025-01-30'),
    (13, 'Maria White',      'maria.white@crm.com',      'silver',   1100.00,  'active', '2025-02-22'),
    (14, 'Nathan Harris',    'nathan.harris@crm.com',    'bronze',    380.00,  'active', '2025-03-08'),
    (15, 'Olivia Martin',    'olivia.martin@crm.com',    'gold',     5500.00,  'active', '2025-01-18'),
    (16, 'Paul Garcia',      'paul.garcia@crm.com',      'silver',   1350.00,  'active', '2025-02-25'),
    (17, 'Quinn Martinez',   'quinn.martinez@crm.com',   'bronze',    290.00,  'active', '2025-03-15'),
    (18, 'Rachel Robinson',  'rachel.robinson@crm.com',  'silver',   1600.00,  'active', '2025-02-12'),
    (19, 'Sam Clark',        'sam.clark@crm.com',        'gold',     3200.00,  'active', '2025-01-22'),
    (20, 'Tina Lewis',       'tina.lewis@crm.com',       'bronze',    310.00,  'active', '2025-03-10'),
    (21, 'Uma Lee',          'uma.lee@crm.com',          'silver',   1450.00,  'active', '2025-02-16'),
    (22, 'Victor Walker',    'victor.walker@crm.com',    'gold',     4100.00,  'active', '2025-01-28'),
    (23, 'Wendy Hall',       'wendy.hall@crm.com',       'bronze',    340.00,  'active', '2025-03-03'),
    (24, 'Xander Allen',     'xander.allen@crm.com',     'silver',   1250.00,  'active', '2025-02-20'),
    (25, 'Yolanda Young',    'yolanda.young@crm.com',    'gold',     4700.00,  'active', '2025-01-12'),
    (26, 'Zane King',        'zane.king@crm.com',        'bronze',    260.00,  'active', '2025-03-14'),
    (27, 'Amber Wright',     'amber.wright@crm.com',     'silver',   1550.00,  'active', '2025-02-08'),
    (28, 'Brian Scott',      'brian.scott@crm.com',      'gold',     3800.00,  'active', '2025-01-26'),
    (29, 'Cindy Green',      'cindy.green@crm.com',      'bronze',    400.00,  'active', '2025-03-06'),
    (30, 'Derek Adams',      'derek.adams@crm.com',      'silver',   1700.00,  'active', '2025-02-15'),
    (31, 'Elena Baker',      'elena.baker@crm.com',      'gold',     5000.00,  'active', '2025-01-20'),
    (32, 'Felix Nelson',     'felix.nelson@crm.com',     'bronze',    330.00,  'active', '2025-03-11'),
    (33, 'Gina Hill',        'gina.hill@crm.com',        'silver',   1400.00,  'active', '2025-02-24'),
    (34, 'Hugo Ramirez',     'hugo.ramirez@crm.com',     'gold',     4300.00,  'active', '2025-01-16'),
    (35, 'Isla Campbell',    'isla.campbell@crm.com',     'bronze',    280.00,  'active', '2025-03-09'),
    (36, 'Jake Mitchell',    'jake.mitchell@crm.com',    'silver',   1650.00,  'active', '2025-02-11'),
    (37, 'Kara Roberts',     'kara.roberts@crm.com',     'gold',     3600.00,  'active', '2025-01-24'),
    (38, 'Liam Carter',      'liam.carter@crm.com',      'bronze',    370.00,  'active', '2025-03-02'),
    (39, 'Megan Phillips',   'megan.phillips@crm.com',   'silver',   1300.00,  'active', '2025-02-19'),
    (40, 'Noah Evans',       'noah.evans@crm.com',       'gold',     4000.00,  'active', '2025-01-14');


-- ============================================================================
-- TABLE 2: customer_updates — 25 staged changes (source)
-- ============================================================================
-- IDs 2,5,8,11,14,17,20,23,26,29,32,35: updates (status='active') — 12 rows
-- IDs 7,19,37: closed accounts (status='closed') — 3 rows
-- IDs 41-50: new customers — 10 rows
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.customer_updates (
    id            INT,
    name          VARCHAR,
    email         VARCHAR,
    tier          VARCHAR,
    balance       DOUBLE,
    status        VARCHAR,
    last_contact  VARCHAR
) LOCATION '{{data_path}}/customer_updates';

GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.customer_updates TO USER {{current_user}};

INSERT INTO {{zone_name}}.delta_demos.customer_updates VALUES
    -- Updates for existing customers (status='active') — refreshed data
    (2,  'Bob Smith',        'bob.smith@newmail.com',     'gold',     2850.00,  'active', '2025-12-01'),
    (5,  'Eve Davis',        'eve.davis@crm.com',        'gold',     3200.00,  'active', '2025-12-01'),
    (8,  'Henry Moore',      'henry.moore@crm.com',      'silver',   2850.00,  'active', '2025-12-01'),
    (11, 'Karen Thomas',     'karen.thomas@crm.com',     'gold',     3100.00,  'active', '2025-12-01'),
    (14, 'Nathan Harris',    'nathan.harris@crm.com',    'silver',    950.00,  'active', '2025-12-01'),
    (17, 'Quinn Martinez',   'quinn.martinez@crm.com',   'bronze',    520.00,  'active', '2025-12-01'),
    (20, 'Tina Lewis',       'tina.lewis@crm.com',       'silver',   1100.00,  'active', '2025-12-01'),
    (23, 'Wendy Hall',       'wendy.hall@crm.com',       'silver',   1050.00,  'active', '2025-12-01'),
    (26, 'Zane King',        'zane.king@crm.com',        'silver',    980.00,  'active', '2025-12-01'),
    (29, 'Cindy Green',      'cindy.green@crm.com',      'bronze',    650.00,  'active', '2025-12-01'),
    (32, 'Felix Nelson',     'felix.nelson@crm.com',     'silver',   1200.00,  'active', '2025-12-01'),
    (35, 'Isla Campbell',    'isla.campbell@crm.com',     'bronze',    540.00,  'active', '2025-12-01'),
    -- Closed accounts (status='closed') — to be deleted
    (7,  'Grace Wilson',     'grace.wilson@crm.com',     'bronze',    275.00,  'closed', '2025-12-01'),
    (19, 'Sam Clark',        'sam.clark@crm.com',        'gold',     3200.00,  'closed', '2025-12-01'),
    (37, 'Kara Roberts',     'kara.roberts@crm.com',     'gold',     3600.00,  'closed', '2025-12-01'),
    -- New customers (IDs 41-50)
    (41, 'Oscar Perry',      'oscar.perry@crm.com',      'bronze',    450.00,  'active', '2025-12-01'),
    (42, 'Paula Reed',       'paula.reed@crm.com',       'silver',   1350.00,  'active', '2025-12-01'),
    (43, 'Ruben Cox',        'ruben.cox@crm.com',        'gold',     4200.00,  'active', '2025-12-01'),
    (44, 'Sophie Ward',      'sophie.ward@crm.com',      'bronze',    320.00,  'active', '2025-12-01'),
    (45, 'Tyler Brooks',     'tyler.brooks@crm.com',     'silver',   1500.00,  'active', '2025-12-01'),
    (46, 'Ursula Gray',      'ursula.gray@crm.com',      'gold',     3800.00,  'active', '2025-12-01'),
    (47, 'Vince James',      'vince.james@crm.com',      'bronze',    280.00,  'active', '2025-12-01'),
    (48, 'Willa Bennett',    'willa.bennett@crm.com',    'silver',   1600.00,  'active', '2025-12-01'),
    (49, 'Xavier Hughes',    'xavier.hughes@crm.com',    'gold',     4500.00,  'active', '2025-12-01'),
    (50, 'Yasmin Price',     'yasmin.price@crm.com',     'silver',   1150.00,  'active', '2025-12-01');

