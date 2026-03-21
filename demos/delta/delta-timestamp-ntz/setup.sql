-- ============================================================================
-- Delta Timestamps — Timezone-Free & Precision Handling — Setup Script
-- ============================================================================
-- Demonstrates timezone-free timestamp handling using VARCHAR columns:
--   - Local departure/arrival times (as the traveler sees them on the board)
--   - UTC times for system coordination across time zones
--   - Timezone context is implicit in the origin/destination
--
-- Table created:
--   1. flight_schedule — 45 airline flights with local + UTC timestamps
--
-- Operations performed:
--   1. CREATE ZONE + SCHEMA
--   2. CREATE DELTA TABLE with VARCHAR timestamp columns
--   3. INSERT 25 rows — domestic US flights (JFK, LAX, ORD, ATL, DFW)
--   4. DETECT SCHEMA + GRANT ADMIN
--   5. INSERT 10 rows — international flights (LHR, NRT, CDG, SYD)
--   6. INSERT 10 rows — red-eye/overnight flights (arrival date is next day)
--   7. UPDATE — delay 5 flights (add 90 min, set status='delayed')
--   8. UPDATE — cancel 3 flights (set status='cancelled')
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: flight_schedule — airline scheduling with timezone-free timestamps
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.flight_schedule (
    id                  INT,
    flight_code         VARCHAR,
    origin              VARCHAR,
    destination         VARCHAR,
    departure_local     VARCHAR,
    arrival_local       VARCHAR,
    departure_utc       VARCHAR,
    duration_minutes    INT,
    status              VARCHAR,
    gate                VARCHAR
) LOCATION '{{data_path}}/flight_schedule';


-- ============================================================================
-- STEP 3: INSERT batch 1 — 25 domestic US flights
-- ============================================================================
-- JFK (UTC-4 EDT): ids 1-5
INSERT INTO {{zone_name}}.delta_demos.flight_schedule VALUES
    (1,  'AA-100',  'JFK', 'LAX', '2025-06-15 08:30:00', '2025-06-15 11:45:00', '2025-06-15 12:30:00', 315, 'on_time', 'A1'),
    (2,  'DL-200',  'JFK', 'ORD', '2025-06-15 09:15:00', '2025-06-15 11:30:00', '2025-06-15 13:15:00', 165, 'on_time', 'A3'),
    (3,  'UA-300',  'JFK', 'ATL', '2025-06-15 10:00:00', '2025-06-15 12:30:00', '2025-06-15 14:00:00', 150, 'on_time', 'B2'),
    (4,  'AA-110',  'JFK', 'DFW', '2025-06-15 11:45:00', '2025-06-15 14:15:00', '2025-06-15 15:45:00', 210, 'on_time', 'A5'),
    (5,  'DL-210',  'JFK', 'LAX', '2025-06-15 14:00:00', '2025-06-15 17:20:00', '2025-06-15 18:00:00', 320, 'on_time', 'C1');

-- LAX (UTC-7 PDT): ids 6-10
INSERT INTO {{zone_name}}.delta_demos.flight_schedule VALUES
    (6,  'UA-400',  'LAX', 'JFK', '2025-06-15 06:00:00', '2025-06-15 14:15:00', '2025-06-15 13:00:00', 300, 'on_time', 'T4-1'),
    (7,  'AA-410',  'LAX', 'ORD', '2025-06-15 07:30:00', '2025-06-15 13:15:00', '2025-06-15 14:30:00', 240, 'on_time', 'T4-3'),
    (8,  'DL-420',  'LAX', 'ATL', '2025-06-15 09:00:00', '2025-06-15 16:30:00', '2025-06-15 16:00:00', 270, 'on_time', 'T5-2'),
    (9,  'UA-430',  'LAX', 'DFW', '2025-06-15 10:45:00', '2025-06-15 15:45:00', '2025-06-15 17:45:00', 195, 'on_time', 'T4-5'),
    (10, 'AA-440',  'LAX', 'JFK', '2025-06-15 12:00:00', '2025-06-15 20:30:00', '2025-06-15 19:00:00', 310, 'on_time', 'T5-1');

-- ORD (UTC-5 CDT): ids 11-15
INSERT INTO {{zone_name}}.delta_demos.flight_schedule VALUES
    (11, 'UA-500',  'ORD', 'JFK', '2025-06-15 07:00:00', '2025-06-15 10:15:00', '2025-06-15 12:00:00', 135, 'on_time', 'C15'),
    (12, 'AA-510',  'ORD', 'LAX', '2025-06-15 08:30:00', '2025-06-15 10:45:00', '2025-06-15 13:30:00', 255, 'on_time', 'C18'),
    (13, 'DL-520',  'ORD', 'ATL', '2025-06-15 11:00:00', '2025-06-15 14:00:00', '2025-06-15 16:00:00', 120, 'on_time', 'B7'),
    (14, 'UA-530',  'ORD', 'DFW', '2025-06-15 13:15:00', '2025-06-15 15:45:00', '2025-06-15 18:15:00', 165, 'on_time', 'C20'),
    (15, 'AA-540',  'ORD', 'JFK', '2025-06-15 16:00:00', '2025-06-15 19:30:00', '2025-06-15 21:00:00', 150, 'on_time', 'C12');

-- ATL (UTC-4 EDT): ids 16-20
INSERT INTO {{zone_name}}.delta_demos.flight_schedule VALUES
    (16, 'DL-600',  'ATL', 'JFK', '2025-06-15 06:30:00', '2025-06-15 08:45:00', '2025-06-15 10:30:00', 135, 'on_time', 'D10'),
    (17, 'DL-610',  'ATL', 'LAX', '2025-06-15 08:00:00', '2025-06-15 10:15:00', '2025-06-15 12:00:00', 285, 'on_time', 'D14'),
    (18, 'AA-620',  'ATL', 'ORD', '2025-06-15 10:30:00', '2025-06-15 11:45:00', '2025-06-15 14:30:00', 120, 'on_time', 'T-S3'),
    (19, 'DL-630',  'ATL', 'DFW', '2025-06-15 12:00:00', '2025-06-15 13:15:00', '2025-06-15 16:00:00', 150, 'on_time', 'D18'),
    (20, 'UA-640',  'ATL', 'JFK', '2025-06-15 15:00:00', '2025-06-15 17:15:00', '2025-06-15 19:00:00', 135, 'on_time', 'D22');

-- DFW (UTC-5 CDT): ids 21-25
INSERT INTO {{zone_name}}.delta_demos.flight_schedule VALUES
    (21, 'AA-700',  'DFW', 'JFK', '2025-06-15 07:00:00', '2025-06-15 11:30:00', '2025-06-15 12:00:00', 210, 'on_time', 'A22'),
    (22, 'AA-710',  'DFW', 'LAX', '2025-06-15 09:30:00', '2025-06-15 10:45:00', '2025-06-15 14:30:00', 195, 'on_time', 'A25'),
    (23, 'DL-720',  'DFW', 'ORD', '2025-06-15 11:00:00', '2025-06-15 13:30:00', '2025-06-15 16:00:00', 150, 'on_time', 'B30'),
    (24, 'UA-730',  'DFW', 'ATL', '2025-06-15 13:45:00', '2025-06-15 17:00:00', '2025-06-15 18:45:00', 135, 'on_time', 'A28'),
    (25, 'AA-740',  'DFW', 'JFK', '2025-06-15 16:30:00', '2025-06-15 21:00:00', '2025-06-15 21:30:00', 215, 'on_time', 'A30');

DETECT SCHEMA FOR TABLE {{zone_name}}.delta_demos.flight_schedule;
GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.flight_schedule TO USER {{current_user}};


-- ============================================================================
-- STEP 5: INSERT batch 2 — 10 international flights
-- ============================================================================
-- Flights to/from LHR, NRT, CDG, SYD with different UTC offsets

-- LHR (UTC+1 BST): ids 26-28
INSERT INTO {{zone_name}}.delta_demos.flight_schedule VALUES
    (26, 'BA-100',  'JFK', 'LHR', '2025-06-15 19:00:00', '2025-06-16 07:00:00', '2025-06-15 23:00:00', 420, 'on_time', 'A7'),
    (27, 'BA-200',  'LHR', 'JFK', '2025-06-15 10:30:00', '2025-06-15 13:30:00', '2025-06-15 09:30:00', 480, 'on_time', 'T5-A'),
    (28, 'VS-300',  'LHR', 'LAX', '2025-06-15 12:00:00', '2025-06-15 15:30:00', '2025-06-15 11:00:00', 660, 'on_time', 'T3-J');

-- NRT (UTC+9): ids 29-31
INSERT INTO {{zone_name}}.delta_demos.flight_schedule VALUES
    (29, 'JL-001',  'JFK', 'NRT', '2025-06-15 13:00:00', '2025-06-16 16:00:00', '2025-06-15 17:00:00', 840, 'on_time', 'B1'),
    (30, 'NH-100',  'NRT', 'LAX', '2025-06-15 17:30:00', '2025-06-15 11:00:00', '2025-06-15 08:30:00', 600, 'on_time', '71A'),
    (31, 'JL-002',  'NRT', 'ORD', '2025-06-15 11:00:00', '2025-06-15 09:00:00', '2025-06-15 02:00:00', 720, 'on_time', '72B');

-- CDG (UTC+2 CEST): ids 32-33
INSERT INTO {{zone_name}}.delta_demos.flight_schedule VALUES
    (32, 'AF-001',  'JFK', 'CDG', '2025-06-15 18:30:00', '2025-06-16 08:00:00', '2025-06-15 22:30:00', 450, 'on_time', 'A9'),
    (33, 'AF-002',  'CDG', 'ATL', '2025-06-15 10:00:00', '2025-06-15 14:00:00', '2025-06-15 08:00:00', 600, 'on_time', '2E-K');

-- SYD (UTC+10 AEST): ids 34-35
INSERT INTO {{zone_name}}.delta_demos.flight_schedule VALUES
    (34, 'QF-011',  'LAX', 'SYD', '2025-06-15 22:00:00', '2025-06-17 06:30:00', '2025-06-16 05:00:00', 1020, 'on_time', 'T-B5'),
    (35, 'QF-012',  'SYD', 'LAX', '2025-06-15 09:00:00', '2025-06-15 06:30:00', '2025-06-14 23:00:00', 840, 'on_time', 'T1-52');


-- ============================================================================
-- STEP 6: INSERT batch 3 — 10 red-eye/overnight flights
-- ============================================================================
-- Arrival date is the next day (common red-eye pattern)

INSERT INTO {{zone_name}}.delta_demos.flight_schedule VALUES
    (36, 'AA-900',  'LAX', 'LHR', '2025-06-15 23:00:00', '2025-06-16 17:00:00', '2025-06-16 06:00:00', 630, 'on_time', 'T-B2'),
    (37, 'DL-910',  'ATL', 'LHR', '2025-06-15 23:30:00', '2025-06-16 13:30:00', '2025-06-16 03:30:00', 510, 'on_time', 'E20'),
    (38, 'UA-920',  'ORD', 'LHR', '2025-06-15 22:45:00', '2025-06-16 12:45:00', '2025-06-16 03:45:00', 495, 'on_time', 'C28'),
    (39, 'AA-930',  'DFW', 'LHR', '2025-06-15 20:00:00', '2025-06-16 11:00:00', '2025-06-16 01:00:00', 570, 'on_time', 'D40'),
    (40, 'DL-940',  'ATL', 'CDG', '2025-06-15 21:30:00', '2025-06-16 12:00:00', '2025-06-16 01:30:00', 555, 'on_time', 'E18'),
    (41, 'UA-950',  'ORD', 'NRT', '2025-06-15 21:00:00', '2025-06-17 01:00:00', '2025-06-16 02:00:00', 780, 'on_time', 'C30'),
    (42, 'AA-960',  'JFK', 'SYD', '2025-06-15 20:30:00', '2025-06-17 08:30:00', '2025-06-16 00:30:00', 1140, 'on_time', 'A12'),
    (43, 'DL-970',  'LAX', 'NRT', '2025-06-15 23:15:00', '2025-06-16 05:15:00', '2025-06-16 06:15:00', 690, 'on_time', 'T-B7'),
    (44, 'UA-980',  'ORD', 'CDG', '2025-06-15 22:00:00', '2025-06-16 13:00:00', '2025-06-16 03:00:00', 540, 'on_time', 'C25'),
    (45, 'AA-990',  'DFW', 'NRT', '2025-06-15 23:45:00', '2025-06-17 05:45:00', '2025-06-16 04:45:00', 810, 'on_time', 'A35');


-- ============================================================================
-- STEP 7: UPDATE — delay 5 flights (add 90 minutes, set status='delayed')
-- ============================================================================
-- Delay ids: 6, 12, 18, 30, 40
UPDATE {{zone_name}}.delta_demos.flight_schedule
SET status = 'delayed',
    departure_local = '2025-06-15 07:30:00'
WHERE id = 6;

UPDATE {{zone_name}}.delta_demos.flight_schedule
SET status = 'delayed',
    departure_local = '2025-06-15 10:00:00'
WHERE id = 12;

UPDATE {{zone_name}}.delta_demos.flight_schedule
SET status = 'delayed',
    departure_local = '2025-06-15 12:00:00'
WHERE id = 18;

UPDATE {{zone_name}}.delta_demos.flight_schedule
SET status = 'delayed',
    departure_local = '2025-06-15 19:00:00'
WHERE id = 30;

UPDATE {{zone_name}}.delta_demos.flight_schedule
SET status = 'delayed',
    departure_local = '2025-06-15 23:00:00'
WHERE id = 40;


-- ============================================================================
-- STEP 8: UPDATE — cancel 3 flights (set status='cancelled')
-- ============================================================================
-- Cancel ids: 8, 22, 36
UPDATE {{zone_name}}.delta_demos.flight_schedule
SET status = 'cancelled'
WHERE id = 8;

UPDATE {{zone_name}}.delta_demos.flight_schedule
SET status = 'cancelled'
WHERE id = 22;

UPDATE {{zone_name}}.delta_demos.flight_schedule
SET status = 'cancelled'
WHERE id = 36;
