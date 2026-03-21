-- ============================================================================
-- Delta Time Travel — Version History Deep Dive — Setup Script
-- ============================================================================
-- An IoT monitoring system tracks sensor readings across 4 locations.
-- Each operation creates a new Delta version. Time travel lets engineers
-- query any historical snapshot.
--
-- Version History:
--   V0: CREATE + INSERT 40 readings             → 40 rows
--   V1: UPDATE  — calibrate lab-a (+2.5)        → 40 rows (10 changed)
--   V2: DELETE  — remove 5 faulty field sensors → 35 rows
--   V3: INSERT  — 15 new sensor readings        → 50 rows
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- VERSION 0: CREATE + INSERT 40 sensor readings
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.sensor_readings (
    id          INT,
    sensor_id   VARCHAR,
    reading     DOUBLE,
    unit        VARCHAR,
    location    VARCHAR,
    recorded_at VARCHAR
) LOCATION '{{data_path}}/sensor_readings';

INSERT INTO {{zone_name}}.delta_demos.sensor_readings VALUES
    -- lab-a sensors (10 rows) — S001 reading = 22.5
    (1,  'S001', 22.5,  'celsius', 'lab-a', '2025-01-15 08:00:00'),
    (2,  'S002', 23.1,  'celsius', 'lab-a', '2025-01-15 08:05:00'),
    (3,  'S003', 21.8,  'celsius', 'lab-a', '2025-01-15 08:10:00'),
    (4,  'S004', 24.0,  'celsius', 'lab-a', '2025-01-15 08:15:00'),
    (5,  'S005', 22.9,  'celsius', 'lab-a', '2025-01-15 08:20:00'),
    (6,  'S006', 23.5,  'celsius', 'lab-a', '2025-01-15 08:25:00'),
    (7,  'S007', 21.2,  'celsius', 'lab-a', '2025-01-15 08:30:00'),
    (8,  'S008', 24.7,  'celsius', 'lab-a', '2025-01-15 08:35:00'),
    (9,  'S009', 22.0,  'celsius', 'lab-a', '2025-01-15 08:40:00'),
    (10, 'S010', 23.8,  'celsius', 'lab-a', '2025-01-15 08:45:00'),
    -- lab-b sensors (10 rows)
    (11, 'S011', 19.5,  'celsius', 'lab-b', '2025-01-15 09:00:00'),
    (12, 'S012', 20.3,  'celsius', 'lab-b', '2025-01-15 09:05:00'),
    (13, 'S013', 18.7,  'celsius', 'lab-b', '2025-01-15 09:10:00'),
    (14, 'S014', 21.1,  'celsius', 'lab-b', '2025-01-15 09:15:00'),
    (15, 'S015', 19.9,  'celsius', 'lab-b', '2025-01-15 09:20:00'),
    (16, 'S016', 20.8,  'celsius', 'lab-b', '2025-01-15 09:25:00'),
    (17, 'S017', 18.2,  'celsius', 'lab-b', '2025-01-15 09:30:00'),
    (18, 'S018', 21.5,  'celsius', 'lab-b', '2025-01-15 09:35:00'),
    (19, 'S019', 19.0,  'celsius', 'lab-b', '2025-01-15 09:40:00'),
    (20, 'S020', 20.6,  'celsius', 'lab-b', '2025-01-15 09:45:00'),
    -- warehouse sensors (10 rows)
    (21, 'S021', 15.2,  'celsius', 'warehouse', '2025-01-15 10:00:00'),
    (22, 'S022', 16.0,  'celsius', 'warehouse', '2025-01-15 10:05:00'),
    (23, 'S023', 14.8,  'celsius', 'warehouse', '2025-01-15 10:10:00'),
    (24, 'S024', 15.9,  'celsius', 'warehouse', '2025-01-15 10:15:00'),
    (25, 'S025', 16.5,  'celsius', 'warehouse', '2025-01-15 10:20:00'),
    (26, 'S026', 14.3,  'celsius', 'warehouse', '2025-01-15 10:25:00'),
    (27, 'S027', 15.7,  'celsius', 'warehouse', '2025-01-15 10:30:00'),
    (28, 'S028', 16.2,  'celsius', 'warehouse', '2025-01-15 10:35:00'),
    (29, 'S029', 14.1,  'celsius', 'warehouse', '2025-01-15 10:40:00'),
    (30, 'S030', 15.4,  'celsius', 'warehouse', '2025-01-15 10:45:00'),
    -- field sensors (10 rows) — ids 36-40 are faulty and will be deleted in V2
    (31, 'S031', 30.5,  'celsius', 'field', '2025-01-15 11:00:00'),
    (32, 'S032', 31.2,  'celsius', 'field', '2025-01-15 11:05:00'),
    (33, 'S033', 29.8,  'celsius', 'field', '2025-01-15 11:10:00'),
    (34, 'S034', 32.0,  'celsius', 'field', '2025-01-15 11:15:00'),
    (35, 'S035', 30.1,  'celsius', 'field', '2025-01-15 11:20:00'),
    (36, 'S036', 99.9,  'celsius', 'field', '2025-01-15 11:25:00'),
    (37, 'S037', 98.7,  'celsius', 'field', '2025-01-15 11:30:00'),
    (38, 'S038', -40.0, 'celsius', 'field', '2025-01-15 11:35:00'),
    (39, 'S039', 105.3, 'celsius', 'field', '2025-01-15 11:40:00'),
    (40, 'S040', -25.6, 'celsius', 'field', '2025-01-15 11:45:00');

DETECT SCHEMA FOR TABLE {{zone_name}}.delta_demos.sensor_readings;
GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.sensor_readings TO USER {{current_user}};


-- ============================================================================
-- VERSION 1: UPDATE — calibrate lab-a sensors (reading += 2.5)
-- ============================================================================
-- All 10 lab-a sensors recalibrated. E.g. S001: 22.5 → 25.0
UPDATE {{zone_name}}.delta_demos.sensor_readings
SET reading = reading + 2.5
WHERE location = 'lab-a';


-- ============================================================================
-- VERSION 2: DELETE — remove 5 faulty field sensors
-- ============================================================================
-- Sensors S036-S040 (ids 36-40) reported impossible readings and are removed.
DELETE FROM {{zone_name}}.delta_demos.sensor_readings
WHERE id IN (36, 37, 38, 39, 40);


-- ============================================================================
-- VERSION 3: INSERT — 15 new sensor readings from newly deployed sensors
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.sensor_readings VALUES
    (41, 'S041', 22.0,  'celsius', 'lab-a',     '2025-01-16 08:00:00'),
    (42, 'S042', 23.3,  'celsius', 'lab-a',     '2025-01-16 08:05:00'),
    (43, 'S043', 21.5,  'celsius', 'lab-a',     '2025-01-16 08:10:00'),
    (44, 'S044', 19.8,  'celsius', 'lab-b',     '2025-01-16 09:00:00'),
    (45, 'S045', 20.1,  'celsius', 'lab-b',     '2025-01-16 09:05:00'),
    (46, 'S046', 18.9,  'celsius', 'lab-b',     '2025-01-16 09:10:00'),
    (47, 'S047', 15.0,  'celsius', 'warehouse', '2025-01-16 10:00:00'),
    (48, 'S048', 16.3,  'celsius', 'warehouse', '2025-01-16 10:05:00'),
    (49, 'S049', 14.7,  'celsius', 'warehouse', '2025-01-16 10:10:00'),
    (50, 'S050', 15.5,  'celsius', 'warehouse', '2025-01-16 10:15:00'),
    (51, 'S051', 29.3,  'celsius', 'field',     '2025-01-16 11:00:00'),
    (52, 'S052', 30.8,  'celsius', 'field',     '2025-01-16 11:05:00'),
    (53, 'S053', 28.5,  'celsius', 'field',     '2025-01-16 11:10:00'),
    (54, 'S054', 31.4,  'celsius', 'field',     '2025-01-16 11:15:00'),
    (55, 'S055', 29.9,  'celsius', 'field',     '2025-01-16 11:20:00');
