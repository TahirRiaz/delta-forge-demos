-- ============================================================================
-- Delta Statistics — Min/Max & Data Skipping — Setup Script
-- ============================================================================
-- Demonstrates how Delta table statistics enable efficient queries:
--   - Distinct value ranges per batch for min/max statistics
--   - NULL-heavy columns for null count statistics
--   - String columns with varied lengths for truncation behavior
--
-- Tables created:
--   1. sensor_readings — 60 readings in 3 batches with distinct ranges
--
-- Operations performed:
--   1. CREATE DELTA TABLE
--   2. INSERT batch 1 — 20 rows, value range [10-100]
--   3. INSERT batch 2 — 20 rows, value range [200-500]
--   4. INSERT batch 3 — 20 rows, value range [1000-5000]
--   5. UPDATE — set quality_score = NULL for 15 rows
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: sensor_readings — readings with distinct value ranges per batch
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.sensor_readings (
    id              INT,
    device          VARCHAR,
    category        VARCHAR,
    value           DOUBLE,
    quality_score   INT,
    description     VARCHAR,
    recorded_at     VARCHAR
) LOCATION '{{data_path}}/sensor_readings';

GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.sensor_readings TO USER {{current_user}};

-- STEP 2: Batch 1 — values [10-100], short descriptions
INSERT INTO {{zone_name}}.delta_demos.sensor_readings VALUES
    (1,  'DEVICE-A', 'temperature', 22.5,  95, 'Normal room temp',          '2025-01-01 08:00:00'),
    (2,  'DEVICE-A', 'temperature', 23.1,  92, 'Slight increase',           '2025-01-01 09:00:00'),
    (3,  'DEVICE-B', 'temperature', 18.7,  88, 'Below average',             '2025-01-01 08:00:00'),
    (4,  'DEVICE-B', 'temperature', 21.0,  90, 'Recovering to normal',      '2025-01-01 09:00:00'),
    (5,  'DEVICE-C', 'humidity',    45.2,  85, 'Acceptable humidity',        '2025-01-01 08:00:00'),
    (6,  'DEVICE-C', 'humidity',    52.8,  87, 'Rising moisture level',      '2025-01-01 09:00:00'),
    (7,  'DEVICE-D', 'humidity',    38.9,  91, 'Dry conditions',             '2025-01-01 08:00:00'),
    (8,  'DEVICE-D', 'humidity',    41.5,  89, 'Stabilizing',                '2025-01-01 09:00:00'),
    (9,  'DEVICE-E', 'pressure',    10.1,  93, 'Low baseline',               '2025-01-01 08:00:00'),
    (10, 'DEVICE-E', 'pressure',    12.3,  94, 'Slight uptick',              '2025-01-01 09:00:00'),
    (11, 'DEVICE-A', 'temperature', 24.6,  86, 'Warm afternoon',             '2025-01-01 10:00:00'),
    (12, 'DEVICE-A', 'temperature', 26.0,  84, 'Peak daytime',               '2025-01-01 11:00:00'),
    (13, 'DEVICE-B', 'temperature', 19.5,  90, 'Cool morning lingering',     '2025-01-01 10:00:00'),
    (14, 'DEVICE-B', 'temperature', 22.3,  88, 'Midday warmup',              '2025-01-01 11:00:00'),
    (15, 'DEVICE-C', 'humidity',    55.0,  82, 'High moisture alert',         '2025-01-01 10:00:00'),
    (16, 'DEVICE-C', 'humidity',    60.2,  80, 'Condensation risk',           '2025-01-01 11:00:00'),
    (17, 'DEVICE-D', 'humidity',    43.7,  88, 'Normal range',                '2025-01-01 10:00:00'),
    (18, 'DEVICE-D', 'humidity',    47.1,  85, 'Comfortable levels',          '2025-01-01 11:00:00'),
    (19, 'DEVICE-E', 'pressure',    15.8,  91, 'Gradual increase',            '2025-01-01 10:00:00'),
    (20, 'DEVICE-E', 'pressure',    100.0, 79, 'Approaching high range',      '2025-01-01 11:00:00');


-- ============================================================================
-- STEP 3: Batch 2 — values [200-500], medium descriptions
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.sensor_readings
SELECT * FROM (VALUES
    (21, 'DEVICE-F', 'vibration', 245.5,  76, 'Moderate vibration detected on bearing assembly',   '2025-01-02 08:00:00'),
    (22, 'DEVICE-F', 'vibration', 312.0,  72, 'Increasing vibration near motor coupling',          '2025-01-02 09:00:00'),
    (23, 'DEVICE-G', 'vibration', 278.3,  78, 'Baseline vibration for pump station',               '2025-01-02 08:00:00'),
    (24, 'DEVICE-G', 'vibration', 350.1,  70, 'Elevated vibration requires inspection',            '2025-01-02 09:00:00'),
    (25, 'DEVICE-H', 'acoustic',  200.0,  81, 'Background noise floor measurement',                '2025-01-02 08:00:00'),
    (26, 'DEVICE-H', 'acoustic',  285.7,  75, 'Noise spike during machine startup',                '2025-01-02 09:00:00'),
    (27, 'DEVICE-I', 'acoustic',  310.5,  73, 'Continuous operational noise level',                 '2025-01-02 08:00:00'),
    (28, 'DEVICE-I', 'acoustic',  425.0,  68, 'Warning threshold exceeded for noise',              '2025-01-02 09:00:00'),
    (29, 'DEVICE-J', 'thermal',   230.0,  80, 'Thermal imaging hotspot detected',                  '2025-01-02 08:00:00'),
    (30, 'DEVICE-J', 'thermal',   298.5,  77, 'Rising thermal signature on panel',                 '2025-01-02 09:00:00'),
    (31, 'DEVICE-F', 'vibration', 388.2,  65, 'Critical vibration alert on compressor unit',       '2025-01-02 10:00:00'),
    (32, 'DEVICE-F', 'vibration', 290.0,  74, 'Post-maintenance vibration check baseline',         '2025-01-02 11:00:00'),
    (33, 'DEVICE-G', 'vibration', 410.5,  62, 'Severe vibration detected on fan blade',            '2025-01-02 10:00:00'),
    (34, 'DEVICE-G', 'vibration', 265.0,  79, 'Vibration reduced after balance adjustment',        '2025-01-02 11:00:00'),
    (35, 'DEVICE-H', 'acoustic',  340.8,  71, 'Peak noise during shift change operation',          '2025-01-02 10:00:00'),
    (36, 'DEVICE-H', 'acoustic',  225.3,  82, 'Quiet period overnight baseline noise',             '2025-01-02 11:00:00'),
    (37, 'DEVICE-I', 'acoustic',  500.0,  60, 'Maximum noise alert safety threshold',              '2025-01-02 10:00:00'),
    (38, 'DEVICE-I', 'acoustic',  355.5,  69, 'Declining noise trend after shutdown',              '2025-01-02 11:00:00'),
    (39, 'DEVICE-J', 'thermal',   450.2,  63, 'Critical overheating warning on motor',             '2025-01-02 10:00:00'),
    (40, 'DEVICE-J', 'thermal',   380.0,  66, 'Thermal reading after cooling activation',          '2025-01-02 11:00:00')
) AS t(id, device, category, value, quality_score, description, recorded_at);


-- ============================================================================
-- STEP 4: Batch 3 — values [1000-5000], long descriptions (>32 chars)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.sensor_readings
SELECT * FROM (VALUES
    (41, 'DEVICE-K', 'power',  1250.0,  55, 'High power consumption reading on main transformer during peak load period',       '2025-01-03 08:00:00'),
    (42, 'DEVICE-K', 'power',  1800.5,  50, 'Power surge detected across distribution board requiring immediate investigation', '2025-01-03 09:00:00'),
    (43, 'DEVICE-L', 'power',  2200.0,  48, 'Sustained high power draw from industrial compressor bank section A',             '2025-01-03 08:00:00'),
    (44, 'DEVICE-L', 'power',  1500.3,  52, 'Moderate power consumption during scheduled maintenance downtime window',         '2025-01-03 09:00:00'),
    (45, 'DEVICE-M', 'torque', 3100.0,  45, 'Maximum torque output measured on primary drive shaft during stress test',         '2025-01-03 08:00:00'),
    (46, 'DEVICE-M', 'torque', 2750.5,  47, 'High torque reading on secondary motor assembly coupling bearing',                '2025-01-03 09:00:00'),
    (47, 'DEVICE-N', 'torque', 1800.0,  53, 'Normal operating torque for conveyor belt drive system under load',               '2025-01-03 08:00:00'),
    (48, 'DEVICE-N', 'torque', 2100.8,  51, 'Elevated torque from increased material throughput on line three',                '2025-01-03 09:00:00'),
    (49, 'DEVICE-O', 'rpm',    4500.0,  42, 'Peak RPM during centrifuge operation cycle at maximum rated speed',               '2025-01-03 08:00:00'),
    (50, 'DEVICE-O', 'rpm',    3800.5,  44, 'Standard operating RPM for mixing drum during batch processing',                 '2025-01-03 09:00:00'),
    (51, 'DEVICE-K', 'power',  2800.0,  40, 'Critical power overload condition triggered automatic breaker disconnect',        '2025-01-03 10:00:00'),
    (52, 'DEVICE-K', 'power',  1000.0,  58, 'Minimum baseline power consumption during complete facility shutdown',            '2025-01-03 11:00:00'),
    (53, 'DEVICE-L', 'power',  1950.2,  49, 'Ramping power draw as production line returns to full operational capacity',      '2025-01-03 10:00:00'),
    (54, 'DEVICE-L', 'power',  3500.0,  38, 'Peak demand power reading during simultaneous startup of all systems',           '2025-01-03 11:00:00'),
    (55, 'DEVICE-M', 'torque', 4200.5,  35, 'Extreme torque spike detected on gearbox output shaft requiring investigation',  '2025-01-03 10:00:00'),
    (56, 'DEVICE-M', 'torque', 1200.0,  56, 'Low torque reading during idle operation with no load on the shaft',             '2025-01-03 11:00:00'),
    (57, 'DEVICE-N', 'torque', 2500.3,  46, 'Moderate torque output on auxiliary drive system during normal run',             '2025-01-03 10:00:00'),
    (58, 'DEVICE-N', 'torque', 5000.0,  33, 'Maximum rated torque capacity reached on heavy duty press machine',              '2025-01-03 11:00:00'),
    (59, 'DEVICE-O', 'rpm',    2200.0,  54, 'Low speed RPM during warm up cycle for turbine generator unit',                  '2025-01-03 10:00:00'),
    (60, 'DEVICE-O', 'rpm',    3000.0,  50, 'Cruising RPM after reaching steady state operating conditions',                  '2025-01-03 11:00:00')
) AS t(id, device, category, value, quality_score, description, recorded_at);


-- ============================================================================
-- STEP 5: UPDATE — set quality_score = NULL for 15 rows (ids 1-5, 21-25, 41-45)
-- ============================================================================
-- This creates mixed NULL patterns across all 3 batches
UPDATE {{zone_name}}.delta_demos.sensor_readings
SET quality_score = NULL
WHERE id <= 5 OR (id >= 21 AND id <= 25) OR (id >= 41 AND id <= 45);
