-- ============================================================================
-- Delta Type Widening — Numeric Precision & Ranges — Setup Script
-- ============================================================================
-- Demonstrates numeric type handling in Delta tables:
--   - INT for small values, BIGINT for large values
--   - DOUBLE precision for financial calculations
--   - Controlled rounding with ROUND()
--
-- Tables created:
--   1. measurements — 40 sensor readings with varied numeric ranges
--
-- Operations performed:
--   1. CREATE DELTA TABLE with INT, BIGINT, DOUBLE columns
--   2. INSERT — 25 small-range measurements
--   3. INSERT — 15 large-range measurements
--   4. UPDATE — scale up readings by 1000x for category 'pressure'
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: measurements — sensor readings with varied numeric ranges
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.measurements (
    id              INT,
    sensor_id       VARCHAR,
    category        VARCHAR,
    small_reading   INT,
    large_reading   BIGINT,
    precise_value   DOUBLE,
    amount          DOUBLE,
    unit            VARCHAR,
    recorded_date   VARCHAR
) LOCATION '{{data_path}}/measurements';

-- STEP 2: Insert 25 small-range measurements (values within INT range)
INSERT INTO {{zone_name}}.delta_demos.measurements VALUES
    (1,  'TEMP-001', 'temperature', 22,     72,          22.456,    19.99,   'celsius',  '2025-01-01'),
    (2,  'TEMP-002', 'temperature', 25,     77,          25.789,    24.50,   'celsius',  '2025-01-01'),
    (3,  'TEMP-003', 'temperature', -5,     23,          -5.123,    15.00,   'celsius',  '2025-01-02'),
    (4,  'TEMP-004', 'temperature', 38,     100,         38.901,    32.75,   'celsius',  '2025-01-02'),
    (5,  'TEMP-005', 'temperature', 0,      32,          0.001,     10.00,   'celsius',  '2025-01-03'),
    (6,  'PRES-001', 'pressure',    1013,   101325,      1013.25,   45.00,   'hpa',      '2025-01-01'),
    (7,  'PRES-002', 'pressure',    1015,   101500,      1015.00,   47.50,   'hpa',      '2025-01-01'),
    (8,  'PRES-003', 'pressure',    998,    99800,       998.75,    42.00,   'hpa',      '2025-01-02'),
    (9,  'PRES-004', 'pressure',    1020,   102000,      1020.50,   50.00,   'hpa',      '2025-01-02'),
    (10, 'PRES-005', 'pressure',    1008,   100800,      1008.33,   44.25,   'hpa',      '2025-01-03'),
    (11, 'HUM-001',  'humidity',    45,     450,         45.67,     12.00,   'percent',  '2025-01-01'),
    (12, 'HUM-002',  'humidity',    62,     620,         62.34,     15.50,   'percent',  '2025-01-01'),
    (13, 'HUM-003',  'humidity',    78,     780,         78.90,     18.00,   'percent',  '2025-01-02'),
    (14, 'HUM-004',  'humidity',    33,     330,         33.21,     11.25,   'percent',  '2025-01-02'),
    (15, 'HUM-005',  'humidity',    55,     550,         55.55,     14.00,   'percent',  '2025-01-03'),
    (16, 'FLOW-001', 'flow',        120,    12000,       120.456,   75.00,   'lpm',      '2025-01-01'),
    (17, 'FLOW-002', 'flow',        85,     8500,        85.789,    60.00,   'lpm',      '2025-01-01'),
    (18, 'FLOW-003', 'flow',        200,    20000,       200.123,   95.00,   'lpm',      '2025-01-02'),
    (19, 'FLOW-004', 'flow',        150,    15000,       150.999,   82.50,   'lpm',      '2025-01-02'),
    (20, 'FLOW-005', 'flow',        95,     9500,        95.001,    65.00,   'lpm',      '2025-01-03'),
    (21, 'VOLT-001', 'voltage',     220,    22000,       220.50,    28.00,   'volts',    '2025-01-01'),
    (22, 'VOLT-002', 'voltage',     110,    11000,       110.25,    22.00,   'volts',    '2025-01-01'),
    (23, 'VOLT-003', 'voltage',     380,    38000,       380.75,    55.00,   'volts',    '2025-01-02'),
    (24, 'VOLT-004', 'voltage',     240,    24000,       240.00,    30.00,   'volts',    '2025-01-02'),
    (25, 'VOLT-005', 'voltage',     12,     1200,        12.60,     8.00,    'volts',    '2025-01-03');

DETECT SCHEMA FOR TABLE {{zone_name}}.delta_demos.measurements;
GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.measurements TO USER {{current_user}};


-- ============================================================================
-- STEP 3: Insert 15 large-range measurements (values requiring BIGINT)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.measurements
SELECT * FROM (VALUES
    (26, 'NANO-001', 'nanoseconds',  2000000,   2000000000000,    2000000000.123,   199.99,  'ns',    '2025-01-04'),
    (27, 'NANO-002', 'nanoseconds',  1500000,   1500000000000,    1500000000.456,   149.50,  'ns',    '2025-01-04'),
    (28, 'NANO-003', 'nanoseconds',  3200000,   3200000000000,    3200000000.789,   299.00,  'ns',    '2025-01-05'),
    (29, 'BYTE-001', 'bytes',        1048576,   1099511627776,    1099511627.776,   500.00,  'bytes', '2025-01-04'),
    (30, 'BYTE-002', 'bytes',        524288,    549755813888,     549755813.888,    250.00,  'bytes', '2025-01-04'),
    (31, 'BYTE-003', 'bytes',        2097152,   2199023255552,    2199023255.552,   750.00,  'bytes', '2025-01-05'),
    (32, 'FREQ-001', 'frequency',    2400,      2400000000,       2400000.000,      120.00,  'mhz',   '2025-01-04'),
    (33, 'FREQ-002', 'frequency',    3500,      3500000000,       3500000.000,      175.00,  'mhz',   '2025-01-04'),
    (34, 'FREQ-003', 'frequency',    5000,      5000000000,       5000000.000,      250.00,  'mhz',   '2025-01-05'),
    (35, 'DIST-001', 'distance',     149597,    149597870700,     149597870.700,    999.99,  'km',    '2025-01-04'),
    (36, 'DIST-002', 'distance',     384400,    384400000,        384400.000,       450.00,  'km',    '2025-01-04'),
    (37, 'DIST-003', 'distance',     227900,    227900000000,     227900000.000,    850.00,  'km',    '2025-01-05'),
    (38, 'TICK-001', 'ticks',        1000000,   9223372036854,    9223372036.854,   1200.00, 'ticks', '2025-01-04'),
    (39, 'TICK-002', 'ticks',        500000,    4611686018427,    4611686018.427,   600.00,  'ticks', '2025-01-04'),
    (40, 'TICK-003', 'ticks',        750000,    6917529027641,    6917529027.641,   900.00,  'ticks', '2025-01-05')
) AS t(id, sensor_id, category, small_reading, large_reading, precise_value, amount, unit, recorded_date);


-- ============================================================================
-- STEP 4: UPDATE — scale up pressure readings by 1000x (hPa → Pa conversion)
-- ============================================================================
-- Pressure ids 6-10: small_reading * 1000, precise_value * 1000
UPDATE {{zone_name}}.delta_demos.measurements
SET small_reading = small_reading * 1000,
    precise_value = ROUND(precise_value * 1000.0, 2),
    unit = 'pa'
WHERE category = 'pressure';
