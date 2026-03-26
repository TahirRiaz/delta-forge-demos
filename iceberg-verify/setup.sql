-- ============================================================================
-- Iceberg UniForm Verification — Setup
-- ============================================================================
-- Creates test tables with Iceberg UniForm enabled. These tables will be
-- exercised by queries.sql, then independently verified by reading the
-- generated Iceberg metadata with an external reader (DuckDB + fastavro).
-- ============================================================================

-- Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_verify
    COMMENT 'Iceberg UniForm verification tables';

-- ============================================================================
-- Table A: Unpartitioned, basic types, Iceberg V2
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_verify.products (
    id        INT,
    name      VARCHAR,
    category  VARCHAR,
    price     DOUBLE,
    stock     INT,
    is_active BOOLEAN
) LOCATION '{{data_path}}/products'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_verify.products TO USER {{current_user}};

INSERT INTO {{zone_name}}.iceberg_verify.products VALUES
    (1,  'Laptop',     'Electronics', 999.99,  50,  true),
    (2,  'Mouse',      'Electronics', 29.99,   200, true),
    (3,  'Hub',        'Electronics', 49.99,   150, true),
    (4,  'Desk',       'Furniture',   549.99,  30,  true),
    (5,  'Chair',      'Furniture',   449.99,  40,  true),
    (6,  'Lamp',       'Furniture',   39.99,   180, false),
    (7,  'Headphones', 'Audio',       249.99,  75,  true),
    (8,  'Speaker',    'Audio',       79.99,   110, true),
    (9,  'Mic',        'Audio',       129.99,  65,  true),
    (10, 'Earbuds',    'Audio',       59.99,   200, false);

-- ============================================================================
-- Table B: Partitioned by region, Iceberg V2
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_verify.sales (
    id      INT,
    product VARCHAR,
    region  VARCHAR,
    quarter VARCHAR,
    amount  DOUBLE,
    qty     INT
) LOCATION '{{data_path}}/sales'
PARTITIONED BY (region)
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_verify.sales TO USER {{current_user}};

INSERT INTO {{zone_name}}.iceberg_verify.sales VALUES
    (1,  'Widget', 'us-east', 'Q1', 100.00, 10),
    (2,  'Gadget', 'us-east', 'Q1', 200.00, 5),
    (3,  'Widget', 'us-east', 'Q2', 150.00, 8),
    (4,  'Gadget', 'us-east', 'Q2', 300.00, 3),
    (5,  'Widget', 'us-west', 'Q1', 120.00, 12),
    (6,  'Gadget', 'us-west', 'Q1', 180.00, 6),
    (7,  'Widget', 'us-west', 'Q2', 90.00,  15),
    (8,  'Gadget', 'us-west', 'Q2', 250.00, 4),
    (9,  'Widget', 'eu-west', 'Q1', 110.00, 11),
    (10, 'Gadget', 'eu-west', 'Q1', 220.00, 7),
    (11, 'Widget', 'eu-west', 'Q2', 130.00, 9),
    (12, 'Gadget', 'eu-west', 'Q2', 270.00, 5);

-- ============================================================================
-- Table C: Schema evolution target, Iceberg V2
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_verify.evolve (
    id    INT,
    name  VARCHAR,
    value DOUBLE
) LOCATION '{{data_path}}/evolve'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_verify.evolve TO USER {{current_user}};

INSERT INTO {{zone_name}}.iceberg_verify.evolve VALUES
    (1, 'Alpha',   10.0),
    (2, 'Beta',    20.0),
    (3, 'Gamma',   30.0),
    (4, 'Delta',   40.0),
    (5, 'Epsilon', 50.0);

-- ============================================================================
-- Table D: Iceberg format V3
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_verify.v3_table (
    id    INT,
    name  VARCHAR,
    value DOUBLE,
    tag   VARCHAR
) LOCATION '{{data_path}}/v3_table'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.universalFormat.icebergVersion' = '3',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_verify.v3_table TO USER {{current_user}};

INSERT INTO {{zone_name}}.iceberg_verify.v3_table VALUES
    (1, 'One',   1.1, 'a'),
    (2, 'Two',   2.2, 'b'),
    (3, 'Three', 3.3, 'a'),
    (4, 'Four',  4.4, 'b'),
    (5, 'Five',  5.5, 'a'),
    (6, 'Six',   6.6, 'b');
