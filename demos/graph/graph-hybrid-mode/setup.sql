-- ============================================================================
-- Graph Hybrid Mode — Setup Script
-- ============================================================================
-- Creates Delta graph tables using the HYBRID property storage mode.
-- Frequently queried properties are stored as individual columns for fast
-- access, while optional/extensible properties live in a JSON extras column.
--
--   1. persons_hybrid     — 5 vertex nodes (core columns + JSON extras)
--   2. friendships_hybrid — 6 directed edges (core columns + JSON extras)
--
-- GRAPH VISUALIZATION (identical data to flattened and JSON modes):
--
--     Alice(30,Engineering,NYC) -----> Bob(25,Marketing,LA)
--       |   ^                            |
--       |   |                            | friend
--       |   |                            |
--       |   +--- Eve(32,Finance,NYC)     |
--       |         ^                      |
--       v         | colleague            v
--    Carol(35,HR,Chicago) ----------> Dave(28,Engineering,SF)
--              manager
--
-- HYBRID MODE ADVANTAGES:
--   - Core columns (name, age) have full predicate pushdown
--   - JSON extras column holds optional/variable properties
--   - Best of both worlds: performance for common queries + flexibility
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.graph
    COMMENT 'Graph property storage mode demo tables';


-- ============================================================================
-- TABLE 1: persons_hybrid — 5 vertex nodes (core columns + JSON extras)
-- ============================================================================
-- Core columns: id, name, age (frequently filtered/joined on).
-- Extras JSON: department, city, skills, level, active (optional/variable).
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.graph.persons_hybrid (
    id          BIGINT,
    name        STRING,
    age         INT,
    label       STRING,
    extras      STRING
) LOCATION '{{data_path}}/persons_hybrid';

INSERT INTO {{zone_name}}.graph.persons_hybrid VALUES
    (1, 'Alice', 30, 'Engineering', '{"department": "Engineering", "city": "NYC", "skills": ["rust", "python"], "level": "senior", "active": true}'),
    (2, 'Bob', 25, 'Marketing', '{"department": "Marketing", "city": "LA", "skills": ["sales", "analytics"], "level": "junior", "active": true}'),
    (3, 'Carol', 35, 'HR', '{"department": "HR", "city": "Chicago", "skills": ["recruiting", "training"], "level": "senior", "active": true}'),
    (4, 'Dave', 28, 'Engineering', '{"department": "Engineering", "city": "SF", "skills": ["golang", "kubernetes"], "level": "mid", "active": false}'),
    (5, 'Eve', 32, 'Finance', '{"department": "Finance", "city": "NYC", "skills": ["accounting", "forecasting"], "level": "senior", "active": true}');

DETECT SCHEMA FOR TABLE {{zone_name}}.graph.persons_hybrid;
GRANT ADMIN ON TABLE {{zone_name}}.graph.persons_hybrid TO USER {{current_user}};


-- ============================================================================
-- TABLE 2: friendships_hybrid — 6 directed edges (core columns + JSON extras)
-- ============================================================================
-- Core columns: src, dst, weight, relationship_type (frequently queried).
-- Extras JSON: since_year, frequency, context, rating (optional metadata).
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.graph.friendships_hybrid (
    src                 BIGINT,
    dst                 BIGINT,
    weight              DOUBLE,
    relationship_type   STRING,
    extras              STRING
) LOCATION '{{data_path}}/friendships_hybrid';

INSERT INTO {{zone_name}}.graph.friendships_hybrid VALUES
    (1, 2, 1.0, 'mentor', '{"since_year": 2020, "frequency": "daily", "context": "work", "rating": 5}'),
    (1, 3, 0.8, 'colleague', '{"since_year": 2019, "frequency": "weekly", "context": "work", "rating": 4}'),
    (2, 3, 0.5, 'friend', '{"since_year": 2021, "frequency": "monthly", "context": "social", "rating": 3}'),
    (3, 4, 0.9, 'manager', '{"since_year": 2018, "frequency": "daily", "context": "work", "rating": 5}'),
    (4, 5, 0.7, 'colleague', '{"since_year": 2022, "frequency": "weekly", "context": "work", "rating": 4}'),
    (5, 1, 0.6, 'friend', '{"since_year": 2020, "frequency": "monthly", "context": "social", "rating": 4}');

DETECT SCHEMA FOR TABLE {{zone_name}}.graph.friendships_hybrid;
GRANT ADMIN ON TABLE {{zone_name}}.graph.friendships_hybrid TO USER {{current_user}};


-- ============================================================================
-- STEP 3: Create named graph definition
-- ============================================================================
-- Creates a graph definition coupling vertex and edge tables together.
-- Extra properties live in the JSON extras column (hybrid mode).
-- ============================================================================
CREATE GRAPH IF NOT EXISTS hybrid_demo
    VERTEX TABLE {{zone_name}}.graph.persons_hybrid ID COLUMN id LABEL COLUMN label
    EDGE TABLE {{zone_name}}.graph.friendships_hybrid SOURCE COLUMN src TARGET COLUMN dst
    WEIGHT COLUMN weight
    LABEL COLUMN relationship_type
    DIRECTED;
