-- ============================================================================
-- Graph JSON Mode — Setup Script
-- ============================================================================
-- Creates Delta graph tables using the JSON property storage mode.
-- All vertex and edge properties are stored in a single JSON string column —
-- the most flexible mode for schema-free, evolving graph data.
--
--   1. persons_json     — 5 vertex nodes (id + JSON props column)
--   2. friendships_json — 6 directed edges (src, dst + JSON props column)
--
-- GRAPH VISUALIZATION (identical data to flattened and hybrid modes):
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
-- JSON MODE ADVANTAGES:
--   - Schema-free: add new properties without ALTER TABLE
--   - Extensible: each vertex/edge can have different properties
--   - SIMD-accelerated JSON extraction in Delta Forge
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.graph
    COMMENT 'Graph property storage mode demo tables';


-- ============================================================================
-- TABLE 1: persons_json — 5 vertex nodes (id + JSON properties)
-- ============================================================================
-- Only the vertex ID is a dedicated column. All other properties live inside
-- a single JSON string. Use json_get_str/json_get_int to extract values.
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.graph.persons_json (
    id      BIGINT,
    label   STRING,
    props   STRING
) LOCATION '{{data_path}}/persons_json';

INSERT INTO {{zone_name}}.graph.persons_json VALUES
    (1, 'Engineering', '{"name": "Alice", "age": 30, "department": "Engineering", "city": "NYC", "skills": ["rust", "python"], "level": "senior", "active": true}'),
    (2, 'Marketing', '{"name": "Bob", "age": 25, "department": "Marketing", "city": "LA", "skills": ["sales", "analytics"], "level": "junior", "active": true}'),
    (3, 'HR', '{"name": "Carol", "age": 35, "department": "HR", "city": "Chicago", "skills": ["recruiting", "training"], "level": "senior", "active": true}'),
    (4, 'Engineering', '{"name": "Dave", "age": 28, "department": "Engineering", "city": "SF", "skills": ["golang", "kubernetes"], "level": "mid", "active": false}'),
    (5, 'Finance', '{"name": "Eve", "age": 32, "department": "Finance", "city": "NYC", "skills": ["accounting", "forecasting"], "level": "senior", "active": true}');

DETECT SCHEMA FOR TABLE {{zone_name}}.graph.persons_json;
GRANT ADMIN ON TABLE {{zone_name}}.graph.persons_json TO USER {{current_user}};


-- ============================================================================
-- TABLE 2: friendships_json — 6 directed edges (src, dst + JSON properties)
-- ============================================================================
-- Only src/dst vertex IDs are dedicated columns. All edge properties are
-- stored in a single JSON string column.
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.graph.friendships_json (
    src     BIGINT,
    dst     BIGINT,
    props   STRING
) LOCATION '{{data_path}}/friendships_json';

INSERT INTO {{zone_name}}.graph.friendships_json VALUES
    (1, 2, '{"weight": 1.0, "relationship_type": "mentor", "since_year": 2020, "frequency": "daily", "context": "work", "rating": 5}'),
    (1, 3, '{"weight": 0.8, "relationship_type": "colleague", "since_year": 2019, "frequency": "weekly", "context": "work", "rating": 4}'),
    (2, 3, '{"weight": 0.5, "relationship_type": "friend", "since_year": 2021, "frequency": "monthly", "context": "social", "rating": 3}'),
    (3, 4, '{"weight": 0.9, "relationship_type": "manager", "since_year": 2018, "frequency": "daily", "context": "work", "rating": 5}'),
    (4, 5, '{"weight": 0.7, "relationship_type": "colleague", "since_year": 2022, "frequency": "weekly", "context": "work", "rating": 4}'),
    (5, 1, '{"weight": 0.6, "relationship_type": "friend", "since_year": 2020, "frequency": "monthly", "context": "social", "rating": 4}');

DETECT SCHEMA FOR TABLE {{zone_name}}.graph.friendships_json;
GRANT ADMIN ON TABLE {{zone_name}}.graph.friendships_json TO USER {{current_user}};


-- ============================================================================
-- STEP 3: Create named graph definition
-- ============================================================================
-- Creates a graph definition coupling vertex and edge tables together.
-- Weight lives inside JSON props, not as a dedicated column.
-- ============================================================================
CREATE GRAPH IF NOT EXISTS json_demo
    VERTEX TABLE {{zone_name}}.graph.persons_json ID COLUMN id LABEL COLUMN label
    EDGE TABLE {{zone_name}}.graph.friendships_json SOURCE COLUMN src TARGET COLUMN dst
    DIRECTED;
