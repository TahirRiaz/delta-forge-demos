-- ============================================================================
-- Graph Flattened Mode — Setup Script
-- ============================================================================
-- Creates Delta graph tables using the FLATTENED property storage mode.
-- All vertex and edge properties are stored as individual columns — the
-- fastest mode with full predicate pushdown and direct column access.
--
--   1. persons_flattened     — 5 vertex nodes (all properties as columns)
--   2. friendships_flattened — 6 directed edges (all properties as columns)
--
-- GRAPH VISUALIZATION:
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
-- Edges with weights + relationship type:
--   Alice -> Bob   (1.0, "mentor")
--   Alice -> Carol (0.8, "colleague")
--   Bob   -> Carol (0.5, "friend")
--   Carol -> Dave  (0.9, "manager")
--   Dave  -> Eve   (0.7, "colleague")
--   Eve   -> Alice (0.6, "friend")
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.graph
    COMMENT 'Graph property storage mode demo tables';


-- ============================================================================
-- TABLE 1: persons_flattened — 5 vertex nodes (all properties as columns)
-- ============================================================================
-- Flattened mode: every property is a dedicated column. This gives the fastest
-- query performance with full predicate pushdown, type safety, and direct
-- column access. Best when the schema is known and stable.
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.graph.persons_flattened (
    id          BIGINT,
    name        STRING,
    age         INT,
    department  STRING,
    city        STRING,
    level       STRING,
    active      BOOLEAN
) LOCATION '{{data_path}}/persons_flattened';

INSERT INTO {{zone_name}}.graph.persons_flattened VALUES
    (1, 'Alice', 30, 'Engineering', 'NYC', 'senior', true),
    (2, 'Bob', 25, 'Marketing', 'LA', 'junior', true),
    (3, 'Carol', 35, 'HR', 'Chicago', 'senior', true),
    (4, 'Dave', 28, 'Engineering', 'SF', 'mid', false),
    (5, 'Eve', 32, 'Finance', 'NYC', 'senior', true);

DETECT SCHEMA FOR TABLE {{zone_name}}.graph.persons_flattened;
GRANT ADMIN ON TABLE {{zone_name}}.graph.persons_flattened TO USER {{current_user}};


-- ============================================================================
-- TABLE 2: friendships_flattened — 6 directed edges (all properties as columns)
-- ============================================================================
-- Each edge carries: weight (0–1 strength), relationship type, year
-- established, interaction frequency, context (work/social), and rating.
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.graph.friendships_flattened (
    src                 BIGINT,
    dst                 BIGINT,
    weight              DOUBLE,
    relationship_type   STRING,
    since_year          INT,
    frequency           STRING,
    context             STRING,
    rating              INT
) LOCATION '{{data_path}}/friendships_flattened';

INSERT INTO {{zone_name}}.graph.friendships_flattened VALUES
    (1, 2, 1.0, 'mentor', 2020, 'daily', 'work', 5),
    (1, 3, 0.8, 'colleague', 2019, 'weekly', 'work', 4),
    (2, 3, 0.5, 'friend', 2021, 'monthly', 'social', 3),
    (3, 4, 0.9, 'manager', 2018, 'daily', 'work', 5),
    (4, 5, 0.7, 'colleague', 2022, 'weekly', 'work', 4),
    (5, 1, 0.6, 'friend', 2020, 'monthly', 'social', 4);

DETECT SCHEMA FOR TABLE {{zone_name}}.graph.friendships_flattened;
GRANT ADMIN ON TABLE {{zone_name}}.graph.friendships_flattened TO USER {{current_user}};


-- ============================================================================
-- STEP 3: Create named graph definition
-- ============================================================================
-- Creates a graph definition coupling vertex and edge tables together.
-- This appears in the Graph Tables page and enables Cypher queries.
-- ============================================================================
CREATE GRAPH IF NOT EXISTS flattened_demo
    VERTEX TABLE {{zone_name}}.graph.persons_flattened ID COLUMN id
    EDGE TABLE {{zone_name}}.graph.friendships_flattened SOURCE COLUMN src TARGET COLUMN dst
    WEIGHT COLUMN weight
    DIRECTED;
