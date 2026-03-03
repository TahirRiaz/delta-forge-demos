-- ============================================================================
-- Graph Cypher Queries — Setup Script
-- ============================================================================
-- Creates Delta graph tables for demonstrating Cypher query language support.
-- Same 5-person social graph as the flattened mode demo, using standard
-- src/dst columns for automatic Cypher column detection.
--
--   1. persons_cypher     — 5 vertex nodes (all properties as columns)
--   2. friendships_cypher — 6 directed edges (all properties as columns)
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
--
-- CYPHER SYNTAX:
--   USE table_name
--   MATCH (n)-[r]->(m)
--   WHERE n.property = value
--   RETURN n, r, m
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.graph
    COMMENT 'Graph property storage mode demo tables';


-- ============================================================================
-- TABLE 1: persons_cypher — 5 vertex nodes
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.graph.persons_cypher (
    id          BIGINT,
    name        STRING,
    age         INT,
    department  STRING,
    city        STRING,
    level       STRING,
    active      BOOLEAN
) LOCATION '{{data_path}}/persons_cypher';

INSERT INTO {{zone_name}}.graph.persons_cypher VALUES
    (1, 'Alice', 30, 'Engineering', 'NYC', 'senior', true),
    (2, 'Bob', 25, 'Marketing', 'LA', 'junior', true),
    (3, 'Carol', 35, 'HR', 'Chicago', 'senior', true),
    (4, 'Dave', 28, 'Engineering', 'SF', 'mid', false),
    (5, 'Eve', 32, 'Finance', 'NYC', 'senior', true);

DETECT SCHEMA FOR TABLE {{zone_name}}.graph.persons_cypher;
GRANT ADMIN ON TABLE {{zone_name}}.graph.persons_cypher TO USER {{current_user}};


-- ============================================================================
-- TABLE 2: friendships_cypher — 6 directed edges
-- ============================================================================
-- Uses standard src/dst columns so Cypher auto-detection works out of the box.
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.graph.friendships_cypher (
    src                 BIGINT,
    dst                 BIGINT,
    weight              DOUBLE,
    relationship_type   STRING,
    since_year          INT,
    frequency           STRING,
    context             STRING,
    rating              INT
) LOCATION '{{data_path}}/friendships_cypher';

INSERT INTO {{zone_name}}.graph.friendships_cypher VALUES
    (1, 2, 1.0, 'mentor', 2020, 'daily', 'work', 5),
    (1, 3, 0.8, 'colleague', 2019, 'weekly', 'work', 4),
    (2, 3, 0.5, 'friend', 2021, 'monthly', 'social', 3),
    (3, 4, 0.9, 'manager', 2018, 'daily', 'work', 5),
    (4, 5, 0.7, 'colleague', 2022, 'weekly', 'work', 4),
    (5, 1, 0.6, 'friend', 2020, 'monthly', 'social', 4);

DETECT SCHEMA FOR TABLE {{zone_name}}.graph.friendships_cypher;
GRANT ADMIN ON TABLE {{zone_name}}.graph.friendships_cypher TO USER {{current_user}};


-- ============================================================================
-- STEP 3: Configure graph metadata for Cypher engine
-- ============================================================================
-- Explicit graph configuration tells the Cypher executor which columns are
-- source, target, weight, and vertex ID. While auto-detection works for
-- standard names (src/dst), explicit config is best practice for clarity.
-- ============================================================================
CONFIGURE GRAPH {{zone_name}}.graph.friendships_cypher AS EDGE
    SOURCE COLUMN src
    TARGET COLUMN dst
    WEIGHT COLUMN weight
    DIRECTED;

CONFIGURE GRAPH {{zone_name}}.graph.persons_cypher AS VERTEX
    VERTEX ID COLUMN id;
