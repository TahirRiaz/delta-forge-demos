-- ============================================================================
-- Manual CSR Cache Management — Setup Script
-- ============================================================================
-- Loads the Zachary Karate Club dataset and creates a graph with automatic
-- CSR disk caching DISABLED (NO AUTO CACHE CSR). This forces every Cypher
-- query to rebuild the graph from Delta tables until the operator manually
-- runs CREATE GRAPHCSR.
--
-- Vertices: 34 club members (IDs 0-33)
-- Edges: 156 rows (78 undirected edges stored bidirectionally, weight=1.0)
-- ============================================================================

-- ############################################################################
-- STEP 1: Zone & Schemas
-- ############################################################################

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.raw_mcsr
    COMMENT 'Manual CSR — external CSV staging tables';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.mcsr
    COMMENT 'Manual CSR — Delta tables and graph definition';

-- ############################################################################
-- STEP 2: External Tables — Raw CSV Readers (pipe-delimited)
-- ############################################################################

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw_mcsr.karate_edges
USING CSV LOCATION '{{data_path}}/edges.csv'
OPTIONS (header = 'true', delimiter = '|');

GRANT ADMIN ON TABLE {{zone_name}}.raw_mcsr.karate_edges TO USER {{current_user}};

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw_mcsr.karate_vertices
USING CSV LOCATION '{{data_path}}/vertices.csv'
OPTIONS (header = 'true', delimiter = '|');

GRANT ADMIN ON TABLE {{zone_name}}.raw_mcsr.karate_vertices TO USER {{current_user}};

-- ############################################################################
-- STEP 3: Delta Tables — Materialized with Proper Types
-- ############################################################################

-- === Edge Table ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.mcsr.edges
LOCATION '{{data_path}}/delta/edges'
AS SELECT
    CAST(src AS BIGINT) AS src,
    CAST(dst AS BIGINT) AS dst,
    CAST(weight AS DOUBLE) AS weight,
    CAST(edge_type AS VARCHAR) AS edge_type
FROM {{zone_name}}.raw_mcsr.karate_edges;

GRANT ADMIN ON TABLE {{zone_name}}.mcsr.edges TO USER {{current_user}};

-- === Vertex Table ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.mcsr.vertices
LOCATION '{{data_path}}/delta/vertices'
AS SELECT
    CAST(vertex_id AS BIGINT) AS vertex_id,
    CAST(name AS VARCHAR) AS name,
    CAST(category AS VARCHAR) AS role
FROM {{zone_name}}.raw_mcsr.karate_vertices;

GRANT ADMIN ON TABLE {{zone_name}}.mcsr.vertices TO USER {{current_user}};

-- ############################################################################
-- STEP 4: Graph Definition — NO AUTO CACHE CSR
-- ############################################################################
-- Creates a named graph with automatic CSR disk caching DISABLED.
-- Without this flag, the engine would auto-write a .dcsr file after every
-- graph rebuild. With NO AUTO CACHE CSR, the operator must explicitly run
-- CREATE GRAPHCSR to populate or refresh the disk cache.

CREATE GRAPH IF NOT EXISTS {{zone_name}}.mcsr.karate_manual
    VERTEX TABLE {{zone_name}}.mcsr.vertices ID COLUMN vertex_id
        NODE TYPE COLUMN role NODE NAME COLUMN name
    EDGE TABLE {{zone_name}}.mcsr.edges SOURCE COLUMN src TARGET COLUMN dst
    WEIGHT COLUMN weight
    EDGE TYPE COLUMN edge_type
    NO AUTO CACHE CSR
    DIRECTED;
