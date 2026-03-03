-- ============================================================================
-- Graph Social Network — Setup Script
-- ============================================================================
-- Creates a 100-employee company social network with 300+ directed connections
-- across 8 departments and 5 cities. Uses deterministic generate_series()
-- for reproducible data generation.
--
--   1. departments — 8 department lookup records
--   2. employees   — 100 employee vertex nodes (deterministic generation)
--   3. connections — 300 directed edges (deterministic pairing)
--   4. employee_stats (VIEW) — per-employee degree centrality metrics
--   5. dept_connections (VIEW) — cross-department connection matrix
--
-- Demonstrates:
--   - CREATE DELTA TABLE with deterministic data generation
--   - INSERT INTO ... SELECT FROM generate_series() for graph data
--   - Degree centrality (in-degree, out-degree, total)
--   - Cross-department connectivity analysis
--   - 2-hop neighborhood traversal
--   - Bridge node detection
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.graph
    COMMENT 'Graph property storage mode demo tables';


-- ============================================================================
-- TABLE 1: departments — 8 department lookup records
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.graph.departments (
    dept_id     INT,
    dept_name   STRING,
    floor_num   INT,
    budget_k    INT
) LOCATION '{{data_path}}/departments';

INSERT INTO {{zone_name}}.graph.departments VALUES
    (0, 'Engineering',  3, 5000),
    (1, 'Marketing',    2, 2000),
    (2, 'HR',           1, 1500),
    (3, 'Finance',      4, 1800),
    (4, 'Sales',        2, 3000),
    (5, 'Operations',   1, 2500),
    (6, 'Legal',        4, 1200),
    (7, 'Product',      3, 2200);

DETECT SCHEMA FOR TABLE {{zone_name}}.graph.departments;
GRANT ADMIN ON TABLE {{zone_name}}.graph.departments TO USER {{current_user}};


-- ============================================================================
-- TABLE 2: employees — 100 employee vertex nodes
-- ============================================================================
-- Deterministic generation using modular arithmetic on generate_series IDs.
-- Each employee gets a name, department, city, title, hire year, and level
-- derived from their ID for full reproducibility.
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.graph.employees (
    id          BIGINT,
    name        STRING,
    age         INT,
    department  STRING,
    city        STRING,
    title       STRING,
    hire_year   INT,
    level       STRING,
    active      BOOLEAN
) LOCATION '{{data_path}}/employees';

INSERT INTO {{zone_name}}.graph.employees
SELECT
    id,
    -- First name from pool of 20
    CASE (id % 20)
        WHEN 0  THEN 'Alice'   WHEN 1  THEN 'Bob'     WHEN 2  THEN 'Carol'
        WHEN 3  THEN 'Dave'    WHEN 4  THEN 'Eve'     WHEN 5  THEN 'Frank'
        WHEN 6  THEN 'Grace'   WHEN 7  THEN 'Hank'    WHEN 8  THEN 'Iris'
        WHEN 9  THEN 'Jack'    WHEN 10 THEN 'Kate'    WHEN 11 THEN 'Leo'
        WHEN 12 THEN 'Mia'    WHEN 13 THEN 'Noah'    WHEN 14 THEN 'Olivia'
        WHEN 15 THEN 'Paul'   WHEN 16 THEN 'Quinn'   WHEN 17 THEN 'Rita'
        WHEN 18 THEN 'Sam'    WHEN 19 THEN 'Tina'
    END || '_' || CAST(id AS VARCHAR) AS name,
    -- Age: 23–55 range, deterministic
    23 + CAST(((CAST(id AS DOUBLE) * 0.618033988749895) % 1.0) * 32.0 AS INT) AS age,
    -- Department: 8 departments
    CASE (id % 8)
        WHEN 0 THEN 'Engineering'  WHEN 1 THEN 'Marketing'
        WHEN 2 THEN 'HR'           WHEN 3 THEN 'Finance'
        WHEN 4 THEN 'Sales'        WHEN 5 THEN 'Operations'
        WHEN 6 THEN 'Legal'        WHEN 7 THEN 'Product'
    END AS department,
    -- City: 5 cities
    CASE (id % 5)
        WHEN 0 THEN 'NYC'     WHEN 1 THEN 'SF'       WHEN 2 THEN 'Chicago'
        WHEN 3 THEN 'London'  WHEN 4 THEN 'Berlin'
    END AS city,
    -- Title based on seniority band
    CASE
        WHEN id % 10 = 0 THEN 'Director'
        WHEN id % 5  = 0 THEN 'Senior Manager'
        WHEN id % 3  = 0 THEN 'Manager'
        ELSE 'Individual Contributor'
    END AS title,
    -- Hire year: 2015–2024
    2015 + CAST(id % 10 AS INT) AS hire_year,
    -- Level: derived from title
    CASE
        WHEN id % 10 = 0 THEN 'L6'
        WHEN id % 5  = 0 THEN 'L5'
        WHEN id % 3  = 0 THEN 'L4'
        WHEN id % 2  = 0 THEN 'L3'
        ELSE 'L2'
    END AS level,
    -- Active: ~90% active
    (id % 11 != 0) AS active
FROM generate_series(1, 100) AS t(id);

DETECT SCHEMA FOR TABLE {{zone_name}}.graph.employees;
GRANT ADMIN ON TABLE {{zone_name}}.graph.employees TO USER {{current_user}};


-- ============================================================================
-- TABLE 3: connections — 300 directed edges
-- ============================================================================
-- Deterministic edge generation using golden-ratio-based pairing.
-- Three types of connections with different generation patterns:
--   - Intra-department (same dept): ~100 edges, higher weight
--   - Cross-department: ~100 edges, medium weight
--   - Mentorship (senior -> junior): ~100 edges, based on level/age gap
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.graph.connections (
    id                  BIGINT,
    src                 BIGINT,
    dst                 BIGINT,
    weight              DOUBLE,
    relationship_type   STRING,
    since_year          INT
) LOCATION '{{data_path}}/connections';

-- Batch 1: Intra-department connections (~100 edges)
-- Pairs employees within the same department modular group
INSERT INTO {{zone_name}}.graph.connections
SELECT
    ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src,
    dst,
    -- Weight: 0.5–1.0 range for same-department connections
    ROUND(0.5 + 0.5 * ((CAST(src * 7 + dst AS DOUBLE) * 0.618033988749895) % 1.0), 2) AS weight,
    CASE (CAST((src + dst) AS BIGINT) % 3)
        WHEN 0 THEN 'colleague'
        WHEN 1 THEN 'teammate'
        WHEN 2 THEN 'collaborator'
    END AS relationship_type,
    2018 + CAST((src + dst) % 7 AS INT) AS since_year
FROM (
    SELECT
        ((i * 7 + 3) % 100) + 1 AS src,
        CASE
            WHEN ((i * 7 + 3) % 100) + 1 + (((i * 13 + 5) % 7) + 1) * 8 > 100
            THEN ((i * 7 + 3) % 100) + 1 + (((i * 13 + 5) % 7) + 1) * 8 - 100
            ELSE ((i * 7 + 3) % 100) + 1 + (((i * 13 + 5) % 7) + 1) * 8
        END AS dst
    FROM generate_series(1, 120) AS t(i)
) sub
WHERE src != dst
  AND src BETWEEN 1 AND 100
  AND dst BETWEEN 1 AND 100;

-- Batch 2: Cross-department connections (~100 edges)
-- Pairs employees from different department groups
INSERT INTO {{zone_name}}.graph.connections
SELECT
    1000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src,
    dst,
    -- Weight: 0.2–0.7 range for cross-department connections
    ROUND(0.2 + 0.5 * ((CAST(src * 11 + dst AS DOUBLE) * 0.381966011250105) % 1.0), 2) AS weight,
    CASE (CAST((src * 3 + dst) AS BIGINT) % 4)
        WHEN 0 THEN 'cross-team'
        WHEN 1 THEN 'project'
        WHEN 2 THEN 'social'
        WHEN 3 THEN 'advisory'
    END AS relationship_type,
    2019 + CAST((src * 2 + dst) % 6 AS INT) AS since_year
FROM (
    SELECT
        ((i * 11 + 1) % 100) + 1 AS src,
        ((i * 17 + 7) % 100) + 1 AS dst
    FROM generate_series(1, 130) AS t(i)
) sub
WHERE src != dst
  AND (src % 8) != (dst % 8);

-- Batch 3: Mentorship connections (~80 edges)
-- Senior employees mentoring more junior ones
INSERT INTO {{zone_name}}.graph.connections
SELECT
    2000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src,
    dst,
    -- Weight: 0.6–1.0 for mentorship (high value)
    ROUND(0.6 + 0.4 * ((CAST(src * 3 + dst AS DOUBLE) * 0.618033988749895) % 1.0), 2) AS weight,
    'mentor' AS relationship_type,
    2020 + CAST((src + dst) % 5 AS INT) AS since_year
FROM (
    SELECT
        ((i * 5 + 2) % 100) + 1 AS src,
        ((i * 19 + 11) % 100) + 1 AS dst
    FROM generate_series(1, 100) AS t(i)
) sub
WHERE src != dst
  AND (src % 3 = 0 OR src % 5 = 0);

DETECT SCHEMA FOR TABLE {{zone_name}}.graph.connections;
GRANT ADMIN ON TABLE {{zone_name}}.graph.connections TO USER {{current_user}};


-- ============================================================================
-- VIEW 4: employee_stats — per-employee degree centrality
-- ============================================================================
-- Computes in-degree (incoming connections), out-degree (outgoing connections),
-- and total degree for each employee. Higher degree = more connected.
-- ============================================================================
-- ============================================================================
-- STEP 3: Configure graph metadata
-- ============================================================================
-- Explicit graph configuration defines which columns are vertex IDs, edge
-- source/target, and weight. Avoids relying on auto-detection.
-- ============================================================================
CONFIGURE GRAPH {{zone_name}}.graph.connections AS EDGE
    SOURCE COLUMN src
    TARGET COLUMN dst
    WEIGHT COLUMN weight
    DIRECTED;

CONFIGURE GRAPH {{zone_name}}.graph.employees AS VERTEX
    VERTEX ID COLUMN id;


-- ============================================================================
-- VIEW 4: employee_stats — per-employee degree centrality
-- ============================================================================
-- Computes in-degree (incoming connections), out-degree (outgoing connections),
-- and total degree for each employee. Higher degree = more connected.
-- ============================================================================
CREATE OR REPLACE VIEW {{zone_name}}.graph.employee_stats AS
SELECT
    e.id,
    e.name,
    e.department,
    e.city,
    e.level,
    COALESCE(out_deg.out_degree, 0) AS out_degree,
    COALESCE(in_deg.in_degree, 0) AS in_degree,
    COALESCE(out_deg.out_degree, 0) + COALESCE(in_deg.in_degree, 0) AS total_degree
FROM {{zone_name}}.graph.employees e
LEFT JOIN (
    SELECT src, COUNT(*) AS out_degree FROM {{zone_name}}.graph.connections GROUP BY src
) out_deg ON e.id = out_deg.src
LEFT JOIN (
    SELECT dst, COUNT(*) AS in_degree FROM {{zone_name}}.graph.connections GROUP BY dst
) in_deg ON e.id = in_deg.dst;


-- ============================================================================
-- VIEW 5: dept_connections — cross-department connection matrix
-- ============================================================================
-- Shows how many connections exist between each pair of departments.
-- Helps identify well-connected vs siloed departments.
-- ============================================================================
CREATE OR REPLACE VIEW {{zone_name}}.graph.dept_connections AS
SELECT
    src_e.department AS src_dept,
    dst_e.department AS dst_dept,
    COUNT(*) AS connection_count,
    ROUND(AVG(c.weight), 2) AS avg_weight
FROM {{zone_name}}.graph.connections c
JOIN {{zone_name}}.graph.employees src_e ON c.src = src_e.id
JOIN {{zone_name}}.graph.employees dst_e ON c.dst = dst_e.id
GROUP BY src_e.department, dst_e.department;
