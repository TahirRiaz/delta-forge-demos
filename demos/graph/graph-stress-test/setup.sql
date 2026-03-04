-- ============================================================================
-- Graph Stress Test — Setup Script
-- ============================================================================
-- Creates a 1,000,000-node graph with 5,000,000+ directed edges for extreme
-- performance benchmarking. Simulates a massive enterprise network with
-- 20 departments, 15 cities, and 200 project teams.
--
--   1. st_departments   — 20 department lookup records
--   2. st_people        — 1,000,000 vertex nodes (deterministic generation)
--   3. st_edges         — 5,000,000+ directed edges (6 batches, deterministic)
--   4. st_people_stats  (VIEW) — per-person degree centrality metrics
--   5. st_dept_matrix   (VIEW) — cross-department connection matrix
--
-- WARNING: This demo generates very large datasets. Setup may take several
-- minutes depending on hardware. Designed for stress-testing only.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.graph
    COMMENT 'Graph property storage mode demo tables';


-- ============================================================================
-- TABLE 1: departments — 20 department lookup records
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.graph.st_departments (
    dept_id     INT,
    dept_name   STRING,
    floor_num   INT,
    budget_k    INT,
    region      STRING
) LOCATION '{{data_path}}/st_departments';

INSERT INTO {{zone_name}}.graph.st_departments VALUES
    (0,  'Engineering',       3, 8000, 'Americas'),
    (1,  'Marketing',         2, 3000, 'Americas'),
    (2,  'HR',                1, 2000, 'Americas'),
    (3,  'Finance',           4, 2500, 'EMEA'),
    (4,  'Sales',             2, 5000, 'EMEA'),
    (5,  'Operations',        1, 3500, 'Americas'),
    (6,  'Legal',             4, 1800, 'EMEA'),
    (7,  'Product',           3, 4000, 'Americas'),
    (8,  'Data Science',      3, 3500, 'APAC'),
    (9,  'DevOps',            3, 2800, 'Americas'),
    (10, 'Security',          4, 3000, 'EMEA'),
    (11, 'Customer Support',  1, 2200, 'APAC'),
    (12, 'Research',          5, 6000, 'Americas'),
    (13, 'Design',            2, 2000, 'EMEA'),
    (14, 'QA',                3, 1500, 'APAC'),
    (15, 'Platform',          3, 4500, 'Americas'),
    (16, 'Infrastructure',    5, 5000, 'EMEA'),
    (17, 'Analytics',         2, 2800, 'APAC'),
    (18, 'Mobile',            3, 3200, 'Americas'),
    (19, 'AI/ML',             5, 7000, 'APAC');

DETECT SCHEMA FOR TABLE {{zone_name}}.graph.st_departments;
GRANT ADMIN ON TABLE {{zone_name}}.graph.st_departments TO USER {{current_user}};


-- ============================================================================
-- TABLE 2: st_people — 1,000,000 vertex nodes
-- ============================================================================
-- Deterministic generation using modular arithmetic on generate_series IDs.
-- Each person gets name, department, city, project team, hire year, level,
-- and salary band derived from their ID for full reproducibility.
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.graph.st_people (
    id              BIGINT,
    name            STRING,
    age             INT,
    department      STRING,
    city            STRING,
    project_team    STRING,
    title           STRING,
    hire_year       INT,
    level           STRING,
    salary_band     STRING,
    active          BOOLEAN
) LOCATION '{{data_path}}/st_people';

INSERT INTO {{zone_name}}.graph.st_people
SELECT
    id,
    -- First name from pool of 40 + ID suffix for uniqueness
    CASE (id % 40)
        WHEN 0  THEN 'Alice'    WHEN 1  THEN 'Bob'      WHEN 2  THEN 'Carol'
        WHEN 3  THEN 'Dave'     WHEN 4  THEN 'Eve'      WHEN 5  THEN 'Frank'
        WHEN 6  THEN 'Grace'    WHEN 7  THEN 'Hank'     WHEN 8  THEN 'Iris'
        WHEN 9  THEN 'Jack'     WHEN 10 THEN 'Kate'     WHEN 11 THEN 'Leo'
        WHEN 12 THEN 'Mia'     WHEN 13 THEN 'Noah'     WHEN 14 THEN 'Olivia'
        WHEN 15 THEN 'Paul'    WHEN 16 THEN 'Quinn'    WHEN 17 THEN 'Rita'
        WHEN 18 THEN 'Sam'     WHEN 19 THEN 'Tina'     WHEN 20 THEN 'Uma'
        WHEN 21 THEN 'Victor'  WHEN 22 THEN 'Wendy'    WHEN 23 THEN 'Xander'
        WHEN 24 THEN 'Yara'    WHEN 25 THEN 'Zane'     WHEN 26 THEN 'Aria'
        WHEN 27 THEN 'Blake'   WHEN 28 THEN 'Cleo'     WHEN 29 THEN 'Dean'
        WHEN 30 THEN 'Elise'   WHEN 31 THEN 'Finn'     WHEN 32 THEN 'Gina'
        WHEN 33 THEN 'Hugo'    WHEN 34 THEN 'Ivy'      WHEN 35 THEN 'Jay'
        WHEN 36 THEN 'Kira'    WHEN 37 THEN 'Liam'     WHEN 38 THEN 'Nina'
        WHEN 39 THEN 'Oscar'
    END || '_' || CAST(id AS VARCHAR) AS name,
    -- Age: 22–60 range, deterministic via golden ratio
    22 + CAST(((CAST(id AS DOUBLE) * 0.618033988749895) % 1.0) * 38.0 AS INT) AS age,
    -- Department: 20 departments
    CASE (id % 20)
        WHEN 0  THEN 'Engineering'      WHEN 1  THEN 'Marketing'
        WHEN 2  THEN 'HR'               WHEN 3  THEN 'Finance'
        WHEN 4  THEN 'Sales'            WHEN 5  THEN 'Operations'
        WHEN 6  THEN 'Legal'            WHEN 7  THEN 'Product'
        WHEN 8  THEN 'Data Science'     WHEN 9  THEN 'DevOps'
        WHEN 10 THEN 'Security'         WHEN 11 THEN 'Customer Support'
        WHEN 12 THEN 'Research'         WHEN 13 THEN 'Design'
        WHEN 14 THEN 'QA'              WHEN 15 THEN 'Platform'
        WHEN 16 THEN 'Infrastructure'   WHEN 17 THEN 'Analytics'
        WHEN 18 THEN 'Mobile'           WHEN 19 THEN 'AI/ML'
    END AS department,
    -- City: 15 cities
    CASE (id % 15)
        WHEN 0  THEN 'NYC'         WHEN 1  THEN 'SF'
        WHEN 2  THEN 'Chicago'     WHEN 3  THEN 'London'
        WHEN 4  THEN 'Berlin'      WHEN 5  THEN 'Tokyo'
        WHEN 6  THEN 'Sydney'      WHEN 7  THEN 'Toronto'
        WHEN 8  THEN 'Singapore'   WHEN 9  THEN 'Dublin'
        WHEN 10 THEN 'Seattle'     WHEN 11 THEN 'Austin'
        WHEN 12 THEN 'Amsterdam'   WHEN 13 THEN 'Mumbai'
        WHEN 14 THEN 'Paris'
    END AS city,
    -- Project team: 200 teams
    'Team_' || CAST((id % 200) + 1 AS VARCHAR) AS project_team,
    -- Title based on seniority band
    CASE
        WHEN id % 1000 = 0 THEN 'VP'
        WHEN id % 500  = 0 THEN 'Director'
        WHEN id % 100  = 0 THEN 'Senior Manager'
        WHEN id % 50   = 0 THEN 'Manager'
        WHEN id % 20   = 0 THEN 'Senior Engineer'
        WHEN id % 5    = 0 THEN 'Engineer'
        ELSE 'Associate'
    END AS title,
    -- Hire year: 2010–2025
    2010 + CAST(id % 16 AS INT) AS hire_year,
    -- Level: derived from title
    CASE
        WHEN id % 1000 = 0 THEN 'L8'
        WHEN id % 500  = 0 THEN 'L7'
        WHEN id % 100  = 0 THEN 'L6'
        WHEN id % 50   = 0 THEN 'L5'
        WHEN id % 20   = 0 THEN 'L4'
        WHEN id % 5    = 0 THEN 'L3'
        WHEN id % 3    = 0 THEN 'L2'
        ELSE 'L1'
    END AS level,
    -- Salary band
    CASE
        WHEN id % 1000 = 0 THEN 'Executive'
        WHEN id % 500  = 0 THEN 'Band-5'
        WHEN id % 100  = 0 THEN 'Band-4'
        WHEN id % 50   = 0 THEN 'Band-3'
        WHEN id % 20   = 0 THEN 'Band-2'
        ELSE 'Band-1'
    END AS salary_band,
    -- Active: ~95% active
    (id % 21 != 0) AS active
FROM generate_series(1, 1000000) AS t(id);

DETECT SCHEMA FOR TABLE {{zone_name}}.graph.st_people;
GRANT ADMIN ON TABLE {{zone_name}}.graph.st_people TO USER {{current_user}};


-- ============================================================================
-- TABLE 3: st_edges — 5,000,000+ directed edges (6 batches)
-- ============================================================================
-- Deterministic edge generation using modular arithmetic and golden-ratio
-- based pairing. Six distinct connection types:
--   Batch 1: Intra-department (same dept)     — ~800K edges, high weight
--   Batch 2: Cross-department collaboration   — ~1,000K edges, medium weight
--   Batch 3: Mentorship (senior -> junior)    — ~500K edges, high weight
--   Batch 4: Project-team connections         — ~1,000K edges, medium weight
--   Batch 5: City-local social connections    — ~800K edges, low-medium weight
--   Batch 6: Random weak ties (long-range)    — ~1,000K edges, low weight
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.graph.st_edges (
    id                  BIGINT,
    src                 BIGINT,
    dst                 BIGINT,
    weight              DOUBLE,
    relationship_type   STRING,
    since_year          INT
) LOCATION '{{data_path}}/st_edges';


-- Batch 1: Intra-department connections (~800K edges)
-- Pairs people within the same department modular group
INSERT INTO {{zone_name}}.graph.st_edges
SELECT
    ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src,
    dst,
    ROUND(0.5 + 0.5 * ((CAST(src * 7 + dst AS DOUBLE) * 0.618033988749895) % 1.0), 3) AS weight,
    CASE (CAST((src + dst) AS BIGINT) % 3)
        WHEN 0 THEN 'colleague'
        WHEN 1 THEN 'teammate'
        WHEN 2 THEN 'collaborator'
    END AS relationship_type,
    2016 + CAST((src + dst) % 10 AS INT) AS since_year
FROM (
    SELECT
        ((i * 7 + 3) % 1000000) + 1 AS src,
        CASE
            WHEN ((i * 7 + 3) % 1000000) + 1 + (((i * 13 + 5) % 7) + 1) * 20 > 1000000
            THEN ((i * 7 + 3) % 1000000) + 1 + (((i * 13 + 5) % 7) + 1) * 20 - 1000000
            ELSE ((i * 7 + 3) % 1000000) + 1 + (((i * 13 + 5) % 7) + 1) * 20
        END AS dst
    FROM generate_series(1, 900000) AS t(i)
) sub
WHERE src != dst
  AND src BETWEEN 1 AND 1000000
  AND dst BETWEEN 1 AND 1000000;


-- Batch 2: Cross-department collaboration (~1,000K edges)
INSERT INTO {{zone_name}}.graph.st_edges
SELECT
    10000000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src,
    dst,
    ROUND(0.2 + 0.5 * ((CAST(src * 11 + dst AS DOUBLE) * 0.381966011250105) % 1.0), 3) AS weight,
    CASE (CAST((src * 3 + dst) AS BIGINT) % 5)
        WHEN 0 THEN 'cross-team'
        WHEN 1 THEN 'project'
        WHEN 2 THEN 'advisory'
        WHEN 3 THEN 'committee'
        WHEN 4 THEN 'taskforce'
    END AS relationship_type,
    2018 + CAST((src * 2 + dst) % 8 AS INT) AS since_year
FROM (
    SELECT
        ((i * 11 + 1) % 1000000) + 1 AS src,
        ((i * 17 + 7) % 1000000) + 1 AS dst
    FROM generate_series(1, 1200000) AS t(i)
) sub
WHERE src != dst
  AND (src % 20) != (dst % 20);


-- Batch 3: Mentorship connections (~500K edges)
-- Senior employees mentoring more junior ones
INSERT INTO {{zone_name}}.graph.st_edges
SELECT
    20000000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src,
    dst,
    ROUND(0.6 + 0.4 * ((CAST(src * 3 + dst AS DOUBLE) * 0.618033988749895) % 1.0), 3) AS weight,
    'mentor' AS relationship_type,
    2019 + CAST((src + dst) % 7 AS INT) AS since_year
FROM (
    SELECT
        ((i * 5 + 2) % 1000000) + 1 AS src,
        ((i * 19 + 11) % 1000000) + 1 AS dst
    FROM generate_series(1, 600000) AS t(i)
) sub
WHERE src != dst
  AND (src % 3 = 0 OR src % 5 = 0);


-- Batch 4: Project-team connections (~1,000K edges)
-- People on the same project team
INSERT INTO {{zone_name}}.graph.st_edges
SELECT
    30000000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src,
    dst,
    ROUND(0.3 + 0.4 * ((CAST(src * 23 + dst AS DOUBLE) * 0.618033988749895) % 1.0), 3) AS weight,
    'project-mate' AS relationship_type,
    2020 + CAST((src + dst) % 6 AS INT) AS since_year
FROM (
    SELECT
        ((i * 13 + 7) % 1000000) + 1 AS src,
        ((i * 29 + 3) % 1000000) + 1 AS dst
    FROM generate_series(1, 1200000) AS t(i)
) sub
WHERE src != dst
  AND (src % 200) = (dst % 200);


-- Batch 5: City-local social connections (~800K edges)
-- People in the same city forming social bonds
INSERT INTO {{zone_name}}.graph.st_edges
SELECT
    40000000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src,
    dst,
    ROUND(0.1 + 0.4 * ((CAST(src * 31 + dst AS DOUBLE) * 0.381966011250105) % 1.0), 3) AS weight,
    CASE (CAST((src + dst * 2) AS BIGINT) % 3)
        WHEN 0 THEN 'social'
        WHEN 1 THEN 'lunch-buddy'
        WHEN 2 THEN 'carpool'
    END AS relationship_type,
    2021 + CAST((src + dst) % 5 AS INT) AS since_year
FROM (
    SELECT
        ((i * 37 + 11) % 1000000) + 1 AS src,
        ((i * 41 + 19) % 1000000) + 1 AS dst
    FROM generate_series(1, 1000000) AS t(i)
) sub
WHERE src != dst
  AND (src % 15) = (dst % 15);


-- Batch 6: Random weak ties — long-range connections (~1,000K edges)
-- Simulates random acquaintances across the organization
INSERT INTO {{zone_name}}.graph.st_edges
SELECT
    50000000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src,
    dst,
    ROUND(0.05 + 0.25 * ((CAST(src * 43 + dst AS DOUBLE) * 0.618033988749895) % 1.0), 3) AS weight,
    CASE (CAST((src * 7 + dst * 3) AS BIGINT) % 4)
        WHEN 0 THEN 'acquaintance'
        WHEN 1 THEN 'conference'
        WHEN 2 THEN 'alumni'
        WHEN 3 THEN 'referral'
    END AS relationship_type,
    2022 + CAST((src + dst) % 4 AS INT) AS since_year
FROM (
    SELECT
        ((i * 97 + 13) % 1000000) + 1 AS src,
        ((i * 53 + 29) % 1000000) + 1 AS dst
    FROM generate_series(1, 1050000) AS t(i)
) sub
WHERE src != dst;

DETECT SCHEMA FOR TABLE {{zone_name}}.graph.st_edges;
GRANT ADMIN ON TABLE {{zone_name}}.graph.st_edges TO USER {{current_user}};


-- ============================================================================
-- GRAPH DEFINITION
-- ============================================================================
CREATE GRAPH IF NOT EXISTS stress_test_network
    VERTEX TABLE {{zone_name}}.graph.st_people ID COLUMN id LABEL COLUMN department
    EDGE TABLE {{zone_name}}.graph.st_edges SOURCE COLUMN src TARGET COLUMN dst
    WEIGHT COLUMN weight
    LABEL COLUMN relationship_type
    DIRECTED;


-- ============================================================================
-- VIEW 4: st_people_stats — per-person degree centrality
-- ============================================================================
CREATE OR REPLACE VIEW {{zone_name}}.graph.st_people_stats AS
SELECT
    p.id,
    p.name,
    p.department,
    p.city,
    p.level,
    p.project_team,
    COALESCE(out_deg.out_degree, 0) AS out_degree,
    COALESCE(in_deg.in_degree, 0) AS in_degree,
    COALESCE(out_deg.out_degree, 0) + COALESCE(in_deg.in_degree, 0) AS total_degree
FROM {{zone_name}}.graph.st_people p
LEFT JOIN (
    SELECT src, COUNT(*) AS out_degree FROM {{zone_name}}.graph.st_edges GROUP BY src
) out_deg ON p.id = out_deg.src
LEFT JOIN (
    SELECT dst, COUNT(*) AS in_degree FROM {{zone_name}}.graph.st_edges GROUP BY dst
) in_deg ON p.id = in_deg.dst;


-- ============================================================================
-- VIEW 5: st_dept_matrix — cross-department connection matrix
-- ============================================================================
CREATE OR REPLACE VIEW {{zone_name}}.graph.st_dept_matrix AS
SELECT
    src_p.department AS src_dept,
    dst_p.department AS dst_dept,
    COUNT(*) AS connection_count,
    ROUND(AVG(e.weight), 3) AS avg_weight,
    COUNT(DISTINCT e.relationship_type) AS rel_type_count
FROM {{zone_name}}.graph.st_edges e
JOIN {{zone_name}}.graph.st_people src_p ON e.src = src_p.id
JOIN {{zone_name}}.graph.st_people dst_p ON e.dst = dst_p.id
GROUP BY src_p.department, dst_p.department;
