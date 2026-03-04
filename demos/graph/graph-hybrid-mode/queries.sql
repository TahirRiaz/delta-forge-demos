-- ============================================================================
-- Graph Hybrid Mode — Queries
-- ============================================================================
-- Demonstrates the hybrid access pattern: core columns for frequent queries
-- plus JSON extraction for optional/extensible properties.
--
-- KEY PATTERN: Use core columns (name, age, weight, relationship_type) for
-- filtering and joins (fast, pushdown-capable), and json_get_* for extras.
-- ============================================================================


-- ============================================================================
-- PART 1: HYBRID ACCESS PATTERNS
-- ============================================================================


-- ============================================================================
-- 1. COMPANY DIRECTORY — Core columns + JSON extras
-- ============================================================================
-- Name and age are core columns (fast access). Department, city, level
-- are extracted from the JSON extras column.

SELECT
    id,
    name,
    age,
    json_get_str(extras, '$.department') AS department,
    json_get_str(extras, '$.city') AS city,
    json_get_str(extras, '$.title') AS title,
    json_get_str(extras, '$.level') AS level
FROM {{zone_name}}.graph.persons_hybrid
ORDER BY id;


-- ============================================================================
-- 2. FAST PATH — Column-only queries skip JSON entirely
-- ============================================================================
-- When you only need core columns, hybrid mode is as fast as flattened.
-- No JSON parsing occurs — the query only touches name and age columns.

SELECT id, name, age
FROM {{zone_name}}.graph.persons_hybrid
WHERE age > 35
ORDER BY age DESC;


-- ============================================================================
-- 3. RELATIONSHIP LEDGER — Core edge columns + JSON metadata
-- ============================================================================
-- Weight and relationship_type are core columns. Since_year and frequency
-- are extracted from JSON extras.

SELECT
    p1.name AS from_person,
    p2.name AS to_person,
    f.weight,
    f.relationship_type,
    json_get_int(f.extras, '$.since_year') AS since_year,
    json_get_str(f.extras, '$.frequency') AS frequency
FROM {{zone_name}}.graph.friendships_hybrid f
JOIN {{zone_name}}.graph.persons_hybrid p1 ON f.src = p1.id
JOIN {{zone_name}}.graph.persons_hybrid p2 ON f.dst = p2.id
ORDER BY f.weight DESC
LIMIT 20;


-- ============================================================================
-- 4. MIXED FILTERING — Column predicate + JSON predicate
-- ============================================================================
-- Core column filter (relationship_type) pushes down to storage.
-- JSON filter (context) is applied after retrieval. This shows the
-- hybrid trade-off: fast for core, flexible for extras.

SELECT
    f.src,
    f.dst,
    f.relationship_type,
    f.weight,
    json_get_str(f.extras, '$.context') AS context,
    json_get_int(f.extras, '$.rating') AS rating
FROM {{zone_name}}.graph.friendships_hybrid f
WHERE f.relationship_type = 'mentor'
  AND json_get_str(f.extras, '$.context') = 'work'
ORDER BY f.weight DESC;


-- ============================================================================
-- PART 2: ANALYTICS WITH HYBRID ACCESS
-- ============================================================================


-- ============================================================================
-- 5. HEADCOUNT BY DEPARTMENT — JSON grouping
-- ============================================================================
-- Department lives in extras JSON. We GROUP BY on JSON-extracted values.

SELECT
    json_get_str(extras, '$.department') AS department,
    COUNT(*) AS headcount,
    ROUND(AVG(age), 1) AS avg_age
FROM {{zone_name}}.graph.persons_hybrid
GROUP BY json_get_str(extras, '$.department')
ORDER BY headcount DESC;


-- ============================================================================
-- 6. OUT-DEGREE RANKING — Core column joins only
-- ============================================================================
-- This query uses only core columns (name, src) — no JSON needed.
-- Pure column joins are as fast as flattened mode.

SELECT
    p.name,
    p.age,
    COUNT(f.dst) AS out_degree
FROM {{zone_name}}.graph.persons_hybrid p
LEFT JOIN {{zone_name}}.graph.friendships_hybrid f ON p.id = f.src
GROUP BY p.id, p.name, p.age
ORDER BY out_degree DESC
LIMIT 10;


-- ============================================================================
-- 7. RELATIONSHIP TYPE ANALYSIS — Core column aggregation
-- ============================================================================
-- relationship_type is a core column, so this GROUP BY is efficient.
-- Rating comes from JSON extras for additional insight.

SELECT
    relationship_type,
    COUNT(*) AS edge_count,
    ROUND(AVG(weight), 2) AS avg_weight,
    ROUND(AVG(CAST(json_get_int(extras, '$.rating') AS DOUBLE)), 1) AS avg_rating
FROM {{zone_name}}.graph.friendships_hybrid
GROUP BY relationship_type
ORDER BY edge_count DESC;


-- ============================================================================
-- 8. DEPARTMENT CONNECTIVITY — Mixed column + JSON access
-- ============================================================================
-- Join on core columns (src, dst), but department is in JSON extras.

SELECT
    json_get_str(p1.extras, '$.department') AS from_dept,
    json_get_str(p2.extras, '$.department') AS to_dept,
    COUNT(*) AS connections,
    ROUND(AVG(f.weight), 2) AS avg_weight
FROM {{zone_name}}.graph.friendships_hybrid f
JOIN {{zone_name}}.graph.persons_hybrid p1 ON f.src = p1.id
JOIN {{zone_name}}.graph.persons_hybrid p2 ON f.dst = p2.id
GROUP BY json_get_str(p1.extras, '$.department'), json_get_str(p2.extras, '$.department')
ORDER BY connections DESC;


-- ============================================================================
-- 9. WORK vs SOCIAL — JSON context breakdown
-- ============================================================================
-- Context lives in extras JSON. Weight is a core column.

SELECT
    json_get_str(extras, '$.context') AS context,
    json_get_str(extras, '$.frequency') AS frequency,
    COUNT(*) AS edge_count,
    ROUND(AVG(weight), 2) AS avg_weight
FROM {{zone_name}}.graph.friendships_hybrid
GROUP BY json_get_str(extras, '$.context'), json_get_str(extras, '$.frequency')
ORDER BY edge_count DESC;


-- ============================================================================
-- 10. MENTORSHIP TREE — Core type filter + JSON enrichment
-- ============================================================================
-- Filter on core column (relationship_type = 'mentor'), then enrich
-- with JSON extras for department and title.

SELECT
    p1.name AS mentor,
    json_get_str(p1.extras, '$.title') AS mentor_title,
    json_get_str(p1.extras, '$.department') AS dept,
    p2.name AS mentee,
    json_get_str(p2.extras, '$.title') AS mentee_title,
    f.weight AS bond_strength
FROM {{zone_name}}.graph.friendships_hybrid f
JOIN {{zone_name}}.graph.persons_hybrid p1 ON f.src = p1.id
JOIN {{zone_name}}.graph.persons_hybrid p2 ON f.dst = p2.id
WHERE f.relationship_type = 'mentor'
ORDER BY json_get_str(p1.extras, '$.department'), p1.name;


-- ============================================================================
-- 11. BRIDGE EMPLOYEES — Cross-department connectors
-- ============================================================================

SELECT
    p.name,
    json_get_str(p.extras, '$.department') AS home_dept,
    COUNT(DISTINCT json_get_str(p2.extras, '$.department')) AS depts_reached,
    COUNT(*) AS total_connections
FROM {{zone_name}}.graph.friendships_hybrid f
JOIN {{zone_name}}.graph.persons_hybrid p ON f.src = p.id
JOIN {{zone_name}}.graph.persons_hybrid p2 ON f.dst = p2.id
WHERE json_get_str(p.extras, '$.department') != json_get_str(p2.extras, '$.department')
GROUP BY p.id, p.name, json_get_str(p.extras, '$.department')
ORDER BY depts_reached DESC, total_connections DESC
LIMIT 10;


-- ============================================================================
-- 12. 2-HOP REACH — Core column traversal
-- ============================================================================
-- The join chain uses only core columns (src/dst). JSON enrichment
-- is added only at the end for display.

SELECT
    p_start.name AS person,
    json_get_str(p_start.extras, '$.department') AS department,
    COUNT(DISTINCT f2.dst) AS two_hop_reach
FROM {{zone_name}}.graph.persons_hybrid p_start
JOIN {{zone_name}}.graph.friendships_hybrid f1 ON p_start.id = f1.src
JOIN {{zone_name}}.graph.friendships_hybrid f2 ON f1.dst = f2.src
WHERE f2.dst != p_start.id
GROUP BY p_start.id, p_start.name, json_get_str(p_start.extras, '$.department')
ORDER BY two_hop_reach DESC
LIMIT 10;


-- ============================================================================
-- 13. RECIPROCAL BONDS — Bidirectional relationships
-- ============================================================================

SELECT
    p1.name AS person_a,
    p2.name AS person_b,
    f1.relationship_type AS a_to_b,
    f2.relationship_type AS b_to_a,
    f1.weight AS a_to_b_weight,
    f2.weight AS b_to_a_weight
FROM {{zone_name}}.graph.friendships_hybrid f1
JOIN {{zone_name}}.graph.friendships_hybrid f2
    ON f1.src = f2.dst AND f1.dst = f2.src
JOIN {{zone_name}}.graph.persons_hybrid p1 ON f1.src = p1.id
JOIN {{zone_name}}.graph.persons_hybrid p2 ON f1.dst = p2.id
WHERE f1.src < f1.dst
ORDER BY f1.weight + f2.weight DESC;


-- ============================================================================
-- PART 3: CYPHER GRAPH QUERIES
-- ============================================================================


-- ============================================================================
-- 14. CYPHER — All nodes
-- ============================================================================

USE hybrid_demo
MATCH (n)
RETURN n;


-- ============================================================================
-- 15. CYPHER — All relationships
-- ============================================================================

USE hybrid_demo
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- 16. CYPHER — PageRank: Informal influencers
-- ============================================================================

USE hybrid_demo
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD nodeId, score, rank
RETURN nodeId, score, rank
ORDER BY score DESC;


-- ============================================================================
-- 17. CYPHER — Louvain community detection
-- ============================================================================

USE hybrid_demo
CALL algo.louvain({resolution: 1.0})
YIELD nodeId, communityId
RETURN communityId, collect(nodeId) AS members, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 18. CYPHER — Betweenness centrality
-- ============================================================================

USE hybrid_demo
CALL algo.betweenness()
YIELD nodeId, centrality, rank
RETURN nodeId, centrality, rank
ORDER BY centrality DESC;


-- ============================================================================
-- PART 4: VISUALIZATION
-- ============================================================================


-- ============================================================================
-- 19. VISUALIZE — Full company graph
-- ============================================================================

USE hybrid_demo
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- 20. VISUALIZE — Mentorship hierarchy
-- ============================================================================

USE hybrid_demo
MATCH (a)-[r]->(b)
WHERE r.relationship_type = 'mentor'
RETURN a, r, b;
