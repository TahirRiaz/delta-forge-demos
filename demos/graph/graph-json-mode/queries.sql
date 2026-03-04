-- ============================================================================
-- Graph JSON Mode — Queries
-- ============================================================================
-- Demonstrates graph operations with JSON property extraction.
-- All vertex/edge properties are stored in a single JSON string column.
--
-- KEY PATTERN: Every property requires json_get_str/json_get_int/json_get_float.
-- This is the most flexible mode — ideal for evolving schemas.
-- ============================================================================


-- ============================================================================
-- PART 1: JSON PROPERTY EXTRACTION
-- ============================================================================


-- ============================================================================
-- 1. COMPANY DIRECTORY — Extract all properties from JSON
-- ============================================================================
-- Every property must be extracted via json_get_*. This is the trade-off:
-- more flexible schema, but every access requires JSON parsing.

SELECT
    id,
    json_get_str(props, '$.name') AS name,
    json_get_int(props, '$.age') AS age,
    json_get_str(props, '$.department') AS department,
    json_get_str(props, '$.city') AS city,
    json_get_str(props, '$.title') AS title,
    json_get_str(props, '$.level') AS level
FROM {{zone_name}}.graph.persons_json
ORDER BY id;


-- ============================================================================
-- 2. RAW JSON — See the full property blobs
-- ============================================================================
-- JSON mode stores everything in one column. This query shows the raw
-- JSON to understand the storage format.

SELECT id, label, props
FROM {{zone_name}}.graph.persons_json
ORDER BY id
LIMIT 10;


-- ============================================================================
-- 3. RELATIONSHIP LEDGER — Extract edge properties from JSON
-- ============================================================================

SELECT
    f.src,
    json_get_str(p1.props, '$.name') AS from_person,
    f.dst,
    json_get_str(p2.props, '$.name') AS to_person,
    json_get_float(f.props, '$.weight') AS weight,
    json_get_str(f.props, '$.relationship_type') AS relationship_type,
    json_get_int(f.props, '$.since_year') AS since_year
FROM {{zone_name}}.graph.friendships_json f
JOIN {{zone_name}}.graph.persons_json p1 ON f.src = p1.id
JOIN {{zone_name}}.graph.persons_json p2 ON f.dst = p2.id
ORDER BY json_get_float(f.props, '$.weight') DESC
LIMIT 20;


-- ============================================================================
-- 4. FILTER BY JSON VALUE — Find a specific department
-- ============================================================================
-- JSON mode filtering: extract value, then compare.

SELECT
    id,
    json_get_str(props, '$.name') AS name,
    json_get_str(props, '$.department') AS department,
    json_get_str(props, '$.city') AS city,
    json_get_str(props, '$.title') AS title
FROM {{zone_name}}.graph.persons_json
WHERE json_get_str(props, '$.department') = 'Engineering'
ORDER BY id;


-- ============================================================================
-- 5. AGE-BASED FILTERING — Numeric extraction from JSON
-- ============================================================================

SELECT
    id,
    json_get_str(props, '$.name') AS name,
    json_get_int(props, '$.age') AS age,
    json_get_str(props, '$.level') AS level
FROM {{zone_name}}.graph.persons_json
WHERE json_get_int(props, '$.age') > 40
ORDER BY json_get_int(props, '$.age') DESC;


-- ============================================================================
-- PART 2: ANALYTICS VIA JSON EXTRACTION
-- ============================================================================


-- ============================================================================
-- 6. HEADCOUNT BY DEPARTMENT — GROUP BY on extracted JSON
-- ============================================================================

SELECT
    json_get_str(props, '$.department') AS department,
    COUNT(*) AS headcount,
    ROUND(AVG(CAST(json_get_int(props, '$.age') AS DOUBLE)), 1) AS avg_age
FROM {{zone_name}}.graph.persons_json
GROUP BY json_get_str(props, '$.department')
ORDER BY headcount DESC;


-- ============================================================================
-- 7. OUT-DEGREE — Outgoing connections per person
-- ============================================================================

SELECT
    json_get_str(p.props, '$.name') AS name,
    json_get_str(p.props, '$.department') AS department,
    COUNT(f.dst) AS out_degree
FROM {{zone_name}}.graph.persons_json p
LEFT JOIN {{zone_name}}.graph.friendships_json f ON p.id = f.src
GROUP BY p.id, json_get_str(p.props, '$.name'), json_get_str(p.props, '$.department')
ORDER BY out_degree DESC
LIMIT 10;


-- ============================================================================
-- 8. RELATIONSHIP TYPE ANALYSIS — JSON edge aggregation
-- ============================================================================

SELECT
    json_get_str(props, '$.relationship_type') AS rel_type,
    COUNT(*) AS edge_count,
    ROUND(AVG(json_get_float(props, '$.weight')), 2) AS avg_weight,
    ROUND(AVG(CAST(json_get_int(props, '$.rating') AS DOUBLE)), 1) AS avg_rating
FROM {{zone_name}}.graph.friendships_json
GROUP BY json_get_str(props, '$.relationship_type')
ORDER BY edge_count DESC;


-- ============================================================================
-- 9. DEPARTMENT CONNECTIVITY — All properties from JSON
-- ============================================================================

SELECT
    json_get_str(p1.props, '$.department') AS from_dept,
    json_get_str(p2.props, '$.department') AS to_dept,
    COUNT(*) AS connections,
    ROUND(AVG(json_get_float(f.props, '$.weight')), 2) AS avg_weight
FROM {{zone_name}}.graph.friendships_json f
JOIN {{zone_name}}.graph.persons_json p1 ON f.src = p1.id
JOIN {{zone_name}}.graph.persons_json p2 ON f.dst = p2.id
GROUP BY json_get_str(p1.props, '$.department'), json_get_str(p2.props, '$.department')
ORDER BY connections DESC;


-- ============================================================================
-- 10. WORK vs SOCIAL — Context from JSON
-- ============================================================================

SELECT
    json_get_str(props, '$.context') AS context,
    COUNT(*) AS edge_count,
    ROUND(AVG(json_get_float(props, '$.weight')), 2) AS avg_weight
FROM {{zone_name}}.graph.friendships_json
GROUP BY json_get_str(props, '$.context')
ORDER BY edge_count DESC;


-- ============================================================================
-- 11. MENTORSHIP TREE — Filter + extract from JSON
-- ============================================================================

SELECT
    json_get_str(p1.props, '$.name') AS mentor,
    json_get_str(p1.props, '$.title') AS mentor_title,
    json_get_str(p1.props, '$.department') AS dept,
    json_get_str(p2.props, '$.name') AS mentee,
    json_get_str(p2.props, '$.title') AS mentee_title,
    json_get_float(f.props, '$.weight') AS bond_strength
FROM {{zone_name}}.graph.friendships_json f
JOIN {{zone_name}}.graph.persons_json p1 ON f.src = p1.id
JOIN {{zone_name}}.graph.persons_json p2 ON f.dst = p2.id
WHERE json_get_str(f.props, '$.relationship_type') = 'mentor'
ORDER BY json_get_str(p1.props, '$.department'), json_get_str(p1.props, '$.name');


-- ============================================================================
-- 12. BRIDGE EMPLOYEES — Cross-department connectors
-- ============================================================================

SELECT
    json_get_str(p.props, '$.name') AS name,
    json_get_str(p.props, '$.department') AS home_dept,
    COUNT(DISTINCT json_get_str(p2.props, '$.department')) AS depts_reached,
    COUNT(*) AS total_connections
FROM {{zone_name}}.graph.friendships_json f
JOIN {{zone_name}}.graph.persons_json p ON f.src = p.id
JOIN {{zone_name}}.graph.persons_json p2 ON f.dst = p2.id
WHERE json_get_str(p.props, '$.department') != json_get_str(p2.props, '$.department')
GROUP BY p.id, json_get_str(p.props, '$.name'), json_get_str(p.props, '$.department')
ORDER BY depts_reached DESC, total_connections DESC
LIMIT 10;


-- ============================================================================
-- 13. 2-HOP REACH — Traversal with JSON enrichment
-- ============================================================================

SELECT
    json_get_str(p_start.props, '$.name') AS person,
    json_get_str(p_start.props, '$.department') AS department,
    COUNT(DISTINCT f2.dst) AS two_hop_reach
FROM {{zone_name}}.graph.persons_json p_start
JOIN {{zone_name}}.graph.friendships_json f1 ON p_start.id = f1.src
JOIN {{zone_name}}.graph.friendships_json f2 ON f1.dst = f2.src
WHERE f2.dst != p_start.id
GROUP BY p_start.id, json_get_str(p_start.props, '$.name'), json_get_str(p_start.props, '$.department')
ORDER BY two_hop_reach DESC
LIMIT 10;


-- ============================================================================
-- PART 3: CYPHER GRAPH QUERIES
-- ============================================================================


-- ============================================================================
-- 14. CYPHER — All nodes
-- ============================================================================

USE json_demo
MATCH (n)
RETURN n;


-- ============================================================================
-- 15. CYPHER — All relationships
-- ============================================================================

USE json_demo
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- 16. CYPHER — PageRank
-- ============================================================================

USE json_demo
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD nodeId, score, rank
RETURN nodeId, score, rank
ORDER BY score DESC;


-- ============================================================================
-- 17. CYPHER — Louvain community detection
-- ============================================================================

USE json_demo
CALL algo.louvain({resolution: 1.0})
YIELD nodeId, communityId
RETURN communityId, collect(nodeId) AS members, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 18. CYPHER — Betweenness centrality
-- ============================================================================

USE json_demo
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

USE json_demo
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- 20. VISUALIZE — Mentorship hierarchy
-- ============================================================================

USE json_demo
MATCH (a)-[r]->(b)
WHERE r.relationship_type = 'mentor'
RETURN a, r, b;
