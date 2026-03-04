-- ============================================================================
-- Graph Flattened Mode — Queries
-- ============================================================================
-- Demonstrates graph analytics on FLATTENED property tables where all
-- properties are direct columns. Every query highlights a real-world
-- scenario and shows the flattened-mode advantage: direct column access
-- with full predicate pushdown — no JSON extraction needed.
-- ============================================================================


-- ============================================================================
-- PART 1: BASIC GRAPH EXPLORATION
-- ============================================================================


-- ============================================================================
-- 1. COMPANY DIRECTORY — Who works here?
-- ============================================================================
-- The most basic graph query: list all vertices with their properties.
-- In flattened mode every property is a direct column — fast and simple.

SELECT id, name, age, department, city, title, level, active
FROM {{zone_name}}.graph.persons_flattened
ORDER BY id;


-- ============================================================================
-- 2. RELATIONSHIP LEDGER — All connections with details
-- ============================================================================
-- List every directed edge enriched with vertex names. Flattened mode means
-- the JOIN and column access are straightforward — no JSON extraction.

SELECT
    f.id AS edge_id,
    p1.name AS from_person,
    p2.name AS to_person,
    f.weight,
    f.relationship_type,
    f.since_year,
    f.frequency,
    f.context
FROM {{zone_name}}.graph.friendships_flattened f
JOIN {{zone_name}}.graph.persons_flattened p1 ON f.src = p1.id
JOIN {{zone_name}}.graph.persons_flattened p2 ON f.dst = p2.id
ORDER BY f.src, f.dst;


-- ============================================================================
-- 3. HEADCOUNT BY DEPARTMENT — Workforce distribution
-- ============================================================================
-- How many people are in each department? Flattened-mode advantage: the
-- GROUP BY pushes down directly to the Parquet column — no extraction.

SELECT
    department,
    COUNT(*) AS headcount,
    ROUND(AVG(age), 1) AS avg_age,
    SUM(CASE WHEN active THEN 1 ELSE 0 END) AS active_count
FROM {{zone_name}}.graph.persons_flattened
GROUP BY department
ORDER BY headcount DESC;


-- ============================================================================
-- 4. PREDICATE PUSHDOWN — Filter by column values directly
-- ============================================================================
-- Find active senior staff in specific cities. In flattened mode, EVERY
-- predicate pushes down to the storage layer for maximum performance.

SELECT name, department, city, title, level
FROM {{zone_name}}.graph.persons_flattened
WHERE active = true
  AND level IN ('L3', 'L4', 'L5')
ORDER BY level DESC, name;


-- ============================================================================
-- PART 2: DEGREE & CONNECTIVITY ANALYSIS
-- ============================================================================


-- ============================================================================
-- 5. WHO IS MOST CONNECTED? — Out-degree ranking
-- ============================================================================
-- People with the most outgoing connections are potential influencers.
-- Bridge nodes (id=13, 26) should rank highest.

SELECT
    p.name,
    p.department,
    p.title,
    COUNT(f.dst) AS out_degree
FROM {{zone_name}}.graph.persons_flattened p
LEFT JOIN {{zone_name}}.graph.friendships_flattened f ON p.id = f.src
GROUP BY p.id, p.name, p.department, p.title
ORDER BY out_degree DESC
LIMIT 10;


-- ============================================================================
-- 6. WHO IS MOST SOUGHT AFTER? — In-degree ranking
-- ============================================================================
-- People with the most incoming connections are the ones others reach
-- out to — often informal leaders or knowledge hubs.

SELECT
    p.name,
    p.department,
    p.title,
    COUNT(f.src) AS in_degree
FROM {{zone_name}}.graph.persons_flattened p
LEFT JOIN {{zone_name}}.graph.friendships_flattened f ON p.id = f.dst
GROUP BY p.id, p.name, p.department, p.title
ORDER BY in_degree DESC
LIMIT 10;


-- ============================================================================
-- 7. TOTAL DEGREE — Combined connectivity
-- ============================================================================
-- Total degree = in + out. High total degree means a person is both
-- well-connected and well-sought. Directors and bridge nodes should top this.

SELECT
    p.name,
    p.department,
    p.title,
    COALESCE(out_deg.cnt, 0) AS out_degree,
    COALESCE(in_deg.cnt, 0) AS in_degree,
    COALESCE(out_deg.cnt, 0) + COALESCE(in_deg.cnt, 0) AS total_degree
FROM {{zone_name}}.graph.persons_flattened p
LEFT JOIN (
    SELECT src, COUNT(*) AS cnt FROM {{zone_name}}.graph.friendships_flattened GROUP BY src
) out_deg ON p.id = out_deg.src
LEFT JOIN (
    SELECT dst, COUNT(*) AS cnt FROM {{zone_name}}.graph.friendships_flattened GROUP BY dst
) in_deg ON p.id = in_deg.dst
ORDER BY total_degree DESC
LIMIT 10;


-- ============================================================================
-- PART 3: RELATIONSHIP PATTERNS
-- ============================================================================


-- ============================================================================
-- 8. DEPARTMENT CONNECTIVITY MATRIX — Who talks to whom?
-- ============================================================================
-- Cross-department connection counts reveal organizational silos and
-- collaboration hotspots. Intra-department counts should dominate (clusters).

SELECT
    p1.department AS from_dept,
    p2.department AS to_dept,
    COUNT(*) AS connections,
    ROUND(AVG(f.weight), 2) AS avg_weight
FROM {{zone_name}}.graph.friendships_flattened f
JOIN {{zone_name}}.graph.persons_flattened p1 ON f.src = p1.id
JOIN {{zone_name}}.graph.persons_flattened p2 ON f.dst = p2.id
GROUP BY p1.department, p2.department
ORDER BY connections DESC;


-- ============================================================================
-- 9. STRONGEST CONNECTIONS — High-weight relationships
-- ============================================================================
-- Weight > 0.8 indicates strong bonds — mentor and close colleague ties.
-- These are the relationships that hold the organization together.

SELECT
    p1.name AS from_person,
    p2.name AS to_person,
    f.weight,
    f.relationship_type,
    f.context
FROM {{zone_name}}.graph.friendships_flattened f
JOIN {{zone_name}}.graph.persons_flattened p1 ON f.src = p1.id
JOIN {{zone_name}}.graph.persons_flattened p2 ON f.dst = p2.id
WHERE f.weight > 0.8
ORDER BY f.weight DESC;


-- ============================================================================
-- 10. WORK vs SOCIAL — Relationship context breakdown
-- ============================================================================
-- How much of the network is professional vs social? Healthy orgs have
-- both strong work ties AND social bonds that cross team boundaries.

SELECT
    context,
    COUNT(*) AS edge_count,
    ROUND(AVG(weight), 2) AS avg_weight,
    ROUND(AVG(CAST(rating AS DOUBLE)), 1) AS avg_rating,
    COUNT(DISTINCT relationship_type) AS relationship_types
FROM {{zone_name}}.graph.friendships_flattened
GROUP BY context
ORDER BY edge_count DESC;


-- ============================================================================
-- 11. RELATIONSHIP TYPE BREAKDOWN — What kinds of connections exist?
-- ============================================================================
-- Edge types reveal organizational patterns: lots of "mentor" edges means
-- strong knowledge transfer; lots of "bridge" edges means good cross-team flow.

SELECT
    relationship_type,
    COUNT(*) AS count,
    ROUND(AVG(weight), 2) AS avg_weight,
    MIN(since_year) AS earliest,
    MAX(since_year) AS latest
FROM {{zone_name}}.graph.friendships_flattened
GROUP BY relationship_type
ORDER BY count DESC;


-- ============================================================================
-- 12. RECIPROCAL RELATIONSHIPS — Bidirectional bonds
-- ============================================================================
-- When A connects to B AND B connects to A, the relationship is mutual.
-- Reciprocal ties are stronger and more stable than one-directional ones.

SELECT
    p1.name AS person_a,
    p2.name AS person_b,
    f1.relationship_type AS a_to_b,
    f2.relationship_type AS b_to_a,
    f1.weight AS a_to_b_weight,
    f2.weight AS b_to_a_weight
FROM {{zone_name}}.graph.friendships_flattened f1
JOIN {{zone_name}}.graph.friendships_flattened f2
    ON f1.src = f2.dst AND f1.dst = f2.src
JOIN {{zone_name}}.graph.persons_flattened p1 ON f1.src = p1.id
JOIN {{zone_name}}.graph.persons_flattened p2 ON f1.dst = p2.id
WHERE f1.src < f1.dst
ORDER BY f1.weight + f2.weight DESC;


-- ============================================================================
-- PART 4: GRAPH TRAVERSAL
-- ============================================================================


-- ============================================================================
-- 13. 2-HOP REACH — Who can each person reach in 2 steps?
-- ============================================================================
-- Two-hop reach measures how quickly information can spread from a person.
-- Bridge nodes should have the widest 2-hop reach.

SELECT
    p_start.name AS person,
    p_start.department,
    COUNT(DISTINCT f2.dst) AS two_hop_reach
FROM {{zone_name}}.graph.persons_flattened p_start
JOIN {{zone_name}}.graph.friendships_flattened f1 ON p_start.id = f1.src
JOIN {{zone_name}}.graph.friendships_flattened f2 ON f1.dst = f2.src
WHERE f2.dst != p_start.id
GROUP BY p_start.id, p_start.name, p_start.department
ORDER BY two_hop_reach DESC
LIMIT 10;


-- ============================================================================
-- 14. MENTORSHIP TREE — Who mentors whom?
-- ============================================================================
-- Map the explicit mentorship hierarchy. Directors and Managers should
-- appear as mentors, with subordinates in the same department.

SELECT
    p1.name AS mentor,
    p1.title AS mentor_title,
    p1.department AS dept,
    p2.name AS mentee,
    p2.title AS mentee_title,
    f.weight AS bond_strength
FROM {{zone_name}}.graph.friendships_flattened f
JOIN {{zone_name}}.graph.persons_flattened p1 ON f.src = p1.id
JOIN {{zone_name}}.graph.persons_flattened p2 ON f.dst = p2.id
WHERE f.relationship_type = 'mentor'
ORDER BY p1.department, p1.name, p2.name;


-- ============================================================================
-- 15. CITY-BASED COLLABORATION — Cross-department bonds within cities
-- ============================================================================
-- People in the same city but different departments form social bridges.
-- These bonds prevent the org from becoming siloed by department alone.

SELECT
    p1.city,
    p1.department AS from_dept,
    p2.department AS to_dept,
    COUNT(*) AS connections,
    ROUND(AVG(f.weight), 2) AS avg_weight
FROM {{zone_name}}.graph.friendships_flattened f
JOIN {{zone_name}}.graph.persons_flattened p1 ON f.src = p1.id
JOIN {{zone_name}}.graph.persons_flattened p2 ON f.dst = p2.id
WHERE p1.city = p2.city
  AND p1.department != p2.department
GROUP BY p1.city, p1.department, p2.department
ORDER BY connections DESC;


-- ============================================================================
-- 16. BRIDGE EMPLOYEES — Cross-department connectors
-- ============================================================================
-- Find people who connect to the most different departments.
-- These are the organizational bridges that prevent information silos.

SELECT
    p.name,
    p.department AS home_dept,
    COUNT(DISTINCT p2.department) AS depts_reached,
    COUNT(*) AS total_connections
FROM {{zone_name}}.graph.friendships_flattened f
JOIN {{zone_name}}.graph.persons_flattened p ON f.src = p.id
JOIN {{zone_name}}.graph.persons_flattened p2 ON f.dst = p2.id
WHERE p.department != p2.department
GROUP BY p.id, p.name, p.department
ORDER BY depts_reached DESC, total_connections DESC
LIMIT 10;


-- ============================================================================
-- PART 5: CYPHER GRAPH QUERIES
-- ============================================================================


-- ============================================================================
-- 17. CYPHER — All nodes in the graph
-- ============================================================================
-- Basic Cypher pattern: return all vertices.

USE flattened_demo
MATCH (n)
RETURN n;


-- ============================================================================
-- 18. CYPHER — All directed relationships
-- ============================================================================
-- Pattern match all edges: (source)-[relationship]->(target).

USE flattened_demo
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- 19. CYPHER — PageRank: Who are the informal influencers?
-- ============================================================================
-- PageRank finds nodes that are referenced by other well-connected nodes.
-- Directors and bridge nodes should rank highest.

USE flattened_demo
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD nodeId, score, rank
RETURN nodeId, score, rank
ORDER BY score DESC;


-- ============================================================================
-- 20. CYPHER — Degree centrality
-- ============================================================================
-- In/out/total degree computed natively in the graph engine.

USE flattened_demo
CALL algo.degree()
YIELD nodeId, inDegree, outDegree, totalDegree
RETURN nodeId, inDegree, outDegree, totalDegree
ORDER BY totalDegree DESC;


-- ============================================================================
-- 21. CYPHER — Louvain community detection
-- ============================================================================
-- Louvain detects natural communities. Should find ~5 clusters matching
-- the department structure, plus possible sub-clusters from city bonds.

USE flattened_demo
CALL algo.louvain({resolution: 1.0})
YIELD nodeId, communityId
RETURN communityId, collect(nodeId) AS members, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 22. CYPHER — Shortest path between two employees
-- ============================================================================
-- Find the shortest weighted path. Useful for answering "how far apart
-- are these two people in the organization?"

USE flattened_demo
CALL algo.shortestPath({source: 1, target: 42})
YIELD nodeId, step, distance
RETURN nodeId, step, distance
ORDER BY step;


-- ============================================================================
-- 23. CYPHER — Betweenness centrality: Critical connectors
-- ============================================================================
-- High betweenness = the person sits on many shortest paths between others.
-- Losing these people would fragment the network.

USE flattened_demo
CALL algo.betweenness()
YIELD nodeId, centrality, rank
RETURN nodeId, centrality, rank
ORDER BY centrality DESC;


-- ============================================================================
-- 24. CYPHER — Connected components
-- ============================================================================
-- Are there isolated groups? In a well-connected org, everyone should be
-- in a single connected component.

USE flattened_demo
CALL algo.connectedComponents()
YIELD nodeId, componentId
RETURN componentId, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 25. CYPHER — Triangle count: Cluster density
-- ============================================================================
-- Triangles indicate tight-knit groups where A knows B, B knows C, and
-- C knows A. High triangle count = strong team cohesion.

USE flattened_demo
CALL algo.triangleCount()
YIELD nodeId, triangleCount
RETURN nodeId, triangleCount
ORDER BY triangleCount DESC;


-- ============================================================================
-- PART 6: VISUALIZATION
-- ============================================================================


-- ============================================================================
-- 26. VISUALIZE — Full company graph
-- ============================================================================
-- All 50 people and ~150 edges — the complete organizational network.

USE flattened_demo
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- 27. VISUALIZE — Mentorship network only
-- ============================================================================
-- Filter to just mentor edges to see the hierarchical structure.

USE flattened_demo
MATCH (a)-[r]->(b)
WHERE r.relationship_type = 'mentor'
RETURN a, r, b;


-- ============================================================================
-- 28. VISUALIZE — Engineering department subgraph
-- ============================================================================
-- Focus on a single department to see its internal structure.

USE flattened_demo
MATCH (a)-[r]->(b)
WHERE a.department = 'Engineering' AND b.department = 'Engineering'
RETURN a, r, b;
