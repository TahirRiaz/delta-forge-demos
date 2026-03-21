-- ############################################################################
-- ############################################################################
--
--   NETSCIENCE — COAUTHORSHIP NETWORK OF NETWORK SCIENTISTS
--   1,461 Vertices / 2,742 Undirected Edges (5,484 rows) / Weighted
--
-- ############################################################################
-- ############################################################################
--
-- A coauthorship network of scientists working in network theory (Newman, 2006).
-- Non-uniform edge weights reflect collaboration strength. The network contains
-- multiple connected components (isolated authors and small groups) with clear
-- research-group community structure (modularity ~0.95).
--
-- PART 1: DATA INTEGRITY CHECKS (queries 1–4)
-- PART 2: CYPHER — GRAPH EXPLORATION (queries 5–9)
-- PART 3: CYPHER — GRAPH ALGORITHMS (queries 10–16)
-- PART 4: VERIFICATION SUMMARY (query 17)
--
-- ############################################################################


-- ############################################################################
-- PART 1: DATA INTEGRITY CHECKS
-- ############################################################################


-- ============================================================================
-- 1. VERTEX & EDGE COUNTS — Verify data loaded correctly
-- ============================================================================
-- 1,461 vertices, 5,484 edge rows (2,742 undirected edges x 2)

-- Verify vertex count
ASSERT VALUE row_count = 1461
SELECT COUNT(*) AS row_count FROM {{zone_name}}.netscience.vertices;

-- Verify edge count (2,742 undirected edges x 2)
ASSERT VALUE row_count = 5484
SELECT COUNT(*) AS row_count FROM {{zone_name}}.netscience.edges;


-- ============================================================================
-- 2. GRAPH CONFIG — Verify graph definition
-- ============================================================================

SHOW GRAPH;


-- ============================================================================
-- 3. REFERENTIAL INTEGRITY — All edges have valid endpoints
-- ============================================================================

ASSERT VALUE orphan_edges = 0
SELECT COUNT(*) AS orphan_edges
FROM {{zone_name}}.netscience.edges e
WHERE NOT EXISTS (SELECT 1 FROM {{zone_name}}.netscience.vertices v WHERE v.vertex_id = e.src)
   OR NOT EXISTS (SELECT 1 FROM {{zone_name}}.netscience.vertices v WHERE v.vertex_id = e.dst);


-- ============================================================================
-- 4. WEIGHT DISTRIBUTION — Non-uniform coauthorship strengths
-- ============================================================================
-- Unlike Karate Club, this dataset has varying weights.
-- Expected: multiple distinct weight values

ASSERT ROW_COUNT = 1
ASSERT VALUE distinct_weights = 77
-- Non-deterministic: float aggregation — avg may vary by ±0.001 across platforms
ASSERT WARNING VALUE avg_weight BETWEEN 0.433 AND 0.435
SELECT
    COUNT(DISTINCT weight) AS distinct_weights,
    MIN(weight) AS min_weight,
    MAX(weight) AS max_weight,
    ROUND(AVG(weight), 4) AS avg_weight
FROM {{zone_name}}.netscience.edges;


-- ############################################################################
-- PART 2: CYPHER — GRAPH EXPLORATION
-- ############################################################################


-- ============================================================================
-- 5. BROWSE VERTICES — Sample of authors
-- ============================================================================

ASSERT ROW_COUNT = 20
USE {{zone_name}}.netscience.netscience_collab
MATCH (v)
RETURN v.id AS author_id
ORDER BY author_id
LIMIT 20;


-- ============================================================================
-- 6. DEGREE DISTRIBUTION — How many coauthors does each scientist have?
-- ============================================================================
-- Network scientists with the most collaborators appear at the top.

ASSERT ROW_COUNT = 20
USE {{zone_name}}.netscience.netscience_collab
MATCH (a)-[r]->(b)
RETURN a.id AS author_id, COUNT(r) AS degree
ORDER BY degree DESC, author_id ASC
LIMIT 20;


-- ============================================================================
-- 7. TOP HUBS — Most connected scientists
-- ============================================================================
-- The most prolific collaborators in the network science community.

ASSERT ROW_COUNT = 10
USE {{zone_name}}.netscience.netscience_collab
MATCH (a)-[r]->(b)
RETURN a.id AS author_id, COUNT(r) AS degree
ORDER BY degree DESC
LIMIT 10;


-- ============================================================================
-- 8. WEIGHTED DEGREE — Total collaboration strength per author
-- ============================================================================
-- Sum of edge weights reveals authors with the strongest collaborative ties.

ASSERT ROW_COUNT = 10
USE {{zone_name}}.netscience.netscience_collab
MATCH (a)-[r]->(b)
RETURN a.id AS author_id, COUNT(r) AS degree, ROUND(SUM(r.weight), 2) AS total_weight
ORDER BY total_weight DESC
LIMIT 10;


-- ============================================================================
-- 9. TWO-HOP REACHABILITY FROM TOP HUB — Research influence
-- ============================================================================
-- How many authors are within 2 hops of the most connected scientist?
-- Due to multiple components, not all 1,461 authors will be reachable.

ASSERT ROW_COUNT = 1
ASSERT VALUE reachable_in_2_hops = 67
USE {{zone_name}}.netscience.netscience_collab
MATCH (a)-[r]->(b)
WITH a.id AS hub, COUNT(r) AS deg
ORDER BY deg DESC LIMIT 1
WITH hub
MATCH (a)-[*1..2]->(b)
WHERE a.id = hub
RETURN hub, COUNT(DISTINCT b.id) AS reachable_in_2_hops;


-- ############################################################################
-- PART 3: CYPHER — GRAPH ALGORITHMS
-- ############################################################################


-- ============================================================================
-- 10. PAGERANK — Identify most influential scientists
-- ============================================================================
-- Top-ranked authors should be prolific collaborators who bridge
-- multiple research groups.

ASSERT ROW_COUNT = 10
USE {{zone_name}}.netscience.netscience_collab
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC
LIMIT 10;


-- ============================================================================
-- 11. DEGREE CENTRALITY — Normalized degree
-- ============================================================================

ASSERT ROW_COUNT = 10
USE {{zone_name}}.netscience.netscience_collab
CALL algo.degree()
YIELD node_id, in_degree, out_degree, total_degree
RETURN node_id, in_degree, out_degree, total_degree
ORDER BY total_degree DESC
LIMIT 10;


-- ============================================================================
-- 12. BETWEENNESS CENTRALITY — Bridge scientists
-- ============================================================================
-- Authors who connect different research groups will have high betweenness.

ASSERT ROW_COUNT = 10
USE {{zone_name}}.netscience.netscience_collab
CALL algo.betweenness()
YIELD node_id, centrality, rank
RETURN node_id, centrality, rank
ORDER BY centrality DESC
LIMIT 10;


-- ============================================================================
-- 13. CLOSENESS CENTRALITY — Proximity to all other authors
-- ============================================================================

ASSERT ROW_COUNT = 10
USE {{zone_name}}.netscience.netscience_collab
CALL algo.closeness()
YIELD node_id, closeness, rank
RETURN node_id, closeness, rank
ORDER BY closeness DESC
LIMIT 10;


-- ============================================================================
-- 14. COMMUNITY DETECTION — Recover research groups
-- ============================================================================
-- With modularity ~0.95, Louvain should find many well-separated
-- research communities.

ASSERT ROW_COUNT >= 2
USE {{zone_name}}.netscience.netscience_collab
CALL algo.louvain({resolution: 1.0})
YIELD node_id, community_id
RETURN community_id, count(*) AS members
ORDER BY members DESC
LIMIT 20;


-- ============================================================================
-- 15. CONNECTED COMPONENTS — Multiple components expected
-- ============================================================================
-- Unlike Karate Club, this network is NOT fully connected.
-- There are isolated authors and small disconnected groups.

ASSERT ROW_COUNT >= 2
USE {{zone_name}}.netscience.netscience_collab
CALL algo.connectedComponents()
YIELD node_id, component_id
RETURN component_id, count(*) AS members
ORDER BY members DESC
LIMIT 20;


-- ============================================================================
-- 16. SHORTEST PATH — Distance between two prolific authors
-- ============================================================================
-- Find the shortest path between vertices 0 and 1 (if in same component).

ASSERT ROW_COUNT >= 1
USE {{zone_name}}.netscience.netscience_collab
CALL algo.shortestPath({source: 0, target: 1})
YIELD node_id, step, distance
RETURN node_id, step, distance
ORDER BY step;


-- ############################################################################
-- PART 4: VERIFICATION SUMMARY
-- ############################################################################


-- ============================================================================
-- 17. AUTOMATED VERIFICATION — PASS/FAIL against golden values
-- ============================================================================
-- All checks should return PASS. Any FAIL indicates data loading issues
-- or algorithm correctness problems.

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 6
SELECT 'Vertex count = 1461' AS test,
       CASE WHEN cnt = 1461 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END AS result
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.netscience.vertices)

UNION ALL
SELECT 'Edge row count = 5484',
       CASE WHEN cnt = 5484 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.netscience.edges)

UNION ALL
SELECT 'Symmetric edges (undirected)',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (' || CAST(cnt AS VARCHAR) || ' missing reverse edges)' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.netscience.edges e1
    WHERE NOT EXISTS (
        SELECT 1 FROM {{zone_name}}.netscience.edges e2
        WHERE e2.src = e1.dst AND e2.dst = e1.src
    )
)

UNION ALL
SELECT 'All edge endpoints exist',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (' || CAST(cnt AS VARCHAR) || ' orphans)' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.netscience.edges e
    WHERE NOT EXISTS (SELECT 1 FROM {{zone_name}}.netscience.vertices v WHERE v.vertex_id = e.src)
       OR NOT EXISTS (SELECT 1 FROM {{zone_name}}.netscience.vertices v WHERE v.vertex_id = e.dst)
)

UNION ALL
SELECT 'Vertex ID range = 0–1588',
       CASE WHEN min_id = 0 AND max_id = 1588 THEN 'PASS'
            ELSE 'FAIL (range ' || CAST(min_id AS VARCHAR) || '–' || CAST(max_id AS VARCHAR) || ')' END
FROM (
    SELECT MIN(vertex_id) AS min_id, MAX(vertex_id) AS max_id FROM {{zone_name}}.netscience.vertices
)

UNION ALL
SELECT 'Non-uniform weights (> 1 distinct value)',
       CASE WHEN cnt > 1 THEN 'PASS' ELSE 'FAIL (only ' || CAST(cnt AS VARCHAR) || ' distinct weight)' END
FROM (
    SELECT COUNT(DISTINCT weight) AS cnt FROM {{zone_name}}.netscience.edges
);
