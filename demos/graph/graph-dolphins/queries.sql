-- ############################################################################
-- ############################################################################
--
--   DOLPHINS SOCIAL NETWORK — COMMUNITY STRUCTURE IN THE WILD
--   62 Vertices / 159 Undirected Edges (318 rows) / Weight = 1.0
--
-- ############################################################################
-- ############################################################################
--
-- A well-studied animal social network (Lusseau et al., 2003). 62 bottlenose
-- dolphins in Doubtful Sound, New Zealand, with associations recorded over
-- several years. The network naturally splits into 2–4 communities, making it
-- a popular benchmark for community detection algorithms.
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
-- 62 vertices, 318 edge rows (159 undirected edges x 2)

-- Verify vertex count
ASSERT VALUE row_count = 62
SELECT COUNT(*) AS row_count FROM {{zone_name}}.dolphins.vertices;

-- Verify edge count (159 undirected edges x 2)
ASSERT VALUE row_count = 318
SELECT COUNT(*) AS row_count FROM {{zone_name}}.dolphins.edges;


-- ============================================================================
-- 2. GRAPH CONFIG — Verify graph definition
-- ============================================================================

SHOW GRAPH;


-- ============================================================================
-- 3. REFERENTIAL INTEGRITY — All edges have valid endpoints
-- ============================================================================

ASSERT VALUE orphan_edges = 0
SELECT COUNT(*) AS orphan_edges
FROM {{zone_name}}.dolphins.edges e
WHERE NOT EXISTS (SELECT 1 FROM {{zone_name}}.dolphins.vertices v WHERE v.vertex_id = e.src)
   OR NOT EXISTS (SELECT 1 FROM {{zone_name}}.dolphins.vertices v WHERE v.vertex_id = e.dst);


-- ============================================================================
-- 4. SELF-LOOP CHECK — No dolphin should be associated with itself
-- ============================================================================

ASSERT VALUE self_loops = 0
SELECT COUNT(*) AS self_loops
FROM {{zone_name}}.dolphins.edges
WHERE src = dst;


-- ############################################################################
-- PART 2: CYPHER — GRAPH EXPLORATION
-- ############################################################################


-- ============================================================================
-- 5. BROWSE VERTICES — List all 62 dolphins
-- ============================================================================

ASSERT ROW_COUNT = 62
USE {{zone_name}}.dolphins.dolphins_social
MATCH (v)
RETURN v.id AS dolphin_id
ORDER BY dolphin_id;


-- ============================================================================
-- 6. DEGREE DISTRIBUTION — How many associations does each dolphin have?
-- ============================================================================
-- Known: Maximum degree is 12.

-- All 62 dolphins have at least one edge
ASSERT ROW_COUNT = 62
USE {{zone_name}}.dolphins.dolphins_social
MATCH (a)-[r]->(b)
RETURN a.id AS dolphin_id, COUNT(r) AS degree
ORDER BY degree DESC, dolphin_id ASC;


-- ============================================================================
-- 7. TOP HUBS — The most connected dolphins
-- ============================================================================
-- Expected: Top dolphins have degree 12 or higher.

ASSERT ROW_COUNT = 5
ASSERT VALUE degree = 12 WHERE dolphin_id = 14
USE {{zone_name}}.dolphins.dolphins_social
MATCH (a)-[r]->(b)
RETURN a.id AS dolphin_id, COUNT(r) AS degree
ORDER BY degree DESC
LIMIT 5;


-- ============================================================================
-- 8. NEIGHBORHOOD OF TOP HUB — Most connected dolphin's associates
-- ============================================================================
-- The most connected dolphin has up to 12 direct associations.

ASSERT ROW_COUNT = 12
USE {{zone_name}}.dolphins.dolphins_social
MATCH (a)-[r]->(b)
WITH a, COUNT(r) AS degree
ORDER BY degree DESC
LIMIT 1
MATCH (a)-[]->(c)
RETURN a.id AS hub_id, c.id AS associate_id
ORDER BY associate_id;


-- ============================================================================
-- 9. TWO-HOP REACHABILITY FROM NODE 0 — How far does association reach?
-- ============================================================================
-- Most of the 62-node graph should be reachable within 2 hops.

ASSERT ROW_COUNT = 1
ASSERT VALUE reachable_in_2_hops = 27
USE {{zone_name}}.dolphins.dolphins_social
MATCH (a)-[*1..2]->(b)
WHERE a.id = 0
RETURN COUNT(DISTINCT b.id) AS reachable_in_2_hops;


-- ############################################################################
-- PART 3: CYPHER — GRAPH ALGORITHMS
-- ############################################################################


-- ============================================================================
-- 10. PAGERANK — Identify most influential dolphins
-- ============================================================================
-- Expected: The most connected dolphins should have the highest PageRank.

ASSERT ROW_COUNT = 10
USE {{zone_name}}.dolphins.dolphins_social
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC
LIMIT 10;


-- ============================================================================
-- 11. DEGREE CENTRALITY — Normalized degree
-- ============================================================================
-- The most connected dolphins should rank highest.

ASSERT ROW_COUNT = 10
USE {{zone_name}}.dolphins.dolphins_social
CALL algo.degree()
YIELD node_id, in_degree, out_degree, total_degree
RETURN node_id, in_degree, out_degree, total_degree
ORDER BY total_degree DESC
LIMIT 10;


-- ============================================================================
-- 12. BETWEENNESS CENTRALITY — Bridge nodes
-- ============================================================================
-- Dolphins that bridge sub-communities will have the highest betweenness.

ASSERT ROW_COUNT = 10
USE {{zone_name}}.dolphins.dolphins_social
CALL algo.betweenness()
YIELD node_id, centrality, rank
RETURN node_id, centrality, rank
ORDER BY centrality DESC
LIMIT 10;


-- ============================================================================
-- 13. CLOSENESS CENTRALITY — How close is each dolphin to all others?
-- ============================================================================

ASSERT ROW_COUNT = 10
USE {{zone_name}}.dolphins.dolphins_social
CALL algo.closeness()
YIELD node_id, closeness, rank
RETURN node_id, closeness, rank
ORDER BY closeness DESC
LIMIT 10;


-- ============================================================================
-- 14. COMMUNITY DETECTION — Can we recover the natural groups?
-- ============================================================================
-- Published results show 2-4 communities with modularity ~0.49-0.53.

ASSERT ROW_COUNT >= 2
USE {{zone_name}}.dolphins.dolphins_social
CALL algo.louvain({resolution: 1.0})
YIELD node_id, community_id
RETURN community_id, count(*) AS members
ORDER BY members DESC;


-- ============================================================================
-- 15. CONNECTED COMPONENTS — Is the graph fully connected?
-- ============================================================================
-- Expected: 1 connected component (all 62 dolphins reachable from any node).

ASSERT ROW_COUNT = 1
ASSERT VALUE members = 62
USE {{zone_name}}.dolphins.dolphins_social
CALL algo.connectedComponents()
YIELD node_id, component_id
RETURN component_id, count(*) AS members
ORDER BY members DESC;


-- ============================================================================
-- 16. SHORTEST PATH — Distance between two dolphins
-- ============================================================================
-- Dolphin 0 and dolphin 61 should be reachable through the network.

-- Path exists: at least 2 rows (source + target)
ASSERT ROW_COUNT >= 2
USE {{zone_name}}.dolphins.dolphins_social
CALL algo.shortestPath({source: 0, target: 61})
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
ASSERT ROW_COUNT = 8
SELECT 'Vertex count = 62' AS test,
       CASE WHEN cnt = 62 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END AS result
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.dolphins.vertices)

UNION ALL
SELECT 'Edge row count = 318',
       CASE WHEN cnt = 318 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.dolphins.edges)

UNION ALL
SELECT 'No self-loops',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.dolphins.edges WHERE src = dst)

UNION ALL
SELECT 'All edge endpoints exist',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (' || CAST(cnt AS VARCHAR) || ' orphans)' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.dolphins.edges e
    WHERE NOT EXISTS (SELECT 1 FROM {{zone_name}}.dolphins.vertices v WHERE v.vertex_id = e.src)
       OR NOT EXISTS (SELECT 1 FROM {{zone_name}}.dolphins.vertices v WHERE v.vertex_id = e.dst)
)

UNION ALL
SELECT 'Max degree >= 12 (most connected dolphin)',
       CASE WHEN max_deg >= 12 THEN 'PASS' ELSE 'FAIL (got ' || CAST(max_deg AS VARCHAR) || ')' END
FROM (
    SELECT MAX(deg) AS max_deg FROM (
        SELECT src, COUNT(*) AS deg FROM {{zone_name}}.dolphins.edges GROUP BY src
    )
)

UNION ALL
SELECT 'Vertex ID range = 0–61',
       CASE WHEN min_id = 0 AND max_id = 61 THEN 'PASS'
            ELSE 'FAIL (range ' || CAST(min_id AS VARCHAR) || '–' || CAST(max_id AS VARCHAR) || ')' END
FROM (
    SELECT MIN(vertex_id) AS min_id, MAX(vertex_id) AS max_id FROM {{zone_name}}.dolphins.vertices
)

UNION ALL
SELECT 'All weights = 1.0 (unweighted)',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (' || CAST(cnt AS VARCHAR) || ' non-unit weights)' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.dolphins.edges WHERE weight <> 1.0
)

UNION ALL
SELECT 'Symmetric edges (undirected)',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (' || CAST(cnt AS VARCHAR) || ' missing reverse edges)' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.dolphins.edges e1
    WHERE NOT EXISTS (
        SELECT 1 FROM {{zone_name}}.dolphins.edges e2
        WHERE e2.src = e1.dst AND e2.dst = e1.src
    )
);
