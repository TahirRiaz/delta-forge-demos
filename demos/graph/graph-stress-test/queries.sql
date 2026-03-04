-- ############################################################################
-- ############################################################################
--
--   GRAPH STRESS TEST — 1M NODES / 5M+ EDGES
--   Performance Benchmark Queries
--
-- ############################################################################
-- ############################################################################
--
-- PART 1: RAW QUERY PERFORMANCE (queries 1–38)
--   Aggregation, analytics, and Cypher algorithm benchmarks.
--   These return summary/tabular data — no graph visualization rendering.
--
-- PART 2: GRAPH VISUALIZATION STRESS TEST (queries 39–50)
--   These return actual node + edge data designed to be rendered in the
--   graph visualizer. Progressive scale from 100 nodes up to full 1M.
--   Use these to test if the visualizer crashes, lags, or handles large
--   graphs gracefully.
--
-- ############################################################################


-- ############################################################################
-- PART 1: RAW QUERY PERFORMANCE
-- ############################################################################
-- SQL and Cypher queries that return aggregated/tabular results.
-- Tests query engine performance without stressing the graph renderer.
-- ############################################################################


-- ============================================================================
-- 1. NODE COUNT — Verify 1,000,000 people generated
-- ============================================================================

SELECT 'node_count' AS check_name,
       COUNT(*) AS actual,
       1000000 AS expected,
       CASE WHEN COUNT(*) = 1000000 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.graph.st_people;


-- ============================================================================
-- 2. EDGE COUNT — Verify 5,000,000+ edges generated
-- ============================================================================

SELECT 'edge_count' AS check_name,
       COUNT(*) AS actual,
       CASE WHEN COUNT(*) >= 4000000 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.graph.st_edges;


-- ============================================================================
-- 3. DEPARTMENT DISTRIBUTION — People per department (20 depts)
-- ============================================================================

SELECT
    department,
    COUNT(*) AS people_count,
    ROUND(AVG(age), 1) AS avg_age,
    COUNT(*) FILTER (WHERE active) AS active_count,
    COUNT(DISTINCT city) AS cities_present
FROM {{zone_name}}.graph.st_people
GROUP BY department
ORDER BY people_count DESC;


-- ============================================================================
-- 4. RELATIONSHIP TYPE BREAKDOWN — Edge categories across 5M+ edges
-- ============================================================================

SELECT
    relationship_type,
    COUNT(*) AS edge_count,
    ROUND(AVG(weight), 3) AS avg_weight,
    ROUND(MIN(weight), 3) AS min_weight,
    ROUND(MAX(weight), 3) AS max_weight,
    MIN(since_year) AS earliest,
    MAX(since_year) AS latest
FROM {{zone_name}}.graph.st_edges
GROUP BY relationship_type
ORDER BY edge_count DESC;


-- ============================================================================
-- 5. CITY DISTRIBUTION — People across 15 cities
-- ============================================================================

SELECT
    city,
    COUNT(*) AS people_count,
    COUNT(DISTINCT department) AS departments_present,
    COUNT(DISTINCT project_team) AS teams_present,
    ROUND(AVG(age), 1) AS avg_age
FROM {{zone_name}}.graph.st_people
GROUP BY city
ORDER BY people_count DESC;


-- ============================================================================
-- 6. TOP 25 MOST CONNECTED — Heavy degree computation on 5M+ edges
-- ============================================================================

SELECT
    id,
    name,
    department,
    city,
    level,
    out_degree,
    in_degree,
    total_degree
FROM {{zone_name}}.graph.st_people_stats
ORDER BY total_degree DESC
LIMIT 25;


-- ============================================================================
-- 7. DEGREE DISTRIBUTION HISTOGRAM — How connected is the average node?
-- ============================================================================

SELECT
    CASE
        WHEN total_degree = 0  THEN '0 (isolated)'
        WHEN total_degree <= 2 THEN '1-2'
        WHEN total_degree <= 5 THEN '3-5'
        WHEN total_degree <= 10 THEN '6-10'
        WHEN total_degree <= 20 THEN '11-20'
        WHEN total_degree <= 50 THEN '21-50'
        ELSE '50+'
    END AS degree_bucket,
    COUNT(*) AS node_count
FROM {{zone_name}}.graph.st_people_stats
GROUP BY
    CASE
        WHEN total_degree = 0  THEN '0 (isolated)'
        WHEN total_degree <= 2 THEN '1-2'
        WHEN total_degree <= 5 THEN '3-5'
        WHEN total_degree <= 10 THEN '6-10'
        WHEN total_degree <= 20 THEN '11-20'
        WHEN total_degree <= 50 THEN '21-50'
        ELSE '50+'
    END
ORDER BY
    CASE
        WHEN degree_bucket = '0 (isolated)' THEN 0
        WHEN degree_bucket = '1-2' THEN 1
        WHEN degree_bucket = '3-5' THEN 2
        WHEN degree_bucket = '6-10' THEN 3
        WHEN degree_bucket = '11-20' THEN 4
        WHEN degree_bucket = '21-50' THEN 5
        ELSE 6
    END;


-- ============================================================================
-- 8. DEPARTMENT CROSS-POLLINATION — 20x20 department matrix (top pairs)
-- ============================================================================

SELECT
    src_dept,
    dst_dept,
    connection_count,
    avg_weight,
    rel_type_count
FROM {{zone_name}}.graph.st_dept_matrix
WHERE src_dept != dst_dept
ORDER BY connection_count DESC
LIMIT 30;


-- ============================================================================
-- 9. INTRA-DEPARTMENT DENSITY — Internal cohesion per department
-- ============================================================================

SELECT
    src_dept AS department,
    connection_count AS intra_connections,
    avg_weight,
    rel_type_count
FROM {{zone_name}}.graph.st_dept_matrix
WHERE src_dept = dst_dept
ORDER BY connection_count DESC;


-- ============================================================================
-- 10. CITY NETWORK — Cross-city connection counts (15x15 matrix, top pairs)
-- ============================================================================

SELECT
    src_p.city AS from_city,
    dst_p.city AS to_city,
    COUNT(*) AS connections,
    ROUND(AVG(e.weight), 3) AS avg_weight,
    COUNT(DISTINCT e.relationship_type) AS rel_types
FROM {{zone_name}}.graph.st_edges e
JOIN {{zone_name}}.graph.st_people src_p ON e.src = src_p.id
JOIN {{zone_name}}.graph.st_people dst_p ON e.dst = dst_p.id
GROUP BY src_p.city, dst_p.city
ORDER BY connections DESC
LIMIT 25;


-- ============================================================================
-- 11. BRIDGE NODES — People connecting 10+ departments
-- ============================================================================

SELECT
    p.name,
    p.department AS own_dept,
    p.city,
    p.level,
    COUNT(DISTINCT dst_p.department) AS depts_reached,
    COUNT(*) AS total_outgoing
FROM {{zone_name}}.graph.st_edges e
JOIN {{zone_name}}.graph.st_people p ON e.src = p.id
JOIN {{zone_name}}.graph.st_people dst_p ON e.dst = dst_p.id
WHERE p.department != dst_p.department
GROUP BY p.name, p.department, p.city, p.level
HAVING COUNT(DISTINCT dst_p.department) >= 10
ORDER BY depts_reached DESC, total_outgoing DESC
LIMIT 25;


-- ============================================================================
-- 12. 2-HOP NEIGHBORHOOD SIZE — How many people can node #1 reach in 2 hops?
-- ============================================================================

SELECT
    '1-hop' AS reach,
    COUNT(DISTINCT dst) AS reachable_count
FROM {{zone_name}}.graph.st_edges
WHERE src = 1
UNION ALL
SELECT
    '2-hop' AS reach,
    COUNT(DISTINCT e2.dst) AS reachable_count
FROM {{zone_name}}.graph.st_edges e1
JOIN {{zone_name}}.graph.st_edges e2 ON e1.dst = e2.src
WHERE e1.src = 1
  AND e2.dst != 1;


-- ============================================================================
-- 13. MENTOR NETWORK DEPTH — Mentor chains (who mentors whom)
-- ============================================================================

SELECT
    src_p.level AS mentor_level,
    dst_p.level AS mentee_level,
    COUNT(*) AS mentorship_count,
    ROUND(AVG(e.weight), 3) AS avg_weight
FROM {{zone_name}}.graph.st_edges e
JOIN {{zone_name}}.graph.st_people src_p ON e.src = src_p.id
JOIN {{zone_name}}.graph.st_people dst_p ON e.dst = dst_p.id
WHERE e.relationship_type = 'mentor'
GROUP BY src_p.level, dst_p.level
ORDER BY mentorship_count DESC;


-- ============================================================================
-- 14. PROJECT TEAM COHESION — Connections within vs across teams
-- ============================================================================

SELECT
    'within_team' AS connection_scope,
    COUNT(*) AS edge_count,
    ROUND(AVG(e.weight), 3) AS avg_weight
FROM {{zone_name}}.graph.st_edges e
JOIN {{zone_name}}.graph.st_people src_p ON e.src = src_p.id
JOIN {{zone_name}}.graph.st_people dst_p ON e.dst = dst_p.id
WHERE src_p.project_team = dst_p.project_team
UNION ALL
SELECT
    'across_teams' AS connection_scope,
    COUNT(*) AS edge_count,
    ROUND(AVG(e.weight), 3) AS avg_weight
FROM {{zone_name}}.graph.st_edges e
JOIN {{zone_name}}.graph.st_people src_p ON e.src = src_p.id
JOIN {{zone_name}}.graph.st_people dst_p ON e.dst = dst_p.id
WHERE src_p.project_team != dst_p.project_team;


-- ============================================================================
-- 15. RECIPROCAL CONNECTIONS — Bidirectional edges at scale
-- ============================================================================

SELECT
    COUNT(*) AS reciprocal_pairs
FROM {{zone_name}}.graph.st_edges e1
JOIN {{zone_name}}.graph.st_edges e2
    ON e1.src = e2.dst AND e1.dst = e2.src
WHERE e1.src < e1.dst;


-- ============================================================================
-- 16. YEARLY GROWTH — Edge creation timeline
-- ============================================================================

SELECT
    since_year,
    COUNT(*) AS edges_created,
    COUNT(DISTINCT src) AS unique_sources,
    COUNT(DISTINCT dst) AS unique_targets,
    ROUND(AVG(weight), 3) AS avg_weight
FROM {{zone_name}}.graph.st_edges
GROUP BY since_year
ORDER BY since_year;


-- ============================================================================
-- 17. LEVEL-TO-LEVEL FLOW — Connection patterns across seniority levels
-- ============================================================================

SELECT
    src_p.level AS from_level,
    dst_p.level AS to_level,
    COUNT(*) AS connection_count,
    ROUND(AVG(e.weight), 3) AS avg_weight
FROM {{zone_name}}.graph.st_edges e
JOIN {{zone_name}}.graph.st_people src_p ON e.src = src_p.id
JOIN {{zone_name}}.graph.st_people dst_p ON e.dst = dst_p.id
GROUP BY src_p.level, dst_p.level
ORDER BY connection_count DESC
LIMIT 20;


-- ============================================================================
-- 18. REGION-TO-REGION FLOW — Connections between company regions
-- ============================================================================

SELECT
    sd.region AS from_region,
    dd.region AS to_region,
    COUNT(*) AS connections,
    ROUND(AVG(e.weight), 3) AS avg_weight
FROM {{zone_name}}.graph.st_edges e
JOIN {{zone_name}}.graph.st_people src_p ON e.src = src_p.id
JOIN {{zone_name}}.graph.st_people dst_p ON e.dst = dst_p.id
JOIN {{zone_name}}.graph.st_departments sd ON src_p.department = sd.dept_name
JOIN {{zone_name}}.graph.st_departments dd ON dst_p.department = dd.dept_name
GROUP BY sd.region, dd.region
ORDER BY connections DESC;


-- ============================================================================
-- 19. PAGERANK APPROXIMATION — 1-iteration simplified PageRank (SQL)
-- ============================================================================

SELECT
    p.id,
    p.name,
    p.department,
    p.city,
    ROUND(SUM(1.0 / GREATEST(src_stats.out_degree, 1)), 4) AS approx_pagerank
FROM {{zone_name}}.graph.st_edges e
JOIN {{zone_name}}.graph.st_people p ON e.dst = p.id
JOIN {{zone_name}}.graph.st_people_stats src_stats ON e.src = src_stats.id
GROUP BY p.id, p.name, p.department, p.city
ORDER BY approx_pagerank DESC
LIMIT 25;


-- ============================================================================
-- 20. GRAPH STATISTICS SUMMARY — Full dataset metrics
-- ============================================================================

SELECT
    (SELECT COUNT(*)                    FROM {{zone_name}}.graph.st_people) AS total_nodes,
    (SELECT COUNT(*)                    FROM {{zone_name}}.graph.st_edges)  AS total_edges,
    (SELECT COUNT(DISTINCT department)  FROM {{zone_name}}.graph.st_people) AS dept_count,
    (SELECT COUNT(DISTINCT city)        FROM {{zone_name}}.graph.st_people) AS city_count,
    (SELECT COUNT(DISTINCT project_team) FROM {{zone_name}}.graph.st_people) AS team_count,
    (SELECT COUNT(DISTINCT relationship_type) FROM {{zone_name}}.graph.st_edges) AS rel_type_count,
    (SELECT ROUND(AVG(weight), 3)       FROM {{zone_name}}.graph.st_edges)  AS avg_weight,
    (SELECT ROUND(
        CAST((SELECT COUNT(*) FROM {{zone_name}}.graph.st_edges) AS DOUBLE)
        / CAST((SELECT COUNT(*) FROM {{zone_name}}.graph.st_people) AS DOUBLE)
    , 1)) AS avg_edges_per_node;


-- ============================================================================
-- 21. FULL VERIFICATION — All PASS/FAIL checks
-- ============================================================================

SELECT 'node_count_1M' AS check_name,
       CASE WHEN COUNT(*) = 1000000 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.graph.st_people
UNION ALL
SELECT 'department_count_20',
       CASE WHEN COUNT(*) = 20 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.st_departments
UNION ALL
SELECT 'edge_count_min_4M',
       CASE WHEN COUNT(*) >= 4000000 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.st_edges
UNION ALL
SELECT 'has_mentor_edges',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.st_edges WHERE relationship_type = 'mentor'
UNION ALL
SELECT 'has_colleague_edges',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.st_edges WHERE relationship_type = 'colleague'
UNION ALL
SELECT 'fifteen_cities',
       CASE WHEN COUNT(DISTINCT city) = 15 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.st_people
UNION ALL
SELECT 'twenty_departments',
       CASE WHEN COUNT(DISTINCT department) = 20 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.st_people
UNION ALL
SELECT '200_project_teams',
       CASE WHEN COUNT(DISTINCT project_team) = 200 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.st_people
UNION ALL
SELECT 'active_people_gt_900K',
       CASE WHEN COUNT(*) FILTER (WHERE active) >= 900000 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.st_people
UNION ALL
SELECT 'weight_range_valid',
       CASE WHEN MIN(weight) >= 0.0 AND MAX(weight) <= 1.0 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.st_edges
UNION ALL
SELECT 'no_self_loops',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.st_edges WHERE src = dst
ORDER BY check_name;


-- ============================================================================
-- CYPHER: 22. ALL NODES COUNT — 1M node scan
-- ============================================================================

USE {{zone_name}}.graph.st_edges
MATCH (n)
RETURN count(n) AS node_count;


-- ============================================================================
-- CYPHER: 23. ALL EDGES — Full edge scan at 5M+ scale
-- ============================================================================

USE {{zone_name}}.graph.st_edges
MATCH (a)-[r]->(b)
RETURN count(r) AS edge_count;


-- ============================================================================
-- CYPHER: 24. FILTERED NODES — Property filter on 1M nodes
-- ============================================================================

USE {{zone_name}}.graph.st_edges
MATCH (n)
WHERE n.department = 'Engineering' AND n.age > 50
RETURN n.name AS name, n.age AS age, n.city AS city
ORDER BY n.age DESC
LIMIT 25;


-- ============================================================================
-- CYPHER: 25. DIRECTED RELATIONSHIPS — Pattern match with properties
-- ============================================================================

USE {{zone_name}}.graph.st_edges
MATCH (a)-[r]->(b)
WHERE r.relationship_type = 'mentor' AND r.weight > 0.8
RETURN a.name AS mentor, b.name AS mentee, r.weight AS strength
ORDER BY r.weight DESC
LIMIT 25;


-- ============================================================================
-- CYPHER: 26. 2-HOP PATHS — Multi-hop traversal at scale
-- ============================================================================

USE {{zone_name}}.graph.st_edges
MATCH (a)-[]->(b)-[]->(c)
WHERE a.id = 1
RETURN a.name AS start, b.name AS hop1, c.name AS hop2
LIMIT 50;


-- ============================================================================
-- CYPHER: 27. VARIABLE-LENGTH PATHS — Reachability within 2 hops
-- ============================================================================

USE {{zone_name}}.graph.st_edges
MATCH (a)-[*1..2]->(b)
WHERE a.id = 1 AND a <> b
RETURN DISTINCT b.name AS reachable, b.department AS dept
LIMIT 50;


-- ============================================================================
-- CYPHER: 28. DEGREE CENTRALITY — On 1M nodes / 5M edges
-- ============================================================================

USE {{zone_name}}.graph.st_edges
CALL algo.degree()
YIELD nodeId, inDegree, outDegree, totalDegree
RETURN nodeId, inDegree, outDegree, totalDegree
ORDER BY totalDegree DESC
LIMIT 25;


-- ============================================================================
-- CYPHER: 29. PAGERANK — Link analysis on 5M+ edges (5 iterations)
-- ============================================================================

USE {{zone_name}}.graph.st_edges
CALL algo.pageRank({dampingFactor: 0.85, iterations: 5})
YIELD nodeId, score, rank
RETURN nodeId, score, rank
ORDER BY score DESC
LIMIT 25;


-- ============================================================================
-- CYPHER: 30. CONNECTED COMPONENTS — Community detection at scale
-- ============================================================================

USE {{zone_name}}.graph.st_edges
CALL algo.connectedComponents()
YIELD nodeId, componentId
RETURN componentId, count(*) AS component_size
ORDER BY component_size DESC
LIMIT 25;


-- ============================================================================
-- CYPHER: 31. LOUVAIN COMMUNITY DETECTION — Modularity clustering
-- ============================================================================

USE {{zone_name}}.graph.st_edges
CALL algo.louvain({resolution: 1.0})
YIELD nodeId, communityId
RETURN communityId, count(*) AS size
ORDER BY size DESC
LIMIT 25;


-- ============================================================================
-- CYPHER: 32. BETWEENNESS CENTRALITY — Bridge detection at scale
-- ============================================================================

USE {{zone_name}}.graph.st_edges
CALL algo.betweenness()
YIELD nodeId, centrality, rank
RETURN nodeId, centrality, rank
ORDER BY centrality DESC
LIMIT 25;


-- ============================================================================
-- CYPHER: 33. TRIANGLE COUNT — Clustering coefficient at scale
-- ============================================================================

USE {{zone_name}}.graph.st_edges
CALL algo.triangleCount()
YIELD nodeId, triangleCount
RETURN nodeId, triangleCount
ORDER BY triangleCount DESC
LIMIT 25;


-- ============================================================================
-- CYPHER: 34. SHORTEST PATH — Dijkstra across 1M nodes
-- ============================================================================

USE {{zone_name}}.graph.st_edges
CALL algo.shortestPath({source: 1, target: 500000})
YIELD nodeId, step, distance
RETURN nodeId, step, distance
ORDER BY step;


-- ============================================================================
-- CYPHER: 35. BFS TRAVERSAL — Breadth-first from node 1
-- ============================================================================

USE {{zone_name}}.graph.st_edges
CALL algo.bfs({source: 1})
YIELD nodeId, depth, parentId
RETURN depth, count(*) AS nodes_at_depth
ORDER BY depth
LIMIT 20;


-- ============================================================================
-- CYPHER: 36. STRONGLY CONNECTED COMPONENTS — Directed reachability
-- ============================================================================

USE {{zone_name}}.graph.st_edges
CALL algo.scc()
YIELD nodeId, componentId
RETURN componentId, count(*) AS scc_size
ORDER BY scc_size DESC
LIMIT 25;


-- ============================================================================
-- CYPHER: 37. CLOSENESS CENTRALITY — Central nodes at scale
-- ============================================================================

USE {{zone_name}}.graph.st_edges
CALL algo.closeness()
YIELD nodeId, closeness, rank
RETURN nodeId, closeness, rank
ORDER BY closeness DESC
LIMIT 25;


-- ============================================================================
-- CYPHER: 38. MINIMUM SPANNING TREE — Lightest edges connecting all nodes
-- ============================================================================

USE {{zone_name}}.graph.st_edges
CALL algo.mst()
YIELD sourceId, targetId, weight
RETURN count(*) AS mst_edge_count, sum(weight) AS total_weight;


-- ############################################################################
-- ############################################################################
--
-- PART 2: GRAPH VISUALIZATION STRESS TEST
--
-- ############################################################################
-- ############################################################################
-- These queries return actual node + edge rows designed to be rendered in
-- the graph visualizer. Run them progressively to find the breaking point.
--
-- Scale ladder:
--   39.  100 nodes  /  ~500 edges     (warm-up)
--   40.  500 nodes  /  ~2,500 edges   (small graph)
--   41.  1,000 nodes / ~5,000 edges   (medium graph)
--   42.  5,000 nodes / ~25,000 edges  (large graph)
--   43.  10,000 nodes / ~50,000 edges (very large)
--   44.  50,000 nodes / ~250,000 edges (extreme)
--   45.  100,000 nodes / ~500K edges  (will it survive?)
--   46.  ALL 1M nodes / ALL 5M+ edges (full blast — expect crash)
--
-- Cypher visualization tests:
--   47.  Cypher: 100 nodes with edges (warm-up)
--   48.  Cypher: 1,000 nodes with edges (medium)
--   49.  Cypher: 10,000 nodes with edges (large)
--   50.  Cypher: ALL nodes + edges (full blast)
-- ############################################################################


-- ============================================================================
-- 39. VIZ: 100 NODES + EDGES — Warm-up (should render fine)
-- ============================================================================
-- Returns first 100 people and all edges between them.

USE {{zone_name}}.graph.st_edges
MATCH (a)-[r]->(b)
WHERE a.id <= 100 AND b.id <= 100
RETURN a, r, b;


-- ============================================================================
-- 40. VIZ: 500 NODES + EDGES — Small graph
-- ============================================================================

USE {{zone_name}}.graph.st_edges
MATCH (a)-[r]->(b)
WHERE a.id <= 500 AND b.id <= 500
RETURN a, r, b;


-- ============================================================================
-- 41. VIZ: 1,000 NODES + EDGES — Medium graph
-- ============================================================================

USE {{zone_name}}.graph.st_edges
MATCH (a)-[r]->(b)
WHERE a.id <= 1000 AND b.id <= 1000
RETURN a, r, b;


-- ============================================================================
-- 42. VIZ: 5,000 NODES + EDGES — Large graph
-- ============================================================================

USE {{zone_name}}.graph.st_edges
MATCH (a)-[r]->(b)
WHERE a.id <= 5000 AND b.id <= 5000
RETURN a, r, b;


-- ============================================================================
-- 43. VIZ: 10,000 NODES + EDGES — Very large graph
-- ============================================================================

USE {{zone_name}}.graph.st_edges
MATCH (a)-[r]->(b)
WHERE a.id <= 10000 AND b.id <= 10000
RETURN a, r, b;


-- ============================================================================
-- 44. VIZ: 50,000 NODES + EDGES — Extreme rendering test
-- ============================================================================
-- WARNING: This will produce a very large result set. The graph visualizer
-- may become unresponsive or crash. This is the intended test.

USE {{zone_name}}.graph.st_edges
MATCH (a)-[r]->(b)
WHERE a.id <= 50000 AND b.id <= 50000
RETURN a, r, b;


-- ============================================================================
-- 45. VIZ: 100,000 NODES + EDGES — Will it survive?
-- ============================================================================
-- WARNING: Expect significant lag or crash at this scale.

USE {{zone_name}}.graph.st_edges
MATCH (a)-[r]->(b)
WHERE a.id <= 100000 AND b.id <= 100000
RETURN a, r, b;


-- ============================================================================
-- 46. VIZ: FULL 1M NODES / 5M+ EDGES — Full blast
-- ============================================================================
-- WARNING: This returns ALL 1,000,000 nodes and ALL 5,000,000+ edges.
-- The graph visualizer will almost certainly crash or freeze.
-- This is the ultimate stress test.

USE {{zone_name}}.graph.st_edges
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- 47. VIZ CYPHER: 100 NODES — Warm-up with node objects
-- ============================================================================

USE {{zone_name}}.graph.st_edges
MATCH (n)
WHERE n.id <= 100
RETURN n;


-- ============================================================================
-- 48. VIZ CYPHER: 1,000 NODES — Medium node rendering
-- ============================================================================

USE {{zone_name}}.graph.st_edges
MATCH (n)
WHERE n.id <= 1000
RETURN n;


-- ============================================================================
-- 49. VIZ CYPHER: 10,000 NODES — Large node rendering
-- ============================================================================

USE {{zone_name}}.graph.st_edges
MATCH (n)
WHERE n.id <= 10000
RETURN n;


-- ============================================================================
-- 50. VIZ CYPHER: ALL 1M NODES — Full node dump
-- ============================================================================
-- WARNING: Returns 1,000,000 node objects. Ultimate renderer stress test.

USE {{zone_name}}.graph.st_edges
MATCH (n)
RETURN n;
