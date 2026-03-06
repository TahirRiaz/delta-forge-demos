-- ############################################################################
-- ############################################################################
--
--   ENTERPRISE ORGANIZATION NETWORK — 1M EMPLOYEES / 5M+ CONNECTIONS
--   Organizational Network Analytics via Cypher
--
-- ############################################################################
-- ############################################################################
--
-- This demo simulates a realistic enterprise organization with 1 million
-- employees across 20 departments, 15 cities, and 200 project teams.
-- The graph has genuine community structure: departments form tight
-- clusters, project teams are nested sub-communities, and a small
-- percentage of bridge/liaison employees connect the clusters.
--
-- PART 1: EXPLORE & ANALYZE (queries 1–16)
--   Pattern matching, property filtering, relationship analysis.
--
-- PART 2: GRAPH ALGORITHMS (queries 17–30)
--   Influence mapping, community detection, and path analysis.
--
-- PART 3: GRAPH VISUALIZATION (queries 31–42)
--   Progressive scale tests for the graph visualizer.
--
-- ############################################################################


-- ############################################################################
-- PART 1: EXPLORE & ANALYZE
-- ############################################################################


-- ============================================================================
-- 1. ORGANIZATION SIZE — Verify all 1M employees loaded
-- ============================================================================

USE stress_test_network
MATCH (n)
RETURN count(n) AS total_employees;


-- ============================================================================
-- 2. TOTAL CONNECTIONS — Full edge scan at 5M+ scale
-- ============================================================================

USE stress_test_network
MATCH (a)-[r]->(b)
RETURN count(r) AS total_connections;


-- ============================================================================
-- 3. WORKFORCE BY DEPARTMENT — Headcount distribution
-- ============================================================================
-- HR needs a snapshot of each department. Uneven headcounts across 20
-- departments may indicate growth imbalances.

USE stress_test_network
MATCH (n)
RETURN n.department AS department, count(n) AS headcount,
       avg(n.age) AS avg_age
ORDER BY headcount DESC;


-- ============================================================================
-- 4. GLOBAL FOOTPRINT — Employee distribution across 15 offices
-- ============================================================================

USE stress_test_network
MATCH (n)
RETURN n.city AS city, count(n) AS headcount
ORDER BY headcount DESC;


-- ============================================================================
-- 5. RELATIONSHIP MIX — What types of bonds exist?
-- ============================================================================
-- Understanding the connection type mix across 5M+ edges reveals
-- organizational patterns at enterprise scale.

USE stress_test_network
MATCH (a)-[r]->(b)
RETURN r.relationship_type AS type, count(r) AS count,
       avg(r.weight) AS avg_strength
ORDER BY count DESC;


-- ============================================================================
-- 6. ENGINEERING VETERANS — Senior engineers over 50
-- ============================================================================
-- HR is planning a mentorship program. Find experienced engineers who
-- could mentor the next generation.

USE stress_test_network
MATCH (n)
WHERE n.department = 'Engineering' AND n.age > 50
RETURN n.name AS name, n.age AS age, n.city AS city
ORDER BY n.age DESC
LIMIT 25;


-- ============================================================================
-- 7. STRONGEST MENTORSHIPS — High-impact mentor bonds
-- ============================================================================
-- Which mentor-mentee pairs have bonds > 0.8? These are the mentorships
-- worth studying and replicating across the enterprise.

USE stress_test_network
MATCH (mentor)-[r]->(mentee)
WHERE r.relationship_type = 'mentor' AND r.weight > 0.8
RETURN mentor.name AS mentor, mentee.name AS mentee, r.weight AS strength
ORDER BY r.weight DESC
LIMIT 25;


-- ============================================================================
-- 8. CROSS-DEPARTMENT BRIDGES — Who connects the silos?
-- ============================================================================

USE stress_test_network
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a.department AS from_dept, b.department AS to_dept,
       count(r) AS connections, avg(r.weight) AS avg_strength
ORDER BY connections DESC
LIMIT 30;


-- ============================================================================
-- 9. OFFICE COLLABORATION — Which city pairs work together?
-- ============================================================================
-- For remote work policy decisions: which office pairs collaborate most?

USE stress_test_network
MATCH (a)-[r]->(b)
RETURN a.city AS from_city, b.city AS to_city,
       count(r) AS connections, avg(r.weight) AS avg_strength
ORDER BY connections DESC
LIMIT 25;


-- ============================================================================
-- 10. KNOWLEDGE PATHS — 2-hop information flow from employee #1
-- ============================================================================

USE stress_test_network
MATCH (a)-[]->(b)-[]->(c)
WHERE a.id = 1
RETURN a.name AS source, b.name AS relay, c.name AS reached
LIMIT 50;


-- ============================================================================
-- 11. REACHABILITY — Who can employee #1 reach within 2 hops?
-- ============================================================================

USE stress_test_network
MATCH (a)-[*1..2]->(b)
WHERE a.id = 1 AND a <> b
RETURN DISTINCT b.name AS reachable, b.department AS dept
LIMIT 50;


-- ============================================================================
-- 12. RECIPROCAL RELATIONSHIPS — Mutual bonds at scale
-- ============================================================================

USE stress_test_network
MATCH (a)-[r1]->(b)-[r2]->(a)
WHERE a.id < b.id
RETURN a.name AS person_a, b.name AS person_b,
       r1.relationship_type AS a_to_b, r2.relationship_type AS b_to_a
LIMIT 25;


-- ============================================================================
-- 13. MENTORSHIP LEVEL FLOW — Are seniors mentoring juniors?
-- ============================================================================
-- A healthy program has senior staff (L5+) mentoring people 1-2 levels
-- below. If VPs only mentor other VPs, the program isn't reaching juniors.

USE stress_test_network
MATCH (mentor)-[r]->(mentee)
WHERE r.relationship_type = 'mentor'
RETURN mentor.level AS mentor_level, mentee.level AS mentee_level,
       count(r) AS mentorship_count, avg(r.weight) AS avg_strength
ORDER BY mentorship_count DESC;


-- ============================================================================
-- 14. SENIORITY FLOW — How does information flow across levels?
-- ============================================================================
-- Do senior people mostly connect to other seniors (echo chamber), or
-- do connections span levels?

USE stress_test_network
MATCH (a)-[r]->(b)
RETURN a.level AS from_level, b.level AS to_level,
       count(r) AS connections, avg(r.weight) AS avg_strength
ORDER BY connections DESC
LIMIT 20;


-- ============================================================================
-- 15. TEAM vs CROSS-TEAM — Is the org collaborating or siloed?
-- ============================================================================

USE stress_test_network
MATCH (a)-[r]->(b)
WHERE a.department = b.department
RETURN 'within_department' AS scope, count(r) AS connections,
       avg(r.weight) AS avg_strength;


-- ============================================================================
-- 16. CROSS-DEPARTMENT VOLUME — Complement to within-department
-- ============================================================================

USE stress_test_network
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN 'cross_department' AS scope, count(r) AS connections,
       avg(r.weight) AS avg_strength;


-- ############################################################################
-- ############################################################################
--
-- PART 2: GRAPH ALGORITHMS — Influence, Communities & Paths
--
-- ############################################################################
-- ############################################################################


-- ============================================================================
-- 17. DEGREE CENTRALITY — Most connected people at 1M scale
-- ============================================================================

USE stress_test_network
CALL algo.degree()
YIELD nodeId, inDegree, outDegree, totalDegree
RETURN nodeId, inDegree, outDegree, totalDegree
ORDER BY totalDegree DESC
LIMIT 25;


-- ============================================================================
-- 18. PAGERANK — True organizational influence at enterprise scale
-- ============================================================================
-- PageRank at 1M nodes and 5M edges: finds the people who are
-- connected to by other well-connected people. The real power structure.

USE stress_test_network
CALL algo.pageRank({dampingFactor: 0.85, iterations: 5})
YIELD nodeId, score, rank
RETURN nodeId, score, rank
ORDER BY score DESC
LIMIT 25;


-- ============================================================================
-- 19. NATURAL COMMUNITIES — Connected components at scale
-- ============================================================================
-- In a healthy org, there should be one giant component. Multiple
-- components indicate truly disconnected groups.

USE stress_test_network
CALL algo.connectedComponents()
YIELD nodeId, componentId
RETURN componentId, count(*) AS community_size
ORDER BY community_size DESC
LIMIT 25;


-- ============================================================================
-- 20. LOUVAIN COMMUNITIES — Real organizational clusters
-- ============================================================================
-- Finds dense subgroups regardless of the formal org chart. Do the
-- detected communities align with the 20 departments?

USE stress_test_network
CALL algo.louvain({resolution: 1.0})
YIELD nodeId, communityId
RETURN communityId, count(*) AS size
ORDER BY size DESC
LIMIT 25;


-- ============================================================================
-- 21. GATEKEEPERS — Who controls information flow at scale?
-- ============================================================================
-- Betweenness centrality at 1M nodes: finds people on many shortest paths.
-- If they leave, communication between groups breaks down. Critical
-- for succession planning and retention strategy.

USE stress_test_network
CALL algo.betweenness()
YIELD nodeId, centrality, rank
RETURN nodeId, centrality, rank
ORDER BY centrality DESC
LIMIT 25;


-- ============================================================================
-- 22. TIGHT-KNIT GROUPS — Triangle count at scale
-- ============================================================================

USE stress_test_network
CALL algo.triangleCount()
YIELD nodeId, triangleCount
RETURN nodeId, triangleCount
ORDER BY triangleCount DESC
LIMIT 25;


-- ============================================================================
-- 23. SHORTEST PATH — Route across the enterprise
-- ============================================================================
-- If employee #1 needs to reach employee #500000 (different department,
-- different city), what's the fastest chain? Tests Dijkstra at 1M scale.

USE stress_test_network
CALL algo.shortestPath({source: 1, target: 500000})
YIELD nodeId, step, distance
RETURN nodeId, step, distance
ORDER BY step;


-- ============================================================================
-- 24. SIX DEGREES — Small world property at 1M scale
-- ============================================================================
-- Most people should be reachable within 4-6 hops even in an enterprise
-- of 1 million. More suggests organizational fragmentation.

USE stress_test_network
CALL algo.bfs({source: 1})
YIELD nodeId, depth, parentId
RETURN depth, count(*) AS people_at_distance
ORDER BY depth
LIMIT 20;


-- ============================================================================
-- 25. DIRECTED REACHABILITY — Strongly connected components
-- ============================================================================
-- A large SCC means good bidirectional communication. Many small SCCs
-- indicate one-way information flow (top-down only).

USE stress_test_network
CALL algo.scc()
YIELD nodeId, componentId
RETURN componentId, count(*) AS scc_size
ORDER BY scc_size DESC
LIMIT 25;


-- ============================================================================
-- 26. ACCESSIBILITY — Who can reach everyone fastest?
-- ============================================================================
-- High closeness = good candidate for company-wide announcements or
-- change agent roles.

USE stress_test_network
CALL algo.closeness()
YIELD nodeId, closeness, rank
RETURN nodeId, closeness, rank
ORDER BY closeness DESC
LIMIT 25;


-- ============================================================================
-- 27. BACKBONE NETWORK — Essential connections
-- ============================================================================
-- The minimum spanning tree at 1M scale: the lightest set of edges
-- that still connects every employee. Reveals the organizational skeleton.

USE stress_test_network
CALL algo.mst()
YIELD sourceId, targetId, weight
RETURN count(*) AS backbone_edges, sum(weight) AS total_weight;


-- ============================================================================
-- 28. ALL DISTANCES FROM EMPLOYEE #1
-- ============================================================================

USE stress_test_network
CALL algo.allShortestPaths({source: 1})
YIELD nodeId, distance, path
RETURN nodeId, distance
ORDER BY distance
LIMIT 50;


-- ============================================================================
-- 29. DEPTH-FIRST EXPLORATION — Trace influence chains
-- ============================================================================

USE stress_test_network
CALL algo.dfs({source: 1})
YIELD nodeId, discoveryTime, finishTime, parentId
RETURN nodeId, discoveryTime, finishTime, parentId
ORDER BY discoveryTime
LIMIT 50;


-- ============================================================================
-- 30. NEAREST NEIGHBORS — Structural similarity at scale
-- ============================================================================

USE stress_test_network
CALL algo.knn({node: 1, k: 10})
YIELD neighborId, similarity, rank
RETURN neighborId, similarity, rank
ORDER BY rank;


-- ############################################################################
-- ############################################################################
--
-- PART 3: GRAPH VISUALIZATION — Progressive Scale Tests
--
-- ############################################################################
-- ############################################################################
-- Each query is a real use case — visualizing a specific organizational
-- slice. Scale increases progressively to find rendering limits.
--
--   31.  Single team (~100 nodes)        — should render instantly
--   32.  Small department slice (500)     — fast render
--   33.  Full department unit (1,000)     — smooth render
--   34.  Multi-team view (5,000)          — may need layout time
--   35.  Division-level (10,000)          — stress test begins
--   36.  Regional network (50,000)        — expect lag
--   37.  Large region (100,000)           — browser stress
--   38.  Full organization (1M + 5M)      — ultimate stress test
--
-- Node-only rendering:
--   39.  100 nodes    — node layout test
--   40.  1,000 nodes  — medium density
--   41.  10,000 nodes — large node cloud
--   42.  All 1M nodes — extreme test
-- ############################################################################


-- ============================================================================
-- 31. VIZ: SINGLE PROJECT TEAM — ~100 person team network
-- ============================================================================

USE stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 100 AND b.id <= 100
RETURN a, r, b;


-- ============================================================================
-- 32. VIZ: DEPARTMENT SLICE — 500 employees
-- ============================================================================

USE stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 500 AND b.id <= 500
RETURN a, r, b;


-- ============================================================================
-- 33. VIZ: FULL DEPARTMENT — 1,000 employees
-- ============================================================================

USE stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 1000 AND b.id <= 1000
RETURN a, r, b;


-- ============================================================================
-- 34. VIZ: MULTI-TEAM — 5,000 employees
-- ============================================================================

USE stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 5000 AND b.id <= 5000
RETURN a, r, b;


-- ============================================================================
-- 35. VIZ: DIVISION — 10,000 employees, stress test begins
-- ============================================================================

USE stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 10000 AND b.id <= 10000
RETURN a, r, b;


-- ============================================================================
-- 36. VIZ: REGIONAL — 50,000 employees, extreme rendering
-- ============================================================================
-- WARNING: Large result set. The visualizer may become sluggish.

USE stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 50000 AND b.id <= 50000
RETURN a, r, b;


-- ============================================================================
-- 37. VIZ: LARGE REGION — 100,000 employees, browser stress
-- ============================================================================
-- WARNING: Expect significant lag or memory pressure.

USE stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 100000 AND b.id <= 100000
RETURN a, r, b;


-- ============================================================================
-- 38. VIZ: FULL ORGANIZATION — All 1M employees + 5M connections
-- ============================================================================
-- WARNING: Ultimate stress test. The visualizer will likely freeze.

USE stress_test_network
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- 39. VIZ NODES: SINGLE TEAM — 100 employee nodes
-- ============================================================================

USE stress_test_network
MATCH (n)
WHERE n.id <= 100
RETURN n;


-- ============================================================================
-- 40. VIZ NODES: DEPARTMENT — 1,000 employee nodes
-- ============================================================================

USE stress_test_network
MATCH (n)
WHERE n.id <= 1000
RETURN n;


-- ============================================================================
-- 41. VIZ NODES: DIVISION — 10,000 employee nodes
-- ============================================================================

USE stress_test_network
MATCH (n)
WHERE n.id <= 10000
RETURN n;


-- ============================================================================
-- 42. VIZ NODES: FULL ORGANIZATION — All 1M employee nodes
-- ============================================================================
-- WARNING: Returns 1,000,000 node objects. Ultimate node rendering test.

USE stress_test_network
MATCH (n)
RETURN n;
