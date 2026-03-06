-- ============================================================================
-- Graph JSON Mode — Cypher Queries
-- ============================================================================
-- Demonstrates graph analytics using Cypher on JSON property tables.
-- All vertex/edge properties are stored in a single JSON string column.
-- The Cypher engine extracts properties transparently — the query syntax
-- is identical to flattened or hybrid mode. JSON mode provides maximum
-- schema flexibility at the cost of JSON parsing on every access.
-- ============================================================================


-- ============================================================================
-- PART 1: EXPLORE THE ORGANIZATION
-- ============================================================================


-- ============================================================================
-- 1. MEET THE TEAM — Browse all 50 employees
-- ============================================================================
-- Properties are stored as JSON blobs, but Cypher accesses them the same
-- way as flat columns: n.name, n.department, etc. The engine handles
-- JSON extraction automatically.

USE json_demo
MATCH (n)
RETURN n.name AS name, n.age AS age, n.department AS dept,
       n.city AS city, n.title AS title, n.level AS level
ORDER BY n.department, n.name;


-- ============================================================================
-- 2. HEADCOUNT BY DEPARTMENT — Workforce distribution
-- ============================================================================

USE json_demo
MATCH (n)
RETURN n.department AS department, count(n) AS headcount,
       avg(n.age) AS avg_age
ORDER BY headcount DESC;


-- ============================================================================
-- 3. FIND ENGINEERING — Department filter
-- ============================================================================
-- In JSON mode, this filter is applied after JSON extraction rather than
-- pushed down to storage. Flattened mode would be faster here, but the
-- query syntax is identical.

USE json_demo
MATCH (n)
WHERE n.department = 'Engineering'
RETURN n.name AS name, n.age AS age, n.city AS city, n.title AS title
ORDER BY n.age DESC;


-- ============================================================================
-- 4. COMPANY NETWORK — Visualize all connections
-- ============================================================================

USE json_demo
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- PART 2: RELATIONSHIP ANALYSIS
-- ============================================================================


-- ============================================================================
-- 5. MENTORSHIP MAP — Who is coaching whom?
-- ============================================================================

USE json_demo
MATCH (mentor)-[r]->(mentee)
WHERE r.relationship_type = 'mentor'
RETURN mentor.name AS mentor, mentor.title AS mentor_title,
       mentor.department AS dept, mentee.name AS mentee,
       mentee.title AS mentee_title, r.weight AS bond_strength
ORDER BY r.weight DESC;


-- ============================================================================
-- 6. VISUALIZE MENTORSHIPS — Coaching hierarchy
-- ============================================================================

USE json_demo
MATCH (mentor)-[r]->(mentee)
WHERE r.relationship_type = 'mentor'
RETURN mentor, r, mentee;


-- ============================================================================
-- 7. STRONGEST BONDS — High-impact relationships
-- ============================================================================

USE json_demo
MATCH (a)-[r]->(b)
WHERE r.weight > 0.8
RETURN a.name AS person_a, b.name AS person_b,
       r.relationship_type AS type, r.weight AS strength
ORDER BY r.weight DESC;


-- ============================================================================
-- 8. CROSS-DEPARTMENT BRIDGES — Who connects the silos?
-- ============================================================================

USE json_demo
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a, r, b;


-- ============================================================================
-- 9. DEPARTMENT CONNECTIVITY — Which teams collaborate?
-- ============================================================================

USE json_demo
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a.department AS from_dept, b.department AS to_dept,
       count(r) AS connections, avg(r.weight) AS avg_strength
ORDER BY connections DESC;


-- ============================================================================
-- 10. RECIPROCAL BONDS — Genuine two-way relationships
-- ============================================================================

USE json_demo
MATCH (a)-[r1]->(b)-[r2]->(a)
WHERE a.id < b.id
RETURN a.name AS person_a, b.name AS person_b,
       r1.relationship_type AS a_to_b, r2.relationship_type AS b_to_a,
       r1.weight AS a_to_b_weight, r2.weight AS b_to_a_weight
ORDER BY r1.weight + r2.weight DESC;


-- ============================================================================
-- PART 3: NETWORK TRAVERSAL
-- ============================================================================


-- ============================================================================
-- 11. FRIENDS OF FRIENDS — 2-hop information flow
-- ============================================================================

USE json_demo
MATCH (a)-[]->(b)-[]->(c)
WHERE a.id = 1 AND a <> c
RETURN a.name AS source, b.name AS relay, c.name AS reached,
       b.department AS relay_dept, c.department AS reached_dept;


-- ============================================================================
-- 12. REACHABILITY — Who can person #1 reach within 3 hops?
-- ============================================================================

USE json_demo
MATCH (a)-[*1..3]->(b)
WHERE a.id = 1 AND a <> b
RETURN DISTINCT b.name AS reachable, b.department AS dept
ORDER BY b.name;


-- ============================================================================
-- PART 4: GRAPH ALGORITHMS
-- ============================================================================


-- ============================================================================
-- 13. PAGERANK — Informal influencers
-- ============================================================================

USE json_demo
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD nodeId, score, rank
RETURN nodeId, score, rank
ORDER BY score DESC;


-- ============================================================================
-- 14. DEGREE CENTRALITY — Connection counts
-- ============================================================================

USE json_demo
CALL algo.degree()
YIELD nodeId, inDegree, outDegree, totalDegree
RETURN nodeId, inDegree, outDegree, totalDegree
ORDER BY totalDegree DESC;


-- ============================================================================
-- 15. GATEKEEPERS — Betweenness centrality
-- ============================================================================

USE json_demo
CALL algo.betweenness()
YIELD nodeId, centrality, rank
RETURN nodeId, centrality, rank
ORDER BY centrality DESC;


-- ============================================================================
-- 16. NATURAL TEAMS — Louvain community detection
-- ============================================================================

USE json_demo
CALL algo.louvain({resolution: 1.0})
YIELD nodeId, communityId
RETURN communityId, collect(nodeId) AS members, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 17. TIGHT-KNIT GROUPS — Triangle count
-- ============================================================================

USE json_demo
CALL algo.triangleCount()
YIELD nodeId, triangleCount
RETURN nodeId, triangleCount
ORDER BY triangleCount DESC;


-- ============================================================================
-- 18. SHORTEST PATH — Route a message across the company
-- ============================================================================

USE json_demo
CALL algo.shortestPath({source: 1, target: 42})
YIELD nodeId, step, distance
RETURN nodeId, step, distance
ORDER BY step;


-- ============================================================================
-- 19. SIX DEGREES — How far apart is everyone?
-- ============================================================================

USE json_demo
CALL algo.bfs({source: 1})
YIELD nodeId, depth, parentId
RETURN depth, count(*) AS people_at_distance
ORDER BY depth;


-- ============================================================================
-- PART 5: VISUALIZATION
-- ============================================================================


-- ============================================================================
-- 20. FULL COMPANY GRAPH
-- ============================================================================

USE json_demo
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- 21. MENTORSHIP HIERARCHY
-- ============================================================================

USE json_demo
MATCH (a)-[r]->(b)
WHERE r.relationship_type = 'mentor'
RETURN a, r, b;


-- ============================================================================
-- 22. CROSS-DEPARTMENT BRIDGES
-- ============================================================================

USE json_demo
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a, r, b;
