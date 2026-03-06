-- ============================================================================
-- Graph Flattened Mode — Cypher Queries
-- ============================================================================
-- All queries use Cypher to demonstrate graph analytics on FLATTENED
-- property tables. The Cypher engine accesses vertex/edge properties
-- transparently — whether stored as flat columns or JSON, the query
-- syntax is the same. Flattened mode provides the fastest property
-- access with full predicate pushdown.
-- ============================================================================


-- ============================================================================
-- PART 1: EXPLORE THE ORGANIZATION
-- ============================================================================


-- ============================================================================
-- 1. MEET THE TEAM — Browse all 50 employees
-- ============================================================================
-- Returns every employee with their key properties. In flattened mode,
-- each property is a direct column — the graph engine reads them without
-- any JSON extraction overhead.

USE flattened_demo
MATCH (n)
RETURN n.name AS name, n.department AS dept, n.title AS title,
       n.city AS city, n.level AS level, n.age AS age
ORDER BY n.department, n.name;


-- ============================================================================
-- 2. HEADCOUNT BY DEPARTMENT — Workforce distribution
-- ============================================================================
-- Aggregations on node properties. How many people are in each department
-- and what's the average age? Identifies teams that may need hiring.

USE flattened_demo
MATCH (n)
RETURN n.department AS department, count(n) AS headcount,
       avg(n.age) AS avg_age
ORDER BY headcount DESC;


-- ============================================================================
-- 3. SENIOR STAFF — Find experienced employees ready to mentor
-- ============================================================================
-- Active senior staff (L3+) could lead mentorship programs. Property
-- predicates push down to storage for fast filtering in flattened mode.

USE flattened_demo
MATCH (n)
WHERE n.active = true AND n.level IN ['L3', 'L4', 'L5']
RETURN n.name AS name, n.department AS dept, n.city AS city,
       n.title AS title, n.level AS level
ORDER BY n.level DESC, n.name;


-- ============================================================================
-- 4. COMPANY NETWORK — Visualize all connections
-- ============================================================================
-- Full graph visualization: 50 people and ~150 connections. Department
-- clusters should appear as dense groups with bridge edges between them.

USE flattened_demo
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- PART 2: RELATIONSHIP ANALYSIS
-- ============================================================================


-- ============================================================================
-- 5. MENTORSHIP MAP — Who is coaching whom?
-- ============================================================================
-- The formal mentorship structure. Strong bonds (high weight) indicate
-- effective mentoring. Cross-level mentorship (L5→L2) is more valuable
-- than peer mentoring (L4→L4).

USE flattened_demo
MATCH (mentor)-[r]->(mentee)
WHERE r.relationship_type = 'mentor'
RETURN mentor.name AS mentor, mentor.title AS mentor_title,
       mentor.department AS dept, mentee.name AS mentee,
       mentee.title AS mentee_title, r.weight AS bond_strength
ORDER BY r.weight DESC;


-- ============================================================================
-- 6. VISUALIZE MENTORSHIPS — See the coaching hierarchy
-- ============================================================================

USE flattened_demo
MATCH (mentor)-[r]->(mentee)
WHERE r.relationship_type = 'mentor'
RETURN mentor, r, mentee;


-- ============================================================================
-- 7. STRONGEST CONNECTIONS — The backbone relationships
-- ============================================================================
-- High-weight edges (> 0.8) are the relationships that hold the org
-- together. Losing one of these is like cutting a load-bearing beam.

USE flattened_demo
MATCH (a)-[r]->(b)
WHERE r.weight > 0.8
RETURN a.name AS person_a, b.name AS person_b,
       r.relationship_type AS type, r.weight AS strength,
       r.context AS context
ORDER BY r.weight DESC;


-- ============================================================================
-- 8. CROSS-DEPARTMENT BRIDGES — Preventing organizational silos
-- ============================================================================
-- Show only edges between different departments. These bridge employees
-- are critical for cross-team collaboration and knowledge sharing.

USE flattened_demo
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a, r, b;


-- ============================================================================
-- 9. DEPARTMENT CONNECTIVITY — Which teams talk to each other?
-- ============================================================================
-- Before a reorg, leadership needs to know which departments are already
-- collaborating. High connection counts suggest natural alignment.

USE flattened_demo
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a.department AS from_dept, b.department AS to_dept,
       count(r) AS connections, avg(r.weight) AS avg_strength
ORDER BY connections DESC;


-- ============================================================================
-- 10. WORK vs SOCIAL — What types of bonds exist?
-- ============================================================================
-- Understanding the relationship context mix reveals organizational health.
-- A balance of work ties AND social bonds means a resilient culture.

USE flattened_demo
MATCH (a)-[r]->(b)
RETURN r.context AS context, r.relationship_type AS type,
       count(r) AS count, avg(r.weight) AS avg_weight
ORDER BY count DESC;


-- ============================================================================
-- 11. RECIPROCAL BONDS — Where are mutual relationships?
-- ============================================================================
-- When A connects to B AND B connects back to A, the relationship is
-- genuinely collaborative. High mutual count = healthy team culture.

USE flattened_demo
MATCH (a)-[r1]->(b)-[r2]->(a)
WHERE a.id < b.id
RETURN a.name AS person_a, b.name AS person_b,
       r1.relationship_type AS a_to_b, r2.relationship_type AS b_to_a,
       r1.weight AS a_to_b_weight, r2.weight AS b_to_a_weight
ORDER BY r1.weight + r2.weight DESC;


-- ============================================================================
-- 12. CITY-BASED COLLABORATION — Cross-department bonds within offices
-- ============================================================================
-- People in the same city but different departments form social bridges
-- that prevent the org from becoming siloed by department alone.

USE flattened_demo
MATCH (a)-[r]->(b)
WHERE a.city = b.city AND a.department <> b.department
RETURN a.city AS city, a.department AS from_dept,
       b.department AS to_dept, count(r) AS connections,
       avg(r.weight) AS avg_weight
ORDER BY connections DESC;


-- ============================================================================
-- PART 3: NETWORK TRAVERSAL
-- ============================================================================


-- ============================================================================
-- 13. FRIENDS OF FRIENDS — 2-hop information flow
-- ============================================================================
-- If person #1 shares important news, who hears it directly and who
-- hears it through the grapevine? Shows the relay chain.

USE flattened_demo
MATCH (a)-[]->(b)-[]->(c)
WHERE a.id = 1 AND a <> c
RETURN a.name AS source, b.name AS relay, c.name AS reached,
       b.department AS relay_dept, c.department AS reached_dept;


-- ============================================================================
-- 14. REACHABILITY — Who can person #1 reach within 3 hops?
-- ============================================================================

USE flattened_demo
MATCH (a)-[*1..3]->(b)
WHERE a.id = 1 AND a <> b
RETURN DISTINCT b.name AS reachable, b.department AS dept
ORDER BY b.name;


-- ============================================================================
-- 15. ENGINEERING SUBGRAPH — Internal team collaboration
-- ============================================================================

USE flattened_demo
MATCH (a)-[r]->(b)
WHERE a.department = 'Engineering' AND b.department = 'Engineering'
RETURN a, r, b;


-- ============================================================================
-- PART 4: GRAPH ALGORITHMS
-- ============================================================================


-- ============================================================================
-- 16. PAGERANK — Who are the informal influencers?
-- ============================================================================
-- PageRank finds nodes referenced by other well-connected nodes.
-- Directors and bridge nodes should rank highest.

USE flattened_demo
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD nodeId, score, rank
RETURN nodeId, score, rank
ORDER BY score DESC;


-- ============================================================================
-- 17. DEGREE CENTRALITY — Connection counts from the graph engine
-- ============================================================================

USE flattened_demo
CALL algo.degree()
YIELD nodeId, inDegree, outDegree, totalDegree
RETURN nodeId, inDegree, outDegree, totalDegree
ORDER BY totalDegree DESC;


-- ============================================================================
-- 18. GATEKEEPERS — Who controls information flow?
-- ============================================================================

USE flattened_demo
CALL algo.betweenness()
YIELD nodeId, centrality, rank
RETURN nodeId, centrality, rank
ORDER BY centrality DESC;


-- ============================================================================
-- 19. NATURAL TEAMS — Louvain community detection
-- ============================================================================
-- Should find ~5 communities matching the department structure, plus
-- possible sub-clusters from city-based bonds.

USE flattened_demo
CALL algo.louvain({resolution: 1.0})
YIELD nodeId, communityId
RETURN communityId, collect(nodeId) AS members, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 20. IS EVERYONE CONNECTED? — Connected components
-- ============================================================================

USE flattened_demo
CALL algo.connectedComponents()
YIELD nodeId, componentId
RETURN componentId, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 21. TIGHT-KNIT GROUPS — Triangle count
-- ============================================================================

USE flattened_demo
CALL algo.triangleCount()
YIELD nodeId, triangleCount
RETURN nodeId, triangleCount
ORDER BY triangleCount DESC;


-- ============================================================================
-- 22. SHORTEST PATH — Fastest route between two employees
-- ============================================================================

USE flattened_demo
CALL algo.shortestPath({source: 1, target: 42})
YIELD nodeId, step, distance
RETURN nodeId, step, distance
ORDER BY step;


-- ============================================================================
-- 23. SIX DEGREES — How many hops apart are people?
-- ============================================================================

USE flattened_demo
CALL algo.bfs({source: 1})
YIELD nodeId, depth, parentId
RETURN depth, count(*) AS people_at_distance
ORDER BY depth;


-- ============================================================================
-- PART 5: VISUALIZATION
-- ============================================================================


-- ============================================================================
-- 24. FULL COMPANY GRAPH — All 50 people and ~150 edges
-- ============================================================================

USE flattened_demo
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- 25. MENTORSHIP HIERARCHY
-- ============================================================================

USE flattened_demo
MATCH (a)-[r]->(b)
WHERE r.relationship_type = 'mentor'
RETURN a, r, b;


-- ============================================================================
-- 26. ENGINEERING DEPARTMENT SUBGRAPH
-- ============================================================================

USE flattened_demo
MATCH (a)-[r]->(b)
WHERE a.department = 'Engineering' AND b.department = 'Engineering'
RETURN a, r, b;


-- ============================================================================
-- 27. CROSS-DEPARTMENT BRIDGES ONLY
-- ============================================================================
-- Strips away intra-department edges to reveal the bridges that prevent silos.

USE flattened_demo
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a, r, b;
