-- ============================================================================
-- Graph Cypher Queries — Demonstration Queries
-- ============================================================================
-- Progressive Cypher queries on the 50-person startup graph. Starts with
-- basic pattern matching and builds up to graph algorithm procedures.
--
-- Cypher syntax: USE graph_name MATCH (n) RETURN n
-- Algorithm syntax: USE graph_name CALL algo.name() YIELD cols RETURN cols
--
-- IMPORTANT: The USE clause references the graph definition name
-- (cypher_demo), NOT the table name.
-- ============================================================================


-- ============================================================================
-- PART 1: BASIC PATTERN MATCHING
-- ============================================================================


-- ============================================================================
-- 1. SHOW GRAPH CONFIG — Verify graph configuration from setup
-- ============================================================================
-- Lists all graph configurations (vertex/edge table mappings, columns).
-- Use this to verify that setup.sql configured the tables correctly.

SHOW GRAPH CONFIG;


-- ============================================================================
-- 2. ALL NODES — Return every vertex in the graph
-- ============================================================================
-- The simplest Cypher query. Returns all 50 employee nodes.

USE cypher_demo
MATCH (n)
RETURN n;


-- ============================================================================
-- 3. ALL RELATIONSHIPS — Directed edge pattern
-- ============================================================================
-- (a)-[r]->(b) matches directed edges. Returns ~150 relationships.

USE cypher_demo
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- 4. NODE PROPERTY FILTER — Find Engineering employees
-- ============================================================================
-- WHERE clause filters on vertex properties.

USE cypher_demo
MATCH (n)
WHERE n.department = 'Engineering'
RETURN n.name AS name, n.age AS age, n.title AS title
ORDER BY n.age DESC;


-- ============================================================================
-- 5. EDGE PROPERTY FILTER — Find mentor relationships
-- ============================================================================
-- Filter edges by relationship_type to find mentorship connections.

USE cypher_demo
MATCH (mentor)-[r]->(mentee)
WHERE r.relationship_type = 'mentor'
RETURN mentor.name AS mentor, mentee.name AS mentee, r.weight AS strength
ORDER BY r.weight DESC;


-- ============================================================================
-- 6. INLINE PROPERTY MATCH — Alice's direct connections
-- ============================================================================
-- {name: 'Alice_1'} is an inline property filter on the node.

USE cypher_demo
MATCH (a {name: 'Alice_1'})-[r]->(b)
RETURN b.name AS friend, r.relationship_type AS rel_type, r.weight AS strength;


-- ============================================================================
-- 7. ORDER BY + LIMIT — Top 10 oldest employees
-- ============================================================================

USE cypher_demo
MATCH (n)
RETURN n.name AS name, n.age AS age, n.department AS dept
ORDER BY n.age DESC
LIMIT 10;


-- ============================================================================
-- 8. NODE COUNT — How many employees?
-- ============================================================================

USE cypher_demo
MATCH (n)
RETURN count(n) AS employee_count;


-- ============================================================================
-- 9. EDGE COUNT — How many connections?
-- ============================================================================

USE cypher_demo
MATCH (a)-[r]->(b)
RETURN count(r) AS connection_count;


-- ============================================================================
-- PART 2: MULTI-HOP TRAVERSAL
-- ============================================================================


-- ============================================================================
-- 10. 2-HOP PATHS — Friends of friends
-- ============================================================================
-- Explicit 2-hop: (a)-[]->(b)-[]->(c). Shows the intermediate node.

USE cypher_demo
MATCH (a)-[]->(b)-[]->(c)
WHERE a.name = 'Alice_1' AND a <> c
RETURN a.name AS start, b.name AS via, c.name AS destination;


-- ============================================================================
-- 11. VARIABLE-LENGTH PATHS — Reachability within 1-3 hops
-- ============================================================================
-- [*1..3] matches paths of length 1, 2, or 3.

USE cypher_demo
MATCH (a)-[*1..3]->(b)
WHERE a.name = 'Alice_1' AND a <> b
RETURN DISTINCT b.name AS reachable
ORDER BY b.name;


-- ============================================================================
-- PART 3: GRAPH ALGORITHMS
-- ============================================================================


-- ============================================================================
-- 12. PAGERANK — Who are the informal influencers?
-- ============================================================================
-- PageRank measures influence by looking at who is referenced by other
-- well-connected nodes. Directors and bridge nodes should rank highest.

USE cypher_demo
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD nodeId, score, rank
RETURN nodeId, score, rank
ORDER BY score DESC;


-- ============================================================================
-- 13. DEGREE CENTRALITY — In/out/total connections
-- ============================================================================
-- Degree counts in the graph engine. Bridge nodes (id=13, 26) should
-- have high out-degree; popular employees have high in-degree.

USE cypher_demo
CALL algo.degree()
YIELD nodeId, inDegree, outDegree, totalDegree
RETURN nodeId, inDegree, outDegree, totalDegree
ORDER BY totalDegree DESC;


-- ============================================================================
-- 14. BETWEENNESS CENTRALITY — Critical connectors
-- ============================================================================
-- Betweenness measures how often a node lies on shortest paths between
-- other pairs. High betweenness = removing this person fragments the network.

USE cypher_demo
CALL algo.betweenness()
YIELD nodeId, centrality, rank
RETURN nodeId, centrality, rank
ORDER BY centrality DESC;


-- ============================================================================
-- 15. CLOSENESS CENTRALITY — Most central employees
-- ============================================================================
-- Closeness measures how quickly a person can reach everyone else.
-- People in central departments or with bridge connections score highest.

USE cypher_demo
CALL algo.closeness()
YIELD nodeId, closeness, rank
RETURN nodeId, closeness, rank
ORDER BY closeness DESC;


-- ============================================================================
-- 16. CONNECTED COMPONENTS — Is the org fully connected?
-- ============================================================================
-- In a healthy organization, everyone should be in the same component.
-- Isolated components indicate communication gaps.

USE cypher_demo
CALL algo.connectedComponents()
YIELD nodeId, componentId
RETURN componentId, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 17. LOUVAIN COMMUNITY DETECTION — Natural clusters
-- ============================================================================
-- Louvain finds communities based on edge density. Should detect ~5
-- primary communities matching the department structure.

USE cypher_demo
CALL algo.louvain({resolution: 1.0})
YIELD nodeId, communityId
RETURN communityId, collect(nodeId) AS members, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 18. TRIANGLE COUNT — Cluster tightness
-- ============================================================================
-- Triangles indicate tight-knit groups (A->B, B->C, C->A). Nodes in
-- dense department clusters should have higher triangle counts.

USE cypher_demo
CALL algo.triangleCount()
YIELD nodeId, triangleCount
RETURN nodeId, triangleCount
ORDER BY triangleCount DESC;


-- ============================================================================
-- 19. STRONGLY CONNECTED COMPONENTS — Directed reachability
-- ============================================================================
-- SCCs are groups where every node can reach every other node via
-- directed paths. Shows true bidirectional communication clusters.

USE cypher_demo
CALL algo.scc()
YIELD nodeId, componentId
RETURN componentId, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- PART 4: PATHFINDING
-- ============================================================================


-- ============================================================================
-- 20. SHORTEST PATH — Fastest route between two employees
-- ============================================================================
-- Weighted shortest path from Alice_1 to person #42.

USE cypher_demo
CALL algo.shortestPath({source: 1, target: 42})
YIELD nodeId, step, distance
RETURN nodeId, step, distance
ORDER BY step;


-- ============================================================================
-- 21. ALL SHORTEST PATHS — Distances from Alice to everyone
-- ============================================================================

USE cypher_demo
CALL algo.allShortestPaths({source: 1})
YIELD nodeId, distance, path
RETURN nodeId, distance, path
ORDER BY distance;


-- ============================================================================
-- 22. BFS — Breadth-first exploration from Alice
-- ============================================================================
-- Explores the graph level by level, showing how information would
-- spread outward from a starting point.

USE cypher_demo
CALL algo.bfs({source: 1})
YIELD nodeId, depth, parentId
RETURN nodeId, depth, parentId
ORDER BY depth, nodeId;


-- ============================================================================
-- 23. DFS — Depth-first exploration from Alice
-- ============================================================================
-- Explores as deep as possible before backtracking.

USE cypher_demo
CALL algo.dfs({source: 1})
YIELD nodeId, discoveryTime, finishTime, parentId
RETURN nodeId, discoveryTime, finishTime, parentId
ORDER BY discoveryTime;


-- ============================================================================
-- 24. MINIMUM SPANNING TREE — Lightest connecting edges
-- ============================================================================
-- The minimum set of edges needed to keep everyone connected,
-- weighted by relationship strength.

USE cypher_demo
CALL algo.mst()
YIELD sourceId, targetId, weight
RETURN sourceId, targetId, weight
ORDER BY weight;


-- ============================================================================
-- PART 5: SIMILARITY
-- ============================================================================


-- ============================================================================
-- 25. KNN — Nearest neighbors of Alice
-- ============================================================================
-- Finds the K most structurally similar nodes to Alice based on
-- shared neighbors.

USE cypher_demo
CALL algo.knn({node: 1, k: 5})
YIELD neighborId, similarity, rank
RETURN neighborId, similarity, rank
ORDER BY rank;


-- ============================================================================
-- 26. PAIRWISE SIMILARITY — Compare two specific employees
-- ============================================================================

USE cypher_demo
CALL algo.similarity({node1: 1, node2: 13, metric: 'jaccard'})
YIELD node1Id, node2Id, score
RETURN node1Id, node2Id, score;


-- ============================================================================
-- PART 6: VISUALIZATION
-- ============================================================================


-- ============================================================================
-- 27. VISUALIZE — Full company graph
-- ============================================================================
-- All 50 people and ~150 connections. Labels show department, edges
-- show relationship_type.

USE cypher_demo
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- 28. VISUALIZE — Mentorship hierarchy only
-- ============================================================================
-- Shows just the mentor->mentee relationships — the formal org structure.

USE cypher_demo
MATCH (a)-[r]->(b)
WHERE r.relationship_type = 'mentor'
RETURN a, r, b;


-- ============================================================================
-- 29. VISUALIZE — Engineering department subgraph
-- ============================================================================

USE cypher_demo
MATCH (a)-[r]->(b)
WHERE a.department = 'Engineering' AND b.department = 'Engineering'
RETURN a, r, b;


-- ============================================================================
-- 30. VISUALIZE — Cross-department bridges
-- ============================================================================
-- Show only edges that connect different departments — the bridges
-- that prevent organizational silos.

USE cypher_demo
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a, r, b;
