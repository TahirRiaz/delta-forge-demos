-- ============================================================================
-- Graph Cypher Queries — Demonstration Queries
-- ============================================================================
-- Progressive Cypher queries on the 5-person social graph. Starts with basic
-- pattern matching and builds up to graph algorithm procedures (CALL algo.*).
--
-- Cypher syntax: USE table_name MATCH (n) RETURN n
-- Algorithm syntax: USE table_name CALL algo.name() YIELD cols RETURN cols
-- ============================================================================


-- ============================================================================
-- 1. SHOW GRAPH CONFIG — Verify graph configuration from setup
-- ============================================================================
-- The setup script configured friendships_cypher as EDGE (src/dst/weight)
-- and persons_cypher as VERTEX (id). This query verifies those configs.

SHOW GRAPH CONFIG;


-- ============================================================================
-- 2. ALL NODES — Basic MATCH for all vertices
-- ============================================================================

USE {{zone_name}}.graph.friendships_cypher
MATCH (n)
RETURN n;


-- ============================================================================
-- 3. ALL EDGES — Directed relationship pattern matching
-- ============================================================================

USE {{zone_name}}.graph.friendships_cypher
MATCH (a)-[r]->(b)
RETURN a, b;


-- ============================================================================
-- 4. FILTERED EDGES — Cypher WHERE clause on node properties
-- ============================================================================
-- Find connections where the source person is over 28 years old.

USE {{zone_name}}.graph.friendships_cypher
MATCH (a)-[r]->(b)
WHERE a.age > 28
RETURN a.name AS person, a.age AS age, b.name AS friend;


-- ============================================================================
-- 5. TOP OLDEST — ORDER BY and LIMIT in Cypher
-- ============================================================================

USE {{zone_name}}.graph.friendships_cypher
MATCH (n)
RETURN n.name AS name, n.age AS age
ORDER BY n.age DESC
LIMIT 3;


-- ============================================================================
-- 6. ALICE'S FRIENDS — Property filter on node name
-- ============================================================================

USE {{zone_name}}.graph.friendships_cypher
MATCH (a {name: 'Alice'})-[r]->(b)
RETURN b.name AS friend_name, r.weight AS friendship_strength;


-- ============================================================================
-- 7. 2-HOP PATHS — Variable-length path traversal
-- ============================================================================
-- Find all nodes reachable from any node within 1–2 hops.

USE {{zone_name}}.graph.friendships_cypher
MATCH (a)-[]->(b)-[]->(c)
RETURN a, b, c;


-- ============================================================================
-- 8. VARIABLE-LENGTH PATHS — Reachability within 2 hops
-- ============================================================================

USE {{zone_name}}.graph.friendships_cypher
MATCH (a)-[*1..2]->(b)
WHERE a <> b
RETURN DISTINCT a AS start_node, b AS reachable_node;


-- ============================================================================
-- 9. NODE COUNT — Cypher aggregation
-- ============================================================================

USE {{zone_name}}.graph.friendships_cypher
MATCH (n)
RETURN count(n) AS node_count;


-- ============================================================================
-- 10. PAGERANK — Influence ranking via link analysis
-- ============================================================================
-- Carol ranks highest (receives links from Alice and Bob).

USE {{zone_name}}.graph.friendships_cypher
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD nodeId, score, rank
RETURN nodeId, score, rank
ORDER BY score DESC;


-- ============================================================================
-- 11. DEGREE CENTRALITY — In/out/total degree per node
-- ============================================================================
-- Alice has highest out-degree (2), Carol has highest in-degree (2).

USE {{zone_name}}.graph.friendships_cypher
CALL algo.degree()
YIELD nodeId, inDegree, outDegree, totalDegree
RETURN nodeId, inDegree, outDegree, totalDegree
ORDER BY totalDegree DESC;


-- ============================================================================
-- 12. BETWEENNESS CENTRALITY — Bridge node detection
-- ============================================================================
-- Nodes on the main cycle (1,3,4,5) have equal betweenness.
-- Bob (2) is a dead-end branch, so betweenness = 0.

USE {{zone_name}}.graph.friendships_cypher
CALL algo.betweenness()
YIELD nodeId, centrality, rank
RETURN nodeId, centrality, rank
ORDER BY centrality DESC;


-- ============================================================================
-- 13. CLOSENESS CENTRALITY — How central is each node?
-- ============================================================================
-- Alice (node 1) has highest closeness (can reach all nodes quickly).

USE {{zone_name}}.graph.friendships_cypher
CALL algo.closeness()
YIELD nodeId, closeness, rank
RETURN nodeId, closeness, rank
ORDER BY closeness DESC;


-- ============================================================================
-- 14. CONNECTED COMPONENTS — Is the graph fully connected?
-- ============================================================================
-- All 5 nodes should be in the same component (graph has a cycle).

USE {{zone_name}}.graph.friendships_cypher
CALL algo.connectedComponents()
YIELD nodeId, componentId
RETURN nodeId, componentId
ORDER BY nodeId;


-- ============================================================================
-- 15. TRIANGLE COUNT — Nodes participating in triangles
-- ============================================================================
-- Alice(1), Bob(2), Carol(3) form the only triangle.

USE {{zone_name}}.graph.friendships_cypher
CALL algo.triangleCount()
YIELD nodeId, triangleCount
RETURN nodeId, triangleCount
ORDER BY triangleCount DESC;


-- ============================================================================
-- 16. LOUVAIN COMMUNITY DETECTION — Modularity-based clustering
-- ============================================================================
-- Expected: 2 communities — triangle {1,2,3} and tail {4,5}.

USE {{zone_name}}.graph.friendships_cypher
CALL algo.louvain({resolution: 1.0})
YIELD nodeId, communityId
RETURN communityId, collect(nodeId) AS members, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 17. SHORTEST PATH — Dijkstra from Alice to Eve
-- ============================================================================
-- Path: Alice(1) -> Carol(3) -> Dave(4) -> Eve(5)
-- Distance: 0.8 + 0.9 + 0.7 = 2.4

USE {{zone_name}}.graph.friendships_cypher
CALL algo.shortestPath({source: 1, target: 5})
YIELD nodeId, step, distance
RETURN nodeId, step, distance
ORDER BY step;


-- ============================================================================
-- 18. ALL SHORTEST PATHS — Distances from Alice to all nodes
-- ============================================================================

USE {{zone_name}}.graph.friendships_cypher
CALL algo.allShortestPaths({source: 1})
YIELD nodeId, distance, path
RETURN nodeId, distance, path
ORDER BY distance;


-- ============================================================================
-- 19. BFS TRAVERSAL — Breadth-first from Alice
-- ============================================================================
-- Explores level by level: Alice(0) -> Bob,Carol(1) -> Dave(2) -> Eve(3)

USE {{zone_name}}.graph.friendships_cypher
CALL algo.bfs({source: 1})
YIELD nodeId, depth, parentId
RETURN nodeId, depth, parentId
ORDER BY depth, nodeId;


-- ============================================================================
-- 20. DFS TRAVERSAL — Depth-first from Alice
-- ============================================================================

USE {{zone_name}}.graph.friendships_cypher
CALL algo.dfs({source: 1})
YIELD nodeId, discoveryTime, finishTime, parentId
RETURN nodeId, discoveryTime, finishTime, parentId
ORDER BY discoveryTime;


-- ============================================================================
-- 21. MINIMUM SPANNING TREE — Lightest edges connecting all nodes
-- ============================================================================
-- 4 edges with minimum total weight connecting all 5 nodes.

USE {{zone_name}}.graph.friendships_cypher
CALL algo.mst()
YIELD sourceId, targetId, weight
RETURN sourceId, targetId, weight
ORDER BY weight;


-- ============================================================================
-- 22. KNN SIMILARITY — Nearest neighbors of Alice
-- ============================================================================
-- Bob is most similar to Alice (they share Carol as a neighbor).

USE {{zone_name}}.graph.friendships_cypher
CALL algo.knn({node: 1, k: 3})
YIELD neighborId, similarity, rank
RETURN neighborId, similarity, rank
ORDER BY rank;


-- ============================================================================
-- 23. PAIRWISE SIMILARITY — Jaccard similarity between Alice and Bob
-- ============================================================================

USE {{zone_name}}.graph.friendships_cypher
CALL algo.similarity({node1: 1, node2: 2, metric: 'jaccard'})
YIELD node1Id, node2Id, score
RETURN node1Id, node2Id, score;


-- ============================================================================
-- 24. STRONGLY CONNECTED COMPONENTS — Directed reachability groups
-- ============================================================================
-- All 5 nodes form one SCC (everyone can reach everyone via directed paths).

USE {{zone_name}}.graph.friendships_cypher
CALL algo.scc()
YIELD nodeId, componentId
RETURN nodeId, componentId
ORDER BY nodeId;


-- ============================================================================
-- 25. SUMMARY — PASS/FAIL verification checks
-- ============================================================================

SELECT 'person_count' AS check_name,
       CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.graph.persons_cypher
UNION ALL
SELECT 'edge_count',
       CASE WHEN COUNT(*) = 6 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.friendships_cypher
UNION ALL
SELECT 'alice_out_degree_2',
       CASE WHEN COUNT(*) = 2 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.friendships_cypher WHERE src = 1
UNION ALL
SELECT 'carol_in_degree_2',
       CASE WHEN COUNT(*) = 2 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.friendships_cypher WHERE dst = 3
UNION ALL
SELECT 'has_mentor_edge',
       CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.friendships_cypher WHERE relationship_type = 'mentor'
UNION ALL
SELECT 'max_weight_is_1',
       CASE WHEN MAX(weight) = 1.0 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.friendships_cypher
UNION ALL
SELECT 'no_self_loops',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.friendships_cypher WHERE src = dst
UNION ALL
SELECT 'bob_to_carol_exists',
       CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.friendships_cypher WHERE src = 2 AND dst = 3
UNION ALL
SELECT 'five_cities',
       CASE WHEN COUNT(DISTINCT city) = 5 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.persons_cypher
UNION ALL
SELECT 'weight_range_valid',
       CASE WHEN MIN(weight) >= 0.0 AND MAX(weight) <= 1.0 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.friendships_cypher
ORDER BY check_name;
