-- ============================================================================
-- Graph Cypher Queries — Demonstration Queries
-- ============================================================================
-- Progressive Cypher queries on the 50-person startup graph. Starts with
-- exploration and builds up to graph algorithms, pathfinding, and similarity.
--
-- Cypher syntax: USE graph_name MATCH (n) RETURN n
-- Algorithm syntax: USE graph_name CALL algo.name() YIELD cols RETURN cols
--
-- IMPORTANT: The USE clause references the graph definition name
-- (cypher_demo), NOT the table name.
-- ============================================================================


-- ============================================================================
-- PART 1: EXPLORE THE ORGANIZATION
-- ============================================================================


-- ============================================================================
-- 1. GRAPH CONFIG — What tables and columns back this graph?
-- ============================================================================
-- Lists all graph configurations (vertex/edge table mappings, columns).
-- Use this to verify the graph definition before writing queries.

SHOW GRAPH CONFIG;


-- ============================================================================
-- 2. MEET THE TEAM — Browse all 50 employees with their roles
-- ============================================================================
-- The CEO wants to see who's in the company. Each node carries properties
-- like name, department, title, city, and seniority level.

USE cypher_demo
MATCH (n)
RETURN n.name AS name, n.department AS dept, n.title AS title,
       n.city AS city, n.level AS level
ORDER BY n.department, n.name;


-- ============================================================================
-- 3. ENGINEERING ROSTER — Who's on the engineering team?
-- ============================================================================
-- The VP of Engineering wants a headcount. Property filtering in Cypher
-- is clean and readable — no JOIN required.

USE cypher_demo
MATCH (n)
WHERE n.department = 'Engineering'
RETURN n.name AS name, n.age AS age, n.title AS title, n.city AS city
ORDER BY n.age DESC;


-- ============================================================================
-- 4. COMPANY MAP — Visualize all 50 employees and their connections
-- ============================================================================
-- Renders the full organizational network. Department clusters should be
-- visible as dense groups with bridge employees spanning between them.

USE cypher_demo
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- PART 2: RELATIONSHIP ANALYSIS
-- ============================================================================


-- ============================================================================
-- 5. MENTORSHIP NETWORK — Who is coaching whom?
-- ============================================================================
-- Filter edges by type to see the formal mentorship structure. Strong
-- bonds (high weight) indicate effective mentoring relationships worth
-- studying and replicating.

USE cypher_demo
MATCH (mentor)-[r]->(mentee)
WHERE r.relationship_type = 'mentor'
RETURN mentor.name AS mentor, mentor.department AS dept,
       mentee.name AS mentee, r.weight AS strength
ORDER BY r.weight DESC;


-- ============================================================================
-- 6. VISUALIZE MENTORSHIPS — See the coaching hierarchy
-- ============================================================================
-- Renders just the mentor-mentee edges. Directors should appear as hub
-- nodes connecting to multiple mentees below them.

USE cypher_demo
MATCH (mentor)-[r]->(mentee)
WHERE r.relationship_type = 'mentor'
RETURN mentor, r, mentee;


-- ============================================================================
-- 7. STRONGEST BONDS — Where are the tightest working relationships?
-- ============================================================================
-- High-weight connections (> 0.8) indicate deep trust and collaboration.
-- These are the relationships that hold the organization together — if
-- one partner leaves, the other loses a critical support.

USE cypher_demo
MATCH (a)-[r]->(b)
WHERE r.weight > 0.8
RETURN a.name AS person_a, b.name AS person_b,
       r.relationship_type AS type, r.weight AS strength
ORDER BY r.weight DESC;


-- ============================================================================
-- 8. CROSS-DEPARTMENT BRIDGES — Who connects the silos?
-- ============================================================================
-- Edges between different departments are the bridges that prevent
-- organizational silos. Without these, Engineering and Sales might
-- never communicate. These bridge employees are critical to retain.

USE cypher_demo
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a, r, b;


-- ============================================================================
-- 9. ALICE'S NETWORK — One person's direct connections
-- ============================================================================
-- Inline property filtering: see everyone Alice_1 connects to directly,
-- what type of relationship each is, and how strong the bond is.

USE cypher_demo
MATCH (a {name: 'Alice_1'})-[r]->(b)
RETURN b.name AS contact, b.department AS dept,
       r.relationship_type AS rel_type, r.weight AS strength
ORDER BY r.weight DESC;


-- ============================================================================
-- PART 3: NETWORK TRAVERSAL — How does information spread?
-- ============================================================================


-- ============================================================================
-- 10. FRIENDS OF FRIENDS — 2-hop information flow from Alice
-- ============================================================================
-- If Alice shares important news, it reaches her direct contacts first,
-- then spreads to their contacts. This shows the intermediate relay
-- person at each step.

USE cypher_demo
MATCH (a)-[]->(b)-[]->(c)
WHERE a.name = 'Alice_1' AND a <> c
RETURN a.name AS source, b.name AS relay, c.name AS reached,
       b.department AS relay_dept, c.department AS reached_dept;


-- ============================================================================
-- 11. REACHABILITY — Who can Alice reach within 3 hops?
-- ============================================================================
-- Variable-length paths [*1..3] show everyone reachable within 3 steps.
-- In a well-connected 50-person startup, Alice should be able to reach
-- most people within 3 hops.

USE cypher_demo
MATCH (a)-[*1..3]->(b)
WHERE a.name = 'Alice_1' AND a <> b
RETURN DISTINCT b.name AS reachable, b.department AS dept
ORDER BY b.name;


-- ============================================================================
-- 12. ENGINEERING SUBGRAPH — How does the engineering team collaborate?
-- ============================================================================
-- Isolate just the engineering department's internal network to see
-- team cohesion — are engineers well-connected, or are there isolated
-- sub-teams within the department?

USE cypher_demo
MATCH (a)-[r]->(b)
WHERE a.department = 'Engineering' AND b.department = 'Engineering'
RETURN a, r, b;


-- ============================================================================
-- PART 4: GRAPH ALGORITHMS — Uncover hidden patterns
-- ============================================================================


-- ============================================================================
-- 13. PAGERANK — Who are the real influencers?
-- ============================================================================
-- PageRank measures influence by looking at who is referenced by other
-- well-connected nodes — not just raw connection count. The informal
-- leaders whose opinions shape company culture rank highest here.

USE cypher_demo
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD nodeId, score, rank
RETURN nodeId, score, rank
ORDER BY score DESC;


-- ============================================================================
-- 14. DEGREE CENTRALITY — In/out/total connections per person
-- ============================================================================
-- High in-degree = many people seek you out (go-to expert).
-- High out-degree = you actively network (connector).
-- Bridge nodes should have high out-degree; popular employees high in-degree.

USE cypher_demo
CALL algo.degree()
YIELD nodeId, inDegree, outDegree, totalDegree
RETURN nodeId, inDegree, outDegree, totalDegree
ORDER BY totalDegree DESC;


-- ============================================================================
-- 15. GATEKEEPERS — Who controls information flow?
-- ============================================================================
-- Betweenness centrality finds people who sit on many shortest paths
-- between others. Removing a high-betweenness person fragments the
-- network. These are the employees you cannot afford to lose.

USE cypher_demo
CALL algo.betweenness()
YIELD nodeId, centrality, rank
RETURN nodeId, centrality, rank
ORDER BY centrality DESC;


-- ============================================================================
-- 16. ACCESSIBILITY — Who can reach everyone fastest?
-- ============================================================================
-- Closeness centrality measures how quickly a person can reach everyone
-- else. High closeness = great for spreading announcements or driving
-- change initiatives across the organization.

USE cypher_demo
CALL algo.closeness()
YIELD nodeId, closeness, rank
RETURN nodeId, closeness, rank
ORDER BY closeness DESC;


-- ============================================================================
-- 17. IS THE ORG FULLY CONNECTED? — Connected components
-- ============================================================================
-- In a healthy organization, everyone should be in one giant component.
-- Multiple components mean groups that literally cannot reach each other
-- through the network — a serious communication gap.

USE cypher_demo
CALL algo.connectedComponents()
YIELD nodeId, componentId
RETURN componentId, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 18. NATURAL TEAMS — Do real clusters match the org chart?
-- ============================================================================
-- Louvain community detection finds groups based on actual connection
-- density — not the formal hierarchy. If communities align with
-- departments (~5 clusters for 5 departments), the org chart reflects
-- reality. If not, people are self-organizing differently.

USE cypher_demo
CALL algo.louvain({resolution: 1.0})
YIELD nodeId, communityId
RETURN communityId, collect(nodeId) AS members, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 19. TIGHT-KNIT GROUPS — Triangle count
-- ============================================================================
-- A triangle means A→B, B→C, C→A — a group of three who all know each
-- other. High triangle count = strong team cohesion. Nodes in dense
-- department clusters should have the most triangles.

USE cypher_demo
CALL algo.triangleCount()
YIELD nodeId, triangleCount
RETURN nodeId, triangleCount
ORDER BY triangleCount DESC;


-- ============================================================================
-- 20. DIRECTED REACHABILITY — Strongly connected components
-- ============================================================================
-- SCCs are groups where everyone can reach everyone else via directed
-- paths. A large SCC means good bidirectional communication. Many small
-- SCCs indicate one-way information flow (top-down only).

USE cypher_demo
CALL algo.scc()
YIELD nodeId, componentId
RETURN componentId, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- PART 5: PATHFINDING — Route messages through the organization
-- ============================================================================


-- ============================================================================
-- 21. SHORTEST PATH — Fastest route between two distant employees
-- ============================================================================
-- If Alice needs to get an urgent message to employee #42 (probably in
-- a different department), what's the fastest chain of introductions?

USE cypher_demo
CALL algo.shortestPath({source: 1, target: 42})
YIELD nodeId, step, distance
RETURN nodeId, step, distance
ORDER BY step;


-- ============================================================================
-- 22. ALL DISTANCES — How far is Alice from everyone?
-- ============================================================================
-- Maps Alice's distance to every other employee. Nearby people (1-2 hops)
-- are in her inner circle; distant people (3+ hops) may never hear her
-- ideas unless bridges carry them.

USE cypher_demo
CALL algo.allShortestPaths({source: 1})
YIELD nodeId, distance, path
RETURN nodeId, distance, path
ORDER BY distance;


-- ============================================================================
-- 23. BFS — How does news spread level by level?
-- ============================================================================
-- Breadth-first search from Alice shows how information radiates outward:
-- first her direct contacts, then their contacts, and so on. In a
-- well-connected startup, everyone should be reachable within 3-4 levels.

USE cypher_demo
CALL algo.bfs({source: 1})
YIELD nodeId, depth, parentId
RETURN depth, count(*) AS people_at_distance
ORDER BY depth;


-- ============================================================================
-- 24. DFS — Deep exploration from Alice
-- ============================================================================
-- Depth-first search explores as deep as possible before backtracking.
-- Discovery and finish times reveal the tree structure of how Alice's
-- influence spreads through chains of contacts.

USE cypher_demo
CALL algo.dfs({source: 1})
YIELD nodeId, discoveryTime, finishTime, parentId
RETURN nodeId, discoveryTime, finishTime, parentId
ORDER BY discoveryTime;


-- ============================================================================
-- 25. BACKBONE NETWORK — Minimum connections to keep everyone linked
-- ============================================================================
-- The minimum spanning tree is the lightest set of edges that still
-- connects every employee. This reveals the organizational skeleton —
-- the essential connections without which people become isolated.

USE cypher_demo
CALL algo.mst()
YIELD sourceId, targetId, weight
RETURN sourceId, targetId, weight
ORDER BY weight;


-- ============================================================================
-- PART 6: SIMILARITY — Find structurally similar people
-- ============================================================================


-- ============================================================================
-- 26. NEAREST NEIGHBORS — Who is most like Alice?
-- ============================================================================
-- KNN finds the K structurally similar people based on shared neighbors.
-- Useful for succession planning: if Alice leaves, who could fill her
-- network role?

USE cypher_demo
CALL algo.knn({node: 1, k: 5})
YIELD neighborId, similarity, rank
RETURN neighborId, similarity, rank
ORDER BY rank;


-- ============================================================================
-- 27. PAIRWISE SIMILARITY — How alike are two specific employees?
-- ============================================================================
-- Jaccard similarity compares shared neighbors. High score = these two
-- people know the same people and could substitute for each other in
-- cross-team coordination roles.

USE cypher_demo
CALL algo.similarity({node1: 1, node2: 13, metric: 'jaccard'})
YIELD node1Id, node2Id, score
RETURN node1Id, node2Id, score;
