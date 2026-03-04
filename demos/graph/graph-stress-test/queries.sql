-- ############################################################################
-- ############################################################################
--
--   ENTERPRISE ORGANIZATION NETWORK — 1M EMPLOYEES / 5M+ CONNECTIONS
--   Real-World HR & Organizational Analytics Queries
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
-- PART 1: SQL ANALYTICS (queries 1–21)
--   Workforce planning, organizational health, and network analysis
--   using standard SQL joins and aggregations.
--
-- PART 2: CYPHER GRAPH ALGORITHMS (queries 22–38)
--   Influence mapping, community detection, and path analysis using
--   Cypher pattern matching and built-in graph algorithms.
--
-- PART 3: GRAPH VISUALIZATION (queries 39–50)
--   Progressive scale tests for the graph visualizer, framed as
--   real departmental and cross-office network maps.
--
-- ############################################################################


-- ############################################################################
-- PART 1: SQL ANALYTICS — Workforce & Organizational Network
-- ############################################################################


-- ============================================================================
-- 1. DATA INTEGRITY CHECK — Verify the organization loaded correctly
-- ============================================================================
-- Before running analytics, confirm the dataset is complete:
-- 1M employees, 20 departments, and 5M+ connections.

SELECT 'node_count' AS check_name,
       COUNT(*) AS actual,
       1000000 AS expected,
       CASE WHEN COUNT(*) = 1000000 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.graph.st_people;


-- ============================================================================
-- 2. NETWORK SIZE — How large is the organization's connection graph?
-- ============================================================================
-- Understanding the total edge count tells us the overall connectivity
-- density. A healthy org of 1M people should have 5+ connections per person.

SELECT 'edge_count' AS check_name,
       COUNT(*) AS actual,
       CASE WHEN COUNT(*) >= 4000000 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.graph.st_edges;


-- ============================================================================
-- 3. WORKFORCE PLANNING — Headcount & demographics by department
-- ============================================================================
-- HR needs a snapshot of each department: how many people, average age,
-- how many are active, and how geographically spread the team is.
-- Departments with low active ratios or few cities may need attention.

SELECT
    department,
    COUNT(*) AS headcount,
    ROUND(AVG(age), 1) AS avg_age,
    COUNT(*) FILTER (WHERE active) AS active_count,
    COUNT(DISTINCT city) AS office_locations
FROM {{zone_name}}.graph.st_people
GROUP BY department
ORDER BY headcount DESC;


-- ============================================================================
-- 4. CONNECTION TYPES — What kinds of relationships exist?
-- ============================================================================
-- Understanding the mix of relationship types reveals how people connect.
-- A healthy org has diverse connection types — not just within-team links.
-- High-weight relationships (colleagues, mentors) vs. low-weight (referrals)
-- show where strong vs. weak ties concentrate.

SELECT
    relationship_type,
    COUNT(*) AS edge_count,
    ROUND(AVG(weight), 3) AS avg_strength,
    ROUND(MIN(weight), 3) AS min_strength,
    ROUND(MAX(weight), 3) AS max_strength,
    MIN(since_year) AS earliest,
    MAX(since_year) AS latest
FROM {{zone_name}}.graph.st_edges
GROUP BY relationship_type
ORDER BY edge_count DESC;


-- ============================================================================
-- 5. GLOBAL FOOTPRINT — Employee distribution across offices
-- ============================================================================
-- Which offices are largest? How many departments and project teams
-- operate from each city? Cities with many departments but few teams
-- may have coordination challenges.

SELECT
    city,
    COUNT(*) AS headcount,
    COUNT(DISTINCT department) AS departments_present,
    COUNT(DISTINCT project_team) AS teams_present,
    ROUND(AVG(age), 1) AS avg_age
FROM {{zone_name}}.graph.st_people
GROUP BY city
ORDER BY headcount DESC;


-- ============================================================================
-- 6. KEY INFLUENCERS — Top 25 most-connected employees
-- ============================================================================
-- Who has the most connections in the organization? These people are the
-- informal hubs — their departure would disrupt the most workflows.
-- Expect VPs and Directors here due to the power-law degree structure.

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
-- 7. CONNECTIVITY HEALTH — How well-connected is the average employee?
-- ============================================================================
-- A degree distribution histogram reveals organizational health.
-- Too many isolated nodes (0 connections) = siloed org.
-- Heavy tail (50+ connections) = healthy hub structure.
-- Most employees should have 3-20 connections.

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
    COUNT(*) AS employee_count
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
-- 8. CROSS-DEPARTMENT COLLABORATION — Which departments work together most?
-- ============================================================================
-- Before a reorg, leadership wants to know which departments are most
-- interdependent. High cross-department connections mean those teams
-- should stay close organizationally. Low connections may indicate silos.

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
-- 9. DEPARTMENT COHESION — Which departments are most internally connected?
-- ============================================================================
-- Internal cohesion shows how well a department collaborates within itself.
-- High intra-department connections with high average weight = strong team.
-- Low numbers may indicate the department is too spread out or siloed
-- within itself.

SELECT
    src_dept AS department,
    connection_count AS internal_connections,
    avg_weight AS avg_strength,
    rel_type_count AS relationship_types
FROM {{zone_name}}.graph.st_dept_matrix
WHERE src_dept = dst_dept
ORDER BY connection_count DESC;


-- ============================================================================
-- 10. CROSS-OFFICE COLLABORATION — How well do offices work together?
-- ============================================================================
-- For remote work policy decisions: which city pairs collaborate most?
-- High connections between cities suggest distributed teams that need
-- good video conferencing and travel budgets. Low connections between
-- large offices may indicate isolation.

SELECT
    src_p.city AS from_city,
    dst_p.city AS to_city,
    COUNT(*) AS connections,
    ROUND(AVG(e.weight), 3) AS avg_strength,
    COUNT(DISTINCT e.relationship_type) AS relationship_types
FROM {{zone_name}}.graph.st_edges e
JOIN {{zone_name}}.graph.st_people src_p ON e.src = src_p.id
JOIN {{zone_name}}.graph.st_people dst_p ON e.dst = dst_p.id
GROUP BY src_p.city, dst_p.city
ORDER BY connections DESC
LIMIT 25;


-- ============================================================================
-- 11. BRIDGE EMPLOYEES — People connecting 10+ departments
-- ============================================================================
-- These cross-functional connectors are the glue of the organization.
-- If they leave, departments lose their inter-team communication channels.
-- HR should ensure these bridge employees are engaged and retained.

SELECT
    p.name,
    p.department AS home_dept,
    p.city,
    p.level,
    COUNT(DISTINCT dst_p.department) AS departments_reached,
    COUNT(*) AS total_outgoing
FROM {{zone_name}}.graph.st_edges e
JOIN {{zone_name}}.graph.st_people p ON e.src = p.id
JOIN {{zone_name}}.graph.st_people dst_p ON e.dst = dst_p.id
WHERE p.department != dst_p.department
GROUP BY p.name, p.department, p.city, p.level
HAVING COUNT(DISTINCT dst_p.department) >= 10
ORDER BY departments_reached DESC, total_outgoing DESC
LIMIT 25;


-- ============================================================================
-- 12. BLAST RADIUS — If employee #1 leaves, how many people are affected?
-- ============================================================================
-- When a key person departs, their direct contacts (1-hop) feel it
-- immediately. Their contacts' contacts (2-hop) feel the ripple effect.
-- This measures the "blast radius" of losing a single employee.

SELECT
    '1-hop (direct impact)' AS reach,
    COUNT(DISTINCT dst) AS people_affected
FROM {{zone_name}}.graph.st_edges
WHERE src = 1
UNION ALL
SELECT
    '2-hop (ripple effect)' AS reach,
    COUNT(DISTINCT e2.dst) AS people_affected
FROM {{zone_name}}.graph.st_edges e1
JOIN {{zone_name}}.graph.st_edges e2 ON e1.dst = e2.src
WHERE e1.src = 1
  AND e2.dst != 1;


-- ============================================================================
-- 13. MENTORSHIP GAPS — Are senior people mentoring the right levels?
-- ============================================================================
-- A healthy mentorship program has senior employees (L5+) mentoring
-- people 1-2 levels below them. If VPs only mentor other VPs, the
-- program isn't reaching junior staff. This matrix shows where the
-- mentorship connections actually flow.

SELECT
    src_p.level AS mentor_level,
    dst_p.level AS mentee_level,
    COUNT(*) AS mentorship_count,
    ROUND(AVG(e.weight), 3) AS avg_strength
FROM {{zone_name}}.graph.st_edges e
JOIN {{zone_name}}.graph.st_people src_p ON e.src = src_p.id
JOIN {{zone_name}}.graph.st_people dst_p ON e.dst = dst_p.id
WHERE e.relationship_type = 'mentor'
GROUP BY src_p.level, dst_p.level
ORDER BY mentorship_count DESC;


-- ============================================================================
-- 14. TEAM vs CROSS-TEAM — Is the org collaborating or siloed?
-- ============================================================================
-- If most connections are within-team, people are siloed in their pods.
-- A healthy org has a mix: strong within-team bonds AND substantial
-- cross-team collaboration. This query measures the ratio.

SELECT
    'within_team' AS scope,
    COUNT(*) AS edge_count,
    ROUND(AVG(e.weight), 3) AS avg_strength
FROM {{zone_name}}.graph.st_edges e
JOIN {{zone_name}}.graph.st_people src_p ON e.src = src_p.id
JOIN {{zone_name}}.graph.st_people dst_p ON e.dst = dst_p.id
WHERE src_p.project_team = dst_p.project_team
UNION ALL
SELECT
    'cross_team' AS scope,
    COUNT(*) AS edge_count,
    ROUND(AVG(e.weight), 3) AS avg_strength
FROM {{zone_name}}.graph.st_edges e
JOIN {{zone_name}}.graph.st_people src_p ON e.src = src_p.id
JOIN {{zone_name}}.graph.st_people dst_p ON e.dst = dst_p.id
WHERE src_p.project_team != dst_p.project_team;


-- ============================================================================
-- 15. RECIPROCAL RELATIONSHIPS — How many connections are mutual?
-- ============================================================================
-- If A connects to B, does B also connect to A? High reciprocity means
-- balanced relationships. Low reciprocity may indicate one-directional
-- information flow (e.g., managers pushing down, not listening up).

SELECT
    COUNT(*) AS mutual_pairs
FROM {{zone_name}}.graph.st_edges e1
JOIN {{zone_name}}.graph.st_edges e2
    ON e1.src = e2.dst AND e1.dst = e2.src
WHERE e1.src < e1.dst;


-- ============================================================================
-- 16. ORGANIZATIONAL GROWTH — When did connections form?
-- ============================================================================
-- Tracking connection formation over time shows organizational growth
-- patterns. A spike in new connections after a merger or reorg is normal.
-- Declining new connections may signal cultural stagnation.

SELECT
    since_year,
    COUNT(*) AS new_connections,
    COUNT(DISTINCT src) AS people_connecting,
    COUNT(DISTINCT dst) AS people_reached,
    ROUND(AVG(weight), 3) AS avg_strength
FROM {{zone_name}}.graph.st_edges
GROUP BY since_year
ORDER BY since_year;


-- ============================================================================
-- 17. SENIORITY FLOW — How does information flow across levels?
-- ============================================================================
-- Do senior people mostly connect to other seniors (echo chamber), or
-- do connections span levels? This level-to-level matrix reveals whether
-- the hierarchy facilitates or blocks information flow.

SELECT
    src_p.level AS from_level,
    dst_p.level AS to_level,
    COUNT(*) AS connections,
    ROUND(AVG(e.weight), 3) AS avg_strength
FROM {{zone_name}}.graph.st_edges e
JOIN {{zone_name}}.graph.st_people src_p ON e.src = src_p.id
JOIN {{zone_name}}.graph.st_people dst_p ON e.dst = dst_p.id
GROUP BY src_p.level, dst_p.level
ORDER BY connections DESC
LIMIT 20;


-- ============================================================================
-- 18. REGIONAL COLLABORATION — Do geographic regions work together?
-- ============================================================================
-- With departments spread across Americas, EMEA, and APAC regions,
-- this shows cross-regional collaboration health. Low inter-region
-- connections may require travel budgets or timezone-overlap policies.

SELECT
    sd.region AS from_region,
    dd.region AS to_region,
    COUNT(*) AS connections,
    ROUND(AVG(e.weight), 3) AS avg_strength
FROM {{zone_name}}.graph.st_edges e
JOIN {{zone_name}}.graph.st_people src_p ON e.src = src_p.id
JOIN {{zone_name}}.graph.st_people dst_p ON e.dst = dst_p.id
JOIN {{zone_name}}.graph.st_departments sd ON src_p.department = sd.dept_name
JOIN {{zone_name}}.graph.st_departments dd ON dst_p.department = dd.dept_name
GROUP BY sd.region, dd.region
ORDER BY connections DESC;


-- ============================================================================
-- 19. INFORMAL INFLUENCE — Who has outsized influence despite low title?
-- ============================================================================
-- Approximated PageRank: people who are connected-to by many well-connected
-- people have outsized influence. Sometimes an L2 Associate is more
-- influential than a Director because of who they know.
-- This finds those "hidden influencers" the org chart doesn't show.

SELECT
    p.id,
    p.name,
    p.department,
    p.city,
    p.level,
    ROUND(SUM(1.0 / GREATEST(src_stats.out_degree, 1)), 4) AS influence_score
FROM {{zone_name}}.graph.st_edges e
JOIN {{zone_name}}.graph.st_people p ON e.dst = p.id
JOIN {{zone_name}}.graph.st_people_stats src_stats ON e.src = src_stats.id
GROUP BY p.id, p.name, p.department, p.city, p.level
ORDER BY influence_score DESC
LIMIT 25;


-- ============================================================================
-- 20. ORGANIZATION DASHBOARD — Full network metrics at a glance
-- ============================================================================
-- Executive summary: one row with all key metrics for the organization.
-- Use this as the top-level health dashboard before drilling into details.

SELECT
    (SELECT COUNT(*)                    FROM {{zone_name}}.graph.st_people) AS total_employees,
    (SELECT COUNT(*)                    FROM {{zone_name}}.graph.st_edges)  AS total_connections,
    (SELECT COUNT(DISTINCT department)  FROM {{zone_name}}.graph.st_people) AS departments,
    (SELECT COUNT(DISTINCT city)        FROM {{zone_name}}.graph.st_people) AS offices,
    (SELECT COUNT(DISTINCT project_team) FROM {{zone_name}}.graph.st_people) AS project_teams,
    (SELECT COUNT(DISTINCT relationship_type) FROM {{zone_name}}.graph.st_edges) AS relationship_types,
    (SELECT ROUND(AVG(weight), 3)       FROM {{zone_name}}.graph.st_edges)  AS avg_connection_strength,
    (SELECT ROUND(
        CAST((SELECT COUNT(*) FROM {{zone_name}}.graph.st_edges) AS DOUBLE)
        / CAST((SELECT COUNT(*) FROM {{zone_name}}.graph.st_people) AS DOUBLE)
    , 1)) AS avg_connections_per_person;


-- ============================================================================
-- 21. FULL HEALTH CHECK — Validate all data integrity constraints
-- ============================================================================
-- Run all validation checks in one query. Every check should return PASS.
-- If any return FAIL, the dataset may have been corrupted or partially loaded.

SELECT 'employee_count_1M' AS check_name,
       CASE WHEN COUNT(*) = 1000000 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.graph.st_people
UNION ALL
SELECT 'department_count_20',
       CASE WHEN COUNT(*) = 20 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.st_departments
UNION ALL
SELECT 'connection_count_min_4M',
       CASE WHEN COUNT(*) >= 4000000 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.st_edges
UNION ALL
SELECT 'has_mentor_relationships',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.st_edges WHERE relationship_type = 'mentor'
UNION ALL
SELECT 'has_colleague_relationships',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.st_edges WHERE relationship_type = 'colleague'
UNION ALL
SELECT 'fifteen_office_locations',
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
SELECT 'active_employees_gt_900K',
       CASE WHEN COUNT(*) FILTER (WHERE active) >= 900000 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.st_people
UNION ALL
SELECT 'connection_weights_normalized',
       CASE WHEN MIN(weight) >= 0.0 AND MAX(weight) <= 1.0 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.st_edges
UNION ALL
SELECT 'no_self_connections',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.st_edges WHERE src = dst
ORDER BY check_name;


-- ############################################################################
-- ############################################################################
--
-- PART 2: CYPHER GRAPH ALGORITHMS — Influence, Communities & Paths
--
-- ############################################################################
-- ############################################################################
-- These queries use the Cypher graph query language and built-in graph
-- algorithms to uncover patterns invisible to SQL: community structure,
-- influence ranking, shortest paths, and centrality metrics.
-- ############################################################################


-- ============================================================================
-- CYPHER 22. ORGANIZATION SIZE — Verify graph loaded all 1M employees
-- ============================================================================
-- Quick sanity check: does the graph engine see all 1 million employees?

USE stress_test_network
MATCH (n)
RETURN count(n) AS total_employees;


-- ============================================================================
-- CYPHER 23. TOTAL CONNECTIONS — Full edge scan at 5M+ scale
-- ============================================================================
-- How many directed connections exist? This tests the graph engine's
-- ability to scan all 5M+ edges efficiently.

USE stress_test_network
MATCH (a)-[r]->(b)
RETURN count(r) AS total_connections;


-- ============================================================================
-- CYPHER 24. ENGINEERING VETERANS — Find senior engineers over 50
-- ============================================================================
-- HR is planning a "senior engineers" mentorship program. Find the most
-- experienced Engineering department members who are over 50 years old —
-- potential mentors for the next generation.

USE stress_test_network
MATCH (n)
WHERE n.department = 'Engineering' AND n.age > 50
RETURN n.name AS name, n.age AS age, n.city AS city
ORDER BY n.age DESC
LIMIT 25;


-- ============================================================================
-- CYPHER 25. STRONGEST MENTORSHIPS — High-impact mentor relationships
-- ============================================================================
-- Which mentor-mentee pairs have the strongest bonds (weight > 0.8)?
-- These are the mentorships that are really working — study them to
-- understand what makes effective mentoring in this organization.

USE stress_test_network
MATCH (a)-[r]->(b)
WHERE r.relationship_type = 'mentor' AND r.weight > 0.8
RETURN a.name AS mentor, b.name AS mentee, r.weight AS strength
ORDER BY r.weight DESC
LIMIT 25;


-- ============================================================================
-- CYPHER 26. KNOWLEDGE PATHS — How does information reach employee #1?
-- ============================================================================
-- Trace the 2-hop information paths from a specific employee. This shows
-- how knowledge flows: who does person #1 talk to, and who do those
-- people talk to? Critical for understanding information propagation.

USE stress_test_network
MATCH (a)-[]->(b)-[]->(c)
WHERE a.id = 1
RETURN a.name AS source, b.name AS relay, c.name AS reached
LIMIT 50;


-- ============================================================================
-- CYPHER 27. REACHABILITY — Who can employee #1 reach within 2 hops?
-- ============================================================================
-- For a critical announcement or project staffing, how many unique people
-- can one employee reach through their network within 2 degrees?
-- This measures an individual's "network reach."

USE stress_test_network
MATCH (a)-[*1..2]->(b)
WHERE a.id = 1 AND a <> b
RETURN DISTINCT b.name AS reachable_person, b.department AS dept
LIMIT 50;


-- ============================================================================
-- CYPHER 28. DEGREE CENTRALITY — Who are the most connected people?
-- ============================================================================
-- Degree centrality measures raw connection count. High in-degree means
-- many people reach out to you. High out-degree means you actively
-- connect to many. Total degree identifies the organizational hubs.

USE stress_test_network
CALL algo.degree()
YIELD nodeId, inDegree, outDegree, totalDegree
RETURN nodeId, inDegree, outDegree, totalDegree
ORDER BY totalDegree DESC
LIMIT 25;


-- ============================================================================
-- CYPHER 29. PAGERANK — Find the most influential employees
-- ============================================================================
-- PageRank goes beyond raw connections: it measures influence by
-- considering who connects to whom. An employee connected to by many
-- well-connected people ranks higher than one connected to by many
-- isolated people. This reveals the true organizational power structure.

USE stress_test_network
CALL algo.pageRank({dampingFactor: 0.85, iterations: 5})
YIELD nodeId, score, rank
RETURN nodeId, score, rank
ORDER BY score DESC
LIMIT 25;


-- ============================================================================
-- CYPHER 30. NATURAL COMMUNITIES — Does the graph reflect our org chart?
-- ============================================================================
-- Connected components analysis reveals naturally isolated groups.
-- In a healthy org, there should be one giant component (everyone is
-- reachable from everyone). Multiple components indicate truly disconnected
-- groups that need cross-team initiatives.

USE stress_test_network
CALL algo.connectedComponents()
YIELD nodeId, componentId
RETURN componentId, count(*) AS community_size
ORDER BY community_size DESC
LIMIT 25;


-- ============================================================================
-- CYPHER 31. LOUVAIN COMMUNITIES — Find the real organizational clusters
-- ============================================================================
-- Louvain community detection finds dense subgroups regardless of the
-- formal org chart. If communities align with departments, the org
-- structure matches reality. If not, people are self-organizing
-- differently than management intended.

USE stress_test_network
CALL algo.louvain({resolution: 1.0})
YIELD nodeId, communityId
RETURN communityId, count(*) AS size
ORDER BY size DESC
LIMIT 25;


-- ============================================================================
-- CYPHER 32. GATEKEEPERS — Who controls information flow between groups?
-- ============================================================================
-- Betweenness centrality identifies employees who sit on many shortest
-- paths between others. These "gatekeepers" control information flow.
-- If they leave, communication between groups breaks down. Critical
-- for succession planning and retention strategy.

USE stress_test_network
CALL algo.betweenness()
YIELD nodeId, centrality, rank
RETURN nodeId, centrality, rank
ORDER BY centrality DESC
LIMIT 25;


-- ============================================================================
-- CYPHER 33. TIGHT-KNIT GROUPS — Find clusters of mutual connections
-- ============================================================================
-- Triangle count measures how many groups-of-three are fully connected.
-- Employees in many triangles are embedded in tight-knit groups with high
-- trust and information sharing. Low triangle count = loose acquaintance.

USE stress_test_network
CALL algo.triangleCount()
YIELD nodeId, triangleCount
RETURN nodeId, triangleCount
ORDER BY triangleCount DESC
LIMIT 25;


-- ============================================================================
-- CYPHER 34. FASTEST PATH — Shortest route between two distant employees
-- ============================================================================
-- If employee #1 (in department 1) needs to reach employee #500000
-- (in a different department/city), what's the fastest path through
-- the organization? This tests Dijkstra's algorithm on the full 1M
-- node graph using weighted edges.

USE stress_test_network
CALL algo.shortestPath({source: 1, target: 500000})
YIELD nodeId, step, distance
RETURN nodeId, step, distance
ORDER BY step;


-- ============================================================================
-- CYPHER 35. SIX DEGREES — How far apart are people in this organization?
-- ============================================================================
-- BFS from employee #1: how many people are at each hop distance?
-- In a well-connected org, most people should be reachable within
-- 4-6 hops (the "small world" property).

USE stress_test_network
CALL algo.bfs({source: 1})
YIELD nodeId, depth, parentId
RETURN depth, count(*) AS people_at_distance
ORDER BY depth
LIMIT 20;


-- ============================================================================
-- CYPHER 36. DIRECTED REACHABILITY — Find one-way information silos
-- ============================================================================
-- Strongly connected components show groups where information flows
-- in both directions. A large SCC means good bidirectional communication.
-- Many small SCCs indicate one-way information flow (top-down only).

USE stress_test_network
CALL algo.scc()
YIELD nodeId, componentId
RETURN componentId, count(*) AS scc_size
ORDER BY scc_size DESC
LIMIT 25;


-- ============================================================================
-- CYPHER 37. ACCESSIBILITY — Who is easiest to reach in the organization?
-- ============================================================================
-- Closeness centrality measures how close an employee is to everyone else.
-- High closeness = this person can spread information quickly. Low
-- closeness = isolated, hard to reach. Good candidates for company-wide
-- announcements or change agents.

USE stress_test_network
CALL algo.closeness()
YIELD nodeId, closeness, rank
RETURN nodeId, closeness, rank
ORDER BY closeness DESC
LIMIT 25;


-- ============================================================================
-- CYPHER 38. BACKBONE NETWORK — Minimum connections to keep everyone linked
-- ============================================================================
-- The minimum spanning tree is the lightest set of edges that still
-- connects every employee. This reveals the "skeleton" of the
-- organization — the essential connections that, if removed, would
-- fragment the network.

USE stress_test_network
CALL algo.mst()
YIELD sourceId, targetId, weight
RETURN count(*) AS backbone_edges, sum(weight) AS total_weight;


-- ############################################################################
-- ############################################################################
--
-- PART 3: GRAPH VISUALIZATION — Progressive Scale Tests
--
-- ############################################################################
-- ############################################################################
-- These queries return actual node + edge data for rendering in the graph
-- visualizer. Run them in order to find the performance ceiling.
--
-- Each query is framed as a real use case — visualizing a specific slice
-- of the organization network. Scale increases progressively:
--
--   39.  Single team (~100 nodes)        — should render instantly
--   40.  Small department slice (500)     — fast render
--   41.  Full department unit (1,000)     — smooth render
--   42.  Multi-team view (5,000)          — may need layout time
--   43.  Division-level (10,000)          — stress test begins
--   44.  Regional network (50,000)        — expect lag
--   45.  Large region (100,000)           — browser stress
--   46.  Full organization (1M + 5M)      — ultimate stress test
--
-- Cypher node-only visualization:
--   47.  100 nodes    — node layout test
--   48.  1,000 nodes  — medium density
--   49.  10,000 nodes — large node cloud
--   50.  All 1M nodes — extreme test
-- ############################################################################


-- ============================================================================
-- 39. VIZ: SINGLE PROJECT TEAM — Visualize one team's internal network
-- ============================================================================
-- A team lead wants to see how their ~100-person team is connected.
-- This should render smoothly with clear cluster structure.

USE stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 100 AND b.id <= 100
RETURN a, r, b;


-- ============================================================================
-- 40. VIZ: DEPARTMENT SLICE — Engineering team connections (500 people)
-- ============================================================================
-- An Engineering manager wants to see how the first 500 employees connect.
-- Small enough to see individual node labels clearly.

USE stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 500 AND b.id <= 500
RETURN a, r, b;


-- ============================================================================
-- 41. VIZ: FULL DEPARTMENT UNIT — 1,000 employees with all connections
-- ============================================================================
-- A department head visualizes their entire unit. At this scale, clusters
-- (project teams) should become visible as dense subgroups.

USE stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 1000 AND b.id <= 1000
RETURN a, r, b;


-- ============================================================================
-- 42. VIZ: MULTI-TEAM VIEW — 5,000 employees across several teams
-- ============================================================================
-- A director looks at connections across multiple project teams.
-- Bridge nodes connecting teams should be visible at this scale.

USE stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 5000 AND b.id <= 5000
RETURN a, r, b;


-- ============================================================================
-- 43. VIZ: DIVISION — 10,000 employees, stress test begins
-- ============================================================================
-- A VP visualizes their entire division. At this scale, the graph
-- layout algorithm is doing significant work. Department clusters
-- and inter-department bridges should be clearly visible.

USE stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 10000 AND b.id <= 10000
RETURN a, r, b;


-- ============================================================================
-- 44. VIZ: REGIONAL NETWORK — 50,000 employees, extreme rendering
-- ============================================================================
-- WARNING: This will produce a large result set. The graph visualizer
-- may become sluggish. This tests rendering performance at regional scale.

USE stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 50000 AND b.id <= 50000
RETURN a, r, b;


-- ============================================================================
-- 45. VIZ: LARGE REGION — 100,000 employees, browser stress test
-- ============================================================================
-- WARNING: Expect significant lag or memory pressure at this scale.
-- Tests whether the visualizer can handle a large regional office map.

USE stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 100000 AND b.id <= 100000
RETURN a, r, b;


-- ============================================================================
-- 46. VIZ: FULL ORGANIZATION — All 1M employees + all 5M connections
-- ============================================================================
-- WARNING: This returns the entire organization graph. The visualizer
-- will almost certainly crash or freeze. This is the ultimate stress test
-- to find the rendering engine's absolute limits.

USE stress_test_network
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- 47. VIZ NODES: SINGLE TEAM — 100 employee nodes (layout warm-up)
-- ============================================================================
-- Test node-only rendering with a small team. No edges — just how the
-- visualizer handles positioning 100 nodes.

USE stress_test_network
MATCH (n)
WHERE n.id <= 100
RETURN n;


-- ============================================================================
-- 48. VIZ NODES: DEPARTMENT — 1,000 employee nodes
-- ============================================================================
-- A department's worth of nodes. Tests the layout algorithm's ability
-- to arrange nodes without overlapping labels.

USE stress_test_network
MATCH (n)
WHERE n.id <= 1000
RETURN n;


-- ============================================================================
-- 49. VIZ NODES: DIVISION — 10,000 employee nodes
-- ============================================================================
-- Large node cloud test. At this scale, individual labels are not readable,
-- but cluster patterns should be visible.

USE stress_test_network
MATCH (n)
WHERE n.id <= 10000
RETURN n;


-- ============================================================================
-- 50. VIZ NODES: FULL ORGANIZATION — All 1M employee nodes
-- ============================================================================
-- WARNING: Returns 1,000,000 node objects. Ultimate node rendering test.
-- This measures the visualizer's limits for node-only rendering.

USE stress_test_network
MATCH (n)
RETURN n;
