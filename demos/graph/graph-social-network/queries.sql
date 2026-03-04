-- ############################################################################
-- ############################################################################
--
--   STARTUP SOCIAL NETWORK — 100 EMPLOYEES / ~300 CONNECTIONS
--   Organizational Network Analytics & Graph Insights
--
-- ############################################################################
-- ############################################################################
--
-- This demo models a 100-person startup with 8 departments across 5 cities.
-- The graph has realistic community structure: departments form tight
-- clusters, cities create cross-department bonds, and a few bridge
-- employees connect the organizational silos.
--
-- PART 1: SQL ANALYTICS (queries 1–17)
--   Workforce insights, team health, and network analysis.
--
-- PART 2: CYPHER GRAPH QUERIES (queries 18–27)
--   Pattern matching, graph algorithms, and path analysis.
--
-- PART 3: GRAPH VISUALIZATION (queries 28–30)
--   Visual exploration of the company network.
--
-- ############################################################################


-- ############################################################################
-- PART 1: SQL ANALYTICS — Workforce & Network Health
-- ############################################################################


-- ============================================================================
-- 1. DATA INTEGRITY — Verify the company dataset loaded correctly
-- ============================================================================
-- Quick check: do we have all 100 employees? This should always return PASS.

SELECT 'employee_count' AS check_name,
       COUNT(*) AS actual,
       100 AS expected,
       CASE WHEN COUNT(*) = 100 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.graph.employees;


-- ============================================================================
-- 2. TEAM SNAPSHOT — Headcount and demographics by department
-- ============================================================================
-- The CEO wants a quick overview: how large is each team, what's the
-- average age, and are there any departments with low active headcount?
-- Departments with fewer active employees may need hiring attention.

SELECT
    department,
    COUNT(*) AS headcount,
    ROUND(AVG(age), 1) AS avg_age,
    COUNT(*) FILTER (WHERE active) AS active_count
FROM {{zone_name}}.graph.employees
GROUP BY department
ORDER BY headcount DESC;


-- ============================================================================
-- 3. NETWORK SIZE — How connected is this startup?
-- ============================================================================
-- A healthy 100-person company should have at least 200 connections
-- (2+ per person on average). Fewer suggests siloed teams.

SELECT 'connection_count' AS check_name,
       COUNT(*) AS actual,
       CASE WHEN COUNT(*) >= 200 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.graph.connections;


-- ============================================================================
-- 4. RELATIONSHIP MIX — What types of bonds hold the company together?
-- ============================================================================
-- Understanding the mix of relationship types reveals organizational health.
-- A startup that's all "colleagues" with no "mentors" or "cross-dept-bridges"
-- may lack knowledge transfer and cross-pollination.

SELECT
    relationship_type,
    COUNT(*) AS count,
    ROUND(AVG(weight), 2) AS avg_strength,
    MIN(since_year) AS earliest,
    MAX(since_year) AS latest
FROM {{zone_name}}.graph.connections
GROUP BY relationship_type
ORDER BY count DESC;


-- ============================================================================
-- 5. KEY PEOPLE — Who are the most connected employees?
-- ============================================================================
-- These are the informal hubs of the organization. If any of them leave,
-- information flow breaks down. The CEO should know who these people are
-- and ensure they're engaged and retained.

SELECT
    id,
    name,
    department,
    level,
    out_degree,
    in_degree,
    total_degree
FROM {{zone_name}}.graph.employee_stats
ORDER BY total_degree DESC
LIMIT 10;


-- ============================================================================
-- 6. CROSS-TEAM COLLABORATION — Which departments talk to each other?
-- ============================================================================
-- Before a reorg, leadership needs to know which teams are already
-- collaborating. High connection counts between departments suggest
-- natural alignment. Low counts may indicate missed opportunities.

SELECT
    src_dept,
    dst_dept,
    connection_count,
    avg_weight
FROM {{zone_name}}.graph.dept_connections
WHERE src_dept != dst_dept
ORDER BY connection_count DESC
LIMIT 15;


-- ============================================================================
-- 7. TEAM COHESION — How tightly connected is each department internally?
-- ============================================================================
-- Departments with high internal connections and strong average weight
-- are well-bonded teams. Low numbers suggest the team may need more
-- face-to-face time or team-building activities.

SELECT
    src_dept AS department,
    connection_count AS internal_connections,
    avg_weight AS avg_strength
FROM {{zone_name}}.graph.dept_connections
WHERE src_dept = dst_dept
ORDER BY connection_count DESC;


-- ============================================================================
-- 8. OFFICE NETWORK — How well do our 5 offices collaborate?
-- ============================================================================
-- With employees in NYC, SF, Chicago, London, and Berlin, the company
-- needs to ensure remote offices aren't isolated. This shows which
-- city pairs have the strongest connections — and which are disconnected.

SELECT
    src_e.city AS from_city,
    dst_e.city AS to_city,
    COUNT(*) AS connections,
    ROUND(AVG(c.weight), 2) AS avg_strength
FROM {{zone_name}}.graph.connections c
JOIN {{zone_name}}.graph.employees src_e ON c.src = src_e.id
JOIN {{zone_name}}.graph.employees dst_e ON c.dst = dst_e.id
GROUP BY src_e.city, dst_e.city
ORDER BY connections DESC
LIMIT 15;


-- ============================================================================
-- 9. GO-TO PEOPLE — Who does everyone reach out to?
-- ============================================================================
-- High in-degree means many people actively connect TO this person.
-- These are the go-to experts, the people others seek out for help,
-- advice, or decisions. They may be bottlenecks if overloaded.

SELECT
    name,
    department,
    city,
    in_degree AS sought_by_count,
    level
FROM {{zone_name}}.graph.employee_stats
WHERE in_degree > 0
ORDER BY in_degree DESC
LIMIT 10;


-- ============================================================================
-- 10. DISENGAGED EMPLOYEES — Who has zero connections?
-- ============================================================================
-- Employees with no connections at all are either brand new, remote
-- without onboarding, or disengaged. HR should check on these people.
-- In a 100-person startup, even one isolated employee is a red flag.

SELECT
    id,
    name,
    department,
    city,
    level
FROM {{zone_name}}.graph.employee_stats
WHERE total_degree = 0
ORDER BY id;


-- ============================================================================
-- 11. INFORMATION FLOW — How far can employee #1 spread a message?
-- ============================================================================
-- If employee #1 shares important news, who hears it directly (1 hop),
-- and who hears it through the grapevine (2 hops)? This reveals how
-- quickly information propagates through the company.

SELECT
    'direct (1-hop)' AS reach,
    e.name,
    e.department
FROM {{zone_name}}.graph.connections c
JOIN {{zone_name}}.graph.employees e ON c.dst = e.id
WHERE c.src = 1
UNION ALL
SELECT DISTINCT
    'grapevine (2-hop)' AS reach,
    e2.name,
    e2.department
FROM {{zone_name}}.graph.connections c1
JOIN {{zone_name}}.graph.connections c2 ON c1.dst = c2.src
JOIN {{zone_name}}.graph.employees e2 ON c2.dst = e2.id
WHERE c1.src = 1
  AND c2.dst != 1
  AND c2.dst NOT IN (SELECT dst FROM {{zone_name}}.graph.connections WHERE src = 1)
ORDER BY reach, name;


-- ============================================================================
-- 12. DEPARTMENT HEALTH SCORE — Internal bond strength per team
-- ============================================================================
-- A high average internal weight with many active connectors means the
-- team is healthy and collaborative. Low weight or few connectors
-- suggests surface-level relationships — people work near each other
-- but don't truly collaborate.

SELECT
    src_e.department,
    COUNT(*) AS internal_bonds,
    ROUND(AVG(c.weight), 2) AS avg_bond_strength,
    COUNT(DISTINCT c.src) AS people_connecting
FROM {{zone_name}}.graph.connections c
JOIN {{zone_name}}.graph.employees src_e ON c.src = src_e.id
JOIN {{zone_name}}.graph.employees dst_e ON c.dst = dst_e.id
WHERE src_e.department = dst_e.department
GROUP BY src_e.department
ORDER BY internal_bonds DESC;


-- ============================================================================
-- 13. CROSS-FUNCTIONAL CONNECTORS — Who bridges the departmental silos?
-- ============================================================================
-- These employees connect to people in 3+ different departments.
-- In a startup, these bridge people are critical for preventing silos.
-- Without them, Engineering and Sales might never talk to each other.

SELECT
    e.name,
    e.department AS home_dept,
    e.level,
    COUNT(DISTINCT dst_e.department) AS departments_reached,
    COUNT(*) AS cross_dept_connections
FROM {{zone_name}}.graph.connections c
JOIN {{zone_name}}.graph.employees e ON c.src = e.id
JOIN {{zone_name}}.graph.employees dst_e ON c.dst = dst_e.id
WHERE e.department != dst_e.department
GROUP BY e.name, e.department, e.level
HAVING COUNT(DISTINCT dst_e.department) >= 3
ORDER BY departments_reached DESC, cross_dept_connections DESC;


-- ============================================================================
-- 14. MENTORSHIP MAP — Who is mentoring whom?
-- ============================================================================
-- Are Directors actually mentoring their teams, or is mentorship happening
-- informally? This shows every mentor-mentee pair, their levels, and
-- how strong the mentorship bond is. Cross-level mentorship (L6→L2)
-- is more valuable than peer mentoring (L4→L4).

SELECT
    src_e.name AS mentor,
    src_e.level AS mentor_level,
    dst_e.name AS mentee,
    dst_e.level AS mentee_level,
    c.weight AS bond_strength,
    c.since_year AS since
FROM {{zone_name}}.graph.connections c
JOIN {{zone_name}}.graph.employees src_e ON c.src = src_e.id
JOIN {{zone_name}}.graph.employees dst_e ON c.dst = dst_e.id
WHERE c.relationship_type = 'mentor'
ORDER BY c.weight DESC
LIMIT 15;


-- ============================================================================
-- 15. MUTUAL RELATIONSHIPS — Where do two people connect both ways?
-- ============================================================================
-- Reciprocal connections (A→B and B→A) are the strongest relationships.
-- These pairs genuinely collaborate — it's not one person reaching out
-- while the other ignores them. High mutual count = healthy culture.

SELECT
    e1.name AS person_a,
    e1.department AS dept_a,
    e2.name AS person_b,
    e2.department AS dept_b,
    c1.relationship_type AS a_to_b_type,
    c2.relationship_type AS b_to_a_type
FROM {{zone_name}}.graph.connections c1
JOIN {{zone_name}}.graph.connections c2
    ON c1.src = c2.dst AND c1.dst = c2.src
JOIN {{zone_name}}.graph.employees e1 ON c1.src = e1.id
JOIN {{zone_name}}.graph.employees e2 ON c1.dst = e2.id
WHERE c1.src < c1.dst
ORDER BY e1.name
LIMIT 15;


-- ============================================================================
-- 16. COMPANY DASHBOARD — All key metrics at a glance
-- ============================================================================
-- One-row executive summary. Use this as a health check dashboard.

SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.graph.employees) AS total_employees,
    (SELECT COUNT(*) FROM {{zone_name}}.graph.connections) AS total_connections,
    (SELECT COUNT(DISTINCT department) FROM {{zone_name}}.graph.employees) AS departments,
    (SELECT COUNT(DISTINCT city) FROM {{zone_name}}.graph.employees) AS offices,
    (SELECT COUNT(DISTINCT relationship_type) FROM {{zone_name}}.graph.connections) AS relationship_types,
    (SELECT ROUND(AVG(weight), 2) FROM {{zone_name}}.graph.connections) AS avg_bond_strength;


-- ============================================================================
-- 17. FULL HEALTH CHECK — All validation rules in one query
-- ============================================================================
-- Every check should return PASS. Any FAIL means the dataset was
-- corrupted or partially loaded.

SELECT 'employee_count' AS check_name,
       CASE WHEN COUNT(*) = 100 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.graph.employees
UNION ALL
SELECT 'department_count',
       CASE WHEN COUNT(*) = 8 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.departments
UNION ALL
SELECT 'connection_min_200',
       CASE WHEN COUNT(*) >= 200 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.connections
UNION ALL
SELECT 'has_mentor_edges',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.connections WHERE relationship_type = 'mentor'
UNION ALL
SELECT 'has_colleague_edges',
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.connections WHERE relationship_type = 'colleague'
UNION ALL
SELECT 'five_cities',
       CASE WHEN COUNT(DISTINCT city) = 5 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.employees
UNION ALL
SELECT 'eight_departments',
       CASE WHEN COUNT(DISTINCT department) = 8 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.employees
UNION ALL
SELECT 'active_employees_gt_80',
       CASE WHEN COUNT(*) FILTER (WHERE active) >= 80 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.employees
UNION ALL
SELECT 'weight_range_valid',
       CASE WHEN MIN(weight) >= 0.0 AND MAX(weight) <= 1.0 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.connections
UNION ALL
SELECT 'no_self_loops',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.connections WHERE src = dst
ORDER BY check_name;


-- ############################################################################
-- ############################################################################
--
-- PART 2: CYPHER GRAPH QUERIES — Patterns, Algorithms & Paths
--
-- ############################################################################
-- ############################################################################
-- These queries use Cypher pattern matching and built-in graph algorithms
-- to uncover insights invisible to SQL: influence ranking, community
-- structure, shortest paths, and centrality metrics.
-- ############################################################################


-- ============================================================================
-- CYPHER 18. THE FULL PICTURE — How many people and connections?
-- ============================================================================
-- Quick graph-level sanity check via Cypher.

USE social_network
MATCH (n)
RETURN count(n) AS total_employees;


-- ============================================================================
-- CYPHER 19. ALL CONNECTIONS — Full relationship scan
-- ============================================================================
-- Count every directed connection in the graph.

USE social_network
MATCH (a)-[r]->(b)
RETURN count(r) AS total_connections;


-- ============================================================================
-- CYPHER 20. ENGINEERING TEAM — Find all engineers and their roles
-- ============================================================================
-- The VP of Engineering wants to see everyone on the team. Cypher makes
-- property-based filtering natural and readable.

USE social_network
MATCH (n)
WHERE n.department = 'Engineering'
RETURN n.name AS name, n.title AS title, n.city AS city, n.level AS level
ORDER BY n.level DESC;


-- ============================================================================
-- CYPHER 21. STRONGEST MENTORSHIPS — High-impact mentor bonds
-- ============================================================================
-- Which mentor-mentee pairs have the strongest connection (weight > 0.8)?
-- These are the mentorships worth studying and replicating across teams.

USE social_network
MATCH (mentor)-[r]->(mentee)
WHERE r.relationship_type = 'mentor' AND r.weight > 0.8
RETURN mentor.name AS mentor, mentee.name AS mentee,
       mentor.level AS mentor_level, r.weight AS strength
ORDER BY r.weight DESC
LIMIT 10;


-- ============================================================================
-- CYPHER 22. KNOWLEDGE PATHS — 2-hop information flow from employee #1
-- ============================================================================
-- If employee #1 knows something important, this traces how it spreads:
-- who they tell directly, and who those people then tell.

USE social_network
MATCH (a)-[]->(b)-[]->(c)
WHERE a.id = 1
RETURN a.name AS source, b.name AS relay, c.name AS reached,
       b.department AS relay_dept, c.department AS reached_dept
LIMIT 25;


-- ============================================================================
-- CYPHER 23. PAGERANK — Who has the most organizational influence?
-- ============================================================================
-- PageRank reveals the truly influential people — not just those with
-- the most connections, but those connected to by other well-connected
-- people. In a startup, these are the informal leaders whose opinions
-- shape company culture and technical direction.

USE social_network
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD nodeId, score, rank
RETURN nodeId, score, rank
ORDER BY score DESC
LIMIT 10;


-- ============================================================================
-- CYPHER 24. NATURAL COMMUNITIES — Do teams match the org chart?
-- ============================================================================
-- Louvain community detection finds groups that naturally cluster together
-- based on actual connections — not the org chart. If communities match
-- departments, the org structure reflects reality. If not, people are
-- self-organizing differently than management expects.

USE social_network
CALL algo.louvain({resolution: 1.0})
YIELD nodeId, communityId
RETURN communityId, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- CYPHER 25. GATEKEEPERS — Who controls information flow?
-- ============================================================================
-- Betweenness centrality finds people who sit on many shortest paths.
-- If these "gatekeepers" leave, communication between groups breaks down.
-- In a 100-person startup, even one key gatekeeper leaving is a crisis.

USE social_network
CALL algo.betweenness()
YIELD nodeId, centrality, rank
RETURN nodeId, centrality, rank
ORDER BY centrality DESC
LIMIT 10;


-- ============================================================================
-- CYPHER 26. SIX DEGREES — How many hops apart are people?
-- ============================================================================
-- BFS from employee #1: at each hop distance, how many people can be
-- reached? In a well-connected startup, everyone should be reachable
-- within 3-4 hops. More than that suggests organizational fragmentation.

USE social_network
CALL algo.bfs({source: 1})
YIELD nodeId, depth, parentId
RETURN depth, count(*) AS people_at_distance
ORDER BY depth;


-- ============================================================================
-- CYPHER 27. SHORTEST PATH — How does a message travel across the company?
-- ============================================================================
-- If employee #1 needs to reach employee #50 (likely in a different
-- department and city), what's the fastest path through the network?
-- Each hop represents a real person passing information along.

USE social_network
CALL algo.shortestPath({source: 1, target: 50})
YIELD nodeId, step, distance
RETURN nodeId, step, distance
ORDER BY step;


-- ############################################################################
-- ############################################################################
--
-- PART 3: GRAPH VISUALIZATION — See the Company Network
--
-- ############################################################################
-- ############################################################################
-- These queries return node + edge data designed for the graph visualizer.
-- At 100 nodes and ~300 edges, the full company graph should render
-- smoothly with visible department clusters and bridge nodes.
-- ############################################################################


-- ============================================================================
-- 28. VIZ: FULL COMPANY GRAPH — All 100 employees with all connections
-- ============================================================================
-- Visualize the entire startup network. Department clusters should be
-- visible as dense groups, with bridge employees spanning between them.
-- This is the complete organizational map.

USE social_network
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- 29. VIZ: MENTORSHIP NETWORK — Only mentor relationships
-- ============================================================================
-- Visualize just the mentorship graph. This reveals the hierarchical
-- skeleton of the organization — who mentors whom. Directors should
-- appear as high-degree hub nodes connecting to multiple mentees.

USE social_network
MATCH (mentor)-[r]->(mentee)
WHERE r.relationship_type = 'mentor'
RETURN mentor, r, mentee;


-- ============================================================================
-- 30. VIZ: CROSS-DEPARTMENT BRIDGES — Only inter-department connections
-- ============================================================================
-- Strip away all intra-department edges to see only the connections
-- that cross departmental boundaries. The bridge employees and city-based
-- social bonds become the dominant structure. Isolated departments
-- (clusters with no outgoing edges) are organizational blind spots.

USE social_network
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a, r, b;
