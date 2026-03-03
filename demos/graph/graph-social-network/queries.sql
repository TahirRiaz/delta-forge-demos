-- ============================================================================
-- Graph Social Network — Verification Queries
-- ============================================================================
-- Progressive queries on a 100-employee company social network with 300+
-- connections. Builds from basic counts to advanced graph analytics.
-- ============================================================================


-- ============================================================================
-- 1. EMPLOYEE COUNT — Verify 100 employees generated
-- ============================================================================

SELECT 'employee_count' AS check_name,
       COUNT(*) AS actual,
       100 AS expected,
       CASE WHEN COUNT(*) = 100 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.graph.employees;


-- ============================================================================
-- 2. DEPARTMENT DISTRIBUTION — Employees per department
-- ============================================================================

SELECT
    department,
    COUNT(*) AS employee_count,
    ROUND(AVG(age), 1) AS avg_age,
    COUNT(*) FILTER (WHERE active) AS active_count
FROM {{zone_name}}.graph.employees
GROUP BY department
ORDER BY employee_count DESC;


-- ============================================================================
-- 3. CONNECTION COUNT — Verify 200+ connections
-- ============================================================================

SELECT 'connection_count' AS check_name,
       COUNT(*) AS actual,
       CASE WHEN COUNT(*) >= 200 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.graph.connections;


-- ============================================================================
-- 4. RELATIONSHIP TYPE BREAKDOWN — Edge categories
-- ============================================================================

SELECT
    relationship_type,
    COUNT(*) AS edge_count,
    ROUND(AVG(weight), 2) AS avg_weight,
    MIN(since_year) AS earliest,
    MAX(since_year) AS latest
FROM {{zone_name}}.graph.connections
GROUP BY relationship_type
ORDER BY edge_count DESC;


-- ============================================================================
-- 5. TOP 10 MOST CONNECTED — By total degree
-- ============================================================================

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
-- 6. DEPARTMENT CROSS-POLLINATION — Inter-department connections
-- ============================================================================

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
-- 7. INTRA-DEPARTMENT DENSITY — Connections within same department
-- ============================================================================

SELECT
    src_dept AS department,
    connection_count AS intra_connections,
    avg_weight
FROM {{zone_name}}.graph.dept_connections
WHERE src_dept = dst_dept
ORDER BY connection_count DESC;


-- ============================================================================
-- 8. CITY NETWORK — Connection distribution across cities
-- ============================================================================

SELECT
    src_e.city AS from_city,
    dst_e.city AS to_city,
    COUNT(*) AS connections,
    ROUND(AVG(c.weight), 2) AS avg_weight
FROM {{zone_name}}.graph.connections c
JOIN {{zone_name}}.graph.employees src_e ON c.src = src_e.id
JOIN {{zone_name}}.graph.employees dst_e ON c.dst = dst_e.id
GROUP BY src_e.city, dst_e.city
ORDER BY connections DESC
LIMIT 15;


-- ============================================================================
-- 9. INFLUENCE SCORE — In-degree as influence proxy
-- ============================================================================
-- Employees with the highest in-degree receive the most connections
-- from others — a simple measure of influence/popularity.

SELECT
    name,
    department,
    city,
    in_degree AS influence_score,
    level
FROM {{zone_name}}.graph.employee_stats
WHERE in_degree > 0
ORDER BY in_degree DESC
LIMIT 10;


-- ============================================================================
-- 10. ISOLATED NODES — Employees with no connections
-- ============================================================================

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
-- 11. 2-HOP NEIGHBORHOOD — Who can employee #1 reach in 2 steps?
-- ============================================================================

SELECT
    'direct' AS hop,
    e.name,
    e.department
FROM {{zone_name}}.graph.connections c
JOIN {{zone_name}}.graph.employees e ON c.dst = e.id
WHERE c.src = 1
UNION ALL
SELECT DISTINCT
    '2-hop' AS hop,
    e2.name,
    e2.department
FROM {{zone_name}}.graph.connections c1
JOIN {{zone_name}}.graph.connections c2 ON c1.dst = c2.src
JOIN {{zone_name}}.graph.employees e2 ON c2.dst = e2.id
WHERE c1.src = 1
  AND c2.dst != 1
  AND c2.dst NOT IN (SELECT dst FROM {{zone_name}}.graph.connections WHERE src = 1)
ORDER BY hop, name;


-- ============================================================================
-- 12. TEAM CLUSTERING — Average connection weight within departments
-- ============================================================================

SELECT
    src_e.department,
    COUNT(*) AS team_edges,
    ROUND(AVG(c.weight), 2) AS avg_internal_weight,
    COUNT(DISTINCT c.src) AS active_connectors
FROM {{zone_name}}.graph.connections c
JOIN {{zone_name}}.graph.employees src_e ON c.src = src_e.id
JOIN {{zone_name}}.graph.employees dst_e ON c.dst = dst_e.id
WHERE src_e.department = dst_e.department
GROUP BY src_e.department
ORDER BY team_edges DESC;


-- ============================================================================
-- 13. BRIDGE NODES — Employees connecting different departments
-- ============================================================================
-- Bridge nodes connect to employees in 3+ different departments.

SELECT
    e.name,
    e.department AS own_dept,
    e.level,
    COUNT(DISTINCT dst_e.department) AS depts_reached,
    COUNT(*) AS total_outgoing
FROM {{zone_name}}.graph.connections c
JOIN {{zone_name}}.graph.employees e ON c.src = e.id
JOIN {{zone_name}}.graph.employees dst_e ON c.dst = dst_e.id
WHERE e.department != dst_e.department
GROUP BY e.name, e.department, e.level
HAVING COUNT(DISTINCT dst_e.department) >= 3
ORDER BY depts_reached DESC, total_outgoing DESC;


-- ============================================================================
-- 14. MENTOR NETWORK — Mentorship connections only
-- ============================================================================

SELECT
    src_e.name AS mentor,
    src_e.level AS mentor_level,
    dst_e.name AS mentee,
    dst_e.level AS mentee_level,
    c.weight,
    c.since_year
FROM {{zone_name}}.graph.connections c
JOIN {{zone_name}}.graph.employees src_e ON c.src = src_e.id
JOIN {{zone_name}}.graph.employees dst_e ON c.dst = dst_e.id
WHERE c.relationship_type = 'mentor'
ORDER BY c.weight DESC
LIMIT 15;


-- ============================================================================
-- 15. RECIPROCAL CONNECTIONS — Bidirectional edges
-- ============================================================================

SELECT
    e1.name AS person_a,
    e1.department AS dept_a,
    e2.name AS person_b,
    e2.department AS dept_b,
    c1.relationship_type AS a_to_b,
    c2.relationship_type AS b_to_a
FROM {{zone_name}}.graph.connections c1
JOIN {{zone_name}}.graph.connections c2
    ON c1.src = c2.dst AND c1.dst = c2.src
JOIN {{zone_name}}.graph.employees e1 ON c1.src = e1.id
JOIN {{zone_name}}.graph.employees e2 ON c1.dst = e2.id
WHERE c1.src < c1.dst
ORDER BY e1.name
LIMIT 15;


-- ============================================================================
-- 16. GRAPH STATISTICS — Summary metrics
-- ============================================================================

SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.graph.employees) AS total_nodes,
    (SELECT COUNT(*) FROM {{zone_name}}.graph.connections) AS total_edges,
    (SELECT COUNT(DISTINCT department) FROM {{zone_name}}.graph.employees) AS dept_count,
    (SELECT COUNT(DISTINCT city) FROM {{zone_name}}.graph.employees) AS city_count,
    (SELECT COUNT(DISTINCT relationship_type) FROM {{zone_name}}.graph.connections) AS rel_type_count,
    (SELECT ROUND(AVG(weight), 2) FROM {{zone_name}}.graph.connections) AS avg_weight;


-- ============================================================================
-- 17. SUMMARY — All PASS/FAIL checks
-- ============================================================================

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
