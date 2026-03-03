-- ============================================================================
-- Graph Flattened Mode — Verification Queries
-- ============================================================================
-- Each query demonstrates a graph operation on the flattened property tables.
-- All properties are accessed as direct columns — no JSON extraction needed.
-- ============================================================================


-- ============================================================================
-- 1. ALL PERSONS — Direct column access
-- ============================================================================

SELECT id, name, age, department, city, level, active
FROM {{zone_name}}.graph.persons_flattened
ORDER BY id;


-- ============================================================================
-- 2. ALL FRIENDSHIPS — Edge properties as columns
-- ============================================================================

SELECT
    f.src,
    p1.name AS from_name,
    f.dst,
    p2.name AS to_name,
    f.weight,
    f.relationship_type,
    f.since_year
FROM {{zone_name}}.graph.friendships_flattened f
JOIN {{zone_name}}.graph.persons_flattened p1 ON f.src = p1.id
JOIN {{zone_name}}.graph.persons_flattened p2 ON f.dst = p2.id
ORDER BY f.src, f.dst;


-- ============================================================================
-- 3. OUT-DEGREE — Number of outgoing connections per person
-- ============================================================================

SELECT
    p.name,
    COUNT(f.dst) AS out_degree
FROM {{zone_name}}.graph.persons_flattened p
LEFT JOIN {{zone_name}}.graph.friendships_flattened f ON p.id = f.src
GROUP BY p.name
ORDER BY out_degree DESC;


-- ============================================================================
-- 4. IN-DEGREE — Number of incoming connections per person
-- ============================================================================

SELECT
    p.name,
    COUNT(f.src) AS in_degree
FROM {{zone_name}}.graph.persons_flattened p
LEFT JOIN {{zone_name}}.graph.friendships_flattened f ON p.id = f.dst
GROUP BY p.name
ORDER BY in_degree DESC;


-- ============================================================================
-- 5. STRONGEST CONNECTIONS — Edges sorted by weight
-- ============================================================================

SELECT
    p1.name AS from_name,
    p2.name AS to_name,
    f.weight,
    f.relationship_type
FROM {{zone_name}}.graph.friendships_flattened f
JOIN {{zone_name}}.graph.persons_flattened p1 ON f.src = p1.id
JOIN {{zone_name}}.graph.persons_flattened p2 ON f.dst = p2.id
ORDER BY f.weight DESC;


-- ============================================================================
-- 6. WORK vs SOCIAL — Connection context breakdown
-- ============================================================================

SELECT
    context,
    COUNT(*) AS edge_count,
    ROUND(AVG(weight), 2) AS avg_weight,
    ROUND(AVG(rating), 1) AS avg_rating
FROM {{zone_name}}.graph.friendships_flattened
GROUP BY context
ORDER BY edge_count DESC;


-- ============================================================================
-- 7. DEPARTMENT CONNECTIVITY — Cross-department links
-- ============================================================================

SELECT
    p1.department AS from_dept,
    p2.department AS to_dept,
    COUNT(*) AS connections
FROM {{zone_name}}.graph.friendships_flattened f
JOIN {{zone_name}}.graph.persons_flattened p1 ON f.src = p1.id
JOIN {{zone_name}}.graph.persons_flattened p2 ON f.dst = p2.id
GROUP BY p1.department, p2.department
ORDER BY connections DESC;


-- ============================================================================
-- 8. 2-HOP NEIGHBORS — Who can Alice reach in 2 steps?
-- ============================================================================
-- Alice (id=1) -> direct friends -> their friends (excluding Alice herself)

SELECT DISTINCT
    p2.name AS two_hop_neighbor,
    p1.name AS via_person
FROM {{zone_name}}.graph.friendships_flattened f1
JOIN {{zone_name}}.graph.friendships_flattened f2 ON f1.dst = f2.src
JOIN {{zone_name}}.graph.persons_flattened p1 ON f1.dst = p1.id
JOIN {{zone_name}}.graph.persons_flattened p2 ON f2.dst = p2.id
WHERE f1.src = 1
  AND f2.dst != 1
ORDER BY two_hop_neighbor;


-- ============================================================================
-- 9. RECIPROCAL FRIENDSHIPS — Bidirectional edges
-- ============================================================================

SELECT
    p1.name AS person_a,
    p2.name AS person_b,
    f1.relationship_type AS a_to_b_type,
    f2.relationship_type AS b_to_a_type,
    f1.weight AS a_to_b_weight,
    f2.weight AS b_to_a_weight
FROM {{zone_name}}.graph.friendships_flattened f1
JOIN {{zone_name}}.graph.friendships_flattened f2
    ON f1.src = f2.dst AND f1.dst = f2.src
JOIN {{zone_name}}.graph.persons_flattened p1 ON f1.src = p1.id
JOIN {{zone_name}}.graph.persons_flattened p2 ON f1.dst = p2.id
WHERE f1.src < f1.dst;


-- ============================================================================
-- 10. PREDICATE PUSHDOWN — Filter by column values directly
-- ============================================================================
-- Flattened mode advantage: WHERE clauses push down to storage layer.

SELECT name, age, department, city
FROM {{zone_name}}.graph.persons_flattened
WHERE department = 'Engineering'
  AND active = true
ORDER BY age;


-- ============================================================================
-- 11. SUMMARY — All PASS/FAIL checks
-- ============================================================================

SELECT 'person_count' AS check_name,
       CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.graph.persons_flattened
UNION ALL
SELECT 'edge_count',
       CASE WHEN COUNT(*) = 6 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.friendships_flattened
UNION ALL
SELECT 'alice_exists',
       CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.persons_flattened WHERE name = 'Alice' AND age = 30
UNION ALL
SELECT 'alice_out_degree',
       CASE WHEN COUNT(*) = 2 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.friendships_flattened WHERE src = 1
UNION ALL
SELECT 'eve_to_alice_edge',
       CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.friendships_flattened WHERE src = 5 AND dst = 1
UNION ALL
SELECT 'mentor_weight_1_0',
       CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.friendships_flattened WHERE relationship_type = 'mentor' AND weight = 1.0
UNION ALL
SELECT 'engineering_count',
       CASE WHEN COUNT(*) = 2 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.persons_flattened WHERE department = 'Engineering'
UNION ALL
SELECT 'work_context_count',
       CASE WHEN COUNT(*) = 4 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.friendships_flattened WHERE context = 'work'
UNION ALL
SELECT 'nyc_persons',
       CASE WHEN COUNT(*) = 2 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.persons_flattened WHERE city = 'NYC'
UNION ALL
SELECT 'max_weight_is_1_0',
       CASE WHEN MAX(weight) = 1.0 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.friendships_flattened
ORDER BY check_name;
