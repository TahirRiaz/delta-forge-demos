-- ============================================================================
-- Graph Hybrid Mode — Verification Queries
-- ============================================================================
-- Demonstrates the hybrid access pattern: core columns for frequent queries
-- plus JSON extraction for optional/extensible properties.
-- ============================================================================


-- ============================================================================
-- 1. ALL PERSONS — Core columns + JSON extras
-- ============================================================================

SELECT
    id,
    name,
    age,
    json_get_str(extras, '$.department') AS department,
    json_get_str(extras, '$.city') AS city,
    json_get_str(extras, '$.level') AS level
FROM {{zone_name}}.graph.persons_hybrid
ORDER BY id;


-- ============================================================================
-- 2. COLUMN-ONLY QUERY — Fast path using core columns
-- ============================================================================
-- No JSON extraction needed for name and age filters.

SELECT id, name, age
FROM {{zone_name}}.graph.persons_hybrid
WHERE age > 28
ORDER BY age DESC;


-- ============================================================================
-- 3. FRIENDSHIPS WITH NAMES — Core columns + JSON extras
-- ============================================================================

SELECT
    f.src,
    p1.name AS from_name,
    f.dst,
    p2.name AS to_name,
    f.weight,
    f.relationship_type,
    json_get_int(f.extras, '$.since_year') AS since_year,
    json_get_str(f.extras, '$.frequency') AS frequency
FROM {{zone_name}}.graph.friendships_hybrid f
JOIN {{zone_name}}.graph.persons_hybrid p1 ON f.src = p1.id
JOIN {{zone_name}}.graph.persons_hybrid p2 ON f.dst = p2.id
ORDER BY f.src, f.dst;


-- ============================================================================
-- 4. MIXED FILTERING — Column predicate + JSON predicate
-- ============================================================================
-- Core column (relationship_type) pushes down; JSON (context) is extracted.

SELECT
    f.src,
    f.dst,
    f.relationship_type,
    f.weight,
    json_get_str(f.extras, '$.context') AS context
FROM {{zone_name}}.graph.friendships_hybrid f
WHERE f.relationship_type = 'colleague'
  AND json_get_str(f.extras, '$.context') = 'work'
ORDER BY f.weight DESC;


-- ============================================================================
-- 5. SKILLS FROM EXTRAS — Array property in JSON
-- ============================================================================

SELECT
    name,
    age,
    json_get_str(extras, '$.skills') AS skills_json,
    json_get_str(extras, '$.level') AS level
FROM {{zone_name}}.graph.persons_hybrid
ORDER BY age;


-- ============================================================================
-- 6. EDGE ANALYTICS — Group by core column, aggregate JSON
-- ============================================================================

SELECT
    relationship_type,
    COUNT(*) AS edge_count,
    ROUND(AVG(weight), 2) AS avg_weight,
    ROUND(AVG(CAST(json_get_int(extras, '$.rating') AS DOUBLE)), 1) AS avg_rating
FROM {{zone_name}}.graph.friendships_hybrid
GROUP BY relationship_type
ORDER BY edge_count DESC;


-- ============================================================================
-- 7. OUT-DEGREE — Join on core columns only
-- ============================================================================

SELECT
    p.name,
    p.age,
    COUNT(f.dst) AS out_degree
FROM {{zone_name}}.graph.persons_hybrid p
LEFT JOIN {{zone_name}}.graph.friendships_hybrid f ON p.id = f.src
GROUP BY p.name, p.age
ORDER BY out_degree DESC;


-- ============================================================================
-- 8. DEPARTMENT ANALYSIS — JSON grouping
-- ============================================================================

SELECT
    json_get_str(extras, '$.department') AS department,
    COUNT(*) AS person_count,
    ROUND(AVG(age), 1) AS avg_age
FROM {{zone_name}}.graph.persons_hybrid
GROUP BY json_get_str(extras, '$.department')
ORDER BY person_count DESC;


-- ============================================================================
-- 9. 2-HOP NEIGHBORS — Core column joins + JSON enrichment
-- ============================================================================

SELECT DISTINCT
    p2.name AS two_hop_neighbor,
    p1.name AS via_person,
    json_get_str(p2.extras, '$.department') AS neighbor_dept
FROM {{zone_name}}.graph.friendships_hybrid f1
JOIN {{zone_name}}.graph.friendships_hybrid f2 ON f1.dst = f2.src
JOIN {{zone_name}}.graph.persons_hybrid p1 ON f1.dst = p1.id
JOIN {{zone_name}}.graph.persons_hybrid p2 ON f2.dst = p2.id
WHERE f1.src = 1
  AND f2.dst != 1
ORDER BY two_hop_neighbor;


-- ============================================================================
-- 10. CONTEXT BREAKDOWN — JSON grouping on extras
-- ============================================================================

SELECT
    json_get_str(extras, '$.context') AS context,
    json_get_str(extras, '$.frequency') AS frequency,
    COUNT(*) AS edge_count,
    ROUND(AVG(weight), 2) AS avg_weight
FROM {{zone_name}}.graph.friendships_hybrid
GROUP BY json_get_str(extras, '$.context'), json_get_str(extras, '$.frequency')
ORDER BY edge_count DESC;


-- ============================================================================
-- 11. SUMMARY — All PASS/FAIL checks
-- ============================================================================

SELECT 'person_count' AS check_name,
       CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.graph.persons_hybrid
UNION ALL
SELECT 'edge_count',
       CASE WHEN COUNT(*) = 6 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.friendships_hybrid
UNION ALL
SELECT 'alice_name_column',
       CASE WHEN name = 'Alice' THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.persons_hybrid WHERE id = 1
UNION ALL
SELECT 'alice_age_column',
       CASE WHEN age = 30 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.persons_hybrid WHERE id = 1
UNION ALL
SELECT 'alice_dept_from_extras',
       CASE WHEN json_get_str(extras, '$.department') = 'Engineering' THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.persons_hybrid WHERE id = 1
UNION ALL
SELECT 'alice_out_degree',
       CASE WHEN COUNT(*) = 2 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.friendships_hybrid WHERE src = 1
UNION ALL
SELECT 'mentor_type_column',
       CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.friendships_hybrid WHERE relationship_type = 'mentor'
UNION ALL
SELECT 'mentor_weight_column',
       CASE WHEN weight = 1.0 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.friendships_hybrid WHERE relationship_type = 'mentor'
UNION ALL
SELECT 'work_context_from_extras',
       CASE WHEN COUNT(*) = 4 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.friendships_hybrid
WHERE json_get_str(extras, '$.context') = 'work'
UNION ALL
SELECT 'skills_in_extras',
       CASE WHEN json_get_str(extras, '$.skills') LIKE '[%' THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.persons_hybrid WHERE id = 1
ORDER BY check_name;
