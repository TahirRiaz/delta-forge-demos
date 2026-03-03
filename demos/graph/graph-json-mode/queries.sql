-- ============================================================================
-- Graph JSON Mode — Verification Queries
-- ============================================================================
-- Each query demonstrates graph operations with JSON property extraction.
-- All vertex/edge properties are stored in a single JSON string column.
-- ============================================================================


-- ============================================================================
-- 1. ALL PERSONS — JSON property extraction
-- ============================================================================

SELECT
    id,
    json_get_str(props, '$.name') AS name,
    json_get_int(props, '$.age') AS age,
    json_get_str(props, '$.department') AS department,
    json_get_str(props, '$.city') AS city,
    json_get_str(props, '$.level') AS level
FROM {{zone_name}}.graph.persons_json
ORDER BY id;


-- ============================================================================
-- 2. RAW JSON — See the full property blobs
-- ============================================================================

SELECT id, props
FROM {{zone_name}}.graph.persons_json
ORDER BY id;


-- ============================================================================
-- 3. FRIENDSHIPS WITH NAMES — Join + JSON extraction
-- ============================================================================

SELECT
    f.src,
    json_get_str(p1.props, '$.name') AS from_name,
    f.dst,
    json_get_str(p2.props, '$.name') AS to_name,
    json_get_float(f.props, '$.weight') AS weight,
    json_get_str(f.props, '$.relationship_type') AS relationship_type
FROM {{zone_name}}.graph.friendships_json f
JOIN {{zone_name}}.graph.persons_json p1 ON f.src = p1.id
JOIN {{zone_name}}.graph.persons_json p2 ON f.dst = p2.id
ORDER BY f.src, f.dst;


-- ============================================================================
-- 4. FILTER BY JSON VALUE — Find Engineering employees
-- ============================================================================

SELECT
    id,
    json_get_str(props, '$.name') AS name,
    json_get_str(props, '$.department') AS department,
    json_get_str(props, '$.city') AS city
FROM {{zone_name}}.graph.persons_json
WHERE json_get_str(props, '$.department') = 'Engineering'
ORDER BY id;


-- ============================================================================
-- 5. JSON ARRAY ACCESS — Skills extraction
-- ============================================================================
-- JSON mode advantage: properties like arrays are naturally stored.

SELECT
    id,
    json_get_str(props, '$.name') AS name,
    json_get_str(props, '$.skills') AS skills_json
FROM {{zone_name}}.graph.persons_json
ORDER BY id;


-- ============================================================================
-- 6. AGE-BASED FILTERING — Employees over 30
-- ============================================================================

SELECT
    id,
    json_get_str(props, '$.name') AS name,
    json_get_int(props, '$.age') AS age,
    json_get_str(props, '$.level') AS level
FROM {{zone_name}}.graph.persons_json
WHERE json_get_int(props, '$.age') > 30
ORDER BY json_get_int(props, '$.age') DESC;


-- ============================================================================
-- 7. EDGE WEIGHT ANALYSIS — JSON numeric extraction
-- ============================================================================

SELECT
    json_get_str(f.props, '$.relationship_type') AS rel_type,
    COUNT(*) AS edge_count,
    ROUND(AVG(json_get_float(f.props, '$.weight')), 2) AS avg_weight,
    ROUND(AVG(CAST(json_get_int(f.props, '$.rating') AS DOUBLE)), 1) AS avg_rating
FROM {{zone_name}}.graph.friendships_json f
GROUP BY json_get_str(f.props, '$.relationship_type')
ORDER BY edge_count DESC;


-- ============================================================================
-- 8. OUT-DEGREE VIA JSON — Outgoing connections per person
-- ============================================================================

SELECT
    json_get_str(p.props, '$.name') AS name,
    COUNT(f.dst) AS out_degree
FROM {{zone_name}}.graph.persons_json p
LEFT JOIN {{zone_name}}.graph.friendships_json f ON p.id = f.src
GROUP BY json_get_str(p.props, '$.name')
ORDER BY out_degree DESC;


-- ============================================================================
-- 9. CONTEXT BREAKDOWN — Work vs social from JSON
-- ============================================================================

SELECT
    json_get_str(props, '$.context') AS context,
    COUNT(*) AS edge_count,
    ROUND(AVG(json_get_float(props, '$.weight')), 2) AS avg_weight
FROM {{zone_name}}.graph.friendships_json
GROUP BY json_get_str(props, '$.context')
ORDER BY edge_count DESC;


-- ============================================================================
-- 10. 2-HOP NEIGHBORS — Via JSON join
-- ============================================================================

SELECT DISTINCT
    json_get_str(p2.props, '$.name') AS two_hop_neighbor,
    json_get_str(p1.props, '$.name') AS via_person
FROM {{zone_name}}.graph.friendships_json f1
JOIN {{zone_name}}.graph.friendships_json f2 ON f1.dst = f2.src
JOIN {{zone_name}}.graph.persons_json p1 ON f1.dst = p1.id
JOIN {{zone_name}}.graph.persons_json p2 ON f2.dst = p2.id
WHERE f1.src = 1
  AND f2.dst != 1
ORDER BY two_hop_neighbor;


-- ============================================================================
-- 11. SUMMARY — All PASS/FAIL checks
-- ============================================================================

SELECT 'person_count' AS check_name,
       CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.graph.persons_json
UNION ALL
SELECT 'edge_count',
       CASE WHEN COUNT(*) = 6 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.friendships_json
UNION ALL
SELECT 'alice_name_from_json',
       CASE WHEN json_get_str(props, '$.name') = 'Alice' THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.persons_json WHERE id = 1
UNION ALL
SELECT 'alice_age_from_json',
       CASE WHEN json_get_int(props, '$.age') = 30 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.persons_json WHERE id = 1
UNION ALL
SELECT 'alice_out_degree',
       CASE WHEN COUNT(*) = 2 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.friendships_json WHERE src = 1
UNION ALL
SELECT 'mentor_edge_exists',
       CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.friendships_json
WHERE json_get_str(props, '$.relationship_type') = 'mentor'
UNION ALL
SELECT 'engineering_count',
       CASE WHEN COUNT(*) = 2 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.persons_json
WHERE json_get_str(props, '$.department') = 'Engineering'
UNION ALL
SELECT 'work_context_count',
       CASE WHEN COUNT(*) = 4 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.friendships_json
WHERE json_get_str(props, '$.context') = 'work'
UNION ALL
SELECT 'max_weight_is_1_0',
       CASE WHEN MAX(json_get_float(props, '$.weight')) = 1.0 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.friendships_json
UNION ALL
SELECT 'skills_is_array',
       CASE WHEN json_get_str(props, '$.skills') LIKE '[%' THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.graph.persons_json WHERE id = 1
ORDER BY check_name;
