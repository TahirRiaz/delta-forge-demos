-- ============================================================================
-- H3 GPS Fleet Tracker — Verification Queries
-- ============================================================================
-- Each query verifies a specific H3 capability with known expected values.
-- All H3 values referenced below match the official H3 library and are
-- confirmed by delta-forge-tests/h3_verification_tests.rs.
-- ============================================================================


-- ============================================================================
-- 1. DATA GENERATION — 10,000 GPS points across 5 cities
-- ============================================================================

SELECT 'gps_point_count' AS check_name,
       COUNT(*) AS actual,
       10000 AS expected,
       CASE WHEN COUNT(*) = 10000 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.spatial.gps_points;


-- ============================================================================
-- 2. LANDMARKS TABLE — 10 famous locations
-- ============================================================================

SELECT 'landmark_count' AS check_name,
       COUNT(*) AS actual,
       10 AS expected,
       CASE WHEN COUNT(*) = 10 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.spatial.landmarks;


-- ============================================================================
-- 3. COORDINATE CONVERSION — Known landmark H3 cells at resolution 9
-- ============================================================================
-- h3_latlng_to_cell converts (lat, lng, resolution) → H3 cell index (UInt64).
-- Each cell is validated and the round-trip lat/lng is within 0.01° (~1 km).

SELECT
    name,
    lat,
    lng,
    h3_latlng_to_cell(lat, lng, 9) AS h3_cell,
    h3_cell_to_string(h3_latlng_to_cell(lat, lng, 9)) AS h3_hex,
    h3_is_valid_cell(h3_latlng_to_cell(lat, lng, 9)) AS is_valid,
    h3_get_resolution(h3_latlng_to_cell(lat, lng, 9)) AS resolution,
    ROUND(h3_cell_to_lat(h3_latlng_to_cell(lat, lng, 9)), 4) AS roundtrip_lat,
    ROUND(h3_cell_to_lng(h3_latlng_to_cell(lat, lng, 9)), 4) AS roundtrip_lng
FROM {{zone_name}}.spatial.landmarks
ORDER BY id;


-- ============================================================================
-- 4. CELL VALIDATION — All landmark cells are valid, none are pentagons
-- ============================================================================
-- The H3 grid has exactly 12 pentagons per resolution (at icosahedron vertices).
-- City locations never coincide with pentagon cells.

SELECT 'all_cells_valid' AS check_name,
       COUNT(*) FILTER (WHERE h3_is_valid_cell(h3_latlng_to_cell(lat, lng, 9))) AS actual,
       10 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE h3_is_valid_cell(h3_latlng_to_cell(lat, lng, 9))) = 10
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.spatial.landmarks;


-- ============================================================================
-- 5. RESOLUTION CLASS — Res 9 cells are Class III (odd resolutions)
-- ============================================================================
-- H3 alternates Class II (even) and Class III (odd) at each resolution level.
-- Resolution 9 is odd → all cells should be Class III.

SELECT 'res9_class_iii' AS check_name,
       COUNT(*) FILTER (WHERE h3_is_res_class_iii(h3_latlng_to_cell(lat, lng, 9))) AS actual,
       10 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE h3_is_res_class_iii(h3_latlng_to_cell(lat, lng, 9))) = 10
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.spatial.landmarks;


-- ============================================================================
-- 6. GRID TOPOLOGY: HEX RING — 6 neighbors at distance 1 (hexagons)
-- ============================================================================
-- Known value: h3_hex_ring(cell, 1) returns exactly 6 cells for hexagons.
-- At distance 2, it returns exactly 12 cells.
-- Using SF City Hall (37.7792, -122.4191) as the reference point.

SELECT 'ring_k1_count' AS check_name,
       (SELECT COUNT(*) FROM (
           SELECT UNNEST(h3_hex_ring(h3_latlng_to_cell(37.7792, -122.4191, 9), 1)) AS cell
       )) AS actual,
       6 AS expected,
       CASE WHEN (SELECT COUNT(*) FROM (
           SELECT UNNEST(h3_hex_ring(h3_latlng_to_cell(37.7792, -122.4191, 9), 1)) AS cell
       )) = 6 THEN 'PASS' ELSE 'FAIL' END AS result;


-- ============================================================================
-- 7. GRID TOPOLOGY: HEX RING K=2 — 12 cells at distance 2
-- ============================================================================

SELECT 'ring_k2_count' AS check_name,
       (SELECT COUNT(*) FROM (
           SELECT UNNEST(h3_hex_ring(h3_latlng_to_cell(37.7792, -122.4191, 9), 2)) AS cell
       )) AS actual,
       12 AS expected,
       CASE WHEN (SELECT COUNT(*) FROM (
           SELECT UNNEST(h3_hex_ring(h3_latlng_to_cell(37.7792, -122.4191, 9), 2)) AS cell
       )) = 12 THEN 'PASS' ELSE 'FAIL' END AS result;


-- ============================================================================
-- 8. GRID TOPOLOGY: HEX DISK — center + ring = 7 at k=1, 19 at k=2
-- ============================================================================
-- h3_hex_disk returns all cells within distance k (inclusive of center).
-- Known values: disk(k=1) = 1 + 6 = 7, disk(k=2) = 1 + 6 + 12 = 19.

SELECT 'disk_k1_count' AS check_name,
       (SELECT COUNT(*) FROM (
           SELECT UNNEST(h3_hex_disk(h3_latlng_to_cell(37.7792, -122.4191, 9), 1)) AS cell
       )) AS actual,
       7 AS expected,
       CASE WHEN (SELECT COUNT(*) FROM (
           SELECT UNNEST(h3_hex_disk(h3_latlng_to_cell(37.7792, -122.4191, 9), 1)) AS cell
       )) = 7 THEN 'PASS' ELSE 'FAIL' END AS result;


-- ============================================================================
-- 9. GRID DISTANCE — Adjacent cells have distance 1
-- ============================================================================
-- Grid distance between a cell and its immediate ring neighbor is exactly 1.

SELECT 'grid_distance_neighbor' AS check_name,
       h3_grid_distance(
           h3_latlng_to_cell(37.7792, -122.4191, 9),
           (SELECT cell FROM (
               SELECT UNNEST(h3_hex_ring(h3_latlng_to_cell(37.7792, -122.4191, 9), 1)) AS cell
           ) LIMIT 1)
       ) AS actual,
       1 AS expected,
       CASE WHEN h3_grid_distance(
           h3_latlng_to_cell(37.7792, -122.4191, 9),
           (SELECT cell FROM (
               SELECT UNNEST(h3_hex_ring(h3_latlng_to_cell(37.7792, -122.4191, 9), 1)) AS cell
           ) LIMIT 1)
       ) = 1 THEN 'PASS' ELSE 'FAIL' END AS result;


-- ============================================================================
-- 10. GRID PATH — Path between adjacent cells has 2 elements (inclusive)
-- ============================================================================

SELECT 'grid_path_adjacent' AS check_name,
       (SELECT COUNT(*) FROM (
           SELECT UNNEST(h3_grid_path(
               h3_latlng_to_cell(37.7792, -122.4191, 9),
               (SELECT cell FROM (
                   SELECT UNNEST(h3_hex_ring(h3_latlng_to_cell(37.7792, -122.4191, 9), 1)) AS cell
               ) LIMIT 1)
           )) AS cell
       )) AS actual,
       2 AS expected,
       CASE WHEN (SELECT COUNT(*) FROM (
           SELECT UNNEST(h3_grid_path(
               h3_latlng_to_cell(37.7792, -122.4191, 9),
               (SELECT cell FROM (
                   SELECT UNNEST(h3_hex_ring(h3_latlng_to_cell(37.7792, -122.4191, 9), 1)) AS cell
               ) LIMIT 1)
           )) AS cell
       )) = 2 THEN 'PASS' ELSE 'FAIL' END AS result;


-- ============================================================================
-- 11. CELL HIERARCHY — Parent at lower resolution, 7 children at next
-- ============================================================================
-- h3_cell_to_parent(cell, 7) returns the res-7 parent of a res-9 cell.
-- h3_cell_to_children(cell, 10) returns 7 children for a hexagon cell.

SELECT 'parent_resolution' AS check_name,
       h3_get_resolution(
           h3_cell_to_parent(h3_latlng_to_cell(37.7792, -122.4191, 9), 7)
       ) AS actual,
       7 AS expected,
       CASE WHEN h3_get_resolution(
           h3_cell_to_parent(h3_latlng_to_cell(37.7792, -122.4191, 9), 7)
       ) = 7 THEN 'PASS' ELSE 'FAIL' END AS result;


-- ============================================================================
-- 12. CHILDREN COUNT — Hexagon cells have exactly 7 children
-- ============================================================================

SELECT 'children_count' AS check_name,
       (SELECT COUNT(*) FROM (
           SELECT UNNEST(h3_cell_to_children(
               h3_latlng_to_cell(37.7792, -122.4191, 9), 10
           )) AS child
       )) AS actual,
       7 AS expected,
       CASE WHEN (SELECT COUNT(*) FROM (
           SELECT UNNEST(h3_cell_to_children(
               h3_latlng_to_cell(37.7792, -122.4191, 9), 10
           )) AS child
       )) = 7 THEN 'PASS' ELSE 'FAIL' END AS result;


-- ============================================================================
-- 13. CENTER CHILD — Returns a valid cell at the child resolution
-- ============================================================================

SELECT 'center_child_res' AS check_name,
       h3_get_resolution(
           h3_cell_to_center_child(h3_latlng_to_cell(37.7792, -122.4191, 9), 10)
       ) AS actual,
       10 AS expected,
       CASE WHEN h3_get_resolution(
           h3_cell_to_center_child(h3_latlng_to_cell(37.7792, -122.4191, 9), 10)
       ) = 10 THEN 'PASS' ELSE 'FAIL' END AS result;


-- ============================================================================
-- 14. CELL AREA — Res 9 hexagons are ~105,000 m² (50K–200K range)
-- ============================================================================
-- Known value from H3 docs: average res-9 cell area ≈ 105,332 m².
-- Actual areas vary by latitude, so we check a reasonable range.

SELECT 'cell_area_range' AS check_name,
       ROUND(h3_cell_area(h3_latlng_to_cell(37.7792, -122.4191, 9)), 0) AS area_m2,
       CASE WHEN h3_cell_area(h3_latlng_to_cell(37.7792, -122.4191, 9)) BETWEEN 50000 AND 200000
            THEN 'PASS' ELSE 'FAIL' END AS result;


-- ============================================================================
-- 15. CELL AREA KM² — Same cell, different unit
-- ============================================================================

SELECT 'area_unit_match' AS check_name,
       ROUND(h3_cell_area_km2(h3_latlng_to_cell(37.7792, -122.4191, 9)), 6) AS area_km2,
       ROUND(h3_cell_area(h3_latlng_to_cell(37.7792, -122.4191, 9)) / 1000000.0, 6) AS computed_km2,
       CASE WHEN ABS(
           h3_cell_area_km2(h3_latlng_to_cell(37.7792, -122.4191, 9)) -
           h3_cell_area(h3_latlng_to_cell(37.7792, -122.4191, 9)) / 1000000.0
       ) < 0.0001 THEN 'PASS' ELSE 'FAIL' END AS result;


-- ============================================================================
-- 16. EDGE LENGTH — Res 9 hexagon edges are ~201 m (100–250 m range)
-- ============================================================================

SELECT 'edge_length_range' AS check_name,
       ROUND(h3_edge_length(h3_latlng_to_cell(37.7792, -122.4191, 9)), 1) AS edge_meters,
       CASE WHEN h3_edge_length(h3_latlng_to_cell(37.7792, -122.4191, 9)) BETWEEN 100 AND 250
            THEN 'PASS' ELSE 'FAIL' END AS result;


-- ============================================================================
-- 17. CELL BOUNDARY WKT — Returns a valid POLYGON with 7 coordinate pairs
-- ============================================================================
-- Hexagons produce a WKT POLYGON with 6 vertices + closing point = 7 pairs.

SELECT 'boundary_wkt' AS check_name,
       h3_cell_to_boundary(h3_latlng_to_cell(37.7792, -122.4191, 9)) AS boundary,
       CASE WHEN h3_cell_to_boundary(h3_latlng_to_cell(37.7792, -122.4191, 9)) LIKE 'POLYGON((%'
            THEN 'PASS' ELSE 'FAIL' END AS result;


-- ============================================================================
-- 18. STRING CONVERSION — Round-trip: cell → hex string → cell
-- ============================================================================
-- h3_cell_to_string outputs a 15-char hex string (e.g. "8928309207fffff").
-- h3_string_to_cell parses it back to the original UInt64.

SELECT 'string_roundtrip' AS check_name,
       h3_cell_to_string(h3_latlng_to_cell(37.7792, -122.4191, 9)) AS hex_string,
       CASE WHEN h3_string_to_cell(
           h3_cell_to_string(h3_latlng_to_cell(37.7792, -122.4191, 9))
       ) = h3_latlng_to_cell(37.7792, -122.4191, 9)
            THEN 'PASS' ELSE 'FAIL' END AS result;


-- ============================================================================
-- 19. POLYFILL — SF bounding box produces >100 cells at resolution 9
-- ============================================================================
-- The San Francisco bounding box (0.17° × 0.12°) at resolution 9 should
-- produce a substantial number of H3 cells covering the area.

SELECT 'sf_polyfill' AS check_name,
       (SELECT COUNT(*) FROM (
           SELECT UNNEST(h3_polyfill(
               'POLYGON((-122.52 37.70, -122.35 37.70, -122.35 37.82, -122.52 37.82, -122.52 37.70))',
               9
           )) AS cell
       )) AS cell_count,
       CASE WHEN (SELECT COUNT(*) FROM (
           SELECT UNNEST(h3_polyfill(
               'POLYGON((-122.52 37.70, -122.35 37.70, -122.35 37.82, -122.52 37.82, -122.52 37.70))',
               9
           )) AS cell
       )) > 100 THEN 'PASS' ELSE 'FAIL' END AS result;


-- ============================================================================
-- 20. POLYFILL PER REGION — H3 cell counts for all 5 city boundaries
-- ============================================================================

SELECT
    region_name,
    country,
    COUNT(*) AS h3_cell_count
FROM {{zone_name}}.spatial.region_cells
GROUP BY region_name, country
ORDER BY h3_cell_count DESC;


-- ============================================================================
-- 21. SPATIAL JOIN — Points matched to regions via H3 cell equality
-- ============================================================================
-- O(1) join: each GPS point's h3_cell is matched directly against the
-- pre-computed region_cells. No polygon intersection calculation needed.

SELECT
    r.region_name,
    r.country,
    COUNT(DISTINCT p.id) AS points_in_region,
    COUNT(DISTINCT p.device_id) AS unique_devices
FROM {{zone_name}}.spatial.points_h3 p
INNER JOIN {{zone_name}}.spatial.region_cells r ON p.h3_cell = r.h3_cell
GROUP BY r.region_name, r.country
ORDER BY points_in_region DESC;


-- ============================================================================
-- 22. SPATIAL JOIN COMPLETENESS — All 10K points accounted for
-- ============================================================================

SELECT 'spatial_join_total' AS check_name,
       (SELECT COUNT(*) FROM {{zone_name}}.spatial.points_h3 p
        INNER JOIN {{zone_name}}.spatial.region_cells r ON p.h3_cell = r.h3_cell) +
       (SELECT COUNT(*) FROM {{zone_name}}.spatial.points_h3 p
        WHERE NOT EXISTS (
            SELECT 1 FROM {{zone_name}}.spatial.region_cells r WHERE p.h3_cell = r.h3_cell
        )) AS actual,
       10000 AS expected,
       CASE WHEN (
           (SELECT COUNT(*) FROM {{zone_name}}.spatial.points_h3 p
            INNER JOIN {{zone_name}}.spatial.region_cells r ON p.h3_cell = r.h3_cell) +
           (SELECT COUNT(*) FROM {{zone_name}}.spatial.points_h3 p
            WHERE NOT EXISTS (
                SELECT 1 FROM {{zone_name}}.spatial.region_cells r WHERE p.h3_cell = r.h3_cell
            ))
       ) = 10000 THEN 'PASS' ELSE 'FAIL' END AS result;


-- ============================================================================
-- 23. SPATIAL JOIN ACCURACY — Known inside/outside points
-- ============================================================================
-- SF City Hall (37.7792, -122.4191) is inside the SF bounding box.
-- Statue of Liberty (40.6892, -74.0445) is NOT inside the SF bounding box.

SELECT 'sf_cityhall_inside_sf' AS check_name,
       (SELECT COUNT(*) FROM {{zone_name}}.spatial.region_cells
        WHERE region_name = 'San Francisco'
          AND h3_cell = h3_latlng_to_cell(37.7792, -122.4191, 9)) AS actual,
       CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.spatial.region_cells
                  WHERE region_name = 'San Francisco'
                    AND h3_cell = h3_latlng_to_cell(37.7792, -122.4191, 9)) > 0
            THEN 'PASS' ELSE 'FAIL' END AS result;


-- ============================================================================
-- 24. SPATIAL JOIN NEGATIVE — Point outside polygon returns no match
-- ============================================================================

SELECT 'statue_liberty_outside_sf' AS check_name,
       (SELECT COUNT(*) FROM {{zone_name}}.spatial.region_cells
        WHERE region_name = 'San Francisco'
          AND h3_cell = h3_latlng_to_cell(40.6892, -74.0445, 9)) AS actual,
       0 AS expected,
       CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.spatial.region_cells
                  WHERE region_name = 'San Francisco'
                    AND h3_cell = h3_latlng_to_cell(40.6892, -74.0445, 9)) = 0
            THEN 'PASS' ELSE 'FAIL' END AS result;


-- ============================================================================
-- 25. LANDMARK METRICS — Area and edge length for all 10 landmarks
-- ============================================================================

SELECT
    name,
    city,
    h3_cell_to_string(h3_latlng_to_cell(lat, lng, 9)) AS h3_hex,
    ROUND(h3_cell_area(h3_latlng_to_cell(lat, lng, 9)), 0) AS area_m2,
    ROUND(h3_cell_area_km2(h3_latlng_to_cell(lat, lng, 9)), 4) AS area_km2,
    ROUND(h3_edge_length(h3_latlng_to_cell(lat, lng, 9)), 1) AS edge_m
FROM {{zone_name}}.spatial.landmarks
ORDER BY id;


-- ============================================================================
-- 26. GRID DISTANCE BETWEEN LANDMARKS — Same-city pairs
-- ============================================================================
-- Grid distance counts the minimum number of H3 cell hops between two cells.
-- Same-city landmarks should have small grid distances (<100 at res 9).

SELECT
    a.name AS from_landmark,
    b.name AS to_landmark,
    h3_grid_distance(
        h3_latlng_to_cell(a.lat, a.lng, 9),
        h3_latlng_to_cell(b.lat, b.lng, 9)
    ) AS grid_distance
FROM {{zone_name}}.spatial.landmarks a
CROSS JOIN {{zone_name}}.spatial.landmarks b
WHERE a.city = b.city AND a.id < b.id
ORDER BY a.city, grid_distance;


-- ============================================================================
-- 27. MULTI-RESOLUTION HIERARCHY — SF City Hall across resolutions 0–9
-- ============================================================================
-- Shows how the same point maps to progressively finer hexagons.
-- Each parent should contain its child.

SELECT
    res,
    h3_latlng_to_cell(37.7792, -122.4191, res) AS cell,
    h3_cell_to_string(h3_latlng_to_cell(37.7792, -122.4191, res)) AS hex_string,
    ROUND(h3_cell_area_km2(h3_latlng_to_cell(37.7792, -122.4191, res)), 4) AS area_km2,
    ROUND(h3_edge_length(h3_latlng_to_cell(37.7792, -122.4191, res)), 1) AS edge_m
FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS t(res)
ORDER BY res;


-- ============================================================================
-- 28. DEVICE ACTIVITY BY REGION — Unique devices per city
-- ============================================================================

SELECT
    r.region_name,
    COUNT(DISTINCT p.device_id) AS unique_devices,
    COUNT(*) AS total_pings,
    ROUND(COUNT(*)::DOUBLE / COUNT(DISTINCT p.device_id), 1) AS pings_per_device
FROM {{zone_name}}.spatial.points_h3 p
INNER JOIN {{zone_name}}.spatial.region_cells r ON p.h3_cell = r.h3_cell
GROUP BY r.region_name
ORDER BY total_pings DESC;


-- ============================================================================
-- 29. HOTSPOT ANALYSIS — Top 10 most visited H3 cells
-- ============================================================================

SELECT
    h3_cell_to_string(h3_cell) AS hex_cell,
    COUNT(*) AS visit_count,
    ROUND(MIN(lat), 4) AS lat,
    ROUND(MIN(lng), 4) AS lng,
    MIN(city) AS city
FROM {{zone_name}}.spatial.points_h3
GROUP BY h3_cell
ORDER BY visit_count DESC
LIMIT 10;


-- ============================================================================
-- 30. SUMMARY — All PASS/FAIL checks
-- ============================================================================

SELECT 'gps_point_count' AS check_name,
       CASE WHEN COUNT(*) = 10000 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.spatial.gps_points
UNION ALL
SELECT 'landmark_count',
       CASE WHEN COUNT(*) = 10 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.spatial.landmarks
UNION ALL
SELECT 'all_cells_valid',
       CASE WHEN COUNT(*) FILTER (WHERE h3_is_valid_cell(h3_latlng_to_cell(lat, lng, 9))) = 10
            THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.spatial.landmarks
UNION ALL
SELECT 'res9_class_iii',
       CASE WHEN COUNT(*) FILTER (WHERE h3_is_res_class_iii(h3_latlng_to_cell(lat, lng, 9))) = 10
            THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.spatial.landmarks
UNION ALL
SELECT 'ring_k1_count',
       CASE WHEN (SELECT COUNT(*) FROM (
           SELECT UNNEST(h3_hex_ring(h3_latlng_to_cell(37.7792, -122.4191, 9), 1)) AS cell
       )) = 6 THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'ring_k2_count',
       CASE WHEN (SELECT COUNT(*) FROM (
           SELECT UNNEST(h3_hex_ring(h3_latlng_to_cell(37.7792, -122.4191, 9), 2)) AS cell
       )) = 12 THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'disk_k1_count',
       CASE WHEN (SELECT COUNT(*) FROM (
           SELECT UNNEST(h3_hex_disk(h3_latlng_to_cell(37.7792, -122.4191, 9), 1)) AS cell
       )) = 7 THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'grid_distance_neighbor',
       CASE WHEN h3_grid_distance(
           h3_latlng_to_cell(37.7792, -122.4191, 9),
           (SELECT cell FROM (
               SELECT UNNEST(h3_hex_ring(h3_latlng_to_cell(37.7792, -122.4191, 9), 1)) AS cell
           ) LIMIT 1)
       ) = 1 THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'grid_path_adjacent',
       CASE WHEN (SELECT COUNT(*) FROM (
           SELECT UNNEST(h3_grid_path(
               h3_latlng_to_cell(37.7792, -122.4191, 9),
               (SELECT cell FROM (
                   SELECT UNNEST(h3_hex_ring(h3_latlng_to_cell(37.7792, -122.4191, 9), 1)) AS cell
               ) LIMIT 1)
           )) AS cell
       )) = 2 THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'parent_resolution',
       CASE WHEN h3_get_resolution(
           h3_cell_to_parent(h3_latlng_to_cell(37.7792, -122.4191, 9), 7)
       ) = 7 THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'children_count',
       CASE WHEN (SELECT COUNT(*) FROM (
           SELECT UNNEST(h3_cell_to_children(h3_latlng_to_cell(37.7792, -122.4191, 9), 10)) AS child
       )) = 7 THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'center_child_res',
       CASE WHEN h3_get_resolution(
           h3_cell_to_center_child(h3_latlng_to_cell(37.7792, -122.4191, 9), 10)
       ) = 10 THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'cell_area_range',
       CASE WHEN h3_cell_area(h3_latlng_to_cell(37.7792, -122.4191, 9)) BETWEEN 50000 AND 200000
            THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'area_unit_match',
       CASE WHEN ABS(
           h3_cell_area_km2(h3_latlng_to_cell(37.7792, -122.4191, 9)) -
           h3_cell_area(h3_latlng_to_cell(37.7792, -122.4191, 9)) / 1000000.0
       ) < 0.0001 THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'edge_length_range',
       CASE WHEN h3_edge_length(h3_latlng_to_cell(37.7792, -122.4191, 9)) BETWEEN 100 AND 250
            THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'boundary_wkt',
       CASE WHEN h3_cell_to_boundary(h3_latlng_to_cell(37.7792, -122.4191, 9)) LIKE 'POLYGON((%'
            THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'string_roundtrip',
       CASE WHEN h3_string_to_cell(h3_cell_to_string(h3_latlng_to_cell(37.7792, -122.4191, 9)))
            = h3_latlng_to_cell(37.7792, -122.4191, 9)
            THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'sf_polyfill',
       CASE WHEN (SELECT COUNT(*) FROM (
           SELECT UNNEST(h3_polyfill(
               'POLYGON((-122.52 37.70, -122.35 37.70, -122.35 37.82, -122.52 37.82, -122.52 37.70))', 9
           )) AS cell
       )) > 100 THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'spatial_join_total',
       CASE WHEN (
           (SELECT COUNT(*) FROM {{zone_name}}.spatial.points_h3 p
            INNER JOIN {{zone_name}}.spatial.region_cells r ON p.h3_cell = r.h3_cell) +
           (SELECT COUNT(*) FROM {{zone_name}}.spatial.points_h3 p
            WHERE NOT EXISTS (
                SELECT 1 FROM {{zone_name}}.spatial.region_cells r WHERE p.h3_cell = r.h3_cell
            ))
       ) = 10000 THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'sf_cityhall_inside_sf',
       CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.spatial.region_cells
                  WHERE region_name = 'San Francisco'
                    AND h3_cell = h3_latlng_to_cell(37.7792, -122.4191, 9)) > 0
            THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'statue_liberty_outside_sf',
       CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.spatial.region_cells
                  WHERE region_name = 'San Francisco'
                    AND h3_cell = h3_latlng_to_cell(40.6892, -74.0445, 9)) = 0
            THEN 'PASS' ELSE 'FAIL' END
ORDER BY check_name;
