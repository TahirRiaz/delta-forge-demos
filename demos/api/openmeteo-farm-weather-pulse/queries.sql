-- ============================================================================
-- Demo: Farm Weather Pulse, Queries
-- ============================================================================
-- This file is where the three per-farm endpoints are actually
-- exercised. Registry inspection, the three INVOKE calls, the
-- per-endpoint run audits, schema detection, and the bronze->silver
-- promotion all live here so the user sees the multi-endpoint fan-out
-- end to end from a single file, before the assertions that prove
-- each farm's coordinates round-tripped correctly.
--
-- Validates per-farm endpoint fan-out end to end:
--   - Exactly 3 rows, one per INVOKE, one per farm.
--   - Each farm's latitude/longitude round-trips correctly: the URL
--     coordinates reached Open-Meteo, came back in the response, and
--     the JSON flatten preserved them.
--   - CASE-based farm_name mapping in silver correctly labels all 3
--     rows, no 'unknown' leak.
--   - All weather values are in physically-plausible ranges (no NaN /
--     negative humidity / 500 deg C temperature sneaking through).
--   - observation_time is Open-Meteo's ISO-8601 minute precision shape.
--   - Silver has at least v0+v1 Delta versions.
--
-- Stability note: weather values (temperature, wind, precipitation) are
-- time-varying and cannot be exact-asserted. Assertions here check
-- physical-plausibility ranges rather than specific values, the
-- rigorous invariants that every weather source should satisfy.
-- ============================================================================

-- ============================================================================
-- API surface, calling the endpoints from SQL
-- ============================================================================

-- Confirm all three farm endpoints exist under one connection.
SHOW API ENDPOINTS IN CONNECTION {{zone_name}}.openmeteo_api;

-- Three INVOKEs, one per farm. Each writes a distinct JSON page into
-- its endpoint's per-run folder.
INVOKE API ENDPOINT {{zone_name}}.openmeteo_api.observation_oslo;
INVOKE API ENDPOINT {{zone_name}}.openmeteo_api.observation_hamburg;
INVOKE API ENDPOINT {{zone_name}}.openmeteo_api.observation_dublin;

-- Per-endpoint run audit, each endpoint has its own history.
SHOW API ENDPOINT RUNS {{zone_name}}.openmeteo_api.observation_oslo LIMIT 5;
SHOW API ENDPOINT RUNS {{zone_name}}.openmeteo_api.observation_hamburg LIMIT 5;
SHOW API ENDPOINT RUNS {{zone_name}}.openmeteo_api.observation_dublin LIMIT 5;

-- Resolve the bronze schema from the freshly written JSON.
DETECT SCHEMA FOR TABLE {{zone_name}}.agri_telemetry.weather_bronze;

-- Bronze -> silver promotion with CASE-based farm_name lookup.
INSERT INTO {{zone_name}}.agri_telemetry.weather_silver
SELECT
    CASE
        WHEN CAST(longitude AS DOUBLE) BETWEEN 10.5 AND 11 THEN 'oslo'
        WHEN CAST(longitude AS DOUBLE) BETWEEN 9.5 AND 10.5 THEN 'hamburg'
        WHEN CAST(longitude AS DOUBLE) BETWEEN -7 AND -5 THEN 'dublin'
        ELSE 'unknown'
    END                                 AS farm_name,
    CAST(latitude AS DOUBLE)            AS latitude,
    CAST(longitude AS DOUBLE)           AS longitude,
    CAST(elevation_m AS DOUBLE)         AS elevation_m,
    observation_time,
    CAST(temperature_c AS DOUBLE)       AS temperature_c,
    CAST(wind_speed_kmh AS DOUBLE)      AS wind_speed_kmh,
    CAST(humidity_pct AS DOUBLE)        AS humidity_pct,
    CAST(precipitation_mm AS DOUBLE)    AS precipitation_mm
FROM {{zone_name}}.agri_telemetry.weather_bronze;

-- ============================================================================
-- Query 1: Farm Count, 3 INVOKEs -> 3 rows
-- ============================================================================
-- Anything other than 3 means one INVOKE failed silently (unlikely,
-- INVOKE failures surface as user errors) or a JSON flatten regression
-- dropped a row.

ASSERT ROW_COUNT = 1
ASSERT VALUE farm_count = 3
SELECT COUNT(*) AS farm_count
FROM {{zone_name}}.agri_telemetry.weather_bronze;

-- ============================================================================
-- Query 2: Farm-Name Classification, CASE resolved all 3 sites
-- ============================================================================
-- Silver's CASE expression keys off latitude+longitude to assign the
-- canonical farm name. Each site must get exactly 1 row. A non-zero
-- unknown_rows means the URL coordinates were wrong or the CASE bands
-- are misaligned.

ASSERT ROW_COUNT = 1
ASSERT VALUE oslo_rows = 1
ASSERT VALUE hamburg_rows = 1
ASSERT VALUE dublin_rows = 1
ASSERT VALUE unknown_rows = 0
SELECT
    SUM(CASE WHEN farm_name = 'oslo'    THEN 1 ELSE 0 END) AS oslo_rows,
    SUM(CASE WHEN farm_name = 'hamburg' THEN 1 ELSE 0 END) AS hamburg_rows,
    SUM(CASE WHEN farm_name = 'dublin'  THEN 1 ELSE 0 END) AS dublin_rows,
    SUM(CASE WHEN farm_name = 'unknown' THEN 1 ELSE 0 END) AS unknown_rows
FROM {{zone_name}}.agri_telemetry.weather_silver;

-- ============================================================================
-- Query 3: Coordinate Round-Trip, URL -> response -> flatten
-- ============================================================================
-- Open-Meteo echoes the requested coordinates back in the response
-- (it may round to its internal grid, but never off by more than 0.1).
-- Each farm's latitude/longitude must land in the expected band. If
-- Oslo came back at 53.x latitude instead of 59.x, the wrong endpoint
-- response landed under that endpoint's folder, a correctness-critical
-- regression.

ASSERT ROW_COUNT = 1
ASSERT VALUE lat_in_oslo_band = 1
ASSERT VALUE lat_in_hamburg_band = 1
ASSERT VALUE lat_in_dublin_band = 1
ASSERT VALUE lon_in_dublin_band = 1
SELECT
    MAX(CASE WHEN farm_name = 'oslo'    AND latitude BETWEEN 59 AND 60    THEN 1 ELSE 0 END) AS lat_in_oslo_band,
    MAX(CASE WHEN farm_name = 'hamburg' AND latitude BETWEEN 53.4 AND 53.8 THEN 1 ELSE 0 END) AS lat_in_hamburg_band,
    MAX(CASE WHEN farm_name = 'dublin'  AND latitude BETWEEN 53.0 AND 53.5 THEN 1 ELSE 0 END) AS lat_in_dublin_band,
    MAX(CASE WHEN farm_name = 'dublin'  AND longitude BETWEEN -7 AND -5    THEN 1 ELSE 0 END) AS lon_in_dublin_band
FROM {{zone_name}}.agri_telemetry.weather_silver;

-- ============================================================================
-- Query 4: Physical-Plausibility Ranges, every weather value sane
-- ============================================================================
-- Temperature -50..60 deg C is the livable-on-Earth surface band.
-- Humidity 0..100 is definitional. Wind and precipitation are non-
-- negative (both are absolute quantities, not deltas). Anything
-- outside these ranges means a JSON flatten type-cast regression.

ASSERT ROW_COUNT = 1
ASSERT VALUE plausible_temps = 3
ASSERT VALUE non_negative_humidity = 3
ASSERT VALUE non_negative_wind = 3
ASSERT VALUE non_negative_precip = 3
ASSERT VALUE humidity_upper_band = 3
SELECT
    SUM(CASE WHEN temperature_c BETWEEN -50 AND 60 THEN 1 ELSE 0 END) AS plausible_temps,
    SUM(CASE WHEN humidity_pct >= 0                THEN 1 ELSE 0 END) AS non_negative_humidity,
    SUM(CASE WHEN wind_speed_kmh >= 0              THEN 1 ELSE 0 END) AS non_negative_wind,
    SUM(CASE WHEN precipitation_mm >= 0            THEN 1 ELSE 0 END) AS non_negative_precip,
    SUM(CASE WHEN humidity_pct <= 100              THEN 1 ELSE 0 END) AS humidity_upper_band
FROM {{zone_name}}.agri_telemetry.weather_silver;

-- ============================================================================
-- Query 5: Timestamp Shape & Timezone, ISO-minute, UTC/GMT
-- ============================================================================
-- Open-Meteo's `current.time` is ISO-8601 at minute precision:
-- `YYYY-MM-DDTHH:MM`. The URL pins `timezone=UTC` so `timezone` comes
-- back as UTC (or GMT if the API aliases). Any other timezone means
-- the URL param didn't reach the server.

ASSERT ROW_COUNT = 1
ASSERT VALUE iso_observations = 3
ASSERT VALUE utc_tz = 3
SELECT
    SUM(CASE WHEN observation_time LIKE '20__-__-__T__:__'    THEN 1 ELSE 0 END) AS iso_observations,
    SUM(CASE WHEN timezone IN ('UTC', 'GMT')                  THEN 1 ELSE 0 END) AS utc_tz
FROM {{zone_name}}.agri_telemetry.weather_bronze;

-- ============================================================================
-- Query 6: Silver Delta History, v0 schema + v1 INSERT
-- ============================================================================

ASSERT ROW_COUNT >= 2
DESCRIBE HISTORY {{zone_name}}.agri_telemetry.weather_silver;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting: 3 farms, 3 distinct coordinates, all 3 labeled, every
-- temperature and humidity value physically plausible.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_farms = 3
ASSERT VALUE distinct_lats = 3
ASSERT VALUE distinct_lons = 3
ASSERT VALUE labeled_farms = 3
ASSERT VALUE plausible_temps_all = 1
ASSERT VALUE plausible_humidity_all = 1
SELECT
    COUNT(*)                                                                    AS total_farms,
    COUNT(DISTINCT latitude)                                                    AS distinct_lats,
    COUNT(DISTINCT longitude)                                                   AS distinct_lons,
    SUM(CASE WHEN farm_name IN ('oslo','hamburg','dublin') THEN 1 ELSE 0 END)   AS labeled_farms,
    CASE WHEN MIN(temperature_c) >= -50 AND MAX(temperature_c) <= 60
         THEN 1 ELSE 0 END                                                      AS plausible_temps_all,
    CASE WHEN MIN(humidity_pct) >= 0   AND MAX(humidity_pct) <= 100
         THEN 1 ELSE 0 END                                                      AS plausible_humidity_all
FROM {{zone_name}}.agri_telemetry.weather_silver;
