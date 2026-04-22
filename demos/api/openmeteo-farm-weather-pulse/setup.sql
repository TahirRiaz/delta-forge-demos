-- ============================================================================
-- Demo: Farm Weather Pulse — Query-Param Overrides via USING
-- Feature: INVOKE ... USING (query_param.* = $x) + SET $name = <expr>
--          standalone script params. Same endpoint hit three times with
--          three sets of coordinates.
-- ============================================================================
--
-- Real-world story: an agritech platform runs a weather-monitoring
-- service across 3 partner farms in Northern Europe. The agronomy team
-- reads the shared bronze table every morning to correlate yields with
-- overnight temperature, humidity, and precipitation.
--
-- ONE endpoint. THREE farms. THREE INVOKE lines.
--
-- The endpoint has NO hardcoded coordinates — only the shared knobs
-- (current= fields, timezone= UTC). Each INVOKE supplies
-- `query_param.latitude` + `query_param.longitude` via USING, pulled
-- from script-scoped `$lat_X` / `$lon_X` parameters. Adding a fourth
-- farm is two SETs + one INVOKE line added to this file — no ALTER
-- API ENDPOINT needed.
--
-- IMPORTANT: this demo's statements share script-scoped parameters. The
-- demo harness MUST execute this file as a SINGLE multi-statement script
-- (one `execute_script_stream` call), not statement-by-statement. The
-- script param bag is cleared between script invocations, so splitting
-- the SETs and INVOKEs across separate HTTP calls would wipe $lat_X /
-- $lon_X between statements.
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'Bronze landing zone for REST API ingests';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.agri_telemetry
    COMMENT 'Agronomy weather telemetry for partner farms';

-- --------------------------------------------------------------------------
-- 2. REST API connection — Open-Meteo public forecast API
-- --------------------------------------------------------------------------

CREATE CONNECTION IF NOT EXISTS openmeteo_api
    TYPE = rest_api
    OPTIONS (
        base_url     = 'https://api.open-meteo.com',
        auth_mode    = 'none',
        storage_zone = '{{zone_name}}',
        base_path    = 'openmeteo_api',
        timeout_secs = '30'
    );

-- --------------------------------------------------------------------------
-- 3. Single endpoint — shared options only, no per-farm coordinates
-- --------------------------------------------------------------------------
-- `query_param.current` and `query_param.timezone` are the shared
-- knobs every farm uses the same way. latitude + longitude are
-- DELIBERATELY omitted — each INVOKE supplies them via USING.

CREATE API ENDPOINT {{zone_name}}.openmeteo_api.current_observation
    URL '/v1/forecast'
    RESPONSE FORMAT JSON
    OPTIONS (
        query_param.current  = 'temperature_2m,wind_speed_10m,relative_humidity_2m,precipitation',
        query_param.timezone = 'UTC',
        rate_limit_rps       = '2'
    );

-- --------------------------------------------------------------------------
-- 4. Per-farm coordinates as script params
-- --------------------------------------------------------------------------
-- SET $x = <scalar-expr> binds a script-scoped parameter. The parameter
-- bag lives for the duration of this multi-statement script. Standalone
-- floats are scalar expressions — DataFusion evaluates them at SET time
-- and stores the resolved ScalarValue. Each INVOKE below references
-- two of these params by name.

SET $lat_oslo     = 59.91;
SET $lon_oslo     = 10.75;

SET $lat_hamburg  = 53.55;
SET $lon_hamburg  = 9.99;

SET $lat_dublin   = 53.35;
SET $lon_dublin   = -6.26;

-- --------------------------------------------------------------------------
-- 5. Three INVOKEs with per-farm USING (...) overrides
-- --------------------------------------------------------------------------
-- Each INVOKE merges its `query_param.latitude / .longitude` overrides
-- on top of the endpoint's stored options. The engine evaluates each
-- expression (`$lat_oslo` → ScalarValue → URL-encoded string), assembles
-- the full URL, and fetches:
--     /v1/forecast?latitude=59.91&longitude=10.75
--                 &current=...&timezone=UTC
-- Each run writes a distinct JSON page into the per-run folder.

INVOKE API ENDPOINT {{zone_name}}.openmeteo_api.current_observation
    USING (
        query_param.latitude  = $lat_oslo,
        query_param.longitude = $lon_oslo
    );

INVOKE API ENDPOINT {{zone_name}}.openmeteo_api.current_observation
    USING (
        query_param.latitude  = $lat_hamburg,
        query_param.longitude = $lon_hamburg
    );

INVOKE API ENDPOINT {{zone_name}}.openmeteo_api.current_observation
    USING (
        query_param.latitude  = $lat_dublin,
        query_param.longitude = $lon_dublin
    );

-- --------------------------------------------------------------------------
-- 6. External table — flatten the nested `$.current` block
-- --------------------------------------------------------------------------
-- Open-Meteo wraps current observations in a `$.current` sub-object
-- sibling to the top-level coordinates. The flatten's column_mappings
-- descend into that sub-object to produce a wide, flat one-row-per-
-- farm shape.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.agri_telemetry.weather_bronze
USING JSON
LOCATION 'openmeteo_api/current_observation'
OPTIONS (
    recursive = 'true',
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.latitude",
            "$.longitude",
            "$.elevation",
            "$.timezone",
            "$.current.time",
            "$.current.temperature_2m",
            "$.current.wind_speed_10m",
            "$.current.relative_humidity_2m",
            "$.current.precipitation"
        ],
        "column_mappings": {
            "$.latitude":                    "latitude",
            "$.longitude":                   "longitude",
            "$.elevation":                   "elevation_m",
            "$.timezone":                    "timezone",
            "$.current.time":                "observation_time",
            "$.current.temperature_2m":      "temperature_c",
            "$.current.wind_speed_10m":      "wind_speed_kmh",
            "$.current.relative_humidity_2m":"humidity_pct",
            "$.current.precipitation":       "precipitation_mm"
        },
        "max_depth": 3,
        "separator": "_",
        "infer_types": true
    }'
);

DETECT SCHEMA FOR TABLE {{zone_name}}.agri_telemetry.weather_bronze;
GRANT ADMIN ON TABLE {{zone_name}}.agri_telemetry.weather_bronze TO USER {{current_user}};

-- --------------------------------------------------------------------------
-- 7. Silver Delta table — typed promotion with farm_name lookup
-- --------------------------------------------------------------------------
-- The agronomy team's dashboards want a farm_name column they can
-- group by — matching each lat/lon to the canonical name via a CASE
-- expression at promotion. Typed columns (DOUBLE for temperature, etc.)
-- let downstream predicates work without casting.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.agri_telemetry.weather_silver (
    farm_name         STRING,
    latitude          DOUBLE,
    longitude         DOUBLE,
    elevation_m       DOUBLE,
    observation_time  STRING,
    temperature_c     DOUBLE,
    wind_speed_kmh    DOUBLE,
    humidity_pct      DOUBLE,
    precipitation_mm  DOUBLE
)
LOCATION 'silver/farm_weather';

INSERT INTO {{zone_name}}.agri_telemetry.weather_silver
SELECT
    CASE
        WHEN latitude BETWEEN 59 AND 60 THEN 'oslo'
        WHEN latitude BETWEEN 53.4 AND 53.8 AND longitude > 9 THEN 'hamburg'
        WHEN latitude BETWEEN 53.0 AND 53.5 AND longitude < 0 THEN 'dublin'
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

GRANT ADMIN ON TABLE {{zone_name}}.agri_telemetry.weather_silver TO USER {{current_user}};
