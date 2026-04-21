-- ============================================================================
-- Demo: European Travel — Incremental Wave-Based Enrichment
-- Feature: Parameter-driven REST API ingest + target-table watermark lookup
-- ============================================================================
--
-- Real-world story: a travel-tech startup rolls out its country reference
-- catalog in geographical "waves". The Nordic wave (NO, SE, DK, IS) was
-- loaded at launch (seeded directly into the silver catalog below). This
-- demo runs the second wave — Finland plus the Baltics (FI, EE, LV, LT) —
-- and walks the parameter-driven incremental pattern the operations team
-- runs every week to pick up the next region:
--
--   1. Inspect the target silver table for the ISO codes already loaded.
--      queries.sql Query 4 runs the exact anti-join a production pipeline
--      would use to compute "codes wanted, not yet present".
--   2. Set the ingest's query_param.codes to those missing codes. In a
--      pipeline, an ALTER API INGEST ... SET OPTIONS step does this at
--      run time; here we encode the wave directly on CREATE so the demo
--      is self-contained and idempotent.
--   3. INVOKE the REST Countries /v3.1/alpha endpoint, which takes
--      ?codes=<comma-list> and returns only those rows — a classic
--      "narrow fetch driven by a target-table read" shape.
--   4. Merge bronze into silver with a NOT EXISTS guard so a replay is
--      a no-op instead of a double-insert.
--
-- Delta Forge mechanics this exercises:
--   • Bearer credential in the OS keychain (CREATE CREDENTIAL)
--   • REST API data source (CREATE CONNECTION TYPE = rest_api)
--   • Query-parameter binding via OPTIONS (query_param.<name> = ...)
--     on CREATE API INGEST — the headline "parameter" feature
--   • INVOKE API INGEST with a configured query parameter, fetching a
--     narrow wire response instead of the full region catalog
--   • Anti-join INSERT pattern for idempotent incremental merge into a
--     pre-existing Delta target table
--
-- NOTE: requires internet. INVOKE issues a real GET against
-- https://restcountries.com/v3.1/alpha?codes=fi,ee,lv,lt.
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Credential (OS keychain — the always-on default vault)
-- --------------------------------------------------------------------------
-- REST Countries v3.1 is public, so the literal SECRET below is a
-- placeholder. The same wiring applies verbatim to bearer-protected
-- APIs (GitHub, Stripe, a partner's gated endpoint, etc.) — only the
-- literal secret value changes. Exercising the credential path even
-- for a public API is deliberate: it means the demo's wiring is the
-- real production wiring.

CREATE CREDENTIAL IF NOT EXISTS travel_incremental_token
    TYPE = CREDENTIAL
    SECRET 'demo-placeholder-token-restcountries-is-public'
    DESCRIPTION 'Bearer token for the incremental travel-catalog wave sync';

-- --------------------------------------------------------------------------
-- 2. Zone + schema
-- --------------------------------------------------------------------------
-- `travel_waves` is a separate schema from the reference-catalog demo's
-- `travel_geo` so both demos can live in the same `bronze` zone without
-- colliding on table names.

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'Bronze landing zone for REST API ingests';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.travel_waves
    COMMENT 'Country catalog — wave-based progressive enrichment from REST APIs';

-- --------------------------------------------------------------------------
-- 3. Silver catalog — seeded with the launch Nordic wave
-- --------------------------------------------------------------------------
-- This Delta table is the INCREMENTAL SOURCE OF TRUTH: every subsequent
-- wave queries THIS table to decide which codes it still needs to fetch
-- from the API. The Nordic wave (NO, SE, DK, IS) is the launch seed —
-- pre-existing before any API call runs.
--
-- `source_batch` tags each row with the wave that loaded it, giving
-- every row a provenance label that queries.sql uses to verify the
-- wave composition end-to-end. In real deployments this column is
-- typically a `source_system` + `ingest_run_id` pair.
--
-- Population values are rounded to recent-census approximations; the
-- exact figures don't matter for this demo — wave provenance does.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.travel_waves.country_catalog (
    cca2         STRING,
    cca3         STRING,
    name_common  STRING,
    region       STRING,
    subregion    STRING,
    population   BIGINT,
    source_batch STRING
)
LOCATION 'silver/country_catalog';

INSERT INTO {{zone_name}}.travel_waves.country_catalog VALUES
    ('NO', 'NOR', 'Norway',  'Europe', 'Northern Europe', 5379475,  'nordic_seed'),
    ('SE', 'SWE', 'Sweden',  'Europe', 'Northern Europe', 10353442, 'nordic_seed'),
    ('DK', 'DNK', 'Denmark', 'Europe', 'Northern Europe', 5831404,  'nordic_seed'),
    ('IS', 'ISL', 'Iceland', 'Europe', 'Northern Europe', 366425,   'nordic_seed');

GRANT ADMIN ON TABLE {{zone_name}}.travel_waves.country_catalog TO USER {{current_user}};

-- --------------------------------------------------------------------------
-- 4. REST API connection
-- --------------------------------------------------------------------------
-- Independent from the reference-catalog demo's `rest_countries`
-- connection so the two demos can coexist and be cleaned up
-- independently. `base_path = 'rest_countries_waves'` gives this demo
-- its own landing subtree under the bronze zone.

CREATE CONNECTION IF NOT EXISTS rest_countries_waves
    TYPE = rest_api
    OPTIONS (
        base_url     = 'https://restcountries.com',
        auth_mode    = 'bearer',
        storage_zone = '{{zone_name}}',
        base_path    = 'rest_countries_waves',
        timeout_secs = '30'
    )
    CREDENTIAL = travel_incremental_token;

-- --------------------------------------------------------------------------
-- 5. API ingest — /v3.1/alpha with a configurable `codes` query parameter
-- --------------------------------------------------------------------------
-- /v3.1/alpha accepts `?codes=<comma-list>` and returns just those
-- countries — a pinpoint batch-lookup endpoint, exactly the shape you
-- want for a narrow "fetch only what's missing" incremental run.
--
-- query_param.codes below is the FIRST wave to run: fi,ee,lv,lt. Those
-- four codes are precisely the ones NOT present in the Nordic silver
-- seed, computed by the anti-join in queries.sql Query 4. In a
-- production pipeline that ratchet is automated:
--
--     ALTER API INGEST bronze.rest_countries_waves.alpha_batch
--         SET OPTIONS (query_param.codes = '<codes-from-anti-join>');
--     INVOKE API INGEST bronze.rest_countries_waves.alpha_batch;
--
-- Here we encode the wave at CREATE time so the demo stays idempotent:
-- every re-run fetches the same four codes, the NOT EXISTS guard at the
-- merge step absorbs the overlap, and the silver catalog ends up with
-- the same eight rows regardless of run count.
--
-- query_param.fields is REST Countries' "trim the response" knob —
-- asking for only the fields we'll actually flatten keeps the wire
-- payload small. That's demonstrating the SECOND value of query
-- parameters: shaping the response, not just the filter.

CREATE API INGEST {{zone_name}}.rest_countries_waves.alpha_batch
    ENDPOINT '/v3.1/alpha'
    RESPONSE FORMAT JSON
    OPTIONS (
        query_param.codes = 'fi,ee,lv,lt',
        query_param.fields = 'name,cca2,cca3,region,subregion,population'
    );

-- --------------------------------------------------------------------------
-- 6. INVOKE — live HTTPS fetch, narrowed by the query param
-- --------------------------------------------------------------------------
-- The engine assembles the URL as
--   https://restcountries.com/v3.1/alpha?codes=fi,ee,lv,lt&fields=...
-- and writes the single-page JSON response to
--   <zone-root>/rest_countries_waves/alpha_batch/<run-ts>/page_0001.json.

INVOKE API INGEST {{zone_name}}.rest_countries_waves.alpha_batch;

-- --------------------------------------------------------------------------
-- 7. Bronze external table over the landed JSON
-- --------------------------------------------------------------------------
-- Only the fields we'll merge into silver are projected. `recursive`
-- walks timestamped per-run subfolders so a second INVOKE (future wave)
-- would automatically be readable without schema changes.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.travel_waves.alpha_batch_bronze
USING JSON
LOCATION 'rest_countries_waves/alpha_batch'
OPTIONS (
    recursive = 'true',
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.name.common",
            "$.cca2",
            "$.cca3",
            "$.region",
            "$.subregion",
            "$.population"
        ],
        "column_mappings": {
            "$.name.common": "name_common",
            "$.cca2":        "cca2",
            "$.cca3":        "cca3",
            "$.region":      "region",
            "$.subregion":   "subregion",
            "$.population":  "population"
        },
        "max_depth": 3,
        "separator": "_",
        "infer_types": true
    }'
);

DETECT SCHEMA FOR TABLE {{zone_name}}.travel_waves.alpha_batch_bronze;
GRANT ADMIN ON TABLE {{zone_name}}.travel_waves.alpha_batch_bronze TO USER {{current_user}};

-- --------------------------------------------------------------------------
-- 8. Anti-join merge — bronze → silver, skipping codes already in silver
-- --------------------------------------------------------------------------
-- NOT EXISTS is the idempotency guard. Re-running this INSERT without a
-- fresh INVOKE is a no-op rather than a duplicate-insert, and the same
-- pattern scales to the "codes from a target-table read" workflow: every
-- wave's source of truth for "what to load" is the target itself, never
-- an external manifest. CAST on population matches bronze's inferred
-- Utf8/numeric shape to silver's declared BIGINT.

INSERT INTO {{zone_name}}.travel_waves.country_catalog
SELECT
    b.cca2,
    b.cca3,
    b.name_common,
    b.region,
    b.subregion,
    CAST(b.population AS BIGINT) AS population,
    'baltic_api' AS source_batch
FROM {{zone_name}}.travel_waves.alpha_batch_bronze b
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.travel_waves.country_catalog s
    WHERE s.cca2 = b.cca2
);
