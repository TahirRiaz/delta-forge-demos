-- ============================================================================
-- Demo: European Travel Reference Catalog
-- Feature: End-to-end REST API ingest — keychain credential, connection,
--          endpoint, INVOKE, external table with JSON flattening
-- ============================================================================
--
-- Real-world story: a travel-tech startup keeps European country reference
-- data (population for GDPR thresholds, languages for localization,
-- currencies for the multi-currency cart) fresh by syncing the public
-- REST Countries API. The data lands as raw JSON in bronze, then a
-- flattened external table makes it queryable for the compliance +
-- localization teams.
--
-- Pipeline:
--   1. Vault entry             — placeholder API token in the OS keychain
--                                (the always-on default credential
--                                storage). REST Countries doesn't require
--                                auth, but we exercise the full credential
--                                path so production APIs that DO require
--                                auth use the same pattern verbatim.
--   2. Zone + schema           — bronze landing + travel_geo for the
--                                queryable external table
--   3. REST API connection     — base URL + auth_mode + storage_zone +
--                                base_path on the data source
--   4. API ingest endpoint     — qualified name + endpoint path +
--                                response format
--   5. INVOKE                  — actual HTTP GET, writes raw JSON to
--                                bronze under a timestamped per-run folder
--   6. External table          — JSON over the bronze landing with
--                                json_flatten_config to project nested
--                                fields into flat columns
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Vault entry (the API token itself)
-- --------------------------------------------------------------------------
-- The OS keychain backend (Windows Credential Manager / macOS Keychain /
-- Linux Secret Service) is the always-on default storage — no explicit
-- registration needed. CREATE CREDENTIAL writes to it directly.
--
-- The literal SECRET below is a placeholder — REST Countries v3 is fully
-- public and ignores the Authorization header. The same syntax + flow
-- applies unchanged for bearer-protected APIs (GitHub, Stripe, etc.);
-- only the literal value changes.

CREATE CREDENTIAL IF NOT EXISTS travel_api_token
    TYPE = CREDENTIAL
    SECRET 'demo-placeholder-token-restcountries-is-public'
    DESCRIPTION 'Bearer token for the REST Countries reference catalog sync';

-- --------------------------------------------------------------------------
-- 2. Zone + schema
-- --------------------------------------------------------------------------
-- Zone is the permission boundary. INVOKE writes downloaded files under
-- <zone-root>/<source>/<endpoint>/<run-ts>/page_NNNN.json — `bronze`
-- here is the destination + the right that gates who can run the ingest.

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'Bronze landing zone for REST API ingests';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.travel_geo
    COMMENT 'Travel geography reference data — country, currency, language metadata';

-- --------------------------------------------------------------------------
-- 3. REST API connection (a `data_sources` row of source_type = rest_api)
-- --------------------------------------------------------------------------
-- Carries the host, auth mode, and storage destination. CREDENTIAL = …
-- references the vault entry above by name; the executor resolves the
-- secret material at INVOKE time without it ever crossing back into SQL.

CREATE CONNECTION IF NOT EXISTS rest_countries
    TYPE = rest_api
    OPTIONS (
        base_url      = 'https://restcountries.com',
        auth_mode     = 'bearer',
        storage_zone  = '{{zone_name}}',
        base_path     = 'rest_countries',
        timeout_secs  = '30'
    )
    CREDENTIAL = travel_api_token;

-- --------------------------------------------------------------------------
-- 4. API ingest endpoint (definition only — no HTTP yet)
-- --------------------------------------------------------------------------
-- Qualified name `<zone>.<source>.<endpoint>` ties the endpoint to its
-- destination zone in one place. SHOW API INGESTS lists this row;
-- DESCRIBE API INGEST shows its full config.

CREATE API INGEST {{zone_name}}.rest_countries.europe
    ENDPOINT '/v3.1/region/europe'
    RESPONSE FORMAT JSON;

-- --------------------------------------------------------------------------
-- 5. INVOKE — actual HTTP fetch, lands raw JSON under bronze
-- --------------------------------------------------------------------------
-- Single-page response (REST Countries returns the full European array in
-- one call), so pagination isn't needed. The engine writes one
-- `page_0001.json` under a timestamped per-run folder.

INVOKE API INGEST {{zone_name}}.rest_countries.europe;

-- --------------------------------------------------------------------------
-- 6. External table over the landed JSON
-- --------------------------------------------------------------------------
-- LOCATION is relative to the zone's storage_root, so it resolves to the
-- same path the ingest engine wrote to. `recursive` walks the
-- timestamped per-run subfolders so adding more INVOKE runs over time
-- expands the row set without editing the table definition.
--
-- json_flatten_config picks specific fields out of each country object
-- in the response array and maps them to friendly flat column names —
-- the queryable shape the localization + compliance teams want.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.travel_geo.european_countries
USING JSON
LOCATION 'rest_countries/europe'
OPTIONS (
    recursive = 'true',
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.name.common",
            "$.name.official",
            "$.cca2",
            "$.cca3",
            "$.region",
            "$.subregion",
            "$.capital[0]",
            "$.population",
            "$.area",
            "$.independent",
            "$.unMember",
            "$.landlocked"
        ],
        "column_mappings": {
            "$.name.common":    "name_common",
            "$.name.official":  "name_official",
            "$.cca2":           "cca2",
            "$.cca3":           "cca3",
            "$.region":         "region",
            "$.subregion":      "subregion",
            "$.capital[0]":     "capital",
            "$.population":     "population",
            "$.area":           "area_sq_km",
            "$.independent":    "is_independent",
            "$.unMember":       "is_un_member",
            "$.landlocked":     "is_landlocked"
        },
        "max_depth": 3,
        "separator": "_",
        "infer_types": true
    }'
);

-- --------------------------------------------------------------------------
-- Schema detection + permissions
-- --------------------------------------------------------------------------

DETECT SCHEMA FOR TABLE {{zone_name}}.travel_geo.european_countries;
GRANT ADMIN ON TABLE {{zone_name}}.travel_geo.european_countries TO USER {{current_user}};
