-- ============================================================================
-- Demo: Public Holiday Calendar Sync — Script-Parameter Incremental Enrichment
-- Feature: Target-table-driven INVOKE USING (...) overrides on a REST API ingest
-- ============================================================================
--
-- Real-world story: a multinational HR platform syncs public-holiday
-- calendars per (country, year) so its time-off tracker and billable-day
-- calculator stay accurate. At launch, a Nordic pair was hand-seeded
-- (Norway 2024 and Sweden 2024 — the core holidays payroll cares about).
-- Every December the ops team runs the next wave: each onboarded country
-- gets its next-year calendar fetched from Nager.Date and merged into
-- the silver catalog. This demo runs one such wave — the next missing
-- Norway year — with NO hardcoded literals propagated through the
-- ingest definition.
--
-- What's different from a static ingest:
--   • The ingest is defined WITHOUT any path_param OPTIONS. The
--     endpoint template still has {year}/{country_code} placeholders,
--     but the ingest itself never commits to values for them.
--   • A target-table SELECT ... INTO $next_country, $next_year reads
--     the silver catalog and captures the next wave's params into
--     session-scoped script parameters.
--   • INVOKE API INGEST ... USING (...) supplies the path params on a
--     per-call basis, referencing the captured $params directly.
--
-- The three pieces together give the authentic "read target → compute
-- gap → parameterise → invoke" shape purely in SQL:
--
--     SELECT <next-wave-projection> FROM silver WHERE ...
--         INTO $next_country, $next_year;
--     INVOKE API INGEST ... USING (
--         'path_param.year'         = $next_year,
--         'path_param.country_code' = $next_country
--     );
--
-- Delta Forge mechanics exercised:
--   • Bearer credential in the OS keychain (CREATE CREDENTIAL)
--   • REST API data source (CREATE CONNECTION TYPE = rest_api)
--   • CREATE API INGEST with an endpoint template but NO path_param
--     options — params are deferred to the caller
--   • Script parameters — SELECT ... INTO $params captures the gap
--     projection from a target-table read
--   • INVOKE API INGEST ... USING (...) — per-call parameter overrides,
--     values resolved at INVOKE time from the captured $params
--   • json_flatten_config on a top-level JSON array response
--   • NOT EXISTS composite-key merge for idempotent incremental load
--
-- Public API: Nager.Date (https://date.nager.at) — a no-auth public
-- holiday service used by scheduling tools and HR platforms worldwide.
--
-- NOTE: requires internet. INVOKE issues a real GET against
-- https://date.nager.at/api/v3/PublicHolidays/<year>/<country>.
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Credential (OS keychain — the always-on default vault)
-- --------------------------------------------------------------------------

CREATE CREDENTIAL IF NOT EXISTS holiday_api_token
    TYPE = CREDENTIAL
    SECRET 'demo-placeholder-nager-is-public'
    DESCRIPTION 'Bearer placeholder for the HR-platform holiday calendar sync';

-- --------------------------------------------------------------------------
-- 2. Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'Bronze landing zone for REST API ingests';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.hr_calendar
    COMMENT 'HR platform public-holiday calendars, wave-loaded per country/year';

-- --------------------------------------------------------------------------
-- 3. Silver catalog — seeded with the launch Nordic pair (2024)
-- --------------------------------------------------------------------------
-- This is the SOURCE OF INCREMENTAL TRUTH. The INTO capture below
-- reads THIS table to decide which (country, year) combo the next
-- wave should fetch.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.hr_calendar.country_holidays (
    country_code   STRING,
    holiday_year   INT,
    holiday_date   DATE,
    local_name     STRING,
    english_name   STRING,
    is_fixed       BOOLEAN,
    is_global      BOOLEAN,
    source_batch   STRING
)
LOCATION 'silver/country_holidays';

INSERT INTO {{zone_name}}.hr_calendar.country_holidays VALUES
    ('NO', 2024, DATE '2024-01-01', 'Forste nyttarsdag',    'New Year''s Day',   true, true, 'launch_seed'),
    ('NO', 2024, DATE '2024-05-01', 'Arbeidernes dag',      'Labour Day',        true, true, 'launch_seed'),
    ('NO', 2024, DATE '2024-05-17', 'Grunnlovsdag',         'Constitution Day',  true, true, 'launch_seed'),
    ('NO', 2024, DATE '2024-12-25', 'Forste juledag',       'Christmas Day',     true, true, 'launch_seed'),
    ('SE', 2024, DATE '2024-01-01', 'Nyarsdagen',           'New Year''s Day',   true, true, 'launch_seed'),
    ('SE', 2024, DATE '2024-05-01', 'Forsta maj',           'Labour Day',        true, true, 'launch_seed'),
    ('SE', 2024, DATE '2024-06-06', 'Sveriges nationaldag', 'National Day',      true, true, 'launch_seed'),
    ('SE', 2024, DATE '2024-12-25', 'Juldagen',             'Christmas Day',     true, true, 'launch_seed');

GRANT ADMIN ON TABLE {{zone_name}}.hr_calendar.country_holidays TO USER {{current_user}};

-- --------------------------------------------------------------------------
-- 4. REST API connection — Nager.Date public endpoint
-- --------------------------------------------------------------------------

CREATE CONNECTION IF NOT EXISTS nager_date_holidays
    TYPE = rest_api
    OPTIONS (
        base_url     = 'https://date.nager.at',
        auth_mode    = 'bearer',
        storage_zone = '{{zone_name}}',
        base_path    = 'nager_date_holidays',
        timeout_secs = '30'
    )
    CREDENTIAL = holiday_api_token;

-- --------------------------------------------------------------------------
-- 5. API ingest — endpoint template only, NO stored path_params
-- --------------------------------------------------------------------------
-- The ingest is deliberately defined without any path_param OPTIONS.
-- The {year} and {country_code} placeholders in the endpoint template
-- stay unresolved at CREATE time; each INVOKE supplies its own values
-- via USING (...). That makes this one ingest row reusable across
-- every wave (every country, every year) without ALTER-ing its
-- options between runs.

CREATE API INGEST {{zone_name}}.nager_date_holidays.public_holidays
    ENDPOINT '/api/v3/PublicHolidays/{year}/{country_code}'
    RESPONSE FORMAT JSON;

-- --------------------------------------------------------------------------
-- 6. Incremental gap lookup — read target, capture next-wave params
-- --------------------------------------------------------------------------
-- The single compound step that replaces a hardcoded wave config.
-- The SELECT projection decides "which country + year do we fetch
-- next?" — here by walking the most recent year we have for Norway
-- and adding one. COALESCE handles the bootstrap case where Norway
-- has no rows yet (falls back to 2024 so the first wave grabs 2025).
--
-- INTO $next_country, $next_year captures the row into two session-
-- scoped script parameters. Because the SELECT is an aggregate, it
-- always returns exactly one row — safe for the INTO grammar's
-- single-row contract.
--
-- The same pattern works unchanged with:
--   • `GET INCREMENTAL FILTER FROM <target> COLUMNS (...) INTO $filter;`
--     for key-based watermarks
--   • Any composite projection producing one row — the harness is
--     generic over the producer.

SELECT
    'NO'                                                  AS next_country,
    COALESCE(MAX(holiday_year), 2024) + 1                 AS next_year
FROM {{zone_name}}.hr_calendar.country_holidays
WHERE country_code = 'NO'
INTO $next_country, $next_year;

-- --------------------------------------------------------------------------
-- 7. INVOKE with runtime-resolved path params via USING (...)
-- --------------------------------------------------------------------------
-- The USING clause supplies per-call overrides that get merged into
-- the ingest's stored config at INVOKE time. Keys are unquoted
-- `<kind>.<key>` where <kind> is path_param / query_param / header —
-- a different grammar from CREATE OPTIONS (where dotted keys are
-- quoted). RHS is any scalar expression: literals, $params, function
-- calls, or (SELECT ...) subqueries.
--
-- The engine substitutes $next_year → 2025 and $next_country → 'NO'
-- at execution time, merges those into the ingest's path_param map,
-- then assembles the URL:
--     https://date.nager.at/api/v3/PublicHolidays/2025/NO
-- fetches, and writes the response to the per-run landing folder.
--
-- Nothing about the ingest definition needs to change between waves —
-- the SAME ingest is re-used with different USING params every time.

INVOKE API INGEST {{zone_name}}.nager_date_holidays.public_holidays
    USING (
        path_param.year         = $next_year,
        path_param.country_code = $next_country
    );

-- --------------------------------------------------------------------------
-- 8. Bronze external table over the landed JSON
-- --------------------------------------------------------------------------
-- Nager.Date returns a bare top-level array of holiday objects.
-- root_path = "$" walks that array element-by-element and maps the
-- six fields we'll merge into silver.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.hr_calendar.public_holidays_bronze
USING JSON
LOCATION 'nager_date_holidays/public_holidays'
OPTIONS (
    recursive = 'true',
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.date",
            "$.localName",
            "$.name",
            "$.countryCode",
            "$.fixed",
            "$.global"
        ],
        "column_mappings": {
            "$.date":        "holiday_date",
            "$.localName":   "local_name",
            "$.name":        "english_name",
            "$.countryCode": "country_code",
            "$.fixed":       "is_fixed",
            "$.global":      "is_global"
        },
        "max_depth": 2,
        "separator": "_",
        "infer_types": true
    }'
);

DETECT SCHEMA FOR TABLE {{zone_name}}.hr_calendar.public_holidays_bronze;
GRANT ADMIN ON TABLE {{zone_name}}.hr_calendar.public_holidays_bronze TO USER {{current_user}};

-- --------------------------------------------------------------------------
-- 9. Anti-join merge — bronze → silver on composite key
-- --------------------------------------------------------------------------
-- holiday_year is sourced from $next_year so the silver row records
-- the year we asked for, not a value re-derived from the payload
-- (Nager.Date's response doesn't carry year as a separate field; it's
-- implicit in the path we requested). NOT EXISTS on (country_code,
-- holiday_year, holiday_date) makes the INSERT idempotent across
-- replays.

INSERT INTO {{zone_name}}.hr_calendar.country_holidays
SELECT
    b.country_code,
    $next_year                     AS holiday_year,
    CAST(b.holiday_date AS DATE)   AS holiday_date,
    b.local_name,
    b.english_name,
    CAST(b.is_fixed  AS BOOLEAN)   AS is_fixed,
    CAST(b.is_global AS BOOLEAN)   AS is_global,
    'nager_api'                    AS source_batch
FROM {{zone_name}}.hr_calendar.public_holidays_bronze b
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.hr_calendar.country_holidays s
    WHERE s.country_code = b.country_code
      AND s.holiday_year = $next_year
      AND s.holiday_date = CAST(b.holiday_date AS DATE)
);
