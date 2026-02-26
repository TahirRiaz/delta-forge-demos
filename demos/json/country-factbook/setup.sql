-- ============================================================================
-- JSON Country Factbook — Setup Script
-- ============================================================================
-- Creates two external tables from 10 CIA World Factbook country JSON files:
--   1. countries       — Flattened overview: one row per country (10 rows)
--   2. country_economy — Economy-focused extraction with more granular data
--
-- Demonstrates:
--   - Deep nesting (3+ levels): $.Geography.Area.total .text
--   - include_paths: selective extraction from 13 top-level sections
--   - exclude_paths: skip verbose Introduction/Background HTML text
--   - column_mappings: deep paths → friendly column names
--   - preserve_original: keep full JSON source for audit
--   - Schema evolution: Terrorism and Space sections are optional (NULL fill)
--   - Multi-file reading: 10 .json files (one per country)
--   - file_metadata: df_file_name reveals country code (eg.json, sf.json...)
--   - max_depth: control flattening depth on complex documents
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.json
    COMMENT 'JSON-backed external tables';

-- ============================================================================
-- TABLE 1: countries — Flattened overview, one row per country (10 total)
-- ============================================================================
-- Extracts key fields from Geography, People, Government, and optional
-- Terrorism/Space sections. The verbose Introduction.Background HTML text
-- is excluded. The full original JSON is preserved for audit via
-- preserve_original.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.json.countries
USING JSON
LOCATION '{{data_path}}'
OPTIONS (
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.Government.Country name.conventional short form.text",
            "$.Government.Capital.name.text",
            "$.Government.Government type.text",
            "$.Government.Independence.text",
            "$.Geography.Location.text",
            "$.Geography.Area.total .text",
            "$.Geography.Climate.text",
            "$.Geography.Terrain.text",
            "$.People and Society.Population.total.text",
            "$.People and Society.Languages.Languages.text",
            "$.People and Society.Religions.text",
            "$.Terrorism.Terrorist group(s).text",
            "$.Space.Space agency/agencies.text",
            "$.Space.Space program overview.text"
        ],
        "exclude_paths": [
            "$.Introduction.Background"
        ],
        "column_mappings": {
            "$.Government.Country name.conventional short form.text": "country_name",
            "$.Government.Capital.name.text": "capital",
            "$.Government.Government type.text": "government_type",
            "$.Government.Independence.text": "independence",
            "$.Geography.Location.text": "location",
            "$.Geography.Area.total .text": "area",
            "$.Geography.Climate.text": "climate",
            "$.Geography.Terrain.text": "terrain",
            "$.People and Society.Population.total.text": "population",
            "$.People and Society.Languages.Languages.text": "languages",
            "$.People and Society.Religions.text": "religions",
            "$.Terrorism.Terrorist group(s).text": "terrorist_groups",
            "$.Space.Space agency/agencies.text": "space_agencies",
            "$.Space.Space program overview.text": "space_program"
        },
        "max_depth": 5,
        "separator": "_",
        "preserve_original": true,
        "infer_types": false
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
DETECT SCHEMA FOR TABLE {{zone_name}}.json.countries;
GRANT ADMIN ON TABLE {{zone_name}}.json.countries TO USER {{current_user}};


-- ============================================================================
-- TABLE 2: country_economy — Economy-focused extraction (10 total)
-- ============================================================================
-- Extracts economic indicators from the deeply nested Economy section.
-- GDP, inflation, unemployment, and sector composition at 3+ levels deep.
-- Introduction.Background is excluded (verbose HTML). Uses column_mappings
-- for clean analytics-ready names.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.json.country_economy
USING JSON
LOCATION '{{data_path}}'
OPTIONS (
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.Government.Country name.conventional short form.text",
            "$.Economy.Economic overview.text",
            "$.Economy.Real GDP (purchasing power parity).Real GDP (purchasing power parity) 2023.text",
            "$.Economy.Real GDP growth rate.Real GDP growth rate 2023.text",
            "$.Economy.Real GDP per capita.Real GDP per capita 2023.text",
            "$.Economy.GDP (official exchange rate).text",
            "$.Economy.Inflation rate (consumer prices).Inflation rate (consumer prices) 2023.text",
            "$.Economy.GDP - composition, by sector of origin.agriculture.text",
            "$.Economy.GDP - composition, by sector of origin.industry.text",
            "$.Economy.GDP - composition, by sector of origin.services.text",
            "$.Economy.Agricultural products.text",
            "$.Economy.Industries.text",
            "$.Economy.Unemployment rate.Unemployment rate 2023.text",
            "$.Economy.Exports.Exports 2023.text",
            "$.Economy.Imports.Imports 2023.text"
        ],
        "exclude_paths": [
            "$.Introduction.Background"
        ],
        "column_mappings": {
            "$.Government.Country name.conventional short form.text": "country_name",
            "$.Economy.Economic overview.text": "economic_overview",
            "$.Economy.Real GDP (purchasing power parity).Real GDP (purchasing power parity) 2023.text": "gdp_ppp_2023",
            "$.Economy.Real GDP growth rate.Real GDP growth rate 2023.text": "gdp_growth_2023",
            "$.Economy.Real GDP per capita.Real GDP per capita 2023.text": "gdp_per_capita_2023",
            "$.Economy.GDP (official exchange rate).text": "gdp_official",
            "$.Economy.Inflation rate (consumer prices).Inflation rate (consumer prices) 2023.text": "inflation_2023",
            "$.Economy.GDP - composition, by sector of origin.agriculture.text": "sector_agriculture",
            "$.Economy.GDP - composition, by sector of origin.industry.text": "sector_industry",
            "$.Economy.GDP - composition, by sector of origin.services.text": "sector_services",
            "$.Economy.Agricultural products.text": "agricultural_products",
            "$.Economy.Industries.text": "industries",
            "$.Economy.Unemployment rate.Unemployment rate 2023.text": "unemployment_2023",
            "$.Economy.Exports.Exports 2023.text": "exports_2023",
            "$.Economy.Imports.Imports 2023.text": "imports_2023"
        },
        "max_depth": 5,
        "separator": "_",
        "infer_types": false
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
DETECT SCHEMA FOR TABLE {{zone_name}}.json.country_economy;
GRANT ADMIN ON TABLE {{zone_name}}.json.country_economy TO USER {{current_user}};
