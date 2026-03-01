-- ============================================================================
-- JSON Subtree Capture — Setup Script
-- ============================================================================
-- Creates two external tables from property listing JSON files (5 listings):
--   1. listings_captured  — location and pricing subtrees captured as JSON blobs
--   2. listings_flattened — same data with location and pricing fully flattened
--
-- Demonstrates:
--   - json_paths: keep complex subtrees as JSON string columns (not flattened)
--   - Combining json_paths with include_paths and column_mappings
--   - Multiple subtree captures per row (location + pricing)
--   - Contrast: captured vs flattened views of the same data
--   - Nested objects with arrays (pricing.tax_history) and sub-objects (location.geo)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.json
    COMMENT 'JSON-backed external tables';

-- ============================================================================
-- TABLE 1: listings_captured — Subtrees captured as JSON strings
-- ============================================================================
-- The location and pricing subtrees are captured as JSON string columns via
-- json_paths. Top-level listing fields (id, title, type, bedrooms, etc.)
-- are flattened normally. This is useful when downstream consumers need the
-- full nested structure (e.g., map rendering, mortgage calculators).
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.json.listings_captured
USING JSON
LOCATION '{{data_path}}'
OPTIONS (
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.id",
            "$.title",
            "$.type",
            "$.bedrooms",
            "$.bathrooms",
            "$.sqft",
            "$.year_built",
            "$.status",
            "$.location",
            "$.pricing",
            "$.tags"
        ],
        "json_paths": [
            "$.location",
            "$.pricing"
        ],
        "column_mappings": {
            "$.id": "listing_id",
            "$.type": "property_type",
            "$.year_built": "year_built",
            "$.location": "location_json",
            "$.pricing": "pricing_json"
        },
        "max_depth": 10,
        "separator": "_",
        "default_array_handling": "to_json",
        "infer_types": true
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
DETECT SCHEMA FOR TABLE {{zone_name}}.json.listings_captured;
GRANT ADMIN ON TABLE {{zone_name}}.json.listings_captured TO USER {{current_user}};


-- ============================================================================
-- TABLE 2: listings_flattened — Same data, fully flattened (no json_paths)
-- ============================================================================
-- For comparison: the same location and pricing subtrees are fully flattened
-- into individual columns. This creates more columns but allows direct SQL
-- filtering on nested fields (e.g., WHERE city = 'Boston').
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.json.listings_flattened
USING JSON
LOCATION '{{data_path}}'
OPTIONS (
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.id",
            "$.title",
            "$.type",
            "$.bedrooms",
            "$.bathrooms",
            "$.sqft",
            "$.year_built",
            "$.status",
            "$.location.address.street",
            "$.location.address.unit",
            "$.location.address.city",
            "$.location.address.state",
            "$.location.address.zip",
            "$.location.geo.lat",
            "$.location.geo.lng",
            "$.location.neighborhood",
            "$.location.walk_score",
            "$.pricing.list_price",
            "$.pricing.price_per_sqft",
            "$.pricing.hoa_monthly",
            "$.pricing.tax_annual",
            "$.pricing.mortgage_estimate.monthly_payment",
            "$.tags"
        ],
        "column_mappings": {
            "$.id": "listing_id",
            "$.type": "property_type",
            "$.location.address.street": "street",
            "$.location.address.unit": "unit",
            "$.location.address.city": "city",
            "$.location.address.state": "state",
            "$.location.address.zip": "zip",
            "$.location.geo.lat": "latitude",
            "$.location.geo.lng": "longitude",
            "$.location.neighborhood": "neighborhood",
            "$.location.walk_score": "walk_score",
            "$.pricing.list_price": "list_price",
            "$.pricing.price_per_sqft": "price_per_sqft",
            "$.pricing.hoa_monthly": "hoa_monthly",
            "$.pricing.tax_annual": "tax_annual",
            "$.pricing.mortgage_estimate.monthly_payment": "monthly_payment"
        },
        "max_depth": 10,
        "separator": "_",
        "default_array_handling": "to_json",
        "infer_types": true
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
DETECT SCHEMA FOR TABLE {{zone_name}}.json.listings_flattened;
GRANT ADMIN ON TABLE {{zone_name}}.json.listings_flattened TO USER {{current_user}};
