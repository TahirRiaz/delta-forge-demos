-- ============================================================================
-- JSON Music Catalog — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Revoke permissions
REVOKE READ ON TABLE {{zone_name}}.json.album_tracks FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.json.album_summary FROM USER {{current_user}};

-- STEP 2: Drop schema columns metadata
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.json.album_tracks;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.json.album_summary;

-- STEP 3: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.json.album_tracks;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.json.album_summary;

-- STEP 4: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.json;
DROP ZONE IF EXISTS {{zone_name}};
