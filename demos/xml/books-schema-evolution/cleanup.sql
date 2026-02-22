-- ============================================================================
-- XML Books Schema Evolution — Cleanup Script
-- ============================================================================

-- Revoke permissions
REVOKE READ ON TABLE {{zone_name}}.xml.books_evolved FROM USER {{current_user}};

-- Drop schema columns metadata
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.xml.books_evolved;

-- Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.xml.books_evolved;

-- Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.xml;
DROP ZONE IF EXISTS {{zone_name}};
