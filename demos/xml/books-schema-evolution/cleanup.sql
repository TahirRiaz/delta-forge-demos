-- ============================================================================
-- XML Books Schema Evolution — Cleanup Script
-- ============================================================================

-- Revoke permissions
REVOKE READ ON TABLE external.xml.books_evolved FROM USER {{current_user}};

-- Drop schema columns metadata
DROP SCHEMA COLUMNS FOR TABLE external.xml.books_evolved;

-- Drop external tables
DROP EXTERNAL TABLE IF EXISTS external.xml.books_evolved;

-- Shared resources (safe — won't fail if other demos use them)
DROP SCHEMA IF EXISTS external.xml;
DROP ZONE IF EXISTS external;
