-- ============================================================================
-- Protobuf Address Book Contacts — Cleanup Script
-- ============================================================================
-- DROP TABLE commands automatically clean up catalog metadata (columns, etc.).
-- ============================================================================

-- Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.protobuf.contacts;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.protobuf.contact_phones;

-- Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.protobuf;
DROP ZONE IF EXISTS {{zone_name}};
