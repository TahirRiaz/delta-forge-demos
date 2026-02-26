-- ============================================================================
-- XML NYT News RSS Feed Analysis — Cleanup Script
-- ============================================================================
-- DROP TABLE commands automatically clean up catalog metadata (columns, etc.).
-- ============================================================================

-- Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.xml.news_articles;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.xml.news_categories;

-- Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.xml;
DROP ZONE IF EXISTS {{zone_name}};
