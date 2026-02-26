-- ============================================================================
-- XML NYT News RSS Feed Analysis — Cleanup Script
-- ============================================================================

-- Revoke permissions
REVOKE READ ON TABLE {{zone_name}}.xml.news_articles FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.xml.news_categories FROM USER {{current_user}};

-- Drop schema columns metadata
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.xml.news_articles;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.xml.news_categories;

-- Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.xml.news_articles;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.xml.news_categories;

-- Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.xml;
DROP ZONE IF EXISTS {{zone_name}};
