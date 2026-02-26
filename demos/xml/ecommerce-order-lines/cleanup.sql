-- ============================================================================
-- XML E-Commerce Order Line Explosion — Cleanup Script
-- ============================================================================

-- Revoke permissions
REVOKE READ ON TABLE {{zone_name}}.xml.order_lines FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.xml.order_summary FROM USER {{current_user}};

-- Drop schema columns metadata
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.xml.order_lines;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.xml.order_summary;

-- Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.xml.order_lines;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.xml.order_summary;

-- Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.xml;
DROP ZONE IF EXISTS {{zone_name}};
