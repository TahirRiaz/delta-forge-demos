-- ============================================================================
-- Excel Sales Analytics — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Revoke permissions
REVOKE READ ON TABLE {{zone_name}}.excel.all_orders FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.excel.orders_2017 FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.excel.orders_range FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.excel.orders_trimmed FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.excel.orders_no_header FROM USER {{current_user}};

-- STEP 2: Drop schema columns metadata
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.excel.all_orders;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.excel.orders_2017;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.excel.orders_range;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.excel.orders_trimmed;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.excel.orders_no_header;

-- STEP 3: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.excel.all_orders;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.excel.orders_2017;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.excel.orders_range;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.excel.orders_trimmed;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.excel.orders_no_header;

-- STEP 4: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.excel;
DROP ZONE IF EXISTS {{zone_name}};
