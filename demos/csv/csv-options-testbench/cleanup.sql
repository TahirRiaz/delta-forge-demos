-- ============================================================================
-- CSV Advanced Options Testbench — Cleanup Script
-- ============================================================================

-- Revoke permissions
REVOKE READ ON TABLE {{zone_name}}.csv.opt_delimiter FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.csv.opt_null_value FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.csv.opt_comment FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.csv.opt_skip_rows FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.csv.opt_max_rows FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.csv.opt_trim FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.csv.opt_quoted FROM USER {{current_user}};
REVOKE READ ON TABLE {{zone_name}}.csv.opt_combined FROM USER {{current_user}};

-- Drop schema columns metadata
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.csv.opt_delimiter;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.csv.opt_null_value;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.csv.opt_comment;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.csv.opt_skip_rows;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.csv.opt_max_rows;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.csv.opt_trim;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.csv.opt_quoted;
DROP SCHEMA COLUMNS FOR TABLE {{zone_name}}.csv.opt_combined;

-- Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.opt_delimiter;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.opt_null_value;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.opt_comment;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.opt_skip_rows;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.opt_max_rows;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.opt_trim;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.opt_quoted;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv.opt_combined;

-- Shared resources (safe — won't fail if other demos use them)
DROP SCHEMA IF EXISTS {{zone_name}}.csv;
DROP ZONE IF EXISTS {{zone_name}};
