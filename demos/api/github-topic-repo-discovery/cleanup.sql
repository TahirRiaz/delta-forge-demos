-- ============================================================================
-- Cleanup: GitHub Topic Repo Discovery
-- ============================================================================
-- Reverse order of creation. Zone stays — sibling API demos share `bronze`.
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.oss_intel.delta_lake_repos_silver WITH FILES;

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.oss_intel.delta_lake_repos_bronze WITH FILES;

DROP API ENDPOINT IF EXISTS {{zone_name}}.github_search_api.delta_lake_topic;

DROP CONNECTION IF EXISTS github_search_api;

DROP SCHEMA IF EXISTS {{zone_name}}.oss_intel;
