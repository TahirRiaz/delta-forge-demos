-- ============================================================================
-- Cleanup: arXiv AI Research Feed
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.research_intel.arxiv_silver WITH FILES;

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.research_intel.arxiv_bronze WITH FILES;

DROP API ENDPOINT IF EXISTS {{zone_name}}.arxiv_api.cs_ai_latest;

DROP CONNECTION IF EXISTS arxiv_api;

DROP SCHEMA IF EXISTS {{zone_name}}.research_intel;
