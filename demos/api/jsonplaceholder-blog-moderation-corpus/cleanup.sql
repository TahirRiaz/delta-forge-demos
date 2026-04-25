-- ============================================================================
-- Cleanup: Blog Moderation Corpus
-- ============================================================================
-- Reverse order of creation: silver Delta → bronze external → API endpoint
-- → connection → schema. Zone is left in place — sibling API demos share
-- `bronze`. WITH FILES on both tables also removes their on-disk data
-- (Delta log + parquet for silver, raw JSON pages for bronze).
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.blog_moderation.posts_silver WITH FILES;

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.blog_moderation.posts_bronze WITH FILES;

DROP API ENDPOINT IF EXISTS {{zone_name}}.blog_moderation.blog_posts;

DROP CONNECTION IF EXISTS blog_moderation;

DROP SCHEMA IF EXISTS {{zone_name}}.blog_moderation;
