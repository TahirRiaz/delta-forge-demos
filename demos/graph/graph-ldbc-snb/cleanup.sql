-- ============================================================================
-- LDBC Social Network Benchmark — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- Graph definition must be dropped before the tables it references.
-- Two schemas are cleaned up:
--   {{zone_name}}.ldbc      — Delta tables + graph definition
--   {{zone_name}}.raw  — External CSV staging tables
-- ============================================================================

-- STEP 1: Drop graph definition
DROP GRAPH IF EXISTS ldbc_social_network;

-- STEP 2: Drop graph configuration
DROP GRAPH CONFIG {{zone_name}}.ldbc.person_knows_person;
DROP GRAPH CONFIG {{zone_name}}.ldbc.person;

-- STEP 3: Drop Delta tables — dynamic edges
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.post_isLocatedIn_place WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.post_hasTag_tag WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.post_hasCreator_person WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.person_workAt_organisation WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.person_studyAt_organisation WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.person_speaks_language WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.person_likes_post WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.person_likes_comment WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.person_isLocatedIn_place WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.person_hasInterest_tag WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.person_email WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.forum_hasTag_tag WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.forum_hasModerator_person WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.forum_hasMember_person WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.forum_containerOf_post WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.comment_replyOf_post WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.comment_replyOf_comment WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.comment_isLocatedIn_place WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.comment_hasTag_tag WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.comment_hasCreator_person WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.person_knows_person WITH FILES;

-- STEP 4: Drop Delta tables — dynamic entities
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.forum WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.post WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.comment WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.person WITH FILES;

-- STEP 5: Drop Delta tables — static edges
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.tagclass_isSubclassOf_tagclass WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.tag_hasType_tagclass WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.place_isPartOf_place WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.organisation_isLocatedIn_place WITH FILES;

-- STEP 6: Drop Delta tables — static entities
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.tagclass WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.tag WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.organisation WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc.place WITH FILES;

-- STEP 7: Drop external tables (staging schema)
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.post_isLocatedIn_place;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.post_hasTag_tag;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.post_hasCreator_person;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.person_workAt_organisation;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.person_studyAt_organisation;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.person_speaks_language;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.person_likes_post;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.person_likes_comment;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.person_isLocatedIn_place;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.person_hasInterest_tag;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.person_email;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.forum_hasTag_tag;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.forum_hasModerator_person;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.forum_hasMember_person;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.forum_containerOf_post;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.comment_replyOf_post;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.comment_replyOf_comment;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.comment_isLocatedIn_place;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.comment_hasTag_tag;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.comment_hasCreator_person;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.person_knows_person;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.forum;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.post;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.comment;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.person;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.tagclass_isSubclassOf_tagclass;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.tag_hasType_tagclass;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.place_isPartOf_place;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.organisation_isLocatedIn_place;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.tagclass;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.tag;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.organisation;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.place;

-- STEP 8: Drop schemas and zone
DROP SCHEMA IF EXISTS {{zone_name}}.ldbc;
DROP SCHEMA IF EXISTS {{zone_name}}.raw;
DROP ZONE IF EXISTS {{zone_name}};
