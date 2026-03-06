-- ============================================================================
-- LDBC Social Network Benchmark — Full Model Setup Script
-- ============================================================================
-- Loads the complete LDBC SNB Scale Factor 0.1 dataset into Delta tables
-- and creates a named graph for algorithm verification.
--
-- Data source: https://ldbcouncil.org/benchmarks/snb/
-- Format: pipe-delimited CSV with headers
--
-- Entities (8 vertex types):
--   Person (1,528)  Comment (151,043)  Post (135,701)  Forum (13,750)
--   Place (1,460)   Organisation (7,955)  Tag (16,080)  TagClass (71)
--
-- Relationships (23 edge types):
--   person_knows_person (14,073)       comment_hasCreator_person (151,043)
--   post_hasCreator_person (135,701)   person_isLocatedIn_place (1,528)
--   comment_isLocatedIn_place (151,043) post_isLocatedIn_place (135,701)
--   comment_replyOf_comment (76,787)   comment_replyOf_post (74,256)
--   comment_hasTag_tag (191,303)       post_hasTag_tag (51,118)
--   forum_hasTag_tag (47,697)          forum_containerOf_post (135,701)
--   forum_hasMember_person (123,268)   forum_hasModerator_person (13,750)
--   person_hasInterest_tag (35,475)    person_likes_comment (62,225)
--   person_likes_post (47,215)         person_studyAt_organisation (1,209)
--   person_workAt_organisation (3,313) organisation_isLocatedIn_place (7,955)
--   place_isPartOf_place (1,454)       tag_hasType_tagclass (16,080)
--   tagclass_isSubclassOf_tagclass (70)
--
-- Graph:
--   ldbc_social_network — Person vertices + KNOWS edges (core social graph)
-- ============================================================================


-- ############################################################################
-- STEP 1: Zone & Schemas
-- ############################################################################
-- This demo creates 33 external tables, 31 Delta tables, and 1 graph.
-- Two schemas keep staging separate from the materialized layer:
--   {{zone_name}}.raw   — External CSV tables (staging / read-only)
--   {{zone_name}}.ldbc  — Delta tables + graph definition (queryable)
-- The cleanup script drops both schemas and everything in them.
-- ############################################################################

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.raw
    COMMENT 'LDBC SNB — external CSV staging tables (pipe-delimited)';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.ldbc
    COMMENT 'LDBC SNB — Delta tables, graph definition, and queries';


-- ############################################################################
-- STEP 2: External Tables — Raw CSV Readers (pipe-delimited)
-- ############################################################################
-- Each external table points to a pipe-delimited CSV file from the LDBC
-- datagen output. Original LDBC headers with dots (e.g. Person.id) and
-- duplicate column names have been renamed for compatibility.
-- ############################################################################


-- === Static Entities ===

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.place
USING CSV LOCATION '{{data_path}}/place_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.organisation
USING CSV LOCATION '{{data_path}}/organisation_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.tag
USING CSV LOCATION '{{data_path}}/tag_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.tagclass
USING CSV LOCATION '{{data_path}}/tagclass_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');


-- === Static Edges ===

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.organisation_isLocatedIn_place
USING CSV LOCATION '{{data_path}}/organisation_isLocatedIn_place_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.place_isPartOf_place
USING CSV LOCATION '{{data_path}}/place_isPartOf_place_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.tag_hasType_tagclass
USING CSV LOCATION '{{data_path}}/tag_hasType_tagclass_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.tagclass_isSubclassOf_tagclass
USING CSV LOCATION '{{data_path}}/tagclass_isSubclassOf_tagclass_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');


-- === Dynamic Entities ===

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.person
USING CSV LOCATION '{{data_path}}/person_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.comment
USING CSV LOCATION '{{data_path}}/comment_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.post
USING CSV LOCATION '{{data_path}}/post_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.forum
USING CSV LOCATION '{{data_path}}/forum_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');


-- === Dynamic Edges ===

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.person_knows_person
USING CSV LOCATION '{{data_path}}/person_knows_person_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.comment_hasCreator_person
USING CSV LOCATION '{{data_path}}/comment_hasCreator_person_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.comment_hasTag_tag
USING CSV LOCATION '{{data_path}}/comment_hasTag_tag_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.comment_isLocatedIn_place
USING CSV LOCATION '{{data_path}}/comment_isLocatedIn_place_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.comment_replyOf_comment
USING CSV LOCATION '{{data_path}}/comment_replyOf_comment_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.comment_replyOf_post
USING CSV LOCATION '{{data_path}}/comment_replyOf_post_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.forum_containerOf_post
USING CSV LOCATION '{{data_path}}/forum_containerOf_post_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.forum_hasMember_person
USING CSV LOCATION '{{data_path}}/forum_hasMember_person_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.forum_hasModerator_person
USING CSV LOCATION '{{data_path}}/forum_hasModerator_person_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.forum_hasTag_tag
USING CSV LOCATION '{{data_path}}/forum_hasTag_tag_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.person_email
USING CSV LOCATION '{{data_path}}/person_email_emailaddress_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.person_hasInterest_tag
USING CSV LOCATION '{{data_path}}/person_hasInterest_tag_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.person_isLocatedIn_place
USING CSV LOCATION '{{data_path}}/person_isLocatedIn_place_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.person_likes_comment
USING CSV LOCATION '{{data_path}}/person_likes_comment_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.person_likes_post
USING CSV LOCATION '{{data_path}}/person_likes_post_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.person_speaks_language
USING CSV LOCATION '{{data_path}}/person_speaks_language_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.person_studyAt_organisation
USING CSV LOCATION '{{data_path}}/person_studyAt_organisation_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.person_workAt_organisation
USING CSV LOCATION '{{data_path}}/person_workAt_organisation_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.post_hasCreator_person
USING CSV LOCATION '{{data_path}}/post_hasCreator_person_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.post_hasTag_tag
USING CSV LOCATION '{{data_path}}/post_hasTag_tag_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.post_isLocatedIn_place
USING CSV LOCATION '{{data_path}}/post_isLocatedIn_place_0_0.csv'
OPTIONS (header = 'true', delimiter = '|');


-- ############################################################################
-- STEP 3: Delta Tables — Materialized with Proper Types
-- ############################################################################
-- CTAS (CREATE TABLE AS SELECT) from external CSV tables into Delta format.
-- All IDs cast to BIGINT, timestamps to BIGINT (epoch millis).
-- ############################################################################


-- === Static Entity Tables ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.place
LOCATION '{{data_path}}/delta/place'
AS SELECT
    CAST(id AS BIGINT) AS id,
    name,
    url,
    type
FROM {{zone_name}}.raw.place;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.place;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.place TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.organisation
LOCATION '{{data_path}}/delta/organisation'
AS SELECT
    CAST(id AS BIGINT) AS id,
    type,
    name,
    url
FROM {{zone_name}}.raw.organisation;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.organisation;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.organisation TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.tag
LOCATION '{{data_path}}/delta/tag'
AS SELECT
    CAST(id AS BIGINT) AS id,
    name,
    url
FROM {{zone_name}}.raw.tag;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.tag;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.tag TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.tagclass
LOCATION '{{data_path}}/delta/tagclass'
AS SELECT
    CAST(id AS BIGINT) AS id,
    name,
    url
FROM {{zone_name}}.raw.tagclass;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.tagclass;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.tagclass TO USER {{current_user}};


-- === Static Edge Tables ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.organisation_isLocatedIn_place
LOCATION '{{data_path}}/delta/organisation_isLocatedIn_place'
AS SELECT
    CAST(organisation_id AS BIGINT) AS organisation_id,
    CAST(place_id AS BIGINT) AS place_id
FROM {{zone_name}}.raw.organisation_isLocatedIn_place;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.organisation_isLocatedIn_place;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.organisation_isLocatedIn_place TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.place_isPartOf_place
LOCATION '{{data_path}}/delta/place_isPartOf_place'
AS SELECT
    CAST(place_id AS BIGINT) AS place_id,
    CAST(parent_place_id AS BIGINT) AS parent_place_id
FROM {{zone_name}}.raw.place_isPartOf_place;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.place_isPartOf_place;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.place_isPartOf_place TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.tag_hasType_tagclass
LOCATION '{{data_path}}/delta/tag_hasType_tagclass'
AS SELECT
    CAST(tag_id AS BIGINT) AS tag_id,
    CAST(tagclass_id AS BIGINT) AS tagclass_id
FROM {{zone_name}}.raw.tag_hasType_tagclass;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.tag_hasType_tagclass;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.tag_hasType_tagclass TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.tagclass_isSubclassOf_tagclass
LOCATION '{{data_path}}/delta/tagclass_isSubclassOf_tagclass'
AS SELECT
    CAST(tagclass_id AS BIGINT) AS tagclass_id,
    CAST(parent_tagclass_id AS BIGINT) AS parent_tagclass_id
FROM {{zone_name}}.raw.tagclass_isSubclassOf_tagclass;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.tagclass_isSubclassOf_tagclass;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.tagclass_isSubclassOf_tagclass TO USER {{current_user}};


-- === Dynamic Entity Tables ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.person
LOCATION '{{data_path}}/delta/person'
AS SELECT
    CAST(id AS BIGINT) AS id,
    firstName,
    lastName,
    gender,
    CAST(birthday AS BIGINT) AS birthday,
    CAST(creationDate AS BIGINT) AS creationDate,
    locationIP,
    browserUsed
FROM {{zone_name}}.raw.person;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.person;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.person TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.comment
LOCATION '{{data_path}}/delta/comment'
AS SELECT
    CAST(id AS BIGINT) AS id,
    CAST(creationDate AS BIGINT) AS creationDate,
    locationIP,
    browserUsed,
    content,
    CAST(length AS INT) AS length
FROM {{zone_name}}.raw.comment;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.comment;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.comment TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.post
LOCATION '{{data_path}}/delta/post'
AS SELECT
    CAST(id AS BIGINT) AS id,
    imageFile,
    CAST(creationDate AS BIGINT) AS creationDate,
    locationIP,
    browserUsed,
    language,
    content,
    CAST(length AS INT) AS length
FROM {{zone_name}}.raw.post;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.post;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.post TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.forum
LOCATION '{{data_path}}/delta/forum'
AS SELECT
    CAST(id AS BIGINT) AS id,
    title,
    CAST(creationDate AS BIGINT) AS creationDate
FROM {{zone_name}}.raw.forum;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.forum;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.forum TO USER {{current_user}};


-- === Dynamic Edge Tables ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.person_knows_person
LOCATION '{{data_path}}/delta/person_knows_person'
AS SELECT
    CAST(src AS BIGINT) AS src,
    CAST(dst AS BIGINT) AS dst,
    CAST(creationDate AS BIGINT) AS creationDate
FROM {{zone_name}}.raw.person_knows_person;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.person_knows_person;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.person_knows_person TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.comment_hasCreator_person
LOCATION '{{data_path}}/delta/comment_hasCreator_person'
AS SELECT
    CAST(comment_id AS BIGINT) AS comment_id,
    CAST(person_id AS BIGINT) AS person_id
FROM {{zone_name}}.raw.comment_hasCreator_person;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.comment_hasCreator_person;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.comment_hasCreator_person TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.comment_hasTag_tag
LOCATION '{{data_path}}/delta/comment_hasTag_tag'
AS SELECT
    CAST(comment_id AS BIGINT) AS comment_id,
    CAST(tag_id AS BIGINT) AS tag_id
FROM {{zone_name}}.raw.comment_hasTag_tag;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.comment_hasTag_tag;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.comment_hasTag_tag TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.comment_isLocatedIn_place
LOCATION '{{data_path}}/delta/comment_isLocatedIn_place'
AS SELECT
    CAST(comment_id AS BIGINT) AS comment_id,
    CAST(place_id AS BIGINT) AS place_id
FROM {{zone_name}}.raw.comment_isLocatedIn_place;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.comment_isLocatedIn_place;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.comment_isLocatedIn_place TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.comment_replyOf_comment
LOCATION '{{data_path}}/delta/comment_replyOf_comment'
AS SELECT
    CAST(comment_id AS BIGINT) AS comment_id,
    CAST(reply_to_comment_id AS BIGINT) AS reply_to_comment_id
FROM {{zone_name}}.raw.comment_replyOf_comment;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.comment_replyOf_comment;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.comment_replyOf_comment TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.comment_replyOf_post
LOCATION '{{data_path}}/delta/comment_replyOf_post'
AS SELECT
    CAST(comment_id AS BIGINT) AS comment_id,
    CAST(post_id AS BIGINT) AS post_id
FROM {{zone_name}}.raw.comment_replyOf_post;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.comment_replyOf_post;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.comment_replyOf_post TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.forum_containerOf_post
LOCATION '{{data_path}}/delta/forum_containerOf_post'
AS SELECT
    CAST(forum_id AS BIGINT) AS forum_id,
    CAST(post_id AS BIGINT) AS post_id
FROM {{zone_name}}.raw.forum_containerOf_post;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.forum_containerOf_post;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.forum_containerOf_post TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.forum_hasMember_person
LOCATION '{{data_path}}/delta/forum_hasMember_person'
AS SELECT
    CAST(forum_id AS BIGINT) AS forum_id,
    CAST(person_id AS BIGINT) AS person_id,
    CAST(joinDate AS BIGINT) AS joinDate
FROM {{zone_name}}.raw.forum_hasMember_person;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.forum_hasMember_person;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.forum_hasMember_person TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.forum_hasModerator_person
LOCATION '{{data_path}}/delta/forum_hasModerator_person'
AS SELECT
    CAST(forum_id AS BIGINT) AS forum_id,
    CAST(person_id AS BIGINT) AS person_id
FROM {{zone_name}}.raw.forum_hasModerator_person;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.forum_hasModerator_person;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.forum_hasModerator_person TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.forum_hasTag_tag
LOCATION '{{data_path}}/delta/forum_hasTag_tag'
AS SELECT
    CAST(forum_id AS BIGINT) AS forum_id,
    CAST(tag_id AS BIGINT) AS tag_id
FROM {{zone_name}}.raw.forum_hasTag_tag;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.forum_hasTag_tag;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.forum_hasTag_tag TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.person_email
LOCATION '{{data_path}}/delta/person_email'
AS SELECT
    CAST(person_id AS BIGINT) AS person_id,
    email
FROM {{zone_name}}.raw.person_email;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.person_email;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.person_email TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.person_hasInterest_tag
LOCATION '{{data_path}}/delta/person_hasInterest_tag'
AS SELECT
    CAST(person_id AS BIGINT) AS person_id,
    CAST(tag_id AS BIGINT) AS tag_id
FROM {{zone_name}}.raw.person_hasInterest_tag;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.person_hasInterest_tag;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.person_hasInterest_tag TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.person_isLocatedIn_place
LOCATION '{{data_path}}/delta/person_isLocatedIn_place'
AS SELECT
    CAST(person_id AS BIGINT) AS person_id,
    CAST(place_id AS BIGINT) AS place_id
FROM {{zone_name}}.raw.person_isLocatedIn_place;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.person_isLocatedIn_place;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.person_isLocatedIn_place TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.person_likes_comment
LOCATION '{{data_path}}/delta/person_likes_comment'
AS SELECT
    CAST(person_id AS BIGINT) AS person_id,
    CAST(comment_id AS BIGINT) AS comment_id,
    CAST(creationDate AS BIGINT) AS creationDate
FROM {{zone_name}}.raw.person_likes_comment;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.person_likes_comment;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.person_likes_comment TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.person_likes_post
LOCATION '{{data_path}}/delta/person_likes_post'
AS SELECT
    CAST(person_id AS BIGINT) AS person_id,
    CAST(post_id AS BIGINT) AS post_id,
    CAST(creationDate AS BIGINT) AS creationDate
FROM {{zone_name}}.raw.person_likes_post;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.person_likes_post;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.person_likes_post TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.person_speaks_language
LOCATION '{{data_path}}/delta/person_speaks_language'
AS SELECT
    CAST(person_id AS BIGINT) AS person_id,
    language
FROM {{zone_name}}.raw.person_speaks_language;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.person_speaks_language;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.person_speaks_language TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.person_studyAt_organisation
LOCATION '{{data_path}}/delta/person_studyAt_organisation'
AS SELECT
    CAST(person_id AS BIGINT) AS person_id,
    CAST(organisation_id AS BIGINT) AS organisation_id,
    CAST(classYear AS INT) AS classYear
FROM {{zone_name}}.raw.person_studyAt_organisation;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.person_studyAt_organisation;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.person_studyAt_organisation TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.person_workAt_organisation
LOCATION '{{data_path}}/delta/person_workAt_organisation'
AS SELECT
    CAST(person_id AS BIGINT) AS person_id,
    CAST(organisation_id AS BIGINT) AS organisation_id,
    CAST(workFrom AS INT) AS workFrom
FROM {{zone_name}}.raw.person_workAt_organisation;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.person_workAt_organisation;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.person_workAt_organisation TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.post_hasCreator_person
LOCATION '{{data_path}}/delta/post_hasCreator_person'
AS SELECT
    CAST(post_id AS BIGINT) AS post_id,
    CAST(person_id AS BIGINT) AS person_id
FROM {{zone_name}}.raw.post_hasCreator_person;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.post_hasCreator_person;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.post_hasCreator_person TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.post_hasTag_tag
LOCATION '{{data_path}}/delta/post_hasTag_tag'
AS SELECT
    CAST(post_id AS BIGINT) AS post_id,
    CAST(tag_id AS BIGINT) AS tag_id
FROM {{zone_name}}.raw.post_hasTag_tag;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.post_hasTag_tag;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.post_hasTag_tag TO USER {{current_user}};


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc.post_isLocatedIn_place
LOCATION '{{data_path}}/delta/post_isLocatedIn_place'
AS SELECT
    CAST(post_id AS BIGINT) AS post_id,
    CAST(place_id AS BIGINT) AS place_id
FROM {{zone_name}}.raw.post_isLocatedIn_place;

DETECT SCHEMA FOR TABLE {{zone_name}}.ldbc.post_isLocatedIn_place;
GRANT ADMIN ON TABLE {{zone_name}}.ldbc.post_isLocatedIn_place TO USER {{current_user}};


-- ############################################################################
-- STEP 4: Graph Definition
-- ############################################################################
-- Creates a named graph coupling Person vertices with KNOWS edges.
-- This is the core social graph used for algorithm verification.
-- Cypher queries reference this by name: USE ldbc_social_network MATCH ...
-- ############################################################################

CREATE GRAPH IF NOT EXISTS ldbc_social_network
    VERTEX TABLE {{zone_name}}.ldbc.person ID COLUMN id LABEL COLUMN gender
    EDGE TABLE {{zone_name}}.ldbc.person_knows_person SOURCE COLUMN src TARGET COLUMN dst
    DIRECTED;
