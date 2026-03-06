-- ############################################################################
-- ############################################################################
--
--   LDBC SOCIAL NETWORK BENCHMARK — FULL MODEL VERIFICATION
--   Scale Factor 0.1: 8 Entity Types / 23 Relationship Types / ~1.2M rows
--
-- ############################################################################
-- ############################################################################
--
-- Uses the industry-standard LDBC SNB dataset with official golden values
-- from the LDBC reference implementation validation parameters.
--
-- The graph definition (ldbc_social_network) maps Person vertices + KNOWS
-- edges. Cypher queries use this for pattern matching and algorithms.
-- SQL queries join across the full relational model for multi-relationship
-- traversals that span beyond the KNOWS graph.
--
-- PART 1: DATA INTEGRITY CHECKS (queries 1–5)
-- PART 2: CYPHER — SOCIAL GRAPH EXPLORATION (queries 6–14)
-- PART 3: CYPHER — GRAPH ALGORITHMS (queries 15–25)
-- PART 4: MIXED SQL + CYPHER — Joining graph results with Delta tables (queries 26–30)
-- PART 5: LDBC INTERACTIVE QUERIES — SQL golden value checks (queries 31–41)
-- PART 6: VERIFICATION SUMMARY (query 42)
--
-- ############################################################################


-- ############################################################################
-- PART 1: DATA INTEGRITY CHECKS
-- ############################################################################


-- ============================================================================
-- 1. ENTITY COUNTS — Verify all 8 entity types loaded correctly
-- ============================================================================
-- Expected: Person 1,528 | Comment 151,043 | Post 135,701 | Forum 13,750
--           Place 1,460  | Organisation 7,955 | Tag 16,080 | TagClass 71

SELECT 'person' AS entity, COUNT(*) AS row_count FROM {{zone_name}}.ldbc.person
UNION ALL SELECT 'comment', COUNT(*) FROM {{zone_name}}.ldbc.comment
UNION ALL SELECT 'post', COUNT(*) FROM {{zone_name}}.ldbc.post
UNION ALL SELECT 'forum', COUNT(*) FROM {{zone_name}}.ldbc.forum
UNION ALL SELECT 'place', COUNT(*) FROM {{zone_name}}.ldbc.place
UNION ALL SELECT 'organisation', COUNT(*) FROM {{zone_name}}.ldbc.organisation
UNION ALL SELECT 'tag', COUNT(*) FROM {{zone_name}}.ldbc.tag
UNION ALL SELECT 'tagclass', COUNT(*) FROM {{zone_name}}.ldbc.tagclass
ORDER BY entity;


-- ============================================================================
-- 2. EDGE COUNTS — Verify all relationship types loaded
-- ============================================================================

SELECT 'person_knows_person' AS edge, COUNT(*) AS row_count FROM {{zone_name}}.ldbc.person_knows_person
UNION ALL SELECT 'comment_hasCreator_person', COUNT(*) FROM {{zone_name}}.ldbc.comment_hasCreator_person
UNION ALL SELECT 'comment_hasTag_tag', COUNT(*) FROM {{zone_name}}.ldbc.comment_hasTag_tag
UNION ALL SELECT 'comment_isLocatedIn_place', COUNT(*) FROM {{zone_name}}.ldbc.comment_isLocatedIn_place
UNION ALL SELECT 'comment_replyOf_comment', COUNT(*) FROM {{zone_name}}.ldbc.comment_replyOf_comment
UNION ALL SELECT 'comment_replyOf_post', COUNT(*) FROM {{zone_name}}.ldbc.comment_replyOf_post
UNION ALL SELECT 'forum_containerOf_post', COUNT(*) FROM {{zone_name}}.ldbc.forum_containerOf_post
UNION ALL SELECT 'forum_hasMember_person', COUNT(*) FROM {{zone_name}}.ldbc.forum_hasMember_person
UNION ALL SELECT 'forum_hasModerator_person', COUNT(*) FROM {{zone_name}}.ldbc.forum_hasModerator_person
UNION ALL SELECT 'forum_hasTag_tag', COUNT(*) FROM {{zone_name}}.ldbc.forum_hasTag_tag
UNION ALL SELECT 'person_email', COUNT(*) FROM {{zone_name}}.ldbc.person_email
UNION ALL SELECT 'person_hasInterest_tag', COUNT(*) FROM {{zone_name}}.ldbc.person_hasInterest_tag
UNION ALL SELECT 'person_isLocatedIn_place', COUNT(*) FROM {{zone_name}}.ldbc.person_isLocatedIn_place
UNION ALL SELECT 'person_likes_comment', COUNT(*) FROM {{zone_name}}.ldbc.person_likes_comment
UNION ALL SELECT 'person_likes_post', COUNT(*) FROM {{zone_name}}.ldbc.person_likes_post
UNION ALL SELECT 'person_speaks_language', COUNT(*) FROM {{zone_name}}.ldbc.person_speaks_language
UNION ALL SELECT 'person_studyAt_organisation', COUNT(*) FROM {{zone_name}}.ldbc.person_studyAt_organisation
UNION ALL SELECT 'person_workAt_organisation', COUNT(*) FROM {{zone_name}}.ldbc.person_workAt_organisation
UNION ALL SELECT 'post_hasCreator_person', COUNT(*) FROM {{zone_name}}.ldbc.post_hasCreator_person
UNION ALL SELECT 'post_hasTag_tag', COUNT(*) FROM {{zone_name}}.ldbc.post_hasTag_tag
UNION ALL SELECT 'post_isLocatedIn_place', COUNT(*) FROM {{zone_name}}.ldbc.post_isLocatedIn_place
UNION ALL SELECT 'organisation_isLocatedIn_place', COUNT(*) FROM {{zone_name}}.ldbc.organisation_isLocatedIn_place
UNION ALL SELECT 'place_isPartOf_place', COUNT(*) FROM {{zone_name}}.ldbc.place_isPartOf_place
UNION ALL SELECT 'tag_hasType_tagclass', COUNT(*) FROM {{zone_name}}.ldbc.tag_hasType_tagclass
UNION ALL SELECT 'tagclass_isSubclassOf_tagclass', COUNT(*) FROM {{zone_name}}.ldbc.tagclass_isSubclassOf_tagclass
ORDER BY edge;


-- ============================================================================
-- 3. GRAPH CONFIG — Verify graph definition
-- ============================================================================

SHOW GRAPH CONFIG;


-- ============================================================================
-- 4. PLACE HIERARCHY — Countries, cities, continents
-- ============================================================================

SELECT type, COUNT(*) AS count
FROM {{zone_name}}.ldbc.place
GROUP BY type
ORDER BY type;


-- ============================================================================
-- 5. REFERENTIAL INTEGRITY — All KNOWS edges have valid endpoints
-- ============================================================================

SELECT COUNT(*) AS orphan_edges
FROM {{zone_name}}.ldbc.person_knows_person k
WHERE NOT EXISTS (SELECT 1 FROM {{zone_name}}.ldbc.person p WHERE p.id = k.src)
   OR NOT EXISTS (SELECT 1 FROM {{zone_name}}.ldbc.person p WHERE p.id = k.dst);


-- ############################################################################
-- PART 2: CYPHER — SOCIAL GRAPH EXPLORATION
-- ############################################################################
-- These queries use the ldbc_social_network graph definition to traverse
-- the Person-KNOWS-Person social graph using Cypher pattern matching.
-- ############################################################################


-- ============================================================================
-- 6. BROWSE THE SOCIAL NETWORK — All 1,528 persons
-- ============================================================================
-- LDBC Short Query 1 pattern: person profile lookup.

USE ldbc_social_network
MATCH (p)
RETURN p.id AS id, p.firstName AS firstName, p.lastName AS lastName,
       p.gender AS gender
ORDER BY p.lastName, p.firstName
LIMIT 25;


-- ============================================================================
-- 7. DIRECT FRIENDS — Who does person 933 (Mahinda Perera) know?
-- ============================================================================
-- LDBC Short Query 3 pattern: friend list with friendship dates.
-- Cypher: MATCH (:Person {id:933})-[r:KNOWS]-(friend) RETURN friend

USE ldbc_social_network
MATCH (a)-[r]->(b)
WHERE a.id = 933
RETURN a.firstName AS person, a.lastName AS person_last,
       b.firstName AS friend_first, b.lastName AS friend_last,
       b.gender AS gender;


-- ============================================================================
-- 8. FRIEND OF FRIEND — 2-hop social exploration
-- ============================================================================
-- LDBC Q1 pattern (simplified): find friends-of-friends.
-- Cypher: MATCH (:Person {id:933})-[:KNOWS*1..2]-(friend)
-- Classic social network recommendation: suggest connections.

USE ldbc_social_network
MATCH (a)-[]->(b)-[]->(c)
WHERE a.id = 933 AND a <> c AND NOT (a)-->(c)
RETURN DISTINCT c.firstName AS suggested_friend, c.lastName AS last_name
ORDER BY c.firstName
LIMIT 20;


-- ============================================================================
-- 9. REACHABILITY — Who can person 933 reach within 3 hops?
-- ============================================================================
-- LDBC Q13 pattern: variable-length KNOWS traversal.
-- In a well-connected social network, most people should be reachable
-- within 3-4 hops (small-world property).

USE ldbc_social_network
MATCH (a)-[*1..3]->(b)
WHERE a.id = 933 AND a <> b
RETURN DISTINCT b.id AS reachable_id, b.firstName AS name
ORDER BY b.firstName
LIMIT 30;


-- ============================================================================
-- 10. MUTUAL FRIENDSHIPS — Reciprocal KNOWS relationships
-- ============================================================================
-- If A knows B AND B knows A, that's a mutual friendship.
-- Cypher: MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(a) WHERE a.id < b.id

USE ldbc_social_network
MATCH (a)-[r1]->(b)-[r2]->(a)
WHERE a.id < b.id
RETURN count(*) AS mutual_friendship_count;


-- ============================================================================
-- 11. GENDER DISTRIBUTION OF CONNECTIONS
-- ============================================================================
-- Do people preferentially connect within or across genders?

USE ldbc_social_network
MATCH (a)-[r]->(b)
RETURN a.gender AS from_gender, b.gender AS to_gender, count(r) AS connections
ORDER BY connections DESC;


-- ============================================================================
-- 12. SOCIAL HUBS — Top connected people via Cypher
-- ============================================================================
-- Cross-verify with degree centrality algorithm results.
-- Cypher: MATCH (a)-[:KNOWS]->(b) RETURN a, count(b) ORDER BY count DESC

USE ldbc_social_network
MATCH (a)-[r]->(b)
RETURN a.id AS person_id, a.firstName AS firstName, a.lastName AS lastName,
       count(r) AS out_degree
ORDER BY out_degree DESC
LIMIT 15;


-- ============================================================================
-- 13. HUB NEIGHBORHOOD — Direct friends of the top hub
-- ============================================================================
-- Person 26388279067534 has degree 340 — who are their direct contacts?

USE ldbc_social_network
MATCH (hub)-[r]->(friend)
WHERE hub.id = 26388279067534
RETURN friend.id AS friend_id, friend.firstName AS firstName,
       friend.lastName AS lastName, friend.gender AS gender
ORDER BY friend.firstName
LIMIT 20;


-- ============================================================================
-- 14. GRAPH VISUALIZATION — Social network structure
-- ============================================================================
-- Renders the KNOWS graph. With 1,528 nodes and 14,073 edges,
-- community structure should be visible as dense clusters.

USE ldbc_social_network
MATCH (a)-[r]->(b)
RETURN a, r, b
LIMIT 500;


-- ############################################################################
-- PART 3: CYPHER — GRAPH ALGORITHMS
-- ############################################################################
-- Each algorithm runs on the ldbc_social_network graph (Person + KNOWS).
-- Golden values come from the raw dataset and LDBC validation parameters.
-- ############################################################################


-- ============================================================================
-- 15. PAGERANK — Most influential people in the social network
-- ============================================================================
-- Golden: Person 26388279067534 (degree 340) and 32985348834375 (degree 338)
-- should rank near the top.

USE ldbc_social_network
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD nodeId, score, rank
RETURN nodeId, score, rank
ORDER BY score DESC
LIMIT 15;


-- ============================================================================
-- 16. DEGREE CENTRALITY — Raw connection counts
-- ============================================================================
-- Golden values (total degree from raw data):
--   26388279067534: 340 | 32985348834375: 338 | 2199023256816: 269
--   24189255811566: 256 | 6597069767242: 230

USE ldbc_social_network
CALL algo.degree()
YIELD nodeId, inDegree, outDegree, totalDegree
RETURN nodeId, inDegree, outDegree, totalDegree
ORDER BY totalDegree DESC
LIMIT 15;


-- ============================================================================
-- 17. BETWEENNESS CENTRALITY — Bridge nodes in the social network
-- ============================================================================
-- Identifies people who sit on the shortest paths between many pairs.
-- Removing high-betweenness nodes would fragment the network.

USE ldbc_social_network
CALL algo.betweenness()
YIELD nodeId, centrality, rank
RETURN nodeId, centrality, rank
ORDER BY centrality DESC
LIMIT 15;


-- ============================================================================
-- 18. CLOSENESS CENTRALITY — Who can reach everyone fastest?
-- ============================================================================

USE ldbc_social_network
CALL algo.closeness()
YIELD nodeId, closeness, rank
RETURN nodeId, closeness, rank
ORDER BY closeness DESC
LIMIT 15;


-- ============================================================================
-- 19. CONNECTED COMPONENTS — Is the network fully connected?
-- ============================================================================
-- A single large component means everyone can reach everyone else.

USE ldbc_social_network
CALL algo.connectedComponents()
YIELD nodeId, componentId
RETURN componentId, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 20. LOUVAIN COMMUNITIES — Natural social clusters
-- ============================================================================
-- Detects communities based on actual connection density.

USE ldbc_social_network
CALL algo.louvain({resolution: 1.0})
YIELD nodeId, communityId
RETURN communityId, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 21. TRIANGLE COUNT — Clustering coefficient
-- ============================================================================

USE ldbc_social_network
CALL algo.triangleCount()
YIELD nodeId, triangleCount
RETURN nodeId, triangleCount
ORDER BY triangleCount DESC
LIMIT 15;


-- ============================================================================
-- 22. STRONGLY CONNECTED COMPONENTS — Directed reachability groups
-- ============================================================================

USE ldbc_social_network
CALL algo.scc()
YIELD nodeId, componentId
RETURN componentId, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 23. SHORTEST PATH — LDBC Q13 golden value verification
-- ============================================================================
-- Golden: 32985348833679 → 26388279067108 = path length 3

USE ldbc_social_network
CALL algo.shortestPath({source: 32985348833679, target: 26388279067108})
YIELD nodeId, step, distance
RETURN nodeId, step, distance
ORDER BY step;


-- ============================================================================
-- 24. SHORTEST PATH — Second golden verification (length 2)
-- ============================================================================
-- Golden: 26388279066869 → 6597069768287 = path length 2

USE ldbc_social_network
CALL algo.shortestPath({source: 26388279066869, target: 6597069768287})
YIELD nodeId, step, distance
RETURN nodeId, step, distance
ORDER BY step;


-- ============================================================================
-- 25. BFS — Distance distribution from top hub
-- ============================================================================
-- Starting from highest-degree node (26388279067534, degree 340).
-- Most nodes should be within 3-4 hops in a well-connected social network.

USE ldbc_social_network
CALL algo.bfs({source: 26388279067534})
YIELD nodeId, depth, parentId
RETURN depth, count(*) AS people_at_distance
ORDER BY depth;


-- ############################################################################
-- PART 4: MIXED SQL + CYPHER — Joining Graph Results with Delta Tables
-- ############################################################################
-- Delta Forge can mix Cypher graph traversal results with Delta table SQL
-- queries using the cypher() table function. This enables powerful patterns:
-- run a graph algorithm or pattern match via Cypher, then enrich or filter
-- the results by joining with the full relational model in SQL.
--
-- Syntax: SELECT * FROM cypher('graph', $$ CYPHER_QUERY $$) AS (col TYPE, ...)
-- The cypher() result acts as a regular table in SQL — use it in JOINs, CTEs,
-- subqueries, WHERE IN clauses, etc.
-- ############################################################################


-- ============================================================================
-- 26. FRIENDS WITH LOCATIONS — Cypher traversal + Delta table enrichment
-- ============================================================================
-- Step 1 (Cypher): Find direct friends of Jun Wang via KNOWS graph
-- Step 2 (SQL): Join with person_isLocatedIn_place + place to get cities/countries
-- This demonstrates the core mixed pattern: graph traversal + relational enrichment.

WITH friends AS (
    SELECT * FROM cypher('ldbc_social_network', $$
        MATCH (a)-[]->(b)
        WHERE a.id = 26388279068220
        RETURN b.id AS friend_id, b.firstName AS first_name, b.lastName AS last_name
    $$) AS (friend_id BIGINT, first_name VARCHAR, last_name VARCHAR)
)
SELECT
    f.first_name, f.last_name,
    city.name AS city, country.name AS country
FROM friends f
JOIN {{zone_name}}.ldbc.person_isLocatedIn_place pip ON f.friend_id = pip.person_id
JOIN {{zone_name}}.ldbc.place city ON pip.place_id = city.id
JOIN {{zone_name}}.ldbc.place_isPartOf_place pipp ON city.id = pipp.place_id
JOIN {{zone_name}}.ldbc.place country ON pipp.parent_place_id = country.id
ORDER BY country.name, city.name, f.last_name;


-- ============================================================================
-- 27. PAGERANK LEADERS WITH EMPLOYMENT — Algorithm results + relational context
-- ============================================================================
-- Step 1 (Cypher): Run PageRank on the KNOWS graph to find influential people
-- Step 2 (SQL): Join with person_workAt_organisation to show where leaders work
-- Shows how graph centrality metrics gain meaning when enriched with metadata.

WITH ranked AS (
    SELECT * FROM cypher('ldbc_social_network', $$
        CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
        YIELD nodeId, score
        RETURN nodeId AS person_id, score
        ORDER BY score DESC
        LIMIT 10
    $$) AS (person_id BIGINT, score DOUBLE)
)
SELECT
    p.firstName, p.lastName, r.score AS pagerank_score,
    o.name AS employer, w.workFrom AS work_since
FROM ranked r
JOIN {{zone_name}}.ldbc.person p ON r.person_id = p.id
LEFT JOIN {{zone_name}}.ldbc.person_workAt_organisation w ON p.id = w.person_id
LEFT JOIN {{zone_name}}.ldbc.organisation o ON w.organisation_id = o.id
ORDER BY r.score DESC;


-- ============================================================================
-- 28. SHORTEST PATH WITH PROFILES — Path enriched with person details
-- ============================================================================
-- Step 1 (Cypher): Compute shortest path between two people (golden: length 3)
-- Step 2 (SQL): Join each node on the path with person + place data
-- Turns abstract graph paths into meaningful "who connects to whom and where".

WITH path_nodes AS (
    SELECT * FROM cypher('ldbc_social_network', $$
        CALL algo.shortestPath({source: 32985348833679, target: 26388279067108})
        YIELD nodeId, step, distance
        RETURN nodeId AS person_id, step, distance
    $$) AS (person_id BIGINT, step BIGINT, distance BIGINT)
)
SELECT
    pn.step, pn.distance,
    p.firstName, p.lastName, p.gender,
    city.name AS city, country.name AS country
FROM path_nodes pn
JOIN {{zone_name}}.ldbc.person p ON pn.person_id = p.id
JOIN {{zone_name}}.ldbc.person_isLocatedIn_place pip ON p.id = pip.person_id
JOIN {{zone_name}}.ldbc.place city ON pip.place_id = city.id
JOIN {{zone_name}}.ldbc.place_isPartOf_place pipp ON city.id = pipp.place_id
JOIN {{zone_name}}.ldbc.place country ON pipp.parent_place_id = country.id
ORDER BY pn.step;


-- ============================================================================
-- 29. COMMUNITY MEMBERS WITH INTERESTS — Louvain communities + tag enrichment
-- ============================================================================
-- Step 1 (Cypher): Run Louvain community detection on the KNOWS graph
-- Step 2 (SQL): For the largest community, find shared interests via person_hasInterest_tag
-- Reveals what topics bind a community together — graph structure meets content.

WITH communities AS (
    SELECT * FROM cypher('ldbc_social_network', $$
        CALL algo.louvain({resolution: 1.0})
        YIELD nodeId, communityId
        RETURN nodeId AS person_id, communityId AS community_id
    $$) AS (person_id BIGINT, community_id BIGINT)
),
largest_community AS (
    SELECT community_id
    FROM communities
    GROUP BY community_id
    ORDER BY COUNT(*) DESC
    LIMIT 1
)
SELECT t.name AS interest, COUNT(DISTINCT c.person_id) AS members_interested
FROM communities c
JOIN largest_community lc ON c.community_id = lc.community_id
JOIN {{zone_name}}.ldbc.person_hasInterest_tag phi ON c.person_id = phi.person_id
JOIN {{zone_name}}.ldbc.tag t ON phi.tag_id = t.id
GROUP BY t.name
ORDER BY members_interested DESC
LIMIT 15;


-- ============================================================================
-- 30. DEGREE CENTRALITY WITH CONTENT ACTIVITY — Hub analysis
-- ============================================================================
-- Step 1 (Cypher): Get degree centrality from the KNOWS graph
-- Step 2 (SQL): Count posts and comments authored by top-degree people
-- Tests whether social network hubs are also the most active content creators.

WITH hub_scores AS (
    SELECT * FROM cypher('ldbc_social_network', $$
        CALL algo.degree()
        YIELD nodeId, score
        RETURN nodeId AS person_id, score AS degree
        ORDER BY score DESC
        LIMIT 10
    $$) AS (person_id BIGINT, degree DOUBLE)
)
SELECT
    p.firstName, p.lastName, h.degree,
    COALESCE(post_counts.post_count, 0) AS posts_authored,
    COALESCE(comment_counts.comment_count, 0) AS comments_authored
FROM hub_scores h
JOIN {{zone_name}}.ldbc.person p ON h.person_id = p.id
LEFT JOIN (
    SELECT person_id, COUNT(*) AS post_count
    FROM {{zone_name}}.ldbc.post_hasCreator_person
    GROUP BY person_id
) post_counts ON h.person_id = post_counts.person_id
LEFT JOIN (
    SELECT person_id, COUNT(*) AS comment_count
    FROM {{zone_name}}.ldbc.comment_hasCreator_person
    GROUP BY person_id
) comment_counts ON h.person_id = comment_counts.person_id
ORDER BY h.degree DESC;


-- ############################################################################
-- PART 5: LDBC INTERACTIVE QUERIES — SQL Golden Value Checks
-- ############################################################################
-- These queries traverse multiple relationship types (KNOWS + HAS_CREATOR +
-- IS_LOCATED_IN + HAS_TAG etc.) which require SQL joins across the full
-- relational model. Golden expected results from validation_params-sf0.1.csv.
-- ############################################################################


-- ============================================================================
-- 31. LDBC SHORT Q1 — Person Profile
-- ============================================================================
-- Cypher equivalent:
--   MATCH (n:Person {id:26388279068220})-[:IS_LOCATED_IN]-(p:Place)
--   RETURN n.firstName, n.lastName, n.birthday, n.locationIP,
--          n.browserUsed, n.gender, n.creationDate, p.id AS cityId
--
-- Golden: firstName=Jun, lastName=Wang, gender=female, browserUsed=Opera,
--         cityId=507

SELECT
    p.firstName, p.lastName, p.birthday, p.locationIP,
    p.browserUsed, p.gender, p.creationDate,
    pl.id AS cityId
FROM {{zone_name}}.ldbc.person p
JOIN {{zone_name}}.ldbc.person_isLocatedIn_place pip ON p.id = pip.person_id
JOIN {{zone_name}}.ldbc.place pl ON pip.place_id = pl.id
WHERE p.id = 26388279068220;


-- ============================================================================
-- 32. LDBC SHORT Q3 — Person's Friends
-- ============================================================================
-- Cypher equivalent:
--   MATCH (n:Person {id:26388279068220})-[r:KNOWS]-(friend)
--   RETURN friend.id, friend.firstName, friend.lastName,
--          r.creationDate AS friendshipCreationDate
--   ORDER BY friendshipCreationDate DESC, friend.id ASC
--
-- Golden: friends include Jie Yang, Alexander Hleb, Otto Muller

SELECT
    p2.id AS personId, p2.firstName, p2.lastName,
    k.creationDate AS friendshipCreationDate
FROM {{zone_name}}.ldbc.person_knows_person k
JOIN {{zone_name}}.ldbc.person p2 ON k.dst = p2.id
WHERE k.src = 26388279068220
ORDER BY k.creationDate DESC, p2.id ASC;


-- ============================================================================
-- 33. LDBC SHORT Q5 — Message Creator
-- ============================================================================
-- Cypher equivalent:
--   MATCH (m:Message {id:1099511997932})-[:HAS_CREATOR]->(p:Person)
--   RETURN p.id, p.firstName, p.lastName
--
-- Golden: personId=26388279068220, Jun Wang

SELECT p.id AS personId, p.firstName, p.lastName
FROM {{zone_name}}.ldbc.comment c
JOIN {{zone_name}}.ldbc.comment_hasCreator_person chc ON c.id = chc.comment_id
JOIN {{zone_name}}.ldbc.person p ON chc.person_id = p.id
WHERE c.id = 1099511997932;


-- ============================================================================
-- 34. LDBC SHORT Q6 — Message Forum
-- ============================================================================
-- Cypher equivalent:
--   MATCH (m:Message {id:1099511997932})-[:REPLY_OF*0..]->(p:Post)
--         <-[:CONTAINER_OF]-(f:Forum)-[:HAS_MODERATOR]->(mod:Person)
--   RETURN f.id, f.title, mod.id, mod.firstName, mod.lastName
--
-- Golden: forumId=824633737506, title="Wall of Anh Pham",
--         moderator=Anh Pham

SELECT
    f.id AS forumId, f.title AS forumTitle,
    mod_p.id AS moderatorId, mod_p.firstName, mod_p.lastName
FROM {{zone_name}}.ldbc.comment c
JOIN {{zone_name}}.ldbc.comment_replyOf_post crp ON c.id = crp.comment_id
JOIN {{zone_name}}.ldbc.forum_containerOf_post fcp ON crp.post_id = fcp.post_id
JOIN {{zone_name}}.ldbc.forum f ON fcp.forum_id = f.id
JOIN {{zone_name}}.ldbc.forum_hasModerator_person fhm ON f.id = fhm.forum_id
JOIN {{zone_name}}.ldbc.person mod_p ON fhm.person_id = mod_p.id
WHERE c.id = 1099511997932;


-- ============================================================================
-- 35. LDBC Q2 — Recent Messages by Friends
-- ============================================================================
-- Cypher equivalent:
--   MATCH (:Person {id:19791209300143})-[:KNOWS]-(friend:Person)
--         <-[:HAS_CREATOR]-(message)
--   WHERE message.creationDate <= 1354060800000
--   RETURN friend.id, friend.firstName, friend.lastName,
--          message.id, message.content, message.creationDate
--   ORDER BY message.creationDate DESC, message.id ASC LIMIT 20
--
-- Golden: first result = The Kunda, messageId=1099511875186

SELECT
    p2.id AS personId, p2.firstName, p2.lastName,
    msg.id AS messageId,
    COALESCE(msg.content, '') AS messageContent,
    msg.creationDate AS messageCreationDate
FROM {{zone_name}}.ldbc.person_knows_person k
JOIN {{zone_name}}.ldbc.person p2 ON k.dst = p2.id
JOIN {{zone_name}}.ldbc.post_hasCreator_person phc ON p2.id = phc.person_id
JOIN {{zone_name}}.ldbc.post msg ON phc.post_id = msg.id
WHERE k.src = 19791209300143
  AND msg.creationDate <= 1354060800000
ORDER BY msg.creationDate DESC, msg.id ASC
LIMIT 20;


-- ============================================================================
-- 36. LDBC Q4 — New Tags in Time Window
-- ============================================================================
-- Cypher equivalent:
--   MATCH (person:Person {id:10995116278874})-[:KNOWS]-(:Person)
--         <-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag:Tag)
--   WHERE post.creationDate >= startDate AND post.creationDate < endDate
--   ... (exclude tags used before startDate)
--   RETURN tag.name, count(post) ORDER BY count DESC, tag ASC LIMIT 10
--
-- Golden: Norodom_Sihanouk (3), George_Clooney (1), Louis_Philippe_I (1)

SELECT t.name AS tagName, COUNT(DISTINCT po.id) AS postCount
FROM {{zone_name}}.ldbc.person_knows_person k
JOIN {{zone_name}}.ldbc.post_hasCreator_person phc ON k.dst = phc.person_id
JOIN {{zone_name}}.ldbc.post po ON phc.post_id = po.id
JOIN {{zone_name}}.ldbc.post_hasTag_tag pht ON po.id = pht.post_id
JOIN {{zone_name}}.ldbc.tag t ON pht.tag_id = t.id
WHERE k.src = 10995116278874
  AND po.creationDate >= 1338508800000
  AND po.creationDate < 1338508800000 + CAST(28 AS BIGINT) * 86400000
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.ldbc.person_knows_person k2
      JOIN {{zone_name}}.ldbc.post_hasCreator_person phc2 ON k2.dst = phc2.person_id
      JOIN {{zone_name}}.ldbc.post old ON phc2.post_id = old.id
      JOIN {{zone_name}}.ldbc.post_hasTag_tag pht2 ON old.id = pht2.post_id
      WHERE k2.src = 10995116278874
        AND pht2.tag_id = pht.tag_id
        AND old.creationDate < 1338508800000
  )
GROUP BY t.name
ORDER BY postCount DESC, tagName ASC
LIMIT 10;


-- ============================================================================
-- 37. LDBC Q6 — Tag Co-occurrence
-- ============================================================================
-- Cypher equivalent:
--   MATCH (person:Person {id:30786325579101})-[:KNOWS*1..2]-(friend:Person),
--         (friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(knownTag:Tag
--         {name:'Shakira'})
--   MATCH (post)-[:HAS_TAG]->(commonTag:Tag) WHERE commonTag <> knownTag
--   RETURN commonTag.name, count(post) ORDER BY count DESC, name ASC LIMIT 10
--
-- Golden: David_Foster (4), Muammar_Gaddafi (2), Robert_John_Mutt_Lange (2)

SELECT t2.name AS tagName, COUNT(DISTINCT po.id) AS postCount
FROM {{zone_name}}.ldbc.person_knows_person k1
LEFT JOIN {{zone_name}}.ldbc.person_knows_person k2 ON k1.dst = k2.src
JOIN {{zone_name}}.ldbc.post_hasCreator_person phc
    ON (k1.dst = phc.person_id OR k2.dst = phc.person_id)
JOIN {{zone_name}}.ldbc.post po ON phc.post_id = po.id
JOIN {{zone_name}}.ldbc.post_hasTag_tag pht1 ON po.id = pht1.post_id
JOIN {{zone_name}}.ldbc.tag t1 ON pht1.tag_id = t1.id AND t1.name = 'Shakira'
JOIN {{zone_name}}.ldbc.post_hasTag_tag pht2 ON po.id = pht2.post_id
JOIN {{zone_name}}.ldbc.tag t2 ON pht2.tag_id = t2.id AND t2.name <> 'Shakira'
WHERE k1.src = 30786325579101
GROUP BY t2.name
ORDER BY postCount DESC, tagName ASC
LIMIT 10;


-- ============================================================================
-- 38. LDBC Q7 — Recent Likes
-- ============================================================================
-- Cypher equivalent:
--   MATCH (person:Person {id:26388279067534})<-[:HAS_CREATOR]-(message)
--         <-[like:LIKES]-(liker:Person)
--   RETURN liker.id, liker.firstName, liker.lastName,
--          like.creationDate, message.id
--   ORDER BY like.creationDate DESC, message.id ASC LIMIT 20
--
-- Golden: first liker = Anh Nguyen (32985348834301),
--         likeDate=1347061110109, messageId=1030792374999

SELECT
    p2.id AS personId, p2.firstName, p2.lastName,
    lk.creationDate AS likeCreationDate,
    po.id AS messageId,
    COALESCE(po.content, po.imageFile) AS messageContent
FROM {{zone_name}}.ldbc.post_hasCreator_person phc
JOIN {{zone_name}}.ldbc.post po ON phc.post_id = po.id
JOIN {{zone_name}}.ldbc.person_likes_post lk ON po.id = lk.post_id
JOIN {{zone_name}}.ldbc.person p2 ON lk.person_id = p2.id
WHERE phc.person_id = 26388279067534
ORDER BY lk.creationDate DESC, po.id ASC
LIMIT 20;


-- ============================================================================
-- 39. LDBC Q8 — Recent Replies
-- ============================================================================
-- Cypher equivalent:
--   MATCH (start:Person {id:2199023256816})<-[:HAS_CREATOR]-()
--         <-[:REPLY_OF]-(comment:Comment)-[:HAS_CREATOR]->(person:Person)
--   RETURN person.id, person.firstName, person.lastName,
--          comment.creationDate, comment.id, comment.content
--   ORDER BY comment.creationDate DESC, comment.id ASC LIMIT 20
--
-- Golden: first reply by Ana Paula Silva, commentId=1099511667820

SELECT
    p2.id AS personId, p2.firstName, p2.lastName,
    c.creationDate AS commentCreationDate,
    c.id AS commentId,
    c.content AS commentContent
FROM {{zone_name}}.ldbc.post_hasCreator_person phc
JOIN {{zone_name}}.ldbc.comment_replyOf_post crp ON phc.post_id = crp.post_id
JOIN {{zone_name}}.ldbc.comment c ON crp.comment_id = c.id
JOIN {{zone_name}}.ldbc.comment_hasCreator_person chc ON c.id = chc.comment_id
JOIN {{zone_name}}.ldbc.person p2 ON chc.person_id = p2.id
WHERE phc.person_id = 2199023256816
ORDER BY c.creationDate DESC, c.id ASC
LIMIT 20;


-- ============================================================================
-- 40. LDBC Q12 — Expert Friends by TagClass
-- ============================================================================
-- Cypher equivalent:
--   MATCH (:Person {id:19791209300143})-[:KNOWS]-(friend:Person)
--   OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(comment:Comment)
--                  -[:REPLY_OF]->(:Post)-[:HAS_TAG]->(tag:Tag),
--                  (tag)-[:HAS_TYPE]->(tc:TagClass)
--   WHERE tc.name = 'BasketballPlayer'
--   RETURN friend.id, friend.firstName, friend.lastName,
--          collect(DISTINCT tag.name), count(DISTINCT comment)
--   ORDER BY count DESC, friend.id ASC LIMIT 20
--
-- Golden: Peng Zhang (8796093023000), replyCount=4

SELECT
    p2.id AS personId, p2.firstName, p2.lastName,
    COUNT(DISTINCT c.id) AS replyCount
FROM {{zone_name}}.ldbc.person_knows_person k
JOIN {{zone_name}}.ldbc.person p2 ON k.dst = p2.id
JOIN {{zone_name}}.ldbc.comment_hasCreator_person chc ON p2.id = chc.person_id
JOIN {{zone_name}}.ldbc.comment c ON chc.comment_id = c.id
JOIN {{zone_name}}.ldbc.comment_replyOf_post crp ON c.id = crp.comment_id
JOIN {{zone_name}}.ldbc.post_hasTag_tag pht ON crp.post_id = pht.post_id
JOIN {{zone_name}}.ldbc.tag t ON pht.tag_id = t.id
JOIN {{zone_name}}.ldbc.tag_hasType_tagclass tht ON t.id = tht.tag_id
JOIN {{zone_name}}.ldbc.tagclass tc ON tht.tagclass_id = tc.id
WHERE k.src = 19791209300143
  AND tc.name = 'BasketballPlayer'
GROUP BY p2.id, p2.firstName, p2.lastName
ORDER BY replyCount DESC, p2.id ASC
LIMIT 20;


-- ============================================================================
-- 41. CONTENT ANALYSIS — Most discussed tags
-- ============================================================================

SELECT t.name AS tagName, COUNT(*) AS comment_count
FROM {{zone_name}}.ldbc.comment_hasTag_tag cht
JOIN {{zone_name}}.ldbc.tag t ON cht.tag_id = t.id
GROUP BY t.name
ORDER BY comment_count DESC
LIMIT 15;


-- ############################################################################
-- PART 6: VERIFICATION SUMMARY
-- ############################################################################


-- ============================================================================
-- 42. AUTOMATED VERIFICATION — PASS/FAIL against golden values
-- ============================================================================
-- All checks should return PASS. Any FAIL indicates data loading issues
-- or algorithm correctness problems.

SELECT 'Person count = 1528' AS test,
       CASE WHEN cnt = 1528 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END AS result
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc.person)

UNION ALL
SELECT 'Comment count = 151043',
       CASE WHEN cnt = 151043 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc.comment)

UNION ALL
SELECT 'Post count = 135701',
       CASE WHEN cnt = 135701 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc.post)

UNION ALL
SELECT 'Forum count = 13750',
       CASE WHEN cnt = 13750 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc.forum)

UNION ALL
SELECT 'Place count = 1460',
       CASE WHEN cnt = 1460 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc.place)

UNION ALL
SELECT 'Organisation count = 7955',
       CASE WHEN cnt = 7955 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc.organisation)

UNION ALL
SELECT 'Tag count = 16080',
       CASE WHEN cnt = 16080 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc.tag)

UNION ALL
SELECT 'TagClass count = 71',
       CASE WHEN cnt = 71 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc.tagclass)

UNION ALL
SELECT 'KNOWS edge count = 14073',
       CASE WHEN cnt = 14073 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc.person_knows_person)

UNION ALL
SELECT 'No self-loops in KNOWS',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc.person_knows_person WHERE src = dst)

UNION ALL
SELECT 'All KNOWS endpoints exist',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (' || CAST(cnt AS VARCHAR) || ' orphans)' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc.person_knows_person k
    WHERE NOT EXISTS (SELECT 1 FROM {{zone_name}}.ldbc.person p WHERE p.id = k.src)
       OR NOT EXISTS (SELECT 1 FROM {{zone_name}}.ldbc.person p WHERE p.id = k.dst)
)

UNION ALL
SELECT 'Top hub degree >= 300',
       CASE WHEN max_deg >= 300 THEN 'PASS' ELSE 'FAIL (got ' || CAST(max_deg AS VARCHAR) || ')' END
FROM (
    SELECT MAX(deg) AS max_deg FROM (
        SELECT src, COUNT(*) AS deg FROM {{zone_name}}.ldbc.person_knows_person GROUP BY src
    )
)

UNION ALL
SELECT 'SQ1: Jun Wang exists at person 26388279068220',
       CASE WHEN cnt = 1 THEN 'PASS' ELSE 'FAIL' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc.person
    WHERE id = 26388279068220 AND firstName = 'Jun' AND lastName = 'Wang'
)

UNION ALL
SELECT 'SQ5: Comment 1099511997932 created by person 26388279068220',
       CASE WHEN cnt = 1 THEN 'PASS' ELSE 'FAIL' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc.comment_hasCreator_person
    WHERE comment_id = 1099511997932 AND person_id = 26388279068220
)

UNION ALL
SELECT 'SQ6: Comment 1099511997932 in forum 824633737506',
       CASE WHEN cnt >= 1 THEN 'PASS' ELSE 'FAIL' END
FROM (
    SELECT COUNT(*) AS cnt
    FROM {{zone_name}}.ldbc.comment_replyOf_post crp
    JOIN {{zone_name}}.ldbc.forum_containerOf_post fcp ON crp.post_id = fcp.post_id
    WHERE crp.comment_id = 1099511997932 AND fcp.forum_id = 824633737506
)

UNION ALL
SELECT 'Gender values valid (male/female only)',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc.person
    WHERE gender NOT IN ('male', 'female')
)

UNION ALL
SELECT 'Avg degree > 10',
       CASE WHEN avg_deg > 10.0 THEN 'PASS' ELSE 'FAIL (got ' || CAST(avg_deg AS VARCHAR) || ')' END
FROM (
    SELECT ROUND(CAST(COUNT(*) AS DOUBLE) * 2.0 / (SELECT COUNT(*) FROM {{zone_name}}.ldbc.person), 1) AS avg_deg
    FROM {{zone_name}}.ldbc.person_knows_person
);
