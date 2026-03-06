# LDBC Social Network Benchmark — Full Model Graph Verification

Verifies Delta Forge graph algorithms and SQL/Cypher queries against the
industry-standard LDBC Social Network Benchmark with official golden values.

## Data Source

[LDBC Social Network Benchmark](https://ldbcouncil.org/benchmarks/snb/) —
Scale Factor 0.1, CsvBasic format. Golden values from official validation
parameters produced by the LDBC reference implementation.

## Entity Tables (8 types)

| Table | Rows | Description |
|-------|------|-------------|
| `person` | 1,528 | People with name, gender, birthday, location |
| `comment` | 151,043 | Comment messages with content |
| `post` | 135,701 | Post messages with content and language |
| `forum` | 13,750 | Forums: walls, groups, albums |
| `place` | 1,460 | Cities, countries, continents |
| `organisation` | 7,955 | Companies and universities |
| `tag` | 16,080 | Content tags |
| `tagclass` | 71 | Tag classification hierarchy |

## Relationship Tables (23 types)

| Table | Rows | Description |
|-------|------|-------------|
| `person_knows_person` | 14,073 | Friendship edges |
| `comment_hasCreator_person` | 151,043 | Comment authorship |
| `post_hasCreator_person` | 135,701 | Post authorship |
| `comment_isLocatedIn_place` | 151,043 | Comment geographic origin |
| `post_isLocatedIn_place` | 135,701 | Post geographic origin |
| `person_isLocatedIn_place` | 1,528 | Person residence |
| `comment_replyOf_comment` | 76,787 | Comment reply chains |
| `comment_replyOf_post` | 74,256 | Comments on posts |
| `comment_hasTag_tag` | 191,303 | Comment tagging |
| `post_hasTag_tag` | 51,118 | Post tagging |
| `forum_hasTag_tag` | 47,697 | Forum topics |
| `forum_containerOf_post` | 135,701 | Posts within forums |
| `forum_hasMember_person` | 123,268 | Forum membership |
| `forum_hasModerator_person` | 13,750 | Forum moderation |
| `person_hasInterest_tag` | 35,475 | Person interests |
| `person_likes_comment` | 62,225 | Comment likes |
| `person_likes_post` | 47,215 | Post likes |
| `person_studyAt_organisation` | 1,209 | Education history |
| `person_workAt_organisation` | 3,313 | Employment history |
| `person_email` | 3,310 | Email addresses |
| `person_speaks_language` | 3,385 | Spoken languages |
| `organisation_isLocatedIn_place` | 7,955 | Organisation locations |
| `place_isPartOf_place` | 1,454 | Place hierarchy |
| `tag_hasType_tagclass` | 16,080 | Tag classification |
| `tagclass_isSubclassOf_tagclass` | 70 | TagClass hierarchy |

## Golden Validation Values

### LDBC Short Query 1 — Person Profile
`personId=26388279068220` → Jun Wang, female, Opera browser, cityId=507

### LDBC Short Query 5 — Message Creator
`messageId=1099511997932` → created by person 26388279068220 (Jun Wang)

### LDBC Short Query 6 — Message Forum
`messageId=1099511997932` → forum 824633737506 "Wall of Anh Pham"

### LDBC Q4 — New Tags
`personId=10995116278874, start=1338508800000, 28 days`
→ Norodom_Sihanouk (3), George_Clooney (1), Louis_Philippe_I (1)

### LDBC Q6 — Tag Co-occurrence
`personId=30786325579101, tag=Shakira`
→ David_Foster (4), Muammar_Gaddafi (2), Robert_John_Mutt_Lange (2)

### LDBC Q13 — Shortest Path
| Source | Target | Path Length |
|--------|--------|-------------|
| 32985348833679 | 26388279067108 | 3 |
| 26388279066869 | 6597069768287 | 2 |
| 17592186045370 | 26388279066795 | 2 |
| 6597069767300 | 17592186045370 | 2 |

### Degree Centrality (Top 5)
| Person ID | Total Degree |
|-----------|-------------|
| 26388279067534 | 340 |
| 32985348834375 | 338 |
| 2199023256816 | 269 |
| 24189255811566 | 256 |
| 6597069767242 | 230 |

## Algorithms Verified

| Algorithm | Queries | Golden Value Source |
|-----------|---------|---------------------|
| PageRank | #20 | High-degree nodes rank highest |
| Degree centrality | #21 | Raw CSV degree counts |
| Betweenness centrality | #22 | Bridge node identification |
| Closeness centrality | #23 | Central positioning |
| Connected components | #24 | Expected single component |
| Louvain communities | #25 | Community structure |
| Triangle count | #26 | Clustering coefficient |
| Shortest path | #27, #28 | LDBC Q13 validation params |
| BFS traversal | #29 | Distance distribution from hub |

## Mixed SQL + Cypher Queries (queries 26–30)

Demonstrates Delta Forge's ability to join Cypher graph results with Delta
tables using the `cypher()` table function. Cypher handles graph traversal and
algorithms; SQL enriches results with relational data.

| Query | # | Description |
|-------|---|-------------|
| Friends + locations | #26 | KNOWS traversal joined with place hierarchy |
| PageRank + employment | #27 | Top influencers joined with work history |
| Shortest path + profiles | #28 | Path nodes enriched with person/city/country |
| Communities + interests | #29 | Louvain communities joined with tag interests |
| Hubs + content activity | #30 | Degree centrality joined with post/comment counts |

## LDBC Interactive Queries Verified

| Query | # | Description |
|-------|---|-------------|
| Short Q1 | #31 | Person profile lookup |
| Short Q3 | #32 | Person's friends list |
| Short Q5 | #33 | Message creator lookup |
| Short Q6 | #34 | Message forum lookup |
| Q2 | #35 | Recent messages by friends |
| Q4 | #36 | New tags in time window |
| Q6 | #37 | Tag co-occurrence |
| Q8 | #39 | Recent replies |
| Q12 | #40 | Expert friends by tag class |

## How to Verify

Run **Query #42 (Verification Summary)** for automated PASS/FAIL checks:
- All 8 entity counts match expected values
- KNOWS edge count = 14,073
- No self-loops, all endpoints valid
- Top hub degree >= 300
- Specific golden values from validation params verified

## Data Notes

- All CSV files are pipe-delimited (`|`), not comma-delimited
- Original LDBC headers with dots (`Person.id`) and duplicates have been renamed
- Timestamps are epoch milliseconds stored as BIGINT
- Person IDs are large integers (up to ~35 trillion) — BIGINT required
- Total data size: ~62MB across 34 CSV files
