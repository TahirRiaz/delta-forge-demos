-- ============================================================================
-- Demo: European Travel — Incremental Wave-Based Enrichment — Queries
-- ============================================================================
-- Validates the parameter-driven incremental ingest flow:
--   • The silver catalog starts with the 4-country Nordic seed.
--   • After INVOKE (narrowed by query_param.codes = 'fi,ee,lv,lt'), the
--     bronze landing has exactly the 4 requested Baltic+Finland rows.
--   • The NOT EXISTS merge promoted those 4 new rows into silver,
--     leaving the 4 seed rows untouched.
--   • source_batch provenance proves each row's lineage.
--   • A replayed anti-join returns zero rows, proving idempotency.
--
-- The pattern on display: the ingest does not own "what to fetch next";
-- the TARGET TABLE does. Each incremental run reads the target for the
-- current high-water set of loaded codes, computes the gap, pushes that
-- gap into the ingest's query_param at run time (ALTER SET OPTIONS),
-- and INVOKEs. This demo encodes the first wave's gap at CREATE time
-- so it is self-contained and idempotent; queries.sql Query 4 is the
-- exact anti-join a pipeline would run to compute the next wave.
-- ============================================================================

-- ============================================================================
-- Query 1: Wave composition — silver holds 4 seed + 4 API rows
-- ============================================================================
-- Splits the catalog by source_batch. If either number drifts, a wave
-- either double-inserted or lost rows — both regressions of the merge
-- contract. Total = 8 is the headline state proof.

ASSERT ROW_COUNT = 1
ASSERT VALUE seed_count = 4
ASSERT VALUE baltic_count = 4
ASSERT VALUE total_count = 8
SELECT
    SUM(CASE WHEN source_batch = 'nordic_seed' THEN 1 ELSE 0 END) AS seed_count,
    SUM(CASE WHEN source_batch = 'baltic_api'  THEN 1 ELSE 0 END) AS baltic_count,
    COUNT(*)                                                      AS total_count
FROM {{zone_name}}.travel_waves.country_catalog;

-- ============================================================================
-- Query 2: Baltic wave content — exactly the four codes we asked for
-- ============================================================================
-- Proves the query_param.codes parameter was honoured on the wire:
-- only fi, ee, lv, lt landed, not the full European region. Exact
-- name match on each of the four — if the parameter was dropped or
-- the endpoint was misrouted, this assertion fails loud.

ASSERT ROW_COUNT = 4
ASSERT VALUE name_common = 'Finland'   WHERE cca2 = 'FI'
ASSERT VALUE name_common = 'Estonia'   WHERE cca2 = 'EE'
ASSERT VALUE name_common = 'Latvia'    WHERE cca2 = 'LV'
ASSERT VALUE name_common = 'Lithuania' WHERE cca2 = 'LT'
SELECT cca2, name_common, source_batch
FROM {{zone_name}}.travel_waves.country_catalog
WHERE source_batch = 'baltic_api'
ORDER BY cca2;

-- ============================================================================
-- Query 3: Seed preservation — the Nordic wave is unchanged
-- ============================================================================
-- The anti-join merge must not touch rows already present. Pick a
-- representative seed row (Norway) and assert both its payload and
-- its source_batch label survived the Baltic wave intact.

ASSERT ROW_COUNT = 1
ASSERT VALUE name_common = 'Norway' WHERE cca2 = 'NO'
ASSERT VALUE source_batch = 'nordic_seed' WHERE cca2 = 'NO'
ASSERT VALUE subregion = 'Northern Europe' WHERE cca2 = 'NO'
SELECT cca2, name_common, source_batch, subregion
FROM {{zone_name}}.travel_waves.country_catalog
WHERE cca2 = 'NO';

-- ============================================================================
-- Query 4: Incremental watermark — the "what to fetch next" anti-join
-- ============================================================================
-- This is the exact shape a scheduled pipeline runs against the target
-- table BEFORE every API call. `wanted` holds the full set a product
-- owner declares as "eventually we want all of these codes"; the
-- anti-join against the target drops the ones already loaded. Post-
-- merge, the result must be empty — proving the last wave completed
-- fully and giving the next wave a clean slate to pick up from.
--
-- In a production pipeline this query's output (a list of codes) is
-- what gets passed to:
--     ALTER API INGEST ... SET OPTIONS (query_param.codes = '<list>');
-- just before the next INVOKE. Keeping the compute in SQL against the
-- target table — not in an external manifest — is what makes the
-- incremental loop self-healing: a missed wave from a prior run is
-- automatically retried next time.

ASSERT ROW_COUNT = 1
ASSERT VALUE missing_codes = 0
SELECT COUNT(*) AS missing_codes
FROM (VALUES ('FI'), ('EE'), ('LV'), ('LT')) AS wanted(code)
WHERE code NOT IN (SELECT cca2 FROM {{zone_name}}.travel_waves.country_catalog);

-- ============================================================================
-- Query 5: Bronze landing — only the parameterised wave hit disk
-- ============================================================================
-- The bronze external table reads the raw JSON pages written by INVOKE.
-- Because query_param.codes narrowed the fetch to four codes, bronze
-- holds exactly four rows — never the full European catalog. If you
-- see 50+ rows here, the query parameter was silently dropped on the
-- wire (or a broader endpoint was hit instead of /v3.1/alpha).

ASSERT ROW_COUNT = 4
ASSERT VALUE name_common = 'Finland'   WHERE cca2 = 'FI'
ASSERT VALUE name_common = 'Estonia'   WHERE cca2 = 'EE'
ASSERT VALUE name_common = 'Latvia'    WHERE cca2 = 'LV'
ASSERT VALUE name_common = 'Lithuania' WHERE cca2 = 'LT'
SELECT cca2, name_common, region, subregion
FROM {{zone_name}}.travel_waves.alpha_batch_bronze
ORDER BY cca2;

-- ============================================================================
-- Query 6: ISO-code coverage — all 8 expected codes present
-- ============================================================================
-- One assertion per ISO alpha-2 code across both waves. Cross-cuts the
-- seed + API halves and proves no wave leaked a code that wasn't asked
-- for and none of the asked-for codes went missing.

ASSERT ROW_COUNT = 8
ASSERT VALUE name_common = 'Norway'    WHERE cca2 = 'NO'
ASSERT VALUE name_common = 'Sweden'    WHERE cca2 = 'SE'
ASSERT VALUE name_common = 'Denmark'   WHERE cca2 = 'DK'
ASSERT VALUE name_common = 'Iceland'   WHERE cca2 = 'IS'
ASSERT VALUE name_common = 'Finland'   WHERE cca2 = 'FI'
ASSERT VALUE name_common = 'Estonia'   WHERE cca2 = 'EE'
ASSERT VALUE name_common = 'Latvia'    WHERE cca2 = 'LV'
ASSERT VALUE name_common = 'Lithuania' WHERE cca2 = 'LT'
SELECT cca2, name_common, source_batch
FROM {{zone_name}}.travel_waves.country_catalog
ORDER BY cca2;

-- ============================================================================
-- Query 7: Population aggregate — silver sums cleanly after the merge
-- ============================================================================
-- silver's population is declared BIGINT so SUM works natively. The
-- total over the 8 countries (approximate census figures) sits around
-- 30-35M. BETWEEN keeps the check tolerant of minor REST Countries
-- data revisions on the Baltic half without letting a catastrophic
-- mis-cast (e.g. string parse landing zero) pass silently.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_population BETWEEN 28000000 AND 36000000
ASSERT VALUE nordic_population BETWEEN 20000000 AND 23000000
SELECT
    SUM(population)                                                           AS total_population,
    SUM(CASE WHEN source_batch = 'nordic_seed' THEN population ELSE 0 END)    AS nordic_population
FROM {{zone_name}}.travel_waves.country_catalog;

-- ============================================================================
-- Query 8: Silver Delta history — at least two writes visible
-- ============================================================================
-- CREATE (v0, schema only) + seed INSERT (v1) + Baltic merge INSERT
-- (v2) means DESCRIBE HISTORY must return ≥ 2 rows. Proves the Delta
-- transaction log is tracking each wave as a discrete version — which
-- is what makes VERSION AS OF time-travel work for wave rollback.

ASSERT ROW_COUNT >= 2
DESCRIBE HISTORY {{zone_name}}.travel_waves.country_catalog;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- One cross-cutting query exercising the whole pipeline:
--   • Row count in range
--   • Nordic seed preserved (has_norway, has_iceland, 4 nordic rows)
--   • Baltic wave merged (has_finland, has_estonia, 4 baltic rows)
--   • Every expected code present
-- If this passes, the credential resolved, the HTTPS fetch happened
-- with the query parameter intact, the flatten produced the expected
-- shape, and the anti-join merge preserved seed rows while adding
-- exactly the new wave.

ASSERT ROW_COUNT = 1
ASSERT VALUE country_count = 8
ASSERT VALUE has_norway = 1
ASSERT VALUE has_iceland = 1
ASSERT VALUE has_finland = 1
ASSERT VALUE has_estonia = 1
ASSERT VALUE nordic_total = 4
ASSERT VALUE baltic_total = 4
ASSERT VALUE every_code_present = 1
SELECT
    COUNT(*)                                                       AS country_count,
    SUM(CASE WHEN cca2 = 'NO' THEN 1 ELSE 0 END)                   AS has_norway,
    SUM(CASE WHEN cca2 = 'IS' THEN 1 ELSE 0 END)                   AS has_iceland,
    SUM(CASE WHEN cca2 = 'FI' THEN 1 ELSE 0 END)                   AS has_finland,
    SUM(CASE WHEN cca2 = 'EE' THEN 1 ELSE 0 END)                   AS has_estonia,
    SUM(CASE WHEN source_batch = 'nordic_seed' THEN 1 ELSE 0 END)  AS nordic_total,
    SUM(CASE WHEN source_batch = 'baltic_api'  THEN 1 ELSE 0 END)  AS baltic_total,
    CASE WHEN COUNT(DISTINCT cca2) = 8 THEN 1 ELSE 0 END           AS every_code_present
FROM {{zone_name}}.travel_waves.country_catalog;
