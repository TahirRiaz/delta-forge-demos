-- ============================================================================
-- Demo: Pokédex First-Generation Reference — Queries
-- ============================================================================
-- Validates offset-pagination + nested-root flatten + dex_id parse:
--   • Exactly 100 rows (5 pages × 20 per page = 100) from offsets 0..80.
--   • The flatten's root_path = "$.results" correctly pulled rows out of
--     the PokeAPI envelope (not one row per envelope).
--   • Specific pinned entries: Bulbasaur #1, Pikachu #25, Voltorb #100 —
--     the canonical Kanto-dex anchors every Pokémon reference uses.
--   • dex_id parsed from detail_url is a contiguous 1..100 integer range.
--   • Bronze ↔ silver promotion preserved every row, every name unique.
--
-- Stability note: PokeAPI's National Dex is immutable upstream — IDs 1-100
-- have named the same creatures since 1996, and the JSON shape has been
-- stable since the v2 API launch. Row count and per-entry names are
-- exact-asserted, not ranged.
-- ============================================================================

-- ============================================================================
-- Query 1: Pokédex Row Count — 5 pages × 20 per page = 100
-- ============================================================================
-- Anything other than 100 means the pagination loop drifted: either
-- max_pages was missed, a page response was empty (flatten produced 0
-- rows for one envelope), or the root_path didn't resolve to $.results.

ASSERT ROW_COUNT = 1
ASSERT VALUE dex_count = 100
SELECT COUNT(*) AS dex_count
FROM {{zone_name}}.game_ref.pokedex_bronze;

-- ============================================================================
-- Query 2: Canon-Entry Presence — Bulbasaur, Pikachu, Voltorb
-- ============================================================================
-- Three canonical names anchor the first, middle, and last entries of
-- the range. Each must appear exactly once. If any is missing, the
-- flatten skipped an envelope or the offset math dropped a page.

ASSERT ROW_COUNT = 1
ASSERT VALUE has_bulbasaur = 1
ASSERT VALUE has_pikachu = 1
ASSERT VALUE has_voltorb = 1
SELECT
    SUM(CASE WHEN pokemon_name = 'bulbasaur' THEN 1 ELSE 0 END) AS has_bulbasaur,
    SUM(CASE WHEN pokemon_name = 'pikachu'   THEN 1 ELSE 0 END) AS has_pikachu,
    SUM(CASE WHEN pokemon_name = 'voltorb'   THEN 1 ELSE 0 END) AS has_voltorb
FROM {{zone_name}}.game_ref.pokedex_bronze;

-- ============================================================================
-- Query 3: Dex-ID Parse — REGEXP_REPLACE round-trip
-- ============================================================================
-- detail_url ends in `/pokemon/<n>/`. The silver-promotion
-- REGEXP_REPLACE + CAST extracts the integer dex_id. Specific pinned
-- anchors prove the regex handled the single-digit (1), double-digit
-- (25), and three-digit (100) cases.

ASSERT ROW_COUNT = 1
ASSERT VALUE bulbasaur_dex = 1
ASSERT VALUE pikachu_dex = 25
ASSERT VALUE voltorb_dex = 100
SELECT
    MAX(CASE WHEN pokemon_name = 'bulbasaur' THEN dex_id END) AS bulbasaur_dex,
    MAX(CASE WHEN pokemon_name = 'pikachu'   THEN dex_id END) AS pikachu_dex,
    MAX(CASE WHEN pokemon_name = 'voltorb'   THEN dex_id END) AS voltorb_dex
FROM {{zone_name}}.game_ref.pokedex_silver;

-- ============================================================================
-- Query 4: Contiguous ID Range — 1..100 with no gaps or duplicates
-- ============================================================================
-- The first-generation dex is a contiguous 1..100 integer sequence.
-- Every value appears exactly once — COUNT(DISTINCT) must equal 100 AND
-- MIN/MAX must pin the range.

ASSERT ROW_COUNT = 1
ASSERT VALUE min_dex = 1
ASSERT VALUE max_dex = 100
ASSERT VALUE distinct_dex = 100
SELECT
    MIN(dex_id)            AS min_dex,
    MAX(dex_id)            AS max_dex,
    COUNT(DISTINCT dex_id) AS distinct_dex
FROM {{zone_name}}.game_ref.pokedex_silver;

-- ============================================================================
-- Query 5: Name-Formatting Invariants — all distinct, all lowercase
-- ============================================================================
-- Every name is unique and PokeAPI emits them in lowercase. This
-- catches flatten regressions where the upstream casing changes, or
-- where accidental whitespace / duplicate rows slip in.

ASSERT ROW_COUNT = 1
ASSERT VALUE distinct_names = 100
ASSERT VALUE all_lowercase = 100
SELECT
    COUNT(DISTINCT pokemon_name)                                       AS distinct_names,
    SUM(CASE WHEN LOWER(pokemon_name) = pokemon_name THEN 1 ELSE 0 END) AS all_lowercase
FROM {{zone_name}}.game_ref.pokedex_bronze;

-- ============================================================================
-- Query 6: Silver Delta History — v0 schema + v1 INSERT
-- ============================================================================
-- CREATE (v0, schema-only) + INSERT (v1, the bronze→silver promotion).
-- DESCRIBE HISTORY must surface at least 2 rows — prerequisite for
-- VERSION AS OF rollback.

ASSERT ROW_COUNT >= 2
DESCRIBE HISTORY {{zone_name}}.game_ref.pokedex_silver;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- One cross-cutting query covering the whole pipeline: row count,
-- distinct names, dex range, every URL on pokeapi.co host, and the
-- two canonical anchor rows (dex 1 / dex 100).

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 100
ASSERT VALUE distinct_names = 100
ASSERT VALUE dex_min = 1
ASSERT VALUE dex_max = 100
ASSERT VALUE url_pokeapi_pct = 100
ASSERT VALUE bulba_first = 1
ASSERT VALUE voltorb_last = 1
SELECT
    COUNT(*)                                                                           AS total_rows,
    COUNT(DISTINCT pokemon_name)                                                       AS distinct_names,
    MIN(dex_id)                                                                        AS dex_min,
    MAX(dex_id)                                                                        AS dex_max,
    SUM(CASE WHEN detail_url LIKE 'https://pokeapi.co/%' THEN 1 ELSE 0 END)            AS url_pokeapi_pct,
    SUM(CASE WHEN dex_id = 1   AND pokemon_name = 'bulbasaur' THEN 1 ELSE 0 END)       AS bulba_first,
    SUM(CASE WHEN dex_id = 100 AND pokemon_name = 'voltorb'   THEN 1 ELSE 0 END)       AS voltorb_last
FROM {{zone_name}}.game_ref.pokedex_silver;
