-- ============================================================================
-- Delta Unicode & Encoding -- Educational Queries
-- ============================================================================
-- WHAT: Delta tables store strings as UTF-8 in Parquet, providing full Unicode
--       support for CJK ideographs, Arabic script, Latin diacritics, and more.
-- WHY:  Global applications must store product names, user content, and metadata
--       faithfully in their original scripts without data corruption or mojibake.
-- HOW:  Parquet uses UTF-8 byte arrays for STRING/VARCHAR columns. Delta's
--       transaction log (JSON) also uses UTF-8, so partition values containing
--       international characters are preserved end-to-end.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Multi-script product names side by side
-- ============================================================================
-- Each product has both an English name and a local-script name. The local
-- names span Japanese (kanji/katakana), Chinese (simplified), Korean (hangul),
-- Arabic, Hebrew, Greek, Turkish, and European diacritics.
-- All stored in the same VARCHAR column via Parquet's UTF-8 encoding.

ASSERT ROW_COUNT = 8
SELECT id, product_name, product_name_local, country, region
FROM {{zone_name}}.delta_demos.global_products
WHERE id IN (1, 2, 3, 33, 42, 19, 11, 44)
ORDER BY region, id;


-- ============================================================================
-- EXPLORE: Partitioning with Unicode -- how Delta handles it
-- ============================================================================
-- This table is PARTITIONED BY (region). Delta creates separate directories
-- for each partition value. While region values here are ASCII, the data
-- within each partition contains full Unicode strings. The partition pruning
-- works identically regardless of the character encoding within the data.

ASSERT ROW_COUNT = 5
SELECT region, COUNT(*) AS products,
       COUNT(DISTINCT country) AS countries
FROM {{zone_name}}.delta_demos.global_products
GROUP BY region
ORDER BY region;


-- ============================================================================
-- LEARN: CJK character storage -- Japanese, Chinese, Korean
-- ============================================================================
-- CJK characters use 3 bytes per character in UTF-8 (vs 1 byte for ASCII).
-- This means a 5-character Japanese name takes 15 bytes of storage. Delta and
-- Parquet handle this transparently -- the logical string length and the
-- physical byte length differ, but queries work on logical characters.

-- Verify Japanese Unicode preserved after write/read cycle
ASSERT VALUE product_name_local = '東京タワー模型'
SELECT product_name_local FROM {{zone_name}}.delta_demos.global_products WHERE id = 1;

ASSERT ROW_COUNT = 10
SELECT id, product_name, product_name_local,
       LENGTH(product_name_local) AS char_length,
       country
FROM {{zone_name}}.delta_demos.global_products
WHERE region = 'Asia'
ORDER BY id;


-- ============================================================================
-- LEARN: Right-to-left scripts -- Arabic and Hebrew
-- ============================================================================
-- Arabic and Hebrew are written right-to-left (RTL). Delta stores the logical
-- character sequence in reading order; display direction is a UI concern, not
-- a storage concern. The bytes in Parquet are always in logical order.

ASSERT ROW_COUNT = 10
SELECT id, product_name, product_name_local, country
FROM {{zone_name}}.delta_demos.global_products
WHERE region = 'MiddleEast'
ORDER BY id;


-- ============================================================================
-- MUTATE: 20% price increase for Europe region
-- ============================================================================
-- Delta's copy-on-write mechanism rewrites the affected Parquet row groups when
-- UPDATE executes. The price column changes, but the VARCHAR columns — including
-- all multi-byte diacritic characters — in the same row groups must be
-- preserved exactly. 10 Europe rows are updated.

ASSERT ROW_COUNT = 10
UPDATE {{zone_name}}.delta_demos.global_products
SET price = ROUND(price * 1.20, 2)
WHERE region = 'Europe';


-- ============================================================================
-- LEARN: Latin diacritics and special characters
-- ============================================================================
-- European languages use diacritics (umlauts, accents, cedillas) that are
-- multi-byte in UTF-8. Characters like 'ü' (0xC3 0xBC) or 'é' (0xC3 0xA9)
-- take 2 bytes each. Delta preserves these exactly through the UPDATE above,
-- which rewrote every Europe row group.

-- Verify German diacritics preserved and price updated (id=11)
ASSERT VALUE product_name_local = 'Würstchen'
SELECT product_name_local FROM {{zone_name}}.delta_demos.global_products WHERE id = 11;

ASSERT ROW_COUNT = 10
SELECT id, product_name, product_name_local, price, currency, country
FROM {{zone_name}}.delta_demos.global_products
WHERE region = 'Europe'
ORDER BY id;


-- ============================================================================
-- MUTATE: Remove discontinued products (price < 5.0)
-- ============================================================================
-- id=9  (Bamboo Chopsticks, Asia,   price=3.50) and
-- id=40 (Madagascar Pepper, Africa, price=3.50) fall below the threshold.
-- id=13 (Jalapeño Salsa, Europe, was 4.99 → 5.99 after UPDATE) survives.
-- Delta rewrites the affected Parquet files without the deleted rows.

ASSERT ROW_COUNT = 2
DELETE FROM {{zone_name}}.delta_demos.global_products
WHERE price < 5.0;


-- ============================================================================
-- LEARN: DELETE operations and Unicode data integrity
-- ============================================================================
-- The DELETE above used copy-on-write: Delta rewrote the affected Parquet
-- files WITHOUT the deleted rows. The remaining Unicode strings must survive
-- this rewrite perfectly. We verify the deleted rows are gone and that
-- neighbors are intact.

-- Verify deleted products (ids 9, 40) are truly gone
ASSERT VALUE deleted_count = 0
SELECT COUNT(*) AS deleted_count FROM {{zone_name}}.delta_demos.global_products WHERE id IN (9, 40);

ASSERT ROW_COUNT = 5
SELECT region, COUNT(*) AS remaining_products
FROM {{zone_name}}.delta_demos.global_products
GROUP BY region
ORDER BY region;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count is 48
ASSERT ROW_COUNT = 48
SELECT * FROM {{zone_name}}.delta_demos.global_products;

-- Verify Asia region has 9 products
ASSERT VALUE asia_count = 9
SELECT COUNT(*) AS asia_count FROM {{zone_name}}.delta_demos.global_products WHERE region = 'Asia';

-- Verify Europe price was updated (id=14, 20% increase)
ASSERT VALUE price = 14.4
SELECT price FROM {{zone_name}}.delta_demos.global_products WHERE id = 14;

-- Verify deleted products (ids 9, 40) are gone
ASSERT VALUE deleted_count = 0
SELECT COUNT(*) AS deleted_count FROM {{zone_name}}.delta_demos.global_products WHERE id IN (9, 40);

-- Verify Japanese Unicode preserved
ASSERT VALUE product_name_local = '東京タワー模型'
SELECT product_name_local FROM {{zone_name}}.delta_demos.global_products WHERE id = 1;

-- Verify Arabic Unicode preserved
ASSERT VALUE product_name_local = 'فلافل لبناني'
SELECT product_name_local FROM {{zone_name}}.delta_demos.global_products WHERE id = 42;

-- Verify German diacritics preserved
ASSERT VALUE product_name_local = 'Würstchen'
SELECT product_name_local FROM {{zone_name}}.delta_demos.global_products WHERE id = 11;

-- Verify 5 distinct regions exist
ASSERT VALUE region_count = 5
SELECT COUNT(DISTINCT region) AS region_count FROM {{zone_name}}.delta_demos.global_products;
