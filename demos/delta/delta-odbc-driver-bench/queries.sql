-- ============================================================================
-- Demo: ACME Corporation Production Warehouse (ODBC Driver Wire Benchmark)
-- ============================================================================
-- Every assertion below is derived from closed-form math against the
-- generation rule, never from an engine round trip. If the engine drifts on
-- counts, sums, or selected cell values, the corresponding ODBC code path is
-- the suspect.

-- ============================================================================
-- Query 1: acme.market_ticks row + sum + spot-check
-- ============================================================================
-- 1M-row equity tick stream. SUM(tick_id) is the closed form
-- N*(N+1)/2 = 1_000_000 * 1_000_001 / 2 = 500_000_500_000.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 1000000
ASSERT VALUE sum_tick_id = 500000500000
ASSERT VALUE min_tick_id = 1
ASSERT VALUE max_tick_id = 1000000
SELECT
    COUNT(*) AS n_rows,
    SUM(tick_id) AS sum_tick_id,
    MIN(tick_id) AS min_tick_id,
    MAX(tick_id) AS max_tick_id
FROM {{zone_name}}.acme.market_ticks;

-- ============================================================================
-- Query 2: acme.market_ticks per-cell deterministic spot-check at tick_id=12345
-- ============================================================================
-- Pins exact values for every column so a wire-decode or cast regression on
-- INT64/DOUBLE drops out immediately. 12345 % 1024 = 57 because
-- 12 * 1024 = 12288 and 12345 - 12288 = 57.

ASSERT ROW_COUNT = 1
ASSERT VALUE instrument_id = 12345
ASSERT VALUE bid_size_units = 86415
ASSERT VALUE exchange_lookup_code = 57
ASSERT VALUE last_price = 12345.0
ASSERT VALUE bid_price = 6172.5
ASSERT VALUE ask_spread_bps = 3.45
SELECT instrument_id, bid_size_units, exchange_lookup_code, last_price, bid_price, ask_spread_bps
FROM {{zone_name}}.acme.market_ticks
WHERE tick_id = 12345;

-- ============================================================================
-- Query 3: acme.manufacturing_runs row count + sum + boolean partition counts
-- ============================================================================
-- 100K rows, 60 fixed-width sensor cols. Multiples of 2 in [1, 100K] = 50_000;
-- multiples of 3 in [1, 100K] = floor(100_000/3) = 33_333.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 100000
ASSERT VALUE sum_run_id = 5000050000
ASSERT VALUE n_overcurrent_true = 50000
ASSERT VALUE n_alarm_true = 33333
SELECT
    COUNT(*) AS n_rows,
    SUM(run_id) AS sum_run_id,
    SUM(CASE WHEN is_overcurrent THEN 1 ELSE 0 END) AS n_overcurrent_true,
    SUM(CASE WHEN has_alarm THEN 1 ELSE 0 END) AS n_alarm_true
FROM {{zone_name}}.acme.manufacturing_runs;

-- ============================================================================
-- Query 4: acme.manufacturing_runs spot-check at run_id=12345
-- ============================================================================
-- machine_serial_l01 = run_id, machine_serial_l02 = run_id+1, etc.
-- 12345 % 32767 = 12345 (since 12345 < 32767).
-- 12345 % 127 = 26 because 12345 / 127 = 97 remainder 26.

ASSERT ROW_COUNT = 1
ASSERT VALUE machine_serial_l01 = 12345
ASSERT VALUE machine_serial_l02 = 12346
ASSERT VALUE machine_serial_l12 = 12356
ASSERT VALUE cycle_count_i01 = 12345
ASSERT VALUE batch_size_s01 = 12345
ASSERT VALUE shift_id_t01 = 26
SELECT machine_serial_l01, machine_serial_l02, machine_serial_l12, cycle_count_i01, batch_size_s01, shift_id_t01
FROM {{zone_name}}.acme.manufacturing_runs
WHERE run_id = 12345;

-- ============================================================================
-- Query 5: acme.support_tickets null density + length spot-check
-- ============================================================================
-- 500K rows, NULL when ticket_id % 10 IN (0,1,2): exactly 30% NULL = 150_000
-- rows. 350_000 non-null rows. md5*6 = 192 chars; lpad to 20 chars.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 500000
ASSERT VALUE n_code_null = 150000
ASSERT VALUE n_code_not_null = 350000
ASSERT VALUE n_summary_null = 150000
SELECT
    COUNT(*) AS n_rows,
    SUM(CASE WHEN ticket_code IS NULL THEN 1 ELSE 0 END) AS n_code_null,
    SUM(CASE WHEN ticket_code IS NOT NULL THEN 1 ELSE 0 END) AS n_code_not_null,
    SUM(CASE WHEN summary    IS NULL THEN 1 ELSE 0 END) AS n_summary_null
FROM {{zone_name}}.acme.support_tickets;

-- ============================================================================
-- Query 6: acme.support_tickets per-cell content + length at ticket_id=12345
-- ============================================================================
-- 12345 % 10 = 5 so the row is non-null. lpad('12345', 20, '0') is fully
-- determined and must be exact.

ASSERT ROW_COUNT = 1
ASSERT VALUE ticket_code = '00000000000000012345'
ASSERT VALUE code_len = 20
ASSERT VALUE summary_len = 192
ASSERT VALUE description_len = 192
SELECT
    ticket_code,
    LENGTH(ticket_code) AS code_len,
    LENGTH(summary) AS summary_len,
    LENGTH(description) AS description_len
FROM {{zone_name}}.acme.support_tickets
WHERE ticket_id = 12345;

-- ============================================================================
-- Query 7: acme.support_tickets null witness at ticket_id=10
-- ============================================================================
-- 10 % 10 = 0, so every text column is NULL by the generation rule.

ASSERT ROW_COUNT = 1
ASSERT VALUE ticket_code      IS NULL
ASSERT VALUE summary          IS NULL
ASSERT VALUE description      IS NULL
ASSERT VALUE resolution_notes IS NULL
ASSERT VALUE internal_comment IS NULL
SELECT ticket_code, summary, description, resolution_notes, internal_comment
FROM {{zone_name}}.acme.support_tickets
WHERE ticket_id = 10;

-- ============================================================================
-- Query 8: acme.product_catalog null density + cell length
-- ============================================================================
-- 100K rows, NULL when product_id % 20 = 0: exactly 5% NULL = 5_000 rows.
-- Each non-null cell is exactly 50 chars.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 100000
ASSERT VALUE n_en_null = 5000
ASSERT VALUE n_en_not_null = 95000
ASSERT VALUE n_ha_null = 5000
SELECT
    COUNT(*) AS n_rows,
    SUM(CASE WHEN display_name_en IS NULL THEN 1 ELSE 0 END) AS n_en_null,
    SUM(CASE WHEN display_name_en IS NOT NULL THEN 1 ELSE 0 END) AS n_en_not_null,
    SUM(CASE WHEN display_name_ha IS NULL THEN 1 ELSE 0 END) AS n_ha_null
FROM {{zone_name}}.acme.product_catalog;

-- ============================================================================
-- Query 9: acme.product_catalog length + null spot at product_id=1 and 20
-- ============================================================================
-- 1 % 20 = 1 (not null, len 50). 20 % 20 = 0 (null).

ASSERT ROW_COUNT = 2
ASSERT VALUE en_len = 50 WHERE product_id = 1
ASSERT VALUE ru_len = 50 WHERE product_id = 1
ASSERT VALUE ha_len = 50 WHERE product_id = 1
ASSERT VALUE en_len IS NULL WHERE product_id = 20
SELECT
    product_id,
    LENGTH(display_name_en) AS en_len,
    LENGTH(display_name_ru) AS ru_len,
    LENGTH(display_name_ha) AS ha_len
FROM {{zone_name}}.acme.product_catalog
WHERE product_id IN (1, 20)
ORDER BY product_id;

-- ============================================================================
-- Query 10: acme.knowledge_articles row count + per-cell length
-- ============================================================================
-- 10K rows. Every cell is exactly md5 (32 chars) repeated 200 times = 6400
-- chars. Tests SQLGetData chunked-read path with cells well over typical
-- buf_len boundaries. SUM(article_id) closed form = 50_005_000.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 10000
ASSERT VALUE sum_article_id = 50005000
ASSERT VALUE min_abstract_len = 6400
ASSERT VALUE max_abstract_len = 6400
ASSERT VALUE min_disclaimer_len = 6400
SELECT
    COUNT(*) AS n_rows,
    SUM(article_id) AS sum_article_id,
    MIN(LENGTH(abstract_text)) AS min_abstract_len,
    MAX(LENGTH(abstract_text)) AS max_abstract_len,
    MIN(LENGTH(legal_disclaimer)) AS min_disclaimer_len
FROM {{zone_name}}.acme.knowledge_articles;

-- ============================================================================
-- Query 11: acme.document_archive row count + per-cell byte length envelopes
-- ============================================================================
-- thumbnail_png  = 32 * (1 + rn%32)   -> [32, 1024]   bytes
-- preview_pdf    = 32 * (1 + rn%64)   -> [32, 2048]   bytes
-- archived       = 32 * (1 + rn%1024) -> [32, 32768]  bytes
-- N=5_000. SUM(document_id) closed form = 12_502_500.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 5000
ASSERT VALUE sum_document_id = 12502500
ASSERT VALUE min_thumb_len = 32
ASSERT VALUE max_thumb_len = 1024
ASSERT VALUE min_preview_len = 32
ASSERT VALUE max_preview_len = 2048
ASSERT VALUE min_archive_len = 32
SELECT
    COUNT(*) AS n_rows,
    SUM(document_id) AS sum_document_id,
    MIN(OCTET_LENGTH(thumbnail_png)) AS min_thumb_len,
    MAX(OCTET_LENGTH(thumbnail_png)) AS max_thumb_len,
    MIN(OCTET_LENGTH(preview_pdf_first_page)) AS min_preview_len,
    MAX(OCTET_LENGTH(preview_pdf_first_page)) AS max_preview_len,
    MIN(OCTET_LENGTH(archived_attachment)) AS min_archive_len
FROM {{zone_name}}.acme.document_archive;

-- ============================================================================
-- Query 12: acme.banking_transactions row + sum + decimal spot-check
-- ============================================================================
-- 500K rows. SUM(transaction_id) closed form = 125_000_250_000.
-- withholding_tax at transaction_id=12345:
-- (12345 % 1_000_000) + 0.000000001 = 12345.000000001.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 500000
ASSERT VALUE sum_transaction_id = 125000250000
SELECT
    COUNT(*) AS n_rows,
    SUM(transaction_id) AS sum_transaction_id
FROM {{zone_name}}.acme.banking_transactions;

ASSERT ROW_COUNT = 1
ASSERT VALUE amount_usd = 12345.123456789
ASSERT VALUE withholding_tax = 12345.000000001
SELECT amount_usd, withholding_tax
FROM {{zone_name}}.acme.banking_transactions
WHERE transaction_id = 12345;

-- ============================================================================
-- Query 13: acme.shipment_orders row + sum
-- ============================================================================
-- 50K rows. STRUCT/ARRAY/MAP exercise the format-bound wire path.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 50000
ASSERT VALUE sum_order_id = 1250025000
SELECT
    COUNT(*) AS n_rows,
    SUM(order_id) AS sum_order_id
FROM {{zone_name}}.acme.shipment_orders;

-- ============================================================================
-- Query 14: acme.patient_records null density across all 30 nullable columns
-- ============================================================================
-- 500K rows, 30 nullable cols. Each col populated only when record_id%20=0,
-- so non-null = 25_000 and null = 475_000 per column. We sample columns of
-- different physical layouts (BIGINT, STRING, DECIMAL, BOOLEAN).

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 500000
ASSERT VALUE n_lab_not_null = 25000
ASSERT VALUE n_lab_null = 475000
ASSERT VALUE n_note_not_null = 25000
ASSERT VALUE n_billing_not_null = 25000
ASSERT VALUE n_amended_not_null = 25000
SELECT
    COUNT(*) AS n_rows,
    SUM(CASE WHEN lab_value_l01      IS NOT NULL THEN 1 ELSE 0 END) AS n_lab_not_null,
    SUM(CASE WHEN lab_value_l01      IS NULL     THEN 1 ELSE 0 END) AS n_lab_null,
    SUM(CASE WHEN clinical_note_s01  IS NOT NULL THEN 1 ELSE 0 END) AS n_note_not_null,
    SUM(CASE WHEN billing_amt_m01    IS NOT NULL THEN 1 ELSE 0 END) AS n_billing_not_null,
    SUM(CASE WHEN is_amended         IS NOT NULL THEN 1 ELSE 0 END) AS n_amended_not_null
FROM {{zone_name}}.acme.patient_records;

-- ============================================================================
-- Query 15: acme.patient_records populated witness at record_id=20
-- ============================================================================
-- 20 % 20 = 0 so every col is populated by the generation rule.

ASSERT ROW_COUNT = 1
ASSERT VALUE lab_value_l01 = 20
ASSERT VALUE lab_value_l02 = 21
ASSERT VALUE diagnosis_code_i01 = 20
ASSERT VALUE clinical_note_s01 = '20'
SELECT lab_value_l01, lab_value_l02, diagnosis_code_i01, clinical_note_s01
FROM {{zone_name}}.acme.patient_records
WHERE record_id = 20;

-- ============================================================================
-- Query 16: acme.forum_posts skew distribution
-- ============================================================================
-- 500K rows. body is 100_000 chars when post_id%100=0 (5_000 rows),
-- otherwise CAST(post_id AS STRING) which is 1..6 chars for post_id in
-- [1, 500_000].

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 500000
ASSERT VALUE n_long_posts = 5000
ASSERT VALUE n_short_posts = 495000
ASSERT VALUE max_body_len = 100000
SELECT
    COUNT(*) AS n_rows,
    SUM(CASE WHEN LENGTH(body) = 100000 THEN 1 ELSE 0 END) AS n_long_posts,
    SUM(CASE WHEN LENGTH(body) <= 6     THEN 1 ELSE 0 END) AS n_short_posts,
    MAX(LENGTH(body)) AS max_body_len
FROM {{zone_name}}.acme.forum_posts;

-- ============================================================================
-- Query 17: acme.forum_posts spot-check at post_id=100 and post_id=101
-- ============================================================================
-- 100 % 100 = 0 -> 100_000-char essay (the 1% long-form case).
-- 101 % 100 = 1 -> CAST(101 AS STRING) = '101' (3 chars).

ASSERT ROW_COUNT = 2
ASSERT VALUE body_len = 100000 WHERE post_id = 100
ASSERT VALUE body_len = 3      WHERE post_id = 101
ASSERT VALUE author_len = 3    WHERE post_id = 100
ASSERT VALUE hash_len = 32     WHERE post_id = 100
SELECT
    post_id,
    LENGTH(body) AS body_len,
    LENGTH(author_handle) AS author_len,
    LENGTH(content_hash) AS hash_len
FROM {{zone_name}}.acme.forum_posts
WHERE post_id IN (100, 101)
ORDER BY post_id;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- One row per ACME table, each pinned to its closed-form row count and
-- closed-form SUM of the synthetic primary key. If any number drifts, the
-- table name in that row points to the regressing wire path.

ASSERT ROW_COUNT = 10
ASSERT RESULT SET INCLUDES
    ('market_ticks',          1000000, 500000500000),
    ('manufacturing_runs',     100000,   5000050000),
    ('support_tickets',        500000, 125000250000),
    ('product_catalog',        100000,   5000050000),
    ('knowledge_articles',      10000,     50005000),
    ('document_archive',         5000,     12502500),
    ('banking_transactions',   500000, 125000250000),
    ('shipment_orders',         50000,   1250025000),
    ('patient_records',        500000, 125000250000),
    ('forum_posts',            500000, 125000250000)
SELECT 'market_ticks'          AS tbl, COUNT(*) AS n, SUM(tick_id)        AS s FROM {{zone_name}}.acme.market_ticks
UNION ALL SELECT 'manufacturing_runs',    COUNT(*), SUM(run_id)            FROM {{zone_name}}.acme.manufacturing_runs
UNION ALL SELECT 'support_tickets',       COUNT(*), SUM(ticket_id)         FROM {{zone_name}}.acme.support_tickets
UNION ALL SELECT 'product_catalog',       COUNT(*), SUM(product_id)        FROM {{zone_name}}.acme.product_catalog
UNION ALL SELECT 'knowledge_articles',    COUNT(*), SUM(article_id)        FROM {{zone_name}}.acme.knowledge_articles
UNION ALL SELECT 'document_archive',      COUNT(*), SUM(document_id)       FROM {{zone_name}}.acme.document_archive
UNION ALL SELECT 'banking_transactions',  COUNT(*), SUM(transaction_id)    FROM {{zone_name}}.acme.banking_transactions
UNION ALL SELECT 'shipment_orders',       COUNT(*), SUM(order_id)          FROM {{zone_name}}.acme.shipment_orders
UNION ALL SELECT 'patient_records',       COUNT(*), SUM(record_id)         FROM {{zone_name}}.acme.patient_records
UNION ALL SELECT 'forum_posts',           COUNT(*), SUM(post_id)           FROM {{zone_name}}.acme.forum_posts;
