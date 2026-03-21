-- ============================================================================
-- Delta MERGE — Upsert, Conditional Update & Delete — Educational Queries
-- ============================================================================
-- WHAT: MERGE INTO with WHEN MATCHED, WHEN NOT MATCHED, and the powerful
--       WHEN NOT MATCHED BY SOURCE clause for full CDC-style upserts.
-- WHY:  In CDC pipelines, you need to handle three cases: existing records
--       that changed, new records to insert, and stale records in the
--       target that are no longer present in the source. The BY SOURCE
--       clause handles this third case without a separate DELETE statement.
-- HOW:  Delta scans both target and source, joins on the ON condition,
--       then evaluates clauses for matched rows AND for target rows with
--       no source match. The entire result is committed atomically.
-- ============================================================================


-- ============================================================================
-- PREVIEW: Target vs Source Before MERGE
-- ============================================================================
-- Let's compare the two tables before merging. The source has 10 updates
-- (ids 1-10) with increased spending, and 5 new customers (ids 21-25).
-- Target customers 11-20 are NOT in the source at all.

ASSERT ROW_COUNT = 20
SELECT 'target' AS table_role, id, name, tier, total_spent
FROM {{zone_name}}.delta_demos.customers
ORDER BY id;

ASSERT ROW_COUNT = 15
SELECT 'source' AS table_role, id, name, tier, total_spent
FROM {{zone_name}}.delta_demos.customer_updates
ORDER BY id;


-- ============================================================================
-- MERGE: Upsert with Conditional Tier Promotion
-- ============================================================================
-- This single MERGE statement handles all three CDC cases atomically:
--
--   WHEN MATCHED → Update total_spent and recalculate tier using CASE:
--       total_spent >= 3000 → gold
--       total_spent >= 1000 → silver
--       otherwise           → bronze
--
--   WHEN NOT MATCHED → Insert new customers (ids 21-25), also with
--       tier calculated via the same CASE logic at insert time.
--
--   WHEN NOT MATCHED BY SOURCE → Delete only bronze-tier target rows
--       that have no corresponding source row. This protects silver
--       and gold customers from accidental removal.
--       Candidates: ids 11-20 not in source
--       Bronze among 11-20: id=14 (350), id=17 (400), id=18 (180), id=20 (220)
--       → 4 bronze customers deleted

ASSERT ROW_COUNT = 19
MERGE INTO {{zone_name}}.delta_demos.customers AS target
USING {{zone_name}}.delta_demos.customer_updates AS source
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET
        total_spent = source.total_spent,
        tier = CASE
            WHEN source.total_spent >= 3000 THEN 'gold'
            WHEN source.total_spent >= 1000 THEN 'silver'
            ELSE 'bronze'
        END
WHEN NOT MATCHED THEN
    INSERT (id, name, email, city, tier, total_spent)
    VALUES (source.id, source.name, source.email, source.city,
            CASE
                WHEN source.total_spent >= 3000 THEN 'gold'
                WHEN source.total_spent >= 1000 THEN 'silver'
                ELSE 'bronze'
            END,
            source.total_spent)
WHEN NOT MATCHED BY SOURCE AND target.tier = 'bronze' THEN
    DELETE;


-- ============================================================================
-- EXPLORE: Customer Table After Upsert MERGE
-- ============================================================================
-- The MERGE performed three operations atomically:
--   - Updated 10 existing customers (ids 1-10) with new spending totals
--   - Inserted 5 new customers (ids 21-25)
--   - Deleted 4 stale bronze customers not in the source (ids 14,17,18,20)
--
-- Let's see the tier distribution after the merge:

ASSERT ROW_COUNT = 3
ASSERT VALUE customer_count = 6 WHERE tier = 'gold'
ASSERT VALUE customer_count = 9 WHERE tier = 'silver'
ASSERT VALUE customer_count = 6 WHERE tier = 'bronze'
ASSERT VALUE tier_total_spent = 23300.00 WHERE tier = 'gold'
ASSERT VALUE tier_total_spent = 13000.00 WHERE tier = 'silver'
ASSERT VALUE tier_total_spent = 3225.00 WHERE tier = 'bronze'
SELECT tier,
       COUNT(*) AS customer_count,
       ROUND(SUM(total_spent), 2) AS tier_total_spent,
       ROUND(AVG(total_spent), 2) AS avg_spent
FROM {{zone_name}}.delta_demos.customers
GROUP BY tier
ORDER BY avg_spent DESC;


-- ============================================================================
-- LEARN: WHEN MATCHED — Conditional Tier Promotion
-- ============================================================================
-- The MERGE did not just copy source values blindly. It recalculated
-- the tier based on the new total_spent using CASE expressions:
--   >= $3,000 -> gold
--   >= $1,000 -> silver
--   otherwise -> bronze
--
-- This pattern pushes business logic into the MERGE itself, ensuring
-- tier consistency. Let's see the updated customers and their new tiers:

ASSERT ROW_COUNT = 10
ASSERT VALUE new_tier = 'gold' WHERE id = 1
ASSERT VALUE new_tier = 'bronze' WHERE id = 4
SELECT id, name, total_spent,
       tier AS new_tier,
       CASE
           WHEN total_spent >= 3000 THEN 'gold'
           WHEN total_spent >= 1000 THEN 'silver'
           ELSE 'bronze'
       END AS expected_tier,
       city
FROM {{zone_name}}.delta_demos.customers
WHERE id BETWEEN 1 AND 10
ORDER BY total_spent DESC;


-- ============================================================================
-- LEARN: WHEN NOT MATCHED BY SOURCE — Stale Record Cleanup
-- ============================================================================
-- This is the most distinctive clause in this MERGE. It fires for
-- target rows that have NO corresponding source row. The additional
-- predicate AND target.tier = 'bronze' limits the delete to only
-- low-value customers, protecting silver and gold customers from
-- accidental removal.
--
-- Customers 11-20 were NOT in the source. Of those, bronze-tier
-- customers (ids 14, 17, 18, 20) were deleted. Silver/gold customers
-- (ids 11, 12, 13, 15, 16, 19) survived:

ASSERT ROW_COUNT = 6
SELECT id, name, tier, total_spent
FROM {{zone_name}}.delta_demos.customers
WHERE id BETWEEN 11 AND 20
ORDER BY id;


-- ============================================================================
-- EXPLORE: New Customers Inserted with Tier Calculation
-- ============================================================================
-- The 5 new customers (ids 21-25) were inserted with tier calculated
-- at INSERT time. Notice that Wendy Hall (id=23) has $2,600 in spending
-- which makes her silver (not gold), since the threshold is $3,000:

ASSERT ROW_COUNT = 5
ASSERT VALUE tier = 'silver' WHERE id = 23
ASSERT VALUE total_spent = 2600.00 WHERE id = 23
ASSERT VALUE tier = 'bronze' WHERE id = 22
SELECT id, name, city, tier, total_spent
FROM {{zone_name}}.delta_demos.customers
WHERE id BETWEEN 21 AND 25
ORDER BY id;


-- ============================================================================
-- EXPLORE: Surviving Non-Source Customers
-- ============================================================================
-- The WHEN NOT MATCHED BY SOURCE clause only deleted bronze-tier
-- customers. This is a safety pattern — you protect high-value
-- records from being removed just because they were not in the
-- latest source batch:

ASSERT VALUE tier = 'silver' WHERE id = 11
ASSERT VALUE tier = 'gold' WHERE id = 12
SELECT id, name, tier, total_spent
FROM {{zone_name}}.delta_demos.customers
WHERE id IN (11, 12, 13, 15, 16, 19)
ORDER BY tier, total_spent DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify final_row_count: 20 - 4 bronze deleted + 5 inserted = 21
ASSERT ROW_COUNT = 21
SELECT * FROM {{zone_name}}.delta_demos.customers;

-- Verify deleted_bronze_gone: 4 stale bronze customers removed by BY SOURCE
ASSERT VALUE cnt = 0
SELECT COUNT(*) FILTER (WHERE id IN (14, 17, 18, 20)) AS cnt FROM {{zone_name}}.delta_demos.customers;

-- Verify new_customers_inserted: 5 new customers (ids 21-25) inserted
ASSERT VALUE cnt = 5
SELECT COUNT(*) FILTER (WHERE id BETWEEN 21 AND 25) AS cnt FROM {{zone_name}}.delta_demos.customers;

-- Verify alice_updated: Alice promoted to gold with 3200 spending
ASSERT VALUE cnt = 1
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.customers WHERE id = 1 AND total_spent = 3200.00 AND tier = 'gold';

-- Verify bob_tier: Bob at silver tier with 1800 spending
ASSERT VALUE cnt = 1
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.customers WHERE id = 2 AND tier = 'silver' AND total_spent = 1800.00;

-- Verify henry_spending_updated: Henry total_spent updated to 2200
ASSERT VALUE total_spent = 2200.00
SELECT total_spent FROM {{zone_name}}.delta_demos.customers WHERE id = 8;

-- Verify karen_unchanged: Karen not in source, survived as silver
ASSERT VALUE cnt = 1
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.customers WHERE id = 11 AND total_spent = 900.00 AND tier = 'silver';

-- Verify gold_count: 6 gold-tier customers total
ASSERT VALUE cnt = 6
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.customers WHERE tier = 'gold';

-- Verify wendy_tier: Wendy inserted as silver with 2600 spending
ASSERT VALUE cnt = 1
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.customers WHERE id = 23 AND tier = 'silver' AND total_spent = 2600.00;

-- Verify non_bronze_survived: 6 non-bronze customers not in source survived
ASSERT VALUE cnt = 6
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.customers WHERE id IN (11, 12, 13, 15, 16, 19);
