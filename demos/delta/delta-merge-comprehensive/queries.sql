-- ============================================================================
-- Delta MERGE Comprehensive — All Clause Patterns — Educational Queries
-- ============================================================================
-- WHAT: A single MERGE INTO that exercises all three clause types:
--       conditional UPDATE, conditional DELETE, and INSERT.
-- WHY:  CRM systems need atomic operations that refresh active accounts,
--       remove closed accounts, and onboard new customers without any
--       intermediate inconsistent state visible to other readers.
-- HOW:  Delta's MERGE reads the target and source, evaluates the ON
--       condition for each pair, then applies the first matching WHEN
--       clause. All changes are committed as a single transaction log
--       entry — readers see either the old state or the new state, never
--       a mix.
-- ============================================================================


-- ============================================================================
-- PREVIEW: Source Data Before MERGE
-- ============================================================================
-- The customer_updates table contains 25 staged changes:
--   12 rows with status='active' and matching IDs → will UPDATE
--    3 rows with status='closed' and matching IDs → will DELETE
--   10 rows with new IDs 41-50                    → will INSERT

ASSERT ROW_COUNT = 25
SELECT id, name, tier, balance, status,
       CASE
           WHEN id IN (2,5,8,11,14,17,20,23,26,29,32,35) THEN 'UPDATE'
           WHEN id IN (7,19,37)                           THEN 'DELETE'
           ELSE                                                'INSERT'
       END AS planned_action
FROM {{zone_name}}.delta_demos.customer_updates
ORDER BY id;


-- ============================================================================
-- MERGE: All Three Clause Patterns in One Atomic Operation
-- ============================================================================
-- WHEN MATCHED AND status='active' → UPDATE (12 rows refreshed)
-- WHEN MATCHED AND status='closed' → DELETE (3 rows removed: ids 7, 19, 37)
-- WHEN NOT MATCHED → INSERT (10 new rows: ids 41-50)
-- Final: 40 - 3 + 10 = 47 rows
-- ============================================================================

ASSERT ROW_COUNT = 25
MERGE INTO {{zone_name}}.delta_demos.customer_master AS target
USING {{zone_name}}.delta_demos.customer_updates AS source
ON target.id = source.id
WHEN MATCHED AND source.status = 'active' THEN
    UPDATE SET
        name         = source.name,
        email        = source.email,
        tier         = source.tier,
        balance      = source.balance,
        status       = source.status,
        last_contact = source.last_contact
WHEN MATCHED AND source.status = 'closed' THEN
    DELETE
WHEN NOT MATCHED THEN
    INSERT (id, name, email, tier, balance, status, last_contact)
    VALUES (source.id, source.name, source.email, source.tier,
            source.balance, source.status, source.last_contact);


-- ============================================================================
-- EXPLORE: Customer Database After MERGE
-- ============================================================================
-- The MERGE applied three operations in one atomic commit:
--   - 12 active customers updated (refreshed tier, balance, contact date)
--   - 3 closed accounts deleted (ids 7, 19, 37)
--   - 10 new customers inserted (ids 41-50)
-- Let's see the tier distribution after the merge:

ASSERT ROW_COUNT = 3
ASSERT VALUE customer_count = 18 WHERE tier = 'gold'
ASSERT VALUE customer_count = 20 WHERE tier = 'silver'
ASSERT VALUE customer_count = 9 WHERE tier = 'bronze'
ASSERT VALUE total_balance = 77550.00 WHERE tier = 'gold'
ASSERT VALUE total_balance = 28080.00 WHERE tier = 'silver'
ASSERT VALUE total_balance = 3900.00 WHERE tier = 'bronze'
SELECT tier,
       COUNT(*) AS customer_count,
       ROUND(SUM(balance), 2) AS total_balance,
       ROUND(AVG(balance), 2) AS avg_balance
FROM {{zone_name}}.delta_demos.customer_master
GROUP BY tier
ORDER BY avg_balance DESC;


-- ============================================================================
-- LEARN: WHEN MATCHED AND status='active' — Conditional UPDATE
-- ============================================================================
-- The first WHEN MATCHED clause checked source.status = 'active' to
-- decide which matched rows to update. This is important because the
-- same source table contained both active (update) and closed (delete)
-- records. Without conditional predicates, you would need separate
-- MERGE statements.
--
-- Let's look at customers who were updated — they have last_contact
-- of '2025-12-01' (the date in the source):

ASSERT ROW_COUNT = 12
SELECT id, name, tier, balance, last_contact
FROM {{zone_name}}.delta_demos.customer_master
WHERE last_contact = '2025-12-01' AND id <= 40
ORDER BY id;


-- ============================================================================
-- LEARN: WHEN MATCHED AND status='closed' — Conditional DELETE
-- ============================================================================
-- The second WHEN MATCHED clause targeted rows where the source had
-- status='closed'. These 3 customers (Grace Wilson, Sam Clark, Kara
-- Roberts) were removed from the master table entirely.
--
-- In Delta's transaction log, deletes are recorded as "remove" actions
-- pointing to the Parquet files that contained these rows. A new data
-- file is written with the remaining rows from those same files.
--
-- Let's confirm they are gone:

ASSERT ROW_COUNT = 1
ASSERT VALUE closed_accounts_remaining = 0
SELECT COUNT(*) AS closed_accounts_remaining
FROM {{zone_name}}.delta_demos.customer_master
WHERE id IN (7, 19, 37);


-- ============================================================================
-- EXPLORE: Tier Promotions from the MERGE
-- ============================================================================
-- Some customers had their tier upgraded in the source data.
-- For example, Bob Smith went from silver to gold, and Eve Davis
-- was promoted from silver to gold. Let's see the promoted customers:

ASSERT ROW_COUNT = 3
SELECT id, name, tier, balance
FROM {{zone_name}}.delta_demos.customer_master
WHERE id IN (2, 5, 11) AND tier = 'gold'
ORDER BY id;


-- ============================================================================
-- LEARN: WHEN NOT MATCHED — Inserting New Customers
-- ============================================================================
-- Source rows with no match in the target (ids 41-50) were inserted.
-- These new customers span all three tiers. In Delta, the INSERT happens
-- in the same commit as the updates and deletes — there is no moment
-- where only some changes are visible.

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 10
SELECT id, name, email, tier, balance
FROM {{zone_name}}.delta_demos.customer_master
WHERE id > 40
ORDER BY id;


-- ============================================================================
-- EXPLORE: Unchanged Customers
-- ============================================================================
-- 25 customers were not in the source table at all, so they remained
-- completely untouched. Their original last_contact dates are preserved:

ASSERT ROW_COUNT = 25
SELECT id, name, tier, balance, last_contact
FROM {{zone_name}}.delta_demos.customer_master
WHERE id NOT IN (2,5,7,8,11,14,17,19,20,23,26,29,32,35,37,41,42,43,44,45,46,47,48,49,50)
ORDER BY id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_row_count: 40 - 3 deleted + 10 inserted = 47
ASSERT ROW_COUNT = 47
SELECT * FROM {{zone_name}}.delta_demos.customer_master;

-- Verify closed_accounts_gone: 3 closed accounts (ids 7, 19, 37) deleted
ASSERT VALUE cnt = 0
SELECT COUNT(*) FILTER (WHERE id IN (7, 19, 37)) AS cnt FROM {{zone_name}}.delta_demos.customer_master;

-- Verify updated_email: Bob Smith email updated
ASSERT VALUE cnt = 1
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.customer_master WHERE id = 2 AND email = 'bob.smith@newmail.com';

-- Verify new_customers: 10 new customers inserted (ids > 40)
ASSERT VALUE cnt = 10
SELECT COUNT(*) FILTER (WHERE id > 40) AS cnt FROM {{zone_name}}.delta_demos.customer_master;

-- Verify updated_tier: Eve Davis promoted to gold
ASSERT VALUE cnt = 1
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.customer_master WHERE id = 5 AND tier = 'gold';

-- Verify updated_balance: id=8 balance updated to 2850.00
ASSERT VALUE cnt = 1
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.customer_master WHERE id = 8 AND balance = 2850.00;

-- Verify unchanged_count: 25 customers not in source remain untouched
ASSERT VALUE cnt = 25
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.customer_master WHERE id NOT IN (2,5,7,8,11,14,17,19,20,23,26,29,32,35,37,41,42,43,44,45,46,47,48,49,50);

-- Verify status_active_count: all 47 remaining customers are active
ASSERT VALUE cnt = 47
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.customer_master WHERE status = 'active';
