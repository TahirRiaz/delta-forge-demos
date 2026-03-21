-- ============================================================================
-- Delta Schema Evolution — Drop Columns & GDPR Cleanup — Educational Queries
-- ============================================================================
-- WHAT: Delta schema evolution allows removing or NULLing column data to
--       comply with data privacy regulations like GDPR.
-- WHY:  GDPR's "right to erasure" (Article 17) requires organizations to
--       delete personal data on request. Delta's transactional model ensures
--       erasure is atomic, auditable, and reversible via time travel.
-- HOW:  Rather than dropping columns (which affects all rows), Delta UPDATEs
--       SET PII columns to NULL for specific users. Views then project only
--       non-PII columns, giving downstream consumers a clean interface.
-- ============================================================================


-- ============================================================================
-- EXPLORE: The Full User Profiles Table (Before Erasure)
-- ============================================================================
-- The table has 10 columns including PII fields: phone and address.
-- All 40 users currently have full PII data. Let's see a sample:

ASSERT ROW_COUNT = 4
SELECT id, username, email, phone, address, city, country
FROM {{zone_name}}.delta_demos.user_profiles
WHERE id IN (1, 2, 16, 17)
ORDER BY id;


-- ============================================================================
-- LEARN: GDPR Erasure Patterns in Delta
-- ============================================================================
-- Delta supports two approaches to GDPR compliance:
--
-- 1. NULL-out approach (used here): SET sensitive columns to NULL for
--    targeted rows. The column remains in the schema, but data is gone.
--    Pros: Simple, selective, preserves table structure.
--
-- 2. Column drop approach: ALTER TABLE DROP COLUMN removes the column
--    entirely. Pros: Guarantees no residual data. Cons: Affects all rows.
--
-- Important: Both approaches create a new version. The old version still
-- contains the PII data until VACUUM removes old Parquet files. For true
-- GDPR compliance, you must also run VACUUM to physically delete old files.


-- ============================================================================
-- STEP 1: GDPR ERASURE — NULL out phone for users 1-15 (erasure request)
-- ============================================================================
-- Users 1-15 have submitted a GDPR "right to erasure" request.
-- First, we NULL out the phone column for these users.

ASSERT ROW_COUNT = 15
UPDATE {{zone_name}}.delta_demos.user_profiles
SET phone = NULL
WHERE id BETWEEN 1 AND 15;


-- ============================================================================
-- STEP 2: GDPR ERASURE — NULL out address for users 1-15
-- ============================================================================
-- Next, we NULL out the address column for the same users.

ASSERT ROW_COUNT = 15
UPDATE {{zone_name}}.delta_demos.user_profiles
SET address = NULL
WHERE id BETWEEN 1 AND 15;


-- ============================================================================
-- EXPLORE: The Table After Erasure — Before/After Contrast
-- ============================================================================
-- Users 1-15 now have NULL phone and address. Users 16-40 are untouched.
-- Compare erased users (1, 2) with intact users (16, 17):

ASSERT ROW_COUNT = 4
SELECT id, username, email,
       CASE WHEN phone IS NULL THEN '** ERASED **' ELSE phone END AS phone,
       CASE WHEN address IS NULL THEN '** ERASED **' ELSE address END AS address,
       city, country
FROM {{zone_name}}.delta_demos.user_profiles
WHERE id IN (1, 2, 16, 17)
ORDER BY id;


-- ============================================================================
-- LEARN: Erasure Summary — How Many Users Were Affected?
-- ============================================================================
-- 40 total users, 15 had phone erased, 15 had address erased, 25 fully intact

ASSERT VALUE total_users = 40
ASSERT VALUE erased_phone = 15
ASSERT VALUE erased_address = 15
ASSERT VALUE fully_intact = 25
ASSERT ROW_COUNT = 1
SELECT
    COUNT(*) AS total_users,
    COUNT(*) FILTER (WHERE phone IS NULL) AS erased_phone,
    COUNT(*) FILTER (WHERE address IS NULL) AS erased_address,
    COUNT(*) FILTER (WHERE phone IS NOT NULL AND address IS NOT NULL) AS fully_intact
FROM {{zone_name}}.delta_demos.user_profiles;


-- ============================================================================
-- STEP 3: CREATE VIEW — Clean Projection Without PII Columns
-- ============================================================================
-- A view that excludes PII columns gives downstream consumers a safe
-- interface. Even if the underlying table still has the columns, the view
-- ensures analysts never accidentally access personal data.

CREATE VIEW {{zone_name}}.delta_demos.user_profiles_clean AS
SELECT id, username, email, city, country, signup_date, last_login, preferences
FROM {{zone_name}}.delta_demos.user_profiles;


-- ============================================================================
-- EXPLORE: Query the Clean View
-- ============================================================================
-- The user_profiles_clean view projects: id, username, email, city, country,
-- signup_date, last_login, preferences (no phone, no address).

ASSERT ROW_COUNT = 5
SELECT id, username, email, city, country, signup_date
FROM {{zone_name}}.delta_demos.user_profiles_clean
WHERE id <= 5
ORDER BY id;


-- ============================================================================
-- EXPLORE: Geographic Distribution (Non-PII Analytics Still Work)
-- ============================================================================
-- After GDPR erasure, non-PII columns remain fully intact. This means
-- analytics on city, country, signup patterns, and preferences are
-- unaffected. The data still has full analytical value.

ASSERT ROW_COUNT = 19
SELECT country, COUNT(*) AS user_count
FROM {{zone_name}}.delta_demos.user_profiles
GROUP BY country
ORDER BY user_count DESC;


-- ============================================================================
-- LEARN: Verifying Erasure Completeness
-- ============================================================================
-- A compliance officer needs to verify that erasure was thorough.
-- For users 1-15, BOTH phone AND address must be NULL.
-- For users 16-40, data must remain untouched.

ASSERT ROW_COUNT = 2
ASSERT VALUE has_phone = 0 WHERE user_group = 'GDPR-erased (ids 1-15)'
ASSERT VALUE has_address = 0 WHERE user_group = 'GDPR-erased (ids 1-15)'
ASSERT VALUE has_phone = 25 WHERE user_group = 'Intact (ids 16-40)'
SELECT
    CASE
        WHEN id BETWEEN 1 AND 15 THEN 'GDPR-erased (ids 1-15)'
        ELSE 'Intact (ids 16-40)'
    END AS user_group,
    COUNT(*) AS users,
    COUNT(phone) AS has_phone,
    COUNT(address) AS has_address
FROM {{zone_name}}.delta_demos.user_profiles
GROUP BY CASE WHEN id BETWEEN 1 AND 15 THEN 'GDPR-erased (ids 1-15)' ELSE 'Intact (ids 16-40)' END
ORDER BY user_group;


-- ============================================================================
-- EXPLORE: Non-PII Data Preserved After Erasure
-- ============================================================================
-- Critically, GDPR erasure only removes PII. The user's non-sensitive data
-- (username, email, city, preferences) remains fully intact.

ASSERT VALUE username = 'alice_dev'
ASSERT ROW_COUNT = 1
SELECT id, username, email, city, country, preferences
FROM {{zone_name}}.delta_demos.user_profiles
WHERE id = 1;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count is 40
ASSERT ROW_COUNT = 40
SELECT * FROM {{zone_name}}.delta_demos.user_profiles;

-- Verify 15 phones were NULLed (GDPR erasure)
ASSERT VALUE null_phone_count = 15
SELECT COUNT(*) AS null_phone_count FROM {{zone_name}}.delta_demos.user_profiles WHERE phone IS NULL;

-- Verify 15 addresses were NULLed (GDPR erasure)
ASSERT VALUE null_address_count = 15
SELECT COUNT(*) AS null_address_count FROM {{zone_name}}.delta_demos.user_profiles WHERE address IS NULL;

-- Verify non-GDPR user (id=16) phone is intact
ASSERT VALUE phone = '+1-555-0116'
SELECT phone FROM {{zone_name}}.delta_demos.user_profiles WHERE id = 16;

-- Verify GDPR-erased user (id=1) phone is NULL
ASSERT VALUE phone IS NULL
SELECT phone FROM {{zone_name}}.delta_demos.user_profiles WHERE id = 1;

-- Verify clean view has all 40 rows
ASSERT VALUE view_row_count = 40
SELECT COUNT(*) AS view_row_count FROM {{zone_name}}.delta_demos.user_profiles_clean;

-- Verify non-PII data preserved after erasure
ASSERT VALUE username = 'alice_dev'
SELECT username FROM {{zone_name}}.delta_demos.user_profiles WHERE id = 1;

-- Verify 19 distinct countries exist
ASSERT VALUE distinct_countries = 19
SELECT COUNT(DISTINCT country) AS distinct_countries FROM {{zone_name}}.delta_demos.user_profiles;
