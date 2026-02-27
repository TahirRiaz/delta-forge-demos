-- ============================================================================
-- Protobuf Address Book Contacts — Verification Queries
-- ============================================================================
-- Each query verifies a specific protobuf feature: nested message flattening,
-- repeated field handling, enum decoding, timestamp conversion, sparse data,
-- multi-file reading, and file metadata.
-- ============================================================================


-- ============================================================================
-- 1. TOTAL CONTACT COUNT — 5 + 5 + 3 = 13 people across 3 team files
-- ============================================================================

SELECT 'contact_count' AS check_name,
       COUNT(*) AS actual,
       13 AS expected,
       CASE WHEN COUNT(*) = 13 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.protobuf.contacts;


-- ============================================================================
-- 2. BROWSE CONTACTS — See flattened data with friendly column names
-- ============================================================================

SELECT contact_id, contact_name, email, phone_numbers, phone_types, last_updated
FROM {{zone_name}}.protobuf.contacts
ORDER BY contact_id;


-- ============================================================================
-- 3. EXPLODED PHONE COUNT — 9 + 9 + 4 = 22 phone number rows
-- ============================================================================
-- Each PhoneNumber within each Person becomes its own row.

SELECT 'phone_rows' AS check_name,
       COUNT(*) AS actual,
       22 AS expected,
       CASE WHEN COUNT(*) = 22 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.protobuf.contact_phones;


-- ============================================================================
-- 4. BROWSE CONTACT PHONES — Exploded view with one row per phone
-- ============================================================================

SELECT contact_id, contact_name, phone_number, phone_type
FROM {{zone_name}}.protobuf.contact_phones
ORDER BY contact_id, phone_type;


-- ============================================================================
-- 5. ENUM DECODING — PhoneType decoded to string labels
-- ============================================================================
-- PhoneType enum: MOBILE=0, HOME=1, WORK=2 should appear as string labels.

SELECT 'enum_decoding' AS check_name,
       COUNT(DISTINCT phone_type) AS distinct_types,
       CASE WHEN COUNT(DISTINCT phone_type) = 3 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.protobuf.contact_phones;


-- ============================================================================
-- 6. PHONE TYPE DISTRIBUTION — Count by MOBILE, HOME, WORK
-- ============================================================================

SELECT phone_type,
       COUNT(*) AS count
FROM {{zone_name}}.protobuf.contact_phones
GROUP BY phone_type
ORDER BY count DESC;


-- ============================================================================
-- 7. REPEATED FIELD JOIN — phone_numbers column has comma-joined values
-- ============================================================================
-- Contacts with multiple phones should have commas in phone_numbers column.

SELECT 'repeated_join' AS check_name,
       COUNT(*) FILTER (WHERE phone_numbers LIKE '%,%') AS multi_phone_contacts,
       CASE WHEN COUNT(*) FILTER (WHERE phone_numbers LIKE '%,%') >= 7
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.protobuf.contacts;


-- ============================================================================
-- 8. SPARSE DATA — Luis Hernandez has no phone numbers (empty repeated)
-- ============================================================================
-- Luis (id=3002) has an empty phones list. In contacts table, phone_numbers
-- should be NULL or empty. In contact_phones, he should have zero rows.

SELECT 'sparse_no_phones' AS check_name,
       CASE WHEN (
           SELECT COUNT(*) FROM {{zone_name}}.protobuf.contact_phones
           WHERE contact_id = 3002
       ) = 0 THEN 'PASS' ELSE 'FAIL' END AS result;


-- ============================================================================
-- 9. SPARSE DATA — Maria Schmidt has no email (empty string field)
-- ============================================================================

SELECT 'sparse_no_email' AS check_name,
       CASE WHEN (
           SELECT COUNT(*) FROM {{zone_name}}.protobuf.contacts
           WHERE contact_id = 3003
             AND (email IS NULL OR email = '')
       ) = 1 THEN 'PASS' ELSE 'FAIL' END AS result;


-- ============================================================================
-- 10. TIMESTAMP CONVERSION — last_updated should be valid ISO 8601 dates
-- ============================================================================
-- All 13 contacts have last_updated set; values should be non-NULL strings
-- starting with a year (e.g., "2024-" or "2025-").

SELECT 'timestamp_populated' AS check_name,
       COUNT(*) FILTER (WHERE last_updated IS NOT NULL) AS actual,
       13 AS expected,
       CASE WHEN COUNT(*) FILTER (WHERE last_updated IS NOT NULL) = 13
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.protobuf.contacts;


-- ============================================================================
-- 11. MULTI-FILE READING — 3 distinct source files
-- ============================================================================

SELECT 'three_source_files' AS check_name,
       COUNT(DISTINCT df_file_name) AS actual,
       3 AS expected,
       CASE WHEN COUNT(DISTINCT df_file_name) = 3 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.protobuf.contacts;


-- ============================================================================
-- 12. CONTACTS PER FILE — verify team distribution
-- ============================================================================

SELECT df_file_name, COUNT(*) AS contact_count
FROM {{zone_name}}.protobuf.contacts
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 13. PHONES PER CONTACT — verify repeated field cardinality
-- ============================================================================

SELECT contact_name, COUNT(*) AS phone_count
FROM {{zone_name}}.protobuf.contact_phones
GROUP BY contact_name
ORDER BY phone_count DESC, contact_name;


-- ============================================================================
-- 14. SUMMARY — All checks in one query
-- ============================================================================

SELECT check_name, result FROM (

    -- Check 1: Total contacts = 13
    SELECT 'contact_count_13' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.protobuf.contacts) = 13
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: Exploded phone rows = 22
    SELECT 'phone_rows_22' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.protobuf.contact_phones) = 22
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: 3 distinct phone types (enum decoding)
    SELECT 'enum_three_types' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT phone_type) FROM {{zone_name}}.protobuf.contact_phones) = 3
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: Multi-phone contacts have commas (repeated join)
    SELECT 'repeated_join_commas' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.protobuf.contacts
               WHERE phone_numbers LIKE '%,%'
           ) >= 7 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: Sparse — Luis has no phone rows
    SELECT 'sparse_no_phones_luis' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.protobuf.contact_phones
               WHERE contact_id = 3002
           ) = 0 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: Sparse — Maria has no email
    SELECT 'sparse_no_email_maria' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.protobuf.contacts
               WHERE contact_id = 3003 AND (email IS NULL OR email = '')
           ) = 1 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 7: Timestamps populated for all contacts
    SELECT 'timestamps_populated' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.protobuf.contacts
               WHERE last_updated IS NOT NULL
           ) = 13 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 8: 3 source files
    SELECT 'three_source_files' AS check_name,
           CASE WHEN (
               SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.protobuf.contacts
           ) = 3 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 9: File metadata populated for all rows
    SELECT 'file_metadata_populated' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.protobuf.contacts
               WHERE df_file_name IS NOT NULL
           ) = 13 THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
