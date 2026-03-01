-- ============================================================================
-- JSON Customers Basics — Verification Queries
-- ============================================================================
-- Each query verifies that JSON fundamentals work correctly: array parsing,
-- column mappings, type inference, and file metadata.
-- ============================================================================


-- ============================================================================
-- 1. TOTAL CUSTOMER COUNT — Single JSON array file should produce 200 rows
-- ============================================================================

SELECT 'total_customers' AS check_name,
       COUNT(*) AS actual,
       200 AS expected,
       CASE WHEN COUNT(*) = 200 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.json.customers;


-- ============================================================================
-- 2. BROWSE CUSTOMERS — See the mapped column names
-- ============================================================================
-- Verify that column_mappings applied:
--   $.first → first_name, $.last → last_name, $.created_at → signup_date

SELECT id, email, first_name, last_name, company, signup_date, country
FROM {{zone_name}}.json.customers
ORDER BY id
LIMIT 10;


-- ============================================================================
-- 3. COLUMN MAPPING VERIFICATION — Mapped names exist, originals do not
-- ============================================================================
-- first_name should have non-NULL values (mapped from $.first).
-- If the mapping failed, the column would be named "first" instead.

SELECT 'column_mappings' AS check_name,
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.json.customers
WHERE first_name IS NOT NULL AND last_name IS NOT NULL;


-- ============================================================================
-- 4. COUNTRY DISTRIBUTION — Top 10 countries by customer count
-- ============================================================================

SELECT country, COUNT(*) AS customer_count
FROM {{zone_name}}.json.customers
GROUP BY country
ORDER BY customer_count DESC
LIMIT 10;


-- ============================================================================
-- 5. SIGNUP DATE RANGE — Verify timestamps parsed correctly
-- ============================================================================
-- All records have created_at values from 2014–2015. With infer_types enabled,
-- signup_date should be a proper timestamp enabling MIN/MAX.

SELECT MIN(signup_date) AS earliest_signup,
       MAX(signup_date) AS latest_signup
FROM {{zone_name}}.json.customers;


-- ============================================================================
-- 6. FILE METADATA — df_file_name and df_row_number populated
-- ============================================================================
-- file_metadata injects source file info alongside flattened columns.

SELECT df_file_name, MIN(df_row_number) AS first_row, MAX(df_row_number) AS last_row
FROM {{zone_name}}.json.customers
GROUP BY df_file_name;


-- ============================================================================
-- 7. NO NULLS IN REQUIRED FIELDS — All 200 rows fully populated
-- ============================================================================
-- Every customer should have id, email, first_name, last_name, and country.

SELECT 'no_null_required' AS check_name,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.json.customers
WHERE id IS NULL
   OR email IS NULL
   OR first_name IS NULL
   OR last_name IS NULL
   OR country IS NULL;


-- ============================================================================
-- 8. SPOT CHECK — Verify first customer data
-- ============================================================================
-- First record: id=1, Torrey Veum, Switzerland

SELECT id, first_name, last_name, company, country
FROM {{zone_name}}.json.customers
WHERE id = 1;


-- ============================================================================
-- 9. COMPANY ANALYTICS — Customers per company (top 5)
-- ============================================================================

SELECT company, COUNT(*) AS employee_count
FROM {{zone_name}}.json.customers
GROUP BY company
ORDER BY employee_count DESC
LIMIT 5;


-- ============================================================================
-- 10. SUMMARY — All checks in one query
-- ============================================================================

SELECT check_name, result FROM (

    -- Check 1: Row count = 200
    SELECT 'row_count_200' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.customers) = 200
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: Column mappings applied (first_name exists with data)
    SELECT 'column_mapping_first_name' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.customers WHERE first_name IS NOT NULL) = 200
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: Column mappings applied (last_name exists with data)
    SELECT 'column_mapping_last_name' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.customers WHERE last_name IS NOT NULL) = 200
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: Column mappings applied (signup_date exists with data)
    SELECT 'column_mapping_signup_date' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.customers WHERE signup_date IS NOT NULL) = 200
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: No NULL IDs
    SELECT 'no_null_ids' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.customers WHERE id IS NULL) = 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: File metadata populated
    SELECT 'file_metadata_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.customers WHERE df_file_name IS NOT NULL) = 200
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 7: Spot check — customer 1 is Torrey Veum from Switzerland
    SELECT 'spot_check_customer_1' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.json.customers
               WHERE id = 1 AND first_name = 'Torrey' AND last_name = 'Veum' AND country = 'Switzerland'
           ) = 1 THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
