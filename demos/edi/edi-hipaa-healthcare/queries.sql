-- ============================================================================
-- EDI HIPAA Healthcare — Demo Queries
-- ============================================================================
-- Queries showcasing how Delta Forge ingests HIPAA X12 healthcare
-- transactions across the complete claims lifecycle into queryable tables.
--
-- Two tables are available:
--   hipaa_messages      — Compact view: ISA/GS/ST headers + full JSON
--   hipaa_materialized  — Enriched view: headers + BHT/NM1/CLM/BPR columns
--
-- Column reference (always available — X12 default columns):
--   ISA_1 through ISA_16 = Interchange Control Header fields
--   ISA_6  = Interchange Sender ID
--   ISA_8  = Interchange Receiver ID
--   ISA_12 = Interchange Control Version (00501 for all HIPAA 5010)
--   GS_1   = Functional Identifier Code (HC=Health Care, BE=Benefit Enrollment)
--   GS_8   = Implementation Guide Version (e.g. 005010X222A1)
--   ST_1   = Transaction Set Identifier Code (270, 271, 276, 277, 278, 820, 834, 835, 837)
--   ST_2   = Transaction Set Control Number
--   df_transaction_json = Full transaction as JSON
--   df_transaction_id   = Unique transaction hash
--
-- Materialized columns (hipaa_materialized table only):
--   BHT_1  = Hierarchical structure code
--   BHT_2  = Transaction set purpose code
--   BHT_6  = Transaction type code
--   NM1_1  = Entity identifier code
--   NM1_2  = Entity type qualifier (1=person, 2=non-person)
--   NM1_3  = Name last or organization name
--   CLM_1  = Claim submitter identifier (patient account number)
--   CLM_2  = Total claim charge amount
--   BPR_1  = Transaction handling code
--   BPR_2  = Total payment amount
-- ============================================================================


-- ============================================================================
-- 1. All HIPAA Transactions — Header Overview
-- ============================================================================
-- This query reads from the compact table (hipaa_messages) to show the
-- ISA/GS/ST header of every HIPAA transaction. Each row is one X12
-- transaction set from one .edi file.
--
-- What you'll see:
--   - df_file_name:  The source .edi file this row came from
--   - sender_id:     ISA-06 interchange sender identifier
--   - transaction_type: ST-01 transaction set ID (270, 271, 837, etc.)
--   - impl_guide:    GS-08 implementation guide version (identifies the HIPAA standard)
--   - edi_version:   ISA-12 interchange control version (00501 = HIPAA 5010)
--
-- Expected: 11 rows — one per .edi file

SELECT
    df_file_name,
    "ISA_6" AS sender_id,
    "ST_1" AS transaction_type,
    "GS_8" AS impl_guide,
    "ISA_12" AS edi_version
FROM {{zone_name}}.edi.hipaa_messages
ORDER BY df_file_name;


-- ============================================================================
-- 2. Transaction Type Distribution
-- ============================================================================
-- Groups transactions by their ST_1 (Transaction Set Identifier Code) to
-- show the mix of HIPAA transaction types ingested.
--
-- What you'll see:
--   - transaction_type: The ST-01 code identifying the transaction kind
--   - type_name:        Human-readable name for the transaction type
--   - transaction_count: How many transactions of that type
--
-- Expected: 9 distinct types (270, 271, 276, 277, 278, 820, 834, 835, 837)
-- Note: ST_1 values come from the file content, not filenames. The 837 type
-- appears 3 times (professional, dental, institutional claims).

SELECT
    "ST_1" AS transaction_type,
    CASE "ST_1"
        WHEN '270' THEN 'Eligibility Inquiry'
        WHEN '271' THEN 'Eligibility Response'
        WHEN '276' THEN 'Claim Status Request'
        WHEN '277' THEN 'Claim Status Response'
        WHEN '278' THEN 'Health Services Review'
        WHEN '820' THEN 'Payment Order'
        WHEN '834' THEN 'Benefit Enrollment'
        WHEN '835' THEN 'Claim Payment/Remittance'
        WHEN '837' THEN 'Healthcare Claim'
        ELSE 'Unknown'
    END AS type_name,
    COUNT(*) AS transaction_count
FROM {{zone_name}}.edi.hipaa_messages
GROUP BY "ST_1"
ORDER BY "ST_1";


-- ============================================================================
-- 3. Implementation Guide Versions
-- ============================================================================
-- Groups by GS_8 (Implementation Guide Version) to show which HIPAA
-- implementation guides are represented. Each guide specifies the exact
-- segment/element structure for a transaction type.
--
-- What you'll see:
--   - impl_guide:        GS-08 version string (e.g. "005010X222A1")
--   - transaction_count: How many transactions use that guide
--
-- Expected: 9 distinct guides — each HIPAA transaction type has its own
-- implementation guide version:
--   005010X279A1 (270/271 eligibility)     = 2 transactions
--   005010X212   (276/277 claim status)    = 2 transactions
--   005010X217   (278 services review)     = 1 transaction
--   005010X218   (820 payment order)       = 1 transaction
--   005010X220A1 (834 enrollment)          = 1 transaction
--   005010X221A1 (835 payment/remittance)  = 1 transaction
--   005010X222A1 (837P professional claim) = 1 transaction
--   005010X223A2 (837I institutional)      = 1 transaction
--   005010X224A2 (837D dental)             = 1 transaction

SELECT
    "GS_8" AS impl_guide,
    COUNT(*) AS transaction_count
FROM {{zone_name}}.edi.hipaa_messages
GROUP BY "GS_8"
ORDER BY "GS_8";


-- ============================================================================
-- 4. HIPAA Transaction Categories
-- ============================================================================
-- Classifies each transaction into a business category using CASE WHEN on
-- ST_1. This shows how a clearinghouse might categorize incoming traffic.
--
-- What you'll see:
--   - category:          Business category (Eligibility, Claim Status, etc.)
--   - transaction_count: How many transactions in that category
--
-- Expected categories:
--   Eligibility    (270, 271)     = 2
--   Claim Status   (276, 277)     = 2
--   Claims         (837)          = 3
--   Payment        (835, 820)     = 2
--   Enrollment     (834)          = 1
--   Authorization  (278)          = 1

SELECT
    CASE
        WHEN "ST_1" IN ('270', '271') THEN 'Eligibility'
        WHEN "ST_1" IN ('276', '277') THEN 'Claim Status'
        WHEN "ST_1" = '837'           THEN 'Claims'
        WHEN "ST_1" IN ('835', '820') THEN 'Payment'
        WHEN "ST_1" = '834'           THEN 'Enrollment'
        WHEN "ST_1" = '278'           THEN 'Authorization'
        ELSE 'Other'
    END AS category,
    COUNT(*) AS transaction_count
FROM {{zone_name}}.edi.hipaa_messages
GROUP BY category
ORDER BY transaction_count DESC;


-- ============================================================================
-- 5. Claim Details — Materialized View
-- ============================================================================
-- Reads from hipaa_materialized to show claim-specific fields that have
-- been extracted as first-class columns. Filters to rows where CLM_1
-- (claim submitter ID) is populated — these are the 837 claim transactions.
--
-- What you'll see:
--   - df_file_name:   Source file
--   - claim_id:       CLM-01 patient account / claim tracking number
--   - claim_amount:   CLM-02 total charge amount
--   - patient_name:   NM1-03 last name (from the first NM1 segment)
--   - transaction_type: ST-01 (should be 837 for all claim rows)
--
-- Expected: Rows from the 837 files (professional, dental, institutional)
-- where CLM_1 is populated with a claim ID

SELECT
    df_file_name,
    "CLM_1" AS claim_id,
    "CLM_2" AS claim_amount,
    "NM1_3" AS patient_name,
    "ST_1" AS transaction_type
FROM {{zone_name}}.edi.hipaa_materialized
WHERE "CLM_1" IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 6. Payment Transactions
-- ============================================================================
-- Filters the materialized table to transactions that contain payment
-- information (BPR segment). BPR_1 is the transaction handling code and
-- BPR_2 is the payment amount.
--
-- What you'll see:
--   - df_file_name:      Source file
--   - handling_code:     BPR-01 (e.g. "I" = remittance info only, "C" = payment)
--   - payment_amount:    BPR-02 total payment amount in dollars
--   - transaction_type:  ST-01 (835 or 820)
--
-- Expected: Rows from the 835/820 payment files where BPR_1 is populated

SELECT
    df_file_name,
    "BPR_1" AS handling_code,
    "BPR_2" AS payment_amount,
    "ST_1" AS transaction_type
FROM {{zone_name}}.edi.hipaa_materialized
WHERE "BPR_1" IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 7. Functional Group Analysis
-- ============================================================================
-- Groups by GS_1 (Functional Identifier Code) to show the functional
-- group breakdown. Most HIPAA healthcare transactions use "HC" (Health
-- Care), while benefit enrollment (834) uses "BE".
--
-- What you'll see:
--   - functional_group: GS-01 code
--   - group_name:       Human-readable name
--   - transaction_count: How many transactions in that group
--
-- Expected: HC = 10 transactions, BE = 1 transaction (834 enrollment)

SELECT
    "GS_1" AS functional_group,
    CASE "GS_1"
        WHEN 'HC' THEN 'Health Care'
        WHEN 'BE' THEN 'Benefit Enrollment and Maintenance'
        ELSE 'Other'
    END AS group_name,
    COUNT(*) AS transaction_count
FROM {{zone_name}}.edi.hipaa_messages
GROUP BY "GS_1"
ORDER BY transaction_count DESC;


-- ============================================================================
-- 8. Full Transaction JSON — Deep Access via df_transaction_json
-- ============================================================================
-- Every row includes df_transaction_json containing the complete parsed X12
-- transaction as a JSON structure. This enables access to ANY segment/element
-- — including segments not materialized (HL hierarchy, SV1 service lines,
-- EB eligibility benefits, STC status codes, etc.).
--
-- What you'll see:
--   - df_file_name:        Source file name
--   - transaction_type:    ST-01 code
--   - df_transaction_json: Full transaction as JSON (click to expand in the UI)
--
-- Tip: The JSON contains every segment with its elements. Use JSON functions
-- for deep access without needing materialized_paths.

SELECT
    df_file_name,
    "ST_1" AS transaction_type,
    df_transaction_json
FROM {{zone_name}}.edi.hipaa_messages
ORDER BY df_file_name
LIMIT 3;


-- ============================================================================
-- 9. SUMMARY — All Checks
-- ============================================================================
-- Automated pass/fail verification that the demo loaded correctly.
-- All checks should return PASS.

SELECT check_name, result FROM (

    -- Check 1: Total transaction count = 11 (one per .edi file)
    SELECT 'transaction_count_11' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.hipaa_messages) = 11
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: 11 distinct source files in df_file_name
    SELECT 'source_files_11' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.edi.hipaa_messages) = 11
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: At least 7 distinct transaction types (actual: 9)
    SELECT 'transaction_types' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT "ST_1") FROM {{zone_name}}.edi.hipaa_messages) >= 7
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: Materialized table also has 11 rows (same files, different columns)
    SELECT 'materialized_count_11' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.hipaa_materialized) = 11
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: BHT_2 (transaction purpose code) is populated in at least some rows
    SELECT 'bht_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.hipaa_materialized
                       WHERE "BHT_2" IS NOT NULL AND "BHT_2" <> '') > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: df_transaction_json is populated for all 11 transactions
    SELECT 'json_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.hipaa_messages
                       WHERE df_transaction_json IS NOT NULL) = 11
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
