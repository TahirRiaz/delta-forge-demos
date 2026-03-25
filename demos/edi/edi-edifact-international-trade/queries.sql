-- ============================================================================
-- EDI EDIFACT International Trade — Demo Queries
-- ============================================================================
-- Queries showcasing how Delta Forge unifies EDIFACT and EANCOM messages
-- from shipping lines, customs authorities, airlines, and retail supply
-- chains into queryable tables.
--
-- Two tables are available:
--   edifact_messages      — Compact view: UNB/UNH headers + full JSON
--   edifact_materialized  — Enriched view: headers + key trade fields
--
-- Column reference (always available — UNB interchange envelope):
--   UNB_1  = Syntax identifier (UNOA, UNOB, UNOC, UNOL, IATB, IATA)
--   UNB_2  = Interchange sender
--   UNB_3  = Interchange recipient
--   UNB_4  = Date/time of preparation
--   UNB_5  = Interchange control reference
--
-- Column reference (always available — UNH message header):
--   UNH_1  = Message reference number
--   UNH_2  = Message identifier (type:directory:version:agency[:association])
--
-- Materialized columns (edifact_materialized table only):
--   BGM_1  = Document name code    BGM_2  = Document number
--   NAD_1  = Party qualifier       NAD_2  = Party identification
--   DTM_1  = Date/time qualifier   DTM_2  = Date/time value
--   LIN_1  = Line item number      LIN_3  = Item number
-- ============================================================================


-- ============================================================================
-- 1. All Messages — Header Overview
-- ============================================================================
-- This query reads from the compact table (edifact_messages) to show the
-- UNB/UNH header of every EDIFACT message. Each row is one message parsed
-- from the 22 source files (some files contain multiple messages).
--
-- What you'll see:
--   - df_file_name:  The source .edi file this row came from
--   - syntax_id:     UNB syntax identifier (UNOA, UNOB, UNOC, UNOL, IATB, IATA)
--   - msg_type:      UNH message identifier (e.g. ORDERS:D:96A:UN)
--   - msg_ref:       UNH message reference number
--   - sender:        Interchange sender from UNB_2

ASSERT ROW_COUNT >= 22
ASSERT VALUE syntax_id = 'UNOB' WHERE df_file_name = 'edifact_ORDERS_purchase_order.edi'
ASSERT VALUE sender = 'SENDER1' WHERE df_file_name = 'edifact_ORDERS_purchase_order.edi'
ASSERT VALUE msg_type = 'ORDERS' WHERE df_file_name = 'edifact_ORDERS_purchase_order.edi'
ASSERT VALUE syntax_id = 'IATB' WHERE df_file_name = 'edifact_wikipedia_example.edi'
ASSERT VALUE msg_type = 'PAORES' WHERE df_file_name = 'edifact_wikipedia_example.edi'
ASSERT VALUE msg_type = 'DESADV' WHERE df_file_name = 'eancom_DESADV_despatch_advice.edi'
SELECT
    df_file_name,
    unb_1 AS syntax_id,
    unh_2 AS msg_type,
    unh_1 AS msg_ref,
    unb_2 AS sender
FROM {{zone_name}}.edi.edifact_messages
ORDER BY df_file_name;


-- ============================================================================
-- 2. Message Type Distribution
-- ============================================================================
-- Groups messages by their UNH_2 message identifier to show the diversity
-- of EDIFACT message types in this international trade feed. Each type
-- represents a different business process: ordering, invoicing, transport,
-- customs clearance, or acknowledgment.
--
-- What you'll see:
--   - msg_type:   UNH message identifier (type:directory:version:agency)
--   - msg_count:  Number of messages of each type
--
-- >= 10 distinct message types across ORDERS, ORDRSP, INVOIC,
-- IFCSUM, CUSCAR, BAPLIE, PAXLST, PNRGOV, APERAK, CONTRL, INFENT,
-- QUOTES, PAORES, DESADV, IFTSTA, PRICAT, IFTMIN

ASSERT ROW_COUNT >= 10
ASSERT VALUE msg_count = 2 WHERE msg_type = 'CONTRL'
ASSERT VALUE msg_count = 1 WHERE msg_type = 'ORDERS'
ASSERT VALUE msg_count = 1 WHERE msg_type = 'PAORES'
SELECT
    unh_2 AS msg_type,
    COUNT(*) AS msg_count
FROM {{zone_name}}.edi.edifact_messages
GROUP BY unh_2
ORDER BY msg_count DESC, unh_2;


-- ============================================================================
-- 3. Syntax Version Distribution
-- ============================================================================
-- Shows how many messages came from each UN/EDIFACT syntax identifier.
-- UNB_1 identifies the character set and syntax rules used:
--   UNOA = Basic Latin (level A)    UNOB = Latin with lowercase (level B)
--   UNOC = Latin-1 extended         UNOL = Latin-2 extended
--   IATB = IATA variant B           IATA = IATA variant A
--
-- This demonstrates Delta Forge parsing multiple syntax variants in a
-- single table without any version-specific configuration.

ASSERT ROW_COUNT >= 4
ASSERT VALUE msg_count >= 1 WHERE syntax_id = 'IATB'
ASSERT VALUE msg_count >= 1 WHERE syntax_id = 'UNOB'
ASSERT VALUE msg_count >= 1 WHERE syntax_id = 'UNOC'
SELECT
    unb_1 AS syntax_id,
    CASE unb_1
        WHEN 'UNOA' THEN 'Basic Latin (Level A)'
        WHEN 'UNOB' THEN 'Latin with lowercase (Level B)'
        WHEN 'UNOC' THEN 'Latin-1 extended (Level C)'
        WHEN 'UNOL' THEN 'Latin-2 extended (Level L)'
        WHEN 'IATB' THEN 'IATA variant B'
        WHEN 'IATA' THEN 'IATA variant A'
        ELSE unb_1
    END AS syntax_name,
    COUNT(*) AS msg_count
FROM {{zone_name}}.edi.edifact_messages
GROUP BY unb_1
ORDER BY msg_count DESC;


-- ============================================================================
-- 4. Commerce vs Transport vs Customs — Domain Classification
-- ============================================================================
-- Categorizes each message into a business domain based on its UNH_2
-- message type. This shows the breadth of international trade processes
-- captured in a single EDIFACT feed.
--
-- Categories:
--   Commerce      — ORDERS, ORDRSP, INVOIC, PRICAT, QUOTES
--   Transport     — IFCSUM, IFTSTA, IFTMIN, BAPLIE, DESADV
--   Border        — CUSCAR, PAXLST, PNRGOV
--   Acknowledgment — APERAK, CONTRL
--   Other         — INFENT, PAORES, etc.

ASSERT ROW_COUNT >= 15
ASSERT VALUE domain = 'Commerce' WHERE msg_type = 'ORDERS'
ASSERT VALUE domain = 'Transport' WHERE msg_type = 'DESADV'
ASSERT VALUE domain = 'Acknowledgment' WHERE msg_type = 'CONTRL'
ASSERT VALUE domain = 'Other' WHERE msg_type = 'PAORES'
SELECT
    CASE
        WHEN unh_2 LIKE 'ORDERS%' OR unh_2 LIKE 'ORDRSP%' OR unh_2 LIKE 'INVOIC%'
             OR unh_2 LIKE 'PRICAT%' OR unh_2 LIKE 'QUOTES%'
            THEN 'Commerce'
        WHEN unh_2 LIKE 'IFCSUM%' OR unh_2 LIKE 'IFTSTA%' OR unh_2 LIKE 'IFTMIN%'
             OR unh_2 LIKE 'BAPLIE%' OR unh_2 LIKE 'DESADV%'
            THEN 'Transport'
        WHEN unh_2 LIKE 'CUSCAR%' OR unh_2 LIKE 'PAXLST%' OR unh_2 LIKE 'PNRGOV%'
            THEN 'Border'
        WHEN unh_2 LIKE 'APERAK%' OR unh_2 LIKE 'CONTRL%'
            THEN 'Acknowledgment'
        ELSE 'Other'
    END AS domain,
    unh_2 AS msg_type,
    COUNT(*) AS msg_count
FROM {{zone_name}}.edi.edifact_messages
GROUP BY
    CASE
        WHEN unh_2 LIKE 'ORDERS%' OR unh_2 LIKE 'ORDRSP%' OR unh_2 LIKE 'INVOIC%'
             OR unh_2 LIKE 'PRICAT%' OR unh_2 LIKE 'QUOTES%'
            THEN 'Commerce'
        WHEN unh_2 LIKE 'IFCSUM%' OR unh_2 LIKE 'IFTSTA%' OR unh_2 LIKE 'IFTMIN%'
             OR unh_2 LIKE 'BAPLIE%' OR unh_2 LIKE 'DESADV%'
            THEN 'Transport'
        WHEN unh_2 LIKE 'CUSCAR%' OR unh_2 LIKE 'PAXLST%' OR unh_2 LIKE 'PNRGOV%'
            THEN 'Border'
        WHEN unh_2 LIKE 'APERAK%' OR unh_2 LIKE 'CONTRL%'
            THEN 'Acknowledgment'
        ELSE 'Other'
    END,
    unh_2
ORDER BY domain, msg_type;


-- ============================================================================
-- 5. Document Details — Materialized Fields
-- ============================================================================
-- This query reads from the materialized table to show document details
-- extracted from BGM (Beginning of Message) segments. The BGM segment
-- appears in most EDIFACT messages and carries the document type code
-- and document number.
--
-- What you'll see:
--   - df_file_name:  Source file name
--   - msg_type:      UNH message identifier
--   - doc_code:      Document name code from BGM_1 (220=Order, 380=Invoice,
--                    231=Cargo report, etc.)
--   - doc_number:    Document/message number from BGM_2

ASSERT ROW_COUNT = 19
ASSERT VALUE doc_code = '220' WHERE df_file_name = 'edifact_ORDERS_purchase_order.edi'
ASSERT VALUE doc_number = '128576' WHERE df_file_name = 'edifact_ORDERS_purchase_order.edi'
ASSERT VALUE doc_code = '351' WHERE df_file_name = 'eancom_DESADV_despatch_advice.edi'
ASSERT VALUE doc_number = 'DES587441' WHERE df_file_name = 'eancom_DESADV_despatch_advice.edi'
SELECT
    df_file_name,
    unh_2 AS msg_type,
    bgm_1 AS doc_code,
    bgm_2 AS doc_number
FROM {{zone_name}}.edi.edifact_materialized
WHERE bgm_1 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 6. Trading Partners — Sender / Receiver Pairs
-- ============================================================================
-- Groups messages by their UNB_2 (sender) and UNB_3 (receiver) to
-- identify all trading partner relationships in the feed. These fields
-- come from the UNB interchange envelope and identify the organizations
-- exchanging messages.
--
-- What you'll see:
--   - sender:        Interchange sender from UNB_2
--   - receiver:      Interchange recipient from UNB_3
--   - msg_count:     Number of messages between this pair
--
-- Multiple distinct sender/receiver pairs spanning different
-- industries (retail, shipping, customs, airlines)

ASSERT ROW_COUNT >= 10
ASSERT VALUE msg_count >= 1 WHERE sender = 'SENDER1'
ASSERT VALUE msg_count >= 1 WHERE sender = '6XPPC'
SELECT
    unb_2 AS sender,
    unb_3 AS receiver,
    COUNT(*) AS msg_count
FROM {{zone_name}}.edi.edifact_messages
GROUP BY unb_2, unb_3
ORDER BY msg_count DESC;


-- ============================================================================
-- 7. EANCOM vs Pure EDIFACT — Standard Classification
-- ============================================================================
-- Classifies messages by whether they came from EANCOM files (GS1 retail
-- supply chain subset of EDIFACT) or pure EDIFACT files. EANCOM messages
-- use the same EDIFACT syntax but include GS1 association-assigned codes
-- in UNH_2 (e.g. :EAN007, :EAN009, :EAN011).
--
-- The classification here uses the file name prefix as the indicator.
--
-- What you'll see:
--   - standard:    'EANCOM' or 'EDIFACT'
--   - file_count:  Number of source files in each category
--   - msg_count:   Number of messages in each category

ASSERT ROW_COUNT = 2
ASSERT VALUE file_count = 6 WHERE standard = 'EANCOM'
ASSERT VALUE file_count = 16 WHERE standard = 'EDIFACT'
ASSERT VALUE msg_count = 6 WHERE standard = 'EANCOM'
ASSERT VALUE msg_count = 18 WHERE standard = 'EDIFACT'
SELECT
    CASE
        WHEN df_file_name LIKE 'eancom%' THEN 'EANCOM'
        ELSE 'EDIFACT'
    END AS standard,
    COUNT(DISTINCT df_file_name) AS file_count,
    COUNT(*) AS msg_count
FROM {{zone_name}}.edi.edifact_messages
GROUP BY
    CASE
        WHEN df_file_name LIKE 'eancom%' THEN 'EANCOM'
        ELSE 'EDIFACT'
    END
ORDER BY standard;


-- ============================================================================
-- 8. Full Transaction JSON — Deep Access via df_transaction_json
-- ============================================================================
-- Every row includes df_transaction_json containing the complete parsed
-- EDIFACT message as a JSON object. This enables access to ANY segment
-- and element — including segments not materialized (TAX tax details,
-- MOA monetary amounts, TDT transport details, GID goods item details,
-- EQD equipment details, DOC document references).
--
-- What you'll see:
--   - df_file_name:         Source file name
--   - msg_type:             Message identifier from UNH_2
--   - df_transaction_json:  Full message as JSON (click to expand in UI)
--
-- Tip: The JSON contains every segment with its elements. Use JSON
-- functions for deep access without needing materialized_paths.

ASSERT ROW_COUNT = 3
SELECT
    df_file_name,
    unh_2 AS msg_type,
    df_transaction_json
FROM {{zone_name}}.edi.edifact_messages
ORDER BY df_file_name
LIMIT 3;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Automated pass/fail verification that the demo loaded correctly.
-- All checks should return PASS.
--
-- Uses >= thresholds where multi-message files (edifact_multi_message.edi
-- and edifact_CONTRL_acknowledgment.edi each contain 2 messages) may
-- produce more rows than the 22 source files.

ASSERT ROW_COUNT = 6
ASSERT VALUE result = 'PASS' WHERE check_name = 'message_count'
ASSERT VALUE result = 'PASS' WHERE check_name = 'source_files_22'
ASSERT VALUE result = 'PASS' WHERE check_name = 'json_populated'
SELECT check_name, result FROM (

    -- Check 1: Total message count >= 22 (22 files, some with 2 messages)
    SELECT 'message_count' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.edifact_messages) >= 22
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: 22 distinct source files in df_file_name
    SELECT 'source_files_22' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.edi.edifact_messages) = 22
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: At least 10 distinct message types (actual: ~17 distinct UNH_2 values)
    SELECT 'multi_message_type' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT unh_2) FROM {{zone_name}}.edi.edifact_messages) >= 10
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: Materialized table also has >= 22 rows
    SELECT 'materialized_count' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.edifact_materialized) >= 22
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: BGM_1 (document code) is populated for at least some rows
    SELECT 'bgm_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.edifact_materialized
                       WHERE bgm_1 IS NOT NULL AND bgm_1 <> '') > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: df_transaction_json is populated for all messages
    SELECT 'json_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.edifact_messages
                       WHERE df_transaction_json IS NOT NULL) >= 22
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
