-- ============================================================================
-- EDI Transportation & Logistics -- Demo Queries
-- ============================================================================
-- Queries showcasing how Delta Forge unifies X12 EDI transportation
-- documents across modes (motor, rail, warehouse) and trading partners
-- into queryable tables.
--
-- Two tables are available:
--   logistics_messages      -- Compact view: ISA/GS/ST headers + full JSON
--   logistics_materialized  -- Enriched view: headers + B2/B3/B10/N1/L3 columns
--
-- Column reference (always available -- X12 default fields):
--   ISA_6  = Interchange Sender ID      ISA_8  = Interchange Receiver ID
--   ISA_12 = Interchange Control Version GS_1   = Functional Identifier Code
--   GS_8   = Version/Release/Industry ID ST_1   = Transaction Set ID Code
--   ST_2   = Transaction Set Control Number
--   df_transaction_json = Full transaction as JSON
--   df_transaction_id   = Unique transaction hash
--
-- Materialized columns (logistics_materialized table only):
--   B2_2  = SCAC (carrier code)         B2_4  = Shipment ID (load tender)
--   B3_2  = Invoice number              B3_3  = Shipment ID (freight invoice)
--   B10_1 = Reference ID (shipment)     B10_2 = BOL number (shipment status)
--   N1_1  = Entity code (SH/CN/BT)     N1_2  = Party name
--   L3_1  = Weight                      L3_5  = Total charges
-- ============================================================================


-- ============================================================================
-- 1. All Transactions -- Header Overview
-- ============================================================================
-- This query reads from the compact table (logistics_messages) to show the
-- ISA/GS/ST header of every EDI transaction. Each row is one transaction
-- from one .edi file.
--
-- What you'll see:
--   - df_file_name:  The source .edi file this row came from
--   - sender:        ISA_6 Interchange Sender ID (e.g. MGCTLYST, SCAC, SENDER1)
--   - receiver:      ISA_8 Interchange Receiver ID (e.g. SCAC, MGCTLYST, RECEIVER1)
--   - transaction:   ST_1 Transaction Set ID Code (204, 210, 214, 404, 820, 832, 945, 990)
--   - func_group:    GS_1 Functional Identifier Code (SM, IM, IN, QM, RA, SC, GF)
--   - version:       GS_8 Version/Release code (004010, 004030, 005010)
--
-- Expected: 12 rows -- one per .edi file

SELECT
    df_file_name,
    "ISA_6" AS sender,
    "ISA_8" AS receiver,
    "ST_1"  AS transaction,
    "GS_1"  AS func_group,
    "GS_8"  AS version
FROM {{zone_name}}.edi.logistics_messages
ORDER BY df_file_name;


-- ============================================================================
-- 2. Transaction Type Distribution
-- ============================================================================
-- Groups transactions by their X12 Transaction Set ID (ST_1) to show how
-- many of each document type are in the dataset.
--
-- What you'll see:
--   - transaction_type: X12 code -- 204 (Load Tender), 210 (Freight Invoice),
--                       214 (Shipment Status), 404 (Rail Shipment),
--                       820 (Payment Order), 832 (Price Catalog),
--                       945 (Warehouse Advice), 990 (Tender Response)
--   - doc_count:        How many transactions of each type
--
-- Expected: 8 distinct types -- 204=1, 210=2, 214=3, 404=1, 820=1,
--           832=2, 945=1, 990=1 (total 12)

SELECT
    "ST_1"  AS transaction_type,
    COUNT(*) AS doc_count
FROM {{zone_name}}.edi.logistics_messages
GROUP BY "ST_1"
ORDER BY "ST_1";


-- ============================================================================
-- 3. Functional Group Codes
-- ============================================================================
-- Groups transactions by GS_1 (Functional Identifier Code). This code
-- indicates the business purpose of the functional group:
--   SM = Motor Carrier Load Tender     IM = Motor Carrier Invoice
--   IN = Invoice (general)             QM = Transportation Status
--   RA = Remittance Advice             SC = Price/Sales Catalog
--   GF = Response to a Load Tender
--
-- What you'll see:
--   - func_group:   GS_1 code
--   - doc_count:    Number of transactions using that code
--
-- Expected: 7 distinct codes -- SM=1, IM=1, IN=5, QM=2, RA=1, SC=1, GF=1
--           (total 12)

SELECT
    "GS_1"  AS func_group,
    COUNT(*) AS doc_count
FROM {{zone_name}}.edi.logistics_messages
GROUP BY "GS_1"
ORDER BY "GS_1";


-- ============================================================================
-- 4. X12 Version Distribution
-- ============================================================================
-- Groups by ISA_12 (Interchange Control Version Number) to show which
-- X12 standard versions are represented. The dataset spans five versions
-- demonstrating Delta Forge parsing multiple X12 versions in a single table.
--
-- What you'll see:
--   - x12_version:   ISA_12 version code
--   - doc_count:     Number of transactions using that version
--
-- Expected: 5 distinct versions --
--   00204=5  (210 edifabric, 214 edifabric, 404, 832 edifabric, 945)
--   00400=1  (990)
--   00401=3  (204, 210 freight, 832 sales)
--   00403=2  (214 shipment, 214 transportation)
--   00501=1  (820)
--   Total = 12

SELECT
    "ISA_12" AS x12_version,
    COUNT(*) AS doc_count
FROM {{zone_name}}.edi.logistics_messages
GROUP BY "ISA_12"
ORDER BY "ISA_12";


-- ============================================================================
-- 5. Shipment Status Messages (214)
-- ============================================================================
-- Filters to only X12 214 (Transportation Carrier Shipment Status)
-- transactions. These are the tracking/status update messages that carriers
-- send to shippers about in-transit shipments.
--
-- What you'll see:
--   - df_file_name:  Source file (3 different 214 files)
--   - sender:        ISA_6 -- who sent the status update
--   - receiver:      ISA_8 -- who received it
--   - control_num:   ST_2 -- transaction set control number
--   - version:       GS_8 -- X12 version used
--
-- Expected: 3 rows -- the three 214 transactions from different sources
--   and different X12 versions (004030 and 004010)

SELECT
    df_file_name,
    "ISA_6"  AS sender,
    "ISA_8"  AS receiver,
    "ST_2"   AS control_num,
    "GS_8"   AS version
FROM {{zone_name}}.edi.logistics_messages
WHERE "ST_1" = '214'
ORDER BY df_file_name;


-- ============================================================================
-- 6. Trading Partner Analysis
-- ============================================================================
-- Groups by sender (ISA_6) and receiver (ISA_8) to reveal the trading
-- partner relationships in this dataset. Each unique sender/receiver pair
-- represents an EDI partnership.
--
-- What you'll see:
--   - sender:    ISA_6 Interchange Sender ID (trimmed by X12 parser)
--   - receiver:  ISA_8 Interchange Receiver ID
--   - doc_count: How many transactions flow between this pair
--
-- Expected: Several distinct pairs including:
--   MGCTLYST -> SCAC (1: load tender 204)
--   SCAC -> MGCTLYST (4: 210 freight, 214 shipment, 214 transportation, 990)
--   SENDER1 -> RECEIVER1 (5: 210 edifabric, 214 edifabric, 404, 832 edifabric, 945)
--   TPTESTUS00 -> TESTCOMP (1: 820)
--   999999999 -> 6309246701 (1: 832 sales)

SELECT
    "ISA_6"  AS sender,
    "ISA_8"  AS receiver,
    COUNT(*) AS doc_count
FROM {{zone_name}}.edi.logistics_messages
GROUP BY "ISA_6", "ISA_8"
ORDER BY doc_count DESC;


-- ============================================================================
-- 7. Materialized Logistics Fields -- Shipment Status Details
-- ============================================================================
-- Reads from the materialized table where B10/N1/L3 fields have been
-- extracted as first-class columns. Filters to rows where B10_1 (reference
-- identification / shipment ID) is populated -- these are the 214 Shipment
-- Status transactions that contain the B10 segment.
--
-- What you'll see:
--   - df_file_name:  Source .edi file
--   - shipment_id:   B10_1 -- reference identification for tracking
--   - bol_number:    B10_2 -- bill of lading number
--   - party_code:    N1_1 -- entity identifier (SH=shipper, CN=consignee, BT=bill-to)
--   - party_name:    N1_2 -- trading partner company name
--
-- Expected: Rows from the 214 transactions where B10 segments are present,
--   showing shipment IDs like "1751807" and "123456" with their parties

SELECT
    df_file_name,
    "B10_1" AS shipment_id,
    "B10_2" AS bol_number,
    "N1_1"  AS party_code,
    "N1_2"  AS party_name
FROM {{zone_name}}.edi.logistics_materialized
WHERE "B10_1" IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 8. Full Transaction JSON -- Deep Access
-- ============================================================================
-- Every row includes df_transaction_json containing the complete parsed
-- X12 transaction as a JSON object of segments. This enables access to ANY
-- segment/field -- including segments not materialized (AT7 status details,
-- BPR payment info, W12 warehouse items, LIN line items, etc.).
--
-- What you'll see:
--   - df_file_name:         Source file name
--   - transaction:          ST_1 transaction set type
--   - df_transaction_json:  Full transaction as JSON (click to expand in the UI)
--
-- Tip: The JSON contains every segment with its fields. Use JSON functions
-- for deep access without needing materialized_paths.

SELECT
    df_file_name,
    "ST_1" AS transaction,
    df_transaction_json
FROM {{zone_name}}.edi.logistics_messages
ORDER BY df_file_name
LIMIT 3;


-- ============================================================================
-- 9. SUMMARY -- All Checks
-- ============================================================================
-- Automated pass/fail verification that the demo loaded correctly.
-- All checks should return PASS.

SELECT check_name, result FROM (

    -- Check 1: Total transaction count = 12 (one per .edi file)
    SELECT 'transaction_count_12' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.logistics_messages) = 12
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: 12 distinct source files in df_file_name
    SELECT 'source_files_12' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.edi.logistics_messages) = 12
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: At least 6 distinct transaction types (actual: 8 -- 204,210,214,404,820,832,945,990)
    SELECT 'transaction_types' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT "ST_1") FROM {{zone_name}}.edi.logistics_messages) >= 6
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: Materialized table also has 12 rows (same files, different columns)
    SELECT 'materialized_count_12' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.logistics_materialized) = 12
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: B10_1 (shipment reference ID) is populated in at least some rows
    SELECT 'b10_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.logistics_materialized
                       WHERE "B10_1" IS NOT NULL AND "B10_1" <> '') > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: df_transaction_json is populated for all 12 transactions
    SELECT 'json_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.logistics_messages
                       WHERE df_transaction_json IS NOT NULL) = 12
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
