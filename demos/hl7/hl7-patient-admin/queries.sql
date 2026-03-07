-- ============================================================================
-- HL7 Patient Administration — Demo Queries
-- ============================================================================
-- Queries showcasing how Delta Forge unifies ADT messages from multiple EHR
-- systems and HL7 versions (v2.3 through v2.6) into queryable tables.
--
-- Two tables are available:
--   adt_messages      — Compact view: MSH header fields + full JSON
--   adt_materialized  — Enriched view: MSH headers + PID/PV1/EVN columns
--
-- Column reference (always available — MSH header fields):
--   MSH_3  = Sending Application     MSH_4  = Sending Facility
--   MSH_7  = Message Date/Time       MSH_9  = Message Type (ADT^A01^ADT_A01)
--   MSH_10 = Message Control ID      MSH_12 = Version ID
--
-- Materialized columns (adt_materialized table only):
--   PID_3  = Patient ID              PID_5  = Patient Name (LAST^FIRST^MID)
--   PID_7  = Date of Birth           PID_8  = Gender
--   PID_11 = Patient Address         PV1_2  = Patient Class (I/O/E)
--   PV1_3  = Assigned Location       PV1_7  = Attending Physician
--   EVN_1  = Event Type Code         EVN_2  = Recorded Date/Time
-- ============================================================================


-- ============================================================================
-- 1. All ADT Messages — Header Overview
-- ============================================================================
-- This query reads from the compact table (adt_messages) to show the
-- MSH header of every ADT message. Each row is one HL7 message file.
--
-- What you'll see:
--   - df_file_name:  The source .hl7 file this row came from
--   - sending_app:   The EHR system that sent the message (EPIC, MegaReg, etc.)
--   - sending_facility: The hospital/clinic that sent it
--   - message_type:  Full HL7 type (e.g. "ADT^A01^ADT_A01" for admission)
--   - hl7_version:   Protocol version (2.3, 2.3.1, 2.5, 2.5.1, or 2.6)
--   - message_control_id: Unique ID assigned by the sending system
--
-- Expected: 8 rows — one per ADT file (6 admissions, 1 update, 1 discharge)

SELECT
    df_file_name,
    msh_3 AS sending_app,
    msh_4 AS sending_facility,
    msh_9 AS message_type,
    msh_12 AS hl7_version,
    msh_10 AS message_control_id
FROM {{zone_name}}.hl7.adt_messages
ORDER BY df_file_name;


-- ============================================================================
-- 2. Patient Demographics — Materialized View
-- ============================================================================
-- This query reads from the materialized table (adt_materialized) where
-- PID and PV1 fields have been extracted as first-class SQL columns.
--
-- What you'll see:
--   - patient_name:  Full name with HL7 component separators
--                    (e.g. "SMITH^JOHN^DAVID^JR^MR" = Last^First^Middle^Suffix^Title)
--   - date_of_birth: Patient DOB from PID-7 (format varies by source system)
--   - gender:        M or F from PID-8
--   - address:       Street^Unit^City^State^Zip from PID-11
--   - patient_class: I=Inpatient, O=Outpatient, E=Emergency from PV1-2
--
-- Expected: 8 rows, one per message file

SELECT
    df_file_name,
    pid_5 AS patient_name,
    pid_7 AS date_of_birth,
    pid_8 AS gender,
    pid_11 AS address,
    pv1_2 AS patient_class
FROM {{zone_name}}.hl7.adt_materialized
ORDER BY pid_5;


-- ============================================================================
-- 3. HL7 Version Distribution
-- ============================================================================
-- Shows how many messages came from each HL7 protocol version.
-- This demonstrates Delta Forge parsing multiple HL7 versions in a
-- single table without any version-specific configuration.
--
-- What you'll see:
--   - hl7_version:    The HL7 v2.x version string from MSH-12
--   - message_count:  How many messages use that version
--
-- Expected: 5 distinct versions — 2.3, 2.3.1, 2.5, 2.5.1, 2.6

SELECT
    msh_12 AS hl7_version,
    COUNT(*) AS message_count
FROM {{zone_name}}.hl7.adt_messages
GROUP BY msh_12
ORDER BY msh_12;


-- ============================================================================
-- 4. Event Type Distribution
-- ============================================================================
-- Groups messages by their HL7 event type. MSH_9 contains the full
-- message type including trigger event. The format varies by HL7 version:
--   - v2.5+: "ADT^A01^ADT_A01" (type^trigger^structure)
--   - v2.3:  "ADT^A01" (type^trigger only)
--
-- What you'll see:
--   - message_type:   Full type string — different A01 formats will group
--                     separately (e.g. "ADT^A01" vs "ADT^A01^ADT_A01")
--   - message_count:  How many messages of each type
--
-- Expected: Several groups — A01 variants (admissions), A03 (discharge),
-- A08 (demographics update)

SELECT
    msh_9 AS message_type,
    COUNT(*) AS message_count
FROM {{zone_name}}.hl7.adt_messages
GROUP BY msh_9
ORDER BY message_count DESC;


-- ============================================================================
-- 5. Visit Information — Inpatient vs Outpatient
-- ============================================================================
-- Combines patient name with visit details from the materialized table.
-- PV1-2 (Patient Class) classifies the encounter type:
--   I = Inpatient (admitted), O = Outpatient, E = Emergency
--
-- What you'll see:
--   - patient_name:        Full name from PID-5
--   - patient_class:       I, O, or E (NULL if PV1-2 is empty)
--   - assigned_location:   Ward^room^bed from PV1-3 (e.g. "ICU^0101^01^HOSP")
--   - attending_physician: Provider ID^name from PV1-7
--
-- Expected: Mix of I, O, and E across the 8 messages

SELECT
    df_file_name,
    pid_5 AS patient_name,
    pv1_2 AS patient_class,
    pv1_3 AS assigned_location,
    pv1_7 AS attending_physician
FROM {{zone_name}}.hl7.adt_materialized
ORDER BY pv1_2;


-- ============================================================================
-- 6. Sending Systems — Cross-EHR Analysis
-- ============================================================================
-- Groups messages by the sending application and facility to show how
-- many different EHR systems feed into this single unified table.
-- Each system may use a different HL7 version.
--
-- What you'll see:
--   - sending_application: EHR system name (EPIC, MegaReg, ADT1, RegSys, etc.)
--   - sending_facility:    Hospital or organization name
--   - hl7_version:         Protocol version used by that system
--   - messages:            Count of messages from that source
--
-- Expected: 8 rows — each message comes from a unique system/facility pair

SELECT
    msh_3 AS sending_application,
    msh_4 AS sending_facility,
    msh_12 AS hl7_version,
    COUNT(*) AS messages
FROM {{zone_name}}.hl7.adt_messages
GROUP BY msh_3, msh_4, msh_12
ORDER BY msh_3;


-- ============================================================================
-- 7. Full Message JSON — Deep Access via df_message_json
-- ============================================================================
-- Every row includes df_message_json containing the complete parsed HL7
-- message as a JSON array of segments. This enables access to ANY
-- segment/field — including segments not materialized (NK1 next of kin,
-- AL1 allergies, DG1 diagnoses, GT1 guarantor, IN1 insurance, ZMP custom).
--
-- What you'll see:
--   - df_file_name:     Source file name
--   - message_type:     ADT event type from MSH-9
--   - df_message_json:  Full message as JSON (click to expand in the UI)
--
-- Tip: The JSON contains every segment with its fields as arrays. Use
-- JSON functions for deep access without needing materialized_paths.

SELECT
    df_file_name,
    msh_9 AS message_type,
    df_message_json
FROM {{zone_name}}.hl7.adt_messages
ORDER BY df_file_name
LIMIT 3;


-- ============================================================================
-- 8. File Metadata — Source Traceability
-- ============================================================================
-- Lists all distinct source file names. The df_file_name column is
-- injected by the file_metadata option and lets you trace every row
-- back to the originating .hl7 file — useful for auditing and debugging.
--
-- What you'll see:
--   - df_file_name: One entry per unique source file
--
-- Expected: 8 distinct files

SELECT DISTINCT df_file_name
FROM {{zone_name}}.hl7.adt_messages
ORDER BY df_file_name;


-- ============================================================================
-- 9. SUMMARY — All Checks
-- ============================================================================
-- Automated pass/fail verification that the demo loaded correctly.
-- All checks should return PASS.

SELECT check_name, result FROM (

    -- Check 1: Total message count = 8 (one per .hl7 file)
    SELECT 'message_count_8' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.hl7.adt_messages) = 8
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: 8 distinct source files in df_file_name
    SELECT 'source_files_8' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.hl7.adt_messages) = 8
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: At least 3 distinct HL7 versions (actual: 5)
    SELECT 'multi_version' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT msh_12) FROM {{zone_name}}.hl7.adt_messages) >= 3
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: Materialized table also has 8 rows (same files, different columns)
    SELECT 'materialized_count_8' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.hl7.adt_materialized) = 8
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: PID_5 (patient name) is populated in at least some rows
    SELECT 'pid5_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.hl7.adt_materialized
                       WHERE pid_5 IS NOT NULL AND pid_5 <> '') > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: df_message_json is populated for all 8 messages
    SELECT 'json_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.hl7.adt_messages
                       WHERE df_message_json IS NOT NULL) = 8
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
