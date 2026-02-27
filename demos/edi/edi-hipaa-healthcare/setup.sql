-- ============================================================================
-- EDI HIPAA Healthcare — Setup Script
-- ============================================================================
-- Ingests 11 HIPAA X12 healthcare transactions covering the complete claims
-- lifecycle: eligibility verification, claims submission, payment/remittance,
-- benefit enrollment, and services review.
--
-- Transaction types included:
--   270/271 — Eligibility inquiry & response
--   276/277 — Claim status request & response
--   278     — Health services review (prior authorization)
--   820     — Payment order
--   834     — Benefit enrollment
--   835     — Claim payment/remittance advice
--   837     — Healthcare claim (professional, dental, institutional)
--
-- Two tables demonstrate different views of the same HIPAA transaction feed:
--   1. hipaa_messages      — Compact: ISA/GS/ST headers + full JSON + transaction ID
--   2. hipaa_materialized  — Materialized: headers + key BHT/NM1/CLM/BPR fields
--
-- Variables (auto-injected by Delta Forge):
--   data_path     — Local or cloud path where demo data files were downloaded
--   current_user  — Username of the current logged-in user
--   zone_name     — Target zone name (defaults to 'external')
--
-- Naming convention: zone_name.format.table
--   zone   = {{zone_name}}  (defaults to 'external')
--   schema = 'edi'          (the file format)
--   table  = object name
-- ============================================================================


-- ============================================================================
-- STEP 1: Zone & Schema
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}}
    TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.edi
    COMMENT 'EDI transaction-backed external tables';


-- ============================================================================
-- TABLE 1: hipaa_messages — Compact view (11 transactions)
-- ============================================================================
-- Default X12 output: ISA envelope fields (ISA_1 through ISA_16),
-- GS functional group fields (GS_1 through GS_8), ST transaction header
-- (ST_1, ST_2), df_transaction_json (full transaction as JSON), and
-- df_transaction_id (unique hash).
-- Use df_transaction_json with JSON functions for deep segment access.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi.hipaa_messages
USING EDI
LOCATION '{{data_path}}/*.edi'
OPTIONS (
    edi_config = '{"ediFormat": "x12"}',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

DETECT SCHEMA FOR TABLE {{zone_name}}.edi.hipaa_messages;

GRANT ADMIN ON TABLE {{zone_name}}.edi.hipaa_messages TO USER {{current_user}};


-- ============================================================================
-- TABLE 2: hipaa_materialized — Key healthcare fields extracted (11 transactions)
-- ============================================================================
-- Uses materialized_paths to extract commonly-queried HIPAA fields as
-- first-class columns alongside the default ISA/GS/ST + JSON output.
--
-- Materialized columns:
--   BHT_1  — Hierarchical structure code (purpose of transaction)
--   BHT_2  — Transaction set purpose code (e.g. "00" = original)
--   BHT_6  — Transaction type code (identifies claim/encounter type)
--   NM1_1  — Entity identifier code (e.g. "IL" = insured, "85" = billing provider)
--   NM1_2  — Entity type qualifier (1 = person, 2 = non-person entity)
--   NM1_3  — Name last or organization name
--   CLM_1  — Claim submitter identifier (patient account number)
--   CLM_2  — Total claim charge amount
--   BPR_1  — Transaction handling code (payment method)
--   BPR_2  — Total payment amount
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi.hipaa_materialized
USING EDI
LOCATION '{{data_path}}/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "x12",
        "materialized_paths": [
            "BHT_1", "BHT_2", "BHT_6",
            "NM1_1", "NM1_2", "NM1_3",
            "CLM_1", "CLM_2",
            "BPR_1", "BPR_2"
        ]
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

DETECT SCHEMA FOR TABLE {{zone_name}}.edi.hipaa_materialized;

GRANT ADMIN ON TABLE {{zone_name}}.edi.hipaa_materialized TO USER {{current_user}};
